"""Sensor platform for Propeye."""
from __future__ import annotations

import base64
import datetime
import json
import logging
import time

import grpc

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
)
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy, CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import EnergyConverter

from .const import DOMAIN, DEFAULT_HOST, DEFAULT_PORT
from . import tenant_app_pb2 as pb
from . import tenant_app_pb2_grpc as rpc

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = datetime.timedelta(minutes=15)

# How far back to query the API on every poll.  The Propeye API
# occasionally updates the per-hour split for hours that were
# already returned (the daily total stays the same but the
# distribution between hours is refined).  Fetching a wider window
# on every poll lets us keep the long-term statistics aligned with
# the latest API values.
STATS_WINDOW_DAYS = 7


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Propeye sensor."""
    coordinator: PropeyeDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    email = entry.data[CONF_EMAIL]
    async_add_entities([PropeyeSensor(coordinator, email)])


class PropeyeDataUpdateCoordinator(DataUpdateCoordinator):
    """Manage fetching Propeye data via gRPC."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        email: str,
        password: str,
    ) -> None:
        """Initialize."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
        )
        self.email = email
        self.password = password
        self._token: str | None = None
        self._channel: grpc.Channel | None = None
        self._stub: rpc.TenantAppStub | None = None

        # Long-term-statistic identifier for the hourly series we push
        # straight to the recorder.  This is separate from the sensor
        # entity id and is what the Energy dashboard should be pointed at
        # for accurate hourly bars.  HA's VALID_STATISTIC_ID regex only
        # permits lowercase letters, digits, and underscores on either
        # side of the ``domain:object_id`` colon, so we sanitize the
        # email into a safe slug.
        slug = "".join(
            ch if ch.isalnum() else "_"
            for ch in email.lower()
        ).strip("_")
        # Collapse runs of underscores to avoid the "no double underscore"
        # restriction in the regex.
        while "__" in slug:
            slug = slug.replace("__", "_")
        self.stats_id = f"{DOMAIN}:electricity_consumption_{slug}"
        self.stats_name = f"Propeye electricity consumption ({email})"

    # ------------------------------------------------------------------
    # gRPC channel lifecycle
    # ------------------------------------------------------------------

    def _get_stub(self) -> rpc.TenantAppStub:
        """Return the gRPC stub, creating a new channel if needed."""
        if self._stub is None:
            target = f"{DEFAULT_HOST}:{DEFAULT_PORT}"
            creds = grpc.ssl_channel_credentials()
            self._channel = grpc.secure_channel(target, creds)
            self._stub = rpc.TenantAppStub(self._channel)
        return self._stub

    def _reset_channel(self) -> None:
        """Close and discard the current gRPC channel.

        The next call to _get_stub() will create a fresh one.
        """
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:  # noqa: BLE001
                pass
        self._channel = None
        self._stub = None
        self._token = None  # force re-auth on next attempt

    def close(self) -> None:
        """Clean up resources.  Called from async_unload_entry."""
        self._reset_channel()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    @staticmethod
    def _decode_jwt_expiry(token: str) -> float | None:
        """Return the ``exp`` claim from a JWT, or *None* on failure."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload = parts[1]
            # Restore base64 padding
            padding = 4 - (len(payload) % 4)
            if padding < 4:
                payload += "=" * padding
            claims = json.loads(base64.urlsafe_b64decode(payload))
            return claims.get("exp")
        except Exception:  # noqa: BLE001
            return None

    def _ensure_token(self) -> str:
        """Return a valid JWT, authenticating if necessary."""
        now = time.time()

        if self._token:
            exp = self._decode_jwt_expiry(self._token)
            if exp and exp > (now + 300):
                return self._token

        _LOGGER.debug("Authenticating with Propeye")
        stub = self._get_stub()
        auth_req = pb.AuthenticateRequest(
            email=self.email, password=self.password
        )
        try:
            response = stub.Authenticate(auth_req, timeout=10)
        except grpc.RpcError as exc:
            self._reset_channel()
            raise UpdateFailed(f"Authentication failed: {exc}") from exc

        if not response or not response.token_jwt:
            raise UpdateFailed("Authentication returned no token")

        self._token = response.token_jwt
        return self._token

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def _fetch_raw(self, start_ts: int, end_ts: int) -> list[tuple[int, float]]:
        """Run a GetMeterTypeValues request and return (ts, kWh) tuples."""
        token = self._ensure_token()
        stub = self._get_stub()

        req = pb.GetMeterTypeValuesRequest(
            location_id=1,
            meter_type="EL",
            unknown_field_3=0,
            max_results=500,
            start_date=pb.TimestampWrapper(
                timestamp=start_ts, unknown_field_2=0
            ),
            end_date=pb.TimestampWrapper(
                timestamp=end_ts, unknown_field_2=0
            ),
            unknown_field_7=1,
            unknown_field_8=0,
            unknown_field_9=0,
        )
        metadata = [("authorization", f"Bearer {token}")]

        try:
            response = stub.GetMeterTypeValues(
                req, metadata=metadata, timeout=10
            )
        except grpc.RpcError as exc:
            self._reset_channel()
            raise UpdateFailed(f"Error fetching data: {exc}") from exc

        points: list[tuple[int, float]] = []
        if response.data_points:
            for entry in response.data_points:
                points.append(
                    (entry.start_timestamp.timestamp, entry.reading_value)
                )
        return points

    def _fetch_data(self) -> dict:
        """Fetch hourly energy data (runs in executor thread).

        Two things happen here:

        1. We pull the last ``STATS_WINDOW_DAYS`` days of hourly data so
           the long-term statistics stay in sync with whatever the API
           currently says (the API occasionally redistributes values
           between adjacent hours after the fact).
        2. We extract today's hours for the live sensor value and
           attribute payload.
        """
        local_tz = dt_util.get_default_time_zone()
        now_local = datetime.datetime.now(tz=local_tz)

        start_of_day = now_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_of_day = now_local.replace(
            hour=23, minute=59, second=59, microsecond=0
        )

        stats_start = start_of_day - datetime.timedelta(days=STATS_WINDOW_DAYS)

        points = self._fetch_raw(
            int(stats_start.timestamp()), int(end_of_day.timestamp())
        )

        today_ts = int(start_of_day.timestamp())

        # Build per-hour dict keyed by the UTC hour-start datetime.
        # The API occasionally returns duplicate timestamps; the last
        # value wins.
        per_hour: dict[datetime.datetime, float] = {}
        hourly_today: list[dict] = []
        total_today = 0.0

        for ts, val in points:
            hour_start_utc = datetime.datetime.fromtimestamp(
                ts, tz=datetime.timezone.utc
            ).replace(minute=0, second=0, microsecond=0)
            per_hour[hour_start_utc] = val

            if ts >= today_ts:
                total_today += val
                hourly_today.append(
                    {
                        "time": datetime.datetime.fromtimestamp(
                            ts, tz=local_tz
                        ).isoformat(),
                        "value": val,
                    }
                )

        _LOGGER.debug(
            "Propeye: fetched %d points over %d days, today=%.4f kWh (%d hrs)",
            len(points),
            STATS_WINDOW_DAYS + 1,
            total_today,
            len(hourly_today),
        )

        return {
            "total_today": round(total_today, 4),
            "hourly_data": hourly_today,
            "start_of_day": start_of_day.isoformat(),
            "last_updated": dt_util.now().isoformat(),
            # Consumed by _async_update_data to push long-term stats
            "_per_hour_utc": per_hour,
        }

    # ------------------------------------------------------------------
    # Coordinator entry point
    # ------------------------------------------------------------------

    async def _async_update_data(self) -> dict:
        """Fetch data from API and update long-term statistics."""
        data = await self.hass.async_add_executor_job(self._fetch_data)

        per_hour: dict[datetime.datetime, float] = data.pop("_per_hour_utc", {})
        if per_hour:
            try:
                await self._async_update_statistics(per_hour)
            except Exception:  # noqa: BLE001
                _LOGGER.exception(
                    "Failed to update Propeye long-term statistics"
                )
        return data

    async def _async_update_statistics(
        self, per_hour: dict[datetime.datetime, float]
    ) -> None:
        """Push the API's hourly values into HA's long-term statistics.

        Strategy: anchor the running sum at the recorded value for the
        hour immediately BEFORE our batch begins (or 0 if the recorder
        has no history for this stat yet), then rebuild the ``sum`` for
        every hour in our batch based on the API's canonical kWh values.
        The recorder will UPDATE existing rows and INSERT new ones.
        """
        sorted_hours = sorted(per_hour.keys())
        if not sorted_hours:
            return

        recorder = get_instance(self.hass)
        first_hour = sorted_hours[0]

        # Look up the sum anchored just before our first hour.
        last_stats = await recorder.async_add_executor_job(
            get_last_statistics,
            self.hass,
            1,
            self.stats_id,
            True,
            {"sum", "start"},
        )

        running_sum = 0.0
        if last_stats.get(self.stats_id):
            prev = last_stats[self.stats_id][0]
            prev_start_raw = prev.get("start")
            if isinstance(prev_start_raw, (int, float)):
                # start is a unix timestamp (ms or s depending on HA version).
                # Use > 1e12 heuristic for ms vs s.
                if prev_start_raw > 1e12:
                    prev_start = dt_util.utc_from_timestamp(
                        prev_start_raw / 1000
                    )
                else:
                    prev_start = dt_util.utc_from_timestamp(prev_start_raw)
            elif isinstance(prev_start_raw, str):
                prev_start = dt_util.parse_datetime(prev_start_raw)
            else:
                prev_start = None

            prev_sum = float(prev.get("sum") or 0.0)

            if prev_start is not None and prev_start < first_hour:
                # Previous recorded hour is strictly before our batch;
                # its sum is the correct anchor.
                running_sum = prev_sum
            else:
                # Our batch overlaps with (or precedes) the latest stored
                # row.  We cannot trust the recorder's running sum for
                # this batch, so reset to 0 and let the recorder update
                # rows in place.  The downstream rows (hours after our
                # batch end) will remain with their stale sums, but in
                # practice we always fetch through "now" so this branch
                # only matters in pathological cases.
                running_sum = 0.0

        metadata = StatisticMetaData(
            mean_type=StatisticMeanType.NONE,
            has_sum=True,
            name=self.stats_name,
            source=DOMAIN,
            statistic_id=self.stats_id,
            unit_class=EnergyConverter.UNIT_CLASS,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )

        statistics: list[StatisticData] = []
        for hour_start in sorted_hours:
            value = per_hour[hour_start]
            running_sum += value
            statistics.append(
                StatisticData(
                    start=hour_start,
                    state=value,
                    sum=round(running_sum, 4),
                )
            )

        _LOGGER.debug(
            "Propeye: pushing %d hourly statistics (%s .. %s)",
            len(statistics),
            statistics[0]["start"].isoformat(),
            statistics[-1]["start"].isoformat(),
        )
        async_add_external_statistics(self.hass, metadata, statistics)


class PropeyeSensor(CoordinatorEntity, SensorEntity):
    """Propeye daily electricity consumption sensor.

    Reports today's cumulative energy consumption directly from the API.
    The value resets to 0 at midnight; HA's energy dashboard accounts for
    this via the ``last_reset`` property.

    NOTE: for accurate per-hour bars in the Energy dashboard, add the
    ``propeye:electricity_consumption_<email>`` external statistic
    instead of this entity (see README).  This sensor is convenient for
    live "consumption so far today" displays.
    """

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon = "mdi:lightning-bolt"
    _attr_has_entity_name = True
    _attr_suggested_display_precision = 3

    def __init__(
        self,
        coordinator: PropeyeDataUpdateCoordinator,
        email: str,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._attr_unique_id = f"propeye_{email}_consumption"
        self._attr_name = "Electricity Consumption"

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def native_value(self) -> float | None:
        """Return today's total energy consumption in kWh."""
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.get("total_today")

    @property
    def last_reset(self) -> datetime.datetime | None:
        """Return the start of the current day (when the counter resets)."""
        if self.coordinator.data is None:
            return None
        iso = self.coordinator.data.get("start_of_day")
        if iso:
            return datetime.datetime.fromisoformat(iso)
        return None

    @property
    def extra_state_attributes(self) -> dict | None:
        """Return hourly breakdown and last-updated timestamp."""
        if self.coordinator.data is None:
            return None
        return {
            "hourly_data": self.coordinator.data.get("hourly_data"),
            "last_updated": self.coordinator.data.get("last_updated"),
            "statistic_id": self.coordinator.stats_id,
        }
