"""Sensor platform for Propeye."""
from __future__ import annotations

import base64
import datetime
import json
import logging
import time

import grpc

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

from .const import DOMAIN, DEFAULT_HOST, DEFAULT_PORT
from . import tenant_app_pb2 as pb
from . import tenant_app_pb2_grpc as rpc

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = datetime.timedelta(minutes=15)


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

    def _fetch_data(self) -> dict:
        """Fetch hourly energy data (runs in executor thread).

        We query the last 12 hours instead of only today-since-midnight
        so that the API's backfilled values for recently-completed hours
        are picked up.  Only data points that fall within today are used
        for ``total_today`` (the sensor state); earlier points are
        discarded.
        """
        token = self._ensure_token()
        stub = self._get_stub()

        local_tz = dt_util.get_default_time_zone()
        now_local = datetime.datetime.now(tz=local_tz)

        start_of_day = now_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_of_day = now_local.replace(
            hour=23, minute=59, second=59, microsecond=0
        )

        # Fetch from 12 hours ago OR midnight (whichever is earlier) so
        # that all of today is always included and recently-backfilled
        # hours from the past few hours are picked up too.
        fetch_start = min(
            start_of_day,
            now_local - datetime.timedelta(hours=12),
        )

        fetch_start_ts = int(fetch_start.timestamp())
        today_ts = int(start_of_day.timestamp())
        end_ts = int(end_of_day.timestamp())

        req = pb.GetMeterTypeValuesRequest(
            location_id=1,
            meter_type="EL",
            unknown_field_3=0,
            max_results=500,
            start_date=pb.TimestampWrapper(
                timestamp=fetch_start_ts, unknown_field_2=0
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

        total_kwh = 0.0
        hourly_data: list[dict] = []

        if response.data_points:
            for entry in response.data_points:
                ts = entry.start_timestamp.timestamp
                # Only count data points from today towards the total.
                if ts < today_ts:
                    continue

                val = entry.reading_value
                total_kwh += val

                dt_local = datetime.datetime.fromtimestamp(ts, tz=local_tz)
                hourly_data.append(
                    {"time": dt_local.isoformat(), "value": val}
                )

        _LOGGER.debug(
            "Propeye: fetched %d data points, total_today=%.4f kWh",
            len(hourly_data),
            total_kwh,
        )

        return {
            "total_today": round(total_kwh, 4),
            "hourly_data": hourly_data,
            "start_of_day": start_of_day.isoformat(),
            "last_updated": dt_util.now().isoformat(),
        }

    async def _async_update_data(self) -> dict:
        """Fetch data from API (called by the coordinator framework)."""
        return await self.hass.async_add_executor_job(self._fetch_data)


class PropeyeSensor(CoordinatorEntity, SensorEntity):
    """Propeye daily electricity consumption sensor.

    Reports today's cumulative energy consumption directly from the API.
    The value resets to 0 at midnight; HA's energy dashboard accounts for
    this via the ``last_reset`` property.
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
        }
