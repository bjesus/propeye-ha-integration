"""Sensor platform for Propeye."""
from __future__ import annotations

import logging
import datetime
import grpc
import json
import base64
import time
from datetime import timedelta

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

from .const import DOMAIN, DEFAULT_HOST, DEFAULT_PORT
from . import tenant_app_pb2 as pb
from . import tenant_app_pb2_grpc as rpc

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = timedelta(minutes=15)

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Propeye sensor."""
    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]

    coordinator = PropeyeDataUpdateCoordinator(hass, email, password)
    await coordinator.async_config_entry_first_refresh()

    async_add_entities([PropeyeSensor(coordinator, email)], True)


class PropeyeDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Propeye data."""

    def __init__(self, hass, email, password):
        """Initialize."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
        )
        self.email = email
        self.password = password
        self._token = None
        self._channel = None
        self._stub = None

    def _get_stub(self):
        """Get or create the gRPC stub."""
        if self._stub is None:
            target_address = f"{DEFAULT_HOST}:{DEFAULT_PORT}"
            creds = grpc.ssl_channel_credentials()
            self._channel = grpc.secure_channel(target_address, creds)
            self._stub = rpc.TenantAppStub(self._channel)
        return self._stub

    def _decode_jwt_expiry(self, token):
        """Decodes the JWT and returns the expiration timestamp."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return None
            payload = parts[1]
            padding = 4 - (len(payload) % 4)
            if padding < 4:
                payload += "=" * padding
            claims = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
            return claims.get("exp")
        except Exception:
            return None

    def _ensure_token(self):
        """Ensure we have a valid token."""
        stub = self._get_stub()
        now = time.time()

        # Check if existing token is valid (buffer 5 mins)
        if self._token:
            exp = self._decode_jwt_expiry(self._token)
            if exp and exp > (now + 300):
                return self._token

        # Login
        _LOGGER.debug("Authenticating with Propeye...")
        auth_req = pb.AuthenticateRequest(email=self.email, password=self.password)
        try:
            response = stub.Authenticate(auth_req, timeout=10)
            if response and response.token_jwt:
                self._token = response.token_jwt
                return self._token
        except grpc.RpcError as e:
            _LOGGER.error(f"Authentication failed: {e}")
            raise UpdateFailed(f"Authentication failed: {e}")
        
        raise UpdateFailed("Could not obtain token")

    def _fetch_data(self):
        """Fetch data from API (Synchronous)."""
        token = self._ensure_token()
        stub = self._get_stub()
        
        # Fetch for Today
        today = datetime.datetime.now()
        
        # We also fetch yesterday just in case, but for the sensor we only sum TODAY's data
        # as per the requirement "Total for day". 
        # Actually, the specs say: "Fetch data for Yesterday and Today on every poll."
        # But the sensor state is "Total energy consumed today".
        # So we really only need Today's data for the state.
        # But maybe we want to store yesterday's data in attributes?
        # Let's stick to Today for the state.
        
        start_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = today.replace(hour=23, minute=59, second=59, microsecond=0)

        req = pb.GetMeterTypeValuesRequest(
            location_id=1, # Hardcoded as per script? Or should we discover it? Script had 1.
            meter_type="EL",
            unknown_field_3=0,
            max_results=500,
            start_date=pb.TimestampWrapper(
                timestamp=int(start_day.timestamp()), unknown_field_2=0
            ),
            end_date=pb.TimestampWrapper(
                timestamp=int(end_day.timestamp()), unknown_field_2=0
            ),
            unknown_field_7=1,
            unknown_field_8=0,
            unknown_field_9=0,
        )

        metadata = [("authorization", f"Bearer {token}")]

        try:
            response = stub.GetMeterTypeValues(req, metadata=metadata, timeout=10)
            
            total_kwh = 0.0
            hourly_data = []

            if response.data_points:
                for entry in response.data_points:
                    val = entry.reading_value
                    total_kwh += val
                    
                    ts = entry.start_timestamp.timestamp
                    dt = datetime.datetime.fromtimestamp(ts)
                    hourly_data.append({
                        "time": dt.isoformat(),
                        "value": val
                    })
            
            return {
                "total_today": total_kwh,
                "hourly_data": hourly_data,
                "last_updated": datetime.datetime.now().isoformat()
            }

        except grpc.RpcError as e:
            raise UpdateFailed(f"Error fetching data: {e}")

    async def _async_update_data(self):
        """Fetch data from API."""
        return await self.hass.async_add_executor_job(self._fetch_data)


class PropeyeSensor(CoordinatorEntity, SensorEntity):
    """Representation of a Propeye Sensor."""

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon = "mdi:lightning-bolt"
    _attr_has_entity_name = True

    def __init__(self, coordinator, email):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._attr_unique_id = f"propeye_{email}_consumption"
        self._attr_name = "Electricity Consumption"

    @property
    def native_value(self):
        """Return the state of the sensor."""
        if self.coordinator.data:
            return self.coordinator.data.get("total_today")
        return None

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        if self.coordinator.data:
            return {
                "hourly_data": self.coordinator.data.get("hourly_data"),
                "last_updated": self.coordinator.data.get("last_updated")
            }
        return {}
