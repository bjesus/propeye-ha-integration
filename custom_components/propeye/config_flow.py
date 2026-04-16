"""Config flow for Propeye integration."""
from __future__ import annotations

import logging
from typing import Any

import grpc
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD

from .const import DOMAIN, DEFAULT_HOST, DEFAULT_PORT
from . import tenant_app_pb2 as pb
from . import tenant_app_pb2_grpc as rpc

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): str,
        vol.Required(CONF_PASSWORD): str,
    }
)

class PropeyeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Propeye."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]

            try:
                # Validate credentials
                await self.hass.async_add_executor_job(
                    self._validate_credentials, email, password
                )
            except grpc.RpcError:
                errors["base"] = "cannot_connect"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                # Create entry
                return self.async_create_entry(
                    title=email,
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )

    def _validate_credentials(self, email, password):
        """Validate credentials by attempting to login."""
        target_address = f"{DEFAULT_HOST}:{DEFAULT_PORT}"
        creds = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(target_address, creds)
        stub = rpc.TenantAppStub(channel)

        auth_req = pb.AuthenticateRequest(email=email, password=password)
        response = stub.Authenticate(auth_req, timeout=10)
        
        if not response or not response.token_jwt:
            raise Exception("Invalid credentials")
        
        return True
