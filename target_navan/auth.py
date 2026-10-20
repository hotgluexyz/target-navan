"""Navan OAuth authenticator (client_credentials)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

import requests

from target_hotglue.auth import Authenticator


NAVAN_TOKEN_URL = "https://app.navan.com/ta-auth/oauth/token"


class NavanAuthenticator(Authenticator):
    """Obtain Bearer tokens for the Navan Expense API.

    Navan apps use the OAuth2 client_credentials grant (same as tap-navan).
    Tokens are cached on the target config and refreshed ~2 minutes before expiry.
    """

    def __init__(
        self,
        target,
        state: Optional[dict] = None,
        auth_endpoint: str = NAVAN_TOKEN_URL,
    ) -> None:
        super().__init__(target, state or {})
        self._auth_endpoint = auth_endpoint

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        return {"Authorization": f"Bearer {self._config.get('access_token')}"}

    @property
    def oauth_request_body(self) -> dict:
        """Build the client_credentials token request body."""
        return {
            "grant_type": "client_credentials",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        expires_in = self._config.get("expires_in")
        if not access_token or expires_in is None:
            return False
        now = round(datetime.utcnow().timestamp())
        return not ((int(expires_in) - now) < 120)

    def update_access_token(self) -> None:
        """Fetch a new access token and persist it on the target config file."""
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.logger.info(
            "Oauth request - endpoint: %s, grant_type: client_credentials",
            self._auth_endpoint,
        )
        token_response = requests.post(
            self._auth_endpoint,
            data=self.oauth_request_body,
            headers=headers,
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            body: Any
            try:
                body = token_response.json()
            except Exception:
                body = token_response.text
            self.state.update({"auth_error_response": body})
            raise RuntimeError(
                f"Failed OAuth login, response was '{body}'. {ex}"
            ) from ex

        token_json = token_response.json()
        now = round(datetime.utcnow().timestamp())
        self._config["access_token"] = token_json["access_token"]
        self._config["expires_in"] = int(token_json["expires_in"]) + now

        config_path = getattr(self._target, "_config_file_path", None) or getattr(
            self._target, "config_file", None
        )
        if config_path:
            with open(config_path, "w") as outfile:
                json.dump(self._config, outfile, indent=4)
