"""Base sink for target-navan."""

from __future__ import annotations

from target_hotglue.client import HotglueSink

from target_navan.auth import NAVAN_TOKEN_URL, NavanAuthenticator


class NavanSink(HotglueSink):
    """Shared Navan sink: base URL, OAuth auth, and record passthrough."""

    # Native sync-status records are not unified-schema objects.
    unified_schema = None

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def base_url(self) -> str:
        """Navan REST root; override via ``api_url`` when pointing at a proxy."""
        return self.config.get("api_url", "https://api.navan.com/v1").rstrip("/")

    @property
    def authenticator(self) -> NavanAuthenticator:
        if not hasattr(self, "_authenticator") or self._authenticator is None:
            self._authenticator = NavanAuthenticator(
                self._target,
                {},
                auth_endpoint=NAVAN_TOKEN_URL,
            )
        return self._authenticator

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def validate_input(self, record: dict):
        return record
