"""Base sinks for target-navan."""

from __future__ import annotations

import hashlib
import json

from target_hotglue.client import HotglueBatchSink, HotglueSink
from target_hotglue.common import HGJSONEncoder

from target_navan.auth import NAVAN_TOKEN_URL, NavanAuthenticator


class _NavanSinkMixin:
    """Shared Navan sink: base URL, OAuth auth, and record passthrough."""

    # Native Navan payloads are not unified-schema objects.
    unified_schema = None

    @property
    def name(self) -> str:
        """Singer stream name for this sink."""
        return self.stream_name

    @property
    def base_url(self) -> str:
        """Navan REST root; override via ``api_url`` when pointing at a proxy."""
        return self.config.get("api_url", "https://api.navan.com/v1").rstrip("/")

    @property
    def authenticator(self) -> NavanAuthenticator:
        """Lazy OAuth authenticator shared across requests."""
        if not hasattr(self, "_authenticator") or self._authenticator is None:
            self._authenticator = NavanAuthenticator(
                self._target,
                {},
                auth_endpoint=NAVAN_TOKEN_URL,
            )
        return self._authenticator

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Pass the record through unchanged (native Navan payloads)."""
        return record

    def validate_input(self, record: dict):
        """Accept the record as-is; sinks enforce their own required fields."""
        return record

    def build_record_hash(self, record: dict) -> str:
        """Stable SHA-256 of the record for Hotglue export-state dedupe."""
        return hashlib.sha256(
            json.dumps(record, cls=HGJSONEncoder).encode()
        ).hexdigest()


class NavanSink(_NavanSinkMixin, HotglueSink):
    """Record-oriented Navan sink (e.g. transactions_sync)."""


class NavanBatchSink(_NavanSinkMixin, HotglueBatchSink):
    """Batch-oriented Navan sink (e.g. custom_field_options)."""
