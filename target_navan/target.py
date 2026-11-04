"""target-navan entrypoint."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_navan.sinks import CustomFieldOptionsSink, TransactionsSyncSink


class TargetNavan(TargetHotglue):
    """Write sync-status updates and custom-field options back to Navan."""

    name = "target-navan"
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth client ID for the Navan app",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth client secret for the Navan app",
        ),
        th.Property(
            "api_url",
            th.StringType,
            description="Base URL for the Navan API",
            default="https://api.navan.com/v1",
        ),
    ).to_dict()

    SINK_TYPES = [TransactionsSyncSink, CustomFieldOptionsSink]

    def get_sink_class(self, stream_name: str):
        """Return the sink class registered for ``stream_name``."""
        for sink_class in self.SINK_TYPES:
            if sink_class.name == stream_name:
                return sink_class
        raise ValueError(f"No sink registered for stream '{stream_name}'")


if __name__ == "__main__":
    TargetNavan.cli()
