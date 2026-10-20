"""target-navan entrypoint."""

from __future__ import annotations

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_navan.sinks import TransactionsSyncSink


class TargetNavan(TargetHotglue):
    """Write sync-status updates back to Navan expense transactions."""

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

    SINK_TYPES = [TransactionsSyncSink]

    def get_sink_class(self, stream_name: str):
        for sink_class in self.SINK_TYPES:
            if sink_class.name == stream_name:
                return sink_class
        raise ValueError(f"No sink registered for stream '{stream_name}'")


if __name__ == "__main__":
    TargetNavan.cli()
