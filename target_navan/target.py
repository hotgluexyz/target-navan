"""navan target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue

from target_navan.sinks import (
    navanSink,
)


class Targetnavan(Target, TargetHotglue):
    """Sample target for navan."""

    name = "target-navan"

    SINK_TYPES = []
    MAX_PARALLELISM = 1

    def get_sink_class(self, stream_name: str):
        return navanSink


if __name__ == "__main__":
    Targetnavan.cli()
