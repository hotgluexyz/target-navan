"""Real-time Lambda entrypoint for target-navan.

Hotglue's real-time write path (POST .../connectors/navan/{stream}) invokes
``hg-prod-real-time-target-navan``, which calls this handler to run the Singer
target against a single schema+record pair.
"""

from __future__ import annotations

import importlib
from logging import Logger


def real_time_handler(
    config: dict,
    stream_name: str,
    schema_line: str,
    record_line: str,
    logger: Logger,
):
    """Delegate to the Hotglue Singer SDK real-time runner for target-navan."""
    mod = importlib.import_module("hotglue_singer_sdk.target_sdk.lambda")

    if not hasattr(mod, "real_time_handler"):
        raise Exception("This target does not support real time")

    return mod.real_time_handler(
        config,
        stream_name,
        schema_line,
        record_line,
        logger,
        cli_cmd="target-navan",
    )
