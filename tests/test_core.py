"""Tests target registration."""

from __future__ import annotations

from target_navan.sinks import CustomFieldOptionsSink, TransactionsSyncSink
from target_navan.target import TargetNavan


def test_target_registers_navan_sinks():
    assert TargetNavan.get_sink_class(TargetNavan, "transactions_sync") is TransactionsSyncSink
    assert TargetNavan.get_sink_class(TargetNavan, "custom_field_options") is CustomFieldOptionsSink
