"""Smoke test: the module 2 notebook runs end to end and its lessons still hold."""

import importlib.util
from pathlib import Path

LAB = Path(__file__).resolve().parents[1] / "labs" / "02_iceberg_basics" / "lab.py"


def load_app():
    spec = importlib.util.spec_from_file_location("lab02", LAB)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


def test_lab02_runs():
    _, defs = load_app().run()
    # Hidden partitioning prunes on order_ts.
    assert defs["planned_files"] == 1
    # The pinned reader and the current reader both see a consistent table.
    assert defs["rows_at_reader_snapshot"] == defs["rows_now"] == 2_000_000
    # Time travel shows the value before the write.
    assert defs["status_before"] != "CANCELLED"
    assert defs["status_now"] == "CANCELLED"
    # Time travel by wall-clock time, by tag, and in SQL (chDB) all reach the same past.
    assert defs["status_at_time"] == defs["status_at_tag"] == defs["status_sql_at_time"] == defs["status_before"]
    # Manifest min/max prunes files on a non-partition column.
    assert defs["exercise1_files"] == {"order_id < 100000": 1, "amount < 1": 12}
    # Partition evolution: both old and new specs still prune.
    assert defs["spec_evolved"]["march"] == 1
    assert defs["spec_evolved"]["jan_1"] == 1
