"""Smoke test: the module 1 notebook runs end to end and its lessons still hold."""

from pathlib import Path

import marimo  # noqa: F401  (fail fast if the notebook runtime is missing)
import importlib.util

LAB = Path(__file__).resolve().parents[1] / "labs" / "01_data_lake" / "lab.py"


def load_app():
    spec = importlib.util.spec_from_file_location("lab01", LAB)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app


def test_lab01_runs():
    _, defs = load_app().run()
    assert "Scanning Files: 1/12" in defs["pruning_lines"]
    assert defs["row_groups_read"]["order_id < 100000"] == 1
    assert defs["row_groups_read"]["amount < 1"] == 17
    assert defs["rows_before"] == defs["rows_after"] == 2_000_000
    assert defs["rows_during"] > defs["rows_before"]
    assert defs["pq_bytes"] < defs["csv_bytes"]
