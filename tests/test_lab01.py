"""Smoke test: the module 1 lab runs end to end and the pruning lessons still hold."""

import runpy
from pathlib import Path

LAB = Path(__file__).resolve().parents[1] / "labs" / "01_data_lake" / "lab.py"


def test_lab01_runs(capsys):
    runpy.run_path(str(LAB), run_name="__main__")
    out = capsys.readouterr().out
    assert "Scanning Files: 1/12" in out
    assert "order_id < 100000 -> must read  1 of" in out
    assert "rows before=2,000,000" in out
