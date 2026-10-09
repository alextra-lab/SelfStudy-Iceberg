"""Smoke test: the module 3 notebook runs end to end against its Docker services.

Skipped unless RustFS, Polaris and Trino are running:
    docker compose -f labs/03_catalogs/docker-compose.yml up -d
"""

import importlib.util
from pathlib import Path

import pytest
import requests

LAB = Path(__file__).resolve().parents[1] / "labs" / "03_catalogs" / "lab.py"


def services_up():
    try:
        for url in ["http://localhost:9000", "http://localhost:8181/api/catalog/v1/config", "http://localhost:8080/v1/info"]:
            requests.get(url, timeout=3)
        return True
    except requests.ConnectionError:
        return False


@pytest.mark.skipif(not services_up(), reason="module 3 Docker services are not running")
def test_lab03_runs():
    spec = importlib.util.spec_from_file_location("lab03", LAB)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _, defs = module.app.run()
    # Trino's UPDATE is visible to PyIceberg through the shared catalog.
    assert defs["status_in_pyiceberg"] == "CANCELLED"
    # With retries off, the catalog rejects the stale writer.
    assert defs["exercise2_result"].startswith("CommitFailedException")
    # The old metadata file still reads, as the table was after step 2.
    assert defs["exercise3_rows"]["00001"] == 2_000_000
