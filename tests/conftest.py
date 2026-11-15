"""Shared fixtures: a worker client, the fixture paths and the replayed Marquez."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures"
EVENTS = FIXTURES / "events"


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "openlineage_worker.worker"]) as c:
        yield c


@pytest.fixture(scope="session")
def mock_marquez() -> Iterator[str]:
    """Start test/mock_marquez/server.py on a free port and return its base URL."""
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_marquez" / "server.py")],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)
