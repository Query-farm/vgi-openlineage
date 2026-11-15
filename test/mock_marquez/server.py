#!/usr/bin/env python3
"""Replay recorded Marquez API responses, and serve the event fixtures over HTTP.

The SQL tests (and ``make lint-ai``) run against this server instead of a live
Marquez, so CI needs no Docker. Recordings live in
``test/fixtures/marquez/recordings.json`` as ``{"<path>?<sorted query>": body}``.

Endpoints
---------
``GET /api/v1/...``
    The recorded body for exactly this path and query, else 404.
``GET /files/<name>``
    A file from ``test/fixtures/events`` (for http(s) event sources).
``GET /health``
    ``{"status": "ok"}``.

Switches
--------
* ``--record <marquez url>``: proxy every API request to a real Marquez and save
  the responses (the recordings file is rewritten on each new request).
* ``MOCK_TOKEN`` env var: every API request must send ``Authorization: Bearer <token>``.
* ``MOCK_FLAKY=1`` env var: the first request for each URL gets ``429`` with
  ``Retry-After: 0``, exercising the client's retry path.

Usage::

    python3 test/mock_marquez/server.py                 # prints PORT:<n> once listening
    python3 test/mock_marquez/server.py --port 5056
    python3 test/mock_marquez/server.py --port 5057 --record http://127.0.0.1:5055
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
RECORDINGS = FIXTURES / "marquez" / "recordings.json"
EVENTS = FIXTURES / "events"


def key_of(raw_path: str) -> str:
    """Canonical recording key: path plus sorted query string."""
    parsed = urlparse(raw_path)
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return parsed.path + (f"?{query}" if query else "")


class State:
    """Shared server state."""

    def __init__(self, record: str | None) -> None:
        """Load the recordings (unless recording from scratch)."""
        self.record = record.rstrip("/") if record else None
        self.recordings: dict[str, object] = json.loads(RECORDINGS.read_text()) if RECORDINGS.exists() else {}
        self.seen: set[str] = set()
        self.lock = threading.Lock()


def make_handler(state: State) -> type[BaseHTTPRequestHandler]:
    """Build the request handler bound to ``state``."""
    token = os.environ.get("MOCK_TOKEN")
    flaky = os.environ.get("MOCK_FLAKY") == "1"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            sys.stderr.write("mock-marquez: " + fmt % args + "\n")

        def _send(self, status: int, body: bytes, ctype: str = "application/json", **headers: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            for k, v in headers.items():
                self.send_header(k.replace("_", "-"), v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, value: object, **headers: str) -> None:
            self._send(status, json.dumps(value).encode(), **headers)

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/health":
                self._json(200, {"status": "ok"})
                return
            if path.startswith("/files/"):
                target = (EVENTS / path.removeprefix("/files/")).resolve()
                if EVENTS.resolve() not in target.parents or not target.is_file():
                    self._json(404, {"code": 404, "message": "no such fixture"})
                    return
                self._send(200, target.read_bytes(), "application/x-ndjson")
                return
            if not path.startswith("/api/v1/"):
                self._json(404, {"code": 404, "message": "not found"})
                return
            if token and self.headers.get("Authorization") != f"Bearer {token}":
                self._json(401, {"code": 401, "message": "missing or wrong bearer token"})
                return
            key = key_of(self.path)
            if flaky:
                with state.lock:
                    first = key not in state.seen
                    state.seen.add(key)
                if first:
                    self._json(429, {"code": 429, "message": "slow down"}, Retry_After="0")
                    return
            if state.record:
                self._proxy(key)
                return
            if key in state.recordings:
                self._json(200, state.recordings[key])
            else:
                self._json(404, {"code": 404, "message": f"no recording for {key}"})

        def _proxy(self, key: str) -> None:
            assert state.record is not None
            try:
                with urllib.request.urlopen(state.record + self.path, timeout=30) as r:
                    body = json.load(r)
            except urllib.error.HTTPError as err:
                self._send(err.code, err.read())
                return
            with state.lock:
                # Merge with what is on disk: two recorders (plain and token) share the file.
                on_disk = json.loads(RECORDINGS.read_text()) if RECORDINGS.exists() else {}
                state.recordings = {**on_disk, **state.recordings, key: body}
                RECORDINGS.write_text(json.dumps(state.recordings, indent=1, sort_keys=True) + "\n")
            self._json(200, body)

    return Handler


def main() -> None:
    """Serve until killed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--record", help="Marquez base URL to proxy and record, e.g. http://127.0.0.1:5055")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(State(args.record)))
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
