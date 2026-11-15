"""POST the checked-in OpenLineage fixture events to a Marquez server, oldest first.

uv run --no-project python test/marquez/seed.py http://127.0.0.1:5055
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

EVENTS = Path(__file__).parent.parent / "fixtures" / "events"


def load() -> list[dict[str, object]]:
    """Every fixture event, sorted by eventTime."""
    events: list[dict[str, object]] = []
    for path in sorted(EVENTS.iterdir()):
        text = path.read_text()
        if path.suffix == ".ndjson":
            events.extend(json.loads(line) for line in text.splitlines() if line.strip())
        else:
            body = json.loads(text)
            events.extend(body if isinstance(body, list) else [body])
    return sorted(events, key=lambda e: str(e["eventTime"]))


def main() -> None:
    """Seed the server named on the command line."""
    base = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:5055"
    for event in load():
        request = urllib.request.Request(
            f"{base}/api/v1/lineage",
            data=json.dumps(event).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            print(response.status, event.get("eventType", "-"), event.get("eventTime"), file=sys.stderr)


if __name__ == "__main__":
    main()
