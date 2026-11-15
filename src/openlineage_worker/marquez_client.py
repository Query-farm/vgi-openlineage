"""Marquez REST client: endpoint from a ``marquez`` DuckDB secret, retries, errors.

Every Marquez table and function reads one ``marquez`` secret::

    CREATE SECRET marquez (TYPE marquez, BASE_URL 'http://localhost:5000');
    -- behind an auth proxy / a hosted OpenLineage backend that takes a bearer token:
    CREATE SECRET marquez (TYPE marquez, BASE_URL 'https://lineage.example.com', API_KEY '...');

``BASE_URL`` is the API server root (``/api/v1`` is appended; a trailing ``/api/v1`` is
accepted). ``API_KEY`` is sent as ``Authorization: Bearer <key>``. ``PAGE_SIZE``
(default 100) is the ``limit`` used for every paged list. The key is read on every
call and never kept in table-function state, which can travel back to DuckDB over HTTP.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.arguments import SecretLookupEntry
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

SECRET_TYPE = "marquez"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
DEFAULT_PAGE_SIZE = 100
USER_AGENT = "vgi-openlineage-worker/0.1"
DOC_URL = "https://github.com/Query-farm/vgi-openlineage#connect-to-marquez"


def _doc_field(name: str, doc: str, *, redact: bool = False) -> pa.Field[Any]:
    metadata = {"vgi_doc": doc}
    if redact:
        metadata["redact"] = "true"
    return pa.field(name, pa.string(), metadata=metadata)


MARQUEZ_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Connection to a Marquez (OpenLineage) API server: BASE_URL plus an optional bearer API_KEY.",
    schema=pa.schema(
        [
            _doc_field(
                "base_url",
                "Root URL of the Marquez API server, e.g. 'http://localhost:5000'. /api/v1 is appended.",
            ),
            _doc_field(
                "api_key",
                "Bearer token sent as 'Authorization: Bearer <api_key>' (for an auth proxy or hosted "
                "deployment). Leave unset for an open Marquez.",
                redact=True,
            ),
            _doc_field("page_size", "Rows requested per page (Marquez 'limit'); 1 to 1000, default 100."),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)

# Every Marquez scan asks DuckDB for the marquez secret at bind time.
SECRET_REQUIREMENT = [SecretLookupEntry(secret_type=SECRET_TYPE)]


class MarquezError(RuntimeError):
    """Marquez returned an error, or the worker is not configured to reach it."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """API root, page size and bearer token (never logged)."""

    api_url: str
    token: str | None
    page_size: int

    def url(self, path: str, query: dict[str, Any] | None = None) -> str:
        """``<api_url><path>?<query>`` with None-valued parameters dropped."""
        params = {k: v for k, v in (query or {}).items() if v is not None}
        qs = urllib.parse.urlencode(params, doseq=True)
        return f"{self.api_url}{path}" + (f"?{qs}" if qs else "")


def _field(secret: dict[str, Any], name: str) -> str | None:
    value = secret.get(name)
    if value is None:
        return None
    py = value.as_py() if hasattr(value, "as_py") else value
    text = str(py).strip() if py is not None else ""
    return text or None


def endpoint_from_secret(secret: dict[str, Any] | None) -> Endpoint:
    """Resolve the API endpoint from a ``marquez`` secret's fields.

    Args:
        secret: The resolved secret's fields (Arrow scalars or plain values), or None.

    Returns:
        The endpoint.

    Raises:
        MarquezError: No secret, a missing/invalid BASE_URL, or a bad PAGE_SIZE.

    """
    if not secret:
        raise MarquezError(
            "no marquez secret found. Create one first: CREATE SECRET (TYPE marquez, BASE_URL 'http://localhost:5000')"
        )
    base_url = _field(secret, "base_url")
    if not base_url:
        raise MarquezError("the marquez secret needs BASE_URL, e.g. 'http://localhost:5000'")
    if not base_url.startswith(("http://", "https://")):
        raise MarquezError(f"marquez secret base_url must start with http:// or https://, got {base_url!r}")
    root = base_url.rstrip("/").removesuffix("/api/v1").rstrip("/")
    raw_size = _field(secret, "page_size")
    try:
        page_size = int(raw_size) if raw_size else DEFAULT_PAGE_SIZE
    except ValueError as err:
        raise MarquezError(f"marquez secret page_size must be an integer, got {raw_size!r}") from err
    if not 1 <= page_size <= 1000:
        raise MarquezError(f"marquez secret page_size must be between 1 and 1000, got {page_size}")
    return Endpoint(api_url=f"{root}/api/v1", token=_field(secret, "api_key"), page_size=page_size)


def endpoint(secrets: Any) -> Endpoint:
    """Resolve the endpoint from a call's resolved secrets (``params.secrets``).

    Args:
        secrets: The call's ``ResolvedSecrets``.

    Returns:
        The endpoint.

    Raises:
        MarquezError: Several different marquez secrets apply, or none is usable.

    """
    found = list(secrets.of_type(SECRET_TYPE)) if secrets is not None else []
    # Identical secrets (same server, key and page size) are interchangeable.
    distinct = {tuple(_field(s, f) for f in ("base_url", "api_key", "page_size")): s for s in found}
    found = list(distinct.values())
    if len(found) > 1:
        raise MarquezError("more than one marquez secret is defined; keep one per session (DROP SECRET the others)")
    return endpoint_from_secret(found[0] if found else None)


def _retry_delay(err: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = str(err.headers.get("Retry-After") or "") if err.headers else ""
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def get_json(ep: Endpoint, path: str, query: dict[str, Any] | None = None, *, missing_ok: bool = False) -> Any:
    """GET one Marquez API path and decode its JSON body.

    429 and 5xx responses are retried with exponential backoff honouring ``Retry-After``.

    Args:
        ep: The endpoint.
        path: The path below ``/api/v1``, already URL-quoted (e.g. ``/namespaces``).
        query: Query parameters (None values are dropped).
        missing_ok: Return None instead of raising on HTTP 404.

    Returns:
        The decoded body (or None for a tolerated 404).

    Raises:
        MarquezError: HTTP failure after retries or a non-retryable status.

    """
    url = ep.url(path, query)
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if ep.token:
        headers["Authorization"] = f"Bearer {ep.token}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=TIMEOUT_SECONDS) as r:
                return json.load(r)
        except urllib.error.HTTPError as err:
            if err.code == 404 and missing_ok:
                return None
            if (err.code != 429 and err.code < 500) or attempt == MAX_ATTEMPTS:
                detail = err.read(300).decode(errors="replace").strip()
                hint = " (check the marquez secret's API_KEY)" if err.code in (401, 403) else ""
                raise MarquezError(f"Marquez GET {path} returned HTTP {err.code}{hint}: {detail}") from err
            delay = _retry_delay(err, attempt)
            log.info("marquez: HTTP %s, retrying in %.1fs", err.code, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == MAX_ATTEMPTS:
                raise MarquezError(f"cannot reach Marquez at {ep.api_url}: {getattr(err, 'reason', err)}") from err
            time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
    raise MarquezError(f"Marquez GET {path} failed")


def quote(segment: str) -> str:
    """URL-quote one path segment (dataset names contain '/', namespaces contain ':')."""
    return urllib.parse.quote(segment, safe="")
