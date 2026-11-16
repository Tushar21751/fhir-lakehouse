import json
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

MIME = {"json": "application/fhir+json", "xml": "application/fhir+xml"}
RETRY_STATUS = {429, 500, 502, 503, 504}
FHIR_NS = "{http://hl7.org/fhir}"


class FhirApiError(Exception):
    def __init__(self, message, url=None, status=None):
        super().__init__(message)
        self.url = url
        self.status = status


@dataclass
class Page:
    number: int
    url: str            # what was actually requested (first page has the search params)
    status: int
    body: str
    called_at: datetime
    received_at: datetime
    attempts: int
    entry_count: int
    next_url: str
    bundle_id: str
    truncated: bool = False


def _utcnow():
    return datetime.now(timezone.utc)


def summarize_bundle(body, fmt):
    """Return (entry_count, next_url, bundle_id) without building any big objects."""
    if fmt == "xml":
        root = ET.fromstring(body.encode("utf-8"))
        entries = root.findall(f"{FHIR_NS}entry")
        next_url = None
        for link in root.findall(f"{FHIR_NS}link"):
            rel = link.find(f"{FHIR_NS}relation")
            if rel is not None and rel.get("value") == "next":
                url = link.find(f"{FHIR_NS}url")
                next_url = url.get("value") if url is not None else None
        id_el = root.find(f"{FHIR_NS}id")
        return len(entries), next_url, id_el.get("value") if id_el is not None else None

    bundle = json.loads(body)
    next_url = None
    for link in bundle.get("link") or []:
        if link.get("relation") == "next":
            next_url = link.get("url")
    return len(bundle.get("entry") or []), next_url, bundle.get("id")


class FhirClient:
    def __init__(self, base_url, fmt="json", timeout=60, max_retries=5, backoff=2.0, session=None):
        fmt = (fmt or "json").lower()
        if fmt not in MIME:
            raise ValueError(f"format must be json or xml, got {fmt!r}")
        self.base_url = base_url.rstrip("/")
        self.fmt = fmt
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": MIME[fmt], "User-Agent": "fhir-lakehouse/0.3"})

    def _wait_time(self, attempt, resp=None):
        if resp is not None and resp.headers.get("Retry-After"):
            value = resp.headers["Retry-After"]
            try:
                return max(0.0, float(value))
            except ValueError:
                try:
                    return max(0.0, (parsedate_to_datetime(value) - _utcnow()).total_seconds())
                except (TypeError, ValueError):
                    pass
        return self.backoff * (2 ** (attempt - 1))

    def get(self, url, params=None):
        """GET with retries on throttling / 5xx / connection errors."""
        attempt = 0
        while True:
            attempt += 1
            called_at = _utcnow()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt > self.max_retries:
                    raise FhirApiError(f"GET {url} failed after {attempt} attempts: {e}", url=url) from e
                time.sleep(self._wait_time(attempt))
                continue

            if resp.status_code in RETRY_STATUS and attempt <= self.max_retries:
                time.sleep(self._wait_time(attempt, resp))
                continue
            if resp.status_code >= 400:
                raise FhirApiError(
                    f"GET {resp.url} returned {resp.status_code}: {resp.text[:500]}",
                    url=resp.url, status=resp.status_code,
                )
            resp.encoding = resp.encoding or "utf-8"
            return resp, called_at, attempt

    def search(self, resource_type, params, max_pages=0):
        """Yield every page of a search, following the Bundle 'next' links.

        params can be a dict or a list of tuples (needed for repeated keys like
        _lastUpdated=ge...&_lastUpdated=lt...). max_pages=0 means no limit.
        """
        params = list(params.items()) if isinstance(params, dict) else list(params)
        params.append(("_format", self.fmt))
        url = f"{self.base_url}/{resource_type}"
        seen = set()
        number = 0

        while url:
            number += 1
            # page 2+ come from the server's next link which already has everything in it
            resp, called_at, attempts = self.get(url, params=params if number == 1 else None)
            received_at = _utcnow()
            body = resp.text
            entry_count, next_url, bundle_id = summarize_bundle(body, self.fmt)

            stop_here = bool(max_pages) and number >= max_pages
            yield Page(
                number=number, url=resp.url, status=resp.status_code, body=body,
                called_at=called_at, received_at=received_at, attempts=attempts,
                entry_count=entry_count, next_url=next_url, bundle_id=bundle_id,
                truncated=stop_here and bool(next_url),
            )

            if stop_here or not next_url or next_url in seen:
                break
            seen.add(next_url)
            url = next_url
