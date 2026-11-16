"""API -> raw files. Incremental by _lastUpdated, one window per UTC day."""
import json
from datetime import datetime, time, timedelta, timezone

from .control import get_watermark, log_api_calls, set_watermark, utcnow
from .fhir_client import FhirApiError, FhirClient


def parse_ts(value):
    """'2026-11-09', '2026-11-09T10:00:00Z' or a datetime -> aware UTC datetime. Blank -> None."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        dt = datetime.strptime(s, "%Y-%m-%d") if len(s) == 10 else datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _midnight(dt):
    return datetime.combine(dt.date(), time(0), tzinfo=timezone.utc)


def resolve_range(now, watermark=None, start=None, end=None, lookback_days=3, lag_minutes=5):
    """Work out [start, end) for this run.

    end   = explicit end, else now minus a small lag (lets in-flight writes on the server settle)
    start = explicit start, else the watermark, else midnight `lookback_days` days back
    """
    end_ts = parse_ts(end) or (now - timedelta(minutes=lag_minutes))
    end_ts = end_ts.replace(microsecond=0)
    start_ts = parse_ts(start)
    if start_ts is None:
        start_ts = watermark if watermark is not None else _midnight(end_ts) - timedelta(days=lookback_days)
    return start_ts, end_ts


def plan_windows(start, end):
    """Split [start, end) at UTC midnights so each window maps to one raw date folder."""
    windows = []
    cur = start
    while cur < end:
        w_end = min(_midnight(cur) + timedelta(days=1), end)
        windows.append((cur, w_end))
        cur = w_end
    return windows


def fhir_instant(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def raw_file_name(resource_type, window_start, page, fmt):
    return f"{resource_type}_{window_start:%Y%m%d}_p{page:04d}.{fmt}"


def raw_rel_path(raw_folder, resource_type, window_start, run_id, page, fmt):
    # raw/fhir/Patient/2026/11/09/<run_id>/Patient_20261109_p0001.json
    return (f"{raw_folder}/{resource_type}/{window_start:%Y/%m/%d}/{run_id}/"
            f"{raw_file_name(resource_type, window_start, page, fmt)}")


def search_params(cfg, resource, w_start, w_end):
    fcfg = cfg["fhir"]
    params = [
        ("_lastUpdated", f"ge{fhir_instant(w_start)}"),
        ("_lastUpdated", f"lt{fhir_instant(w_end)}"),
        ("_count", str(fcfg["page_size"])),
    ]
    extra = {**(fcfg.get("extra_params") or {}), **(resource.get("params") or {})}
    params += [(k, str(v)) for k, v in extra.items()]
    return params


def ingest_resource(lh, cfg, resource, run_id, start=None, end=None, now=None, client=None, log=print):
    """Pull one resource type for every pending day window and land the pages in raw.

    Each page is written as-is and logged to ctl_api_call_log. The api watermark moves
    forward after every finished window, so a failure only repeats the window it failed in.
    """
    fcfg = cfg["fhir"]
    fmt = fcfg.get("format", "json").lower()
    resource_type = resource["name"]
    client = client or FhirClient(
        fcfg["base_url"], fmt=fmt, timeout=fcfg.get("timeout_seconds", 60),
        max_retries=fcfg.get("max_retries", 5), backoff=fcfg.get("backoff_seconds", 2),
    )
    max_pages = int(fcfg.get("max_pages_per_window") or 0)

    watermark = get_watermark(lh, cfg, "api", resource_type)
    start_ts, end_ts = resolve_range(
        now or utcnow(), watermark, start, end,
        lookback_days=cfg["load"]["initial_lookback_days"],
        lag_minutes=cfg["load"]["safety_lag_minutes"],
    )
    windows = plan_windows(start_ts, end_ts)
    summary = {"resource_type": resource_type, "format": fmt, "from": start_ts, "to": end_ts,
               "windows": len(windows), "pages": 0, "resources": 0, "bytes": 0,
               "retried_pages": 0, "truncated_windows": 0}
    if not windows:
        log(f"{resource_type}: nothing to load ({start_ts} >= {end_ts})")
        return summary

    for w_start, w_end in windows:
        params = search_params(cfg, resource, w_start, w_end)
        params_json = json.dumps(params)
        calls = []
        w_pages = w_resources = 0
        try:
            for page in client.search(resource_type, params, max_pages=max_pages):
                rel = raw_rel_path(lh.raw_folder, resource_type, w_start, run_id, page.number, fmt)
                raw_path = lh.write_file(rel, page.body)
                calls.append({
                    "run_id": run_id, "resource_type": resource_type, "response_format": fmt,
                    "window_start": w_start, "window_end": w_end, "page_number": page.number,
                    "api_url_or_params": page.url, "search_params": params_json,
                    "http_status": page.status, "attempts": page.attempts,
                    "entry_count": page.entry_count, "response_bytes": len(page.body.encode("utf-8")),
                    "next_url": page.next_url, "bundle_id": page.bundle_id,
                    "called_at": page.called_at, "received_at": page.received_at, "saved_at": utcnow(),
                    "raw_path": raw_path,
                    "raw_key": f"{run_id}/{raw_file_name(resource_type, w_start, page.number, fmt)}",
                    "truncated": page.truncated, "status": "OK",
                })
                w_pages += 1
                w_resources += page.entry_count
                summary["retried_pages"] += int(page.attempts > 1)
                summary["bytes"] += calls[-1]["response_bytes"]
                if page.truncated:
                    summary["truncated_windows"] += 1
                    log(f"  !! stopped after {max_pages} pages, more data left in this window")
        except FhirApiError as e:
            calls.append({
                "run_id": run_id, "resource_type": resource_type, "response_format": fmt,
                "window_start": w_start, "window_end": w_end, "page_number": w_pages + 1,
                "api_url_or_params": e.url, "search_params": params_json, "http_status": e.status,
                "called_at": utcnow(), "status": "FAILED", "error": str(e)[:4000],
            })
            log_api_calls(lh, cfg, calls)
            raise

        log_api_calls(lh, cfg, calls)
        watermark = w_end if watermark is None else max(watermark, w_end)
        set_watermark(lh, cfg, "api", resource_type, watermark, run_id)
        summary["pages"] += w_pages
        summary["resources"] += w_resources
        log(f"{resource_type} {w_start:%Y-%m-%d %H:%M} -> {w_end:%Y-%m-%d %H:%M}: "
            f"{w_pages} page(s), {w_resources} resources")

    return summary
