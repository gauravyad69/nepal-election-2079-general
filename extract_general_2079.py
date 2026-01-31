#!/usr/bin/env python3
"""Extract Nepal ECN General Election datasets.

This script is designed for working from an HTTP Toolkit HAR capture, but it can
also complete missing content by fetching public JSON/image endpoints directly
from https://result.election.gov.np.

Outputs a clean, hierarchical folder structure under --out.

Example:
    python3 extract_general_2079.py --year 2079 --har HTTPToolkit_2026-01-30_18-39.har --out out --fetch-missing --download-photos
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_ECN_BASE = "https://result.election.gov.np"
# NOTE: This is overridden at runtime via --base-url.
ECN_BASE = DEFAULT_ECN_BASE


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int | None
    ok: bool
    path: Path
    bytes_written: int
    error: str | None = None


@dataclass(frozen=True)
class HarCache:
    """Maps request URLs captured in a HAR to extracted raw response files."""

    out_dir: Path
    by_url: dict[str, Path]
    by_path: dict[str, list[Path]]

    def match(self, url: str) -> Path | None:
        p = self.by_url.get(url)
        if p is not None:
            return p
        try:
            parsed = urllib.parse.urlparse(url)
            url_path = parsed.path
        except Exception:
            return None
        candidates = self.by_path.get(url_path)
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        return None


def _build_har_cache(out_dir: Path, manifest: list[dict[str, Any]] | None) -> HarCache | None:
    if not manifest:
        return None
    by_url: dict[str, Path] = {}
    by_path: dict[str, list[Path]] = {}
    for row in manifest:
        if not isinstance(row, dict):
            continue
        if not row.get("saved"):
            continue
        url = row.get("url")
        rel = row.get("path")
        if not isinstance(url, str) or not isinstance(rel, str):
            continue
        src = out_dir / rel
        if not src.exists() or src.stat().st_size <= 0:
            continue
        by_url.setdefault(url, src)
        try:
            parsed = urllib.parse.urlparse(url)
            by_path.setdefault(parsed.path, []).append(src)
        except Exception:
            pass
    return HarCache(out_dir=out_dir, by_url=by_url, by_path=by_path)


def _safe_relpath_for_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.replace(":", "_")
    path = parsed.path.lstrip("/")
    if not path:
        path = "root"

    # Some endpoints (e.g. Google services) contain extremely long path segments
    # which exceed filesystem filename limits. Collapse long paths into a hash.
    if len(path) > 140:
        p_hash = hashlib.sha256(parsed.path.encode("utf-8")).hexdigest()[:16]
        # keep a short suffix for debugging (last component, truncated)
        tail = parsed.path.rsplit("/", 1)[-1][:40] if parsed.path else "path"
        path = f"p_{p_hash}_{tail}"
    q = parsed.query
    if q:
        h = hashlib.sha256(q.encode("utf-8")).hexdigest()[:12]
        return f"raw/{host}/{path}__q_{h}"
    return f"raw/{host}/{path}"


def _mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_bytes(path: Path, data: bytes) -> int:
    _mkdir(path.parent)
    path.write_bytes(data)
    return len(data)


def _write_text(path: Path, text: str) -> int:
    return _write_bytes(path, text.encode("utf-8"))


def _guess_ext_from_content_type(mime: str | None) -> str:
    if not mime:
        return ""
    mime = mime.split(";", 1)[0].strip().lower()
    return {
        "application/json": ".json",
        "text/json": ".json",
        "text/plain": ".txt",
        "text/html": ".html",
        "text/css": ".css",
        "application/javascript": ".js",
        "application/x-javascript": ".js",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "application/octet-stream": ".bin",
    }.get(mime, "")


def _http_get(url: str, timeout_s: int = 60) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0 Safari/537.36",
            "Accept": "*/*",
        },
        method="GET",
    )

    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        status = getattr(resp, "status", 200)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        data = resp.read()

    if headers.get("content-encoding", "").lower() == "gzip":
        data = gzip.decompress(data)

    return status, headers, data


def fetch_to_file(url: str, out_path: Path, *, har_cache: HarCache | None = None, skip_if_exists: bool = True) -> FetchResult:
    if skip_if_exists and out_path.exists() and out_path.stat().st_size > 0:
        return FetchResult(url=url, status=None, ok=True, path=out_path, bytes_written=0)

    if har_cache is not None:
        src = har_cache.match(url)
        if src is not None:
            try:
                bytes_written = _write_bytes(out_path, src.read_bytes())
                return FetchResult(url=url, status=None, ok=True, path=out_path, bytes_written=bytes_written)
            except Exception as e:
                return FetchResult(url=url, status=None, ok=False, path=out_path, bytes_written=0, error=repr(e))

    try:
        status, headers, data = _http_get(url)
        if status < 200 or status >= 300:
            return FetchResult(url=url, status=status, ok=False, path=out_path, bytes_written=0, error=f"HTTP {status}")

        bytes_written = _write_bytes(out_path, data)
        return FetchResult(url=url, status=status, ok=True, path=out_path, bytes_written=bytes_written)
    except urllib.error.HTTPError as e:
        return FetchResult(url=url, status=getattr(e, "code", None), ok=False, path=out_path, bytes_written=0, error=str(e))
    except Exception as e:
        return FetchResult(url=url, status=None, ok=False, path=out_path, bytes_written=0, error=repr(e))


def _load_json(path: Path) -> Any:
    data = path.read_bytes()
    # Some ECN JSON files are served with a UTF-8 BOM.
    text = data.decode("utf-8-sig")
    return json.loads(text)


def _write_json(path: Path, obj: Any) -> None:
    _mkdir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _parse_har_entries(har_path: Path) -> list[dict[str, Any]]:
    return json.loads(har_path.read_text(encoding="utf-8"))["log"].get("entries", [])


def _har_response_bytes(entry: dict[str, Any]) -> tuple[bytes | None, str | None]:
    content = entry.get("response", {}).get("content", {})
    text = content.get("text")
    if not text:
        return None, content.get("mimeType")

    encoding = content.get("encoding")
    if encoding == "base64":
        try:
            return base64.b64decode(text), content.get("mimeType")
        except Exception:
            return None, content.get("mimeType")

    # HAR text is usually already decoded
    return text.encode("utf-8"), content.get("mimeType")


def extract_raw_from_har(entries: list[dict[str, Any]], out_dir: Path) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for e in entries:
        req = e.get("request", {})
        url = req.get("url", "")
        if not url:
            continue

        resp = e.get("response", {})
        status = resp.get("status")

        data, mime = _har_response_bytes(e)
        rel = _safe_relpath_for_url(url)
        ext = _guess_ext_from_content_type(mime)
        out_path = out_dir / (rel + ext)

        bytes_written = 0
        has_body = False
        if data is not None and len(data) > 0:
            bytes_written = _write_bytes(out_path, data)
            has_body = True

        manifest.append(
            {
                "url": url,
                "method": req.get("method"),
                "status": status,
                "mimeType": mime,
                "saved": bool(has_body),
                "path": str(out_path.relative_to(out_dir)) if has_body else None,
            }
        )

    _write_json(out_dir / "raw_manifest.json", manifest)
    return manifest


def _ecn_url(path: str) -> str:
    return urllib.parse.urljoin(ECN_BASE + "/", path.lstrip("/"))


def _url_status(url: str, timeout_s: int = 30) -> int | None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "*/*",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            # Read a small amount so proxies/CDNs don't return misleading status.
            resp.read(1)
            return getattr(resp, "status", 200)
    except urllib.error.HTTPError as e:
        return getattr(e, "code", None)
    except Exception:
        return None


def _pick_first_existing_url(urls: list[str]) -> str | None:
    for u in urls:
        st = _url_status(u)
        if st is not None and 200 <= st < 300:
            return u
    return None


def _download_many(urls: list[str], out_paths: list[Path], *, max_workers: int, har_cache: HarCache | None = None) -> list[FetchResult]:
    results: list[FetchResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(fetch_to_file, u, p, har_cache=har_cache) for u, p in zip(urls, out_paths, strict=True)]
        for f in as_completed(futs):
            results.append(f.result())
    return results


def _extract_constituency_pairs(constituencies: list[dict[str, Any]]) -> list[tuple[int, int]]:
    # The upstream lookup occasionally contains duplicate distId rows.
    # Collapse to the maximum constituency count per district.
    dist_to_consts: dict[int, int] = {}
    for row in constituencies:
        try:
            dist_id = int(row.get("distId"))
            consts = int(row.get("consts"))
        except Exception:
            continue
        dist_to_consts[dist_id] = max(dist_to_consts.get(dist_id, 0), consts)

    pairs: list[tuple[int, int]] = []
    for dist_id in sorted(dist_to_consts):
        for c in range(1, dist_to_consts[dist_id] + 1):
            pairs.append((dist_id, c))
    return pairs


def _collect_ids_from_candidate_lists(json_paths: Iterable[Path]) -> tuple[set[int], set[int], set[int]]:
    candidate_ids: set[int] = set()
    symbol_ids: set[int] = set()
    party_ids: set[int] = set()

    for p in json_paths:
        try:
            obj = _load_json(p)
        except Exception:
            continue
        if not isinstance(obj, list):
            continue
        for row in obj:
            if not isinstance(row, dict):
                continue
            cid = row.get("CandidateID")
            sid = row.get("SymbolID")
            pid = row.get("PartyID")
            if isinstance(cid, int) and cid > 0:
                candidate_ids.add(cid)
            if isinstance(sid, int) and sid > 0:
                symbol_ids.add(sid)
            if isinstance(pid, int) and pid > 0:
                party_ids.add(pid)

    return candidate_ids, symbol_ids, party_ids


def _normalize_parties(pr_central: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[int, dict[str, Any]] = {}
    for row in pr_central:
        if not isinstance(row, dict):
            continue
        pid = row.get("PartyID")
        if not isinstance(pid, int):
            continue
        item = seen.setdefault(
            pid,
            {
                "PartyID": pid,
                "PoliticalPartyName": row.get("PoliticalPartyName"),
                "SymbolID": row.get("SymbolID"),
                "SymbolName": row.get("SymbolName"),
            },
        )
        # keep first non-null-ish
        for k in ["PoliticalPartyName", "SymbolID", "SymbolName"]:
            if not item.get(k) and row.get(k) is not None:
                item[k] = row.get(k)
    return sorted(seen.values(), key=lambda x: x.get("PartyID") or 0)


def _load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return _load_json(path)
    except Exception:
        return None


def _build_constituency_index(
    *,
    base_dir: Path,
    out_dir: Path,
    constituencies: list[dict[str, Any]],
    districts: list[dict[str, Any]] | None,
    states: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Create a compact, consumption-friendly index of where each file lives."""

    state_name_by_id: dict[int, str] = {}
    if states:
        for s in states:
            if isinstance(s, dict) and isinstance(s.get("id"), int) and isinstance(s.get("name"), str):
                state_name_by_id[s["id"]] = s["name"]

    district_meta_by_id: dict[int, dict[str, Any]] = {}
    if districts:
        for d in districts:
            if not isinstance(d, dict) or not isinstance(d.get("id"), int):
                continue
            district_meta_by_id[d["id"]] = {
                "id": d["id"],
                "name": d.get("name"),
                "stateId": d.get("parentId"),
                "stateName": state_name_by_id.get(d.get("parentId")) if isinstance(d.get("parentId"), int) else None,
            }

    pairs = _extract_constituency_pairs(constituencies)
    const_items: list[dict[str, Any]] = []
    for dist_id, const_id in pairs:
        files = {
            "horFptp": f"datasets/hor/fptp/constituencies/HOR-{dist_id}-{const_id}.json",
            "horPr": f"datasets/hor/pr/constituencies/HOR-{dist_id}-{const_id}.json",
            "paPr": f"datasets/pa/pr/constituencies/HOR-{dist_id}-{const_id}.json",
        }
        item: dict[str, Any] = {
            "districtId": dist_id,
            "constituency": const_id,
            "files": files,
            "geo": {
                # HoR constituency boundaries are provided as one GeoJSON file per district;
                # select the feature where properties.F_CONST == constituency.
                "horDistrictConstituencies": f"geo/hor/constituencies/dist-{dist_id}.json",
                # PA boundaries are provided as one GeoJSON file per HoR constituency.
                "paConstituencies": f"geo/pa/constituencies/const-{dist_id}-{const_id}.json",
            },
        }
        meta = district_meta_by_id.get(dist_id)
        if meta:
            item.update({"districtName": meta.get("name"), "stateId": meta.get("stateId"), "stateName": meta.get("stateName")})
        const_items.append(item)

    return {
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base": str(base_dir.relative_to(out_dir)),
        "lookups": {
            "states": "lookups/states.json",
            "districts": "lookups/districts.json",
            "constituencies": "lookups/constituencies.json",
        },
        "constituencies": const_items,
    }


def _dedupe_hor_fptp_candidates(*, base_dir: Path) -> list[dict[str, Any]]:
    """Build a deduped candidate list across all HoR FPTP constituencies.

    Note: this is a convenience view; the source of truth remains the per-constituency files.
    """

    fptp_dir = base_dir / "datasets" / "hor" / "fptp" / "constituencies"
    media_index = _load_json_if_exists(base_dir / "media" / "index.json") or {}
    photo_map = media_index.get("candidatePhotos", {}) if isinstance(media_index, dict) else {}

    candidates: dict[int, dict[str, Any]] = {}

    for p in sorted(fptp_dir.glob("HOR-*-*.json")):
        try:
            rows = _load_json(p)
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        # parse district/const from filename
        m = re.match(r"HOR-(\d+)-(\d+)\.json$", p.name)
        dist_id = int(m.group(1)) if m else None
        const_id = int(m.group(2)) if m else None

        for row in rows:
            if not isinstance(row, dict):
                continue
            cid = row.get("CandidateID")
            if not isinstance(cid, int) or cid <= 0:
                continue

            entry = candidates.get(cid)
            if entry is None:
                entry = {
                    "CandidateID": cid,
                    "CandidateName": row.get("CandidateName"),
                    "Gender": row.get("Gender"),
                    "Age": row.get("Age"),
                    "DOB": row.get("DOB"),
                    "PoliticalPartyName": row.get("PoliticalPartyName"),
                    "PartyID": row.get("PartyID"),
                    "SymbolID": row.get("SymbolID"),
                    "SymbolName": row.get("SymbolName"),
                    "photo": photo_map.get(str(cid)),
                    "appearances": [],
                }
                # keep a few biographical fields if present
                for k in [
                    "CTZDIST",
                    "FATHER_NAME",
                    "SPOUCE_NAME",
                    "QUALIFICATION",
                    "EXPERIENCE",
                    "OTHERDETAILS",
                    "NAMEOFINST",
                    "ADDRESS",
                ]:
                    if row.get(k) is not None:
                        entry[k] = row.get(k)
                candidates[cid] = entry

            entry["appearances"].append(
                {
                    "districtId": dist_id,
                    "constituency": const_id,
                    "TotalVoteReceived": row.get("TotalVoteReceived"),
                    "Rank": row.get("Rank"),
                    "Remarks": row.get("Remarks"),
                }
            )

    return sorted(candidates.values(), key=lambda x: x.get("CandidateID") or 0)


def main() -> int:
    global ECN_BASE
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--year",
        type=int,
        default=2079,
        help="Election year in Bikram Sambat (e.g. 2079, 2082). Note: older years may not be hosted on result.election.gov.np.",
    )
    ap.add_argument(
        "--base-url",
        type=str,
        default=ECN_BASE,
        help="Base URL for ECN results site (override if using an archive/mirror).",
    )
    ap.add_argument(
        "--election-root",
        type=str,
        default=None,
        help="Override JSON root prefix (default: /JSONFiles/Election<year>). Useful if older elections use a different layout.",
    )
    ap.add_argument("--har", type=Path, required=False, help="Path to HTTP Toolkit .har")
    ap.add_argument("--out", type=Path, default=Path("out"), help="Output directory")
    ap.add_argument("--fetch-missing", action="store_true", help="Fetch key datasets not present in HAR")
    ap.add_argument("--download-photos", action="store_true", help="Download candidate photos and party symbols")
    ap.add_argument("--download-geo", action="store_true", help="Download GeoJSON boundary files for map visualizations")
    ap.add_argument("--max-workers", type=int, default=8, help="Max parallel downloads")
    ap.add_argument("--max-photo-workers", type=int, default=8, help="Max parallel photo downloads")
    args = ap.parse_args()

    ECN_BASE = (args.base_url or ECN_BASE).rstrip("/")

    out_dir: Path = args.out
    _mkdir(out_dir)

    manifest = None
    har_cache: HarCache | None = None
    entries: list[dict[str, Any]] = []
    if args.har:
        entries = _parse_har_entries(args.har)
        print(f"HAR entries: {len(entries)}")
        print("Extracting raw responses from HAR...")
        manifest = extract_raw_from_har(entries, out_dir)
        har_cache = _build_har_cache(out_dir, manifest)
        print(f"Raw manifest written: {out_dir / 'raw_manifest.json'}")

    year = int(args.year)
    base = out_dir / "elections" / str(year) / "general"
    lookup_dir = base / "lookups"
    data_dir = base / "datasets"
    media_dir = base / "media"

    _mkdir(lookup_dir)
    _mkdir(data_dir)
    _mkdir(media_dir)

    election_root = (args.election_root or f"/JSONFiles/Election{year}").rstrip("/")

    # --- Core lookups ---
    # Endpoint layouts have changed across elections; try a small set of known variants.
    lookups: dict[str, list[str]] = {
        "states": [
            f"{election_root}/Local/Lookup/states.json",
            f"{election_root}/Local/lookup/states.json",
        ],
        "districts": [
            f"{election_root}/Local/Lookup/districts.json",
            f"{election_root}/Local/lookup/districts.json",
        ],
        "constituencies": [
            f"{election_root}/HOR/Lookup/constituencies.json",
            f"{election_root}/HoR/Lookup/constituencies.json",
        ],
    }

    for name, paths in lookups.items():
        out_path = lookup_dir / f"{name}.json"
        if out_path.exists() and out_path.stat().st_size > 0:
            continue
        if not args.fetch_missing and not args.har:
            # without HAR and without fetch, we can't proceed
            continue
        url = _pick_first_existing_url([_ecn_url(p) for p in paths])
        if not url:
            print(f"Lookup endpoint not found for '{name}' (year={year}).")
            continue
        print(f"Fetching lookup: {name}")
        res = fetch_to_file(url, out_path, har_cache=har_cache)
        if not res.ok:
            print(f"  failed: {url} ({res.error})")

    if not (lookup_dir / "constituencies.json").exists():
        if args.fetch_missing:
            print("Missing lookups/constituencies.json (no lookup endpoints found for this configuration).")
        else:
            print("Missing lookups/constituencies.json; rerun with --fetch-missing.")
        # Provide a hint for the common "year not hosted" case.
        sample_url = _ecn_url(f"{election_root}/Local/Lookup/states.json")
        sample_status = _url_status(sample_url)
        if sample_status == 404:
            print(f"Note: {sample_url} returned 404; year {year} may not be hosted on {ECN_BASE}.")
        return 2

    constituencies = _load_json(lookup_dir / "constituencies.json")
    pairs = _extract_constituency_pairs(constituencies)

    # --- Download per-constituency datasets ---
    per_const_tasks: list[tuple[str, Path]] = []
    for dist_id, const_id in pairs:
        per_const_tasks.append(
            (
                _ecn_url(f"{election_root}/HOR/FPTP/HOR-{dist_id}-{const_id}.json"),
                data_dir / "hor" / "fptp" / "constituencies" / f"HOR-{dist_id}-{const_id}.json",
            )
        )
        per_const_tasks.append(
            (
                _ecn_url(f"{election_root}/HOR/PR/HOR/HOR-{dist_id}-{const_id}.json"),
                data_dir / "hor" / "pr" / "constituencies" / f"HOR-{dist_id}-{const_id}.json",
            )
        )
        per_const_tasks.append(
            (
                _ecn_url(f"{election_root}/PA/PR/HOR/HOR-{dist_id}-{const_id}.json"),
                data_dir / "pa" / "pr" / "constituencies" / f"HOR-{dist_id}-{const_id}.json",
            )
        )

    # De-duplicate tasks
    seen_task = set()
    uniq_tasks: list[tuple[str, Path]] = []
    for url, pth in per_const_tasks:
        key = (url, str(pth))
        if key in seen_task:
            continue
        seen_task.add(key)
        uniq_tasks.append((url, pth))

    if args.fetch_missing:
        print(f"Fetching constituency datasets: {len(uniq_tasks)} files")
        results = _download_many([u for u, _ in uniq_tasks], [p for _, p in uniq_tasks], max_workers=args.max_workers, har_cache=har_cache)
        _write_json(
            base / "fetch_results_constituencies.json",
            [
                {
                    "url": r.url,
                    "status": r.status,
                    "ok": r.ok,
                    "path": str(r.path.relative_to(out_dir)) if r.path else None,
                    "error": r.error,
                }
                for r in results
            ],
        )
        ok = sum(1 for r in results if r.ok)
        print(f"  downloaded ok: {ok}/{len(results)}")

    # --- District + Province PR aggregates ---
    districts = _load_json(lookup_dir / "districts.json") if (lookup_dir / "districts.json").exists() else []
    states = _load_json(lookup_dir / "states.json") if (lookup_dir / "states.json").exists() else []

    # --- GeoJSON boundaries (for maps) ---
    if args.download_geo:
        geo_dir = base / "geo"
        _mkdir(geo_dir)
        geo_tasks: list[tuple[str, Path]] = []

        geo_tasks.append((_ecn_url("/JSONFiles/JSONMap/geojson/Province.json"), geo_dir / "province.json"))

        # District boundaries are provided per province/state
        for s in states:
            sid = s.get("id") if isinstance(s, dict) else None
            if not isinstance(sid, int):
                continue
            geo_tasks.append(
                (
                    _ecn_url(f"/JSONFiles/JSONMap/geojson/District/STATE_C_{sid}.json"),
                    geo_dir / "districts" / f"state-{sid}.json",
                )
            )

        # HoR constituency boundaries are provided per district
        for d in districts:
            did = d.get("id") if isinstance(d, dict) else None
            if not isinstance(did, int):
                continue
            # districts.json contains placeholder rows (e.g. id 98/99 name 'NA')
            # which do not have corresponding GeoJSON boundary files.
            if isinstance(d, dict) and (d.get("name") in (None, "NA")):
                continue
            geo_tasks.append(
                (
                    _ecn_url(f"/JSONFiles/JSONMap/geojson/Const/dist-{did}.json"),
                    geo_dir / "hor" / "constituencies" / f"dist-{did}.json",
                )
            )

        # PA boundaries are provided per HoR constituency
        for dist_id, const_id in pairs:
            geo_tasks.append(
                (
                    _ecn_url(f"/JSONFiles/JSONMap/geojson/pa/const-{dist_id}-{const_id}.json"),
                    geo_dir / "pa" / "constituencies" / f"const-{dist_id}-{const_id}.json",
                )
            )

        # De-duplicate tasks
        seen_geo = set()
        uniq_geo: list[tuple[str, Path]] = []
        for url, pth in geo_tasks:
            key = (url, str(pth))
            if key in seen_geo:
                continue
            seen_geo.add(key)
            uniq_geo.append((url, pth))

        print(f"Fetching GeoJSON boundaries: {len(uniq_geo)} files")
        results = _download_many([u for u, _ in uniq_geo], [p for _, p in uniq_geo], max_workers=args.max_workers, har_cache=har_cache)
        _write_json(
            base / "fetch_results_geo.json",
            [
                {
                    "url": r.url,
                    "status": r.status,
                    "ok": r.ok,
                    "path": str(r.path.relative_to(out_dir)) if r.path else None,
                    "error": r.error,
                }
                for r in results
            ],
        )
        ok = sum(1 for r in results if r.ok)
        print(f"  downloaded ok: {ok}/{len(results)}")

        # Write a compact geo index for convenient consumption
        geo_index: dict[str, Any] = {
            "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "joinKeys": {
                "stateId": "properties.STATE_C",
                "districtId": "properties.DCODE",
                "horConstituency": "properties.F_CONST",
                "paConstituency": "properties.P_CONST",
            },
            "province": str((geo_dir / "province.json").relative_to(base)) if (geo_dir / "province.json").exists() else None,
            "districtsByState": {},
            "horConstituenciesByDistrict": {},
            "paConstituenciesByHor": {},
        }

        for s in states:
            sid = s.get("id") if isinstance(s, dict) else None
            if not isinstance(sid, int):
                continue
            p = geo_dir / "districts" / f"state-{sid}.json"
            if p.exists():
                geo_index["districtsByState"][str(sid)] = str(p.relative_to(base))

        for d in districts:
            did = d.get("id") if isinstance(d, dict) else None
            if not isinstance(did, int):
                continue
            p = geo_dir / "hor" / "constituencies" / f"dist-{did}.json"
            if p.exists():
                geo_index["horConstituenciesByDistrict"][str(did)] = str(p.relative_to(base))

        for dist_id, const_id in pairs:
            key = f"{dist_id}-{const_id}"
            p = geo_dir / "pa" / "constituencies" / f"const-{dist_id}-{const_id}.json"
            if p.exists():
                geo_index["paConstituenciesByHor"][key] = str(p.relative_to(base))

        _write_json(geo_dir / "index.json", geo_index)

    agg_tasks: list[tuple[str, Path]] = []
    for d in districts:
        did = d.get("id")
        if not isinstance(did, int):
            continue
        agg_tasks.append((_ecn_url(f"{election_root}/HOR/PR/District/{did}.json"), data_dir / "hor" / "pr" / "districts" / f"{did}.json"))
        agg_tasks.append((_ecn_url(f"{election_root}/PA/PR/District/{did}.json"), data_dir / "pa" / "pr" / "districts" / f"{did}.json"))

    for s in states:
        sid = s.get("id")
        if not isinstance(sid, int):
            continue
        agg_tasks.append((_ecn_url(f"{election_root}/HOR/PR/Province/{sid}.json"), data_dir / "hor" / "pr" / "provinces" / f"{sid}.json"))
        agg_tasks.append((_ecn_url(f"{election_root}/PA/PR/Province/{sid}.json"), data_dir / "pa" / "pr" / "provinces" / f"{sid}.json"))

    # Summary/top5 files
    top5_tasks: list[tuple[str, Path]] = []
    top5_tasks.append((_ecn_url(f"{election_root}/Common/HoRPartyTop5.txt"), data_dir / "summary" / "top5" / "HoRPartyTop5.json"))
    top5_tasks.append((_ecn_url(f"{election_root}/Common/PRHoRPartyTop5.txt"), data_dir / "summary" / "top5" / "PRHoRPartyTop5.json"))
    for i in range(1, 8):
        top5_tasks.append((_ecn_url(f"{election_root}/Common/PAPartyTop5-S{i}.txt"), data_dir / "summary" / "top5" / f"PAPartyTop5-S{i}.json"))
        top5_tasks.append((_ecn_url(f"{election_root}/Common/PRPAPartyTop5-S{i}.txt"), data_dir / "summary" / "top5" / f"PRPAPartyTop5-S{i}.json"))

    # Central results endpoints vary by election.
    central_tasks: list[tuple[str, Path]] = []

    central_candidate = _pick_first_existing_url(
        [
            _ecn_url(f"/JSONFiles/ElectionResultCentral{year}.txt"),
            _ecn_url("/JSONFiles/ElectionResultCentral.txt"),
        ]
    )
    if central_candidate:
        central_tasks.append((central_candidate, data_dir / "summary" / "central" / f"ElectionResultCentral{year}.json"))

    pr_central = _pick_first_existing_url(
        [
            _ecn_url("/JSONFiles/ElectionResultCentralPR.txt"),
            _ecn_url(f"/JSONFiles/ElectionResultCentralPR{year}.txt"),
        ]
    )
    pr_elected = _pick_first_existing_url(
        [
            _ecn_url("/JSONFiles/ElectedCandidatePRCentral.txt"),
            _ecn_url(f"/JSONFiles/ElectedCandidatePRCentral{year}.txt"),
        ]
    )
    if pr_central:
        central_tasks.append((pr_central, data_dir / "pr" / "central" / "ElectionResultCentralPR.json"))
    if pr_elected:
        central_tasks.append((pr_elected, data_dir / "pr" / "central" / "ElectedCandidatePRCentral.json"))

    if args.fetch_missing:
        all_tasks = agg_tasks + top5_tasks + central_tasks
        print(f"Fetching aggregate + summary datasets: {len(all_tasks)} files")
        results = _download_many([u for u, _ in all_tasks], [p for _, p in all_tasks], max_workers=args.max_workers, har_cache=har_cache)
        _write_json(
            base / "fetch_results_aggregates.json",
            [
                {
                    "url": r.url,
                    "status": r.status,
                    "ok": r.ok,
                    "path": str(r.path.relative_to(out_dir)) if r.path else None,
                    "error": r.error,
                }
                for r in results
            ],
        )
        ok = sum(1 for r in results if r.ok)
        print(f"  downloaded ok: {ok}/{len(results)}")

    # --- Normalized party list ---
    pr_central_path = data_dir / "pr" / "central" / "ElectionResultCentralPR.json"
    if pr_central_path.exists():
        try:
            pr_central = _load_json(pr_central_path)
            parties = _normalize_parties(pr_central)
            _write_json(base / "normalized" / "parties.json", parties)
        except Exception as e:
            print(f"Failed to normalize parties: {e}")

    # --- Download media ---
    if args.download_photos:
        print("Collecting CandidateID/SymbolID from downloaded datasets...")
        json_paths: list[Path] = []
        for p in (data_dir / "hor" / "fptp" / "constituencies").glob("*.json"):
            json_paths.append(p)
        for p in (data_dir / "hor" / "pr" / "constituencies").glob("*.json"):
            json_paths.append(p)
        for p in (data_dir / "pr" / "central").glob("*.json"):
            json_paths.append(p)

        candidate_ids, symbol_ids, party_ids = _collect_ids_from_candidate_lists(json_paths)
        print(f"  candidate_ids: {len(candidate_ids)}  symbol_ids: {len(symbol_ids)}  party_ids: {len(party_ids)}")

        media_index: dict[str, Any] = {
            "candidatePhotos": {},
            "symbolsHorPa": {},
            "symbolsGeneral": {},
        }

        # Candidate photos
        cand_tasks: list[tuple[str, Path]] = []
        for cid in sorted(candidate_ids):
            url = _ecn_url(f"/Images/Candidate/{cid}.jpg")
            cand_tasks.append((url, media_dir / "candidates" / f"{cid}.jpg"))

        print(f"Downloading candidate photos: {len(cand_tasks)}")
        results = _download_many([u for u, _ in cand_tasks], [p for _, p in cand_tasks], max_workers=args.max_photo_workers, har_cache=har_cache)
        for r in results:
            if r.ok and r.path.exists() and r.path.stat().st_size > 0:
                cid = int(r.path.stem)
                media_index["candidatePhotos"][str(cid)] = str(r.path.relative_to(base))

        # Party symbol photos (HoR/PA)
        sym_tasks: list[tuple[str, Path]] = []
        for sid in sorted(symbol_ids):
            sym_tasks.append((_ecn_url(f"/Images/symbol-hor-pa/{sid}.jpg"), media_dir / "symbols" / "symbol-hor-pa" / f"{sid}.jpg"))
            sym_tasks.append((_ecn_url(f"/Images/Symbols/{sid}.jpg"), media_dir / "symbols" / "Symbols" / f"{sid}.jpg"))

        print(f"Downloading party symbols: {len(sym_tasks)}")
        results = _download_many([u for u, _ in sym_tasks], [p for _, p in sym_tasks], max_workers=args.max_photo_workers, har_cache=har_cache)
        for r in results:
            if not r.ok or not r.path.exists() or r.path.stat().st_size == 0:
                continue
            sid = int(r.path.stem)
            if "symbol-hor-pa" in str(r.path):
                media_index["symbolsHorPa"][str(sid)] = str(r.path.relative_to(base))
            elif "/Symbols/" in str(r.path):
                media_index["symbolsGeneral"][str(sid)] = str(r.path.relative_to(base))

        _write_json(base / "media" / "index.json", media_index)

    # --- Normalized indexes (hierarchical, consumption-friendly) ---
    try:
        normalized_dir = base / "normalized"
        _mkdir(normalized_dir)

        districts_obj = _load_json_if_exists(lookup_dir / "districts.json")
        states_obj = _load_json_if_exists(lookup_dir / "states.json")
        index_obj = _build_constituency_index(
            base_dir=base,
            out_dir=out_dir,
            constituencies=constituencies,
            districts=districts_obj if isinstance(districts_obj, list) else None,
            states=states_obj if isinstance(states_obj, list) else None,
        )
        index_obj["geo"] = {
            "index": "geo/index.json",
            "province": "geo/province.json",
        }
        # counts
        index_obj["counts"] = {
            "horFptpConstituencyFiles": len(list((data_dir / "hor" / "fptp" / "constituencies").glob("*.json"))),
            "horPrConstituencyFiles": len(list((data_dir / "hor" / "pr" / "constituencies").glob("*.json"))),
            "paPrConstituencyFiles": len(list((data_dir / "pa" / "pr" / "constituencies").glob("*.json"))),
            "candidatePhotos": len(list((media_dir / "candidates").glob("*.jpg"))) if (media_dir / "candidates").exists() else 0,
            "symbolHorPa": len(list((media_dir / "symbols" / "symbol-hor-pa").glob("*.jpg"))) if (media_dir / "symbols" / "symbol-hor-pa").exists() else 0,
            "geoFiles": len(list((base / "geo").rglob("*.json"))) if (base / "geo").exists() else 0,
        }
        _write_json(normalized_dir / "index.json", index_obj)

        # Deduped candidate list for HoR FPTP
        candidates = _dedupe_hor_fptp_candidates(base_dir=base)
        _write_json(normalized_dir / "candidates_hor_fptp.json", candidates)
    except Exception as e:
        print(f"Failed to generate normalized outputs: {e}")

    # --- README ---
    readme_path = base / "README.md"
    if not readme_path.exists():
        _mkdir(readme_path.parent)
        readme_path.write_text(
            f"""# ECN — General Election {year} (Extracted)

This folder is generated by `extract_general_2079.py --year {year}`.

## Structure

- `lookups/`: reference tables for `states`, `districts`, and HoR constituency counts.
- `datasets/hor/fptp/constituencies/`: HoR FPTP candidate lists per constituency (`HOR-<districtId>-<constId>.json`).
- `datasets/hor/pr/`: HoR PR aggregates by province/district and per-constituency PR views.
- `datasets/pa/pr/`: Province Assembly PR aggregates.
- `datasets/pr/central/`: Central PR result + PR elected candidates list.
- `datasets/summary/top5/`: “Top 5” party vote summaries from the site.
- `media/`: downloaded candidate photos + party symbols.
- `geo/`: GeoJSON boundaries (provinces, districts, constituencies) if `--download-geo` is used.
- `normalized/parties.json`: deduped party list inferred from PR central results.

## Notes

- Some HAR captures omit response bodies for static files. Use `--fetch-missing` to download missing JSON directly.
- Photos are downloaded by ID referenced in the JSON datasets. Missing/404 photos are skipped.
""",
            encoding="utf-8",
        )

    print(f"Done. Output: {base}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
