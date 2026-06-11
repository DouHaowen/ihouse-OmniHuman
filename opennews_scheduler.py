"""OpenNews automatic hot-topic polling and candidate pool."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from opennews_trends import search_english_trends


DEFAULT_CONFIG = {
    "enabled": False,
    "interval_minutes": 60,
    "categories": ["all"],
    "time_range": "6h",
    "limit": 20,
    "last_run_at": 0,
    "next_run_at": 0,
    "last_run_message": "自动抓取尚未启动。",
    "last_run_error": "",
}

VALID_INTERVALS = {5, 30, 60, 180, 360}
VALID_TIME_RANGES = {"1h", "6h", "24h"}
VALID_CATEGORIES = {"all", "military", "politics", "technology", "finance", "ai", "society"}
VALID_STATUSES = {"pending", "ignored", "drafted", "produced"}
_FILE_LOCK = threading.Lock()
_RUN_LOCK = threading.Lock()
_SCHEDULER_STARTED = False
RETENTION_SECONDS = max(3600, int(os.getenv("OPENNEWS_AUTO_RETENTION_SECONDS", str(2 * 24 * 60 * 60)) or str(2 * 24 * 60 * 60)))


def _config_path(root: Path) -> Path:
    return root / "config.json"


def _candidates_path(root: Path) -> Path:
    return root / "candidates.json"


def _runs_dir(root: Path) -> Path:
    return root / "runs"


def _ensure_root(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _runs_dir(root).mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _parse_news_timestamp(value: Any) -> float:
    if value in (None, "", 0, "0"):
        return 0.0
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000
        return ts if ts > 0 else 0.0
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return _parse_news_timestamp(float(text))
    except Exception:
        pass
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _candidate_reference_timestamp(item: dict) -> float:
    for key in ("published_ts", "published_at", "news_time", "date", "auto_fetched_at", "auto_created_at"):
        ts = _parse_news_timestamp(item.get(key))
        if ts:
            return ts
    return 0.0


def _is_recent_candidate(item: dict, *, now: float | None = None) -> bool:
    now = now or time.time()
    ts = _candidate_reference_timestamp(item)
    if not ts:
        return True
    if ts > now + 6 * 60 * 60:
        return True
    return ts >= now - RETENTION_SECONDS


def _prune_old_candidates_locked(root: Path, *, now: float | None = None) -> dict:
    now = now or time.time()
    items = _read_json(_candidates_path(root), [])
    if not isinstance(items, list):
        return {"removed_items": 0}
    fresh = [item for item in items if isinstance(item, dict) and _is_recent_candidate(item, now=now)]
    if len(fresh) != len(items):
        _write_json(_candidates_path(root), fresh)
    return {"removed_items": max(0, len(items) - len(fresh))}


def cleanup_old_auto_candidates(root: Path) -> dict:
    _ensure_root(root)
    with _FILE_LOCK:
        return _prune_old_candidates_locked(root)


def _candidate_key(candidate: dict) -> str:
    basis = "|".join([
        str(candidate.get("url") or "").strip().lower(),
        str(candidate.get("title") or "").strip().lower(),
        str(candidate.get("published_at") or ""),
    ])
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def _normalize_categories(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return ["all"]
    categories = []
    for item in value:
        category = str(item or "").strip().lower()
        if category in VALID_CATEGORIES and category not in categories:
            categories.append(category)
    return categories or ["all"]


def load_auto_config(root: Path) -> dict:
    _ensure_root(root)
    with _FILE_LOCK:
        raw = _read_json(_config_path(root), {})
    config = dict(DEFAULT_CONFIG)
    if isinstance(raw, dict):
        config.update(raw)
    config["enabled"] = bool(config.get("enabled"))
    try:
        interval = int(config.get("interval_minutes") or 60)
    except Exception:
        interval = 60
    config["interval_minutes"] = interval if interval in VALID_INTERVALS else 60
    config["categories"] = _normalize_categories(config.get("categories"))
    time_range = str(config.get("time_range") or "6h").strip().lower()
    config["time_range"] = time_range if time_range in VALID_TIME_RANGES else "6h"
    try:
        config["limit"] = max(5, min(int(config.get("limit") or 20), 50))
    except Exception:
        config["limit"] = 20
    return config


def save_auto_config(root: Path, config: dict) -> dict:
    _ensure_root(root)
    clean = load_auto_config(root)
    clean.update(config or {})
    clean["enabled"] = bool(clean.get("enabled"))
    try:
        interval = int(clean.get("interval_minutes") or 60)
    except Exception:
        interval = 60
    clean["interval_minutes"] = interval if interval in VALID_INTERVALS else 60
    clean["categories"] = _normalize_categories(clean.get("categories"))
    time_range = str(clean.get("time_range") or "6h").strip().lower()
    clean["time_range"] = time_range if time_range in VALID_TIME_RANGES else "6h"
    try:
        clean["limit"] = max(5, min(int(clean.get("limit") or 20), 50))
    except Exception:
        clean["limit"] = 20
    now = time.time()
    if clean.get("enabled") and not float(clean.get("next_run_at") or 0):
        clean["next_run_at"] = now + clean["interval_minutes"] * 60
    if not clean.get("enabled"):
        clean["next_run_at"] = 0
    with _FILE_LOCK:
        _write_json(_config_path(root), clean)
    return clean


def list_auto_candidates(root: Path, status: str = "pending", limit: int = 120) -> list[dict]:
    _ensure_root(root)
    status = str(status or "pending").strip().lower()
    with _FILE_LOCK:
        _prune_old_candidates_locked(root)
        items = _read_json(_candidates_path(root), [])
    if not isinstance(items, list):
        items = []
    if status and status != "all":
        items = [item for item in items if str(item.get("status") or "pending") == status]
    return sorted(items, key=lambda item: float(item.get("auto_fetched_at") or item.get("published_ts") or 0), reverse=True)[: max(1, min(limit, 300))]


def update_auto_candidate_status(root: Path, candidate_id: str, status: str, *, username: str = "") -> dict | None:
    _ensure_root(root)
    status = str(status or "").strip().lower()
    if status not in VALID_STATUSES:
        raise ValueError("不支持的候选状态")
    with _FILE_LOCK:
        items = _read_json(_candidates_path(root), [])
        if not isinstance(items, list):
            items = []
        found = None
        for item in items:
            if str(item.get("id") or "") == candidate_id:
                item["status"] = status
                item["status_updated_at"] = time.time()
                item["status_updated_by"] = username
                found = item
                break
        if found:
            _write_json(_candidates_path(root), items)
        return found


def _merge_candidates(root: Path, candidates: list[dict], *, category: str, run_id: str) -> dict:
    now = time.time()
    with _FILE_LOCK:
        _prune_old_candidates_locked(root, now=now)
        existing = _read_json(_candidates_path(root), [])
        if not isinstance(existing, list):
            existing = []
        by_key: dict[str, dict] = {}
        for item in existing:
            key = str(item.get("auto_key") or _candidate_key(item))
            item["auto_key"] = key
            by_key[key] = item
        added = 0
        updated = 0
        for candidate in candidates:
            if not candidate.get("title") or not candidate.get("url"):
                continue
            item = dict(candidate)
            key = _candidate_key(item)
            item["auto_key"] = key
            item["auto_category"] = category
            item["auto_run_id"] = run_id
            item["auto_fetched_at"] = now
            item.setdefault("status", "pending")
            existing_item = by_key.get(key)
            if existing_item:
                status = existing_item.get("status") or "pending"
                created_at = existing_item.get("auto_created_at") or existing_item.get("auto_fetched_at") or now
                existing_item.update({k: v for k, v in item.items() if v not in (None, "", [])})
                existing_item["status"] = status
                existing_item["auto_created_at"] = created_at
                existing_item["auto_fetched_at"] = now
                updated += 1
            else:
                item["auto_created_at"] = now
                item["status"] = "pending"
                by_key[key] = item
                added += 1
        merged = sorted(by_key.values(), key=lambda value: float(value.get("auto_fetched_at") or value.get("published_ts") or 0), reverse=True)[:500]
        _write_json(_candidates_path(root), merged)
    return {"added": added, "updated": updated, "total": len(merged)}


def run_auto_fetch_once(root: Path, *, triggered_by: str = "manual") -> dict:
    _ensure_root(root)
    if not _RUN_LOCK.acquire(blocking=False):
        return {"ok": False, "running": True, "message": "OpenNews 自动抓取正在执行中，请稍后刷新。"}
    run_id = time.strftime("%Y%m%d_%H%M%S")
    started_at = time.time()
    config = load_auto_config(root)
    categories = _normalize_categories(config.get("categories"))
    time_range = str(config.get("time_range") or "6h")
    limit = max(5, min(int(config.get("limit") or 20), 50))
    run_payload = {
        "run_id": run_id,
        "triggered_by": triggered_by,
        "started_at": started_at,
        "config": config,
        "category_results": [],
        "errors": [],
    }
    total_added = 0
    total_updated = 0
    try:
        for category in categories:
            try:
                result = search_english_trends(category=category, time_range=time_range, keyword="", limit=limit)
                candidates = result.get("candidates") or []
                merge = _merge_candidates(root, candidates, category=category, run_id=run_id)
                total_added += int(merge.get("added") or 0)
                total_updated += int(merge.get("updated") or 0)
                run_payload["category_results"].append({
                    "category": category,
                    "count": len(candidates),
                    "added": merge.get("added", 0),
                    "updated": merge.get("updated", 0),
                    "source_errors": result.get("source_errors", []),
                })
            except Exception as exc:
                run_payload["errors"].append({"category": category, "error": str(exc)})
        finished_at = time.time()
        message = f"抓取完成：新增 {total_added} 条，更新 {total_updated} 条。"
        if run_payload["errors"] and not run_payload["category_results"]:
            message = f"抓取失败：{run_payload['errors'][0].get('error')}"
        config["last_run_at"] = finished_at
        config["next_run_at"] = finished_at + int(config.get("interval_minutes") or 60) * 60 if config.get("enabled") else 0
        config["last_run_message"] = message
        config["last_run_error"] = "；".join(item.get("error", "") for item in run_payload["errors"][:3])
        save_auto_config(root, config)
        run_payload.update({
            "finished_at": finished_at,
            "added": total_added,
            "updated": total_updated,
            "message": message,
        })
        with _FILE_LOCK:
            _write_json(_runs_dir(root) / f"{run_id}.json", run_payload)
        return {"ok": not bool(run_payload["errors"] and not run_payload["category_results"]), **run_payload}
    finally:
        _RUN_LOCK.release()


def start_opennews_auto_scheduler(root: Path, *, poll_seconds: int = 30) -> None:
    global _SCHEDULER_STARTED
    if _SCHEDULER_STARTED:
        return
    _SCHEDULER_STARTED = True
    _ensure_root(root)

    def loop() -> None:
        while True:
            try:
                config = load_auto_config(root)
                if config.get("enabled"):
                    next_run_at = float(config.get("next_run_at") or 0)
                    if not next_run_at or time.time() >= next_run_at:
                        run_auto_fetch_once(root, triggered_by="scheduler")
            except Exception:
                pass
            time.sleep(max(10, int(poll_seconds)))

    thread = threading.Thread(target=loop, name="opennews-auto-scheduler", daemon=True)
    thread.start()
