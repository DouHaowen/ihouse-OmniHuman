"""OpenNews collection video helpers.

This module builds longer collection videos from already completed OpenNews
items. It is intentionally separate from the single-news production pipeline.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

FILE_LOCK = threading.Lock()


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


def _ensure_root(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "collections").mkdir(parents=True, exist_ok=True)
    (root / "jobs").mkdir(parents=True, exist_ok=True)


def _collection_path(root: Path, collection_id: str) -> Path:
    return root / "collections" / f"{collection_id}.json"


def _job_path(root: Path, job_id: str) -> Path:
    return root / "jobs" / f"{job_id}.json"


def _history_relpath(output_dir: Path, value: str) -> str:
    if not value:
        return ""
    path = Path(str(value))
    if path.is_absolute():
        try:
            return path.relative_to(output_dir).as_posix()
        except Exception:
            parts = path.as_posix().split("/")
            if output_dir.name in parts:
                idx = parts.index(output_dir.name)
                return "/".join(parts[idx + 1 :])
            return ""
    return path.as_posix()


def _is_opennews_result(result: dict) -> bool:
    workflow = result.get("workflow_config") or {}
    return bool(
        workflow.get("opennews")
        or workflow.get("opennews_material_only")
        or str(workflow.get("digital_human_engine") or "") == "opennews_material_only"
        or str(result.get("topic") or "").startswith("OpenNews")
    )


def _opennews_title(result: dict) -> str:
    return str(
        result.get("title")
        or ((result.get("script") or {}).get("title") if isinstance(result.get("script"), dict) else "")
        or result.get("topic")
        or "OpenNews 新闻"
    ).strip()


def _variant_video_path(output_dir: Path, result: dict, aspect_ratio: str) -> Path | None:
    variants = result.get("final_video_variants") or {}
    if isinstance(variants, dict):
        item = variants.get(aspect_ratio)
        if isinstance(item, dict):
            rel = _history_relpath(output_dir, str(item.get("final_video_path") or ""))
            if rel and (output_dir / rel).exists():
                return output_dir / rel
    rel = _history_relpath(output_dir, str(result.get("final_video_path") or ""))
    if rel and (output_dir / rel).exists():
        return output_dir / rel
    return None


def _load_collection_state(root: Path) -> dict:
    _ensure_root(root)
    return _read_json(root / "state.json", {"used_history_ids": {}})


def _save_collection_state(root: Path, state: dict) -> None:
    _ensure_root(root)
    _write_json(root / "state.json", state)


def ensure_collection_auto_started_at(root: Path) -> float:
    _ensure_root(root)
    with FILE_LOCK:
        state = _load_collection_state(root)
        started_at = float(state.get("auto_started_at") or 0)
        if started_at <= 0:
            started_at = time.time()
            state["auto_started_at"] = started_at
            _save_collection_state(root, state)
        return started_at


def list_collection_pool(root: Path, output_root: Path, *, limit: int = 80, include_used: bool = False, min_created_at: float = 0) -> list[dict]:
    _ensure_root(root)
    state = _load_collection_state(root)
    used = state.get("used_history_ids") or {}
    items: list[dict] = []
    for result_path in sorted(output_root.glob("*/result.json"), key=lambda p: p.parent.stat().st_mtime, reverse=True):
        output_dir = result_path.parent
        created_at = output_dir.stat().st_mtime
        if min_created_at and created_at < float(min_created_at):
            continue
        result = _read_json(result_path, {})
        if not isinstance(result, dict) or not _is_opennews_result(result):
            continue
        history_id = output_dir.name
        if not include_used and history_id in used:
            continue
        horizontal = _variant_video_path(output_dir, result, "horizontal")
        vertical = _variant_video_path(output_dir, result, "vertical")
        if not horizontal and not vertical:
            continue
        source = ((result.get("workflow_config") or {}).get("source") or {}).get("article") or {}
        items.append(
            {
                "history_id": history_id,
                "title": _opennews_title(result),
                "topic": result.get("topic") or "",
                "created_at": created_at,
                "source_name": source.get("source_name") or "",
                "source_url": source.get("url") or "",
                "published_at": source.get("published_at") or "",
                "horizontal_path": str(horizontal) if horizontal else "",
                "vertical_path": str(vertical) if vertical else "",
                "used_in_collection": used.get(history_id) or "",
            }
        )
        if len(items) >= max(1, min(int(limit or 80), 200)):
            break
    return items


def create_collection_job(root: Path, output_root: Path, *, history_ids: list[str], aspect_ratio: str = "horizontal", title: str = "", username: str = "") -> dict:
    _ensure_root(root)
    pool = {item["history_id"]: item for item in list_collection_pool(root, output_root, limit=300, include_used=False)}
    selected = []
    missing = []
    for history_id in history_ids:
        item = pool.get(str(history_id or "").strip())
        if item:
            selected.append(item)
        else:
            missing.append(str(history_id or "").strip())
    if missing:
        raise ValueError(f"以下视频不可用于合集或已入过合集：{', '.join(missing[:5])}")
    if not selected:
        raise ValueError("请先选择要加入合集的视频")
    if len(selected) > 10:
        raise ValueError("第一阶段一次最多选择 10 条新闻")
    aspect_ratio = "vertical" if str(aspect_ratio).lower() == "vertical" else "horizontal"
    job_id = f"opennews_collection_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job = {
        "job_id": job_id,
        "status": "queued",
        "message": "合集任务已创建",
        "created_at": time.time(),
        "updated_at": time.time(),
        "username": username,
        "aspect_ratio": aspect_ratio,
        "title": title or f"OpenNews 新闻合集 {time.strftime('%Y-%m-%d')}",
        "items": selected,
        "result": {},
        "error": "",
    }
    with FILE_LOCK:
        _write_json(_job_path(root, job_id), job)
    return job


def update_collection_job(root: Path, job_id: str, **updates: Any) -> dict:
    _ensure_root(root)
    with FILE_LOCK:
        job = _read_json(_job_path(root, job_id), {"job_id": job_id})
        job.update(updates)
        job["updated_at"] = time.time()
        _write_json(_job_path(root, job_id), job)
        return job


def load_collection_job(root: Path, job_id: str) -> dict | None:
    _ensure_root(root)
    job = _read_json(_job_path(root, job_id), None)
    return job if isinstance(job, dict) else None


def list_collection_jobs(root: Path, *, limit: int = 20) -> list[dict]:
    _ensure_root(root)
    paths = sorted((root / "jobs").glob("opennews_collection_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    jobs = []
    for path in paths[: max(1, min(int(limit or 20), 100))]:
        job = _read_json(path, {})
        if isinstance(job, dict):
            jobs.append(job)
    return jobs


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raw = (result.stderr or result.stdout or "ffmpeg failed").strip()
        raise RuntimeError(raw[-1800:])


def _safe_name(value: str, fallback: str = "collection") -> str:
    text = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fffぁ-んァ-ヶ一-龯_-]+", "_", value or "").strip("_")
    return text[:40] or fallback


def build_collection_video(root: Path, output_root: Path, job_id: str) -> dict:
    job = load_collection_job(root, job_id)
    if not job:
        raise ValueError("合集任务不存在")
    update_collection_job(root, job_id, status="running", message="正在准备合集视频素材...")
    aspect_ratio = "vertical" if job.get("aspect_ratio") == "vertical" else "horizontal"
    target_w, target_h = (1080, 1920) if aspect_ratio == "vertical" else (1920, 1080)
    collection_dir = root / "collections" / job_id
    work_dir = collection_dir / "work"
    collection_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    clips: list[Path] = []
    for index, item in enumerate(job.get("items") or [], start=1):
        video_path = Path(item.get(f"{aspect_ratio}_path") or item.get("horizontal_path") or item.get("vertical_path") or "")
        if not video_path.exists():
            raise RuntimeError(f"合集素材不存在：{item.get('title')}")
        normalized = work_dir / f"clip_{index:02d}.mp4"
        update_collection_job(root, job_id, message=f"正在统一视频规格：{index}/{len(job.get('items') or [])}")
        _run([
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-vf", f"scale={target_w}:{target_h}:force_original_aspect_ratio=decrease,pad={target_w}:{target_h}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30",
            "-af", "aresample=48000,loudnorm=I=-15:TP=-1.5:LRA=11",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
            str(normalized),
        ])
        clips.append(normalized)
    if not clips:
        raise RuntimeError("没有可用于合集的视频素材")
    concat_list = work_dir / "concat.txt"
    concat_list.write_text("".join(f"file '{clip.as_posix()}'\n" for clip in clips), encoding="utf-8")
    final_name = f"{_safe_name(job.get('title') or job_id)}_{aspect_ratio}.mp4"
    final_path = collection_dir / final_name
    update_collection_job(root, job_id, message="正在拼接合集成片...")
    _run([
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_list),
        "-c", "copy",
        "-movflags", "+faststart",
        str(final_path),
    ])
    collection_items = list(job.get("items") or [])
    titles = [str(item.get("title") or "") for item in collection_items]
    collection = {
        "collection_id": job_id,
        "job_id": job_id,
        "title": job.get("title") or "OpenNews 新闻合集",
        "aspect_ratio": aspect_ratio,
        "created_at": time.time(),
        "video_path": str(final_path),
        "items": collection_items,
        "description": "\n".join(f"{idx + 1}. {title}" for idx, title in enumerate(titles)),
    }
    with FILE_LOCK:
        _write_json(_collection_path(root, job_id), collection)
        state = _load_collection_state(root)
        used = state.get("used_history_ids")
        if not isinstance(used, dict):
            used = {}
        for item in collection_items:
            used[item.get("history_id")] = job_id
        state["used_history_ids"] = used
        _save_collection_state(root, state)
    update_collection_job(root, job_id, status="done", message="合集视频已生成", result=collection, error="")
    return collection
