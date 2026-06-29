"""OpenNews collection video helpers.

This module builds longer collection videos from already completed OpenNews
items. It is intentionally separate from the single-news production pipeline.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

FILE_LOCK = threading.Lock()


def _duplicate_image_blocking_enabled() -> bool:
    return os.getenv("OPENNEWS_COLLECTION_DUPLICATE_IMAGE_BLOCKING", "0").strip().lower() not in {
        "0", "false", "no", "off", ""
    }


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


def _video_duration_seconds(video_path: Path) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return max(0.0, float((result.stdout or "0").strip() or 0))
    except Exception:
        return 0.0


def _segment_has_material_asset(segment: dict, output_dir: Path) -> bool:
    for item in segment.get("material_items") or []:
        if not isinstance(item, dict):
            continue
        raw_path = str(item.get("path") or "").strip()
        if not raw_path:
            continue
        path = Path(raw_path)
        if not path.is_absolute():
            path = output_dir / raw_path
        if path.exists() and path.stat().st_size > 0:
            return True
    for raw_path in segment.get("material_paths") or []:
        path = Path(str(raw_path or ""))
        if not path:
            continue
        if not path.is_absolute():
            path = output_dir / str(raw_path)
        if path.exists() and path.stat().st_size > 0:
            return True
    return False


def _result_has_complete_material_assets(result: dict, output_dir: Path) -> bool:
    material_count = 0
    for segment in result.get("segments") or []:
        if not isinstance(segment, dict) or segment.get("type") != "material":
            continue
        material_count += 1
        if not _segment_has_material_asset(segment, output_dir):
            return False
    return material_count > 0


def _material_item_resolved_path(item: dict, output_dir: Path) -> Path | None:
    raw_path = str((item or {}).get("path") or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = output_dir / raw_path
    return path if path.exists() and path.is_file() else None


def _image_material_fingerprint(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".mp4", ".mov", ".m4v", ".webm"}:
        return ""
    try:
        with Image.open(path) as image:
            image = ImageOps.exif_transpose(image).convert("L")
            image = image.resize((32, 32), Image.Resampling.LANCZOS)
            avg = sum(image.getdata()) / (32 * 32)
            bits = "".join("1" if pixel >= avg else "0" for pixel in image.getdata())
            visual_hash = hex(int(bits, 2))[2:].rjust(256, "0")
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"{visual_hash}:{digest.hexdigest()}"
    except Exception:
        try:
            digest = hashlib.sha256()
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except Exception:
            return ""


def image_material_fingerprint(path: Path) -> str:
    return _image_material_fingerprint(path)


def _image_material_fingerprint_label(fingerprint: str) -> str:
    text = str(fingerprint or "")
    if ":" in text:
        visual_hash, digest = text.split(":", 1)
        return f"{visual_hash[:16]}:{digest[:16]}"
    return text[:32]


def audit_result_image_duplicates(result: dict, output_dir: Path) -> dict:
    segments = result.get("segments") if isinstance(result, dict) else []
    fingerprint_map: dict[str, list[dict]] = {}
    segment_count = 0
    image_count = 0
    if not isinstance(segments, list):
        segments = []
    for segment_index, segment in enumerate(segments, start=1):
        if not isinstance(segment, dict) or segment.get("type") != "material":
            continue
        segment_count += 1
        for item_index, item in enumerate(segment.get("material_items") or [], start=1):
            if not isinstance(item, dict):
                continue
            if str(item.get("kind") or "image").lower() == "video":
                continue
            path = _material_item_resolved_path(item, output_dir)
            if not path:
                continue
            fingerprint = _image_material_fingerprint(path)
            if not fingerprint:
                continue
            image_count += 1
            fingerprint_map.setdefault(fingerprint, []).append(
                {
                    "segment_index": segment_index,
                    "item_index": item_index,
                    "path": str(path),
                    "name": path.name,
                    "source": str(item.get("source") or ""),
                    "title": str(item.get("title") or ""),
                }
            )
    duplicates = []
    for fingerprint, entries in fingerprint_map.items():
        if len(entries) <= 1:
            continue
        duplicates.append(
            {
                "fingerprint": _image_material_fingerprint_label(fingerprint),
                "occurrences": len(entries),
                "entries": entries,
            }
        )
    duplicates.sort(key=lambda item: (-int(item.get("occurrences") or 0), str(item.get("fingerprint") or "")))
    return {
        "ok": not duplicates,
        "segment_count": segment_count,
        "image_count": image_count,
        "duplicate_group_count": len(duplicates),
        "duplicate_groups": duplicates,
    }


def audit_collection_candidate_image_duplicates(root: Path, output_root: Path, history_ids: list[str]) -> dict:
    pool = {item["history_id"]: item for item in list_collection_pool(root, output_root, limit=300, include_used=True)}
    item_audits: list[dict] = []
    collection_fingerprints: dict[str, list[dict]] = {}
    invalid_history_ids: list[str] = []

    for history_id in history_ids:
        key = str(history_id or "").strip()
        if not key:
            continue
        item = pool.get(key)
        if not item:
            invalid_history_ids.append(key)
            continue
        output_dir = output_root / key
        result = _read_json(output_dir / "result.json", {})
        if not isinstance(result, dict):
            invalid_history_ids.append(key)
            continue
        audit = audit_result_image_duplicates(result, output_dir)
        item_audits.append({
            "history_id": key,
            "title": str(item.get("title") or _opennews_title(result)),
            **audit,
        })
        segments = result.get("segments") if isinstance(result, dict) else []
        if not isinstance(segments, list):
            continue
        for segment_index, segment in enumerate(segments, start=1):
            if not isinstance(segment, dict) or segment.get("type") != "material":
                continue
            for item_index, material_item in enumerate(segment.get("material_items") or [], start=1):
                if not isinstance(material_item, dict):
                    continue
                if str(material_item.get("kind") or "image").lower() == "video":
                    continue
                path = _material_item_resolved_path(material_item, output_dir)
                if not path:
                    continue
                fingerprint = _image_material_fingerprint(path)
                if not fingerprint:
                    continue
                collection_fingerprints.setdefault(fingerprint, []).append(
                    {
                        "history_id": key,
                        "title": str(item.get("title") or _opennews_title(result)),
                        "segment_index": segment_index,
                        "item_index": item_index,
                        "path": str(path),
                        "name": path.name,
                        "source": str(material_item.get("source") or ""),
                        "title_material": str(material_item.get("title") or ""),
                    }
                )

    cross_duplicates = []
    for fingerprint, entries in collection_fingerprints.items():
        history_set = {str(entry.get("history_id") or "") for entry in entries if str(entry.get("history_id") or "")}
        if len(history_set) <= 1:
            continue
        cross_duplicates.append(
            {
                "fingerprint": _image_material_fingerprint_label(fingerprint),
                "history_count": len(history_set),
                "occurrences": len(entries),
                "entries": entries,
            }
        )
    cross_duplicates.sort(key=lambda item: (-int(item.get("history_count") or 0), -int(item.get("occurrences") or 0)))
    self_duplicates = [item for item in item_audits if not item.get("ok")]
    return {
        "ok": not invalid_history_ids and not self_duplicates and not cross_duplicates,
        "invalid_history_ids": invalid_history_ids,
        "self_duplicate_items": self_duplicates,
        "cross_duplicate_groups": cross_duplicates,
        "item_audits": item_audits,
    }


def _collection_video_item_is_usable(item: dict, aspect_ratio: str) -> tuple[bool, str, Path | None]:
    video_path = Path(item.get(f"{aspect_ratio}_path") or item.get("horizontal_path") or item.get("vertical_path") or "")
    if not video_path.exists() or not video_path.is_file():
        return False, "成片文件不存在", None
    if video_path.stat().st_size < 256 * 1024:
        return False, "成片文件过小，疑似未完整生成", video_path
    duration = _video_duration_seconds(video_path)
    if duration < 3:
        return False, f"成片时长过短：{duration:.1f}s", video_path
    return True, "", video_path


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
        if not _result_has_complete_material_assets(result, output_dir):
            continue
        source = ((result.get("workflow_config") or {}).get("source") or {}).get("article") or {}
        image_audit = audit_result_image_duplicates(result, output_dir)
        tts_providers = []
        for segment in result.get("segments") or []:
            if isinstance(segment, dict):
                provider = str(segment.get("tts_provider") or segment.get("audio_provider") or "").strip()
                if provider:
                    tts_providers.append(provider)
        primary_tts_provider = str(result.get("tts_provider") or "").strip() or (tts_providers[0] if tts_providers else "")
        items.append(
            {
                "history_id": history_id,
                "title": _opennews_title(result),
                "topic": result.get("topic") or "",
                "created_at": created_at,
                "tts_provider": primary_tts_provider,
                "tts_providers": list(dict.fromkeys(tts_providers)),
                "source_name": source.get("source_name") or "",
                "source_url": source.get("url") or "",
                "published_at": source.get("published_at") or "",
                "horizontal_path": str(horizontal) if horizontal else "",
                "vertical_path": str(vertical) if vertical else "",
                "used_in_collection": used.get(history_id) or "",
                "has_duplicate_images": not image_audit.get("ok", True),
                "duplicate_image_group_count": int(image_audit.get("duplicate_group_count") or 0),
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
    if not selected:
        if missing:
            raise ValueError(f"选择的视频都不可用于合集或已入过合集：{', '.join(missing[:5])}")
        raise ValueError("请先选择要加入合集的视频")
    if len(selected) > 10:
        raise ValueError("第一阶段一次最多选择 10 条新闻")
    duplicate_audit = audit_collection_candidate_image_duplicates(root, output_root, [item.get("history_id") for item in selected])
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
        "skipped_items": [{"history_id": item, "reason": "不可用于合集或已入过合集"} for item in missing if item],
        "image_duplicate_audit": duplicate_audit,
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
    duplicate_audit = audit_collection_candidate_image_duplicates(
        root,
        output_root,
        [str((item or {}).get("history_id") or "") for item in (job.get("items") or [])],
    )
    update_collection_job(root, job_id, image_duplicate_audit=duplicate_audit)
    if not duplicate_audit.get("ok", True) and _duplicate_image_blocking_enabled():
        raise RuntimeError("合集素材审核失败：检测到重复图片素材，已阻止生成合集")
    update_collection_job(root, job_id, status="running", message="正在准备合集视频素材...")
    aspect_ratio = "vertical" if job.get("aspect_ratio") == "vertical" else "horizontal"
    target_w, target_h = (1080, 1920) if aspect_ratio == "vertical" else (1920, 1080)
    collection_dir = root / "collections" / job_id
    work_dir = collection_dir / "work"
    collection_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    clips: list[Path] = []
    raw_intro_video_path = str(job.get("intro_video_path") or "").strip()
    intro_video_path = Path(raw_intro_video_path) if raw_intro_video_path else None
    if intro_video_path and intro_video_path.is_file():
        intro_normalized = work_dir / "clip_00_intro.mp4"
        update_collection_job(root, job_id, message="正在统一数字人开场片头...")
        _run([
            "ffmpeg", "-y",
            "-i", str(intro_video_path),
            "-vf", f"scale={target_w}:{target_h}:force_original_aspect_ratio=decrease,pad={target_w}:{target_h}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30",
            "-af", "aresample=48000,loudnorm=I=-15:TP=-1.5:LRA=11",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-movflags", "+faststart",
            str(intro_normalized),
        ])
        clips.append(intro_normalized)
    skipped_items = list(job.get("skipped_items") or [])
    included_items: list[dict] = []
    for index, item in enumerate(job.get("items") or [], start=1):
        usable, reason, video_path = _collection_video_item_is_usable(item, aspect_ratio)
        if not usable or not video_path:
            skipped_items.append({**item, "skip_reason": reason or "成片不可用"})
            update_collection_job(
                root,
                job_id,
                message=f"已跳过不可用短片：{item.get('title') or item.get('history_id')}｜{reason}",
                skipped_items=skipped_items,
            )
            continue
        normalized = work_dir / f"clip_{index:02d}.mp4"
        update_collection_job(root, job_id, message=f"正在统一视频规格：{index}/{len(job.get('items') or [])}")
        try:
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
        except Exception as exc:
            skipped_items.append({**item, "skip_reason": f"规格统一失败：{exc}"})
            update_collection_job(
                root,
                job_id,
                message=f"已跳过规格异常短片：{item.get('title') or item.get('history_id')}",
                skipped_items=skipped_items,
            )
            continue
        clips.append(normalized)
        included_items.append(item)
    content_clip_count = len(included_items)
    if content_clip_count <= 0:
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
    collection_items = included_items
    titles = [str(item.get("title") or "") for item in collection_items]
    collection = {
        "collection_id": job_id,
        "job_id": job_id,
        "title": job.get("title") or "OpenNews 新闻合集",
        "aspect_ratio": aspect_ratio,
        "created_at": time.time(),
        "video_path": str(final_path),
        "intro_video_path": str(intro_video_path) if intro_video_path and intro_video_path.is_file() else "",
        "intro_script": str(job.get("intro_script") or ""),
        "items": collection_items,
        "skipped_items": skipped_items,
        "included_count": len(collection_items),
        "skipped_count": len(skipped_items),
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
