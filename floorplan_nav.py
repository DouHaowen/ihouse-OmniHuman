"""Admin-only floorplan navigation test workflow."""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import requests


VIDEO_SUFFIXES = {".mp4", ".mov", ".m4v", ".webm"}
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
MAX_ANALYSIS_FRAMES = 12


def _now() -> float:
    return time.time()


def _safe_suffix(filename: str, allowed: set[str], fallback: str) -> str:
    suffix = Path(filename or "").suffix.lower()
    return suffix if suffix in allowed else fallback


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def probe_duration_seconds(path: Path) -> float:
    try:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
        if completed.returncode == 0:
            return max(0.0, float((completed.stdout or "0").strip() or 0))
    except Exception:
        pass
    return 0.0


def extract_video_frames(video_path: Path, frames_dir: Path, duration: float) -> list[dict[str, Any]]:
    frames_dir.mkdir(parents=True, exist_ok=True)
    if duration <= 0:
        timestamps = [0]
    else:
        frame_count = min(MAX_ANALYSIS_FRAMES, max(4, int(duration // 8) + 1))
        if frame_count <= 1:
            timestamps = [duration / 2]
        else:
            timestamps = [round((duration * index) / (frame_count - 1), 2) for index in range(frame_count)]
    frames: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        output = frames_dir / f"frame_{index:02d}_{int(timestamp):04d}s.jpg"
        completed = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{timestamp:.2f}",
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-vf",
                "scale='min(960,iw)':-2",
                "-q:v",
                "3",
                str(output),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if completed.returncode == 0 and output.exists():
            frames.append({"index": index, "time": timestamp, "path": str(output)})
    return frames


def _image_data_url(path: Path) -> str:
    suffix = path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/webp" if suffix == ".webp" else "image/jpeg"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _fallback_segments(duration: float) -> list[dict[str, Any]]:
    if duration <= 0:
        return [{"start": 0, "end": 10, "room": "待确认空间", "confidence": 0.3, "evidence": "未能读取视频时长"}]
    labels = ["玄关/入口", "走廊/过渡", "LDK/客厅", "厨房/水回り", "卧室/阳台"]
    count = min(len(labels), max(2, int(duration // 20) + 1))
    step = duration / count
    return [
        {
            "start": round(index * step, 2),
            "end": round(duration if index == count - 1 else (index + 1) * step, 2),
            "room": labels[index],
            "confidence": 0.2,
            "evidence": "AI 分析不可用时的占位时间段，请管理员手动校正",
        }
        for index in range(count)
    ]


def analyze_floorplan_navigation(
    *,
    video_path: Path,
    floorplan_paths: list[Path],
    notes: str = "",
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    duration = probe_duration_seconds(video_path)
    if log:
        log(f"视频时长约 {duration:.1f}s，正在抽取关键帧...")
    frames = extract_video_frames(video_path, video_path.parent / "analysis_frames", duration)
    if log:
        log(f"已抽取 {len(frames)} 张关键帧，准备分析户型图与空间顺序...")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or not frames:
        return {
            "duration_seconds": duration,
            "frames": frames,
            "segments": _fallback_segments(duration),
            "floorplan_labels": [],
            "summary": "未配置 OpenAI 或关键帧抽取失败，已生成占位时间段。",
            "model": "",
        }

    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                "你是日本房产看房视频分析助手。请根据视频关键帧识别销售走到的空间顺序，"
                "并结合户型图读取可见房间标签。输出必须是 JSON，不要 Markdown。"
                "如果无法确定具体是哪间卧室，请用“卧室/洋室待确认”。"
                f"\n视频总时长：{duration:.1f} 秒。"
                f"\n销售补充信息：{notes or '无'}"
                "\n输出格式："
                "{\"summary\":\"...\",\"segments\":[{\"start\":0,\"end\":8,\"room\":\"玄关\",\"confidence\":0.7,\"evidence\":\"...\"}],"
                "\"floorplan_labels\":[{\"floor\":\"1F\",\"label\":\"LDK\",\"hint\":\"图中可见标签\"}]}"
            ),
        }
    ]
    for item in frames:
        content.append({"type": "text", "text": f"视频关键帧 time={item['time']}s"})
        content.append({"type": "image_url", "image_url": {"url": _image_data_url(Path(item["path"]))}})
    for index, floorplan in enumerate(floorplan_paths):
        content.append({"type": "text", "text": f"户型图 {index + 1}"})
        content.append({"type": "image_url", "image_url": {"url": _image_data_url(floorplan)}})

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": os.getenv("FLOORPLAN_NAV_OPENAI_MODEL", "gpt-4o-mini"),
                "messages": [{"role": "user", "content": content}],
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
                "max_tokens": 1800,
            },
            timeout=180,
        )
        response.raise_for_status()
        payload = response.json()
        parsed = json.loads(payload["choices"][0]["message"]["content"])
        segments = parsed.get("segments") if isinstance(parsed.get("segments"), list) else []
        normalized_segments = []
        for index, segment in enumerate(segments):
            start = max(0.0, float(segment.get("start") or 0))
            end = max(start + 0.5, float(segment.get("end") or min(duration, start + 8)))
            if duration > 0:
                end = min(duration, end)
            normalized_segments.append({
                "index": index,
                "start": round(start, 2),
                "end": round(end, 2),
                "room": str(segment.get("room") or "待确认空间"),
                "confidence": float(segment.get("confidence") or 0),
                "evidence": str(segment.get("evidence") or ""),
            })
        if not normalized_segments:
            normalized_segments = _fallback_segments(duration)
        return {
            "duration_seconds": duration,
            "frames": frames,
            "segments": normalized_segments,
            "floorplan_labels": parsed.get("floorplan_labels") if isinstance(parsed.get("floorplan_labels"), list) else [],
            "summary": str(parsed.get("summary") or ""),
            "usage": payload.get("usage") or {},
            "model": os.getenv("FLOORPLAN_NAV_OPENAI_MODEL", "gpt-4o-mini"),
        }
    except Exception as exc:
        if log:
            log(f"AI 分析失败，已生成占位时间段：{exc}")
        return {
            "duration_seconds": duration,
            "frames": frames,
            "segments": _fallback_segments(duration),
            "floorplan_labels": [],
            "summary": f"AI 分析失败：{exc}",
            "model": os.getenv("FLOORPLAN_NAV_OPENAI_MODEL", "gpt-4o-mini"),
        }


def create_floorplan_nav_job(
    *,
    jobs_root: Path,
    video_file,
    floorplan_files: list,
    notes: str,
    owner: dict,
) -> dict[str, Any]:
    job_id = f"fpnav_{uuid.uuid4().hex[:10]}"
    job_dir = jobs_root / job_id
    upload_dir = job_dir / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    video_suffix = _safe_suffix(getattr(video_file, "filename", ""), VIDEO_SUFFIXES, ".mp4")
    video_path = upload_dir / f"video{video_suffix}"
    with video_path.open("wb") as handle:
        shutil.copyfileobj(video_file.file, handle)

    floorplans = []
    for index, floorplan_file in enumerate(floorplan_files):
        suffix = _safe_suffix(getattr(floorplan_file, "filename", ""), IMAGE_SUFFIXES, ".jpg")
        path = upload_dir / f"floorplan_{index + 1:02d}{suffix}"
        with path.open("wb") as handle:
            shutil.copyfileobj(floorplan_file.file, handle)
        floorplans.append({
            "index": index,
            "name": getattr(floorplan_file, "filename", "") or f"户型图 {index + 1}",
            "path": str(path),
        })

    job = {
        "job_id": job_id,
        "status": "pending",
        "message": "户型图联动测试任务已创建",
        "notes": notes,
        "owner_username": owner.get("username"),
        "owner_display_name": owner.get("display_name"),
        "created_at": _now(),
        "updated_at": _now(),
        "video": {"name": getattr(video_file, "filename", "") or video_path.name, "path": str(video_path)},
        "floorplans": floorplans,
        "analysis": {},
        "points": [],
    }
    _write_json(job_dir / "job.json", job)
    return job


def load_floorplan_nav_job(jobs_root: Path, job_id: str) -> dict[str, Any] | None:
    safe_id = Path(job_id).name
    return _read_json(jobs_root / safe_id / "job.json")


def save_floorplan_nav_job(jobs_root: Path, job: dict[str, Any]) -> None:
    job["updated_at"] = _now()
    _write_json(jobs_root / str(job["job_id"]) / "job.json", job)


def run_floorplan_nav_job_async(jobs_root: Path, job_id: str) -> None:
    def worker() -> None:
        job = load_floorplan_nav_job(jobs_root, job_id)
        if not job:
            return

        def log(message: str) -> None:
            current = load_floorplan_nav_job(jobs_root, job_id) or job
            current["message"] = message
            current.setdefault("logs", []).append({"time": _now(), "message": message})
            save_floorplan_nav_job(jobs_root, current)

        try:
            job["status"] = "running"
            job["message"] = "正在分析实拍视频与户型图..."
            save_floorplan_nav_job(jobs_root, job)
            analysis = analyze_floorplan_navigation(
                video_path=Path(job["video"]["path"]),
                floorplan_paths=[Path(item["path"]) for item in job.get("floorplans", [])],
                notes=str(job.get("notes") or ""),
                log=log,
            )
            job = load_floorplan_nav_job(jobs_root, job_id) or job
            job["analysis"] = analysis
            job["status"] = "done"
            job["message"] = "分析完成，请在户型图上确认点位"
            save_floorplan_nav_job(jobs_root, job)
        except Exception as exc:
            job = load_floorplan_nav_job(jobs_root, job_id) or job
            job["status"] = "error"
            job["message"] = "分析失败"
            job["error"] = str(exc)
            save_floorplan_nav_job(jobs_root, job)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
