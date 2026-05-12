"""
Local InfiniteTalk HTTP worker.

This service runs on the 5090 machine. It accepts image/audio uploads,
persists jobs to disk, and processes one generation at a time so GPU memory
does not get oversubscribed.
"""

from __future__ import annotations

import json
import math
import os
import queue
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, JSONResponse


BASE_DIR = Path(os.getenv("INFINITETALK_AVATAR_BASE_DIR", "/home/saita/InfiniteTalk")).resolve()
JOBS_DIR = Path(os.getenv("INFINITETALK_AVATAR_JOBS_DIR", str(BASE_DIR / "api_jobs"))).resolve()
CONDA_SH = os.getenv("INFINITETALK_AVATAR_CONDA_SH", "/home/saita/miniforge3/etc/profile.d/conda.sh")
CONDA_ENV = os.getenv("INFINITETALK_AVATAR_CONDA_ENV", "infinitetalk5090")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("INFINITETALK_AVATAR_TIMEOUT_SECONDS", "21600"))
DEFAULT_RETRIES = int(os.getenv("INFINITETALK_AVATAR_RETRIES", "1"))
DEFAULT_FRAME_NUM = int(os.getenv("INFINITETALK_AVATAR_FRAME_NUM", "81"))
DEFAULT_SAMPLE_STEPS = int(os.getenv("INFINITETALK_AVATAR_SAMPLE_STEPS", "8"))
DEFAULT_MOTION_FRAME = int(os.getenv("INFINITETALK_AVATAR_MOTION_FRAME", "9"))
DEFAULT_PERSISTENT_DIT = int(os.getenv("INFINITETALK_AVATAR_PERSISTENT_DIT", "0"))

app = FastAPI(title="iHouse InfiniteTalk Worker")
job_queue: "queue.Queue[str]" = queue.Queue()
queue_lock = threading.Lock()
worker_started = False


def _now() -> float:
    return time.time()


def _job_dir(job_id: str) -> Path:
    safe_id = Path(job_id).name
    return JOBS_DIR / safe_id


def _job_path(job_id: str) -> Path:
    return _job_dir(job_id) / "job.json"


def _read_job(job_id: str) -> dict[str, Any] | None:
    path = _job_path(job_id)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_job(job: dict[str, Any]) -> None:
    job_id = str(job["job_id"])
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    job["updated_at"] = _now()
    tmp_path = _job_path(job_id).with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(_job_path(job_id))


def _status_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    if not JOBS_DIR.exists():
        return counts
    for path in JOBS_DIR.glob("*/job.json"):
        try:
            status = json.loads(path.read_text(encoding="utf-8")).get("status", "unknown")
        except Exception:
            status = "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _enqueue(job_id: str) -> None:
    with queue_lock:
        queued_ids = set(job_queue.queue)
        if job_id not in queued_ids:
            job_queue.put(job_id)


def _save_upload(upload: UploadFile, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        shutil.copyfileobj(upload.file, handle)


def _probe_media_duration_seconds(path: Path) -> float | None:
    try:
        result = subprocess.run(
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
            check=True,
            timeout=20,
        )
    except Exception:
        return None
    raw = (result.stdout or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _normalize_audio_for_infinitetalk(source_path: Path) -> Path:
    target_path = source_path.with_name("audio_norm.wav")
    if source_path.suffix.lower() == ".wav" and source_path.name == target_path.name:
        return source_path
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(source_path),
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                str(target_path),
            ],
            capture_output=True,
            check=True,
            timeout=120,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode("utf-8", errors="ignore")[:500]
        raise RuntimeError(f"音频转 WAV 失败：{stderr}") from exc
    except Exception as exc:
        raise RuntimeError(f"音频预处理失败：{exc}") from exc
    return target_path


def _align_frame_num(frame_count: int) -> int:
    frame_count = max(17, int(frame_count or 17))
    remainder = (frame_count - 1) % 4
    if remainder:
        frame_count += 4 - remainder
    return frame_count


def _resolve_frame_num(audio_path: Path, configured: int) -> int:
    requested = _align_frame_num(configured or DEFAULT_FRAME_NUM)
    duration_seconds = _probe_media_duration_seconds(audio_path)
    if not duration_seconds or duration_seconds <= 0:
        return requested
    inferred = _align_frame_num(int(math.floor(duration_seconds * 16)) + 1)
    return max(17, min(requested, inferred))


def _build_input_json(job: dict[str, Any], input_json_path: Path) -> None:
    payload = {
        "prompt": job.get("prompt")
        or "A professional iHouse news anchor speaks naturally to camera with accurate lip sync, stable face, calm body, and a clear iHouse logo table sign.",
        "cond_video": job["image_path"],
        "cond_audio": {"person1": job["audio_path"]},
    }
    input_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_generation(job: dict[str, Any]) -> None:
    job_id = str(job["job_id"])
    job_dir = _job_dir(job_id)
    result_dir = job_dir / "result"
    result_dir.mkdir(parents=True, exist_ok=True)
    input_json = job_dir / "input.json"
    _build_input_json(job, input_json)

    settings = job.get("settings") or {}
    size = str(settings.get("size") or "infinitetalk-480")
    frame_num = _resolve_frame_num(Path(job["audio_path"]), int(settings.get("frame_num") or DEFAULT_FRAME_NUM))
    sample_steps = max(1, int(settings.get("sample_steps") or DEFAULT_SAMPLE_STEPS))
    motion_frame = max(1, int(settings.get("motion_frame") or DEFAULT_MOTION_FRAME))
    persistent_dit = max(0, int(settings.get("num_persistent_param_in_dit") or DEFAULT_PERSISTENT_DIT))
    timeout_seconds = int(settings.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
    save_prefix = result_dir / "segment"
    output_mp4 = Path(f"{save_prefix}.mp4")

    setup_parts = [
        f"source {CONDA_SH}",
        f"conda activate {CONDA_ENV}",
        f"cd {BASE_DIR}",
        "export CUDA_HOME=/home/saita/miniforge3/envs/infinitetalk5090",
        'export NVIDIA_ROOT="$CUDA_HOME/lib/python3.10/site-packages/nvidia"',
        'export ALL_INCLUDES=$(find "$NVIDIA_ROOT" -maxdepth 2 -type d -name include | paste -sd: -)',
        'export ALL_LIBS=$(find "$NVIDIA_ROOT" -maxdepth 2 -type d -name lib | paste -sd: -)',
        'export BASE_INCLUDES="$CUDA_HOME/targets/x86_64-linux/include:$CUDA_HOME/include"',
        'export BASE_LIBS="$CUDA_HOME/targets/x86_64-linux/lib:$CUDA_HOME/lib"',
        'export CPATH="$BASE_INCLUDES:$ALL_INCLUDES:${CPATH:-}"',
        'export CPLUS_INCLUDE_PATH="$CPATH"',
        'export LIBRARY_PATH="$BASE_LIBS:$ALL_LIBS:${LIBRARY_PATH:-}"',
        'export LD_LIBRARY_PATH="$BASE_LIBS:$ALL_LIBS:${LD_LIBRARY_PATH:-}"',
        "export TORCH_EXTENSIONS_DIR=/home/saita/InfiniteTalk/.torch_extensions",
        "export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True,max_split_size_mb:128",
    ]
    command_parts = [
        "CUDA_VISIBLE_DEVICES=0 python generate_infinitetalk.py",
        "--ckpt_dir weights/Wan2.1-I2V-14B-480P",
        "--wav2vec_dir weights/chinese-wav2vec2-base",
        "--infinitetalk_dir weights/InfiniteTalk/single/infinitetalk.safetensors",
        f"--input_json {input_json}",
        f"--size {size}",
        f"--frame_num {frame_num}",
        f"--sample_steps {sample_steps}",
        "--mode streaming",
        "--quant fp8",
        "--quant_dir weights/InfiniteTalk/quant_models/infinitetalk_single_fp8.safetensors",
        f"--motion_frame {motion_frame}",
        f"--num_persistent_param_in_dit {persistent_dit}",
        f"--save_file {save_prefix}",
    ]
    command = " && ".join(setup_parts + [" ".join(str(part) for part in command_parts)])
    log_path = job_dir / "run.log"

    with log_path.open("ab") as log_handle:
        process = subprocess.run(
            ["bash", "-lc", command],
            cwd=str(BASE_DIR),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
    if process.returncode != 0:
        raise RuntimeError(f"InfiniteTalk generation failed with exit code {process.returncode}; see {log_path}")
    if not output_mp4.exists():
        raise RuntimeError(f"InfiniteTalk finished but output was not found: {output_mp4}")

    expected_duration = _probe_media_duration_seconds(Path(job["audio_path"])) or 0
    actual_duration = _probe_media_duration_seconds(output_mp4) or 0
    duration_ratio = actual_duration / expected_duration if expected_duration > 0 else 1.0
    if expected_duration > 0 and actual_duration <= 0:
        raise RuntimeError("InfiniteTalk 输出视频缺少有效时长")
    if expected_duration > 0 and duration_ratio < 0.85:
        raise RuntimeError(
            f"InfiniteTalk 输出时长明显短于音频：audio={expected_duration:.2f}s, video={actual_duration:.2f}s"
        )

    job["status"] = "done"
    job["message"] = "生成完成"
    job["result_path"] = str(output_mp4)
    job["expected_duration"] = expected_duration
    job["video_duration"] = actual_duration
    job["finished_at"] = _now()
    _write_job(job)


def _worker_loop() -> None:
    while True:
        job_id = job_queue.get()
        try:
            job = _read_job(job_id)
            if not job or job.get("status") == "done":
                continue
            max_attempts = max(1, int((job.get("settings") or {}).get("retries") or DEFAULT_RETRIES))
            attempt = int(job.get("attempt") or 0) + 1
            job["attempt"] = attempt
            job["status"] = "running"
            job["message"] = f"正在生成 InfiniteTalk 数字人视频（第 {attempt}/{max_attempts} 次）"
            job["started_at"] = job.get("started_at") or _now()
            _write_job(job)
            try:
                _run_generation(job)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"InfiniteTalk generation timed out after {exc.timeout} seconds") from exc
            except Exception as exc:
                job = _read_job(job_id) or job
                job["error"] = str(exc)
                if attempt < max_attempts:
                    job["status"] = "queued"
                    job["message"] = f"生成失败，已进入重试队列：{exc}"
                    _write_job(job)
                    _enqueue(job_id)
                else:
                    job["status"] = "error"
                    job["message"] = "生成失败"
                    job["finished_at"] = _now()
                    _write_job(job)
        finally:
            job_queue.task_done()


def _recover_jobs() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    for path in sorted(JOBS_DIR.glob("*/job.json")):
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if job.get("status") in {"queued", "running"}:
            job["status"] = "queued"
            job["message"] = "服务重启后已恢复排队"
            _write_job(job)
            _enqueue(str(job["job_id"]))


@app.on_event("startup")
def startup() -> None:
    global worker_started
    if not worker_started:
        _recover_jobs()
        thread = threading.Thread(target=_worker_loop, daemon=True)
        thread.start()
        worker_started = True


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "queue_size": job_queue.qsize(), "statuses": _status_counts()}


@app.post("/generate")
async def generate(
    image: UploadFile = File(...),
    audio: UploadFile = File(...),
    prompt: str = Form(""),
    external_task_id: str = Form(""),
    segment_index: int = Form(0),
    settings_json: str = Form("{}"),
):
    job_id = f"it_{uuid.uuid4().hex[:12]}"
    job_dir = _job_dir(job_id)
    input_dir = job_dir / "input"
    image_suffix = Path(image.filename or "image.png").suffix or ".png"
    audio_suffix = Path(audio.filename or "audio.wav").suffix or ".wav"
    image_path = input_dir / f"image{image_suffix}"
    audio_path = input_dir / f"audio{audio_suffix}"
    _save_upload(image, image_path)
    _save_upload(audio, audio_path)
    normalized_audio_path = _normalize_audio_for_infinitetalk(audio_path)

    try:
        settings = json.loads(settings_json or "{}")
    except json.JSONDecodeError:
        settings = {}

    job = {
        "job_id": job_id,
        "external_task_id": external_task_id,
        "segment_index": segment_index,
        "status": "queued",
        "message": "已进入 InfiniteTalk 队列",
        "prompt": prompt,
        "settings": settings,
        "image_path": str(image_path),
        "audio_path": str(normalized_audio_path),
        "fps": int(settings.get("fps") or 25),
        "attempt": 0,
        "created_at": _now(),
        "updated_at": _now(),
    }
    _write_job(job)
    _enqueue(job_id)
    return {"job_id": job_id, "status": "queued", "message": job["message"]}


@app.get("/status/{job_id}")
def status(job_id: str):
    job = _read_job(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    payload = {
        key: job.get(key)
        for key in (
            "job_id",
            "status",
            "message",
            "error",
            "attempt",
            "created_at",
            "updated_at",
            "started_at",
            "finished_at",
            "expected_duration",
            "video_duration",
        )
    }
    payload["has_result"] = bool(job.get("result_path") and Path(job["result_path"]).exists())
    payload["queue_size"] = job_queue.qsize()
    return payload


@app.get("/result/{job_id}")
def result(job_id: str):
    job = _read_job(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    result_path = Path(str(job.get("result_path") or ""))
    if job.get("status") != "done" or not result_path.exists():
        return JSONResponse({"error": "result not ready", "status": job.get("status")}, status_code=409)
    return FileResponse(str(result_path), media_type="video/mp4", filename=f"{job_id}.mp4")
