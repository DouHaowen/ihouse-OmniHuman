"""
Local HunyuanVideo-Avatar HTTP worker.

This service runs on the 5090 machine. It accepts image/audio uploads,
persists jobs to disk, and processes one generation at a time so GPU memory
does not get oversubscribed.
"""

from __future__ import annotations

import csv
import math
import json
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


BASE_DIR = Path(os.getenv("HUNYUAN_AVATAR_BASE_DIR", "/home/saita/hunyuanvideo-avatar-poc")).resolve()
JOBS_DIR = Path(os.getenv("HUNYUAN_AVATAR_JOBS_DIR", str(BASE_DIR / "api_jobs"))).resolve()
CONDA_SH = os.getenv("HUNYUAN_AVATAR_CONDA_SH", "/home/saita/miniforge3/etc/profile.d/conda.sh")
CONDA_ENV = os.getenv("HUNYUAN_AVATAR_CONDA_ENV", "hunyuan-avatar")
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("HUNYUAN_AVATAR_TIMEOUT_SECONDS", "7200"))
DEFAULT_RETRIES = int(os.getenv("HUNYUAN_AVATAR_RETRIES", "2"))

app = FastAPI(title="iHouse HunyuanVideo-Avatar Worker")
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


def _align_sample_n_frames(frame_count: int) -> int:
    # The model expects video lengths in the 4n+1 form.
    frame_count = max(129, int(frame_count or 129))
    remainder = (frame_count - 1) % 4
    if remainder:
        frame_count += 4 - remainder
    return frame_count


def _resolve_sample_n_frames(audio_path: Path, settings: dict[str, Any], fps: int) -> int:
    configured = int(settings.get("sample_n_frames") or 0)
    if configured > 0:
        return _align_sample_n_frames(configured)
    duration_seconds = _probe_media_duration_seconds(audio_path)
    if duration_seconds and duration_seconds > 0:
        return _align_sample_n_frames(int(math.ceil(duration_seconds * max(1, fps))) + 1)
    return 129


def _build_csv(job: dict[str, Any], csv_path: Path) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["videoid", "image", "audio", "prompt", "fps"])
        writer.writeheader()
        writer.writerow(
            {
                "videoid": "segment",
                "image": job["image_rel"],
                "audio": job["audio_rel"],
                "prompt": job.get("prompt") or "A professional iHouse news anchor speaks naturally to camera with accurate lip sync, stable face, calm body, and a clear iHouse logo table sign.",
                "fps": int(job.get("fps") or 25),
            }
        )


def _run_generation(job: dict[str, Any]) -> None:
    job_id = str(job["job_id"])
    job_dir = _job_dir(job_id)
    output_dir = job_dir / "result"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = job_dir / "input.csv"
    _build_csv(job, csv_path)

    settings = job.get("settings") or {}
    image_size = int(settings.get("image_size") or 672)
    infer_steps = int(settings.get("infer_steps") or 20)
    cfg_scale = float(settings.get("cfg_scale") or 7.0)
    fps = int(job.get("fps") or settings.get("fps") or 25)
    sample_n_frames = _resolve_sample_n_frames(Path(job["audio_path"]), settings, fps)
    flow_shift = float(settings.get("flow_shift") or 5.0)
    timeout_seconds = int(settings.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
    use_cpu_offload = bool(settings.get("cpu_offload", True))
    use_fp8 = bool(settings.get("use_fp8", True))

    checkpoint = "./weights/ckpts/hunyuan-video-t2v-720p/transformers/mp_rank_00_model_states_fp8.pt"
    setup_parts = [
        f"source {CONDA_SH}",
        f"conda activate {CONDA_ENV}",
        f"cd {BASE_DIR}",
        "export PYTHONPATH=./",
        "export MODEL_BASE=./weights",
        "export DISABLE_SP=1",
        "export CPU_OFFLOAD=1",
        "export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True",
    ]
    command_parts = [
        "CUDA_VISIBLE_DEVICES=0 python3 hymm_sp/sample_gpu_poor.py",
        f"--input {csv_path}",
        f"--ckpt {checkpoint}",
        f"--sample-n-frames {sample_n_frames}",
        f"--seed {int(settings.get('seed') or 128)}",
        f"--image-size {image_size}",
        f"--cfg-scale {cfg_scale}",
        f"--infer-steps {infer_steps}",
        "--use-deepcache 1",
        f"--flow-shift-eval-video {flow_shift}",
        f"--save-path {output_dir}",
    ]
    if use_fp8:
        command_parts.append("--use-fp8")
    if use_cpu_offload:
        command_parts.append("--cpu-offload")
    if sample_n_frames <= 129:
        command_parts.append("--infer-min")

    command = " && ".join(setup_parts + [" ".join(str(part) for part in command_parts)])
    log_path = job_dir / "run.log"
    output_mp4 = output_dir / "segment_audio.mp4"

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
        raise RuntimeError(f"Hunyuan generation failed with exit code {process.returncode}; see {log_path}")
    if not output_mp4.exists():
        raise RuntimeError(f"Hunyuan finished but output was not found: {output_mp4}")

    job["status"] = "done"
    job["message"] = "生成完成"
    job["result_path"] = str(output_mp4)
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
            job["message"] = f"正在生成 Hunyuan 数字人视频（第 {attempt}/{max_attempts} 次）"
            job["started_at"] = job.get("started_at") or _now()
            _write_job(job)
            try:
                _run_generation(job)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"Hunyuan generation timed out after {exc.timeout} seconds") from exc
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
    job_id = f"hy_{uuid.uuid4().hex[:12]}"
    job_dir = _job_dir(job_id)
    input_dir = job_dir / "input"
    image_suffix = Path(image.filename or "image.png").suffix or ".png"
    audio_suffix = Path(audio.filename or "audio.wav").suffix or ".wav"
    image_path = input_dir / f"image{image_suffix}"
    audio_path = input_dir / f"audio{audio_suffix}"
    _save_upload(image, image_path)
    _save_upload(audio, audio_path)

    try:
        settings = json.loads(settings_json or "{}")
    except json.JSONDecodeError:
        settings = {}

    job = {
        "job_id": job_id,
        "external_task_id": external_task_id,
        "segment_index": segment_index,
        "status": "queued",
        "message": "已进入 Hunyuan 队列",
        "prompt": prompt,
        "settings": settings,
        "image_path": str(image_path),
        "audio_path": str(audio_path),
        "image_rel": image_path.relative_to(BASE_DIR).as_posix(),
        "audio_rel": audio_path.relative_to(BASE_DIR).as_posix(),
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
    payload = {key: job.get(key) for key in ("job_id", "status", "message", "error", "attempt", "created_at", "updated_at", "started_at", "finished_at")}
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
