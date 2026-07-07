#!/usr/bin/env python3
"""Lightweight 5090 GPU service profile orchestrator.

Runs on the 5090 machine and switches heavyweight GPU services by workflow
stage so InfiniteTalk and Qwen3-TTS do not compete for VRAM.
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel


TOKEN = os.getenv("IHOUSE_GPU_ORCHESTRATOR_TOKEN", "local-gpu-orchestrator-5090").strip()
STATE_PATH = Path(os.getenv("IHOUSE_GPU_ORCHESTRATOR_STATE", "/home/saita/ihouse-gpu-orchestrator/state.txt"))
SYSTEMCTL_TIMEOUT = int(os.getenv("IHOUSE_GPU_ORCHESTRATOR_SYSTEMCTL_TIMEOUT", "90"))
STATE_WAIT_SECONDS = float(os.getenv("IHOUSE_GPU_ORCHESTRATOR_STATE_WAIT_SECONDS", "12"))
PROFILE_SETTLE_SECONDS = float(os.getenv("IHOUSE_GPU_ORCHESTRATOR_PROFILE_SETTLE_SECONDS", "8"))
READINESS_WAIT_SECONDS = float(os.getenv("IHOUSE_GPU_ORCHESTRATOR_READINESS_WAIT_SECONDS", "60"))
QWEN3_TTS_API_TOKEN = os.getenv("QWEN3_TTS_API_TOKEN", "local-qwen3-tts-5090").strip()
INFINITETALK_CLEANUP_SCRIPT = (
    os.getenv("IHOUSE_INFINITETALK_CLEANUP_SCRIPT", "/home/saita/InfiniteTalk/cleanup_infinitetalk_processes.sh")
    or "/home/saita/InfiniteTalk/cleanup_infinitetalk_processes.sh"
).strip()

SERVICE_GROUPS = {
    "tts": ["ihouse-qwen3-tts.service"],
    "tts_watchdog": ["ihouse-qwen3-tts-watchdog.timer"],
    "tts_watchdog_job": ["ihouse-qwen3-tts-watchdog.service"],
    "digital": ["ihouse-infinitetalk.service"],
}

PROFILES = {
    # Digital-human generation is VRAM-sensitive, but stopping a loaded
    # Qwen3-TTS process can trigger NVIDIA Xid 79 on this 5090/driver stack.
    # Keep Qwen resident and only pause its watchdog during digital work.
    "digital_intro": {
        "stop": SERVICE_GROUPS["tts_watchdog"] + SERVICE_GROUPS["tts_watchdog_job"],
        "start": SERVICE_GROUPS["digital"],
    },
    # OpenNews production uses only local Qwen3-TTS on the 5090.
    "material": {
        "stop": SERVICE_GROUPS["digital"],
        "pre_start_commands": [[INFINITETALK_CLEANUP_SCRIPT]],
        "start": SERVICE_GROUPS["tts"] + SERVICE_GROUPS["tts_watchdog"],
    },
    # Safe idle keeps only TTS warm.
    "idle": {
        "stop": SERVICE_GROUPS["digital"],
        "pre_start_commands": [[INFINITETALK_CLEANUP_SCRIPT]],
        "start": SERVICE_GROUPS["tts"] + SERVICE_GROUPS["tts_watchdog"],
    },
}

SERVICE_HEALTH_ENDPOINTS = {
    "ihouse-infinitetalk.service": {
        "url": "http://127.0.0.1:8893/health",
        "headers": {},
    },
    "ihouse-qwen3-tts.service": {
        "url": "http://127.0.0.1:8895/health",
        "headers": {"X-Token": QWEN3_TTS_API_TOKEN},
    },
}

GPU_SERVICES = {
    "ihouse-infinitetalk.service",
    "ihouse-qwen3-tts.service",
}

app = FastAPI(title="iHouse 5090 GPU Orchestrator")


class ProfileRequest(BaseModel):
    profile: str
    reason: str = ""


def _require_token(x_token: str | None) -> None:
    if TOKEN and x_token != TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")


def _run_systemctl(action: str, services: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for service in services:
        if action == "start" and service in GPU_SERVICES and not _gpu_available():
            results.append(
                {
                    "service": service,
                    "action": action,
                    "ok": False,
                    "returncode": 1,
                    "active": "gpu-unavailable",
                    "stdout": "",
                    "stderr": "nvidia-smi failed; GPU is unavailable or has fallen off the bus",
                    "readiness": {"ok": False, "error": "gpu-unavailable"},
                    "gpu_processes": _gpu_process_snapshot(),
                    "elapsed": 0,
                }
            )
            continue
        if action == "start":
            subprocess.run(
                ["systemctl", "--user", "reset-failed", service],
                capture_output=True,
                text=True,
                timeout=10,
            )
        cmd = ["systemctl", "--user", action, service]
        started_at = time.time()
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT,
        )
        final_active = ""
        final_ok = completed.returncode == 0
        readiness: dict[str, Any] = {}
        deadline = time.time() + max(0.0, STATE_WAIT_SECONDS)
        while time.time() <= deadline:
            status = subprocess.run(
                ["systemctl", "--user", "is-active", service],
                capture_output=True,
                text=True,
                timeout=10,
            )
            final_active = (status.stdout or "").strip()
            if action == "stop" and final_active in {"inactive", "failed", ""}:
                final_ok = True
                if final_active == "failed":
                    subprocess.run(
                        ["systemctl", "--user", "reset-failed", service],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    final_active = "inactive"
                break
            if action == "start" and final_active == "active":
                final_ok = True
                break
            time.sleep(0.5)
        if action == "start" and final_ok:
            readiness = _wait_service_ready(service)
            final_ok = bool(readiness.get("ok", True))
        gpu_processes = _gpu_process_snapshot() if service in GPU_SERVICES and action == "start" and not final_ok else []
        results.append(
            {
                "service": service,
                "action": action,
                "ok": final_ok,
                "returncode": completed.returncode,
                "active": final_active,
                "stdout": (completed.stdout or "").strip()[-1000:],
                "stderr": (completed.stderr or "").strip()[-1000:],
                "readiness": readiness,
                "gpu_processes": gpu_processes,
                "elapsed": round(time.time() - started_at, 2),
            }
        )
    return results


def _gpu_available() -> bool:
    try:
        completed = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return False
    return completed.returncode == 0 and bool((completed.stdout or "").strip())


def _wait_service_ready(service: str) -> dict[str, Any]:
    spec = SERVICE_HEALTH_ENDPOINTS.get(service)
    if not spec:
        return {"ok": True, "skipped": True}
    url = str(spec.get("url") or "")
    headers = dict(spec.get("headers") or {})
    deadline = time.time() + max(0.0, READINESS_WAIT_SECONDS)
    attempts = 0
    last_error = ""
    while time.time() <= deadline:
        attempts += 1
        try:
            request = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(request, timeout=5) as response:
                body = response.read(500).decode("utf-8", errors="ignore")
            if 200 <= int(response.status) < 300:
                return {"ok": True, "url": url, "attempts": attempts, "body": body[:200]}
            last_error = f"status={response.status} body={body[:200]}"
        except urllib.error.URLError as exc:
            last_error = str(exc)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(1)
    return {
        "ok": False,
        "url": url,
        "attempts": attempts,
        "error": last_error[-500:],
        "gpu_processes": _gpu_process_snapshot(),
        "waited": READINESS_WAIT_SECONDS,
    }


def _gpu_process_snapshot() -> list[dict[str, Any]]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_gpu_memory",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return []
    if completed.returncode != 0:
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in (completed.stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",", 2)]
        if len(parts) != 3:
            continue
        pid_text, process_name, used_memory_text = parts
        try:
            pid = int(pid_text)
        except Exception:
            pid = 0
        try:
            used_memory_mb = int(float(used_memory_text))
        except Exception:
            used_memory_mb = 0
        rows.append(
            {
                "pid": pid,
                "process_name": process_name,
                "used_memory_mb": used_memory_mb,
            }
        )
    return rows


def _run_commands(stage: str, commands: list[list[str]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for command in commands:
        started_at = time.time()
        if not command:
            continue
        program = command[0]
        if not Path(program).exists():
            results.append(
                {
                    "stage": stage,
                    "command": command,
                    "ok": False,
                    "returncode": 127,
                    "stdout": "",
                    "stderr": f"missing command: {program}",
                    "elapsed": round(time.time() - started_at, 2),
                }
            )
            continue
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT,
        )
        results.append(
            {
                "stage": stage,
                "command": command,
                "ok": completed.returncode == 0,
                "returncode": completed.returncode,
                "stdout": (completed.stdout or "").strip()[-1000:],
                "stderr": (completed.stderr or "").strip()[-1000:],
                "elapsed": round(time.time() - started_at, 2),
            }
        )
    return results


def _service_status(services: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for service in services:
        completed = subprocess.run(
            ["systemctl", "--user", "is-active", service],
            capture_output=True,
            text=True,
            timeout=10,
        )
        rows.append(
            {
                "service": service,
                "active": (completed.stdout or "").strip(),
                "ok": completed.returncode == 0,
            }
        )
    return rows


def _all_services() -> list[str]:
    seen: list[str] = []
    for names in SERVICE_GROUPS.values():
        for name in names:
            if name not in seen:
                seen.append(name)
    return seen


@app.get("/health")
def health(x_token: str | None = Header(None)):
    _require_token(x_token)
    return {
        "ok": True,
        "profile": STATE_PATH.read_text(encoding="utf-8").strip() if STATE_PATH.exists() else "",
        "gpu_available": _gpu_available(),
        "gpu_processes": _gpu_process_snapshot(),
        "services": _service_status(_all_services()),
    }


@app.post("/profile")
def switch_profile(req: ProfileRequest, x_token: str | None = Header(None)):
    _require_token(x_token)
    profile = (req.profile or "").strip().lower()
    if profile not in PROFILES:
        raise HTTPException(status_code=400, detail=f"unknown profile: {profile}")
    spec = PROFILES[profile]
    results = []
    # Stop first to free VRAM before starting the target profile.
    results.extend(_run_systemctl("stop", list(spec.get("stop") or [])))
    if spec.get("stop") and spec.get("start") and PROFILE_SETTLE_SECONDS > 0:
        time.sleep(PROFILE_SETTLE_SECONDS)
    results.extend(_run_commands("pre_start", list(spec.get("pre_start_commands") or [])))
    results.extend(_run_systemctl("start", list(spec.get("start") or [])))
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(profile, encoding="utf-8")
    return {
        "ok": all(item.get("ok") for item in results),
        "profile": profile,
        "reason": req.reason,
        "results": results,
        "gpu_processes": _gpu_process_snapshot(),
        "services": _service_status(_all_services()),
    }
