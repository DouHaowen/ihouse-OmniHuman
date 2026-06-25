#!/usr/bin/env python3
"""Lightweight 5090 GPU service profile orchestrator.

Runs on the 5090 machine and switches heavyweight GPU services by workflow
stage so InfiniteTalk, Qwen3-VL and ComfyUI do not compete for VRAM.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel


TOKEN = os.getenv("IHOUSE_GPU_ORCHESTRATOR_TOKEN", "local-gpu-orchestrator-5090").strip()
STATE_PATH = Path(os.getenv("IHOUSE_GPU_ORCHESTRATOR_STATE", "/home/saita/ihouse-gpu-orchestrator/state.txt"))
SYSTEMCTL_TIMEOUT = int(os.getenv("IHOUSE_GPU_ORCHESTRATOR_SYSTEMCTL_TIMEOUT", "90"))

SERVICE_GROUPS = {
    "tts": ["ihouse-qwen3-tts.service"],
    "vl": ["ihouse-material-vl-vector.service"],
    "image": ["ihouse-opennews-image.service", "ihouse-comfyui.service"],
    "digital": ["ihouse-infinitetalk.service"],
}

PROFILES = {
    # Digital-human generation is the most VRAM-sensitive stage. Keep TTS
    # available, but stop Qwen3-VL and ComfyUI/image generation.
    "digital_intro": {
        "stop": SERVICE_GROUPS["vl"] + SERVICE_GROUPS["image"],
        "start": SERVICE_GROUPS["tts"] + SERVICE_GROUPS["digital"],
    },
    # OpenNews production after intro: local TTS and Qwen3-VL material review,
    # with digital-human and ComfyUI stopped to protect VRAM.
    "material": {
        "stop": SERVICE_GROUPS["digital"] + SERVICE_GROUPS["image"],
        "start": SERVICE_GROUPS["tts"] + SERVICE_GROUPS["vl"],
    },
    # Optional image-generation mode for future use.
    "image": {
        "stop": SERVICE_GROUPS["digital"] + SERVICE_GROUPS["vl"],
        "start": SERVICE_GROUPS["tts"] + SERVICE_GROUPS["image"],
    },
    # Safe idle keeps only TTS warm.
    "idle": {
        "stop": SERVICE_GROUPS["digital"] + SERVICE_GROUPS["vl"] + SERVICE_GROUPS["image"],
        "start": SERVICE_GROUPS["tts"],
    },
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
        cmd = ["systemctl", "--user", action, service]
        started_at = time.time()
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT,
        )
        results.append(
            {
                "service": service,
                "action": action,
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
    results.extend(_run_systemctl("start", list(spec.get("start") or [])))
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(profile, encoding="utf-8")
    return {
        "ok": all(item.get("ok") for item in results),
        "profile": profile,
        "reason": req.reason,
        "results": results,
        "services": _service_status(_all_services()),
    }
