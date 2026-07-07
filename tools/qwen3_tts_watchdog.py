#!/usr/bin/env python3
"""Watchdog for the 5090 local Qwen3-TTS service.

Checks the TTS HTTP service for responsiveness. It intentionally does not call
model-loading endpoints by default, because repeated cold model loads are risky
on the 5090 driver stack.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


SERVICE_URL = (os.getenv("QWEN3_TTS_WATCHDOG_URL", "http://127.0.0.1:8895") or "http://127.0.0.1:8895").strip().rstrip("/")
API_TOKEN = (os.getenv("QWEN3_TTS_API_TOKEN", "local-qwen3-tts-5090") or "local-qwen3-tts-5090").strip()
SERVICE_NAME = (os.getenv("QWEN3_TTS_SYSTEMD_SERVICE", "ihouse-qwen3-tts.service") or "ihouse-qwen3-tts.service").strip()
HEALTH_TIMEOUT = max(3, int(os.getenv("QWEN3_TTS_WATCHDOG_HEALTH_TIMEOUT", "8") or "8"))
VOICES_TIMEOUT = max(5, int(os.getenv("QWEN3_TTS_WATCHDOG_VOICES_TIMEOUT", "25") or "25"))
POST_RESTART_WAIT = max(5, int(os.getenv("QWEN3_TTS_WATCHDOG_POST_RESTART_WAIT", "20") or "20"))
RESTART_COOLDOWN_SECONDS = max(30, int(os.getenv("QWEN3_TTS_WATCHDOG_RESTART_COOLDOWN_SECONDS", "180") or "180"))
STATE_PATH = Path(os.getenv("QWEN3_TTS_WATCHDOG_STATE", "/home/saita/qwen3-tts-service/watchdog_state.json"))
CHECK_VOICES = (os.getenv("QWEN3_TTS_WATCHDOG_CHECK_VOICES", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
RESTART_ENABLED = (os.getenv("QWEN3_TTS_WATCHDOG_RESTART_ENABLED", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


def _now() -> float:
    return time.time()


def _request_json(path: str, timeout: int) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{SERVICE_URL}{path}",
        headers={"X-Token": API_TOKEN},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read()
    data = json.loads(payload.decode("utf-8"))
    return data if isinstance(data, dict) else {}


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(payload: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(STATE_PATH)


def _restart_allowed(state: dict[str, Any]) -> bool:
    last_restart_at = float(state.get("last_restart_at") or 0)
    return _now() - last_restart_at >= RESTART_COOLDOWN_SECONDS


def _systemctl(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", "--user", *args, SERVICE_NAME],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


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


def _status_payload(ok: bool, *, message: str, restarted: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": ok,
        "message": message,
        "restarted": restarted,
        "checked_at": int(_now()),
        "detail": detail or {},
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


def _check_health() -> dict[str, Any]:
    health = _request_json("/health", HEALTH_TIMEOUT)
    if not health.get("ok"):
        raise RuntimeError(f"/health returned not ok: {health}")
    return health


def _check_voices() -> dict[str, Any]:
    voices = _request_json("/voices", VOICES_TIMEOUT)
    if not voices.get("ok"):
        raise RuntimeError(f"/voices returned not ok: {voices}")
    speakers = voices.get("speakers")
    if not isinstance(speakers, list) or not speakers:
        raise RuntimeError(f"/voices missing speakers: {voices}")
    return voices


def _restart_service(reason: str, state: dict[str, Any]) -> dict[str, Any]:
    completed = _systemctl("restart")
    if completed.returncode != 0:
        raise RuntimeError(
            f"restart {SERVICE_NAME} failed: {completed.returncode} "
            f"stdout={completed.stdout[-500:]} stderr={completed.stderr[-500:]}"
        )
    time.sleep(POST_RESTART_WAIT)
    detail: dict[str, Any] = {
        "restart_reason": reason,
        "restart_stdout": (completed.stdout or "").strip()[-500:],
        "restart_stderr": (completed.stderr or "").strip()[-500:],
    }
    try:
        detail["health"] = _check_health()
        if CHECK_VOICES:
            detail["voices"] = _check_voices()
    except Exception as exc:
        detail["post_restart_error"] = str(exc)
        raise RuntimeError(f"{SERVICE_NAME} restarted but validation failed: {exc}") from exc
    state["last_restart_at"] = _now()
    state["last_restart_reason"] = reason
    _save_state(state)
    return _status_payload(True, message=f"{SERVICE_NAME} restarted by watchdog", restarted=True, detail=detail)


def main() -> int:
    state = _load_state()
    if not _gpu_available():
        state["last_gpu_unavailable_at"] = _now()
        _save_state(state)
        print(
            json.dumps(
                _status_payload(
                    False,
                    message="gpu unavailable; watchdog skipped qwen3-tts restart",
                    detail={"gpu_processes": _gpu_process_snapshot()},
                ),
                ensure_ascii=False,
            )
        )
        return 0
    try:
        health = _check_health()
        voices: dict[str, Any] = {}
        if CHECK_VOICES:
            voices = _check_voices()
        state["last_ok_at"] = _now()
        _save_state(state)
        detail = {"health": health}
        if CHECK_VOICES:
            detail["voices_count"] = len(voices.get("speakers") or [])
        print(json.dumps(_status_payload(True, message="qwen3-tts healthy", detail=detail), ensure_ascii=False))
        return 0
    except Exception as exc:
        reason = str(exc)
        if not _gpu_available():
            state["last_gpu_unavailable_at"] = _now()
            state["last_gpu_unavailable_reason"] = reason
            _save_state(state)
            print(
                json.dumps(
                    _status_payload(
                        False,
                        message=f"gpu unavailable after health failure; skipped restart: {reason[:200]}",
                        detail={"gpu_processes": _gpu_process_snapshot()},
                    ),
                    ensure_ascii=False,
                )
            )
            return 0
        if not RESTART_ENABLED:
            state["last_unhealthy_at"] = _now()
            state["last_unhealthy_reason"] = reason
            _save_state(state)
            print(json.dumps(_status_payload(False, message=f"qwen3-tts unhealthy; automatic restart disabled: {reason[:200]}"), ensure_ascii=False))
            return 0
        if not _restart_allowed(state):
            print(json.dumps(_status_payload(False, message=f"qwen3-tts unhealthy but in cooldown: {reason}", detail={"cooldown_seconds": RESTART_COOLDOWN_SECONDS}), ensure_ascii=False))
            return 0
        try:
            payload = _restart_service(reason, state)
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        except Exception as restart_exc:
            print(json.dumps(_status_payload(False, message=f"qwen3-tts restart failed: {restart_exc}", detail={"initial_error": reason}), ensure_ascii=False), file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
