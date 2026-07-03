#!/usr/bin/env python3
"""Watchdog for the 5090 local Qwen3-TTS service.

Checks the TTS HTTP service for responsiveness. If health or voices endpoints
hang/fail, restart the systemd user unit to recover from a stuck model process.
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


def _status_payload(ok: bool, *, message: str, restarted: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": ok,
        "message": message,
        "restarted": restarted,
        "checked_at": int(_now()),
        "detail": detail or {},
    }


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
    try:
        health = _check_health()
        voices = _check_voices()
        state["last_ok_at"] = _now()
        _save_state(state)
        print(json.dumps(_status_payload(True, message="qwen3-tts healthy", detail={"health": health, "voices_count": len(voices.get("speakers") or [])}), ensure_ascii=False))
        return 0
    except Exception as exc:
        reason = str(exc)
        if not _restart_allowed(state):
            print(json.dumps(_status_payload(False, message=f"qwen3-tts unhealthy but in cooldown: {reason}", detail={"cooldown_seconds": RESTART_COOLDOWN_SECONDS}), ensure_ascii=False), file=sys.stderr)
            return 2
        try:
            payload = _restart_service(reason, state)
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        except Exception as restart_exc:
            print(json.dumps(_status_payload(False, message=f"qwen3-tts restart failed: {restart_exc}", detail={"initial_error": reason}), ensure_ascii=False), file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
