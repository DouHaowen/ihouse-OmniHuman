"""Client for the 5090 InfiniteTalk worker service."""

from __future__ import annotations

import json
import math
import os
import subprocess
import time
from pathlib import Path

import requests
import urllib3


DEFAULT_SETTINGS = {
    "size": "infinitetalk-480",
    "frame_num": 81,
    "sample_steps": 6,
    "mode": "streaming",
    "quant": "fp8",
    "motion_frame": 3,
    "sample_text_guide_scale": 5.0,
    "sample_audio_guide_scale": 2.2,
    "num_persistent_param_in_dit": 0,
    "retries": 1,
}


def _infinitetalk_prompt(prompt: str) -> str:
    base = (prompt or "").strip()
    suffix = " 主播口型自然清晰，但嘴部开合保持克制，不要夸张张嘴，面部动作稳定柔和。"
    return f"{base}{suffix}".strip()


class InfiniteTalkAvatarError(RuntimeError):
    pass


def _retry_attempts() -> int:
    return max(1, int(os.getenv("INFINITETALK_AVATAR_RETRY_ATTEMPTS", "3")))


def _retry_delay_seconds() -> int:
    return max(3, int(os.getenv("INFINITETALK_AVATAR_RETRY_DELAY_SECONDS", "15")))


def _is_retryable_message(text: str) -> bool:
    lowered = (text or "").lower()
    retry_tokens = [
        "502",
        "503",
        "504",
        "bad gateway",
        "connection refused",
        "connection aborted",
        "connection reset",
        "remote end closed",
        "timed out",
        "timeout",
        "cuda",
        "gpu",
        "busy or unavailable",
        "device(s) is/are busy",
        "health",
        "status query failed",
        "结果下载失败",
        "提交失败",
        "service unavailable",
    ]
    return any(token in lowered for token in retry_tokens)


def _sleep_before_retry(attempt: int) -> None:
    time.sleep(_retry_delay_seconds() * attempt)


def _base_url() -> str:
    return os.getenv("INFINITETALK_AVATAR_API_BASE_URL", "https://172.18.0.1/__infinitetalk_avatar__").rstrip("/")


def _verify_tls() -> bool:
    configured = os.getenv("INFINITETALK_AVATAR_VERIFY_TLS", "").strip().lower()
    if configured in {"0", "false", "no", "off"}:
        return False
    if configured in {"1", "true", "yes", "on"}:
        return True
    return "172.18.0.1" not in _base_url()


def _request_timeout() -> int:
    return max(10, int(os.getenv("INFINITETALK_AVATAR_REQUEST_TIMEOUT_SECONDS", "180")))


def _status_failure_tolerance() -> int:
    return max(1, int(os.getenv("INFINITETALK_AVATAR_STATUS_FAILURE_TOLERANCE", "6")))


def _probe_audio_duration_seconds(path: Path) -> float:
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
            timeout=15,
        )
        if completed.returncode == 0:
            return max(0.0, float((completed.stdout or "0").strip() or 0))
    except Exception:
        return 0.0
    return 0.0


def _resolve_generation_timeout_seconds(audio: Path) -> int:
    min_timeout = int(os.getenv("INFINITETALK_AVATAR_MIN_TIMEOUT_SECONDS", "900"))
    max_timeout = int(os.getenv("INFINITETALK_AVATAR_MAX_TIMEOUT_SECONDS", "2400"))
    per_audio_second = float(os.getenv("INFINITETALK_AVATAR_TIMEOUT_PER_AUDIO_SECOND", "120"))
    audio_duration = _probe_audio_duration_seconds(audio)
    if audio_duration <= 0:
        return int(os.getenv("INFINITETALK_AVATAR_TIMEOUT_SECONDS", "1800"))
    dynamic_timeout = int(math.ceil(audio_duration * per_audio_second))
    return max(min_timeout, min(max_timeout, dynamic_timeout))


def generate_infinitetalk_avatar_video(
    *,
    image_path: str,
    audio_path: str,
    output_path: str,
    prompt: str = "",
    external_task_id: str = "",
    segment_index: int = 0,
    settings: dict | None = None,
    poll_interval_seconds: int | None = None,
    max_wait_seconds: int | None = None,
) -> str:
    image = Path(image_path)
    audio = Path(audio_path)
    if not image.exists():
        raise InfiniteTalkAvatarError(f"主播图片不存在: {image}")
    if not audio.exists():
        raise InfiniteTalkAvatarError(f"音频文件不存在: {audio}")

    merged_settings = dict(DEFAULT_SETTINGS)
    merged_settings.update(settings or {})
    if not merged_settings.get("timeout_seconds"):
        merged_settings["timeout_seconds"] = _resolve_generation_timeout_seconds(audio)
    base_url = _base_url()
    timeout = _request_timeout()
    verify_tls = _verify_tls()
    if not verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    poll_interval = max(5, int(poll_interval_seconds or os.getenv("INFINITETALK_AVATAR_POLL_INTERVAL_SECONDS", "20")))
    configured_max_wait = max_wait_seconds or os.getenv("INFINITETALK_AVATAR_MAX_WAIT_SECONDS")
    if configured_max_wait:
        max_wait = max(300, int(configured_max_wait))
    else:
        max_wait = int(merged_settings["timeout_seconds"]) + 300
    last_error: Exception | None = None

    for attempt in range(1, _retry_attempts() + 1):
        job_id = ""
        try:
            health_response = requests.get(f"{base_url}/health", timeout=min(timeout, 30), verify=verify_tls)
            if health_response.status_code >= 400:
                raise InfiniteTalkAvatarError(
                    f"InfiniteTalk 健康检查失败: HTTP {health_response.status_code} {health_response.text[:200]}"
                )

            with image.open("rb") as image_file, audio.open("rb") as audio_file:
                response = requests.post(
                    f"{base_url}/generate",
                    files={
                        "image": (image.name, image_file, "application/octet-stream"),
                        "audio": (audio.name, audio_file, "application/octet-stream"),
                    },
                    data={
                        "prompt": _infinitetalk_prompt(prompt),
                        "external_task_id": external_task_id or "",
                        "segment_index": str(segment_index or 0),
                        "settings_json": json.dumps(merged_settings, ensure_ascii=False),
                    },
                    timeout=timeout,
                    verify=verify_tls,
                )
            if response.status_code >= 400:
                raise InfiniteTalkAvatarError(f"InfiniteTalk 提交失败: HTTP {response.status_code} {response.text[:500]}")
            job_id = (response.json() or {}).get("job_id")
            if not job_id:
                raise InfiniteTalkAvatarError(f"InfiniteTalk 提交成功但未返回 job_id: {response.text[:500]}")

            start = time.time()
            last_message = ""
            status_failures = 0
            while time.time() - start < max_wait:
                try:
                    status_response = requests.get(f"{base_url}/status/{job_id}", timeout=timeout, verify=verify_tls)
                    if status_response.status_code >= 400:
                        raise InfiniteTalkAvatarError(
                            f"InfiniteTalk 状态查询失败: HTTP {status_response.status_code} {status_response.text[:500]}"
                        )
                except (requests.RequestException, InfiniteTalkAvatarError) as exc:
                    status_failures += 1
                    if status_failures <= _status_failure_tolerance():
                        print(
                            f"InfiniteTalk job {job_id}: 状态查询临时失败"
                            f"（{status_failures}/{_status_failure_tolerance()}）：{exc}",
                            flush=True,
                        )
                        time.sleep(poll_interval)
                        continue
                    raise InfiniteTalkAvatarError(
                        f"InfiniteTalk 状态查询连续失败，已停止等待当前 job {job_id}: {exc}"
                    ) from exc
                status_failures = 0
                status_data = status_response.json() or {}
                status = status_data.get("status")
                message = status_data.get("message") or status
                if message and message != last_message:
                    print(f"InfiniteTalk job {job_id}: {message}", flush=True)
                    last_message = message
                if status == "done":
                    output = Path(output_path)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    download_response = requests.get(
                        f"{base_url}/result/{job_id}",
                        stream=True,
                        timeout=max(timeout, 600),
                        verify=verify_tls,
                    )
                    if download_response.status_code >= 400:
                        raise InfiniteTalkAvatarError(
                            f"InfiniteTalk 结果下载失败: HTTP {download_response.status_code} {download_response.text[:500]}"
                        )
                    tmp_path = output.with_suffix(output.suffix + ".part")
                    with tmp_path.open("wb") as handle:
                        for chunk in download_response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                    tmp_path.replace(output)
                    return str(output)
                if status == "error":
                    raise InfiniteTalkAvatarError(status_data.get("error") or "InfiniteTalk 生成失败")
                time.sleep(poll_interval)

            raise InfiniteTalkAvatarError(f"InfiniteTalk 任务超时（超过 {max_wait} 秒）: {job_id}")
        except requests.RequestException as exc:
            last_error = InfiniteTalkAvatarError(f"InfiniteTalk 网络异常：{exc}")
        except Exception as exc:
            last_error = exc

        if attempt >= _retry_attempts() or not _is_retryable_message(str(last_error or "")):
            break
        print(
            f"InfiniteTalk attempt {attempt}/{_retry_attempts()} failed"
            f"{f' for {job_id}' if job_id else ''}: {last_error}. retrying...",
            flush=True,
        )
        _sleep_before_retry(attempt)

    if last_error:
        raise last_error
    raise InfiniteTalkAvatarError("InfiniteTalk 生成失败")
