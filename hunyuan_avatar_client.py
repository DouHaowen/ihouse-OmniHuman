"""Client for the 5090 HunyuanVideo-Avatar worker service."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests
import urllib3


DEFAULT_SETTINGS = {
    "image_size": 672,
    "infer_steps": 20,
    "fps": 25,
    "cfg_scale": 7.0,
    "flow_shift": 5.0,
    "use_fp8": True,
    "cpu_offload": True,
    "retries": 2,
}


class HunyuanAvatarError(RuntimeError):
    pass


def _base_url() -> str:
    # In production this code runs inside Docker. The host keeps an SSH tunnel
    # to the 5090 worker and exposes it to the container through Nginx on 443.
    return os.getenv("HUNYUAN_AVATAR_API_BASE_URL", "https://172.18.0.1/__hunyuan_avatar__").rstrip("/")


def _verify_tls() -> bool:
    configured = os.getenv("HUNYUAN_AVATAR_VERIFY_TLS", "").strip().lower()
    if configured in {"0", "false", "no", "off"}:
        return False
    if configured in {"1", "true", "yes", "on"}:
        return True
    return "172.18.0.1" not in _base_url()


def _request_timeout() -> int:
    return max(10, int(os.getenv("HUNYUAN_AVATAR_REQUEST_TIMEOUT_SECONDS", "120")))


def generate_hunyuan_avatar_video(
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
    """Submit a Hunyuan job, poll until it is done, and download the MP4."""
    image = Path(image_path)
    audio = Path(audio_path)
    if not image.exists():
        raise HunyuanAvatarError(f"主播图片不存在: {image}")
    if not audio.exists():
        raise HunyuanAvatarError(f"音频文件不存在: {audio}")

    merged_settings = dict(DEFAULT_SETTINGS)
    merged_settings.update(settings or {})
    base_url = _base_url()
    timeout = _request_timeout()
    verify_tls = _verify_tls()
    if not verify_tls:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    with image.open("rb") as image_file, audio.open("rb") as audio_file:
        response = requests.post(
            f"{base_url}/generate",
            files={
                "image": (image.name, image_file, "application/octet-stream"),
                "audio": (audio.name, audio_file, "application/octet-stream"),
            },
            data={
                "prompt": prompt or "",
                "external_task_id": external_task_id or "",
                "segment_index": str(segment_index or 0),
                "settings_json": json.dumps(merged_settings, ensure_ascii=False),
            },
            timeout=timeout,
            verify=verify_tls,
        )
    if response.status_code >= 400:
        raise HunyuanAvatarError(f"Hunyuan 提交失败: HTTP {response.status_code} {response.text[:500]}")
    job_id = (response.json() or {}).get("job_id")
    if not job_id:
        raise HunyuanAvatarError(f"Hunyuan 提交成功但未返回 job_id: {response.text[:500]}")

    poll_interval = max(5, int(poll_interval_seconds or os.getenv("HUNYUAN_AVATAR_POLL_INTERVAL_SECONDS", "20")))
    max_wait = max(300, int(max_wait_seconds or os.getenv("HUNYUAN_AVATAR_MAX_WAIT_SECONDS", "10800")))
    start = time.time()
    last_message = ""
    while time.time() - start < max_wait:
        status_response = requests.get(f"{base_url}/status/{job_id}", timeout=timeout, verify=verify_tls)
        if status_response.status_code >= 400:
            raise HunyuanAvatarError(f"Hunyuan 状态查询失败: HTTP {status_response.status_code} {status_response.text[:500]}")
        status_data = status_response.json() or {}
        status = status_data.get("status")
        message = status_data.get("message") or status
        if message and message != last_message:
            print(f"Hunyuan job {job_id}: {message}", flush=True)
            last_message = message
        if status == "done":
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            download_response = requests.get(
                f"{base_url}/result/{job_id}",
                stream=True,
                timeout=max(timeout, 300),
                verify=verify_tls,
            )
            if download_response.status_code >= 400:
                raise HunyuanAvatarError(
                    f"Hunyuan 结果下载失败: HTTP {download_response.status_code} {download_response.text[:500]}"
                )
            tmp_path = output.with_suffix(output.suffix + ".part")
            with tmp_path.open("wb") as handle:
                for chunk in download_response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
            tmp_path.replace(output)
            return str(output)
        if status == "error":
            raise HunyuanAvatarError(status_data.get("error") or "Hunyuan 生成失败")
        time.sleep(poll_interval)

    raise HunyuanAvatarError(f"Hunyuan 任务超时（超过 {max_wait} 秒）: {job_id}")
