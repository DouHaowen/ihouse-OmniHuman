"""Client helpers for pushing OpenNews videos into LocalTok."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


class LocalTokError(RuntimeError):
    """Raised when LocalTok is not configured or rejects a request."""


def _base_url() -> str:
    return os.getenv("LOCALTOK_BASE_URL", "http://100.107.224.7:8190").strip().rstrip("/")


def _token() -> str:
    return os.getenv("LOCALTOK_TOKEN", "").strip()


def _timeout() -> float:
    try:
        return max(3.0, float(os.getenv("LOCALTOK_TIMEOUT_SECONDS", "20")))
    except Exception:
        return 20.0


def localtok_status() -> dict[str, Any]:
    base_url = _base_url()
    token = _token()
    return {
        "configured": bool(base_url and token),
        "base_url": base_url,
        "token_configured": bool(token),
    }


def _headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    token = _token()
    if not token:
        raise LocalTokError("LocalTok 令牌未配置，请设置 LOCALTOK_TOKEN。")
    headers = {"X-Token": token}
    headers.update(extra or {})
    return headers


def _raise_for_response(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:500] if response.text else ""
        raise LocalTokError(f"LocalTok 请求失败：HTTP {response.status_code} {body}") from exc


def get_used_titles() -> list[str]:
    response = requests.get(
        f"{_base_url()}/news/used_titles",
        headers=_headers(),
        timeout=_timeout(),
    )
    _raise_for_response(response)
    data = response.json()
    titles = data.get("titles") if isinstance(data, dict) else []
    return [str(title).strip() for title in (titles or []) if str(title).strip()]


def propose_news(*, titles: list[str], summary: str, options: list[str]) -> dict[str, Any]:
    response = requests.post(
        f"{_base_url()}/news/propose",
        headers=_headers({"Content-Type": "application/json"}),
        json={
            "titles": titles,
            "summary": summary,
            "options": options,
        },
        timeout=_timeout(),
    )
    _raise_for_response(response)
    data = response.json()
    if not isinstance(data, dict) or not data.get("id"):
        raise LocalTokError("LocalTok 提案接口未返回有效 id。")
    return data


def get_decision(proposal_id: str | int) -> dict[str, Any]:
    response = requests.get(
        f"{_base_url()}/news/decision",
        headers=_headers(),
        params={"id": str(proposal_id)},
        timeout=_timeout(),
    )
    _raise_for_response(response)
    data = response.json()
    if not isinstance(data, dict) or not data.get("status"):
        raise LocalTokError("LocalTok 审核接口未返回有效状态。")
    return data


def publish_video(*, video_path: str | Path, name: str, title: str) -> dict[str, Any]:
    path = Path(video_path)
    if not path.exists() or not path.is_file():
        raise LocalTokError(f"发布视频不存在：{path}")
    encoded_title = quote(str(title or "").strip() or path.stem, safe="")
    safe_name = str(name or path.stem).strip() or path.stem
    with path.open("rb") as fh:
        response = requests.post(
            f"{_base_url()}/news/publish?name={quote(safe_name, safe='')}&title={encoded_title}",
            headers=_headers({"Content-Type": "application/octet-stream"}),
            data=fh,
            timeout=max(_timeout(), 120.0),
        )
    _raise_for_response(response)
    data = response.json()
    if not isinstance(data, dict) or not data.get("ok"):
        raise LocalTokError("LocalTok 发布接口未确认 ok=true。")
    return data
