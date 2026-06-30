import base64
import hashlib
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv(override=False)


X_AUTHORIZE_URL = os.getenv("X_OAUTH_AUTHORIZE_URL", "https://x.com/i/oauth2/authorize").strip()
X_TOKEN_URL = os.getenv("X_OAUTH_TOKEN_URL", "https://api.x.com/2/oauth2/token").strip()
X_USERS_ME_URL = os.getenv("X_USERS_ME_URL", "https://api.x.com/2/users/me").strip()
X_TWEETS_URL = os.getenv("X_TWEETS_URL", "https://api.x.com/2/tweets").strip()
X_TWEET_DETAIL_URL = os.getenv("X_TWEET_DETAIL_URL", "https://api.x.com/2/tweets").strip()
X_MEDIA_UPLOAD_URL = os.getenv("X_MEDIA_UPLOAD_URL", "https://api.x.com/2/media/upload").strip()
X_SCOPE = "tweet.read tweet.write users.read media.write offline.access"


class XPublishError(RuntimeError):
    pass


def x_env_config() -> dict[str, str]:
    return {
        "consumer_key": os.getenv("X_CONSUMER_KEY", "").strip(),
        "consumer_secret": os.getenv("X_CONSUMER_SECRET", "").strip(),
        "bearer_token": os.getenv("X_BEARER_TOKEN", "").strip(),
        "client_id": os.getenv("X_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("X_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.getenv("X_REDIRECT_URI", "").strip() or os.getenv("X_OAUTH_REDIRECT_URI", "").strip(),
        "access_token": os.getenv("X_ACCESS_TOKEN", "").strip(),
        "refresh_token": os.getenv("X_REFRESH_TOKEN", "").strip(),
        "token_expires_at": os.getenv("X_TOKEN_EXPIRES_AT", "").strip(),
    }


def generate_x_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)[:128]
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def build_x_authorization_url(
    *,
    state: str,
    code_challenge: str,
    scope: str = X_SCOPE,
) -> str:
    config = x_env_config()
    if not config["client_id"] or not config["redirect_uri"]:
        raise XPublishError("未配置 X_CLIENT_ID / X_REDIRECT_URI")
    params = {
        "response_type": "code",
        "client_id": config["client_id"],
        "redirect_uri": config["redirect_uri"],
        "scope": scope,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    query = "&".join(f"{key}={quote(str(value), safe='')}" for key, value in params.items())
    return f"{X_AUTHORIZE_URL}?{query}"


def _basic_auth_headers(client_id: str, client_secret: str) -> dict[str, str]:
    token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _token_request_data(grant_type: str, **extra: str) -> tuple[dict[str, str], dict[str, str]]:
    config = x_env_config()
    if not config["client_id"]:
        raise XPublishError("未配置 X_CLIENT_ID")
    data = {"grant_type": grant_type, **extra}
    if config["client_secret"]:
        headers = _basic_auth_headers(config["client_id"], config["client_secret"])
    else:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data["client_id"] = config["client_id"]
    return data, headers


def load_x_token_store(token_store_path: Path) -> dict[str, Any]:
    if not token_store_path.exists():
        return {}
    try:
        payload = json.loads(token_store_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_x_tokens(token_store_path: Path, tokens: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    token_store_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_x_token_store(token_store_path)
    access_token = str(tokens.get("access_token") or existing.get("access_token") or "").strip()
    refresh_token = str(tokens.get("refresh_token") or existing.get("refresh_token") or "").strip()
    expires_in = tokens.get("expires_in")
    try:
        expires_in_seconds = int(float(expires_in))
    except Exception:
        expires_in_seconds = 0
    expires_at = time.time() + expires_in_seconds if expires_in_seconds > 0 else float(existing.get("expires_at") or 0)
    payload = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at,
        "scope": tokens.get("scope") or existing.get("scope") or "",
        "token_type": tokens.get("token_type") or existing.get("token_type") or "bearer",
        "updated_at": time.time(),
        "meta": meta or existing.get("meta") or {},
    }
    tmp_path = token_store_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(token_store_path)
    return payload


def _env_token_expires_at() -> float:
    value = x_env_config().get("token_expires_at") or ""
    try:
        return float(value)
    except Exception:
        return 0.0


def load_x_refresh_token(token_store_path: Path) -> str:
    env_token = x_env_config()["refresh_token"]
    if env_token:
        return env_token
    return str(load_x_token_store(token_store_path).get("refresh_token") or "").strip()


def exchange_x_code_for_tokens(code: str, code_verifier: str) -> dict[str, Any]:
    config = x_env_config()
    if not config["client_id"] or not config["redirect_uri"]:
        raise XPublishError("未配置 X_CLIENT_ID / X_REDIRECT_URI")
    if not code_verifier:
        raise XPublishError("缺少 OAuth PKCE code_verifier")
    data, headers = _token_request_data(
        "authorization_code",
        code=code,
        redirect_uri=config["redirect_uri"],
        code_verifier=code_verifier,
    )
    response = requests.post(X_TOKEN_URL, data=data, headers=headers, timeout=30)
    if response.status_code >= 400:
        raise XPublishError(f"X OAuth code 交换 token 失败：{response.status_code} {response.text[:500]}")
    return response.json()


def refresh_x_access_token(token_store_path: Path) -> str:
    config = x_env_config()
    env_access_token = config["access_token"]
    env_expires_at = _env_token_expires_at()
    if env_access_token and (not env_expires_at or env_expires_at - time.time() > 120):
        return env_access_token
    store = load_x_token_store(token_store_path)
    stored_access_token = str(store.get("access_token") or "").strip()
    try:
        stored_expires_at = float(store.get("expires_at") or 0)
    except Exception:
        stored_expires_at = 0.0
    if stored_access_token and stored_expires_at - time.time() > 120:
        return stored_access_token
    refresh_token = load_x_refresh_token(token_store_path)
    if not refresh_token:
        raise XPublishError("未配置或未授权 X_REFRESH_TOKEN，请先访问 /api/x/oauth/start 完成授权")
    data, headers = _token_request_data("refresh_token", refresh_token=refresh_token)
    response = requests.post(X_TOKEN_URL, data=data, headers=headers, timeout=30)
    if response.status_code >= 400:
        raise XPublishError(f"刷新 X access_token 失败：{response.status_code} {response.text[:500]}")
    tokens = response.json()
    save_x_tokens(token_store_path, tokens)
    access_token = str(tokens.get("access_token") or "").strip()
    if not access_token:
        raise XPublishError("刷新 X access_token 失败：响应缺少 access_token")
    return access_token


def get_x_user(token_store_path: Path) -> dict[str, Any]:
    access_token = refresh_x_access_token(token_store_path)
    response = requests.get(
        X_USERS_ME_URL,
        params={"user.fields": "id,name,username,verified,profile_image_url"},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise XPublishError(f"读取 X 授权账号失败：{response.status_code} {response.text[:500]}")
    payload = response.json().get("data") or {}
    return {
        "id": payload.get("id") or "",
        "username": payload.get("username") or "",
        "name": payload.get("name") or "",
        "verified": payload.get("verified"),
        "profile_image_url": payload.get("profile_image_url") or "",
        "raw": payload,
    }


def get_x_post_metrics(token_store_path: Path, post_id: str) -> dict[str, Any]:
    post_id = str(post_id or "").strip()
    if not post_id:
        raise XPublishError("读取 X 帖子数据失败：缺少 post_id")
    access_token = refresh_x_access_token(token_store_path)
    response = requests.get(
        f"{X_TWEET_DETAIL_URL}/{quote(post_id, safe='')}",
        params={
            "tweet.fields": "created_at,public_metrics,organic_metrics,non_public_metrics,text",
            "expansions": "author_id",
        },
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise XPublishError(f"读取 X 帖子数据失败：{response.status_code} {response.text[:500]}")
    data = response.json().get("data") or {}
    if not isinstance(data, dict) or not data:
        raise XPublishError("读取 X 帖子数据失败：未找到对应帖子")
    public_metrics = data.get("public_metrics") or {}
    organic_metrics = data.get("organic_metrics") or {}
    non_public_metrics = data.get("non_public_metrics") or {}
    view_count = organic_metrics.get("impression_count")
    if view_count in (None, ""):
        view_count = non_public_metrics.get("impression_count")
    return {
        "platform": "x",
        "post_id": post_id,
        "text": str(data.get("text") or "").strip(),
        "created_at": str(data.get("created_at") or "").strip(),
        "like_count": int(public_metrics.get("like_count") or 0),
        "comment_count": int(public_metrics.get("reply_count") or 0),
        "repost_count": int(public_metrics.get("retweet_count") or 0),
        "quote_count": int(public_metrics.get("quote_count") or 0),
        "bookmark_count": int(public_metrics.get("bookmark_count") or 0),
        "view_count": int(view_count or 0) if view_count not in (None, "") else None,
        "raw": data,
    }


def _extract_media_id(payload: dict[str, Any]) -> str:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    for key in ("id", "media_id", "media_id_string"):
        value = str(data.get(key) or "").strip()
        if value:
            return value
    return ""


def _media_processing_info(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    info = data.get("processing_info") if isinstance(data, dict) else {}
    return info if isinstance(info, dict) else {}


def _poll_x_media_processing(access_token: str, media_id: str, initial_payload: dict[str, Any]) -> dict[str, Any]:
    latest = initial_payload
    info = _media_processing_info(initial_payload)
    if not info:
        return latest
    deadline = time.time() + max(60, int(os.getenv("X_MEDIA_PROCESSING_TIMEOUT_SECONDS", "900") or "900"))
    while time.time() < deadline:
        state = str(info.get("state") or "").strip().lower()
        if state in {"succeeded", "complete", "completed"}:
            return latest
        if state == "failed":
            error = info.get("error") or info
            raise XPublishError(f"X 媒体处理失败：{error}")
        try:
            wait_seconds = int(float(info.get("check_after_secs") or 5))
        except Exception:
            wait_seconds = 5
        time.sleep(max(1, min(wait_seconds, 30)))
        status_response = requests.get(
            X_MEDIA_UPLOAD_URL,
            params={"media_id": media_id},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        if status_response.status_code >= 400:
            raise XPublishError(f"查询 X 媒体处理状态失败：{status_response.status_code} {status_response.text[:500]}")
        latest = status_response.json()
        info = _media_processing_info(latest)
        if not info:
            return latest
    raise XPublishError("X 媒体处理超时，请稍后在后台检查上传状态")


def upload_video_media_to_x(token_store_path: Path, video_path: Path) -> dict[str, Any]:
    video_path = Path(video_path)
    if not video_path.exists() or not video_path.is_file():
        raise XPublishError("要上传到 X 的 mp4 文件不存在")
    if video_path.suffix.lower() != ".mp4":
        raise XPublishError("X 自动发布目前仅支持 mp4 成片")
    access_token = refresh_x_access_token(token_store_path)
    size = video_path.stat().st_size
    init_response = requests.post(
        f"{X_MEDIA_UPLOAD_URL}/initialize",
        json={
            "media_type": "video/mp4",
            "media_category": "tweet_video",
            "total_bytes": size,
        },
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        timeout=30,
    )
    if init_response.status_code >= 400:
        raise XPublishError(f"X 初始化媒体上传失败：{init_response.status_code} {init_response.text[:800]}")
    init_payload = init_response.json()
    media_id = _extract_media_id(init_payload)
    if not media_id:
        raise XPublishError("X 初始化媒体上传失败：响应缺少 media id")
    base_chunk_size = max(1024 * 1024, int(os.getenv("X_MEDIA_UPLOAD_CHUNK_SIZE", str(4 * 1024 * 1024)) or str(4 * 1024 * 1024)))
    chunk_size = max(base_chunk_size, int(size / 999) + 1)
    with video_path.open("rb") as video_file:
        segment_index = 0
        while True:
            chunk = video_file.read(chunk_size)
            if not chunk:
                break
            append_response = requests.post(
                f"{X_MEDIA_UPLOAD_URL}/{quote(media_id, safe='')}/append",
                data={"segment_index": str(segment_index)},
                files={"media": ("chunk", chunk, "application/octet-stream")},
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=300,
            )
            if append_response.status_code >= 400:
                raise XPublishError(f"X 上传媒体分片失败：{append_response.status_code} {append_response.text[:800]}")
            segment_index += 1
    finalize_response = requests.post(
        f"{X_MEDIA_UPLOAD_URL}/{quote(media_id, safe='')}/finalize",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    if finalize_response.status_code >= 400:
        raise XPublishError(f"X 完成媒体上传失败：{finalize_response.status_code} {finalize_response.text[:800]}")
    finalize_payload = finalize_response.json() if finalize_response.text.strip() else {}
    status_payload = _poll_x_media_processing(access_token, media_id, finalize_payload)
    return {
        "media_id": media_id,
        "size": size,
        "segments": segment_index,
        "init": init_payload,
        "finalize": finalize_payload,
        "status": status_payload,
    }


def create_x_post(
    token_store_path: Path,
    *,
    text: str,
    media_ids: list[str] | tuple[str, ...] | None = None,
    made_with_ai: bool = True,
) -> dict[str, Any]:
    access_token = refresh_x_access_token(token_store_path)
    body: dict[str, Any] = {"text": str(text or "").strip()[:280]}
    clean_media_ids = [str(item or "").strip() for item in (media_ids or []) if str(item or "").strip()]
    if clean_media_ids:
        body["media"] = {"media_ids": clean_media_ids}
    if made_with_ai:
        body["made_with_ai"] = True
    response = requests.post(
        X_TWEETS_URL,
        json=body,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        timeout=60,
    )
    if response.status_code >= 400:
        raise XPublishError(f"创建 X Post 失败：{response.status_code} {response.text[:800]}")
    payload = response.json()
    data = payload.get("data") or {}
    post_id = str(data.get("id") or "").strip()
    if not post_id:
        raise XPublishError("X Post 创建成功但响应缺少 post id")
    return {
        "post_id": post_id,
        "x_url": f"https://x.com/i/web/status/{post_id}",
        "text": body["text"],
        "media_ids": clean_media_ids,
        "raw": payload,
    }


def upload_video_to_x(
    token_store_path: Path,
    video_path: Path,
    *,
    text: str,
    made_with_ai: bool = True,
) -> dict[str, Any]:
    media_result = upload_video_media_to_x(token_store_path, video_path)
    media_id = str(media_result.get("media_id") or "")
    post_result = create_x_post(
        token_store_path,
        text=text,
        media_ids=[media_id] if media_id else [],
        made_with_ai=made_with_ai,
    )
    return {
        "post_id": post_result.get("post_id") or "",
        "x_url": post_result.get("x_url") or "",
        "text": post_result.get("text") or "",
        "media_id": media_id,
        "media": media_result,
        "raw": post_result.get("raw") or {},
    }
