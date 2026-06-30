import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv(override=False)


FACEBOOK_GRAPH_VERSION = os.getenv("FACEBOOK_GRAPH_VERSION", "v23.0").strip() or "v23.0"
FACEBOOK_OAUTH_AUTHORIZE_URL = os.getenv(
    "FACEBOOK_OAUTH_AUTHORIZE_URL",
    f"https://www.facebook.com/{FACEBOOK_GRAPH_VERSION}/dialog/oauth",
).strip()
FACEBOOK_OAUTH_TOKEN_URL = os.getenv(
    "FACEBOOK_OAUTH_TOKEN_URL",
    f"https://graph.facebook.com/{FACEBOOK_GRAPH_VERSION}/oauth/access_token",
).strip()
FACEBOOK_GRAPH_BASE_URL = os.getenv(
    "FACEBOOK_GRAPH_BASE_URL",
    f"https://graph.facebook.com/{FACEBOOK_GRAPH_VERSION}",
).strip().rstrip("/")
FACEBOOK_SCOPE = "pages_show_list,pages_read_engagement,pages_manage_posts"


class FacebookPublishError(RuntimeError):
    pass


def facebook_env_config() -> dict[str, str]:
    return {
        "app_id": os.getenv("FACEBOOK_APP_ID", "").strip(),
        "app_secret": os.getenv("FACEBOOK_APP_SECRET", "").strip(),
        "redirect_uri": os.getenv("FACEBOOK_REDIRECT_URI", "").strip() or os.getenv("FACEBOOK_OAUTH_REDIRECT_URI", "").strip(),
        "page_id": os.getenv("FACEBOOK_PAGE_ID", "").strip(),
        "user_access_token": os.getenv("FACEBOOK_USER_ACCESS_TOKEN", "").strip(),
        "page_access_token": os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "").strip(),
    }


def build_facebook_authorization_url(*, state: str, scope: str = FACEBOOK_SCOPE) -> str:
    config = facebook_env_config()
    if not config["app_id"] or not config["redirect_uri"]:
        raise FacebookPublishError("未配置 FACEBOOK_APP_ID / FACEBOOK_REDIRECT_URI")
    params = {
        "client_id": config["app_id"],
        "redirect_uri": config["redirect_uri"],
        "state": state,
        "scope": scope,
        "response_type": "code",
    }
    query = "&".join(f"{key}={quote(str(value), safe='')}" for key, value in params.items())
    return f"{FACEBOOK_OAUTH_AUTHORIZE_URL}?{query}"


def _graph_get(path: str, *, params: dict[str, Any] | None = None, access_token: str = "", timeout: int = 30) -> dict[str, Any]:
    token = str(access_token or "").strip()
    if not token:
        raise FacebookPublishError("缺少 Facebook access_token")
    payload = dict(params or {})
    payload["access_token"] = token
    response = requests.get(f"{FACEBOOK_GRAPH_BASE_URL}/{path.lstrip('/')}", params=payload, timeout=timeout)
    if response.status_code >= 400:
        raise FacebookPublishError(f"Facebook Graph 请求失败：{response.status_code} {response.text[:800]}")
    return response.json()


def load_facebook_token_store(token_store_path: Path) -> dict[str, Any]:
    if not token_store_path.exists():
        return {}
    try:
        payload = json.loads(token_store_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_facebook_tokens(token_store_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    token_store_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_facebook_token_store(token_store_path)
    merged = {
        "user_access_token": str(payload.get("user_access_token") or existing.get("user_access_token") or "").strip(),
        "user_token_expires_at": float(payload.get("user_token_expires_at") or existing.get("user_token_expires_at") or 0),
        "page_id": str(payload.get("page_id") or existing.get("page_id") or "").strip(),
        "page_name": str(payload.get("page_name") or existing.get("page_name") or "").strip(),
        "page_access_token": str(payload.get("page_access_token") or existing.get("page_access_token") or "").strip(),
        "scopes": payload.get("scopes") or existing.get("scopes") or [],
        "user": payload.get("user") or existing.get("user") or {},
        "pages": payload.get("pages") or existing.get("pages") or [],
        "updated_at": time.time(),
        "meta": payload.get("meta") or existing.get("meta") or {},
    }
    tmp_path = token_store_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(token_store_path)
    return merged


def exchange_facebook_code_for_tokens(code: str) -> dict[str, Any]:
    config = facebook_env_config()
    if not config["app_id"] or not config["app_secret"] or not config["redirect_uri"]:
        raise FacebookPublishError("未配置 FACEBOOK_APP_ID / FACEBOOK_APP_SECRET / FACEBOOK_REDIRECT_URI")
    response = requests.get(
        FACEBOOK_OAUTH_TOKEN_URL,
        params={
            "client_id": config["app_id"],
            "client_secret": config["app_secret"],
            "redirect_uri": config["redirect_uri"],
            "code": code,
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise FacebookPublishError(f"Facebook OAuth code 交换 token 失败：{response.status_code} {response.text[:800]}")
    payload = response.json()
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise FacebookPublishError("Facebook OAuth 响应缺少 access_token")
    expires_in = float(payload.get("expires_in") or 0)
    return {
        "access_token": access_token,
        "expires_at": time.time() + expires_in if expires_in > 0 else 0.0,
        "raw": payload,
    }


def exchange_facebook_long_lived_user_token(user_access_token: str) -> dict[str, Any]:
    config = facebook_env_config()
    if not config["app_id"] or not config["app_secret"]:
        raise FacebookPublishError("未配置 FACEBOOK_APP_ID / FACEBOOK_APP_SECRET")
    response = requests.get(
        FACEBOOK_OAUTH_TOKEN_URL,
        params={
            "grant_type": "fb_exchange_token",
            "client_id": config["app_id"],
            "client_secret": config["app_secret"],
            "fb_exchange_token": user_access_token,
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise FacebookPublishError(f"获取 Facebook 长期 user token 失败：{response.status_code} {response.text[:800]}")
    payload = response.json()
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise FacebookPublishError("Facebook 长期 user token 响应缺少 access_token")
    expires_in = float(payload.get("expires_in") or 0)
    return {
        "access_token": access_token,
        "expires_at": time.time() + expires_in if expires_in > 0 else 0.0,
        "raw": payload,
    }


def get_facebook_user(user_access_token: str) -> dict[str, Any]:
    payload = _graph_get("me", params={"fields": "id,name"}, access_token=user_access_token)
    return {
        "id": str(payload.get("id") or "").strip(),
        "name": str(payload.get("name") or "").strip(),
        "raw": payload,
    }


def get_facebook_pages(user_access_token: str) -> list[dict[str, Any]]:
    payload = _graph_get("me/accounts", params={"fields": "id,name,access_token,category,tasks"}, access_token=user_access_token)
    items = payload.get("data") or []
    if not isinstance(items, list):
        items = []
    pages: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        pages.append(
            {
                "id": str(item.get("id") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "access_token": str(item.get("access_token") or "").strip(),
                "category": str(item.get("category") or "").strip(),
                "tasks": list(item.get("tasks") or []),
                "raw": item,
            }
        )
    return pages


def _select_facebook_page(pages: list[dict[str, Any]], preferred_page_id: str = "") -> dict[str, Any]:
    preferred_page_id = str(preferred_page_id or "").strip()
    if preferred_page_id:
        for page in pages:
            if str(page.get("id") or "").strip() == preferred_page_id:
                return page
    if not pages:
        raise FacebookPublishError("当前授权账号没有可管理的 Facebook Page")
    return pages[0]


def save_facebook_authorization(
    token_store_path: Path,
    *,
    user_access_token: str,
    user_token_expires_at: float = 0.0,
    preferred_page_id: str = "",
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_info = get_facebook_user(user_access_token)
    pages = get_facebook_pages(user_access_token)
    selected_page = _select_facebook_page(pages, preferred_page_id or facebook_env_config().get("page_id", ""))
    page_token = str(selected_page.get("access_token") or "").strip()
    if not page_token:
        raise FacebookPublishError("Facebook Page 响应缺少 page access token")
    return save_facebook_tokens(
        token_store_path,
        {
            "user_access_token": user_access_token,
            "user_token_expires_at": user_token_expires_at,
            "page_id": selected_page.get("id") or "",
            "page_name": selected_page.get("name") or "",
            "page_access_token": page_token,
            "user": user_info,
            "pages": pages,
            "meta": meta or {},
        },
    )


def _load_page_config(token_store_path: Path) -> tuple[str, str]:
    config = facebook_env_config()
    page_id = config["page_id"]
    page_access_token = config["page_access_token"]
    if page_id and page_access_token:
        return page_id, page_access_token
    stored = load_facebook_token_store(token_store_path)
    page_id = str(stored.get("page_id") or "").strip()
    page_access_token = str(stored.get("page_access_token") or "").strip()
    if not page_id or not page_access_token:
        raise FacebookPublishError("未配置或未授权 Facebook Page，请先完成 /api/facebook/oauth/start")
    return page_id, page_access_token


def get_facebook_page(token_store_path: Path) -> dict[str, Any]:
    page_id, page_access_token = _load_page_config(token_store_path)
    payload = _graph_get(page_id, params={"fields": "id,name,link,category,followers_count"}, access_token=page_access_token)
    return {
        "id": str(payload.get("id") or page_id).strip(),
        "name": str(payload.get("name") or "").strip(),
        "link": str(payload.get("link") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "followers_count": payload.get("followers_count"),
        "raw": payload,
    }


def get_facebook_video_metrics(token_store_path: Path, video_id: str) -> dict[str, Any]:
    video_id = str(video_id or "").strip()
    if not video_id:
        raise FacebookPublishError("读取 Facebook 视频数据失败：缺少 video_id")
    _, page_access_token = _load_page_config(token_store_path)
    payload = _graph_get(
        video_id,
        params={"fields": "id,title,description,created_time,length,permalink_url,views"},
        access_token=page_access_token,
    )
    like_count = None
    comment_count = None
    try:
        reactions_payload = _graph_get(
            f"{video_id}/reactions",
            params={"summary": "true", "limit": 0},
            access_token=page_access_token,
        )
        like_count = int((((reactions_payload.get("summary") or {}).get("total_count")) or 0))
    except Exception:
        like_count = None
    try:
        comments_payload = _graph_get(
            f"{video_id}/comments",
            params={"summary": "true", "limit": 0},
            access_token=page_access_token,
        )
        comment_count = int((((comments_payload.get("summary") or {}).get("total_count")) or 0))
    except Exception:
        comment_count = None
    return {
        "platform": "facebook",
        "video_id": str(payload.get("id") or video_id).strip(),
        "title": str(payload.get("title") or "").strip(),
        "description": str(payload.get("description") or "").strip(),
        "created_at": str(payload.get("created_time") or "").strip(),
        "permalink_url": str(payload.get("permalink_url") or "").strip(),
        "length": payload.get("length"),
        "view_count": int(payload.get("views") or 0),
        "like_count": like_count,
        "comment_count": comment_count,
        "raw": payload,
    }


def upload_video_to_facebook_page(
    token_store_path: Path,
    video_path: Path,
    *,
    description: str,
    title: str = "",
) -> dict[str, Any]:
    video_path = Path(video_path)
    if not video_path.exists() or not video_path.is_file():
        raise FacebookPublishError("要上传到 Facebook 的 mp4 文件不存在")
    if video_path.suffix.lower() != ".mp4":
        raise FacebookPublishError("Facebook 自动发布目前仅支持 mp4 成片")
    page_id, page_access_token = _load_page_config(token_store_path)
    data = {
        "access_token": page_access_token,
        "description": str(description or "").strip(),
    }
    if title:
        data["title"] = str(title or "").strip()[:255]
    with video_path.open("rb") as video_file:
        response = requests.post(
            f"{FACEBOOK_GRAPH_BASE_URL}/{quote(page_id, safe='')}/videos",
            data=data,
            files={"source": (video_path.name, video_file, "video/mp4")},
            timeout=3600,
        )
    if response.status_code >= 400:
        raise FacebookPublishError(f"Facebook Page 视频上传失败：{response.status_code} {response.text[:800]}")
    payload = response.json()
    video_id = str(payload.get("id") or "").strip()
    if not video_id:
        raise FacebookPublishError("Facebook 视频上传成功但响应缺少 video id")
    page_url = f"https://www.facebook.com/{page_id}/videos/{video_id}"
    return {
        "video_id": video_id,
        "facebook_url": page_url,
        "description": str(description or "").strip(),
        "title": str(title or "").strip(),
        "raw": payload,
    }
