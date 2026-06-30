import json
import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(override=False)


YOUTUBE_TOKEN_URL = "https://oauth2.googleapis.com/token"
YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
YOUTUBE_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_UPLOAD_URL = "https://www.googleapis.com/upload/youtube/v3/videos"
YOUTUBE_THUMBNAIL_URL = "https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube"


class YouTubePublishError(RuntimeError):
    pass


def youtube_env_config() -> dict[str, str]:
    return {
        "client_id": os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.getenv("GOOGLE_OAUTH_REDIRECT_URI", "").strip(),
        "refresh_token": os.getenv("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip() or os.getenv("YOUTUBE_REFRESH_TOKEN", "").strip(),
    }


def youtube_is_configured() -> bool:
    config = youtube_env_config()
    return bool(config["client_id"] and config["client_secret"] and config["refresh_token"])


def save_youtube_refresh_token(token_store_path: Path, refresh_token: str, meta: dict[str, Any] | None = None) -> None:
    token_store_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "refresh_token": refresh_token,
        "updated_at": time.time(),
        "meta": meta or {},
    }
    token_store_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_youtube_refresh_token(token_store_path: Path) -> str:
    env_token = youtube_env_config()["refresh_token"]
    if env_token:
        return env_token
    if not token_store_path.exists():
        return ""
    try:
        data = json.loads(token_store_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(data.get("refresh_token") or "").strip()


def exchange_youtube_code_for_tokens(code: str) -> dict[str, Any]:
    config = youtube_env_config()
    if not config["client_id"] or not config["client_secret"] or not config["redirect_uri"]:
        raise YouTubePublishError("未配置 Google OAuth client_id/client_secret/redirect_uri")
    response = requests.post(
        YOUTUBE_TOKEN_URL,
        data={
            "code": code,
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "redirect_uri": config["redirect_uri"],
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise YouTubePublishError(f"OAuth code 交换 token 失败：{response.status_code} {response.text[:500]}")
    return response.json()


def refresh_youtube_access_token(token_store_path: Path) -> str:
    config = youtube_env_config()
    refresh_token = load_youtube_refresh_token(token_store_path)
    if not config["client_id"] or not config["client_secret"] or not refresh_token:
        raise YouTubePublishError("未配置 YouTube OAuth refresh_token/client_id/client_secret")
    response = requests.post(
        YOUTUBE_TOKEN_URL,
        data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise YouTubePublishError(f"刷新 YouTube access_token 失败：{response.status_code} {response.text[:500]}")
    access_token = str(response.json().get("access_token") or "").strip()
    if not access_token:
        raise YouTubePublishError("刷新 YouTube access_token 失败：响应缺少 access_token")
    return access_token


def get_youtube_channel(token_store_path: Path) -> dict[str, Any]:
    access_token = refresh_youtube_access_token(token_store_path)
    response = requests.get(
        YOUTUBE_CHANNELS_URL,
        params={"part": "snippet,contentDetails,status", "mine": "true"},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise YouTubePublishError(f"读取 YouTube 频道失败：{response.status_code} {response.text[:500]}")
    data = response.json()
    items = data.get("items") or []
    if not items:
        raise YouTubePublishError("当前授权账号没有可用 YouTube 频道")
    item = items[0]
    return {
        "channel_id": item.get("id", ""),
        "title": ((item.get("snippet") or {}).get("title") or ""),
        "uploads_playlist": (((item.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads") or ""),
        "privacy_status": ((item.get("status") or {}).get("privacyStatus") or ""),
        "long_uploads_status": ((item.get("status") or {}).get("longUploadsStatus") or ""),
        "raw": item,
    }


def get_youtube_video_metrics(token_store_path: Path, video_id: str) -> dict[str, Any]:
    video_id = str(video_id or "").strip()
    if not video_id:
        raise YouTubePublishError("读取 YouTube 视频数据失败：缺少 video_id")
    access_token = refresh_youtube_access_token(token_store_path)
    response = requests.get(
        YOUTUBE_VIDEOS_URL,
        params={"part": "snippet,statistics,status", "id": video_id},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise YouTubePublishError(f"读取 YouTube 视频数据失败：{response.status_code} {response.text[:500]}")
    items = (response.json().get("items") or [])
    if not items:
        raise YouTubePublishError("读取 YouTube 视频数据失败：未找到对应视频")
    item = items[0]
    statistics = item.get("statistics") or {}
    snippet = item.get("snippet") or {}
    status = item.get("status") or {}
    return {
        "platform": "youtube",
        "video_id": video_id,
        "title": str(snippet.get("title") or "").strip(),
        "published_at": str(snippet.get("publishedAt") or "").strip(),
        "privacy_status": str(status.get("privacyStatus") or "").strip(),
        "view_count": int(statistics.get("viewCount") or 0),
        "like_count": int(statistics.get("likeCount") or 0),
        "comment_count": int(statistics.get("commentCount") or 0),
        "favorite_count": int(statistics.get("favoriteCount") or 0),
        "raw": item,
    }


def _clean_tags(tags: Any) -> list[str]:
    if isinstance(tags, str):
        values = [part.strip() for part in tags.replace("，", ",").split(",")]
    elif isinstance(tags, list):
        values = [str(item or "").strip() for item in tags]
    else:
        values = []
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item[:100])
        if len(result) >= 20:
            break
    return result


def upload_video_to_youtube(
    token_store_path: Path,
    video_path: Path,
    *,
    title: str,
    description: str = "",
    tags: Any = None,
    privacy_status: str = "unlisted",
    category_id: str = "25",
    made_for_kids: bool = False,
    publish_at: str = "",
    thumbnail_path: Path | None = None,
) -> dict[str, Any]:
    if not video_path.exists() or not video_path.is_file():
        raise YouTubePublishError("要上传的 mp4 文件不存在")
    suffix = video_path.suffix.lower()
    if suffix != ".mp4":
        raise YouTubePublishError("YouTube 上传第一阶段仅支持 mp4 成片")
    access_token = refresh_youtube_access_token(token_store_path)
    privacy_status = privacy_status if privacy_status in {"private", "unlisted", "public"} else "unlisted"
    status: dict[str, Any] = {
        "privacyStatus": privacy_status,
        "selfDeclaredMadeForKids": bool(made_for_kids),
    }
    if publish_at and privacy_status == "private":
        status["publishAt"] = publish_at
    body = {
        "snippet": {
            "title": (title or video_path.stem)[:100],
            "description": description or "",
            "tags": _clean_tags(tags),
            "categoryId": str(category_id or "25"),
        },
        "status": status,
    }
    init_response = requests.post(
        YOUTUBE_UPLOAD_URL,
        params={"part": "snippet,status", "uploadType": "resumable"},
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": "video/mp4",
            "X-Upload-Content-Length": str(video_path.stat().st_size),
        },
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        timeout=30,
    )
    if init_response.status_code >= 400:
        raise YouTubePublishError(f"YouTube 初始化上传失败：{init_response.status_code} {init_response.text[:800]}")
    upload_url = init_response.headers.get("Location")
    if not upload_url:
        raise YouTubePublishError("YouTube 初始化上传失败：响应缺少 resumable upload URL")
    with video_path.open("rb") as video_file:
        upload_response = requests.put(
            upload_url,
            headers={"Content-Type": "video/mp4"},
            data=video_file,
            timeout=3600,
        )
    if upload_response.status_code >= 400:
        raise YouTubePublishError(f"YouTube 上传视频失败：{upload_response.status_code} {upload_response.text[:800]}")
    result = upload_response.json()
    video_id = str(result.get("id") or "").strip()
    if not video_id:
        raise YouTubePublishError("YouTube 上传完成但响应缺少 video id")
    thumbnail_result: dict[str, Any] = {}
    if thumbnail_path:
        thumbnail_path = Path(thumbnail_path)
        if thumbnail_path.exists() and thumbnail_path.is_file():
            thumbnail_result = set_youtube_thumbnail(token_store_path, video_id, thumbnail_path, access_token=access_token)
    return {
        "video_id": video_id,
        "youtube_url": f"https://www.youtube.com/watch?v={video_id}",
        "privacy_status": privacy_status,
        "title": body["snippet"]["title"],
        "thumbnail": thumbnail_result,
        "raw": result,
    }


def set_youtube_thumbnail(
    token_store_path: Path,
    video_id: str,
    thumbnail_path: Path,
    *,
    access_token: str = "",
) -> dict[str, Any]:
    video_id = str(video_id or "").strip()
    thumbnail_path = Path(thumbnail_path)
    if not video_id:
        return {"ok": False, "error": "YouTube 封面上传失败：缺少 video_id"}
    if not thumbnail_path.exists() or not thumbnail_path.is_file():
        return {"ok": False, "error": f"YouTube 封面上传失败：封面文件不存在 {thumbnail_path}"}
    access_token = access_token or refresh_youtube_access_token(token_store_path)
    content_type = "image/png" if thumbnail_path.suffix.lower() == ".png" else "image/jpeg"
    with thumbnail_path.open("rb") as image_file:
        thumb_response = requests.post(
            YOUTUBE_THUMBNAIL_URL,
            params={"videoId": video_id},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": content_type,
            },
            data=image_file,
            timeout=120,
        )
    if thumb_response.status_code >= 400:
        return {
            "ok": False,
            "status_code": thumb_response.status_code,
            "error": f"YouTube 封面上传失败：{thumb_response.status_code} {thumb_response.text[:500]}",
        }
    return {"ok": True, "raw": thumb_response.json()}
