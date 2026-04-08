"""
iHouse 视频自动化生产系统 - Web 应用
FastAPI + SSE 实时进度推送
"""

import csv
import json
import os
import threading
import requests
import shutil
import time
import uuid
import zipfile
from functools import wraps
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.sessions import SessionMiddleware

load_dotenv(override=False)

app = FastAPI(title="iHouse 内容工作台")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "ihouse-content-studio-session"), max_age=60 * 60 * 24 * 30, same_site="lax")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

tasks = {}
ASSETS_DIR = BASE_DIR / "assets"
ASSETS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

VOICE_PRESETS = [
    {
        "id": "mandarin_male",
        "name": "沉稳男声",
        "subtitle": "中文普通话",
        "gender": "male",
        "language": "zh-CN",
        "style": "适合资讯解读、房产科普、专业解说",
        "voice_id": os.getenv("VOICE_MANDARIN_MALE", "Chinese (Mandarin)_Warm_Bestie"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["男声", "普通话", "沉稳"],
        "sample_text": "大家好，今天带你快速看懂这个选题最重要的关键信息。",
    },
    {
        "id": "mandarin_female",
        "name": "温润女声",
        "subtitle": "中文普通话",
        "gender": "female",
        "language": "zh-CN",
        "style": "适合品牌表达、生活方式、轻讲解内容",
        "voice_id": os.getenv("VOICE_MANDARIN_FEMALE", "Chinese (Mandarin)_Warm_Bestie"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["女声", "普通话", "温润"],
        "sample_text": "大家好，欢迎你用更轻松的方式了解这次的话题重点。",
    },
    {
        "id": "taiwan_female",
        "name": "甜美女声",
        "subtitle": "中文台湾语",
        "gender": "female",
        "language": "zh-TW",
        "style": "适合面向台湾用户的生活资讯、移居内容、服务介绍",
        "voice_id": os.getenv("VOICE_TAIWAN_FEMALE", "Chinese (Mandarin)_Warm_Bestie"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["女声", "台湾", "亲切"],
        "sample_text": "嗨，今天想用更贴近生活的方式，陪你快速看懂这个主题。",
    },
    {
        "id": "japanese_female",
        "name": "自然日语女声",
        "subtitle": "日语",
        "gender": "female",
        "language": "ja-JP",
        "style": "适合日本生活、置业资讯、服务介绍等内容",
        "voice_id": os.getenv("VOICE_JAPANESE_FEMALE", "Chinese (Mandarin)_Warm_Bestie"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["女声", "日语", "自然"],
        "sample_text": "こんにちは。今日はこのテーマを、わかりやすく短く整理してご紹介します。",
    },
]


INTERFACE_LANGUAGES = [
    {"id": "zh-CN", "name": "简体中文"},
    {"id": "zh-TW", "name": "繁體中文"},
    {"id": "ja-JP", "name": "日本語"},
]

DEPARTMENTS = [
    {"id": "real_estate", "name": "房地产"},
    {"id": "robotics", "name": "机器人"},
]

TARGET_MARKETS = [
    {"id": "cn", "name": "中国市场", "content_language": "简体中文", "default_voice_preset_id": "mandarin_female"},
    {"id": "tw", "name": "台湾市场", "content_language": "繁體中文", "default_voice_preset_id": "taiwan_female"},
    {"id": "jp", "name": "日本市场", "content_language": "日语", "default_voice_preset_id": "japanese_female"},
]

COMPOSITION_TRANSITIONS = [
    {"id": "none", "name": "直接切换"},
    {"id": "fade", "name": "柔和淡入"},
]

SUBTITLE_TEMPLATES = [
    {"id": "classic", "name": "经典字幕"},
    {"id": "minimal", "name": "极简字幕"},
    {"id": "bold", "name": "强化字幕"},
]

USERS = {
    "admin": {
        "password": "admin123",
        "role": "admin",
        "display_name": "管理员",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "zhong": {
        "password": "zhong123",
        "role": "user",
        "display_name": "zhong",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "tai": {
        "password": "tai123",
        "role": "user",
        "display_name": "tai",
        "interface_language": "zh-TW",
        "department_id": "real_estate",
        "target_market": "tw",
    },
    "ri": {
        "password": "ri123",
        "role": "user",
        "display_name": "ri",
        "interface_language": "ja-JP",
        "department_id": "robotics",
        "target_market": "jp",
    },
}

AVATAR_STYLE_PROMPTS = [
    "人物面向镜头自然讲述，表情亲和，口型清晰，动作克制但真实，轻微点头和手势配合内容节奏",
    "人物以温柔自然的情绪面对镜头，表情轻松，动作柔和，镜头稳定，整体适合生活方式和服务介绍场景",
    "人物自然礼貌地对镜头讲述，表情克制细腻，动作简洁，节奏平稳，适合日语解说场景",
]


class ProgressTracker:
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.messages = []
        self.step = 0
        self.total_steps = 4
        self.status = "running"
        self.result = None


def _public_user(username: str, profile: dict) -> dict:
    return {
        "username": username,
        "role": profile.get("role", "user"),
        "display_name": profile.get("display_name", username),
        "interface_language": profile.get("interface_language", "zh-CN"),
        "department_id": profile.get("department_id", "real_estate"),
        "target_market": profile.get("target_market", "cn"),
    }


def _get_current_user(request: Request) -> Optional[dict]:
    username = request.session.get("username")
    if not username:
        return None
    profile = USERS.get(username)
    if not profile:
        request.session.pop("username", None)
        return None
    return _public_user(username, profile)


def _auth_error(message: str = "请先登录") -> JSONResponse:
    return JSONResponse({"error": message}, status_code=401)


def _forbidden_error(message: str = "没有权限访问该内容") -> JSONResponse:
    return JSONResponse({"error": message}, status_code=403)


def _is_admin(user: Optional[dict]) -> bool:
    return bool(user and user.get("role") == "admin")


def _user_can_access_task(user: Optional[dict], task: Optional[dict]) -> bool:
    if not user or not task:
        return False
    if _is_admin(user):
        return True
    return task.get("owner_username") == user.get("username")


def _owner_summary(result: dict) -> dict:
    return {
        "owner_username": result.get("owner_username"),
        "owner_display_name": result.get("owner_display_name") or result.get("owner_username") or "",
        "owner_role": result.get("owner_role", "user"),
    }


def _history_visible_to_user(result: dict, user: Optional[dict]) -> bool:
    if not user:
        return False
    if _is_admin(user):
        return True
    owner_username = result.get("owner_username")
    return bool(owner_username) and owner_username == user.get("username")


def _attach_owner_metadata(payload: dict, user: Optional[dict]) -> dict:
    if not user:
        return payload
    payload["owner_username"] = user.get("username")
    payload["owner_display_name"] = user.get("display_name")
    payload["owner_role"] = user.get("role")
    return payload


def _require_user(request: Request) -> tuple[Optional[dict], Optional[JSONResponse]]:
    user = _get_current_user(request)
    if not user:
        return None, _auth_error()
    return user, None


def _resolve_history_for_user(history_id: str, user: Optional[dict]) -> tuple[Optional[Path], Optional[dict], Optional[JSONResponse]]:
    output_dir = _resolve_history_output_dir(history_id)
    if not output_dir:
        return None, None, JSONResponse({"error": "历史任务不存在"}, status_code=404)
    result = _load_result_from_output_dir(output_dir)
    if not result:
        return None, None, JSONResponse({"error": "历史结果不存在"}, status_code=404)
    if not _history_visible_to_user(result, user):
        return None, None, _forbidden_error()
    return output_dir, result, None


def _build_admin_stats() -> dict:
    summaries = {
        username: {
            "username": username,
            "display_name": profile.get("display_name", username),
            "role": profile.get("role", "user"),
            "count": 0,
            "histories": [],
        }
        for username, profile in USERS.items()
    }
    unassigned = []
    for item in _list_history_items(None, include_all=True):
        owner_username = item.get("owner_username")
        if owner_username and owner_username in summaries:
            summaries[owner_username]["count"] += 1
            summaries[owner_username]["histories"].append(item)
        else:
            unassigned.append(item)
    return {
        "users": sorted(summaries.values(), key=lambda row: (-row.get("count", 0), row.get("username", ""))),
        "unassigned": unassigned,
        "total_count": sum(row.get("count", 0) for row in summaries.values()) + len(unassigned),
    }

    def log(self, message: str, step: Optional[int] = None):
        if step is not None:
            self.step = step
        self.messages.append(
            {
                "time": time.time(),
                "message": message,
                "step": self.step,
                "total_steps": self.total_steps,
            }
        )

    def finish(self, result: dict):
        self.status = "done"
        self.result = result
        self.log("全部完成！", step=self.total_steps)

    def fail(self, error: str):
        self.status = "error"
        self.log(f"出错了：{error}")


def _make_safe_name(value: str, fallback: str = "task") -> str:
    safe = "".join(c for c in value[:20] if c.isalnum() or c in "，。_-")
    return safe or fallback


def _create_output_dir(prefix: str, label: str) -> str:
    timestamp = int(time.time())
    safe_label = _make_safe_name(label, fallback=prefix)
    output_dir = str(OUTPUT_DIR / f"{timestamp}_{prefix}_{safe_label}")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _normalize_public_base_url(value: str) -> str:
    return value.rstrip("/")


def _get_public_base_url(request: Request) -> str:
    env_url = os.getenv("PUBLIC_BASE_URL")
    if env_url:
        return _normalize_public_base_url(env_url)

    forwarded_proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    forwarded_host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc))
    return _normalize_public_base_url(f"{forwarded_proto}://{forwarded_host}")


def _parse_bool_form(value: Optional[str]) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _resolve_local_file(file_path: str) -> Optional[Path]:
    if not file_path:
        return None

    path = Path(file_path)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    else:
        path = path.resolve()
    return path if path.exists() else None


def _history_id_from_output_dir(output_dir: Optional[str]) -> str:
    if not output_dir:
        return ""
    return Path(output_dir).resolve().name


def _resolve_history_output_dir(history_id: str) -> Optional[Path]:
    if not history_id:
        return None
    output_root = OUTPUT_DIR.resolve()
    candidate = (output_root / history_id).resolve()
    if not str(candidate).startswith(str(output_root)):
        return None
    if not candidate.exists() or not candidate.is_dir():
        return None
    return candidate


def _find_live_task_id_for_output_dir(output_dir: Optional[str]) -> Optional[str]:
    history_id = _history_id_from_output_dir(output_dir)
    if not history_id:
        return None
    for task_id, task in tasks.items():
        if _history_id_from_output_dir(task.get("output_dir")) == history_id:
            return task_id
    return None


def _load_result_from_output_dir(output_dir: Path) -> Optional[dict]:
    result_path = output_dir / 'result.json'
    if not result_path.exists():
        return None
    with open(result_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _build_media_item(output_dir: Optional[str], file_path: str) -> Optional[dict]:
    resolved = _resolve_local_file(file_path)
    if not resolved:
        return None

    output_root = Path(output_dir).resolve() if output_dir else None
    if output_root and str(resolved).startswith(str(output_root)):
        rel_path = resolved.relative_to(output_root).as_posix()
        history_id = output_root.name
        return {
            "name": resolved.name,
            "path": rel_path,
            "url": f"/api/history/{history_id}/download/{rel_path}",
        }

    assets_root = ASSETS_DIR.resolve()
    if str(resolved).startswith(str(assets_root)):
        rel_path = resolved.relative_to(assets_root).as_posix()
        return {
            "name": resolved.name,
            "path": rel_path,
            "url": f"/public/assets/{rel_path}",
        }

    return None


def _fetch_news_reference_links(query: str, max_items: int = 3) -> list[dict]:
    if not query:
        return []

    feed_url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query[:180])}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    )

    try:
        response = requests.get(
            feed_url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 iHouse Content Studio"},
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception:
        return []

    items = []
    for item in root.findall('.//item')[:max_items]:
        title = (item.findtext('title') or '').strip()
        url = (item.findtext('link') or '').strip()
        pub_date = (item.findtext('pubDate') or '').strip()
        source = ''
        source_node = item.find('source')
        if source_node is not None and source_node.text:
            source = source_node.text.strip()
        if not source and ' - ' in title:
            parts = title.rsplit(' - ', 1)
            if len(parts) == 2:
                title, source = parts[0].strip(), parts[1].strip()
        if not title or not url:
            continue
        items.append({
            'title': title,
            'url': url,
            'source': source,
            'published_at': pub_date,
        })
    return items


def _build_material_reference_links(topic: str, seg: dict) -> list[dict]:
    keyword = (seg.get("material_keyword") or "").strip()
    desc = (seg.get("material_desc") or "").strip()
    script = (seg.get("script") or "").strip()

    query_parts = []
    for value in (keyword, desc, script, topic.strip() if topic else ""):
        if value and value not in query_parts:
            query_parts.append(value)

    query = " ".join(query_parts[:3]).strip()
    if not query:
        return []

    return _fetch_news_reference_links(query)


def _serialize_segment(output_dir: Optional[str], topic: str, seg: dict, index: int) -> dict:
    return {
        "index": index + 1,
        "type": seg.get("type", ""),
        "start": seg.get("start", 0),
        "end": seg.get("end", 0),
        "duration": seg.get("duration", 0),
        "script": seg.get("script", ""),
        "action": seg.get("action", ""),
        "material_keyword": seg.get("material_keyword", ""),
        "material_search_keyword": seg.get("material_search_keyword", ""),
        "material_desc": seg.get("material_desc", ""),
        "reference_links": _build_material_reference_links(topic, seg) if seg.get("type") == "material" else [],
        "audio": _build_media_item(output_dir, seg.get("audio_path")),
        "video": _build_media_item(output_dir, seg.get("video_path")),
        "materials": [
            item for item in (_build_media_item(output_dir, path) for path in seg.get("material_paths", [])) if item
        ],
    }


def _serialize_result_for_ui(output_dir: Optional[str], result: dict, topic: str = "") -> dict:
    payload = dict(result)
    payload.update(_owner_summary(result))
    history_id = _history_id_from_output_dir(output_dir)
    payload["history_id"] = history_id
    payload["live_task_id"] = _find_live_task_id_for_output_dir(output_dir)
    payload["segments"] = [
        _serialize_segment(output_dir, topic or payload.get("topic", ""), seg, index)
        for index, seg in enumerate(result.get("segments", []))
    ]
    payload["audio"] = _build_media_item(output_dir, result.get("audio_path"))
    payload["video"] = _build_media_item(output_dir, result.get("video_path"))
    payload["image"] = _build_media_item(output_dir, result.get("image_path"))
    payload["final_video"] = _build_media_item(output_dir, result.get("final_video_path"))
    payload["cover_image"] = _build_media_item(output_dir, result.get("cover_image_path"))
    payload["subtitle_file"] = _build_media_item(output_dir, result.get("subtitle_path"))
    return payload


def _get_avatar_image_path_for_task(task: dict) -> str:
    workflow_config = task.get("workflow_config", {}) or {}
    avatar_id = workflow_config.get("avatar_id")
    avatar_option = _get_avatar_option(avatar_id)
    if avatar_option and avatar_option.get("image_path"):
        return avatar_option["image_path"]
    return task.get("image_path", "")


def _get_avatar_prompt_for_task(task: dict) -> str:
    workflow_config = task.get("workflow_config", {}) or {}
    avatar_id = workflow_config.get("avatar_id")
    avatar_option = _get_avatar_option(avatar_id)
    if avatar_option:
        return avatar_option.get("style_prompt", "")
    return ""


def _persist_task_result(task: dict):
    output_dir = task.get("output_dir")
    result = task.get("result")
    if not output_dir or not result:
        return
    _attach_owner_metadata(result, {
        "username": task.get("owner_username"),
        "display_name": task.get("owner_display_name"),
        "role": task.get("owner_role", "user"),
    } if task.get("owner_username") else None)
    with open(os.path.join(output_dir, "result.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)

def _sync_live_task_result(output_dir: Optional[str], result: dict):
    live_task_id = _find_live_task_id_for_output_dir(output_dir)
    if live_task_id and live_task_id in tasks:
        tasks[live_task_id]["result"] = result


def _bundle_root_name(history_id: str, result: dict) -> str:
    base = _make_safe_name(result.get("topic") or result.get("title") or history_id, fallback="content_bundle")
    return f"{history_id}_{base}"


def _build_timeline_rows(result: dict) -> list[dict]:
    rows = []
    for index, seg in enumerate(result.get("segments", []), start=1):
        materials = []
        for material_index, material_path in enumerate(seg.get("material_paths", []) or [], start=1):
            ext = Path(material_path).suffix or ".jpg"
            materials.append(f"{index:02d}_material_{material_index:02d}{ext}")
        row = {
            "index": index,
            "type": seg.get("type", ""),
            "start": seg.get("start", 0),
            "end": seg.get("end", 0),
            "duration": seg.get("duration", 0),
            "script": seg.get("script", ""),
            "action": seg.get("action", ""),
            "material_keyword": seg.get("material_keyword", ""),
            "material_desc": seg.get("material_desc", ""),
            "audio_file": f"{index:02d}_{seg.get('type', 'segment')}.mp3" if seg.get("audio_path") else "",
            "video_file": f"{index:02d}_digital_human.mp4" if seg.get("video_path") else "",
            "material_files": "|".join(materials),
        }
        rows.append(row)
    return rows


def _build_readme_text(result: dict, history_id: str) -> str:
    lines = [
        "iHouse 剪辑交付包",
        "=" * 40,
        f"任务ID：{history_id}",
        f"选题：{result.get('topic', '')}",
        f"标题：{result.get('title', '')}",
        f"封面标题：{result.get('cover_title', '')}",
        f"总时长：{result.get('total_duration', 0)}秒",
        f"段落数量：{len(result.get('segments', []))}",
        "",
        "文件夹说明",
        "- 01_脚本：脚本与时间轴说明",
        "- 02_配音：按段落顺序命名的配音文件",
        "- 03_数字人视频：按段落顺序命名的数字人视频",
        "- 04_素材：按段落顺序命名的素材图片",
        "- 05_SNS：SNS 文案",
        "- 06_剪辑时间轴数据：timeline.csv，可直接对应剪辑软件时间轴",
        "",
        "段落顺序说明",
    ]
    for row in _build_timeline_rows(result):
        lines.append(f"第{row['index']}段 | {row['type']} | {row['start']}s-{row['end']}s | {row['duration']}s")
        if row['audio_file']:
            lines.append(f"  配音：{row['audio_file']}")
        if row['video_file']:
            lines.append(f"  数字人视频：{row['video_file']}")
        if row['material_files']:
            lines.append(f"  素材：{row['material_files']}")
        if row['action']:
            lines.append(f"  动作提示：{row['action']}")
        if row['material_keyword'] or row['material_desc']:
            lines.append(f"  素材说明：{row['material_keyword']} {row['material_desc']}")
        lines.append(f"  文案：{row['script']}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_timeline_csv_bytes(result: dict) -> bytes:
    import io
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=[
        "index", "type", "start", "end", "duration", "script", "action",
        "material_keyword", "material_desc", "audio_file", "video_file", "material_files"
    ])
    writer.writeheader()
    for row in _build_timeline_rows(result):
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8-sig")


def _build_history_bundle_zip(output_dir: Path, result: dict) -> Path:
    history_id = output_dir.name
    bundle_path = Path('/tmp') / f"{history_id}_bundle.zip"
    root = _bundle_root_name(history_id, result)
    with zipfile.ZipFile(bundle_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{root}/00_项目说明/README.txt", _build_readme_text(result, history_id))
        zf.writestr(f"{root}/06_剪辑时间轴数据/timeline.csv", _build_timeline_csv_bytes(result))

        script_json = output_dir / 'script.json'
        if script_json.exists():
            zf.write(script_json, f"{root}/01_脚本/script.json")
        script_readable = output_dir / 'script_readable.txt'
        if script_readable.exists():
            zf.write(script_readable, f"{root}/01_脚本/script_readable.txt")

        social_posts = output_dir / 'social_posts.txt'
        if social_posts.exists():
            zf.write(social_posts, f"{root}/05_SNS/social_posts.txt")

        final_video = _resolve_local_file(result.get("final_video_path"))
        if final_video and final_video.exists():
            zf.write(final_video, f"{root}/07_成片/final_video{final_video.suffix or '.mp4'}")
        cover_image = _resolve_local_file(result.get("cover_image_path"))
        if cover_image and cover_image.exists():
            zf.write(cover_image, f"{root}/07_成片/cover{cover_image.suffix or '.jpg'}")
        subtitle_file = _resolve_local_file(result.get("subtitle_path"))
        if subtitle_file and subtitle_file.exists():
            zf.write(subtitle_file, f"{root}/07_成片/timeline_subtitles{subtitle_file.suffix or '.srt'}")

        for index, seg in enumerate(result.get("segments", []), start=1):
            audio_path = _resolve_local_file(seg.get("audio_path"))
            if audio_path and audio_path.exists():
                zf.write(audio_path, f"{root}/02_配音/{index:02d}_{seg.get('type', 'segment')}.mp3")

            video_path = _resolve_local_file(seg.get("video_path"))
            if video_path and video_path.exists():
                zf.write(video_path, f"{root}/03_数字人视频/{index:02d}_digital_human{video_path.suffix or '.mp4'}")

            for material_index, material_path in enumerate(seg.get("material_paths", []) or [], start=1):
                resolved = _resolve_local_file(material_path)
                if not resolved or not resolved.exists():
                    continue
                zf.write(resolved, f"{root}/04_素材/{index:02d}_material_{material_index:02d}{resolved.suffix or '.jpg'}")
    return bundle_path


def _build_file_entries(output_dir: str) -> list[dict]:
    history_id = _history_id_from_output_dir(output_dir)
    files = []
    for root, _, filenames in os.walk(output_dir):
        for fname in filenames:
            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, output_dir)
            files.append({
                "name": fname,
                "path": rel_path,
                "size": os.path.getsize(full_path),
                "url": f"/api/history/{history_id}/download/{Path(rel_path).as_posix()}",
            })
    files.sort(key=lambda item: item["path"])
    return files


def _build_history_item_from_output_dir(output_dir: Path, current_user: Optional[dict] = None) -> Optional[dict]:
    result = _load_result_from_output_dir(output_dir)
    if not result:
        return None
    if not _history_visible_to_user(result, current_user) and current_user is not None:
        return None
    history_id = output_dir.name
    created_at = output_dir.stat().st_mtime
    try:
        created_at = float(history_id.split('_', 1)[0])
    except (ValueError, IndexError):
        pass
    return {
        "id": history_id,
        "history_id": history_id,
        "topic": result.get("topic", ""),
        "title": result.get("title", ""),
        "cover_title": result.get("cover_title", ""),
        "segment_count": len(result.get("segments", [])),
        "total_duration": result.get("total_duration", 0),
        "mode": result.get("mode", "full"),
        "created_at": created_at,
        "live_task_id": _find_live_task_id_for_output_dir(str(output_dir)),
        **_owner_summary(result),
    }


def _list_history_items(current_user: Optional[dict], include_all: bool = False) -> list[dict]:
    items = []
    if OUTPUT_DIR.exists():
        for child in OUTPUT_DIR.iterdir():
            if not child.is_dir():
                continue
            item = _build_history_item_from_output_dir(child, None if include_all else current_user)
            if item:
                items.append(item)
    items.sort(key=lambda item: item.get("created_at", 0), reverse=True)
    return items


def _guess_avatar_layout(total_images: int, index: int) -> dict:
    prompt = AVATAR_STYLE_PROMPTS[min(index, len(AVATAR_STYLE_PROMPTS) - 1)]
    return {
        "id": f"avatar_{index + 1}",
        "style_prompt": prompt,
    }


def _list_avatar_options() -> list[dict]:
    image_paths = sorted(
        path for path in ASSETS_DIR.iterdir()
        if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
    )
    options = []
    total = len(image_paths)
    for index, image_path in enumerate(image_paths):
        meta = _guess_avatar_layout(total, index)
        options.append(
            {
                "id": meta["id"],
                "image_url": f"/public/assets/{image_path.name}",
                "image_path": str(image_path),
                "style_prompt": meta["style_prompt"],
            }
        )
    return options


def _get_target_market(target_market_id: Optional[str]) -> dict:
    if target_market_id:
        for market in TARGET_MARKETS:
            if market["id"] == target_market_id:
                return dict(market)
    return dict(TARGET_MARKETS[0])


def _get_department(department_id: Optional[str]) -> dict:
    if department_id:
        for department in DEPARTMENTS:
            if department["id"] == department_id:
                return dict(department)
    return dict(DEPARTMENTS[0])


def _get_voice_preset(voice_preset_id: Optional[str], target_market_id: Optional[str] = None) -> dict:
    if voice_preset_id:
        for preset in VOICE_PRESETS:
            if preset["id"] == voice_preset_id:
                return dict(preset)
    target_market = _get_target_market(target_market_id)
    default_id = target_market.get("default_voice_preset_id")
    if default_id:
        for preset in VOICE_PRESETS:
            if preset["id"] == default_id:
                return dict(preset)
    return dict(VOICE_PRESETS[0])


def _get_avatar_option(avatar_id: Optional[str]) -> Optional[dict]:
    avatars = _list_avatar_options()
    if not avatars:
        return None
    if avatar_id:
        for avatar in avatars:
            if avatar["id"] == avatar_id:
                return avatar
    return avatars[0]


def _build_script_preview_payload(script_data: dict, topic: str = "", web_search_enabled: bool = False, target_market: str = "cn", department_id: str = "real_estate") -> dict:
    segments = []
    for index, seg in enumerate(script_data.get("segments", []), start=1):
        segments.append(
            {
                "index": index,
                "type": seg.get("type", ""),
                "start": seg.get("start", 0),
                "end": seg.get("end", 0),
                "duration": seg.get("duration", 0),
                "script": seg.get("script", ""),
                "action": seg.get("action", ""),
                "material_keyword": seg.get("material_keyword", ""),
                "material_search_keyword": seg.get("material_search_keyword", ""),
                "material_desc": seg.get("material_desc", ""),
                "reference_links": _build_material_reference_links(topic, seg) if seg.get("type") == "material" else [],
            }
        )
    return {
        "title": script_data.get("title", ""),
        "cover_title": script_data.get("cover_title", ""),
        "total_duration": script_data.get("total_duration", 0),
        "segment_count": len(segments),
        "segments": segments,
        "social_post": _get_social_post(script_data, target_market),
        "web_search_enabled": web_search_enabled,
        "target_market": target_market,
        "department_id": department_id,
    }


def _get_social_post(script_data: dict, target_market: str = "cn") -> str:
    social_post = script_data.get("social_post", "")
    if social_post:
        return social_post
    if target_market == "tw":
        return script_data.get("facebook_post", "") or script_data.get("xiaohongshu_post", "")
    if target_market == "jp":
        return script_data.get("social_post", "") or script_data.get("facebook_post", "") or script_data.get("xiaohongshu_post", "")
    return script_data.get("xiaohongshu_post", "") or script_data.get("facebook_post", "")


def _combine_prompt(avatar_prompt: str, segment_action: str) -> str:
    parts = [part.strip() for part in [avatar_prompt, segment_action] if part and part.strip()]
    return "。".join(parts)


def _save_readable_script(script_data: dict, output_path: str):
    lines = []
    lines.append(f"标题：{script_data.get('title', '')}")
    lines.append(f"封面：{script_data.get('cover_title', '')}")
    lines.append(f"总时长：{script_data.get('total_duration', 0)}秒")
    lines.append("\n" + "=" * 50)
    lines.append("【播报稿+时间轴】")
    lines.append("=" * 50)
    for seg in script_data.get("segments", []):
        seg_type = "数字人" if seg.get("type") == "digital_human" else "素材"
        lines.append(f"\n【{seg_type} | {seg.get('start', 0)}s~{seg.get('end', 0)}s】")
        lines.append(seg.get("script", ""))
        if seg.get("type") == "digital_human":
            lines.append(f"动作描述：{seg.get('action', '')}")
        else:
            lines.append(f"素材关键词：{seg.get('material_keyword', '')}")
            lines.append(f"素材说明：{seg.get('material_desc', '')}")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _save_social_posts(script_data: dict, output_path: str, target_market: str = "cn"):
    lines = []
    lines.append("=" * 50)
    lines.append("【SNS发布文案】")
    lines.append("=" * 50)
    lines.append(_get_social_post(script_data, target_market))
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def run_pipeline_with_progress(
    task_id: str,
    topic: str,
    image_path: str,
    public_base_url: str,
    script_data: Optional[dict] = None,
    voice_preset: Optional[dict] = None,
    avatar_option: Optional[dict] = None,
):
    tracker = tasks[task_id]["tracker"]

    try:
        from fetch_materials import fetch_all_materials
        from generate_audio import generate_audio
        from generate_digital_human import generate_digital_human_video
        from generate_script import generate_script
        from tos_uploader import upload_file_and_get_url

        workflow_config = tasks[task_id].get("workflow_config", {}) or {}
        target_market = workflow_config.get("target_market", "cn")
        department_id = workflow_config.get("department_id", "real_estate")
        target_market_obj = _get_target_market(target_market)
        voice_preset = dict(voice_preset or _get_voice_preset(None, target_market))
        avatar_option = avatar_option or _get_avatar_option(None)
        tts_voice = voice_preset.get("voice_id")
        tts_speed = float(voice_preset.get("selected_speed", voice_preset.get("default_speed", 1.1)))
        tts_volume = float(voice_preset.get("selected_volume", voice_preset.get("default_volume", 1.0)))
        avatar_prompt = avatar_option.get("style_prompt", "") if avatar_option else ""

        output_dir = _create_output_dir("full", topic)
        tasks[task_id]["output_dir"] = output_dir

        image_url = None
        if image_path and os.path.exists(image_path):
            image_url = upload_file_and_get_url(image_path, key_prefix="full/image")
            tracker.log("数字人主播素材已上传到 TOS")

        if script_data is None:
            tracker.log("正在生成视频文案...", step=1)
            script_data = generate_script(topic, enable_web_search=workflow_config.get("web_search_enabled", False), target_market=target_market, department_id=department_id)
        else:
            tracker.log("已加载确认后的文案脚本", step=1)

        with open(os.path.join(output_dir, "script.json"), "w", encoding="utf-8") as f:
            json.dump(script_data, f, ensure_ascii=False, indent=2)
        _save_readable_script(script_data, os.path.join(output_dir, "script_readable.txt"))
        _save_social_posts(script_data, os.path.join(output_dir, "social_posts.txt"), target_market=target_market)
        tracker.log(
            f"文案准备完成，共 {len(script_data.get('segments', []))} 段，总时长 {script_data.get('total_duration', 0)} 秒"
        )

        tracker.log("正在生成全部配音...", step=2)
        audio_segments = []
        total_segments = len(script_data.get("segments", []))
        for index, seg in enumerate(script_data.get("segments", []), start=1):
            script_text = seg.get("script", "").strip()
            if not script_text:
                continue
            tracker.log(f"配音生成中（{index}/{total_segments}）：{script_text[:28]}...")
            seg_type = seg.get("type", "")
            filename = f"segment_{index - 1:02d}_{seg_type}.mp3"
            audio_path = os.path.join(output_dir, "audio", filename)
            generate_audio(script_text, audio_path, tts_voice, speed=tts_speed, volume=tts_volume)
            seg_with_audio = seg.copy()
            seg_with_audio["audio_path"] = audio_path
            seg_with_audio["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
            seg_with_audio["target_market"] = target_market
            audio_segments.append(seg_with_audio)
        tracker.log(f"全部配音完成，共 {len(audio_segments)} 段")

        tracker.log("正在生成数字人视频...", step=3)
        if not image_url:
            tracker.log("未选择数字人主播图，跳过数字人视频生成")
            segments_with_dh = audio_segments
        else:
            results = []
            dh_segments = [seg for seg in audio_segments if seg.get("type") == "digital_human"]
            completed = 0
            for i, seg in enumerate(audio_segments):
                if seg.get("type") != "digital_human":
                    results.append(seg)
                    continue
                completed += 1
                tracker.log(f"数字人生成中（{completed}/{len(dh_segments)}）")
                video_output = os.path.join(output_dir, "digital_human", f"dh_{i:02d}.mp4")
                video_path = generate_digital_human_video(
                    image_url=image_url,
                    audio_url=seg.get("audio_url"),
                    output_path=video_output,
                    prompt=_combine_prompt(avatar_prompt, seg.get("action", "")),
                )
                seg_copy = seg.copy()
                seg_copy["video_path"] = video_path
                results.append(seg_copy)
            segments_with_dh = results
            tracker.log("数字人视频生成完成")

        tracker.log("正在匹配素材内容...", step=4)
        try:
            final_segments = fetch_all_materials(segments=segments_with_dh, output_dir=output_dir)
            tracker.log(
                f"素材匹配完成，共 {sum(1 for seg in final_segments if seg.get('material_paths'))} 组素材"
            )
        except Exception as exc:
            tracker.log(f"素材匹配失败：{exc}，已跳过该步骤")
            final_segments = segments_with_dh

        result_data = {
            "topic": topic,
            "owner_username": tasks[task_id].get("owner_username"),
            "owner_display_name": tasks[task_id].get("owner_display_name"),
            "owner_role": tasks[task_id].get("owner_role", "user"),
            "title": script_data.get("title", ""),
            "cover_title": script_data.get("cover_title", ""),
            "total_duration": script_data.get("total_duration", 0),
            "segment_count": len(final_segments),
            "script": script_data,
            "segments": final_segments,
            "social_post": _get_social_post(script_data, target_market),
            "workflow_config": {
                "voice_preset": {
                    "id": voice_preset.get("id"),
                    "name": voice_preset.get("name"),
                    "subtitle": voice_preset.get("subtitle"),
                    "selected_speed": tts_speed,
                    "selected_volume": tts_volume,
                    "language": target_market_obj.get("content_language", ""),
                },
                "web_search_enabled": tasks[task_id].get("workflow_config", {}).get("web_search_enabled", False),
                "target_market": tasks[task_id].get("workflow_config", {}).get("target_market", "cn"),
                "department_id": tasks[task_id].get("workflow_config", {}).get("department_id", "real_estate"),
                "avatar": {
                    "id": avatar_option.get("id") if avatar_option else None,
                    "image_url": avatar_option.get("image_url") if avatar_option else "",
                },
            },
        }
        tasks[task_id]["result"] = result_data
        _persist_task_result(tasks[task_id])
        tracker.finish(result_data)
    except Exception as exc:
        tracker.fail(str(exc))
        import traceback
        traceback.print_exc()


def run_avatar_test_with_progress(task_id: str, image_path: str, audio_path: str, public_base_url: str):
    tracker = tasks[task_id]["tracker"]
    try:
        from generate_digital_human import generate_digital_human_video
        from tos_uploader import upload_file_and_get_url

        output_dir = tasks[task_id]["output_dir"]
        tracker.log("正在上传图片和音频到 TOS...", step=1)
        image_url = upload_file_and_get_url(image_path, key_prefix="avatar-test/image")
        audio_url = upload_file_and_get_url(audio_path, key_prefix="avatar-test/audio")
        tracker.log("TOS 上传完成")

        tracker.log("正在合成数字人视频...", step=2)
        video_dir = os.path.join(output_dir, "digital_human")
        os.makedirs(video_dir, exist_ok=True)
        video_path = os.path.join(video_dir, "avatar_test.mp4")
        generate_digital_human_video(
            image_url=image_url,
            audio_url=audio_url,
            output_path=video_path,
            prompt="",
            output_resolution=720,
            pe_fast_mode=True,
        )
        tracker.log("数字人视频生成完成")

        result_data = {
            "mode": "avatar_test",
            "owner_username": tasks[task_id].get("owner_username"),
            "owner_display_name": tasks[task_id].get("owner_display_name"),
            "owner_role": tasks[task_id].get("owner_role", "user"),
            "title": "数字人单段测试完成",
            "cover_title": "数字人生成测试",
            "total_duration": 0,
            "segment_count": 1,
            "image_path": image_path,
            "audio_path": audio_path,
            "image_url": image_url,
            "audio_url": audio_url,
            "video_path": video_path,
            "social_post": "",
            "segments": [
                {
                    "type": "digital_human",
                    "start": 0,
                    "end": 0,
                    "duration": 0,
                    "script": "单段数字人测试",
                    "action": "",
                    "audio_path": audio_path,
                    "video_path": video_path,
                    "material_paths": [],
                }
            ],
        }
        tasks[task_id]["result"] = result_data
        _persist_task_result(tasks[task_id])
        tracker.finish(result_data)
    except Exception as exc:
        tracker.fail(str(exc))
        import traceback
        traceback.print_exc()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.post("/api/login")
async def login(username: str = Form(...), password: str = Form(...), request: Request = None):
    profile = USERS.get(username)
    if not profile or profile.get("password") != password:
        return JSONResponse({"error": "账号或密码错误"}, status_code=401)
    request.session["username"] = username
    return {"ok": True, "user": _public_user(username, profile)}


@app.post("/api/logout")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/api/me")
async def me(request: Request):
    user = _get_current_user(request)
    if not user:
        return _auth_error()
    return {"user": user}


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_admin_stats()


@app.api_route("/public/assets/{file_path:path}", methods=["GET", "HEAD"])
async def public_asset(file_path: str):
    full_path = (ASSETS_DIR / file_path).resolve()
    if not str(full_path).startswith(str(ASSETS_DIR.resolve())) or not full_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(full_path))


@app.api_route("/public/tasks/{task_id}/{file_path:path}", methods=["GET", "HEAD"])
async def public_task_file(task_id: str, file_path: str):
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    output_dir = tasks[task_id].get("output_dir")
    if not output_dir:
        return JSONResponse({"error": "输出目录不存在"}, status_code=404)
    full_path = (Path(output_dir) / file_path).resolve()
    output_root = Path(output_dir).resolve()
    if not str(full_path).startswith(str(output_root)) or not full_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(full_path))


@app.get("/api/workbench/options")
async def workbench_options(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"voice_presets": VOICE_PRESETS, "avatars": _list_avatar_options(), "interface_languages": INTERFACE_LANGUAGES, "departments": DEPARTMENTS, "target_markets": TARGET_MARKETS, "composition_transitions": COMPOSITION_TRANSITIONS, "subtitle_templates": SUBTITLE_TEMPLATES, "current_user": user}


@app.post("/api/script-preview")
async def script_preview(request: Request, topic: str = Form(...), use_web_search: str = Form("false"), target_market: str = Form("cn"), department_id: str = Form("real_estate")):
    user, error = _require_user(request)
    if error:
        return error

    from generate_script import generate_script

    web_search_enabled = _parse_bool_form(use_web_search)
    try:
        script_data = generate_script(topic, enable_web_search=web_search_enabled, target_market=target_market, department_id=department_id)
    except Exception as exc:
        return JSONResponse({"error": f"文案生成失败：{exc}"}, status_code=500)
    return {
        "topic": topic,
        "script": script_data,
        "preview": _build_script_preview_payload(script_data, topic, web_search_enabled=web_search_enabled, target_market=target_market, department_id=department_id),
    }


@app.post("/api/produce")
async def produce_video(
    request: Request,
    topic: str = Form(...),
    script_json: str = Form(...),
    voice_preset_id: str = Form(...),
    avatar_id: str = Form(...),
    speed: float = Form(1.1),
    use_web_search: str = Form("false"),
    target_market: str = Form("cn"),
    department_id: str = Form("real_estate"),
):
    user, error = _require_user(request)
    if error:
        return error

    try:
        script_data = json.loads(script_json)
    except json.JSONDecodeError:
        return JSONResponse({"error": "文案数据格式错误"}, status_code=400)

    web_search_enabled = _parse_bool_form(use_web_search)

    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    avatar_option = _get_avatar_option(avatar_id)
    if not avatar_option:
        return JSONResponse({"error": "当前还没有可用的数字人主播图片，请先上传到服务器 assets 目录"}, status_code=400)

    voice_preset["selected_speed"] = speed
    image_path = avatar_option.get("image_path", "")
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "workflow_config": {
            "voice_preset_id": voice_preset_id,
            "avatar_id": avatar_id,
            "speed": speed,
            "web_search_enabled": web_search_enabled,
            "target_market": target_market,
            "department_id": department_id,
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
        },
    }
    tracker.log("任务已创建，准备开始...")
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, image_path, tasks[task_id]["public_base_url"], script_data, voice_preset, avatar_option),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id}


@app.post("/api/generate")
async def start_generation(request: Request, topic: str = Form(...), image: Optional[UploadFile] = File(None)):
    user, error = _require_user(request)
    if error:
        return error
    task_id = str(uuid.uuid4())[:8]
    image_path = ""
    if image and image.filename:
        ext = Path(image.filename).suffix or ".jpg"
        image_path = str(ASSETS_DIR / f"anchor_{task_id}{ext}")
        with open(image_path, "wb") as f:
            f.write(await image.read())
    else:
        avatar_option = _get_avatar_option(None)
        image_path = avatar_option.get("image_path", "") if avatar_option else ""

    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
    }
    tracker.log("任务已创建，准备开始...")
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, image_path, tasks[task_id]["public_base_url"]),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id}


@app.post("/api/avatar-test")
async def start_avatar_test(request: Request, image: UploadFile = File(...), audio: UploadFile = File(...)):
    user, error = _require_user(request)
    if error:
        return error
    if not image or not image.filename:
        return JSONResponse({"error": "请上传数字人图片"}, status_code=400)
    if not audio or not audio.filename:
        return JSONResponse({"error": "请上传音频文件"}, status_code=400)

    task_id = str(uuid.uuid4())[:8]
    output_dir = _create_output_dir("avatar_test", "avatar")
    image_ext = Path(image.filename).suffix or ".jpg"
    image_path = str(ASSETS_DIR / f"avatar_test_{task_id}{image_ext}")
    with open(image_path, "wb") as f:
        f.write(await image.read())
    audio_ext = Path(audio.filename).suffix or ".mp3"
    audio_path = str(ASSETS_DIR / f"avatar_test_{task_id}{audio_ext}")
    with open(audio_path, "wb") as f:
        f.write(await audio.read())

    tracker = ProgressTracker(task_id)
    tracker.total_steps = 2
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "mode": "avatar_test",
        "topic": "数字人单段测试",
        "image_path": image_path,
        "audio_path": audio_path,
        "tracker": tracker,
        "output_dir": output_dir,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
    }
    tracker.log("测试任务已创建，准备开始...")
    thread = threading.Thread(
        target=run_avatar_test_with_progress,
        args=(task_id, image_path, audio_path, tasks[task_id]["public_base_url"]),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id}


@app.get("/api/tasks/{task_id}/progress")
async def task_progress(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)

    if not _user_can_access_task(user, tasks.get(task_id)):
        return _forbidden_error()

    async def event_generator():
        tracker = tasks[task_id]["tracker"]
        sent_count = 0
        while True:
            while sent_count < len(tracker.messages):
                msg = tracker.messages[sent_count]
                yield {
                    "event": "progress",
                    "data": json.dumps(
                        {
                            "message": msg["message"],
                            "step": msg["step"],
                            "total_steps": msg["total_steps"],
                            "status": tracker.status,
                        },
                        ensure_ascii=False,
                    ),
                }
                sent_count += 1

            if tracker.status in ("done", "error"):
                result_data = {}
                if tracker.status == "done" and tasks[task_id].get("result"):
                    r = tasks[task_id]["result"]
                    result_data = {
                        "mode": r.get("mode", tasks[task_id].get("mode", "full")),
                        "title": r.get("title", ""),
                        "cover_title": r.get("cover_title", ""),
                        "total_duration": r.get("total_duration", 0),
                        "segment_count": r.get("segment_count", 0),
                        "social_post": r.get("social_post", _get_social_post(r, tasks[task_id].get("workflow_config", {}).get("target_market", "cn"))),
                        "output_dir": tasks[task_id].get("output_dir", ""),
                    }
                yield {
                    "event": "done",
                    "data": json.dumps({"status": tracker.status, "result": result_data}, ensure_ascii=False),
                }
                break

            import asyncio

            await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())


@app.get("/api/tasks/{task_id}/result")
async def task_result(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task = tasks[task_id]
    if not _user_can_access_task(user, task):
        return _forbidden_error()
    if not task.get("result"):
        return JSONResponse({"error": "任务尚未完成"}, status_code=202)
    return _serialize_result_for_ui(task.get("output_dir"), task["result"], task.get("topic", ""))



@app.post("/api/tasks/{task_id}/segments/{segment_index}/regenerate-digital-human")
async def regenerate_digital_human_segment(task_id: str, segment_index: int, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)

    task = tasks[task_id]
    if not _user_can_access_task(user, task):
        return _forbidden_error()
    result = task.get("result")
    if not result:
        return JSONResponse({"error": "任务尚未完成"}, status_code=400)

    segments = result.get("segments", [])
    if segment_index < 1 or segment_index > len(segments):
        return JSONResponse({"error": "段落不存在"}, status_code=404)

    segment = segments[segment_index - 1]
    if segment.get("type") != "digital_human":
        return JSONResponse({"error": "只有数字人段支持重新生成"}, status_code=400)

    from generate_digital_human import generate_digital_human_video
    from tos_uploader import upload_file_and_get_url

    audio_path = segment.get("audio_path")
    if not audio_path or not os.path.exists(audio_path):
        return JSONResponse({"error": "该段缺少可用音频文件"}, status_code=400)

    image_path = _get_avatar_image_path_for_task(task)
    if not image_path or not os.path.exists(image_path):
        return JSONResponse({"error": "当前任务缺少可用的主播图片"}, status_code=400)

    image_url = upload_file_and_get_url(image_path, key_prefix="full/image")
    audio_url = segment.get("audio_url") or upload_file_and_get_url(audio_path, key_prefix="full/audio")
    segment["audio_url"] = audio_url

    output_dir = task.get("output_dir")
    if not output_dir:
        return JSONResponse({"error": "输出目录不存在"}, status_code=400)

    os.makedirs(os.path.join(output_dir, "digital_human"), exist_ok=True)
    video_output = os.path.join(
        output_dir,
        "digital_human",
        f"dh_{segment_index - 1:02d}_regen_{int(time.time())}.mp4",
    )
    video_path = generate_digital_human_video(
        image_url=image_url,
        audio_url=audio_url,
        output_path=video_output,
        prompt=_combine_prompt(_get_avatar_prompt_for_task(task), segment.get("action", "")),
    )
    segment["video_path"] = video_path
    task["result"] = result
    _persist_task_result(task)
    return {
        "message": "数字人视频已重新生成",
        "segment": _serialize_segment(task.get("output_dir"), task.get("topic", ""), segment, segment_index - 1),
        "result": _serialize_result_for_ui(task.get("output_dir"), result, task.get("topic", "")),
    }


@app.post("/api/script-preview/revise")
async def revise_script_preview_segment(
    request: Request,
    topic: str = Form(...),
    script_json: str = Form(...),
    segment_index: int = Form(...),
    instruction: str = Form(...),
    use_web_search: str = Form("false"),
    target_market: str = Form("cn"),
    department_id: str = Form("real_estate"),
):
    user, error = _require_user(request)
    if error:
        return error

    try:
        script_data = json.loads(script_json)
    except json.JSONDecodeError:
        return JSONResponse({"error": "文案数据格式错误"}, status_code=400)

    if not instruction.strip():
        return JSONResponse({"error": "请先填写修改要求"}, status_code=400)

    segments = script_data.get("segments", [])
    if segment_index < 1 or segment_index > len(segments):
        return JSONResponse({"error": "段落不存在"}, status_code=404)

    from generate_script import revise_script_segment

    web_search_enabled = _parse_bool_form(use_web_search)
    try:
        revised_segment = revise_script_segment(topic, script_data, segment_index - 1, instruction.strip(), enable_web_search=web_search_enabled, target_market=target_market, department_id=department_id)
    except Exception as exc:
        return JSONResponse({"error": f"AI 修改失败：{exc}"}, status_code=500)
    script_data["segments"][segment_index - 1] = revised_segment
    return {
        "script": script_data,
        "preview": _build_script_preview_payload(script_data, topic, web_search_enabled=web_search_enabled, target_market=target_market, department_id=department_id),
        "segment": revised_segment,
    }


@app.delete("/api/history/{history_id}/segments/{segment_index}/materials/{material_index}")
async def delete_history_material(history_id: str, segment_index: int, material_index: int, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error

    segments = result.get("segments", [])
    if segment_index < 1 or segment_index > len(segments):
        return JSONResponse({"error": "段落不存在"}, status_code=404)
    segment = segments[segment_index - 1]
    if segment.get("type") != "material":
        return JSONResponse({"error": "只有素材段支持删除素材"}, status_code=400)

    material_paths = segment.get("material_paths", []) or []
    if material_index < 0 or material_index >= len(material_paths):
        return JSONResponse({"error": "素材不存在"}, status_code=404)

    removed_path = material_paths.pop(material_index)
    resolved = _resolve_local_file(removed_path)
    if resolved and str(resolved).startswith(str(output_dir.resolve())) and resolved.exists():
        try:
            resolved.unlink()
        except OSError:
            pass

    segment["material_paths"] = material_paths
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {"result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))}


@app.post("/api/history/{history_id}/segments/{segment_index}/materials/upload")
async def upload_history_materials(history_id: str, segment_index: int, request: Request, images: list[UploadFile] = File(...)):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error

    segments = result.get("segments", [])
    if segment_index < 1 or segment_index > len(segments):
        return JSONResponse({"error": "段落不存在"}, status_code=404)
    segment = segments[segment_index - 1]
    if segment.get("type") != "material":
        return JSONResponse({"error": "只有素材段支持上传素材"}, status_code=400)

    material_dir = output_dir / "materials"
    material_dir.mkdir(parents=True, exist_ok=True)
    material_paths = segment.get("material_paths", []) or []
    for upload in images:
        if not upload.filename:
            continue
        ext = Path(upload.filename).suffix or ".jpg"
        filename = f"material_{segment_index:02d}_manual_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}{ext}"
        output_path = material_dir / filename
        with open(output_path, "wb") as f:
            f.write(await upload.read())
        material_paths.append(str(output_path))
    segment["material_paths"] = material_paths
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {"result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))}


@app.get("/api/tasks/{task_id}/files")
async def list_files(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, tasks.get(task_id)):
        return _forbidden_error()
    output_dir = tasks[task_id].get("output_dir")
    if not output_dir or not os.path.exists(output_dir):
        return JSONResponse({"error": "输出目录不存在"}, status_code=404)
    return {"files": _build_file_entries(output_dir)}


@app.get("/api/tasks/{task_id}/download/{file_path:path}")
async def download_file(task_id: str, file_path: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if task_id not in tasks:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, tasks.get(task_id)):
        return _forbidden_error()
    output_dir = tasks[task_id].get("output_dir")
    if not output_dir:
        return JSONResponse({"error": "输出目录不存在"}, status_code=404)
    full_path = os.path.join(output_dir, file_path)
    if not os.path.abspath(full_path).startswith(os.path.abspath(output_dir)):
        return JSONResponse({"error": "非法路径"}, status_code=403)
    if not os.path.exists(full_path):
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(full_path, filename=os.path.basename(full_path))


@app.get("/api/history")
async def history(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _list_history_items(user)


@app.delete("/api/history/{history_id}")
async def delete_history(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error

    live_task_id = _find_live_task_id_for_output_dir(str(output_dir))
    if live_task_id and live_task_id in tasks:
        tracker = tasks[live_task_id].get("tracker")
        if tracker and getattr(tracker, "status", "") == "running":
            return JSONResponse({"error": "任务仍在运行中，暂时无法删除"}, status_code=400)
        tasks.pop(live_task_id, None)

    try:
        shutil.rmtree(output_dir)
    except FileNotFoundError:
        return JSONResponse({"error": "历史任务不存在"}, status_code=404)
    except OSError as exc:
        return JSONResponse({"error": f"删除历史任务失败：{exc}"}, status_code=500)

    return {"ok": True, "history_id": history_id}


@app.get("/api/history/{history_id}/result")
async def history_result(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    return _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))


@app.get("/api/history/{history_id}/files")
async def history_files(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, _, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    return {"files": _build_file_entries(str(output_dir))}


@app.post("/api/history/{history_id}/compose")
async def compose_history_video_endpoint(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error

    payload = {}
    if "application/json" in (request.headers.get("content-type") or ""):
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    transition_id = str(payload.get("transition_id") or ((result.get("workflow_config") or {}).get("compose_transition_id") or "fade")).strip()
    subtitle_template_id = str(payload.get("subtitle_template_id") or ((result.get("workflow_config") or {}).get("subtitle_template_id") or "classic")).strip()
    if transition_id not in {item["id"] for item in COMPOSITION_TRANSITIONS}:
        transition_id = "fade"
    if subtitle_template_id not in {item["id"] for item in SUBTITLE_TEMPLATES}:
        subtitle_template_id = "classic"

    try:
        from video_composer import compose_history_video
        compose_result = compose_history_video(
            str(output_dir),
            result,
            transition_id=transition_id,
            subtitle_template_id=subtitle_template_id,
        )
    except Exception as exc:
        return JSONResponse({"error": f"自动成片失败：{exc}"}, status_code=500)

    workflow_config = result.get("workflow_config") or {}
    workflow_config["compose_transition_id"] = transition_id
    workflow_config["subtitle_template_id"] = subtitle_template_id
    result["workflow_config"] = workflow_config
    result.update(compose_result)
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {"ok": True, "result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))}


@app.get("/api/history/{history_id}/bundle")
async def history_bundle(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    bundle_path = _build_history_bundle_zip(output_dir, result)
    return FileResponse(str(bundle_path), filename=f"{history_id}_剪辑交付包.zip", media_type="application/zip")


@app.get("/api/history/{history_id}/download/{file_path:path}")
async def history_download(history_id: str, file_path: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, _, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    full_path = (output_dir / file_path).resolve()
    if not str(full_path).startswith(str(output_dir.resolve())):
        return JSONResponse({"error": "非法路径"}, status_code=403)
    if not full_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(full_path), filename=full_path.name)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
