"""
iHouse 视频自动化生产系统 - Web 应用
FastAPI + SSE 实时进度推送
"""

import csv
from collections import deque
import hashlib
import json
import os
import re
import subprocess
import threading
import requests
import shutil
import time
import uuid
import zipfile
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Optional
from urllib.parse import quote
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.sessions import SessionMiddleware

load_dotenv(override=False)

from avatar_generator import AvatarGenerationError, generate_avatar_candidates
from material_library import (
    MATERIAL_LIBRARY_DIR,
    delete_material_library_item,
    list_material_library_items,
    register_material_file,
    update_material_library_item,
)
from source_ingest import analyze_topic_fields, analyze_topic_input

app = FastAPI(title="iHouse 内容工作台")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "ihouse-content-studio-session"), max_age=60 * 60 * 24 * 30, same_site="lax")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

tasks = {}
ASSETS_DIR = BASE_DIR / "assets"
ASSETS_DIR.mkdir(exist_ok=True)
AVATAR_LIBRARY_MANIFEST_PATH = ASSETS_DIR / "avatar_library_manifest.json"
AVATAR_LIBRARY_LOCK = threading.Lock()
MATERIAL_LIBRARY_PUBLIC_DIR = MATERIAL_LIBRARY_DIR
ADMIN_AVATAR_JOBS: dict[str, dict] = {}
ADMIN_AVATAR_JOBS_LOCK = threading.Lock()

AVATAR_DISPLAY_NAME_MAP = {
    "avatar_test_0cd3d70a.png": "女主播A",
    "avatar_host_c.png": "男主播A",
    "avatar_host_d.png": "女主播C",
    "avatar_ultraman.png": "奥特曼",
    "avatar_test_new_01.png": "男主播B",
    "avatar_custom_林晨专属_male_manual.png": "男主播B",
}
AVATAR_OPTION_EXCLUDE_FILENAMES = {"ihouse-logo.webp"}
AVATAR_RULES = {
    "avatar_test_0cd3d70a.png": {
        "gender": "female",
        "allowed_target_markets": ["cn", "tw", "jp"],
        "preferred_voice_by_market": {
            "cn": "mandarin_female",
            "tw": "taiwan_clone",
            "jp": "japanese_female",
        },
    },
    "avatar_host_c.png": {
        "gender": "male",
        "allowed_target_markets": ["cn"],
        "preferred_voice_by_market": {
            "cn": "mandarin_male",
        },
    },
    "avatar_host_d.png": {
        "gender": "female",
        "allowed_target_markets": ["cn", "tw", "jp"],
        "preferred_voice_by_market": {
            "cn": "mandarin_female",
            "tw": "taiwan_clone",
            "jp": "japanese_female",
        },
    },
    "avatar_ultraman.png": {
        "gender": "male",
        "allowed_target_markets": ["cn", "tw", "jp"],
        "preferred_voice_by_market": {
            "cn": "mandarin_male",
            "tw": "taiwan_clone",
            "jp": "japanese_female",
        },
    },
    "avatar_test_new_01.png": {
        "gender": "male",
        "allowed_target_markets": ["cn"],
        "preferred_voice_by_market": {
            "cn": "mandarin_male",
        },
    },
    "avatar_custom_林晨专属_male_manual.png": {
        "gender": "male",
        "allowed_target_markets": ["cn"],
        "preferred_voice_by_market": {
            "cn": "mandarin_male",
        },
    },
}
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
        "voice_id": os.getenv("VOICE_MANDARIN_MALE", "Chinese (Mandarin)_Gentleman"),
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
        "id": "taiwan_clone",
        "name": "みん音色",
        "subtitle": "中文台湾语",
        "gender": "female",
        "language": "zh-TW",
        "style": "使用台湾同事真实声音克隆，适合台湾市场口播与生活资讯内容。",
        "voice_id": os.getenv("VOICE_TAIWAN_CLONE", ""),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["女声", "台湾", "克隆"],
        "sample_text": "嗨，今天用更自然亲切的语气，陪你快速看懂这个主题。",
        "enabled": bool(os.getenv("VOICE_TAIWAN_CLONE", "").strip()),
        "availability_note": "已启用",
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
    {"id": "tw", "name": "台湾市场", "content_language": "繁體中文", "default_voice_preset_id": "taiwan_clone"},
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
    "yi": {
        "password": "yi123",
        "role": "user",
        "display_name": "yi",
        "interface_language": "ja-JP",
        "department_id": "robotics",
        "target_market": "jp",
    },
    "mei": {
        "password": "mei123",
        "role": "user",
        "display_name": "mei",
        "interface_language": "ja-JP",
        "department_id": "robotics",
        "target_market": "jp",
    },
    "da": {
        "password": "da123",
        "role": "user",
        "display_name": "da",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
}

OMNIHUMAN_MAX_CONCURRENT = max(1, int(os.getenv("OMNIHUMAN_MAX_CONCURRENT", "1")))
OMNIHUMAN_QUEUE_CONDITION = threading.Condition()
OMNIHUMAN_WAITING_JOBS: list[dict] = []
OMNIHUMAN_RUNNING_JOBS = 0
OMNIHUMAN_RUNNING_ITEMS: list[dict] = []
HUNYUAN_ENGINE_ID = "hunyuan_local"
VOLC_ENGINE_ID = "volc_omnihuman"
SCRIPT_MODEL_CLAUDE = "claude"
SCRIPT_MODEL_GLM = "glm_5_1"
SCRIPT_MODEL_CHATGPT = "chatgpt"
DIGITAL_HUMAN_ENGINES = [
    {
        "id": VOLC_ENGINE_ID,
        "name": "火山 OmniHuman",
        "description": "现有生产默认，速度较快，稳定性更成熟。",
        "admin_only": False,
        "default": True,
    },
    {
        "id": HUNYUAN_ENGINE_ID,
        "name": "5090 本地 HunyuanVideo-Avatar",
        "description": "测试功能：672 + 20 steps，口型更好但速度很慢。",
        "admin_only": True,
        "default": False,
    },
]
SCRIPT_MODEL_OPTIONS = [
    {
        "id": SCRIPT_MODEL_CLAUDE,
        "name": "Claude",
        "description": "当前默认，支持实时联网检索，适合最新资讯与结构化脚本生成。",
        "admin_only": False,
        "default": True,
    },
    {
        "id": SCRIPT_MODEL_GLM,
        "name": "GLM-5.1",
        "description": "智谱旗舰模型，中文写作和结构化输出更强，适合内容文案测试。",
        "admin_only": True,
        "default": False,
    },
    {
        "id": SCRIPT_MODEL_CHATGPT,
        "name": "ChatGPT",
        "description": "OpenAI GPT-5 mini 路线，作为管理员可选文案模型保留。",
        "admin_only": True,
        "default": False,
    },
]
SCRIPT_AI_MAX_CONCURRENT = max(1, int(os.getenv("SCRIPT_AI_MAX_CONCURRENT", "1")))
SCRIPT_AI_QUEUE_CONDITION = threading.Condition()
SCRIPT_AI_WAITING_JOBS: list[dict] = []
SCRIPT_AI_RUNNING_JOBS = 0
SCRIPT_AI_RUNNING_ITEMS: list[dict] = []
LIVE_EVENTS = deque(maxlen=120)
COST_LEDGER_PATH = OUTPUT_DIR / "_cost_ledger.json"
COST_LEDGER_LOCK = threading.Lock()
COST_CURRENCY = "USD"
FX_CNY_PER_USD = float(os.getenv("FX_CNY_PER_USD", "7.2"))
COST_RULES = {
    "script_generate": {"provider": "anthropic", "base": 0.006, "per_char": 0.000004, "web_search": 0.010, "input_token_rate": 0.000003, "output_token_rate": 0.000015, "cache_creation_token_rate": 0.00000375, "cache_read_token_rate": 0.0000003},
    "script_revise": {"provider": "anthropic", "base": 0.003, "per_char": 0.000003, "web_search": 0.010, "input_token_rate": 0.000003, "output_token_rate": 0.000015, "cache_creation_token_rate": 0.00000375, "cache_read_token_rate": 0.0000003},
    "tts_generate": {"provider": "minimax", "base": 0.0, "per_char": 0.0001, "per_second": 0.0},
    "digital_human_generate": {"provider": "volc_omnihuman", "base": 0.0, "per_second": round(1.0 / FX_CNY_PER_USD, 6)},
    "material_fetch": {"provider": "pexels", "base": 0.0, "per_segment": 0.0},
    "tos_upload": {"provider": "volc_tos", "minimum": 0.0, "per_mb": 0.0},
    "compose_video": {"provider": "ffmpeg", "base": 0.0, "per_second": 0.0},
}

AVATAR_STYLE_PROMPTS = [
    "人物面向镜头自然讲述，表情亲和，口型清晰，动作克制但真实，轻微点头和手势配合内容节奏",
    "人物以温柔自然的情绪面对镜头，表情轻松，动作柔和，镜头稳定，整体适合生活方式和服务介绍场景",
    "人物自然礼貌地对镜头讲述，表情克制细腻，动作简洁，节奏平稳，适合日语解说场景",
]

DEFAULT_CUSTOM_AVATAR_ORDER = 100


def _load_avatar_library_manifest() -> dict:
    if not AVATAR_LIBRARY_MANIFEST_PATH.exists():
        return {}
    try:
        with open(AVATAR_LIBRARY_MANIFEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_avatar_library_manifest(manifest: dict) -> None:
    AVATAR_LIBRARY_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(AVATAR_LIBRARY_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)


def _default_preferred_voices_for_gender(gender: str, allowed_target_markets: list[str]) -> dict:
    if (gender or "").strip().lower() == "male":
        return {market: "mandarin_male" for market in allowed_target_markets}
    return {
        market: (
            "mandarin_female"
            if market == "cn"
            else "taiwan_clone"
            if market == "tw" and os.getenv("VOICE_TAIWAN_CLONE", "").strip()
            else "taiwan_female"
            if market == "tw"
            else "japanese_female"
            if market == "jp"
            else "mandarin_female"
        )
        for market in allowed_target_markets
    }


def _normalize_avatar_manifest_entry(filename: str, metadata: dict | None = None) -> dict:
    metadata = metadata or {}
    allowed_target_markets = list(metadata.get("allowed_target_markets") or [])
    gender = (metadata.get("gender") or "").strip().lower()
    preferred_voice_by_market = dict(metadata.get("preferred_voice_by_market") or {})
    if not preferred_voice_by_market:
        preferred_voice_by_market = _default_preferred_voices_for_gender(gender, allowed_target_markets)
    return {
        "name": metadata.get("name") or Path(filename).stem,
        "gender": gender or "female",
        "allowed_target_markets": allowed_target_markets or ["cn", "tw", "jp"],
        "preferred_voice_by_market": preferred_voice_by_market,
        "style_prompt": metadata.get("style_prompt") or AVATAR_STYLE_PROMPTS[0],
        "created_at": metadata.get("created_at") or time.time(),
        "source": metadata.get("source") or "manual",
    }


def _slugify_avatar_name(name: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", (name or "").strip()).strip("_")
    return slug or "avatar"


def _build_generated_avatar_filename(name: str, gender: str, index: int = 1) -> str:
    slug = _slugify_avatar_name(name)
    stamp = int(time.time())
    return f"avatar_custom_{slug}_{gender or 'female'}_{stamp}_{index:02d}.png"


def _register_avatar_library_file(filename: str, metadata: dict) -> dict:
    with AVATAR_LIBRARY_LOCK:
        manifest = _load_avatar_library_manifest()
        manifest[filename] = _normalize_avatar_manifest_entry(filename, metadata)
        _save_avatar_library_manifest(manifest)
        return manifest[filename]


def _delete_avatar_library_file(filename: str) -> dict:
    safe_name = Path(filename or "").name
    if not safe_name:
        raise ValueError("主播文件名不能为空")
    if safe_name in AVATAR_OPTION_EXCLUDE_FILENAMES or safe_name in {"ihouse-logo.webp"}:
        raise ValueError("品牌 logo 不能删除")
    file_path = (ASSETS_DIR / safe_name).resolve()
    assets_root = ASSETS_DIR.resolve()
    if not str(file_path).startswith(str(assets_root)):
        raise ValueError("非法文件路径")
    if not file_path.exists():
        raise FileNotFoundError("主播图片不存在")
    with AVATAR_LIBRARY_LOCK:
        manifest = _load_avatar_library_manifest()
        manifest.pop(safe_name, None)
        _save_avatar_library_manifest(manifest)
    file_path.unlink(missing_ok=True)
    return {"filename": safe_name}


def _material_library_item_payload(item: dict, current_user: Optional[dict] = None) -> dict:
    payload = dict(item or {})
    payload["url"] = f"/public/material-library/{quote(str(payload.get('filename') or ''))}"
    payload["can_review"] = bool(current_user and _is_admin(current_user))
    payload["can_delete"] = bool(
        current_user
        and (
            _is_admin(current_user)
            or str(payload.get("uploader_username") or "") == str(current_user.get("username") or "")
        )
    )
    return payload


class ProgressTracker:
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.messages = []
        self.step = 0
        self.total_steps = 4
        self.status = "running"
        self.result = None

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
        if self.status == "cancelled":
            return
        self.status = "done"
        self.result = result
        self.log("全部完成！", step=self.total_steps)

    def fail(self, error: str):
        if self.status == "cancelled":
            return
        self.status = "error"
        self.log(f"出错了：{error}")

    def cancel(self, message: str = "任务已停止"):
        if self.status in ("done", "error", "cancelled"):
            return
        self.status = "cancelled"
        self.log(message)


class TaskCancelled(Exception):
    pass


def _make_safe_name(value: str, fallback: str = "task") -> str:
    safe = "".join(c for c in (value or "")[:20] if c.isalnum() or c in "，。_-")
    return safe or fallback


def _create_output_dir(prefix: str, label: str) -> str:
    output_dir = OUTPUT_DIR / f"{int(time.time())}_{prefix}_{_make_safe_name(label, fallback=prefix)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


def _normalize_public_base_url(value: str) -> str:
    return (value or "").rstrip("/")


def _get_public_base_url(request: Request) -> str:
    env_url = os.getenv("PUBLIC_BASE_URL")
    if env_url:
        return _normalize_public_base_url(env_url)
    forwarded_proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    forwarded_host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc))
    return _normalize_public_base_url(f"{forwarded_proto}://{forwarded_host}")


def _resolve_local_file(file_path: str) -> Optional[Path]:
    if not file_path:
        return None
    path = Path(file_path)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    else:
        path = path.resolve()
    return path if path.exists() else None


def _friendly_ai_error_message(exc: Exception, action_label: str) -> tuple[str, int]:
    text = str(exc).lower()
    if any(token in text for token in ["overloaded_error", "overloaded", "rate limit", "rate_limit", "429", "529", "暂时繁忙"]):
        return f"{action_label}服务当前较忙，请稍后重试", 503
    return f"{action_label}失败，请稍后重试", 500


def _is_task_cancel_requested(task_id: str) -> bool:
    task = tasks.get(task_id) or {}
    return bool(task.get("cancel_requested"))


def _raise_if_task_cancel_requested(task_id: str, message: str = "任务已停止"):
    if _is_task_cancel_requested(task_id):
        raise TaskCancelled(message)


def _cancel_waiting_omnihuman_jobs(task_id: str) -> int:
    removed = 0
    with OMNIHUMAN_QUEUE_CONDITION:
        before = len(OMNIHUMAN_WAITING_JOBS)
        OMNIHUMAN_WAITING_JOBS[:] = [item for item in OMNIHUMAN_WAITING_JOBS if item.get("task_id") != task_id]
        removed = before - len(OMNIHUMAN_WAITING_JOBS)
        if removed:
            OMNIHUMAN_QUEUE_CONDITION.notify_all()
    return removed


def _history_id_from_output_dir(output_dir: Optional[str]) -> str:
    if not output_dir:
        return ""
    return Path(output_dir).resolve().name


def _get_target_market(target_market_id: Optional[str]) -> dict:
    for market in TARGET_MARKETS:
        if market["id"] == target_market_id:
            return dict(market)
    return dict(TARGET_MARKETS[0])


def _get_department(department_id: Optional[str]) -> dict:
    for department in DEPARTMENTS:
        if department["id"] == department_id:
            return dict(department)
    return dict(DEPARTMENTS[0])


def _get_voice_preset(voice_preset_id: Optional[str], target_market_id: Optional[str] = None) -> dict:
    for preset in VOICE_PRESETS:
        if preset["id"] == voice_preset_id:
            return dict(preset)
    target_market = _get_target_market(target_market_id)
    default_id = target_market.get("default_voice_preset_id")
    for preset in VOICE_PRESETS:
        if preset["id"] == default_id:
            return dict(preset)
    return dict(VOICE_PRESETS[0])


def _get_visible_voice_preset_ids(target_market_id: Optional[str]) -> set[str]:
    target_market_id = (target_market_id or "cn").strip() or "cn"
    if target_market_id == "tw":
        return {"taiwan_clone"}
    if target_market_id == "jp":
        return {"japanese_female"}
    return {"mandarin_female", "mandarin_male"}


def _is_avatar_voice_compatible(avatar_option: Optional[dict], voice_preset: Optional[dict]) -> bool:
    if not avatar_option or not voice_preset:
        return True
    avatar_gender = (avatar_option.get("gender") or "").strip().lower()
    voice_gender = (voice_preset.get("gender") or "").strip().lower()
    if not avatar_gender or not voice_gender:
        return True
    return avatar_gender == voice_gender


def _normalize_digital_human_engine(engine_id: str | None, user: Optional[dict] = None) -> str:
    requested = (engine_id or VOLC_ENGINE_ID).strip()
    if requested == HUNYUAN_ENGINE_ID and _is_admin(user):
        return HUNYUAN_ENGINE_ID
    return VOLC_ENGINE_ID


def _digital_human_engine_label(engine_id: str | None) -> str:
    normalized = _normalize_digital_human_engine(engine_id, {"role": "admin"} if engine_id == HUNYUAN_ENGINE_ID else None)
    for item in DIGITAL_HUMAN_ENGINES:
        if item["id"] == normalized:
            return item["name"]
    return "火山 OmniHuman"


def _digital_human_engine_options_for_user(user: Optional[dict]) -> list[dict]:
    if _is_admin(user):
        return DIGITAL_HUMAN_ENGINES
    return [item for item in DIGITAL_HUMAN_ENGINES if not item.get("admin_only")]


def _normalize_script_model(model_id: str | None, user: Optional[dict] = None) -> str:
    requested = str(model_id or "").strip().lower()
    if requested == SCRIPT_MODEL_CLAUDE:
        return SCRIPT_MODEL_CLAUDE
    if requested in {SCRIPT_MODEL_GLM, SCRIPT_MODEL_CHATGPT} and _is_admin(user):
        return requested
    return SCRIPT_MODEL_CLAUDE


def _script_model_label(model_id: str | None) -> str:
    normalized = _normalize_script_model(model_id, {"role": "admin"} if model_id in {SCRIPT_MODEL_GLM, SCRIPT_MODEL_CHATGPT} else None)
    for item in SCRIPT_MODEL_OPTIONS:
        if item["id"] == normalized:
            return item["name"]
    return "Claude"


def _script_model_options_for_user(user: Optional[dict]) -> list[dict]:
    if _is_admin(user):
        return SCRIPT_MODEL_OPTIONS
    return [item for item in SCRIPT_MODEL_OPTIONS if not item.get("admin_only")]


def _get_avatar_option(avatar_id: Optional[str], target_market_id: Optional[str] = None) -> Optional[dict]:
    avatars = _list_avatar_options(target_market_id=target_market_id, include_all=not target_market_id)
    if not avatars:
        return None
    for index, avatar in enumerate(avatars):
        if avatar["id"] == avatar_id or avatar_id is None:
            enriched = dict(avatar)
            enriched["image_path"] = str(ASSETS_DIR / avatar["filename"])
            enriched["style_prompt"] = AVATAR_STYLE_PROMPTS[min(index, len(AVATAR_STYLE_PROMPTS) - 1)]
            return enriched
    avatar = dict(avatars[0])
    avatar["image_path"] = str(ASSETS_DIR / avatar["filename"])
    avatar["style_prompt"] = AVATAR_STYLE_PROMPTS[0]
    return avatar


def _get_social_post(script_data: dict, target_market: str = "cn") -> str:
    social_post = (script_data or {}).get("social_post", "")
    if social_post:
        return social_post
    if target_market == "tw":
        return script_data.get("facebook_post", "") or script_data.get("xiaohongshu_post", "")
    if target_market == "jp":
        return script_data.get("social_post", "") or script_data.get("facebook_post", "") or script_data.get("xiaohongshu_post", "")
    return script_data.get("xiaohongshu_post", "") or script_data.get("facebook_post", "")


def _get_avatar_image_path_for_task(task: dict) -> str:
    workflow_config = task.get("workflow_config", {}) or {}
    avatar_option = _get_avatar_option(workflow_config.get("avatar_id"))
    if avatar_option and avatar_option.get("image_path"):
        return avatar_option["image_path"]
    return task.get("image_path", "")


def _get_avatar_prompt_for_task(task: dict) -> str:
    workflow_config = task.get("workflow_config", {}) or {}
    avatar_option = _get_avatar_option(workflow_config.get("avatar_id"))
    return avatar_option.get("style_prompt", "") if avatar_option else ""


def _sync_live_task_result(output_dir: Optional[str], result: dict):
    live_task_id = _find_live_task_id_for_output_dir(output_dir or "")
    if live_task_id and live_task_id in tasks:
        tasks[live_task_id]["result"] = result


def _segment_has_audio(seg: dict) -> bool:
    audio_path = str((seg or {}).get("audio_path") or "").strip()
    audio_url = str((seg or {}).get("audio_url") or "").strip()
    return bool((audio_path and os.path.exists(audio_path)) or audio_url)


def _segment_has_video(seg: dict) -> bool:
    video_path = str((seg or {}).get("video_path") or "").strip()
    return bool(video_path and os.path.exists(video_path))


def _segment_has_materials(seg: dict) -> bool:
    items = _segment_material_items(seg or {})
    return any(item.get("path") and os.path.exists(str(item.get("path"))) for item in items)


def _history_stage_from_running_task(task: Optional[dict]) -> tuple[str, str]:
    if not task:
        return "running", "script"
    tracker = task.get("tracker")
    task_id = str(task.get("id", ""))
    queue = _omnihuman_queue_snapshot()
    waiting_task_ids = {str(item.get("task_id", "")) for item in (queue.get("waiting") or []) if item.get("task_id")}
    running_task_ids = {str(item.get("task_id", "")) for item in (queue.get("running") or []) if item.get("task_id")}
    if task.get("cancel_requested"):
        return "stopping", "stopping"
    if task_id in waiting_task_ids:
        return "running", "digital_human"
    if task_id in running_task_ids:
        return "running", "digital_human"
    step = int(getattr(tracker, "step", 0) or 0)
    if step <= 1:
        return "running", "script"
    if step == 2:
        return "running", "audio"
    if step == 3:
        return "running", "digital_human"
    if step == 4:
        return "running", "materials"
    return "running", "compose"


def _build_history_lifecycle(output_dir: Optional[Path], result: Optional[dict]) -> dict:
    output_dir_str = str(output_dir or "")
    live_task_id = _find_live_task_id_for_output_dir(output_dir_str)
    live_task = tasks.get(live_task_id) if live_task_id else None
    if live_task and getattr(live_task.get("tracker"), "status", "") == "running":
        status, stage = _history_stage_from_running_task(live_task)
        return {
            "status": status,
            "stage_key": stage,
            "can_resume_production": False,
            "can_compose": False,
            "live_task_id": live_task_id,
        }

    segments = list((result or {}).get("segments") or [])
    has_script = bool((result or {}).get("script") or segments or (result or {}).get("title"))
    audio_ready = bool(segments) and all(_segment_has_audio(seg) for seg in segments)
    digital_human_ready = bool(segments) and all(seg.get("type") != "digital_human" or _segment_has_video(seg) for seg in segments)
    materials_ready = bool(segments) and all(seg.get("type") != "material" or _segment_has_materials(seg) for seg in segments)
    compose_ready = bool((result or {}).get("final_video_path"))

    if compose_ready:
        status = "completed"
        stage_key = "compose"
    elif materials_ready:
        status = "ready_compose"
        stage_key = "compose"
    elif digital_human_ready:
        status = "interrupted"
        stage_key = "materials"
    elif audio_ready:
        status = "interrupted"
        stage_key = "digital_human"
    elif has_script:
        status = "interrupted"
        stage_key = "audio"
    else:
        status = "draft"
        stage_key = "script"

    return {
        "status": status,
        "stage_key": stage_key,
        "can_resume_production": bool(has_script and not materials_ready and not compose_ready),
        "can_compose": bool(materials_ready and not compose_ready),
        "live_task_id": "",
    }


def _combine_prompt(avatar_prompt: str, segment_action: str) -> str:
    parts = [part.strip() for part in [avatar_prompt, segment_action] if part and part.strip()]
    return "。".join(parts)


def _generate_digital_human_video_by_engine(
    *,
    engine_id: str,
    image_url: str,
    image_path: str,
    audio_url: str,
    audio_path: str,
    output_path: str,
    prompt: str,
    task_id: str,
    segment_index: int,
):
    if engine_id == HUNYUAN_ENGINE_ID:
        from hunyuan_avatar_client import generate_hunyuan_avatar_video

        return generate_hunyuan_avatar_video(
            image_path=image_path,
            audio_path=audio_path,
            output_path=output_path,
            prompt=prompt,
            external_task_id=task_id,
            segment_index=segment_index,
        )

    from generate_digital_human import generate_digital_human_video

    return generate_digital_human_video(
        image_url=image_url,
        audio_url=audio_url,
        output_path=output_path,
        prompt=prompt,
    )


def _save_readable_script(script_data: dict, output_path: str):
    lines = [
        f"标题：{script_data.get('title', '')}",
        f"封面：{script_data.get('cover_title', '')}",
        f"总时长：{script_data.get('total_duration', 0)}秒",
        "\n" + "=" * 50,
        "【播报稿+时间轴】",
        "=" * 50,
    ]
    for seg in script_data.get("segments", []):
        seg_type = "数字人" if seg.get("type") == "digital_human" else "素材"
        lines.append(f"\n【{seg_type} | {seg.get('start', 0)}s~{seg.get('end', 0)}s】")
        lines.append(seg.get("script", ""))
        if seg.get("type") == "digital_human":
            lines.append(f"动作描述：{seg.get('action', '')}")
        else:
            lines.append(f"素材关键词：{seg.get('material_keyword', '')}")
            lines.append(f"素材说明：{seg.get('material_desc', '')}")
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")


def _save_social_posts(script_data: dict, output_path: str, target_market: str = "cn"):
    content = "\n".join(["=" * 50, "【SNS发布文案】", "=" * 50, _get_social_post(script_data, target_market)])
    Path(output_path).write_text(content, encoding="utf-8")


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
        _raise_if_task_cancel_requested(task_id)
        from fetch_materials import fetch_all_materials
        from generate_audio import generate_audio
        from generate_digital_human import generate_digital_human_video
        from generate_script import generate_script
        from tos_uploader import upload_file_and_get_url
        from video_composer import compose_history_video

        task = tasks[task_id]
        workflow_config = task.get("workflow_config", {}) or {}
        target_market = workflow_config.get("target_market", "cn")
        department_id = workflow_config.get("department_id", "real_estate")
        script_model = _normalize_script_model(workflow_config.get("script_model"), task)
        digital_human_engine = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), task)
        target_market_obj = _get_target_market(target_market)
        voice_preset = dict(voice_preset or _get_voice_preset(workflow_config.get("voice_preset_id"), target_market))
        avatar_option = avatar_option or _get_avatar_option(workflow_config.get("avatar_id"))
        tts_voice = voice_preset.get("voice_id")
        tts_speed = float(voice_preset.get("selected_speed", voice_preset.get("default_speed", 1.1)))
        tts_volume = float(voice_preset.get("selected_volume", voice_preset.get("default_volume", 1.0)))
        avatar_prompt = avatar_option.get("style_prompt", "") if avatar_option else ""

        output_dir = _create_output_dir("full", topic)
        task["output_dir"] = output_dir

        image_url = None
        if image_path and os.path.exists(image_path):
            _raise_if_task_cancel_requested(task_id)
            image_url = upload_file_and_get_url(image_path, key_prefix="full/image")
            tracker.log("数字人主播素材已上传到 TOS")

        if script_data is None:
            _raise_if_task_cancel_requested(task_id)
            tracker.log("正在生成视频文案...", step=1)
            script_data = generate_script(
                topic,
                enable_web_search=workflow_config.get("web_search_enabled", False),
                target_market=target_market,
                department_id=department_id,
                provider=script_model,
            )
        else:
            tracker.log("已加载确认后的文案脚本", step=1)

        Path(output_dir, "script.json").write_text(json.dumps(script_data, ensure_ascii=False, indent=2), encoding="utf-8")
        _save_readable_script(script_data, os.path.join(output_dir, "script_readable.txt"))
        _save_social_posts(script_data, os.path.join(output_dir, "social_posts.txt"), target_market=target_market)
        partial_checkpoint_result = {
            "topic": topic,
            "owner_username": task.get("owner_username"),
            "owner_display_name": task.get("owner_display_name"),
            "owner_role": task.get("owner_role", "user"),
            "title": script_data.get("title", ""),
            "cover_title": script_data.get("cover_title", ""),
            "total_duration": script_data.get("total_duration", 0),
            "segment_count": len(script_data.get("segments", [])),
            "script": script_data,
            "segments": [dict(seg) for seg in script_data.get("segments", [])],
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
                "avatar": {
                    "id": avatar_option.get("id") if avatar_option else "",
                    "name": avatar_option.get("name") if avatar_option else "",
                },
                "target_market": target_market,
                "department_id": department_id,
                "web_search_enabled": workflow_config.get("web_search_enabled", False),
                "script_model": script_model,
                "script_model_name": _script_model_label(script_model),
                "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
                "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
            },
            "image_path": image_path,
            "image_url": image_url or "",
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }
        _persist_production_checkpoint(task, partial_checkpoint_result, stage="audio")
        tracker.log(f"文案准备完成，共 {len(script_data.get('segments', []))} 段，总时长 {script_data.get('total_duration', 0)} 秒")

        tracker.log("正在生成全部配音...", step=2)
        audio_segments = []
        base_segments = list(script_data.get("segments", []))
        total_segments = len(base_segments)
        for index, seg in enumerate(base_segments, start=1):
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续生成后续配音")
            script_text = (seg.get("script") or "").strip()
            if not script_text:
                continue
            tracker.log(f"配音生成中（{index}/{total_segments}）：{script_text[:28]}...")
            seg_type = seg.get("type", "")
            audio_path = os.path.join(output_dir, "audio", f"segment_{index - 1:02d}_{seg_type}.mp3")
            generate_audio(
                script_text,
                audio_path,
                tts_voice,
                speed=tts_speed,
                volume=tts_volume,
                language=voice_preset.get("language", ""),
            )
            seg_with_audio = dict(seg)
            seg_with_audio["audio_path"] = audio_path
            seg_with_audio["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
            seg_with_audio["target_market"] = target_market
            seg_with_audio["department_id"] = department_id
            audio_segments.append(seg_with_audio)
            _record_cost_entry(
                event_type="tts_generate",
                amount=_estimate_tts_cost(script_text, audio_path),
                provider=COST_RULES["tts_generate"]["provider"],
                task=task,
                meta={"segment_index": index, "audio_path": audio_path, "scope": "produce"},
            )
            partial_checkpoint_result["segments"] = list(audio_segments) + [dict(item) for item in base_segments[index:]]
            partial_checkpoint_result["segment_count"] = len(partial_checkpoint_result["segments"])
            partial_checkpoint_result["cost_entries"] = task.get("cost_entries", [])
            partial_checkpoint_result["cost_summary"] = task.get("cost_summary", _empty_cost_summary())
            _persist_production_checkpoint(task, partial_checkpoint_result, stage="audio")
        tracker.log(f"全部配音完成，共 {len(audio_segments)} 段")

        checkpoint_result = {
            "topic": topic,
            "owner_username": task.get("owner_username"),
            "owner_display_name": task.get("owner_display_name"),
            "owner_role": task.get("owner_role", "user"),
            "title": script_data.get("title", ""),
            "cover_title": script_data.get("cover_title", ""),
            "total_duration": script_data.get("total_duration", 0),
            "segment_count": len(audio_segments),
            "script": script_data,
            "segments": audio_segments,
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
                "avatar": {
                    "id": avatar_option.get("id") if avatar_option else "",
                    "name": avatar_option.get("name") if avatar_option else "",
                },
                "target_market": target_market,
                "department_id": department_id,
                "web_search_enabled": workflow_config.get("web_search_enabled", False),
                "script_model": script_model,
                "script_model_name": _script_model_label(script_model),
                "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
                "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
            },
            "image_path": image_path,
            "image_url": image_url,
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }
        _persist_production_checkpoint(task, checkpoint_result, stage="digital_human")

        tracker.log("正在生成数字人视频...", step=3)
        if not image_url:
            tracker.log("未选择数字人主播图，跳过数字人视频生成")
            segments_with_dh = audio_segments
        else:
            segments_with_dh = []
            dh_segments = [seg for seg in audio_segments if seg.get("type") == "digital_human"]
            completed = 0
            for index, seg in enumerate(audio_segments):
                _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续生成后续数字人片段")
                if seg.get("type") != "digital_human":
                    segments_with_dh.append(seg)
                    continue
                completed += 1
                tracker.log(f"数字人生成中（{completed}/{len(dh_segments)}）：{_digital_human_engine_label(digital_human_engine)}")
                video_output = os.path.join(output_dir, "digital_human", f"dh_{index:02d}.mp4")
                video_path = _run_omnihuman_job_with_retry(
                    task_id=task_id,
                    job_id=f"{task_id}:segment:{index}",
                    label=f"数字人生成（第{completed}/{len(dh_segments)}段）",
                    tracker=tracker,
                    runner=lambda seg=seg, video_output=video_output, segment_number=index + 1: _generate_digital_human_video_by_engine(
                        engine_id=digital_human_engine,
                        image_url=image_url,
                        image_path=image_path,
                        audio_url=seg.get("audio_url"),
                        audio_path=seg.get("audio_path", ""),
                        output_path=video_output,
                        prompt=_combine_prompt(avatar_prompt, seg.get("action", "")),
                        task_id=task_id,
                        segment_index=segment_number,
                    ),
                )
                seg_copy = dict(seg)
                seg_copy["video_path"] = video_path
                seg_copy["digital_human_engine"] = digital_human_engine
                segments_with_dh.append(seg_copy)
                checkpoint_result["segments"] = list(segments_with_dh) + [dict(item) for item in audio_segments[index + 1:]]
                checkpoint_result["segment_count"] = len(checkpoint_result["segments"])
                _persist_production_checkpoint(task, checkpoint_result, stage="digital_human")
                _record_cost_entry(
                    event_type="digital_human_generate",
                    amount=_estimate_digital_human_cost(_probe_media_duration(video_path) or seg.get("duration", 0)),
                    provider=_digital_human_engine_label(digital_human_engine),
                    task=task,
                    meta={"segment_index": index + 1, "video_path": video_path, "scope": "produce"},
                )
            tracker.log("数字人视频生成完成")

        tracker.log("正在匹配素材内容...", step=4)
        try:
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续匹配素材")
            final_segments = fetch_all_materials(segments=segments_with_dh, output_dir=output_dir)
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，素材匹配完成后未继续收尾")
            tracker.log(f"素材匹配完成，共 {sum(1 for seg in final_segments if seg.get('material_paths'))} 组素材")
        except TaskCancelled:
            raise
        except Exception as exc:
            tracker.log(f"素材匹配失败：{exc}，已跳过该步骤")
            final_segments = segments_with_dh

        result_data = {
            "topic": topic,
            "owner_username": task.get("owner_username"),
            "owner_display_name": task.get("owner_display_name"),
            "owner_role": task.get("owner_role", "user"),
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
                "web_search_enabled": workflow_config.get("web_search_enabled", False),
                "target_market": target_market,
                "department_id": department_id,
                "avatar": {
                    "id": avatar_option.get("id") if avatar_option else None,
                    "image_url": avatar_option.get("image_url") if avatar_option else "",
                },
                "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
                "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
            },
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }

        task["result"] = result_data
        _persist_task_result(task)
        tracker.finish(result_data)
    except TaskCancelled as exc:
        tracker.cancel(str(exc) or "任务已停止")
    except Exception as exc:
        tracker.fail(str(exc))
        import traceback
        traceback.print_exc()


def _fetch_materials_for_single_segment(seg: dict, output_dir: str, segment_index: int) -> dict:
    from fetch_materials import fetch_materials_for_segment

    return fetch_materials_for_segment(
        seg,
        output_dir,
        segment_index,
        target_market=str(seg.get("target_market") or ""),
        department_id=str(seg.get("department_id") or ""),
    )


def run_resume_pipeline_with_progress(task_id: str):
    tracker = tasks[task_id]["tracker"]
    try:
        _raise_if_task_cancel_requested(task_id)
        from generate_audio import generate_audio
        from generate_digital_human import generate_digital_human_video
        from tos_uploader import upload_file_and_get_url

        task = tasks[task_id]
        output_dir = task.get("output_dir")
        if not output_dir:
            raise RuntimeError("历史任务缺少输出目录")
        output_path = Path(output_dir)
        result = _load_result_from_output_dir(output_path)
        if not result:
            raise RuntimeError("历史结果不存在，无法继续生产")

        workflow_config = task.get("workflow_config", {}) or result.get("workflow_config", {}) or {}
        target_market = workflow_config.get("target_market", "cn")
        department_id = workflow_config.get("department_id", "real_estate")
        digital_human_engine = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), task)
        target_market_obj = _get_target_market(target_market)
        voice_cfg = workflow_config.get("voice_preset", {}) or {}
        voice_preset = _get_voice_preset(voice_cfg.get("id"), target_market)
        avatar_cfg = workflow_config.get("avatar", {}) or {}
        avatar_option = _get_avatar_option(avatar_cfg.get("id"), target_market_id=target_market)
        tts_voice = voice_preset.get("voice_id")
        tts_speed = float(voice_cfg.get("selected_speed", voice_preset.get("default_speed", 1.1)))
        tts_volume = float(voice_cfg.get("selected_volume", voice_preset.get("default_volume", 1.0)))
        avatar_prompt = avatar_option.get("style_prompt", "") if avatar_option else ""

        script_data = result.get("script") or {}
        if not script_data:
            script_path = output_path / "script.json"
            if script_path.exists():
                script_data = json.loads(script_path.read_text(encoding="utf-8"))
        if not script_data or not script_data.get("segments"):
            raise RuntimeError("历史脚本不存在，无法继续生产")

        tracker.log("已从历史记录恢复任务，准备继续补齐中间结果", step=1)

        base_segments = list(script_data.get("segments") or [])
        existing_segments = list(result.get("segments") or [])
        image_path = _get_avatar_image_path_for_task(task)

        tracker.log("正在检查并补齐配音...", step=2)
        audio_segments = []
        for index, base_seg in enumerate(base_segments, start=1):
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续生成后续配音")
            seg = dict(base_seg)
            if index - 1 < len(existing_segments):
                seg.update(existing_segments[index - 1] or {})
            script_text = (seg.get("script") or "").strip()
            if not script_text:
                continue
            seg_type = seg.get("type", "")
            audio_path = seg.get("audio_path") or os.path.join(output_dir, "audio", f"segment_{index - 1:02d}_{seg_type}.mp3")
            if audio_path and os.path.exists(audio_path):
                seg["audio_path"] = audio_path
                # Historical signed TOS URLs expire, so refresh them on every resume.
                seg["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
            else:
                tracker.log(f"补生成配音（{index}/{len(base_segments)}）：{script_text[:28]}...")
                generate_audio(
                    script_text,
                    audio_path,
                    tts_voice,
                    speed=tts_speed,
                    volume=tts_volume,
                    language=voice_cfg.get("language", voice_preset.get("language", "")),
                )
                seg["audio_path"] = audio_path
                seg["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
                _record_cost_entry(
                    event_type="tts_generate",
                    amount=_estimate_tts_cost(script_text, audio_path),
                    provider=COST_RULES["tts_generate"]["provider"],
                    task=task,
                    meta={"segment_index": index, "audio_path": audio_path, "scope": "resume"},
                )
            seg["target_market"] = target_market
            seg["department_id"] = department_id
            audio_segments.append(seg)

        checkpoint_result = {
            "topic": result.get("topic", task.get("topic", "")),
            "owner_username": task.get("owner_username"),
            "owner_display_name": task.get("owner_display_name"),
            "owner_role": task.get("owner_role", "user"),
            "title": script_data.get("title", result.get("title", "")),
            "cover_title": script_data.get("cover_title", result.get("cover_title", "")),
            "total_duration": script_data.get("total_duration", result.get("total_duration", 0)),
            "segment_count": len(audio_segments),
            "script": script_data,
            "segments": audio_segments,
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
                "avatar": {
                    "id": avatar_option.get("id") if avatar_option else "",
                    "name": avatar_option.get("name") if avatar_option else "",
                },
                "target_market": target_market,
                "department_id": department_id,
                "web_search_enabled": workflow_config.get("web_search_enabled", False),
                "script_model": script_model,
                "script_model_name": _script_model_label(script_model),
                "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
                "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
            },
            "image_path": image_path,
            "image_url": "",
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }
        _persist_production_checkpoint(task, checkpoint_result, stage="digital_human")

        tracker.log("正在检查并补齐数字人视频...", step=3)
        segments_with_dh = []
        pending_dh_count = sum(1 for seg in audio_segments if seg.get("type") == "digital_human" and not _segment_has_video(seg))
        image_url = None
        if pending_dh_count and image_path and os.path.exists(image_path):
            image_url = upload_file_and_get_url(image_path, key_prefix="full/image")
        checkpoint_result["image_url"] = image_url or ""
        completed = 0
        total_dh = sum(1 for seg in audio_segments if seg.get("type") == "digital_human")
        for index, seg in enumerate(audio_segments):
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续生成后续数字人片段")
            if seg.get("type") != "digital_human":
                segments_with_dh.append(seg)
                continue
            completed += 1
            if _segment_has_video(seg):
                segments_with_dh.append(seg)
                continue
            if not image_url:
                segments_with_dh.append(seg)
                continue
            tracker.log(f"数字人补生成中（{completed}/{total_dh}）：{_digital_human_engine_label(digital_human_engine)}")
            video_output = os.path.join(output_dir, "digital_human", f"dh_{index:02d}.mp4")
            video_path = _run_omnihuman_job_with_retry(
                task_id=task_id,
                job_id=f"{task_id}:resume:{index}",
                label=f"数字人补生成（第{completed}/{total_dh}段）",
                tracker=tracker,
                runner=lambda seg=seg, video_output=video_output, segment_number=index + 1: _generate_digital_human_video_by_engine(
                    engine_id=digital_human_engine,
                    image_url=image_url,
                    image_path=image_path,
                    audio_url=seg.get("audio_url"),
                    audio_path=seg.get("audio_path", ""),
                    output_path=video_output,
                    prompt=_combine_prompt(avatar_prompt, seg.get("action", "")),
                    task_id=task_id,
                    segment_index=segment_number,
                ),
            )
            seg_copy = dict(seg)
            seg_copy["video_path"] = video_path
            seg_copy["digital_human_engine"] = digital_human_engine
            segments_with_dh.append(seg_copy)
            checkpoint_result["segments"] = list(segments_with_dh) + [dict(item) for item in audio_segments[index + 1:]]
            checkpoint_result["segment_count"] = len(checkpoint_result["segments"])
            _persist_production_checkpoint(task, checkpoint_result, stage="digital_human")
            _record_cost_entry(
                event_type="digital_human_generate",
                amount=_estimate_digital_human_cost(_probe_media_duration(video_path) or seg.get("duration", 0)),
                provider=_digital_human_engine_label(digital_human_engine),
                task=task,
                meta={"segment_index": index + 1, "video_path": video_path, "scope": "resume"},
            )

        tracker.log("正在检查并补齐素材...", step=4)
        final_segments = []
        material_total = sum(1 for seg in segments_with_dh if seg.get("type") == "material")
        material_done = 0
        for index, seg in enumerate(segments_with_dh):
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续匹配后续素材")
            if seg.get("type") != "material":
                final_segments.append(seg)
                continue
            material_done += 1
            if _segment_has_materials(seg):
                final_segments.append(seg)
                continue
            tracker.log(f"素材补生成中（{material_done}/{material_total}）")
            final_segments.append(_fetch_materials_for_single_segment(seg, output_dir, index))

        result["topic"] = task.get("topic") or result.get("topic", "")
        result["owner_username"] = task.get("owner_username")
        result["owner_display_name"] = task.get("owner_display_name")
        result["owner_role"] = task.get("owner_role", "user")
        result["title"] = script_data.get("title", result.get("title", ""))
        result["cover_title"] = script_data.get("cover_title", result.get("cover_title", ""))
        result["total_duration"] = script_data.get("total_duration", result.get("total_duration", 0))
        result["segment_count"] = len(final_segments)
        result["script"] = script_data
        result["segments"] = final_segments
        result["social_post"] = _get_social_post(script_data, target_market)
        result["workflow_config"] = {
            "voice_preset": {
                "id": voice_preset.get("id"),
                "name": voice_preset.get("name"),
                "subtitle": voice_preset.get("subtitle"),
                "selected_speed": tts_speed,
                "selected_volume": tts_volume,
                "language": target_market_obj.get("content_language", ""),
            },
            "web_search_enabled": workflow_config.get("web_search_enabled", False),
            "target_market": target_market,
            "department_id": department_id,
            "avatar": {
                "id": avatar_option.get("id") if avatar_option else None,
                "image_url": avatar_option.get("image_url") if avatar_option else "",
            },
            "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
            "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
            "digital_human_engine": digital_human_engine,
            "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
        }
        result["cost_entries"] = task.get("cost_entries", result.get("cost_entries", []))
        result["cost_summary"] = task.get("cost_summary", result.get("cost_summary", _empty_cost_summary()))

        task["result"] = result
        _persist_task_result(task)
        tracker.finish(result)
    except TaskCancelled as exc:
        tracker.cancel(str(exc) or "任务已停止")
    except Exception as exc:
        tracker.fail(str(exc))
        import traceback
        traceback.print_exc()


def run_avatar_test_with_progress(task_id: str, image_path: str, audio_path: str, public_base_url: str):
    tracker = tasks[task_id]["tracker"]
    try:
        _raise_if_task_cancel_requested(task_id)
        from generate_digital_human import generate_digital_human_video
        from tos_uploader import upload_file_and_get_url

        output_dir = tasks[task_id]["output_dir"]
        tracker.log("正在上传图片和音频到 TOS...", step=1)
        _raise_if_task_cancel_requested(task_id)
        image_url = upload_file_and_get_url(image_path, key_prefix="avatar-test/image")
        audio_url = upload_file_and_get_url(audio_path, key_prefix="avatar-test/audio")
        tracker.log("TOS 上传完成")

        tracker.log("正在合成数字人视频...", step=2)
        video_dir = os.path.join(output_dir, "digital_human")
        os.makedirs(video_dir, exist_ok=True)
        video_path = _run_omnihuman_job_with_retry(
            task_id=task_id,
            job_id=f"{task_id}:avatar-test",
            label="数字人单段测试",
            tracker=tracker,
            runner=lambda: generate_digital_human_video(
                image_url=image_url,
                audio_url=audio_url,
                output_path=os.path.join(video_dir, "avatar_test.mp4"),
                prompt="",
                output_resolution=720,
                pe_fast_mode=True,
            ),
        )
        tracker.log("数字人视频生成完成")

        result_data = {
            "mode": "avatar_test",
            "topic": "数字人单段测试",
            "owner_username": tasks[task_id].get("owner_username"),
            "owner_display_name": tasks[task_id].get("owner_display_name"),
            "owner_role": tasks[task_id].get("owner_role", "user"),
            "title": "数字人单段测试完成",
            "cover_title": "数字人生成测试",
            "total_duration": _probe_media_duration(video_path),
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
                    "end": _probe_media_duration(video_path),
                    "duration": _probe_media_duration(video_path),
                    "script": "单段数字人测试",
                    "action": "",
                    "audio_path": audio_path,
                    "video_path": video_path,
                    "material_paths": [],
                }
            ],
            "cost_entries": tasks[task_id].get("cost_entries", []),
            "cost_summary": tasks[task_id].get("cost_summary", _empty_cost_summary()),
        }
        tasks[task_id]["result"] = result_data
        _persist_task_result(tasks[task_id])
        tracker.finish(result_data)
    except TaskCancelled as exc:
        tracker.cancel(str(exc) or "任务已停止")
    except Exception as exc:
        tracker.fail(str(exc))
        import traceback
        traceback.print_exc()


def _omnihuman_queue_snapshot() -> dict:
    with OMNIHUMAN_QUEUE_CONDITION:
        running = [dict(item) for item in OMNIHUMAN_RUNNING_ITEMS]
        waiting = [dict(item) for item in OMNIHUMAN_WAITING_JOBS]
    return {
        "max_concurrent": OMNIHUMAN_MAX_CONCURRENT,
        "running_count": len(running),
        "waiting_count": len(waiting),
        "running": running,
        "waiting": waiting,
        "current_owner_username": running[0].get("owner_username") if running else "",
        "current_owner_display_name": running[0].get("owner_display_name") if running else "",
    }


def _run_script_ai_job(job_id: str, label: str, runner):
    global SCRIPT_AI_RUNNING_JOBS
    queue_item = {
        "job_id": job_id,
        "label": label,
        "created_at": time.time(),
    }
    with SCRIPT_AI_QUEUE_CONDITION:
        SCRIPT_AI_WAITING_JOBS.append(queue_item)
        while True:
            try:
                ahead = next((idx for idx, item in enumerate(SCRIPT_AI_WAITING_JOBS) if item.get("job_id") == job_id), 0)
            except ValueError:
                ahead = 0
            can_run = ahead == 0 and SCRIPT_AI_RUNNING_JOBS < SCRIPT_AI_MAX_CONCURRENT
            if can_run:
                SCRIPT_AI_WAITING_JOBS.pop(0)
                SCRIPT_AI_RUNNING_JOBS += 1
                SCRIPT_AI_RUNNING_ITEMS.append(queue_item)
                break
            SCRIPT_AI_QUEUE_CONDITION.wait(timeout=1)
    try:
        return runner()
    finally:
        with SCRIPT_AI_QUEUE_CONDITION:
            SCRIPT_AI_RUNNING_JOBS = max(0, SCRIPT_AI_RUNNING_JOBS - 1)
            SCRIPT_AI_RUNNING_ITEMS[:] = [item for item in SCRIPT_AI_RUNNING_ITEMS if item.get("job_id") != job_id]
            SCRIPT_AI_QUEUE_CONDITION.notify_all()


def _run_omnihuman_job(job_id: str, label: str, runner, tracker: Optional[ProgressTracker] = None):
    global OMNIHUMAN_RUNNING_JOBS
    task_key = str(job_id).split(':', 1)[0]
    task = tasks.get(task_key, {}) if isinstance(tasks, dict) else {}
    _raise_if_task_cancel_requested(task_key, "已停止当前任务，未继续进入数字人队列")
    queue_item = {
        "job_id": job_id,
        "task_id": task_key,
        "label": label,
        "topic": task.get("topic", ""),
        "mode": task.get("mode", "full"),
        "owner_username": task.get("owner_username", ""),
        "owner_display_name": task.get("owner_display_name") or task.get("owner_username") or "",
        "created_at": task.get("created_at", time.time()),
    }
    waiting_logged = False
    with OMNIHUMAN_QUEUE_CONDITION:
        OMNIHUMAN_WAITING_JOBS.append(queue_item)
        while True:
            if _is_task_cancel_requested(task_key):
                OMNIHUMAN_WAITING_JOBS[:] = [item for item in OMNIHUMAN_WAITING_JOBS if item.get("job_id") != job_id]
                OMNIHUMAN_QUEUE_CONDITION.notify_all()
                raise TaskCancelled("已停止当前任务，未继续等待数字人生成")
            try:
                ahead = next((idx for idx, item in enumerate(OMNIHUMAN_WAITING_JOBS) if item.get("job_id") == job_id), 0)
            except ValueError:
                ahead = 0
            can_run = ahead == 0 and OMNIHUMAN_RUNNING_JOBS < OMNIHUMAN_MAX_CONCURRENT
            if can_run:
                OMNIHUMAN_WAITING_JOBS.pop(0)
                OMNIHUMAN_RUNNING_JOBS += 1
                OMNIHUMAN_RUNNING_ITEMS.append(queue_item)
                break
            if tracker and not waiting_logged:
                tracker.log(f"{label}排队中，前方还有 {ahead} 个任务")
                _push_live_event("omnihuman_waiting", f"{label}排队中，前方还有 {ahead} 个任务", task, {"label": label, "queue_ahead": ahead})
                waiting_logged = True
            OMNIHUMAN_QUEUE_CONDITION.wait(timeout=2)
    try:
        _push_live_event("omnihuman_running", f"{label}开始生成", task, {"label": label})
        if tracker and waiting_logged:
            tracker.log(f"{label}开始生成")
        _raise_if_task_cancel_requested(task_key, "已停止当前任务，未继续生成数字人视频")
        return runner()
    finally:
        _push_live_event("omnihuman_finished", f"{label}已结束处理", task, {"label": label})
        with OMNIHUMAN_QUEUE_CONDITION:
            OMNIHUMAN_RUNNING_JOBS = max(0, OMNIHUMAN_RUNNING_JOBS - 1)
            OMNIHUMAN_RUNNING_ITEMS[:] = [item for item in OMNIHUMAN_RUNNING_ITEMS if item.get("job_id") != job_id]
            OMNIHUMAN_QUEUE_CONDITION.notify_all()


def _is_retryable_omnihuman_error(exc: Exception) -> bool:
    text = str(exc).lower()
    retry_tokens = [
        "50500",
        "internal error",
        "serveroverloaded",
        "concurrent limit",
        "request has reached api concurrent limit",
        "rate limit",
        "429",
        "500",
        "502",
        "503",
        "504",
        "timeout",
        "timed out",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "service unavailable",
        "cuda-capable device",
        "device(s) is/are busy",
        "gpu",
        "hunyuan",
    ]
    return any(token in text for token in retry_tokens)


def _run_omnihuman_job_with_retry(
    *,
    task_id: str,
    job_id: str,
    label: str,
    tracker: Optional[ProgressTracker],
    runner,
    retries: Optional[int] = None,
    retry_delay_seconds: Optional[int] = None,
):
    attempts = max(1, int(retries or os.getenv("OMNIHUMAN_STAGE_RETRIES", "3")))
    base_delay = max(1, int(retry_delay_seconds or os.getenv("OMNIHUMAN_STAGE_RETRY_DELAY_SECONDS", "5")))
    last_error: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        attempt_job_id = job_id if attempt == 1 else f"{job_id}:retry:{attempt}"
        try:
            if attempt > 1 and tracker:
                tracker.log(f"{label}重试中（{attempt}/{attempts}）")
            return _run_omnihuman_job(job_id=attempt_job_id, label=label, tracker=tracker, runner=runner)
        except TaskCancelled:
            raise
        except Exception as exc:
            last_error = exc
            if attempt >= attempts or not _is_retryable_omnihuman_error(exc):
                raise
            wait_seconds = base_delay * attempt
            retry_message = f"{label}失败，{wait_seconds} 秒后重试（{attempt}/{attempts}）"
            if tracker:
                tracker.log(retry_message)
            _push_live_event(
                "omnihuman_retry",
                retry_message,
                tasks.get(task_id),
                {
                    "label": label,
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "error": str(exc),
                },
            )
            time.sleep(wait_seconds)

    if last_error:
        raise last_error
    raise RuntimeError(f"{label}重试失败")


def _persist_production_checkpoint(task: dict, result: dict, stage: Optional[str] = None):
    if stage:
        task["production_stage"] = stage
    task["result"] = result
    _persist_task_result(task)


def _push_live_event(event_type: str, message: str, task: Optional[dict] = None, extra: Optional[dict] = None):
    payload = {
        "time": time.time(),
        "type": event_type,
        "message": message,
        "owner_username": "",
        "owner_display_name": "",
        "topic": "",
        "mode": "",
    }
    if task:
        payload.update({
            "owner_username": task.get("owner_username", ""),
            "owner_display_name": task.get("owner_display_name") or task.get("owner_username") or "",
            "topic": task.get("topic", ""),
            "mode": task.get("mode", ""),
        })
    if extra:
        payload.update(extra)
    LIVE_EVENTS.appendleft(payload)


def _recent_live_events(limit: int = 12) -> list[dict]:
    return list(LIVE_EVENTS)[:limit]


def _same_local_day(ts_a: float, ts_b: float) -> bool:
    return time.strftime("%Y-%m-%d", time.localtime(float(ts_a or 0))) == time.strftime("%Y-%m-%d", time.localtime(float(ts_b or 0)))


def _round_cost(value: float) -> float:
    return round(float(value or 0.0), 4)


def _empty_cost_summary() -> dict:
    return {
        "currency": COST_CURRENCY,
        "estimated_total": 0.0,
        "today_total": 0.0,
        "month_total": 0.0,
        "entry_count": 0,
        "by_type": {},
        "recent": [],
    }


def _load_cost_ledger() -> list[dict]:
    if not COST_LEDGER_PATH.exists():
        return []
    try:
        with open(COST_LEDGER_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_cost_ledger(entries: list[dict]):
    COST_LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(COST_LEDGER_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _append_cost_ledger_entry(entry: dict):
    with COST_LEDGER_LOCK:
        entries = _load_cost_ledger()
        entries.append(entry)
        _save_cost_ledger(entries)


def _cost_label(event_type: str) -> str:
    labels = {
        "script_generate": "文案生成",
        "script_revise": "AI改单段",
        "tts_generate": "配音生成",
        "digital_human_generate": "数字人生成",
        "material_fetch": "素材匹配",
        "tos_upload": "对象存储上传",
        "compose_video": "自动成片",
    }
    return labels.get(event_type, event_type)


def _estimate_script_cost(topic: str, script_data: Optional[dict] = None, web_search_enabled: bool = False, revise: bool = False, usage: Optional[dict] = None) -> float:
    rule = COST_RULES["script_revise" if revise else "script_generate"]
    usage = usage or {}
    input_tokens = float(usage.get("input_tokens", 0) or 0)
    output_tokens = float(usage.get("output_tokens", 0) or 0)
    cache_creation_input_tokens = float(usage.get("cache_creation_input_tokens", 0) or 0)
    cache_read_input_tokens = float(usage.get("cache_read_input_tokens", 0) or 0)
    web_search_calls = float(usage.get("web_search_calls", 0) or 0)
    if input_tokens or output_tokens or cache_creation_input_tokens or cache_read_input_tokens:
        amount = (
            input_tokens * rule.get("input_token_rate", 0)
            + output_tokens * rule.get("output_token_rate", 0)
            + cache_creation_input_tokens * rule.get("cache_creation_token_rate", 0)
            + cache_read_input_tokens * rule.get("cache_read_token_rate", 0)
        )
        if web_search_enabled:
            amount += max(1.0, web_search_calls) * rule.get("web_search", 0)
        return _round_cost(amount)
    chars = len(topic or "")
    if script_data:
        chars += len(json.dumps(script_data, ensure_ascii=False))
    amount = rule["base"] + chars * rule["per_char"]
    if web_search_enabled:
        amount += rule["web_search"]
    return _round_cost(amount)


def _probe_media_duration(file_path: str) -> float:
    if not file_path or not os.path.exists(file_path):
        return 0.0
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            return max(0.0, float((result.stdout or "0").strip() or 0.0))
    except Exception:
        return 0.0
    return 0.0


def _estimate_tts_cost(script_text: str, audio_path: str = "") -> float:
    rule = COST_RULES["tts_generate"]
    amount = rule.get("base", 0.0) + len(script_text or "") * rule.get("per_char", 0.0)
    return _round_cost(amount)


def _estimate_digital_human_cost(duration_seconds: float) -> float:
    rule = COST_RULES["digital_human_generate"]
    duration_seconds = max(1.0, float(duration_seconds or 0))
    amount = rule["base"] + duration_seconds * rule["per_second"]
    return _round_cost(amount)


def _estimate_material_cost(material_segment_count: int) -> float:
    return 0.0


def _estimate_tos_upload_cost(file_path: str) -> float:
    return 0.0


def _estimate_compose_cost(total_duration: float) -> float:
    return 0.0


def _summarize_cost_entries(entries: list[dict]) -> dict:
    summary = _empty_cost_summary()
    if not entries:
        return summary
    now_ts = time.time()
    today_key = time.strftime("%Y-%m-%d", time.localtime(now_ts))
    month_key = time.strftime("%Y-%m", time.localtime(now_ts))
    sorted_entries = sorted(entries, key=lambda row: float(row.get("time", 0) or 0), reverse=True)
    summary["entry_count"] = len(sorted_entries)
    summary["recent"] = sorted_entries[:8]
    by_type = {}
    total = 0.0
    today_total = 0.0
    month_total = 0.0
    for entry in sorted_entries:
        amount = _round_cost(entry.get("amount", 0.0))
        total += amount
        key = str(entry.get("event_type", ""))
        by_type[key] = _round_cost(by_type.get(key, 0.0) + amount)
        entry_ts = float(entry.get("time", 0) or 0)
        if time.strftime("%Y-%m-%d", time.localtime(entry_ts)) == today_key:
            today_total += amount
        if time.strftime("%Y-%m", time.localtime(entry_ts)) == month_key:
            month_total += amount
    summary["estimated_total"] = _round_cost(total)
    summary["today_total"] = _round_cost(today_total)
    summary["month_total"] = _round_cost(month_total)
    summary["by_type"] = by_type
    return summary


def _derived_cost_entry(*, event_type: str, amount: float, provider: str, owner_username: str, owner_display_name: str, owner_role: str, history_id: str, topic: str, entry_time: float, meta: Optional[dict] = None) -> dict:
    return {
        "time": float(entry_time or time.time()),
        "event_type": event_type,
        "label": _cost_label(event_type),
        "provider": provider,
        "currency": COST_CURRENCY,
        "amount": _round_cost(amount),
        "owner_username": owner_username or "admin",
        "owner_display_name": owner_display_name or owner_username or "admin",
        "owner_role": owner_role or "user",
        "task_id": "",
        "history_id": history_id,
        "topic": topic or "",
        "meta": meta or {},
    }


def _derive_cost_entries_for_result(output_dir: Optional[Path], result: dict) -> list[dict]:
    existing = result.get("cost_entries") or []
    if existing:
        return existing

    owner = _owner_summary(result)
    owner_username = owner.get("owner_username") or "admin"
    owner_display_name = owner.get("owner_display_name") or owner_username
    owner_role = owner.get("owner_role") or "user"
    history_id = output_dir.name if output_dir else ""
    topic = result.get("topic", "")
    workflow = result.get("workflow_config") or {}
    segments = list(result.get("segments") or [])
    base_time = float(result.get("created_at") or (output_dir.stat().st_mtime if output_dir and output_dir.exists() else time.time()))
    entries = []
    tick = 0.0

    def add(event_type: str, amount: float, provider: str, meta: Optional[dict] = None):
        nonlocal tick
        if amount <= 0:
            return
        tick += 1.0
        entries.append(
            _derived_cost_entry(
                event_type=event_type,
                amount=amount,
                provider=provider,
                owner_username=owner_username,
                owner_display_name=owner_display_name,
                owner_role=owner_role,
                history_id=history_id,
                topic=topic,
                entry_time=base_time + tick,
                meta=meta,
            )
        )

    add(
        "script_generate",
        _estimate_script_cost(topic, result, web_search_enabled=bool(workflow.get("web_search_enabled"))),
        COST_RULES["script_generate"]["provider"],
        {"scope": "history_backfill", "web_search_enabled": bool(workflow.get("web_search_enabled"))},
    )

    material_segments = [seg for seg in segments if seg.get("type") == "material"]
    if material_segments:
        add(
            "material_fetch",
            _estimate_material_cost(len(material_segments)),
            COST_RULES["material_fetch"]["provider"],
            {"scope": "history_backfill", "segment_count": len(material_segments)},
        )

    for index, seg in enumerate(segments, start=1):
        script_text = seg.get("script", "")
        audio_path = seg.get("audio_path", "")
        if script_text or audio_path:
            add(
                "tts_generate",
                _estimate_tts_cost(script_text, audio_path),
                COST_RULES["tts_generate"]["provider"],
                {"scope": "history_backfill", "segment_index": index, "audio_path": audio_path, "audio_duration": _probe_media_duration(audio_path)},
            )
        if seg.get("type") == "digital_human" and seg.get("video_path"):
            video_duration = _probe_media_duration(seg.get("video_path", "")) or float(seg.get("duration", 0) or 0)
            add(
                "digital_human_generate",
                _estimate_digital_human_cost(video_duration),
                COST_RULES["digital_human_generate"]["provider"],
                {"scope": "history_backfill", "segment_index": index, "video_path": seg.get("video_path", ""), "video_duration": video_duration},
            )

    if result.get("final_video_path"):
        add(
            "compose_video",
            _estimate_compose_cost(result.get("total_duration", 0)),
            COST_RULES["compose_video"]["provider"],
            {"scope": "history_backfill", "final_video_path": result.get("final_video_path", "")},
        )

    return entries


def _record_cost_entry(*, event_type: str, amount: float, provider: str, user: Optional[dict] = None, task: Optional[dict] = None, history_id: str = "", topic: str = "", meta: Optional[dict] = None) -> dict:
    owner_username = ""
    owner_display_name = ""
    owner_role = "user"
    task_id = ""
    if task:
        owner_username = task.get("owner_username", "")
        owner_display_name = task.get("owner_display_name") or owner_username
        owner_role = task.get("owner_role", "user")
        task_id = task.get("id", "")
        history_id = history_id or _history_id_from_output_dir(task.get("output_dir"))
        topic = topic or task.get("topic", "")
    elif user:
        owner_username = user.get("username", "")
        owner_display_name = user.get("display_name") or owner_username
        owner_role = user.get("role", "user")
    entry = {
        "time": time.time(),
        "event_type": event_type,
        "label": _cost_label(event_type),
        "provider": provider,
        "currency": COST_CURRENCY,
        "amount": _round_cost(amount),
        "owner_username": owner_username,
        "owner_display_name": owner_display_name,
        "owner_role": owner_role,
        "task_id": task_id,
        "history_id": history_id,
        "topic": topic,
        "meta": meta or {},
    }
    _append_cost_ledger_entry(entry)
    if task is not None:
        task.setdefault("cost_entries", []).append(entry)
        task["cost_summary"] = _summarize_cost_entries(task.get("cost_entries", []))
        if task.get("result") is not None:
            task["result"]["cost_entries"] = task.get("cost_entries", [])
            task["result"]["cost_summary"] = task.get("cost_summary", _empty_cost_summary())
    return entry


def _record_history_cost(*, output_dir: Path, result: dict, user: Optional[dict], event_type: str, amount: float, provider: str, topic: str = "", meta: Optional[dict] = None) -> dict:
    entry = _record_cost_entry(
        event_type=event_type,
        amount=amount,
        provider=provider,
        user=user,
        history_id=output_dir.name,
        topic=topic or result.get("topic", ""),
        meta=meta,
    )
    result.setdefault("cost_entries", []).append(entry)
    result["cost_summary"] = _summarize_cost_entries(result.get("cost_entries", []))
    return entry


def _list_cost_entries(current_user: Optional[dict], include_all: bool = False) -> list[dict]:
    entries = _load_cost_ledger()
    if include_all or (current_user and _is_admin(current_user)):
        return sorted(entries, key=lambda row: float(row.get("time", 0) or 0), reverse=True)
    if not current_user:
        return []
    username = current_user.get("username")
    return sorted([entry for entry in entries if entry.get("owner_username") == username], key=lambda row: float(row.get("time", 0) or 0), reverse=True)


def _build_cost_summary_payload(current_user: Optional[dict], include_all: bool = False) -> dict:
    entries = _list_cost_entries(current_user, include_all=include_all)
    summary = _summarize_cost_entries(entries)
    summary["recent"] = summary.get("recent", [])[:10]
    return summary


def _public_user(username: str, profile: dict) -> dict:
    return {
        "username": username,
        "role": profile.get("role", "user"),
        "display_name": profile.get("display_name", username),
        "interface_language": profile.get("interface_language", "zh-CN"),
        "department_id": profile.get("department_id", "real_estate"),
        "target_market": profile.get("target_market", "cn"),
    }


def _parse_bool_form(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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
    return bool(user and (user.get("role") == "admin" or user.get("owner_role") == "admin"))


def _user_can_access_task(user: Optional[dict], task: Optional[dict]) -> bool:
    if not user or not task:
        return False
    if _is_admin(user):
        return True
    return task.get("owner_username") == user.get("username")


def _owner_summary(result: dict) -> dict:
    owner_username = result.get("owner_username") or "admin"
    owner_display_name = result.get("owner_display_name") or ("管理员" if owner_username == "admin" else owner_username)
    owner_role = result.get("owner_role") or ("admin" if owner_username == "admin" else "user")
    return {
        "owner_username": owner_username,
        "owner_display_name": owner_display_name,
        "owner_role": owner_role,
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


def _resolve_history_output_dir(history_id: str) -> Optional[Path]:
    if not history_id:
        return None
    output_dir = OUTPUT_DIR / history_id
    if output_dir.exists() and output_dir.is_dir():
        return output_dir
    return None


def _load_result_from_output_dir(output_dir: Path) -> Optional[dict]:
    result_path = Path(output_dir) / "result.json"
    if not result_path.exists():
        return None
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(result.get("cost_entries"), list):
        result["cost_entries"] = []
    if not result.get("cost_summary"):
        result["cost_summary"] = _summarize_cost_entries(result["cost_entries"])
    return result


def _find_live_task_id_for_output_dir(output_dir: str) -> str:
    target = str(output_dir or "")
    for task_id, task in tasks.items():
        if str(task.get("output_dir") or "") == target:
            return task_id
    return ""


def _make_produce_submission_key(
    *,
    owner_username: str,
    topic: str,
    script_data: dict,
    voice_preset_id: str,
    avatar_id: str,
    speed: float,
    web_search_enabled: bool,
    target_market: str,
    department_id: str,
    script_model: str = SCRIPT_MODEL_CLAUDE,
    digital_human_engine: str = VOLC_ENGINE_ID,
) -> str:
    payload = {
        "owner_username": owner_username or "",
        "topic": (topic or "").strip(),
        "script": script_data,
        "voice_preset_id": voice_preset_id or "",
        "avatar_id": avatar_id or "",
        "speed": round(float(speed or 0), 3),
        "web_search_enabled": bool(web_search_enabled),
        "target_market": target_market or "",
        "department_id": department_id or "",
        "script_model": _normalize_script_model(script_model),
        "digital_human_engine": digital_human_engine or VOLC_ENGINE_ID,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _find_reusable_running_task(*, owner_username: str, submission_key: str, dedupe_window_seconds: int = 1800) -> Optional[dict]:
    now_ts = time.time()
    for task in tasks.values():
        tracker = task.get("tracker")
        if task.get("owner_username") != owner_username:
            continue
        if task.get("submission_key") != submission_key:
            continue
        if not tracker or tracker.status != "running":
            continue
        created_at = float(task.get("created_at") or 0)
        if now_ts - created_at > dedupe_window_seconds:
            continue
        return task
    return None


def _persist_task_result(task: dict):
    output_dir = task.get("output_dir")
    result = task.get("result")
    if not output_dir or not result:
        return
    path = Path(output_dir) / "result.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _build_file_entries(output_dir: str) -> list[dict]:
    base = Path(output_dir)
    entries = []
    if not base.exists():
        return entries
    for path in sorted(base.rglob('*')):
        if path.is_file():
            rel = path.relative_to(base).as_posix()
            entries.append({
                "path": rel,
                "name": path.name,
                "size": path.stat().st_size,
            })
    return entries


def _history_relpath_from_value(output_dir: str, value: str) -> str:
    if not output_dir or not value:
        return ""
    base = Path(output_dir)
    history_id = base.name
    raw = str(value).strip()
    if not raw or raw.startswith("http://") or raw.startswith("https://"):
        return ""

    path_obj = Path(raw)
    if not path_obj.is_absolute():
        candidate = (base / raw).resolve()
        if candidate.exists():
            return candidate.relative_to(base).as_posix()

    posix_parts = PurePosixPath(raw).parts
    if history_id in posix_parts:
        idx = posix_parts.index(history_id)
        rel = "/".join(posix_parts[idx + 1:])
        if rel:
            return rel

    basename = Path(raw).name
    if basename:
        matches = sorted(base.rglob(basename))
        if matches:
            return matches[0].relative_to(base).as_posix()
    return ""


def _history_file_url(output_dir: str, value: str) -> str:
    rel = _history_relpath_from_value(output_dir, value)
    if not rel:
        return ""
    history_id = Path(output_dir).name
    return f"/api/history/{history_id}/download/{rel}"


def _serialize_segment(output_dir: str, topic: str, seg: dict, index: int) -> dict:
    data = dict(seg)
    data["index"] = index + 1
    data["type"] = data.get("type", "material")
    for field in ("start", "end", "duration"):
        raw = data.get(field, 0)
        try:
            numeric = float(raw or 0)
        except (TypeError, ValueError):
            numeric = 0.0
        data[field] = int(numeric) if numeric.is_integer() else round(numeric, 2)

    audio_url = _history_file_url(output_dir, data.get("audio_path", ""))
    if audio_url:
        data["audio"] = {
            "url": audio_url,
            "name": Path(str(data.get("audio_path", ""))).name or f"segment_{index + 1:02d}.mp3",
        }

    video_url = _history_file_url(output_dir, data.get("video_path", ""))
    if video_url:
        data["video"] = {
            "url": video_url,
            "name": Path(str(data.get("video_path", ""))).name or f"segment_{index + 1:02d}.mp4",
        }

    materials = []
    raw_materials = data.get("material_items") or [{"path": path} for path in (data.get("material_paths") or [])]
    for item in raw_materials:
        material_path = item.get("path") if isinstance(item, dict) else str(item)
        material_kind = (
            item.get("kind") if isinstance(item, dict) else None
        ) or ("video" if Path(str(material_path)).suffix.lower() in {".mp4", ".mov", ".m4v", ".webm"} else "image")
        material_url = _history_file_url(output_dir, material_path)
        if not material_url:
            continue
        materials.append({
            "url": material_url,
            "name": Path(str(material_path)).name or f"material_{index + 1:02d}.jpg",
            "kind": material_kind,
        })
    if materials:
        data["materials"] = materials
    return data


def _serialize_result_for_ui(output_dir: str, result: dict, topic: str) -> dict:
    payload = dict(result)
    payload["topic"] = topic or payload.get("topic", "")
    payload["output_dir"] = output_dir
    payload["history_id"] = _history_id_from_output_dir(output_dir)
    payload["id"] = payload["history_id"]
    payload["live_task_id"] = _find_live_task_id_for_output_dir(output_dir) if output_dir else payload.get("live_task_id", "")
    payload["files"] = _build_file_entries(output_dir) if output_dir else []
    if not isinstance(payload.get("cost_entries"), list):
        payload["cost_entries"] = []
    payload["cost_summary"] = payload.get("cost_summary") or _summarize_cost_entries(payload["cost_entries"])
    payload["segments"] = [_serialize_segment(output_dir, payload["topic"], seg, index) for index, seg in enumerate(payload.get("segments") or [])]
    payload["segment_count"] = int(payload.get("segment_count") or len(payload["segments"]))
    payload["social_post"] = payload.get("social_post") or payload.get("xiaohongshu_post") or payload.get("facebook_post") or ""
    payload["lifecycle"] = _build_history_lifecycle(Path(output_dir) if output_dir else None, payload)

    final_video_url = _history_file_url(output_dir, payload.get("final_video_path", ""))
    if final_video_url:
        payload["final_video"] = {
            "url": final_video_url,
            "name": Path(str(payload.get("final_video_path", ""))).name or "final_video.mp4",
        }

    cover_image_url = _history_file_url(output_dir, payload.get("cover_image_path", ""))
    if cover_image_url:
        payload["cover_image"] = {
            "url": cover_image_url,
            "name": Path(str(payload.get("cover_image_path", ""))).name or "cover.jpg",
        }

    subtitle_url = _history_file_url(output_dir, payload.get("subtitle_path", ""))
    if subtitle_url:
        payload["subtitle_file"] = {
            "url": subtitle_url,
            "name": Path(str(payload.get("subtitle_path", ""))).name or "timeline_subtitles.srt",
        }
    return payload


def _segment_material_items(segment: dict) -> list[dict]:
    items = segment.get("material_items")
    if isinstance(items, list) and items:
        normalized = []
        for item in items:
            if isinstance(item, dict) and item.get("path"):
                path = str(item.get("path"))
                kind = item.get("kind") or ("video" if Path(path).suffix.lower() in {".mp4", ".mov", ".m4v", ".webm"} else "image")
                normalized.append({"path": path, "kind": kind})
        if normalized:
            segment["material_items"] = normalized
            segment["material_paths"] = [item["path"] for item in normalized]
            return normalized

    paths = [str(path) for path in (segment.get("material_paths") or []) if path]
    normalized = [{"path": path, "kind": ("video" if Path(path).suffix.lower() in {".mp4", ".mov", ".m4v", ".webm"} else "image")} for path in paths]
    segment["material_items"] = normalized
    segment["material_paths"] = paths
    return normalized


def _build_script_preview_payload(
    script_data: dict,
    topic: str,
    web_search_enabled: bool = False,
    target_market: str = "cn",
    department_id: str = "real_estate",
    script_model: str = SCRIPT_MODEL_CLAUDE,
    source_info: Optional[dict] = None,
    input_topic: str = "",
) -> dict:
    payload = dict(script_data or {})
    payload["topic"] = topic or payload.get("topic", "")
    payload["input_topic"] = input_topic or payload.get("input_topic", "")
    payload["web_search_enabled"] = bool(web_search_enabled)
    payload["target_market"] = target_market or payload.get("target_market", "cn")
    payload["department_id"] = department_id or payload.get("department_id", "real_estate")
    payload["script_model"] = _normalize_script_model(script_model)
    payload["script_model_name"] = _script_model_label(script_model)
    if source_info:
        payload["source"] = source_info

    segments = []
    total_duration = 0.0
    for index, seg in enumerate(payload.get("segments", []) or []):
        item = dict(seg or {})
        item["index"] = index + 1
        item["type"] = item.get("type", "material")
        item["script"] = item.get("script", "")
        item["action"] = item.get("action", "")
        item["material_keyword"] = item.get("material_keyword", "")
        item["material_desc"] = item.get("material_desc", "")
        item["material_search_keyword"] = item.get("material_search_keyword", "")
        item["reference_links"] = item.get("reference_links") or []

        for field in ("start", "end", "duration"):
            raw = item.get(field, 0)
            try:
                numeric = float(raw or 0)
            except (TypeError, ValueError):
                numeric = 0.0
            item[field] = int(numeric) if numeric.is_integer() else round(numeric, 2)

        total_duration = max(total_duration, float(item.get("end", 0) or 0))
        segments.append(item)

    payload["segments"] = segments
    payload["segment_count"] = len(segments)
    if not payload.get("total_duration"):
        payload["total_duration"] = int(total_duration) if float(total_duration).is_integer() else round(total_duration, 2)
    payload["social_post"] = payload.get("social_post", "")
    payload["title"] = payload.get("title", "")
    payload["cover_title"] = payload.get("cover_title", "")
    return payload


def _shorten_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _build_source_generation_topic(source_info: dict, topic_text: str = "", fallback_topic: str = "") -> str:
    source_kind = (source_info or {}).get("kind") or "text"
    if source_kind == "text":
        return _shorten_text(topic_text or fallback_topic or "", 1200)

    kind_label = {
        "youtube": "YouTube来源",
        "news": "新闻来源",
    }.get(source_kind, "来源链接")
    lines = [f"【{kind_label}】"]
    title = _shorten_text((source_info or {}).get("title", ""), 160)
    source_name = _shorten_text((source_info or {}).get("source_name", ""), 80)
    url = _shorten_text((source_info or {}).get("url", ""), 240)
    summary = _shorten_text((source_info or {}).get("summary", ""), 260)
    user_note = _shorten_text((source_info or {}).get("user_note", ""), 120)

    if title:
        lines.append(f"标题：{title}")
    if source_name:
        lines.append(f"来源：{source_name}")
    if url:
        lines.append(f"链接：{url}")
    if summary:
        lines.append(f"摘要：{summary}")
    if user_note:
        lines.append(f"用户备注：{user_note}")
    lines.append("请基于以上来源提炼适合短视频表达的选题角度，并在不捏造事实的前提下输出脚本。")
    return "\n".join(lines)


def _source_ready_for_script(source_info: dict) -> tuple[bool, str]:
    source_info = source_info or {}
    kind = source_info.get("kind") or "text"
    if kind == "douyin":
        return True, ""
    if kind not in {"bilibili", "douyin", "xiaohongshu"}:
        return True, ""
    method = str(source_info.get("extraction_method") or "")
    if method == "whisper_audio" or method.endswith("_subtitle"):
        return True, ""
    platform_name = {
        "bilibili": "B站",
        "douyin": "抖音",
        "xiaohongshu": "小红书",
    }.get(kind, "视频平台")
    error = str(source_info.get("error") or "").strip()
    return False, error or f"没有提取到{platform_name}视频的字幕或音频内容，暂不能生成文案。请换一个可公开解析的链接，或先配置该平台 cookies 后再试。"


def _list_history_items(user: Optional[dict], include_all: bool = False) -> list[dict]:
    items = []
    if not OUTPUT_DIR.exists():
        return items
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        if not result:
            continue
        if not include_all and not _history_visible_to_user(result, user):
            continue
        cost_summary = result.get("cost_summary") or _summarize_cost_entries(result.get("cost_entries", []))
        owner = _owner_summary(result)
        lifecycle = _build_history_lifecycle(output_dir, result)
        items.append({
            "id": output_dir.name,
            "history_id": output_dir.name,
            "topic": result.get("topic", ""),
            "title": result.get("title", ""),
            "cover_title": result.get("cover_title", ""),
            "segment_count": int(result.get("segment_count", len(result.get("segments", [])) or 0) or 0),
            "total_duration": int(float(result.get("total_duration", 0) or 0)),
            "created_at": int(output_dir.stat().st_mtime),
            "estimated_cost_total": _round_cost(cost_summary.get("estimated_total", 0.0)),
            "cost_currency": cost_summary.get("currency", COST_CURRENCY),
            "lifecycle": lifecycle,
            **owner,
        })
    return items


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


def _list_avatar_options(target_market_id: Optional[str] = None, include_all: bool = False) -> list[dict]:
    items = []
    manifest = _load_avatar_library_manifest()
    preferred_order = {
        "avatar_test_0cd3d70a.png": 0,
        "avatar_host_c.png": 1,
        "avatar_host_d.png": 2,
        "avatar_ultraman.png": 3,
        "avatar_test_new_01.png": 4,
        "avatar_custom_林晨专属_male_manual.png": 5,
    }
    for path in sorted(ASSETS_DIR.iterdir() if ASSETS_DIR.exists() else [], key=lambda p: (preferred_order.get(p.name, 999), p.name)):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        if path.name in AVATAR_OPTION_EXCLUDE_FILENAMES or path.stem in {"ihouse-logo"}:
            continue
        if re.fullmatch(r"avatar_test_[0-9a-f]{8}", path.stem) and path.name not in AVATAR_DISPLAY_NAME_MAP:
            continue
        rule = AVATAR_RULES.get(path.name, {})
        allowed_target_markets = list(rule.get("allowed_target_markets") or [])
        if target_market_id and not include_all and allowed_target_markets and target_market_id not in allowed_target_markets:
            continue
        metadata = dict(manifest.get(path.name) or {})
        items.append({
            "id": path.name,
            "name": metadata.get("name") or AVATAR_DISPLAY_NAME_MAP.get(path.name, path.stem),
            "image_url": f"/public/assets/{path.name}",
            "filename": path.name,
            "gender": metadata.get("gender") or rule.get("gender", ""),
            "allowed_target_markets": list(metadata.get("allowed_target_markets") or allowed_target_markets),
            "preferred_voice_by_market": dict(metadata.get("preferred_voice_by_market") or rule.get("preferred_voice_by_market") or {}),
            "style_prompt": metadata.get("style_prompt") or rule.get("style_prompt") or AVATAR_STYLE_PROMPTS[0],
            "source": metadata.get("source") or ("builtin" if path.name in AVATAR_RULES else "custom"),
        })
    return items


def _build_admin_live_status() -> dict:
    queue = _omnihuman_queue_snapshot()
    users = []
    active_tasks = []
    completed_today = 0
    now_ts = time.time()
    for item in _list_history_items(None, include_all=True):
        created_at = float(item.get("created_at", 0) or 0)
        if _same_local_day(created_at, now_ts):
            completed_today += 1
    for username, profile in USERS.items():
        display_name = profile.get("display_name", username)
        current_task = None
        for task in tasks.values():
            if task.get("owner_username") == username and task.get("tracker") and task["tracker"].status == "running":
                current_task = task
                break
        status = "空闲"
        detail = ""
        current_topic = ""
        if current_task:
            current_topic = current_task.get("topic", "")
            detail = current_task.get("tracker").messages[-1]["message"] if current_task.get("tracker").messages else "处理中"
            status = "任务处理中"
            waiting_hit = next((item for item in queue.get("waiting", []) if item.get("owner_username") == username), None)
            running_hit = next((item for item in queue.get("running", []) if item.get("owner_username") == username), None)
            if running_hit:
                status = "数字人生成中"
            elif waiting_hit:
                status = "数字人排队中"
        users.append({
            "username": username,
            "display_name": display_name,
            "status": status,
            "detail": detail,
            "current_topic": current_topic,
        })
        if current_task:
            workflow_config = current_task.get("workflow_config", {}) or {}
            engine_id = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), current_task)
            tracker = current_task.get("tracker")
            active_tasks.append({
                "task_id": current_task.get("id", ""),
                "topic": current_task.get("topic", ""),
                "owner_username": username,
                "owner_display_name": display_name,
                "mode_label": "完整生产" if current_task.get("mode") == "full" else "测试",
                "digital_human_engine": engine_id,
                "digital_human_engine_name": _digital_human_engine_label(engine_id),
                "step": getattr(tracker, "step", 0),
                "total_steps": getattr(tracker, "total_steps", 0),
                "latest_message": tracker.messages[-1]["message"] if tracker and tracker.messages else "处理中",
            })
    return {
        "summary": {
            "running_task_count": len(active_tasks),
            "waiting_queue_count": queue.get("waiting_count", 0),
            "current_owner_username": queue.get("current_owner_username", ""),
            "current_owner_display_name": queue.get("current_owner_display_name", ""),
            "completed_today": completed_today,
        },
        "queue": queue,
        "users": users,
        "active_tasks": active_tasks,
        "recent_events": _recent_live_events(),
    }


def _build_current_task_payload(user: Optional[dict]) -> Optional[dict]:
    if not user:
        return None
    username = user.get("username", "")
    current_task = None
    for task in sorted(tasks.values(), key=lambda item: float(item.get("created_at") or 0), reverse=True):
        tracker = task.get("tracker")
        if task.get("owner_username") != username:
            continue
        if not tracker or tracker.status != "running":
            continue
        current_task = task
        break
    if not current_task:
        return None
    tracker = current_task.get("tracker")
    latest_message = tracker.messages[-1]["message"] if tracker and tracker.messages else "处理中"
    workflow_config = current_task.get("workflow_config", {}) or {}
    engine_id = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), current_task)
    return {
        "task_id": current_task.get("id", ""),
        "topic": current_task.get("topic", ""),
        "mode": current_task.get("mode", "full"),
        "digital_human_engine": engine_id,
        "digital_human_engine_name": _digital_human_engine_label(engine_id),
        "step": getattr(tracker, "step", 0),
        "total_steps": getattr(tracker, "total_steps", 0),
        "status": getattr(tracker, "status", "running"),
        "latest_message": latest_message,
        "output_dir": current_task.get("output_dir") or "",
    }


def _build_active_tasks_payload(user: Optional[dict]) -> list[dict]:
    if not user:
        return []
    username = user.get("username", "")
    queue = _omnihuman_queue_snapshot()
    waiting_task_ids = {str(item.get("task_id", "")) for item in (queue.get("waiting") or []) if item.get("task_id")}
    running_task_ids = {str(item.get("task_id", "")) for item in (queue.get("running") or []) if item.get("task_id")}
    items = []
    for task in sorted(tasks.values(), key=lambda item: float(item.get("created_at") or 0), reverse=True):
        tracker = task.get("tracker")
        if task.get("owner_username") != username:
            continue
        if not tracker or tracker.status != "running":
            continue
        task_id = str(task.get("id", ""))
        latest_message = tracker.messages[-1]["message"] if tracker.messages else "处理中"
        workflow_config = task.get("workflow_config", {}) or {}
        engine_id = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), task)
        if task.get("cancel_requested"):
            status_group = "stopping"
            stage_key = "stopping"
        elif task_id in waiting_task_ids:
            status_group = "queued"
            stage_key = "digital_human_waiting"
        elif task_id in running_task_ids:
            status_group = "running"
            stage_key = "digital_human_running"
        elif int(getattr(tracker, "step", 0) or 0) <= 1:
            status_group = "running"
            stage_key = "script"
        elif int(getattr(tracker, "step", 0) or 0) == 2:
            status_group = "running"
            stage_key = "audio"
        elif int(getattr(tracker, "step", 0) or 0) == 3:
            status_group = "running"
            stage_key = "digital_human_preparing"
        else:
            status_group = "running"
            stage_key = "materials"
        items.append({
            "task_id": task_id,
            "topic": task.get("topic", ""),
            "mode": task.get("mode", "full"),
            "step": getattr(tracker, "step", 0),
            "total_steps": getattr(tracker, "total_steps", 0),
            "status": getattr(tracker, "status", "running"),
            "status_group": status_group,
            "stage_key": stage_key,
            "digital_human_engine": engine_id,
            "digital_human_engine_name": _digital_human_engine_label(engine_id),
            "latest_message": latest_message,
            "created_at": float(task.get("created_at") or 0),
            "output_dir": task.get("output_dir") or "",
        })
    return items


def _build_admin_stats() -> dict:
    histories = []
    derived_entries = []
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True) if OUTPUT_DIR.exists() else []:
        result = _load_result_from_output_dir(output_dir)
        if not result:
            continue
        histories.append(result)
        derived_entries.extend(result.get("cost_entries") or _derive_cost_entries_for_result(output_dir, result))

    cost_entries = derived_entries or _list_cost_entries(None, include_all=True)
    now_ts = time.time()
    cost_by_user = {}
    by_type_total = {}
    by_type_today = {}
    by_type_month = {}
    for entry in cost_entries:
        username = entry.get("owner_username") or "admin"
        bucket = cost_by_user.setdefault(username, {"estimated_cost_total": 0.0, "today_total": 0.0, "month_total": 0.0, "by_type": {}})
        amount = _round_cost(entry.get("amount", 0.0))
        event_type = str(entry.get("event_type", "") or "unknown")
        bucket["estimated_cost_total"] = _round_cost(bucket["estimated_cost_total"] + amount)
        bucket["by_type"][event_type] = _round_cost(bucket["by_type"].get(event_type, 0.0) + amount)
        by_type_total[event_type] = _round_cost(by_type_total.get(event_type, 0.0) + amount)
        entry_ts = float(entry.get("time", 0) or 0)
        if _same_local_day(entry_ts, now_ts):
            bucket["today_total"] = _round_cost(bucket["today_total"] + amount)
            by_type_today[event_type] = _round_cost(by_type_today.get(event_type, 0.0) + amount)
        if time.strftime("%Y-%m", time.localtime(entry_ts)) == time.strftime("%Y-%m", time.localtime(now_ts)):
            bucket["month_total"] = _round_cost(bucket["month_total"] + amount)
            by_type_month[event_type] = _round_cost(by_type_month.get(event_type, 0.0) + amount)

    summaries = {
        username: {
            "username": username,
            "display_name": profile.get("display_name", username),
            "role": profile.get("role", "user"),
            "count": 0,
            "histories": [],
            "estimated_cost_total": _round_cost(cost_by_user.get(username, {}).get("estimated_cost_total", 0.0)),
            "today_total": _round_cost(cost_by_user.get(username, {}).get("today_total", 0.0)),
            "month_total": _round_cost(cost_by_user.get(username, {}).get("month_total", 0.0)),
            "by_type": dict(sorted((cost_by_user.get(username, {}).get("by_type") or {}).items(), key=lambda kv: kv[1], reverse=True)),
        }
        for username, profile in USERS.items()
    }
    for item in _list_history_items(None, include_all=True):
        owner_username = item.get("owner_username") or "admin"
        if owner_username not in summaries:
            summaries[owner_username] = {
                "username": owner_username,
                "display_name": item.get("owner_display_name") or owner_username,
                "role": item.get("owner_role", "user"),
                "count": 0,
                "histories": [],
                "estimated_cost_total": _round_cost(cost_by_user.get(owner_username, {}).get("estimated_cost_total", 0.0)),
                "today_total": _round_cost(cost_by_user.get(owner_username, {}).get("today_total", 0.0)),
                "month_total": _round_cost(cost_by_user.get(owner_username, {}).get("month_total", 0.0)),
                "by_type": dict(sorted((cost_by_user.get(owner_username, {}).get("by_type") or {}).items(), key=lambda kv: kv[1], reverse=True)),
            }
        summaries[owner_username]["count"] += 1
        summaries[owner_username]["histories"].append(item)
    cost_breakdown = []
    for event_type, amount in sorted(by_type_total.items(), key=lambda kv: kv[1], reverse=True):
        cost_breakdown.append({
            "event_type": event_type,
            "label": _cost_label(event_type),
            "estimated_total": _round_cost(amount),
            "today_total": _round_cost(by_type_today.get(event_type, 0.0)),
            "month_total": _round_cost(by_type_month.get(event_type, 0.0)),
        })
    return {
        "users": sorted(summaries.values(), key=lambda row: (-row.get("count", 0), -row.get("estimated_cost_total", 0.0), row.get("username", ""))),
        "cost_breakdown": cost_breakdown,
        "unassigned": [],
        "currency": COST_CURRENCY,
        "total_count": sum(row.get("count", 0) for row in summaries.values()),
        "total_estimated_cost": _round_cost(sum(row.get("estimated_cost_total", 0.0) for row in summaries.values())),
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard_page(request: Request):
    return templates.TemplateResponse(request, "admin.html")


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


@app.get("/logout")
async def logout_redirect(request: Request):
    request.session.clear()
    return RedirectResponse(url=f"/?logged_out={int(time.time())}", status_code=302)


@app.get("/api/me")
async def me(request: Request):
    user = _get_current_user(request)
    if not user:
        return _auth_error()
    return {"user": user}


@app.get("/api/costs/summary")
async def costs_summary(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _build_cost_summary_payload(user, include_all=_is_admin(user))


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_admin_stats()


@app.get("/api/admin/live-status")
async def admin_live_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_admin_live_status()


def _admin_avatar_job_snapshot(job: dict) -> dict:
    return {
        "job_id": job.get("job_id", ""),
        "status": job.get("status", "pending"),
        "message": job.get("message", ""),
        "error": job.get("error", ""),
        "avatar_name": job.get("avatar_name", ""),
        "gender": job.get("gender", ""),
        "allowed_target_markets": job.get("allowed_target_markets", []),
        "style_note": job.get("style_note", ""),
        "reference_url": job.get("reference_url", ""),
        "candidates": job.get("candidates", []),
        "created_at": job.get("created_at", 0),
        "updated_at": job.get("updated_at", 0),
    }


def _admin_avatar_job_manifest_path(job_id: str) -> Path:
    return OUTPUT_DIR / "admin_avatar_jobs" / job_id / "job.json"


def _admin_avatar_persist_job(job: dict) -> None:
    job_id = str(job.get("job_id", "")).strip()
    if not job_id:
        return
    manifest_path = _admin_avatar_job_manifest_path(job_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(job)
    payload["output_dir"] = str(payload.get("output_dir", ""))
    payload["reference_path"] = str(payload.get("reference_path", ""))
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)


def _admin_avatar_latest_job() -> Optional[dict]:
    with ADMIN_AVATAR_JOBS_LOCK:
        if not ADMIN_AVATAR_JOBS:
            pass
        else:
            job = max(ADMIN_AVATAR_JOBS.values(), key=lambda item: float(item.get("updated_at") or item.get("created_at") or 0))
            return dict(job)

    jobs_root = OUTPUT_DIR / "admin_avatar_jobs"
    if not jobs_root.exists():
        return None
    candidates: list[tuple[float, dict]] = []
    for job_dir in jobs_root.iterdir():
        if not job_dir.is_dir():
            continue
        job_manifest = job_dir / "job.json"
        if job_manifest.exists():
            try:
                payload = json.loads(job_manifest.read_text(encoding="utf-8"))
                if isinstance(payload, dict) and payload.get("job_id"):
                    job = dict(payload)
                    job["output_dir"] = str(job.get("output_dir") or job_dir)
                    job["candidates"] = [
                        {
                            "filename": item.get("filename", ""),
                            "url": item.get("url") or f"/api/admin/avatar-lab/jobs/{job_dir.name}/download/{Path(item.get('filename', '')).name}",
                            "prompt": item.get("prompt", ""),
                        }
                        for item in (payload.get("candidates") or [])
                        if Path(item.get("filename", "")).name
                    ]
                    candidates.append((float(job.get("updated_at") or job_dir.stat().st_mtime), job))
                    continue
            except Exception:
                pass
        candidates_dir = job_dir / "candidates"
        if not candidates_dir.exists():
            continue
        files = sorted(
            [path for path in candidates_dir.iterdir() if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}],
            key=lambda item: item.name,
        )
        if not files:
            continue
        updated_at = max([job_dir.stat().st_mtime] + [path.stat().st_mtime for path in files])
        job_id = job_dir.name
        job = {
            "job_id": job_id,
            "status": "done",
            "message": "主播候选图已生成",
            "error": "",
            "avatar_name": "",
            "gender": "",
            "allowed_target_markets": [],
            "style_note": "",
            "reference_url": "",
            "candidates": [
                {
                    "filename": path.name,
                    "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{path.name}",
                    "prompt": "",
                }
                for path in files
            ],
            "created_at": job_dir.stat().st_mtime,
            "updated_at": updated_at,
        }
        candidates.append((updated_at, job))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _admin_avatar_job_from_disk(job_id: str) -> Optional[dict]:
    job_dir = OUTPUT_DIR / "admin_avatar_jobs" / job_id
    job_manifest = job_dir / "job.json"
    if job_manifest.exists():
        try:
            payload = json.loads(job_manifest.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload.get("job_id"):
                job = dict(payload)
                job["output_dir"] = str(job.get("output_dir") or job_dir)
                candidates_dir = job_dir / "candidates"
                job["candidates"] = [
                    {
                        "filename": item.get("filename", ""),
                        "url": item.get("url") or f"/api/admin/avatar-lab/jobs/{job_id}/download/{Path(item.get('filename', '')).name}",
                        "prompt": item.get("prompt", ""),
                    }
                    for item in (job.get("candidates") or [])
                    if Path(item.get("filename", "")).name and (candidates_dir / Path(item.get("filename", "")).name).exists()
                ]
                if not job["candidates"] and job.get("status") == "done":
                    return None
                return job
        except Exception:
            pass
    candidates_dir = job_dir / "candidates"
    if not candidates_dir.exists():
        return None
    files = sorted(
        [path for path in candidates_dir.iterdir() if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}],
        key=lambda item: item.name,
    )
    if not files:
        return None
    updated_at = max([job_dir.stat().st_mtime] + [path.stat().st_mtime for path in files])
    return {
        "job_id": job_id,
        "status": "done",
        "message": "主播候选图已生成",
        "error": "",
        "avatar_name": "",
        "gender": "",
        "allowed_target_markets": [],
        "style_note": "",
        "reference_url": "",
        "candidates": [
            {
                "filename": path.name,
                "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{path.name}",
                "prompt": "",
            }
            for path in files
        ],
        "created_at": job_dir.stat().st_mtime,
        "updated_at": updated_at,
        "output_dir": str(job_dir),
    }


def _admin_avatar_job_set(job: dict, **updates) -> dict:
    job.update(updates)
    job["updated_at"] = time.time()
    with ADMIN_AVATAR_JOBS_LOCK:
        ADMIN_AVATAR_JOBS[job["job_id"]] = job
    _admin_avatar_persist_job(job)
    return job


def _admin_avatar_generation_worker(job_id: str) -> None:
    with ADMIN_AVATAR_JOBS_LOCK:
        job = ADMIN_AVATAR_JOBS.get(job_id)
    if not job:
        return
    try:
        _admin_avatar_job_set(job, status="running", message="正在调用 Seedream 生成主播候选图...")
        candidates_dir = Path(job["output_dir"]) / "candidates"
        candidates = generate_avatar_candidates(
            reference_path=job["reference_path"],
            output_dir=str(candidates_dir),
            avatar_name=job["avatar_name"],
            gender=job["gender"],
            style_note=job.get("style_note", ""),
            target_markets=job.get("allowed_target_markets", []),
            count=int(job.get("candidate_count", 3)),
            size="1440x2560",
        )
        normalized = []
        for item in candidates:
            normalized.append({
                "filename": item["filename"],
                "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{Path(item['path']).name}",
                "prompt": item.get("prompt", ""),
            })
        _admin_avatar_job_set(job, status="done", message="主播候选图已生成", candidates=normalized, error="")
    except Exception as exc:
        _admin_avatar_job_set(job, status="error", message="主播图生成失败", error=str(exc))


@app.post("/api/admin/avatar-lab/generate")
async def admin_generate_avatar(request: Request, reference_image: UploadFile = File(...), avatar_name: str = Form("新主播"), gender: str = Form("female"), target_markets: str = Form("cn,tw,jp"), style_note: str = Form(""), candidate_count: int = Form(3)):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    if not reference_image or not reference_image.filename:
        return JSONResponse({"error": "请上传一张参考人脸图片"}, status_code=400)

    gender = (gender or "female").strip().lower()
    if gender not in {"female", "male"}:
        gender = "female"
    allowed_target_markets = [item.strip() for item in (target_markets or "").split(",") if item.strip()]
    if gender == "male":
        allowed_target_markets = [market for market in allowed_target_markets if market == "cn"] or ["cn"]
    else:
        allowed_target_markets = [market for market in allowed_target_markets if market in {"cn", "tw", "jp"}] or ["cn", "tw", "jp"]

    job_id = str(uuid.uuid4())[:8]
    output_dir = OUTPUT_DIR / "admin_avatar_jobs" / job_id
    upload_dir = output_dir / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    ext = Path(reference_image.filename).suffix or ".jpg"
    reference_path = upload_dir / f"reference{ext}"
    with open(reference_path, "wb") as f:
        f.write(await reference_image.read())

    job = {
        "job_id": job_id,
        "status": "pending",
        "message": "等待开始",
        "error": "",
        "avatar_name": avatar_name.strip() or "新主播",
        "gender": gender,
        "allowed_target_markets": allowed_target_markets,
        "style_note": style_note.strip(),
        "reference_path": str(reference_path),
        "reference_url": f"/public/tasks/{job_id}/{reference_path.name}",
        "output_dir": str(output_dir),
        "candidate_count": max(1, min(int(candidate_count or 3), 6)),
        "candidates": [],
        "created_at": time.time(),
        "updated_at": time.time(),
        "created_by": user.get("username"),
    }
    with ADMIN_AVATAR_JOBS_LOCK:
        ADMIN_AVATAR_JOBS[job_id] = job
    _admin_avatar_persist_job(job)
    thread = threading.Thread(target=_admin_avatar_generation_worker, args=(job_id,), daemon=True)
    thread.start()
    return _admin_avatar_job_snapshot(job)


@app.get("/api/admin/avatar-lab/jobs/latest")
async def admin_avatar_latest_job_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    job = _admin_avatar_latest_job()
    if not job:
        return JSONResponse({"error": "暂无主播图任务"}, status_code=404)
    return _admin_avatar_job_snapshot(job)


@app.get("/api/admin/avatar-lab/jobs/{job_id}")
async def admin_avatar_job_status(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with ADMIN_AVATAR_JOBS_LOCK:
        job = ADMIN_AVATAR_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return _admin_avatar_job_snapshot(job)


@app.get("/api/admin/avatar-lab/jobs/{job_id}/download/{file_path:path}")
async def admin_avatar_job_download(job_id: str, file_path: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with ADMIN_AVATAR_JOBS_LOCK:
        job = ADMIN_AVATAR_JOBS.get(job_id)
    if not job:
        job = _admin_avatar_job_from_disk(job_id)
    if not job:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    output_dir = Path(job.get("output_dir", ""))
    full_path = (output_dir / "candidates" / file_path).resolve()
    if not output_dir or not str(full_path).startswith(str(output_dir.resolve())) or not full_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(full_path))


@app.post("/api/admin/avatar-lab/jobs/{job_id}/import")
async def admin_avatar_job_import(job_id: str, request: Request, filename: str = Form(...), display_name: str = Form(""), gender: str = Form("female"), target_markets: str = Form("cn,tw,jp"), style_note: str = Form("")):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with ADMIN_AVATAR_JOBS_LOCK:
        job = ADMIN_AVATAR_JOBS.get(job_id)
    if not job:
        job = _admin_avatar_job_from_disk(job_id)
    if not job:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if job.get("status") != "done":
        return JSONResponse({"error": "主播图还未生成完成"}, status_code=400)

    candidate = next((item for item in job.get("candidates", []) if item.get("filename") == filename), None)
    if not candidate:
        return JSONResponse({"error": "候选图片不存在"}, status_code=404)

    source_path = Path(job["output_dir"]) / "candidates" / filename
    if not source_path.exists():
        return JSONResponse({"error": "候选图片文件不存在"}, status_code=404)

    final_name = _build_generated_avatar_filename(display_name or job.get("avatar_name", "新主播"), gender or job.get("gender", "female"), index=1)
    final_name = f"{Path(final_name).stem}_{job_id[:4]}{Path(final_name).suffix}"
    final_path = ASSETS_DIR / final_name
    shutil.copy2(source_path, final_path)

    gender = (gender or job.get("gender") or "female").strip().lower()
    if gender not in {"female", "male"}:
        gender = "female"
    selected_markets = ["cn", "tw", "jp"]

    metadata = _register_avatar_library_file(
        final_name,
        {
            "name": display_name.strip() or job.get("avatar_name") or Path(final_name).stem,
            "gender": gender,
            "allowed_target_markets": selected_markets,
            "preferred_voice_by_market": _default_preferred_voices_for_gender(gender, selected_markets),
            "style_prompt": style_note.strip() or AVATAR_STYLE_PROMPTS[0],
            "source": "admin_generated",
        },
    )

    try:
        source_path.unlink()
    except Exception:
        pass

    remaining_candidates = []
    for item in job.get("candidates", []):
        if item.get("filename") == filename:
            continue
        item_name = Path(item.get("filename", "")).name
        if item_name and (Path(job["output_dir"]) / "candidates" / item_name).exists():
            remaining_candidates.append(
                {
                    "filename": item_name,
                    "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{item_name}",
                    "prompt": item.get("prompt", ""),
                }
            )
    with ADMIN_AVATAR_JOBS_LOCK:
        if job_id in ADMIN_AVATAR_JOBS:
            ADMIN_AVATAR_JOBS[job_id]["candidates"] = remaining_candidates
            ADMIN_AVATAR_JOBS[job_id]["message"] = "主播候选图已保存到主播库"
            ADMIN_AVATAR_JOBS[job_id]["updated_at"] = time.time()

    return {
        "ok": True,
        "avatar": {
            "id": final_name,
            "name": metadata.get("name"),
            "filename": final_name,
            "image_url": f"/public/assets/{final_name}",
            "gender": metadata.get("gender"),
            "allowed_target_markets": metadata.get("allowed_target_markets"),
            "preferred_voice_by_market": metadata.get("preferred_voice_by_market"),
            "style_prompt": metadata.get("style_prompt"),
            "source": metadata.get("source"),
        },
        "candidates": remaining_candidates,
    }


@app.delete("/api/admin/avatar-lab/jobs/{job_id}/candidates/{filename}")
async def admin_avatar_job_delete_candidate(job_id: str, filename: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()

    safe_filename = Path(filename).name
    with ADMIN_AVATAR_JOBS_LOCK:
        job = ADMIN_AVATAR_JOBS.get(job_id)
    if not job:
        job = _admin_avatar_job_from_disk(job_id)
    if not job:
        return JSONResponse({"error": "任务不存在"}, status_code=404)

    output_dir = Path(job.get("output_dir", ""))
    candidates_dir = output_dir / "candidates"
    candidate_path = (candidates_dir / safe_filename).resolve()
    if not output_dir or not str(candidate_path).startswith(str(candidates_dir.resolve())) or not candidate_path.exists():
        return JSONResponse({"error": "候选图片不存在"}, status_code=404)

    try:
        candidate_path.unlink()
    except Exception as exc:
        return JSONResponse({"error": f"删除候选图片失败：{exc}"}, status_code=500)

    remaining_candidates = []
    for item in job.get("candidates", []):
        if item.get("filename") == safe_filename:
            continue
        item_name = Path(item.get("filename", "")).name
        if item_name and (candidates_dir / item_name).exists():
            remaining_candidates.append(
                {
                    "filename": item_name,
                    "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{item_name}",
                    "prompt": item.get("prompt", ""),
                }
            )

    if not remaining_candidates:
        remaining_candidates = [
            {
                "filename": path.name,
                "url": f"/api/admin/avatar-lab/jobs/{job_id}/download/{path.name}",
                "prompt": "",
            }
            for path in sorted(candidates_dir.iterdir(), key=lambda item: item.name)
            if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
        ]

    with ADMIN_AVATAR_JOBS_LOCK:
        if job_id in ADMIN_AVATAR_JOBS:
            ADMIN_AVATAR_JOBS[job_id]["candidates"] = remaining_candidates
            ADMIN_AVATAR_JOBS[job_id]["message"] = "主播候选图已更新"
            ADMIN_AVATAR_JOBS[job_id]["updated_at"] = time.time()

    return {"ok": True, "candidates": remaining_candidates}


@app.api_route("/public/assets/{file_path:path}", methods=["GET", "HEAD"])
async def public_asset(file_path: str):
    full_path = (ASSETS_DIR / file_path).resolve()
    if not str(full_path).startswith(str(ASSETS_DIR.resolve())) or not full_path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(full_path))


@app.api_route("/public/material-library/{file_path:path}", methods=["GET", "HEAD"])
async def public_material_library_file(file_path: str):
    safe_name = Path(file_path or "").name
    full_path = (MATERIAL_LIBRARY_PUBLIC_DIR / safe_name).resolve()
    if not str(full_path).startswith(str(MATERIAL_LIBRARY_PUBLIC_DIR.resolve())) or not full_path.exists():
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
    return {
        "voice_presets": VOICE_PRESETS,
        "avatars": _list_avatar_options(),
        "interface_languages": INTERFACE_LANGUAGES,
        "departments": DEPARTMENTS,
        "target_markets": TARGET_MARKETS,
        "composition_transitions": COMPOSITION_TRANSITIONS,
        "subtitle_templates": SUBTITLE_TEMPLATES,
        "digital_human_engines": _digital_human_engine_options_for_user(user),
        "script_models": _script_model_options_for_user(user),
        "current_user": user,
        "current_task": _build_current_task_payload(user),
        "active_tasks": _build_active_tasks_payload(user),
    }


@app.get("/api/material-library")
async def material_library_items(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    approved = [_material_library_item_payload(item, user) for item in list_material_library_items(status="approved")]
    pending = []
    if _is_admin(user):
        pending = [_material_library_item_payload(item, user) for item in list_material_library_items(status="pending")]
    return {"items": approved, "pending_items": pending}


@app.post("/api/material-library/upload")
async def upload_material_library_items(
    request: Request,
    files: list[UploadFile] = File(...),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    upload_dir = OUTPUT_DIR / "material_library_uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    created = []
    for index, upload in enumerate(files or [], start=1):
        original_name = Path(upload.filename or "").name
        suffix = Path(original_name).suffix.lower()
        temp_path = upload_dir / f"{uuid.uuid4().hex[:12]}_{index}{suffix}"
        with temp_path.open("wb") as f:
            shutil.copyfileobj(upload.file, f)
        try:
            item = register_material_file(
                temp_path=str(temp_path),
                original_filename=original_name,
                title=Path(original_name).stem,
                notes=notes,
                uploader_username=user.get("username", ""),
                uploader_display_name=user.get("display_name", ""),
                source="manual_upload",
            )
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        created.append(_material_library_item_payload(item, user))
    return {"ok": True, "items": created}


@app.post("/api/material-library/{item_id}/review")
async def review_material_library_item(
    item_id: str,
    request: Request,
    status: str = Form(...),
    title: str = Form(""),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以审核素材")
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"approved", "rejected"}:
        return JSONResponse({"error": "审核状态非法"}, status_code=400)
    try:
        existing = next((item for item in list_material_library_items() if str(item.get("id")) == str(item_id)), None)
        if not existing:
            return JSONResponse({"error": "素材不存在"}, status_code=404)
        final_title = title.strip() or existing.get("title") or Path(str(existing.get("original_filename") or existing.get("filename") or "素材")).stem
        updated = update_material_library_item(
            item_id,
            {
                "status": normalized_status,
                "title": final_title,
                "notes": notes.strip(),
                "reviewed_at": time.time(),
                "reviewed_by_username": user.get("username", ""),
                "reviewed_by_display_name": user.get("display_name", ""),
            },
        )
    except FileNotFoundError:
        return JSONResponse({"error": "素材不存在"}, status_code=404)
    return {"ok": True, "item": _material_library_item_payload(updated, user)}


@app.delete("/api/material-library/{item_id}")
async def delete_material_library_item_endpoint(item_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        items = list_material_library_items()
        existing = next((item for item in items if str(item.get("id")) == str(item_id)), None)
        if not existing:
            return JSONResponse({"error": "素材不存在"}, status_code=404)
        if not (_is_admin(user) or str(existing.get("uploader_username") or "") == str(user.get("username") or "")):
            return _forbidden_error("只能删除自己上传的素材")
        deleted = delete_material_library_item(item_id)
    except FileNotFoundError:
        return JSONResponse({"error": "素材不存在"}, status_code=404)
    return {"ok": True, "deleted": _material_library_item_payload(deleted, user)}


@app.delete("/api/admin/avatars/{filename:path}")
async def delete_avatar_library_item(request: Request, filename: str):
    user, error = _require_user(request)
    if error:
        return error
    if user.get("role") != "admin":
        return _forbidden_error()
    try:
        deleted = _delete_avatar_library_file(filename)
    except FileNotFoundError:
        return JSONResponse({"error": "主播图片不存在"}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return {"ok": True, "deleted": deleted}


@app.get("/api/tasks/active")
async def active_tasks(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"items": _build_active_tasks_payload(user)}


@app.post("/api/script-preview")
async def script_preview(
    request: Request,
    topic_text: str = Form(""),
    source_url: str = Form(""),
    topic: str = Form(""),
    use_web_search: str = Form("false"),
    target_market: str = Form("cn"),
    department_id: str = Form("real_estate"),
    script_model: str = Form(SCRIPT_MODEL_CLAUDE),
    digital_human_engine: str = Form(VOLC_ENGINE_ID),
):
    user, error = _require_user(request)
    if error:
        return error

    from generate_script import generate_script

    source_info = analyze_topic_fields(topic_text=topic_text, source_url=source_url, fallback_topic=topic)
    source_ready, source_error = _source_ready_for_script(source_info)
    if not source_ready:
        return JSONResponse({"error": source_error, "source": source_info}, status_code=422)
    generation_topic = _build_source_generation_topic(source_info, topic_text=topic_text, fallback_topic=topic)
    selected_script_model = _normalize_script_model(script_model, user)
    web_search_enabled = _parse_bool_form(use_web_search) or source_info.get("kind") == "news"
    try:
        script_data = _run_script_ai_job(
            job_id=f"preview:{user.get('username', 'guest')}:{time.time_ns()}",
            label="文案生成",
            runner=lambda: generate_script(generation_topic, enable_web_search=web_search_enabled, target_market=target_market, department_id=department_id, provider=selected_script_model),
        )
        script_usage = (script_data.pop("_meta", {}) or {}).get("usage", {})
    except Exception as exc:
        import traceback
        print(f"[script_preview_error] model={selected_script_model} web_search={web_search_enabled} topic={generation_topic!r} error={exc!r}")
        traceback.print_exc()
        message, status_code = _friendly_ai_error_message(exc, "文案生成")
        return JSONResponse({"error": message}, status_code=status_code)
    _record_cost_entry(
        event_type="script_generate",
        amount=_estimate_script_cost(generation_topic, script_data, web_search_enabled=web_search_enabled, usage=script_usage),
        provider=_script_model_label(selected_script_model),
        user=user,
        topic=generation_topic,
        meta={"scope": "preview", "web_search_enabled": web_search_enabled, "target_market": target_market, "department_id": department_id, "usage": script_usage, "source": source_info, "script_model": selected_script_model},
    )
    return {
        "topic": generation_topic,
        "input_topic": topic_text or topic,
        "script": script_data,
        "preview": _build_script_preview_payload(script_data, generation_topic, web_search_enabled=web_search_enabled, target_market=target_market, department_id=department_id, script_model=selected_script_model, source_info=source_info, input_topic=topic),
        "source": source_info,
    }


@app.post("/api/produce")
async def produce_video(
    request: Request,
    topic_text: str = Form(""),
    source_url: str = Form(""),
    topic: str = Form(""),
    script_json: str = Form(...),
    voice_preset_id: str = Form(...),
    avatar_id: str = Form(...),
    speed: float = Form(1.1),
    use_web_search: str = Form("false"),
    target_market: str = Form("cn"),
    department_id: str = Form("real_estate"),
    script_model: str = Form(SCRIPT_MODEL_CLAUDE),
    digital_human_engine: str = Form(VOLC_ENGINE_ID),
):
    user, error = _require_user(request)
    if error:
        return error

    try:
        script_data = json.loads(script_json)
    except json.JSONDecodeError:
        return JSONResponse({"error": "文案数据格式错误"}, status_code=400)

    source_info = analyze_topic_fields(topic_text=topic_text, source_url=source_url, fallback_topic=topic)
    source_ready, source_error = _source_ready_for_script(source_info)
    if not source_ready:
        return JSONResponse({"error": source_error, "source": source_info}, status_code=422)
    generation_topic = _build_source_generation_topic(source_info, topic_text=topic_text, fallback_topic=topic)
    selected_script_model = _normalize_script_model(script_model, user)
    web_search_enabled = _parse_bool_form(use_web_search) or source_info.get("kind") == "news"
    selected_digital_human_engine = _normalize_digital_human_engine(digital_human_engine, user)
    submission_key = _make_produce_submission_key(
        owner_username=user.get("username", ""),
        topic=generation_topic,
        script_data=script_data,
        voice_preset_id=voice_preset_id,
        avatar_id=avatar_id,
        speed=speed,
        web_search_enabled=web_search_enabled,
        target_market=target_market,
        department_id=department_id,
        script_model=selected_script_model,
        digital_human_engine=selected_digital_human_engine,
    )

    reusable_task = _find_reusable_running_task(
        owner_username=user.get("username", ""),
        submission_key=submission_key,
    )
    if reusable_task:
        tracker = reusable_task.get("tracker")
        if tracker and tracker.messages:
            latest_message = tracker.messages[-1].get("message", "任务已在后台执行")
        else:
            latest_message = "任务已在后台执行"
        return {
            "task_id": reusable_task.get("id", ""),
            "reused_existing": True,
            "message": latest_message,
        }

    visible_voice_ids = _get_visible_voice_preset_ids(target_market)
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    if voice_preset.get("id") not in visible_voice_ids:
        return JSONResponse({"error": "当前目标市场不支持该配音方案，请调整后再试"}, status_code=400)

    avatar_option = _get_avatar_option(avatar_id, target_market_id=target_market)
    if not avatar_option:
        return JSONResponse({"error": "当前目标市场没有可用的主播图片，请调整市场或主播后再试"}, status_code=400)
    if not _is_avatar_voice_compatible(avatar_option, voice_preset):
        return JSONResponse({"error": "当前主播与音色不匹配，请调整为同类形象后再试"}, status_code=400)

    voice_preset["selected_speed"] = speed
    image_path = avatar_option.get("image_path", "")
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": generation_topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "submission_key": submission_key,
        "workflow_config": {
            "voice_preset_id": voice_preset_id,
            "avatar_id": avatar_id,
            "speed": speed,
            "web_search_enabled": web_search_enabled,
            "target_market": target_market,
            "department_id": department_id,
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
            "source": source_info,
            "script_model": selected_script_model,
            "digital_human_engine": selected_digital_human_engine,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("任务已创建，准备开始...")
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, generation_topic, image_path, tasks[task_id]["public_base_url"], script_data, voice_preset, avatar_option),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id, "reused_existing": False}


@app.post("/api/generate")
async def start_generation(
    request: Request,
    topic_text: str = Form(""),
    source_url: str = Form(""),
    topic: str = Form(""),
    image: Optional[UploadFile] = File(None),
):
    user, error = _require_user(request)
    if error:
        return error
    source_info = analyze_topic_fields(topic_text=topic_text, source_url=source_url, fallback_topic=topic)
    source_ready, source_error = _source_ready_for_script(source_info)
    if not source_ready:
        return JSONResponse({"error": source_error, "source": source_info}, status_code=422)
    generation_topic = _build_source_generation_topic(source_info, topic_text=topic_text, fallback_topic=topic)
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
        "topic": generation_topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("任务已创建，准备开始...")
    _push_live_event("task_created", "创建了完整生产任务", tasks[task_id])
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, generation_topic, image_path, tasks[task_id]["public_base_url"]),
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
    upload_dir = Path(output_dir) / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    image_ext = Path(image.filename).suffix or ".jpg"
    image_path = str(upload_dir / f"avatar_test_{task_id}{image_ext}")
    with open(image_path, "wb") as f:
        f.write(await image.read())
    audio_ext = Path(audio.filename).suffix or ".mp3"
    audio_path = str(upload_dir / f"avatar_test_{task_id}{audio_ext}")
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
        "cancel_requested": False,
        "cancel_requested_at": None,
    }
    tracker.log("测试任务已创建，准备开始...")
    _push_live_event("task_created", "创建了数字人单段测试", tasks[task_id])
    thread = threading.Thread(
        target=run_avatar_test_with_progress,
        args=(task_id, image_path, audio_path, tasks[task_id]["public_base_url"]),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id}


@app.get("/api/omnihuman-queue")
async def omnihuman_queue_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _omnihuman_queue_snapshot()


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

            if tracker.status in ("done", "error", "cancelled"):
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
                elif tracker.status == "error":
                    output_dir = tasks[task_id].get("output_dir", "")
                    result = _load_result_from_output_dir(Path(output_dir)) if output_dir else None
                    lifecycle = _build_history_lifecycle(Path(output_dir), result) if result else {}
                    result_data = {
                        "mode": tasks[task_id].get("mode", "full"),
                        "output_dir": output_dir,
                        "history_id": Path(output_dir).name if output_dir else "",
                        "can_retry": bool(lifecycle.get("can_resume_production")),
                    }
                yield {
                    "event": "done",
                    "data": json.dumps({"status": tracker.status, "result": result_data}, ensure_ascii=False),
                }
                break

            import asyncio

            await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())


@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    task = tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, task):
        return _forbidden_error()

    tracker = task.get("tracker")
    if not tracker:
        return JSONResponse({"error": "任务状态异常"}, status_code=400)
    if tracker.status == "done":
        return JSONResponse({"error": "任务已完成，无法停止"}, status_code=400)
    if tracker.status == "error":
        return JSONResponse({"error": "任务已失败，无需停止"}, status_code=400)
    if tracker.status == "cancelled":
        return {"task_id": task_id, "status": "cancelled", "message": "任务已停止"}

    task["cancel_requested"] = True
    task["cancel_requested_at"] = time.time()
    removed_jobs = _cancel_waiting_omnihuman_jobs(task_id)

    if tracker.step <= 0:
        tracker.step = 1
    tracker.log("已收到停止请求，系统会尽快停止当前任务")
    if removed_jobs:
        tracker.log(f"已从数字人队列移除 {removed_jobs} 个待执行任务")
    _push_live_event("task_cancel_requested", "任务已请求停止", task, {"removed_waiting_jobs": removed_jobs})

    return {
        "task_id": task_id,
        "status": "cancelling",
        "message": "已收到停止请求，系统会尽快停止当前任务",
        "removed_waiting_jobs": removed_jobs,
    }


def _start_resume_task_for_result(user: dict, result: dict, output_dir: Path, request: Request) -> dict:
    workflow_config = result.get("workflow_config") or {}
    voice_cfg = workflow_config.get("voice_preset", {}) or {}
    avatar_cfg = workflow_config.get("avatar", {}) or {}
    target_market = workflow_config.get("target_market", "cn")
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": result.get("topic", ""),
        "image_path": "",
        "tracker": tracker,
        "output_dir": str(output_dir),
        "result": result,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "workflow_config": {
            "voice_preset_id": voice_cfg.get("id"),
            "avatar_id": avatar_cfg.get("id"),
            "speed": voice_cfg.get("selected_speed", 1.1),
            "web_search_enabled": workflow_config.get("web_search_enabled", False),
            "target_market": target_market,
            "department_id": workflow_config.get("department_id", "real_estate"),
            "compose_transition_id": workflow_config.get("compose_transition_id", "fade"),
            "subtitle_template_id": workflow_config.get("subtitle_template_id", "classic"),
            "voice_preset": voice_cfg,
            "avatar": avatar_cfg,
            "digital_human_engine": _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), user),
        },
        "cost_entries": list(result.get("cost_entries", [])),
        "cost_summary": result.get("cost_summary", _empty_cost_summary()),
    }
    tracker.log("已从失败位置恢复任务，准备继续补齐中间结果")
    thread = threading.Thread(target=run_resume_pipeline_with_progress, args=(task_id,), daemon=True)
    thread.start()
    return {"task_id": task_id, "reused_existing": False}


@app.post("/api/tasks/{task_id}/retry")
async def retry_failed_task(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    task = tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, task):
        return _forbidden_error()

    tracker = task.get("tracker")
    if not tracker or tracker.status != "error":
        return JSONResponse({"error": "只有失败任务可以重试"}, status_code=400)

    output_dir = Path(str(task.get("output_dir") or ""))
    if not output_dir.exists():
        return JSONResponse({"error": "失败任务缺少可恢复的输出目录"}, status_code=400)
    result = _load_result_from_output_dir(output_dir)
    if not result:
        return JSONResponse({"error": "失败任务缺少恢复检查点，请从历史任务或文案重新开始"}, status_code=400)

    lifecycle = _build_history_lifecycle(output_dir, result)
    if lifecycle.get("live_task_id"):
        return {
            "task_id": lifecycle.get("live_task_id", ""),
            "reused_existing": True,
            "message": "这条任务已经在后台继续执行中",
        }
    if not lifecycle.get("can_resume_production"):
        return JSONResponse({"error": "这条任务当前没有可继续的中间产物"}, status_code=400)

    return _start_resume_task_for_result(user, result, output_dir, request)


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
    workflow_config = task.get("workflow_config", {}) or result.get("workflow_config", {}) or {}
    digital_human_engine = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), user)
    video_output = os.path.join(
        output_dir,
        "digital_human",
        f"dh_{segment_index - 1:02d}_regen_{int(time.time())}.mp4",
    )
    video_path = _run_omnihuman_job_with_retry(
        task_id=task_id,
        job_id=f"{task_id}:regen:{segment_index}",
        label=f"数字人重生成（第{segment_index}段）：{_digital_human_engine_label(digital_human_engine)}",
        tracker=task.get("tracker"),
        runner=lambda: _generate_digital_human_video_by_engine(
            engine_id=digital_human_engine,
            image_url=image_url,
            image_path=image_path,
            audio_url=audio_url,
            audio_path=audio_path,
            output_path=video_output,
            prompt=_combine_prompt(_get_avatar_prompt_for_task(task), segment.get("action", "")),
            task_id=task_id,
            segment_index=segment_index,
        ),
    )
    segment["video_path"] = video_path
    segment["digital_human_engine"] = digital_human_engine
    _record_cost_entry(
        event_type="digital_human_generate",
        amount=_estimate_digital_human_cost(segment.get("duration", 0)),
        provider=_digital_human_engine_label(digital_human_engine),
        task=task,
        meta={"scope": "regenerate_segment", "segment_index": segment_index, "duration": segment.get("duration", 0), "video_path": video_path, "video_duration": _probe_media_duration(video_path)},
    )
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
    topic_text: str = Form(""),
    source_url: str = Form(""),
    topic: str = Form(""),
    script_json: str = Form(...),
    segment_index: int = Form(...),
    instruction: str = Form(...),
    use_web_search: str = Form("false"),
    target_market: str = Form("cn"),
    department_id: str = Form("real_estate"),
    script_model: str = Form(SCRIPT_MODEL_CLAUDE),
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

    source_info = analyze_topic_fields(topic_text=topic_text, source_url=source_url, fallback_topic=topic)
    source_ready, source_error = _source_ready_for_script(source_info)
    if not source_ready:
        return JSONResponse({"error": source_error, "source": source_info}, status_code=422)
    generation_topic = _build_source_generation_topic(source_info, topic_text=topic_text, fallback_topic=topic)
    selected_script_model = _normalize_script_model(script_model, user)
    web_search_enabled = _parse_bool_form(use_web_search) or source_info.get("kind") == "news"
    try:
        revised_segment = _run_script_ai_job(
            job_id=f"revise:{user.get('username', 'guest')}:{time.time_ns()}",
            label="AI 修改",
            runner=lambda: revise_script_segment(generation_topic, script_data, segment_index - 1, instruction.strip(), enable_web_search=web_search_enabled, target_market=target_market, department_id=department_id, provider=selected_script_model),
        )
        revise_usage = (revised_segment.pop("_meta", {}) or {}).get("usage", {})
    except Exception as exc:
        message, status_code = _friendly_ai_error_message(exc, "AI 修改")
        return JSONResponse({"error": message}, status_code=status_code)
    _record_cost_entry(
        event_type="script_revise",
        amount=_estimate_script_cost(instruction.strip(), {"segment": revised_segment}, web_search_enabled=web_search_enabled, revise=True, usage=revise_usage),
        provider=_script_model_label(selected_script_model),
        user=user,
        topic=topic,
        meta={"segment_index": segment_index, "web_search_enabled": web_search_enabled, "target_market": target_market, "department_id": department_id, "usage": revise_usage, "script_model": selected_script_model},
    )
    script_data["segments"][segment_index - 1] = revised_segment
    return {
        "script": script_data,
        "preview": _build_script_preview_payload(script_data, topic, web_search_enabled=web_search_enabled, target_market=target_market, department_id=department_id, script_model=selected_script_model),
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

    material_items = _segment_material_items(segment)
    if material_index < 0 or material_index >= len(material_items):
        return JSONResponse({"error": "素材不存在"}, status_code=404)

    removed_item = material_items.pop(material_index)
    removed_path = removed_item.get("path", "")
    resolved = _resolve_local_file(removed_path)
    if resolved and str(resolved).startswith(str(output_dir.resolve())) and resolved.exists():
        try:
            resolved.unlink()
        except OSError:
            pass

    segment["material_items"] = material_items
    segment["material_paths"] = [item.get("path", "") for item in material_items if item.get("path")]
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
    material_items = _segment_material_items(segment)
    for upload in images:
        if not upload.filename:
            continue
        ext = Path(upload.filename).suffix or ".jpg"
        filename = f"material_{segment_index:02d}_manual_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}{ext}"
        output_path = material_dir / filename
        with open(output_path, "wb") as f:
            f.write(await upload.read())
        kind = "video" if ext.lower() in {".mp4", ".mov", ".m4v", ".webm"} else "image"
        material_items.append({"path": str(output_path), "kind": kind})
    segment["material_items"] = material_items
    segment["material_paths"] = [item.get("path", "") for item in material_items if item.get("path")]
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {"result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))}


@app.post("/api/history/{history_id}/segments/{segment_index}/regenerate-audio")
async def regenerate_history_segment_audio(history_id: str, segment_index: int, request: Request):
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
    script_text = str(segment.get("script") or "").strip()
    if not script_text:
        return JSONResponse({"error": "该段缺少可用文案"}, status_code=400)

    workflow_config = result.get("workflow_config", {}) or {}
    target_market = workflow_config.get("target_market", "cn")
    voice_cfg = workflow_config.get("voice_preset", {}) or {}
    voice_preset = _get_voice_preset(voice_cfg.get("id"), target_market)
    tts_voice = voice_preset.get("voice_id")
    if not tts_voice:
        return JSONResponse({"error": "当前任务缺少可用配音方案"}, status_code=400)
    tts_speed = float(voice_cfg.get("selected_speed", voice_preset.get("default_speed", 1.1)))
    tts_volume = float(voice_cfg.get("selected_volume", voice_preset.get("default_volume", 1.0)))
    tts_language = voice_cfg.get("language", voice_preset.get("language", ""))

    from generate_audio import generate_audio
    from tos_uploader import upload_file_and_get_url

    audio_dir = output_dir / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)
    seg_type = segment.get("type", "segment")
    audio_path = audio_dir / f"segment_{segment_index - 1:02d}_{seg_type}.mp3"

    try:
        generate_audio(
            script_text,
            str(audio_path),
            tts_voice,
            speed=tts_speed,
            volume=tts_volume,
            language=tts_language,
        )
    except Exception as exc:
        return JSONResponse({"error": f"重新生成配音失败：{exc}"}, status_code=500)

    segment["audio_path"] = str(audio_path)
    try:
        segment["audio_url"] = upload_file_and_get_url(str(audio_path), key_prefix="full/audio")
    except Exception:
        segment["audio_url"] = segment.get("audio_url", "")

    if segment.get("type") == "digital_human":
        segment["video_path"] = ""
        segment["video_url"] = ""

    result["final_video_path"] = ""
    result["subtitle_path"] = ""
    _record_history_cost(
        output_dir=output_dir,
        result=result,
        user=user,
        event_type="tts_generate",
        amount=_estimate_tts_cost(script_text, str(audio_path)),
        provider=COST_RULES["tts_generate"]["provider"],
        topic=result.get("topic", ""),
        meta={"segment_index": segment_index, "audio_path": str(audio_path), "scope": "regenerate_audio"},
    )
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {
        "message": "配音已重新生成",
        "segment": _serialize_segment(str(output_dir), result.get("topic", ""), segment, segment_index - 1),
        "result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", "")),
    }


@app.post("/api/history/{history_id}/segments/{segment_index}/regenerate-digital-human")
async def regenerate_history_segment_digital_human(history_id: str, segment_index: int, request: Request):
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
    if segment.get("type") != "digital_human":
        return JSONResponse({"error": "只有数字人段支持重新生成"}, status_code=400)

    audio_path = str(segment.get("audio_path") or "").strip()
    if not audio_path or not os.path.exists(audio_path):
        return JSONResponse({"error": "该段缺少可用音频文件"}, status_code=400)

    workflow_config = result.get("workflow_config", {}) or {}
    target_market = workflow_config.get("target_market", "cn")
    avatar_cfg = workflow_config.get("avatar", {}) or {}
    avatar_option = _get_avatar_option(avatar_cfg.get("id"), target_market_id=target_market)
    image_path = avatar_option.get("image_path") if avatar_option else ""
    if not image_path or not os.path.exists(image_path):
        return JSONResponse({"error": "当前任务缺少可用的主播图片"}, status_code=400)

    from tos_uploader import upload_file_and_get_url

    try:
        image_url = upload_file_and_get_url(image_path, key_prefix="full/image")
        audio_url = segment.get("audio_url") or upload_file_and_get_url(audio_path, key_prefix="full/audio")
        segment["audio_url"] = audio_url
    except Exception as exc:
        return JSONResponse({"error": f"准备数字人素材失败：{exc}"}, status_code=500)

    digital_human_dir = output_dir / "digital_human"
    digital_human_dir.mkdir(parents=True, exist_ok=True)
    video_output = digital_human_dir / f"dh_{segment_index - 1:02d}_regen_{int(time.time())}.mp4"
    digital_human_engine = _normalize_digital_human_engine(workflow_config.get("digital_human_engine"), user)

    try:
        video_path = _run_omnihuman_job_with_retry(
            task_id=history_id,
            job_id=f"{history_id}:regen:{segment_index}",
            label=f"历史数字人重生成（第{segment_index}段）：{_digital_human_engine_label(digital_human_engine)}",
            tracker=None,
            runner=lambda: _generate_digital_human_video_by_engine(
                engine_id=digital_human_engine,
                image_url=image_url,
                image_path=image_path,
                audio_url=audio_url,
                audio_path=audio_path,
                output_path=str(video_output),
                prompt=_combine_prompt(avatar_option.get("style_prompt", "") if avatar_option else "", segment.get("action", "")),
                task_id=history_id,
                segment_index=segment_index,
            ),
        )
    except Exception as exc:
        return JSONResponse({"error": f"重新生成数字人视频失败：{exc}"}, status_code=500)

    segment["video_path"] = video_path
    segment["digital_human_engine"] = digital_human_engine
    result["final_video_path"] = ""
    result["subtitle_path"] = ""
    _record_history_cost(
        output_dir=output_dir,
        result=result,
        user=user,
        event_type="digital_human_generate",
        amount=_estimate_digital_human_cost(segment.get("duration", 0)),
        provider=_digital_human_engine_label(digital_human_engine),
        topic=result.get("topic", ""),
        meta={"segment_index": segment_index, "video_path": video_path, "scope": "regenerate_digital_human", "duration": segment.get("duration", 0), "video_duration": _probe_media_duration(video_path)},
    )
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {
        "message": "数字人视频已重新生成",
        "segment": _serialize_segment(str(output_dir), result.get("topic", ""), segment, segment_index - 1),
        "result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", "")),
    }


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

    transition_id = "fade"
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
    _record_history_cost(
        output_dir=output_dir,
        result=result,
        user=user,
        event_type="compose_video",
        amount=_estimate_compose_cost(result.get("total_duration", 0)),
        provider=COST_RULES["compose_video"]["provider"],
        topic=result.get("topic", ""),
        meta={"transition_id": transition_id, "subtitle_template_id": subtitle_template_id},
    )
    with open(output_dir / "result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    _sync_live_task_result(str(output_dir), result)
    return {"ok": True, "result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))}


@app.post("/api/history/{history_id}/resume")
async def resume_history_production_endpoint(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error

    lifecycle = _build_history_lifecycle(output_dir, result)
    if lifecycle.get("live_task_id"):
        return {
            "task_id": lifecycle.get("live_task_id", ""),
            "reused_existing": True,
            "message": "这条任务已经在后台继续执行中",
        }
    if lifecycle.get("can_compose") and not lifecycle.get("can_resume_production"):
        return JSONResponse({"error": "这条任务已经完成中间产物，请直接生成成片"}, status_code=400)
    if not lifecycle.get("can_resume_production"):
        return JSONResponse({"error": "这条历史任务当前不需要继续生产"}, status_code=400)

    return _start_resume_task_for_result(user, result, output_dir, request)


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
