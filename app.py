"""
iHouse 视频自动化生产系统 - Web 应用
FastAPI + SSE 实时进度推送
"""

import csv
import base64
import asyncio
from collections import deque
import hashlib
import hmac
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
from typing import Any, Optional
from urllib.parse import quote, urlparse
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.sessions import SessionMiddleware

load_dotenv(override=False)

from avatar_generator import AvatarGenerationError, generate_avatar_candidates
from ai_material_harvester import (
    NEWS_HARVEST_PRESETS,
    clear_harvest_candidates,
    create_harvest_job,
    delete_harvest_candidate,
    import_harvest_candidate_to_material_library,
    list_harvest_candidates,
    list_harvest_jobs,
    run_harvest_job_async,
    update_harvest_candidate,
)
from material_library import (
    MATERIAL_CATEGORIES,
    MATERIAL_LIBRARY_DIR,
    AUDIO_SUFFIXES,
    batch_delete_material_library_items,
    batch_update_material_library_items,
    delete_material_library_item,
    list_material_library_items,
    material_item_matches_filters,
    register_material_file,
    update_material_library_item,
)
from property_video_workflow import PROPERTY_VIDEO_EXTENSIONS, build_property_video
from property_video_vision import analyze_property_video_with_openai
from floorplan_nav import (
    IMAGE_SUFFIXES as FLOORPLAN_NAV_IMAGE_SUFFIXES,
    VIDEO_SUFFIXES as FLOORPLAN_NAV_VIDEO_SUFFIXES,
    create_floorplan_nav_job,
    load_floorplan_nav_job,
    run_floorplan_nav_job_async,
    save_floorplan_nav_job,
)
from opennews_admin import (
    build_opennews_script_data,
    category_payloads as opennews_category_payloads,
    generate_opennews_draft,
    save_opennews_payload,
    search_opennews_candidates,
    search_opennews_candidates_with_stats,
    source_payloads as opennews_source_payloads,
)
from opennews_trends import (
    search_english_trends,
    trend_category_payloads as opennews_trend_category_payloads,
    trend_time_range_payloads as opennews_trend_time_range_payloads,
)
from opennews_scheduler import (
    list_auto_candidates as list_opennews_auto_candidates,
    load_auto_config as load_opennews_auto_config,
    run_auto_fetch_once,
    save_auto_config as save_opennews_auto_config,
    update_auto_candidate_status,
)
from opennews_batch import (
    create_batch_job as create_opennews_batch_job,
    find_batch_items as find_opennews_batch_items,
    list_batch_jobs as list_opennews_batch_jobs,
    list_batches as list_opennews_batches,
    load_batch_config as load_opennews_batch_config,
    load_batch_job as load_opennews_batch_job,
    mark_batch_items as mark_opennews_batch_items,
    run_batch_fetch_once as run_opennews_batch_fetch_once,
    save_batch_config as save_opennews_batch_config,
    set_after_fetch_callback as set_opennews_batch_after_fetch_callback,
    start_batch_scheduler as start_opennews_batch_scheduler,
    update_batch_job as update_opennews_batch_job,
)
from opennews_collections import (
    build_collection_video,
    create_collection_job,
    ensure_collection_auto_started_at,
    list_collection_jobs,
    list_collection_pool,
    load_collection_job,
    update_collection_job,
)
from localtok_client import (
    LocalTokError,
    get_decision as get_localtok_decision,
    get_used_titles as get_localtok_used_titles,
    localtok_status,
    propose_news as propose_localtok_news,
    publish_video as publish_localtok_video,
)
from opennews_localtok import (
    create_proposal as create_localtok_proposal,
    list_proposals as list_localtok_proposals,
    load_proposal as load_localtok_proposal,
    make_local_proposal_id,
    update_proposal as update_localtok_proposal,
)
from source_ingest import analyze_topic_fields, analyze_topic_input
from youtube_publisher import (
    YOUTUBE_SCOPE,
    YouTubePublishError,
    exchange_youtube_code_for_tokens,
    get_youtube_channel,
    save_youtube_refresh_token,
    upload_video_to_youtube,
    youtube_env_config,
)

app = FastAPI(title="iHouse 内容工作台")
SESSION_SAME_SITE = os.getenv("SESSION_SAME_SITE", "lax").strip().lower()
if SESSION_SAME_SITE not in {"lax", "strict", "none"}:
    SESSION_SAME_SITE = "lax"
SESSION_HTTPS_ONLY = os.getenv("SESSION_HTTPS_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "ihouse-content-studio-session"),
    max_age=60 * 60 * 24 * 30,
    same_site=SESSION_SAME_SITE,
    https_only=SESSION_HTTPS_ONLY,
)


BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

JCLAW_HANDOFF_SECRET = os.getenv("JCLAW_AI_AGENT_HANDOFF_SECRET", "").strip()
JCLAW_HANDOFF_ISSUER = os.getenv("JCLAW_AI_AGENT_HANDOFF_ISSUER", "jclaw").strip()
JCLAW_HANDOFF_AUDIENCE = os.getenv("JCLAW_AI_AGENT_HANDOFF_AUDIENCE", "aiagent.office.ihousejapan.cn").strip()
JCLAW_HANDOFF_PURPOSE = "ai-agent-handoff"
JCLAW_HANDOFF_CLOCK_SKEW_SECONDS = max(0, int(os.getenv("JCLAW_AI_AGENT_HANDOFF_CLOCK_SKEW_SECONDS", "30")))
JCLAW_HANDOFF_CONSUMED_JTIS: dict[str, float] = {}
JCLAW_HANDOFF_USER_MAP: dict[str, str] = {}

tasks = {}
OPENNEWS_DRAFT_JOBS: dict[str, dict[str, Any]] = {}
OPENNEWS_DRAFT_LOCK = threading.Lock()
YOUTUBE_UPLOAD_JOBS: dict[str, dict[str, Any]] = {}
YOUTUBE_UPLOAD_LOCK = threading.Lock()
OPENNEWS_COLLECTION_AUTO_LOCK = threading.Lock()
OPENNEWS_BATCH_AUTO_PRODUCE_LOCK = threading.Lock()
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
FLOORPLAN_NAV_JOBS_DIR = OUTPUT_DIR / "admin_floorplan_nav_jobs"
OPENNEWS_ADMIN_DIR = OUTPUT_DIR / "admin_opennews"
OPENNEWS_AUTO_DIR = OUTPUT_DIR / "opennews_auto"
OPENNEWS_BATCH_DIR = OUTPUT_DIR / "opennews_batches"
OPENNEWS_COLLECTION_DIR = OUTPUT_DIR / "opennews_collections"
OPENNEWS_LOCALTOK_DIR = OUTPUT_DIR / "opennews_localtok"
YOUTUBE_AUTH_DIR = OUTPUT_DIR / "youtube_auth"
YOUTUBE_TOKEN_STORE_PATH = YOUTUBE_AUTH_DIR / "youtube_token.json"
FLOORPLAN_NAV_JOBS_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_AUTO_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_BATCH_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_COLLECTION_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_LOCALTOK_DIR.mkdir(parents=True, exist_ok=True)
YOUTUBE_AUTH_DIR.mkdir(parents=True, exist_ok=True)


@app.on_event("startup")
async def _start_opennews_batch_scheduler() -> None:
    ensure_collection_auto_started_at(OPENNEWS_COLLECTION_DIR)
    set_opennews_batch_after_fetch_callback(_handle_opennews_batch_after_fetch)
    start_opennews_batch_scheduler(OPENNEWS_BATCH_DIR, poll_seconds=20)

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
        "id": "ricky_clone",
        "name": "Ricky 音色",
        "subtitle": "中文克隆男声",
        "gender": "male",
        "language": "zh-CN",
        "style": "使用 Ricky 克隆声音，适合房源实拍解说、客户介绍和专业讲解。",
        "voice_id": os.getenv("VOICE_RICKY_CLONE", "moss_audio_8b8f2575-5814-11f1-9bad-16a399225e91"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["男声", "克隆", "Ricky"],
        "sample_text": "大家好，我来带你快速看一下这套房子的实际空间和重点细节。",
        "enabled": True,
        "availability_note": "已启用",
    },
    {
        "id": "bin_clone",
        "name": "Bin 音色",
        "subtitle": "中文克隆男声",
        "gender": "male",
        "language": "zh-CN",
        "style": "使用 Bin 克隆声音，适合房源实拍解说、销售跟进和客户沟通。",
        "voice_id": os.getenv("VOICE_BIN_CLONE", "moss_audio_aac68cec-5811-11f1-9d84-fa57111a9d42"),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["男声", "克隆", "Bin"],
        "sample_text": "大家好，下面我带你按顺序看看这套房子的空间布局和居住感受。",
        "enabled": True,
        "availability_note": "已启用",
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
    "bin": {
        "password": "bin123",
        "role": "user",
        "display_name": "bin",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "ricky": {
        "password": "ricky123",
        "role": "user",
        "display_name": "ricky",
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
    "da": {
        "password": "da123",
        "role": "user",
        "display_name": "da",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "liyh": {
        "password": "liyh123",
        "role": "user",
        "display_name": "liyh",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "zhoubing": {
        "password": "zhoubing123",
        "role": "user",
        "display_name": "zhoubing",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "ikemoto": {
        "password": "ikemoto123",
        "role": "user",
        "display_name": "ikemoto",
        "interface_language": "ja-JP",
        "department_id": "robotics",
        "target_market": "jp",
    },
    "zck": {
        "password": "zck123",
        "role": "user",
        "display_name": "zck",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "saita": {
        "password": "saita123",
        "role": "admin",
        "display_name": "saita",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "han": {
        "password": "han123",
        "role": "user",
        "display_name": "han",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "sunqinxue": {
        "password": "sunqinxue123",
        "role": "user",
        "display_name": "sunqinxue",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "aki": {
        "password": "aki123",
        "role": "admin",
        "display_name": "aki",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "baicy": {
        "password": "baicy123",
        "role": "user",
        "display_name": "baicy",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "lidj": {
        "password": "lidj123",
        "role": "user",
        "display_name": "lidj",
        "interface_language": "zh-CN",
        "department_id": "real_estate",
        "target_market": "cn",
    },
    "zhaozy": {
        "password": "zhaozy123",
        "role": "user",
        "display_name": "zhaozy",
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
INFINITETALK_ENGINE_ID = "infinitetalk_local"
VOLC_ENGINE_ID = "volc_omnihuman"
SCRIPT_MODEL_CLAUDE = "claude"
SCRIPT_MODEL_API_RELAY = "api_relay"
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
    {
        "id": INFINITETALK_ENGINE_ID,
        "name": "5090 本地 InfiniteTalk",
        "description": "测试功能：480P + fp8 + 整段音频时长透传，默认嘴型更收、速度更快。",
        "admin_only": True,
        "default": False,
    },
]
SCRIPT_MODEL_OPTIONS = [
    {
        "id": SCRIPT_MODEL_CLAUDE,
        "name": "Claude",
        "description": "恢复为主文案模型：中文口播和新闻转写更自然，支持实时联网检索。",
        "admin_only": False,
        "default": True,
    },
    {
        "id": SCRIPT_MODEL_API_RELAY,
        "name": "API中转模型",
        "description": "管理员测试：走 sub2api 中转站 Responses 接口，默认 gpt-5.5。",
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
OPENNEWS_QWEN_TTS_ENABLED = (os.getenv("OPENNEWS_QWEN_TTS_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_QWEN_TTS_BASE_URL = (os.getenv("OPENNEWS_QWEN_TTS_BASE_URL") or "http://192.168.0.34:8895").strip().rstrip("/")
OPENNEWS_QWEN_TTS_TOKEN = os.getenv("OPENNEWS_QWEN_TTS_TOKEN", "local-qwen3-tts-5090").strip()
OPENNEWS_QWEN_TTS_SPEAKER = os.getenv("OPENNEWS_QWEN_TTS_SPEAKER", "serena").strip() or "serena"
OPENNEWS_QWEN_TTS_LANGUAGE = os.getenv("OPENNEWS_QWEN_TTS_LANGUAGE", "chinese").strip() or "chinese"
OPENNEWS_QWEN_TTS_TIMEOUT = max(15, int(os.getenv("OPENNEWS_QWEN_TTS_TIMEOUT", "180") or "180"))
OPENNEWS_QWEN_TTS_FALLBACK_MINIMAX = (os.getenv("OPENNEWS_QWEN_TTS_FALLBACK_MINIMAX", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_QWEN_TTS_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_INSTRUCT",
    "用自然、清晰、专业的中文新闻女主播语气朗读，节奏稳定，声音有亲和力。",
).strip()
OPENNEWS_COLLECTION_INTRO_ENABLED = (os.getenv("OPENNEWS_COLLECTION_INTRO_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH = ASSETS_DIR / os.getenv("OPENNEWS_COLLECTION_INTRO_ANCHOR_FILENAME", "opennews_anchor_daily.png").strip()
OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED = (
    (os.getenv("OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED", "1") or "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES = [
    part.strip()
    for part in (os.getenv("OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES", INFINITETALK_ENGINE_ID) or "").split(",")
    if part.strip()
]

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
    width = int(payload.get("width") or 0)
    height = int(payload.get("height") or 0)
    payload["resolution_label"] = f"{width}×{height}" if width and height else ""
    payload["duration_label"] = (
        f"{round(float(payload.get('duration_seconds') or 0), 1):g} 秒"
        if str(payload.get("kind") or "") in {"video", "audio"} and float(payload.get("duration_seconds") or 0) > 0
        else ""
    )
    payload["source_url"] = str(payload.get("source_url") or "")
    payload["source_site"] = str(payload.get("source_site") or "")
    payload["license_note"] = str(payload.get("license_note") or "")
    payload["safety_status"] = str(payload.get("safety_status") or "unchecked")
    payload["news_topics"] = payload.get("news_topics") or []
    payload["usage_count"] = int(payload.get("usage_count") or 0)
    payload["last_used_at"] = float(payload.get("last_used_at") or 0)
    payload["can_review"] = bool(current_user and _is_admin(current_user))
    payload["can_delete"] = bool(
        current_user
        and (
            _is_admin(current_user)
            or str(payload.get("uploader_username") or "") == str(current_user.get("username") or "")
        )
    )
    return payload


def _property_bgm_track_payloads() -> list[dict]:
    tracks = []
    for item in list_material_library_items(status="approved"):
        if str(item.get("kind") or "") != "audio":
            continue
        payload = _material_library_item_payload(item, {"role": "admin"})
        payload["name"] = payload.get("title") or payload.get("original_filename") or "BGM"
        tracks.append(payload)
    return tracks


def _get_approved_bgm_path(item_id: str) -> Optional[Path]:
    normalized_id = str(item_id or "").strip()
    if not normalized_id:
        return None
    for item in list_material_library_items(status="approved"):
        if str(item.get("id") or "") != normalized_id or str(item.get("kind") or "") != "audio":
            continue
        filename = Path(str(item.get("filename") or "")).name
        full_path = (MATERIAL_LIBRARY_DIR / filename).resolve()
        if str(full_path).startswith(str(MATERIAL_LIBRARY_DIR.resolve())) and full_path.exists():
            return full_path
    return None


def _harvest_job_payload(job: dict) -> dict:
    payload = dict(job or {})
    payload["source_count"] = len(payload.get("source_urls") or []) + len(payload.get("discovered_source_urls") or [])
    payload["manual_source_count"] = len(payload.get("source_urls") or [])
    payload["discovered_source_count"] = len(payload.get("discovered_source_urls") or [])
    return payload


def _harvest_candidate_payload(candidate: dict, current_user: Optional[dict] = None) -> dict:
    payload = dict(candidate or {})
    payload["preview_url"] = str(payload.get("asset_url") or "").strip()
    payload["category"] = str(payload.get("category") or "")
    payload["tags"] = payload.get("tags") or []
    payload["source_site"] = str(payload.get("source_site") or payload.get("domain") or "")
    payload["safety_status"] = str(payload.get("safety_status") or "needs_review")
    payload["license_note"] = str(payload.get("license_note") or "")
    payload["can_import"] = bool(current_user and _is_admin(current_user) and payload.get("status") == "pending")
    payload["can_reject"] = bool(current_user and _is_admin(current_user) and payload.get("status") == "pending")
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
    base_ids = {"ricky_clone", "bin_clone"}
    if target_market_id == "tw":
        return {"mandarin_female", "mandarin_male", "taiwan_female", "taiwan_clone", "japanese_female"} | base_ids
    if target_market_id == "jp":
        return {"mandarin_female", "mandarin_male", "japanese_female"} | base_ids
    return {"mandarin_female", "mandarin_male"} | base_ids


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
    if requested == "opennews_material_only":
        return "opennews_material_only"
    if not _is_admin(user):
        return INFINITETALK_ENGINE_ID
    if requested == HUNYUAN_ENGINE_ID and _is_admin(user):
        return HUNYUAN_ENGINE_ID
    if requested == INFINITETALK_ENGINE_ID and _is_admin(user):
        return INFINITETALK_ENGINE_ID
    return VOLC_ENGINE_ID


def _digital_human_engine_label(engine_id: str | None) -> str:
    if (engine_id or "").strip() == "opennews_material_only":
        return "无数字人（素材成片）"
    admin_user = {"role": "admin"} if engine_id in {HUNYUAN_ENGINE_ID, INFINITETALK_ENGINE_ID} else None
    normalized = _normalize_digital_human_engine(engine_id, admin_user)
    for item in DIGITAL_HUMAN_ENGINES:
        if item["id"] == normalized:
            return item["name"]
    return "火山 OmniHuman"


def _digital_human_engine_options_for_user(user: Optional[dict]) -> list[dict]:
    if _is_admin(user):
        return DIGITAL_HUMAN_ENGINES
    return [
        {
            "id": INFINITETALK_ENGINE_ID,
            "name": "5090 本地 InfiniteTalk",
            "description": "员工默认：走本地 5090 数字人队列，按整段音频时长生成，默认小嘴型并带自动重试。",
            "admin_only": False,
            "default": True,
        }
    ]


def _normalize_script_model(model_id: str | None, user: Optional[dict] = None) -> str:
    requested = str(model_id or "").strip().lower()
    if requested == SCRIPT_MODEL_API_RELAY and _is_admin(user):
        return SCRIPT_MODEL_API_RELAY
    return SCRIPT_MODEL_CLAUDE


def _script_model_label(model_id: str | None) -> str:
    normalized = _normalize_script_model(model_id, {"role": "admin"} if model_id == SCRIPT_MODEL_API_RELAY else None)
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
    if engine_id == INFINITETALK_ENGINE_ID:
        from infinitetalk_avatar_client import generate_infinitetalk_avatar_video

        return generate_infinitetalk_avatar_video(
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
                "compose_aspect_ratio": workflow_config.get("compose_aspect_ratio") or workflow_config.get("aspect_ratio") or "vertical",
                "source": workflow_config.get("source") or {},
                "opennews": bool(workflow_config.get("opennews")),
                "opennews_material_only": bool(workflow_config.get("opennews_material_only")),
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
            audio_path, tts_provider = _generate_audio_for_workflow(
                script_text=script_text,
                audio_path=audio_path,
                voice=tts_voice,
                speed=tts_speed,
                volume=tts_volume,
                language=voice_preset.get("language", ""),
                workflow_config=workflow_config,
                generate_audio_fn=generate_audio,
                log=tracker.log,
            )
            seg_with_audio = dict(seg)
            seg_with_audio["audio_path"] = audio_path
            seg_with_audio["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
            seg_with_audio["tts_provider"] = tts_provider
            seg_with_audio["target_market"] = target_market
            seg_with_audio["department_id"] = department_id
            audio_segments.append(seg_with_audio)
            _record_cost_entry(
                event_type="tts_generate",
                amount=_estimate_tts_cost(script_text, audio_path),
                provider=tts_provider,
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
            "tts_provider": "qwen3-tts" if any((seg.get("tts_provider") == "qwen3-tts") for seg in audio_segments) else "",
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
                "compose_aspect_ratio": workflow_config.get("compose_aspect_ratio") or workflow_config.get("aspect_ratio") or "vertical",
                "source": workflow_config.get("source") or {},
                "opennews": bool(workflow_config.get("opennews")),
                "opennews_material_only": bool(workflow_config.get("opennews_material_only")),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
            },
            "image_path": image_path,
            "image_url": image_url,
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }
        _persist_production_checkpoint(task, checkpoint_result, stage="digital_human")

        dh_segments = [seg for seg in audio_segments if seg.get("type") == "digital_human"]
        tracker.log("正在生成数字人视频..." if dh_segments else "当前脚本无数字人段，跳过数字人生成", step=3)
        if not dh_segments:
            segments_with_dh = audio_segments
        elif not image_url:
            tracker.log("未选择数字人主播图，跳过数字人视频生成")
            segments_with_dh = audio_segments
        else:
            segments_with_dh = []
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
            material_group_count = sum(1 for seg in final_segments if seg.get("material_paths"))
            if (
                (workflow_config.get("opennews") or workflow_config.get("opennews_material_only") or digital_human_engine == "opennews_material_only")
                and not _opennews_result_has_material_assets({"segments": final_segments}, Path(output_dir))
            ):
                raise RuntimeError("OpenNews 素材为空：5090 AI图片和严格新闻源兜底都没有拿到可用素材，已中止以避免生成白底占位视频。")
            tracker.log(f"素材匹配完成，共 {material_group_count} 组素材")
        except TaskCancelled:
            raise
        except Exception as exc:
            if workflow_config.get("opennews") or workflow_config.get("opennews_material_only") or digital_human_engine == "opennews_material_only":
                raise RuntimeError(f"OpenNews 素材匹配失败，已中止成片：{exc}") from exc
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


def run_property_video_with_progress(
    task_id: str,
    uploaded_video_paths: list[str],
    script_text: str,
    voice_preset: dict,
    target_market: str,
    speed: float,
    bgm_item_id: str = "",
    bgm_volume: float = 0.10,
    timeline_segments: Optional[list[dict]] = None,
):
    tracker = tasks[task_id]["tracker"]
    tracker.total_steps = 4
    task = tasks[task_id]
    try:
        from generate_audio import generate_audio

        output_dir = Path(task["output_dir"])
        bgm_path = _get_approved_bgm_path(bgm_item_id)
        result = build_property_video(
            output_dir=output_dir,
            uploaded_video_paths=[Path(path) for path in uploaded_video_paths],
            script_text=script_text,
            voice_id=voice_preset.get("voice_id") or voice_preset.get("id") or "",
            voice_preset=voice_preset,
            speed=speed,
            target_market=target_market,
            bgm_path=bgm_path,
            bgm_volume=bgm_volume,
            timeline_segments=timeline_segments,
            generate_audio_fn=generate_audio,
            log=lambda message, step=None: tracker.log(message, step=step),
        )
        result["owner_username"] = task.get("owner_username")
        result["owner_display_name"] = task.get("owner_display_name")
        result["owner_role"] = task.get("owner_role")
        task["result"] = result
        task["topic"] = result.get("title") or "房源实拍成片"
        _persist_task_result(task)
        tracker.finish(result)
        _push_live_event("task_completed", "房源实拍成片已完成", task, {"scope": "property_video"})
    except Exception as exc:
        tracker.fail(str(exc))
        _push_live_event("task_failed", str(exc), task, {"scope": "property_video"})
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
        script_model = _normalize_script_model(workflow_config.get("script_model"), task)
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
                audio_path, tts_provider = _generate_audio_for_workflow(
                    script_text=script_text,
                    audio_path=audio_path,
                    voice=tts_voice,
                    speed=tts_speed,
                    volume=tts_volume,
                    language=voice_cfg.get("language", voice_preset.get("language", "")),
                    workflow_config=workflow_config,
                    generate_audio_fn=generate_audio,
                    log=tracker.log,
                )
                seg["audio_path"] = audio_path
                seg["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
                seg["tts_provider"] = tts_provider
                _record_cost_entry(
                    event_type="tts_generate",
                    amount=_estimate_tts_cost(script_text, audio_path),
                    provider=tts_provider,
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
            "tts_provider": "qwen3-tts" if any((seg.get("tts_provider") == "qwen3-tts") for seg in audio_segments) else "",
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
            "compose_aspect_ratio": workflow_config.get("compose_aspect_ratio") or workflow_config.get("aspect_ratio") or "vertical",
            "source": workflow_config.get("source") or {},
            "opennews": bool(workflow_config.get("opennews")),
            "opennews_material_only": bool(workflow_config.get("opennews_material_only")),
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
        "connection refused",
        "remote end closed",
        "temporarily unavailable",
        "service unavailable",
        "bad gateway",
        "health check failed",
        "cuda-capable device",
        "device(s) is/are busy",
        "gpu",
        "hunyuan",
        "infinitetalk",
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


def _should_use_qwen_tts_for_workflow(workflow_config: dict) -> bool:
    if not OPENNEWS_QWEN_TTS_ENABLED:
        return False
    source = workflow_config.get("source") or {}
    source_kind = str(source.get("kind") or "").strip().lower() if isinstance(source, dict) else ""
    engine = str(workflow_config.get("digital_human_engine") or "").strip().lower()
    return (
        bool(workflow_config.get("opennews"))
        or bool(workflow_config.get("opennews_material_only"))
        or engine == "opennews_material_only"
        or source_kind == "opennews"
    )


def _generate_opennews_qwen_tts_audio(script_text: str, output_path: str) -> str:
    if not OPENNEWS_QWEN_TTS_BASE_URL or not OPENNEWS_QWEN_TTS_TOKEN:
        raise RuntimeError("OpenNews Qwen3-TTS 未配置 base_url 或 token")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    headers = {
        "X-Token": OPENNEWS_QWEN_TTS_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {
        "text": script_text,
        "language": OPENNEWS_QWEN_TTS_LANGUAGE,
        "speaker": OPENNEWS_QWEN_TTS_SPEAKER,
        "instruct": OPENNEWS_QWEN_TTS_INSTRUCT,
    }
    response = requests.post(
        f"{OPENNEWS_QWEN_TTS_BASE_URL}/tts",
        headers=headers,
        json=payload,
        timeout=OPENNEWS_QWEN_TTS_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok") or not data.get("url"):
        raise RuntimeError(f"Qwen3-TTS 返回异常：{data}")
    audio_url = str(data["url"])
    if audio_url.startswith("/"):
        audio_url = f"{OPENNEWS_QWEN_TTS_BASE_URL}{audio_url}"
    wav_response = requests.get(
        audio_url,
        headers={"X-Token": OPENNEWS_QWEN_TTS_TOKEN},
        timeout=OPENNEWS_QWEN_TTS_TIMEOUT,
    )
    wav_response.raise_for_status()
    wav_path = output.with_suffix(output.suffix + ".qwen.wav")
    wav_path.write_bytes(wav_response.content)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(wav_path),
            "-vn",
            "-ar",
            "32000",
            "-ac",
            "1",
            "-b:a",
            "128k",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    try:
        wav_path.unlink()
    except Exception:
        pass
    return str(output)


def _generate_audio_for_workflow(
    *,
    script_text: str,
    audio_path: str,
    voice: str,
    speed: float,
    volume: float,
    language: str,
    workflow_config: dict,
    generate_audio_fn,
    log=None,
) -> tuple[str, str]:
    if _should_use_qwen_tts_for_workflow(workflow_config):
        try:
            if log:
                log(f"OpenNews 使用 5090 Qwen3-TTS 本地配音：{OPENNEWS_QWEN_TTS_SPEAKER}")
            _generate_opennews_qwen_tts_audio(script_text, audio_path)
            return audio_path, "qwen3-tts"
        except Exception as exc:
            if not OPENNEWS_QWEN_TTS_FALLBACK_MINIMAX:
                raise
            if log:
                log(f"Qwen3-TTS 配音失败，已回退 MiniMax：{exc}")
    generate_audio_fn(
        script_text,
        audio_path,
        voice,
        speed=speed,
        volume=volume,
        language=language,
    )
    return audio_path, COST_RULES["tts_generate"]["provider"]


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
    if username:
        profile = USERS.get(username)
        if not profile:
            request.session.pop("username", None)
            return None
        return _public_user(username, profile)
    bearer_user = _verify_app_api_token(_bearer_token_from_request(request))
    if bearer_user:
        return bearer_user
    return None


def _auth_error(message: str = "请先登录") -> JSONResponse:
    return JSONResponse({"error": message}, status_code=401)


def _base64url_encode_json(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _base64url_decode_json(value: str) -> dict[str, Any]:
    padding = "=" * (-len(value) % 4)
    raw = base64.urlsafe_b64decode(f"{value}{padding}".encode("utf-8"))
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("JWT 内容格式错误")
    return parsed


def _jwt_hs256_signature(signing_input: str, secret: str) -> str:
    return _jwt_hs256_signature_with_key(signing_input, secret.encode("utf-8"))


def _jwt_hs256_signature_with_key(signing_input: str, secret_key: bytes) -> str:
    digest = hmac.new(secret_key, signing_input.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")


def _app_api_token_secret() -> str:
    return os.getenv("APP_API_TOKEN_SECRET") or os.getenv("SESSION_SECRET") or "ihouse-content-studio-session"


def _create_app_api_token(username: str, *, ttl_seconds: Optional[int] = None) -> dict:
    now = int(time.time())
    ttl = int(ttl_seconds or int(os.getenv("APP_API_TOKEN_TTL_SECONDS", str(60 * 60 * 24 * 30))))
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": "ihouse-aiagent",
        "aud": "ihouse-app",
        "sub": username,
        "iat": now,
        "exp": now + max(60, ttl),
        "scope": "app",
    }
    signing_input = f"{_base64url_encode_json(header)}.{_base64url_encode_json(payload)}"
    token = f"{signing_input}.{_jwt_hs256_signature(signing_input, _app_api_token_secret())}"
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": payload["exp"],
        "expires_in": payload["exp"] - now,
    }


def _bearer_token_from_request(request: Request) -> str:
    auth = str(request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return ""


def _verify_app_api_token(token: str) -> Optional[dict]:
    parts = str(token or "").split(".")
    if len(parts) != 3 or not all(parts):
        return None
    header_b64, payload_b64, signature = parts
    try:
        header = _base64url_decode_json(header_b64)
        payload = _base64url_decode_json(payload_b64)
    except Exception:
        return None
    if header.get("alg") != "HS256":
        return None
    signing_input = f"{header_b64}.{payload_b64}"
    expected = _jwt_hs256_signature(signing_input, _app_api_token_secret())
    if not hmac.compare_digest(signature, expected):
        return None
    now = int(time.time())
    if payload.get("iss") != "ihouse-aiagent" or payload.get("aud") != "ihouse-app":
        return None
    if int(payload.get("exp") or 0) <= now:
        return None
    username = str(payload.get("sub") or "").strip()
    profile = USERS.get(username)
    if not profile:
        return None
    return _public_user(username, profile)


def _jclaw_handoff_secret_keys(secret: str) -> list[tuple[str, bytes]]:
    keys: list[tuple[str, bytes]] = [("plain", secret.encode("utf-8"))]
    padded = secret + ("=" * (-len(secret) % 4))
    try:
        decoded = base64.b64decode(padded, validate=True)
        if decoded:
            keys.append(("base64", decoded))
    except Exception:
        pass
    return keys


def _load_jclaw_user_map() -> dict[str, str]:
    raw_map = os.getenv("JCLAW_AI_AGENT_USER_MAP", "").strip()
    if not raw_map:
        return {}
    try:
        parsed = json.loads(raw_map)
    except json.JSONDecodeError:
        print("JCLAW_AI_AGENT_USER_MAP is not valid JSON; ignored", flush=True)
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key).strip().lower(): str(value).strip() for key, value in parsed.items() if str(key).strip() and str(value).strip()}


def _consume_jclaw_jti(jti: str, exp: int) -> None:
    now = int(time.time())
    expired = [key for key, expires_at in JCLAW_HANDOFF_CONSUMED_JTIS.items() if expires_at <= now]
    for key in expired:
        JCLAW_HANDOFF_CONSUMED_JTIS.pop(key, None)
    if not jti:
        raise ValueError("handoff token 缺少 jti")
    if jti in JCLAW_HANDOFF_CONSUMED_JTIS:
        raise ValueError("handoff token 已使用")
    JCLAW_HANDOFF_CONSUMED_JTIS[jti] = max(exp, now + 60)


def _verify_jclaw_handoff_token(token: str) -> dict[str, Any]:
    if not JCLAW_HANDOFF_SECRET:
        raise ValueError("子系统未配置 JCLAW_AI_AGENT_HANDOFF_SECRET")
    parts = str(token or "").split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError("handoff token 格式错误")

    header_b64, payload_b64, signature = parts
    header = _base64url_decode_json(header_b64)
    payload = _base64url_decode_json(payload_b64)
    if header.get("alg") != "HS256":
        raise ValueError("handoff token 算法不支持")

    signing_input = f"{header_b64}.{payload_b64}"
    signature_matched = False
    matched_secret_mode = ""
    for mode, secret_key in _jclaw_handoff_secret_keys(JCLAW_HANDOFF_SECRET):
        expected_signature = _jwt_hs256_signature_with_key(signing_input, secret_key)
        if hmac.compare_digest(signature, expected_signature):
            signature_matched = True
            matched_secret_mode = mode
            break
    if not signature_matched:
        raise ValueError("handoff token 签名错误")
    if matched_secret_mode != "plain":
        print(f"JClaw handoff signature accepted with {matched_secret_mode} secret mode", flush=True)

    now = int(time.time())
    skew = JCLAW_HANDOFF_CLOCK_SKEW_SECONDS
    exp = int(payload.get("exp") or 0)
    nbf = int(payload.get("nbf") or 0)
    if payload.get("iss") != JCLAW_HANDOFF_ISSUER:
        raise ValueError("handoff token 签发方错误")
    aud = payload.get("aud")
    if isinstance(aud, list):
        aud_ok = JCLAW_HANDOFF_AUDIENCE in aud
    else:
        aud_ok = aud == JCLAW_HANDOFF_AUDIENCE
    if not aud_ok:
        raise ValueError("handoff token 目标系统错误")
    if payload.get("purpose") != JCLAW_HANDOFF_PURPOSE:
        raise ValueError("handoff token 用途错误")
    if nbf and now + skew < nbf:
        raise ValueError("handoff token 尚未生效")
    if not exp or now - skew >= exp:
        raise ValueError("handoff token 已过期")

    _consume_jclaw_jti(str(payload.get("jti") or ""), exp)
    return payload


def _resolve_jclaw_user(payload: dict[str, Any]) -> str:
    global JCLAW_HANDOFF_USER_MAP
    if not JCLAW_HANDOFF_USER_MAP:
        JCLAW_HANDOFF_USER_MAP = _load_jclaw_user_map()

    candidates = [
        str(payload.get("uid") or "").strip(),
        str(payload.get("username") or "").strip(),
        str(payload.get("email") or "").strip(),
    ]
    for candidate in candidates:
        mapped = JCLAW_HANDOFF_USER_MAP.get(candidate.lower())
        if mapped and mapped in USERS:
            return mapped

    username = str(payload.get("username") or "").strip()
    if username in USERS:
        return username
    if username.lower() in USERS:
        return username.lower()

    email = str(payload.get("email") or "").strip()
    email_name = email.split("@", 1)[0].strip().lower() if "@" in email else ""
    if email_name and email_name in USERS:
        return email_name

    raise ValueError("主系统账号未映射到 AI 子系统账号")


def _jclaw_sso_error_response(message: str) -> HTMLResponse:
    safe_message = message.replace("<", "&lt;").replace(">", "&gt;")
    return HTMLResponse(
        f"""
        <!doctype html>
        <html lang="zh-CN">
        <head><meta charset="utf-8"><title>SSO 登录失败</title></head>
        <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:40px;color:#172033;">
          <h2>AI 子系统登录失败</h2>
          <p>{safe_message}</p>
          <p>请重新从 JClaw 主系统进入，或联系管理员检查账号映射。</p>
          <p><a href="/">返回登录页</a></p>
        </body>
        </html>
        """,
        status_code=401,
    )


def _complete_jclaw_handoff_login(request: Request, token: str):
    try:
        payload = _verify_jclaw_handoff_token(token)
        username = _resolve_jclaw_user(payload)
    except Exception as exc:
        print(f"JClaw handoff login failed: {exc}", flush=True)
        return _jclaw_sso_error_response(str(exc))

    request.session["username"] = username
    request.session["sso_source"] = "jclaw"
    request.session["sso_username"] = str(payload.get("username") or "")
    request.session["sso_uid"] = str(payload.get("uid") or "")
    request.session["sso_login_at"] = int(time.time())
    return RedirectResponse(url="/", status_code=302)


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


def _external_news_token_candidates() -> list[str]:
    candidates = [
        os.getenv("EXTERNAL_NEWS_API_TOKEN", ""),
        os.getenv("LOCALTOK_TOKEN", ""),
    ]
    return [str(token).strip() for token in candidates if str(token).strip()]


def _require_external_news_token(request: Request) -> Optional[JSONResponse]:
    provided = str(request.headers.get("X-Token") or "").strip()
    tokens = _external_news_token_candidates()
    if not tokens:
        return JSONResponse({"error": "外部新闻 API 令牌未配置"}, status_code=503)
    if not provided or not any(hmac.compare_digest(provided, token) for token in tokens):
        return JSONResponse({"error": "无效的 X-Token"}, status_code=401)
    return None


def _is_opennews_result(result: Optional[dict]) -> bool:
    if not result:
        return False
    workflow_config = result.get("workflow_config") or {}
    return bool(
        workflow_config.get("opennews")
        or workflow_config.get("opennews_material_only")
        or str(workflow_config.get("digital_human_engine") or "") == "opennews_material_only"
        or str(result.get("topic") or "").startswith("OpenNews")
    )


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
    script_model: str = SCRIPT_MODEL_API_RELAY,
    digital_human_engine: str = VOLC_ENGINE_ID,
    compose_aspect_ratio: str = "",
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
        "compose_aspect_ratio": (compose_aspect_ratio or "").strip().lower(),
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
    raw_variants = payload.get("final_video_variants")
    if isinstance(raw_variants, dict):
        serialized_variants = {}
        for variant_key, variant_data in raw_variants.items():
            if not isinstance(variant_data, dict):
                continue
            variant_video_url = _history_file_url(output_dir, variant_data.get("final_video_path", ""))
            if not variant_video_url:
                continue
            variant_payload = {
                "url": variant_video_url,
                "name": Path(str(variant_data.get("final_video_path", ""))).name or f"final_video_{variant_key}.mp4",
                "aspect_ratio": str(variant_data.get("compose_aspect_ratio") or variant_key),
            }
            variant_cover_url = _history_file_url(output_dir, variant_data.get("cover_image_path", ""))
            if variant_cover_url:
                variant_payload["cover_url"] = variant_cover_url
            variant_subtitle_url = _history_file_url(output_dir, variant_data.get("subtitle_path", ""))
            if variant_subtitle_url:
                variant_payload["subtitle_url"] = variant_subtitle_url
            serialized_variants[str(variant_key)] = variant_payload
        if serialized_variants:
            payload["final_video_variants"] = serialized_variants

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
    narration_audio_url = _history_file_url(output_dir, payload.get("narration_audio_path", ""))
    if narration_audio_url:
        payload["narration_audio"] = {
            "url": narration_audio_url,
            "name": Path(str(payload.get("narration_audio_path", ""))).name or "narration.mp3",
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
    script_model: str = SCRIPT_MODEL_API_RELAY,
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


def _save_result_to_output_dir(output_dir: Path, result: dict) -> None:
    (output_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _resolve_youtube_publish_video(output_dir: Path, result: dict, aspect_ratio: str = "vertical") -> Path:
    aspect_ratio = (aspect_ratio or "vertical").strip().lower()
    variants = result.get("final_video_variants")
    if isinstance(variants, dict):
        preferred = variants.get(aspect_ratio) if isinstance(variants.get(aspect_ratio), dict) else None
        if preferred and preferred.get("final_video_path"):
            rel = _history_relpath_from_value(str(output_dir), str(preferred.get("final_video_path") or ""))
            if rel and (output_dir / rel).exists():
                return output_dir / rel
        for key in ("vertical", "horizontal"):
            item = variants.get(key) if isinstance(variants.get(key), dict) else None
            if item and item.get("final_video_path"):
                rel = _history_relpath_from_value(str(output_dir), str(item.get("final_video_path") or ""))
                if rel and (output_dir / rel).exists():
                    return output_dir / rel
    final_video_path = str(result.get("final_video_path") or "")
    rel = _history_relpath_from_value(str(output_dir), final_video_path)
    if rel and (output_dir / rel).exists():
        return output_dir / rel
    raise YouTubePublishError("当前历史记录还没有可上传的成片 mp4，请先生成成片。")


def _build_default_youtube_metadata(result: dict, *, title: str = "", description: str = "", tags: Any = None) -> dict:
    workflow_config = result.get("workflow_config") or {}
    source = (workflow_config.get("source") or {}).get("article") or {}
    default_title = (
        title
        or result.get("title")
        or ((result.get("script") or {}).get("title") if isinstance(result.get("script"), dict) else "")
        or result.get("topic")
        or "iHouse OpenNews"
    )
    default_description_parts = [
        description or "",
        "",
        str(source.get("summary_zh") or source.get("summary") or "").strip(),
        "",
        f"来源：{source.get('source_name') or ''}".strip(),
        f"原文：{source.get('url') or ''}".strip(),
        f"新闻时间：{source.get('published_at') or ''}".strip(),
    ]
    default_description = "\n".join(part for part in default_description_parts if part).strip()
    if not default_description:
        default_description = "由 iHouse OpenNews 自动生成。"
    if tags is None:
        tags = ["OpenNews", "iHouse", "AIニュース", "新闻"]
    return {
        "title": str(default_title)[:100],
        "description": default_description[:5000],
        "tags": tags,
    }


def _update_youtube_upload_job(job_id: str, **updates: Any) -> dict:
    with YOUTUBE_UPLOAD_LOCK:
        job = YOUTUBE_UPLOAD_JOBS.get(job_id, {})
        job.update(updates)
        job["updated_at"] = time.time()
        YOUTUBE_UPLOAD_JOBS[job_id] = job
        return dict(job)


def _run_youtube_upload_job(job_id: str) -> None:
    job = _update_youtube_upload_job(job_id, status="running", message="正在上传到 YouTube...")
    try:
        output_dir = Path(job.get("output_dir") or "")
        result = _load_result_from_output_dir(output_dir)
        if not result:
            raise YouTubePublishError("历史结果不存在，无法上传 YouTube")
        upload_result = upload_video_to_youtube(
            YOUTUBE_TOKEN_STORE_PATH,
            Path(job.get("video_path") or ""),
            title=str(job.get("title") or ""),
            description=str(job.get("description") or ""),
            tags=job.get("tags") or [],
            privacy_status=str(job.get("privacy_status") or "unlisted"),
            category_id=str(job.get("category_id") or "25"),
            made_for_kids=bool(job.get("made_for_kids")),
            publish_at=str(job.get("publish_at") or ""),
        )
        publish_record = {
            "job_id": job_id,
            "history_id": output_dir.name,
            "aspect_ratio": job.get("aspect_ratio") or "",
            "video_path": job.get("video_path") or "",
            "created_at": time.time(),
            **upload_result,
        }
        records = result.get("youtube_publish_records")
        if not isinstance(records, list):
            records = []
        records.insert(0, publish_record)
        result["youtube_publish_records"] = records[:20]
        result["youtube_publish_latest"] = publish_record
        _save_result_to_output_dir(output_dir, result)
        _update_youtube_upload_job(
            job_id,
            status="done",
            message="YouTube 上传完成",
            result=upload_result,
            youtube_url=upload_result.get("youtube_url", ""),
            video_id=upload_result.get("video_id", ""),
        )
    except Exception as exc:
        _update_youtube_upload_job(job_id, status="failed", message=str(exc), error=str(exc))


def _publish_opennews_result_to_youtube(
    output_dir: Path,
    result: dict,
    *,
    aspects: list[str] | tuple[str, ...] = ("horizontal", "vertical"),
    privacy_status: str = "public",
    category_id: str = "25",
) -> list[dict]:
    records: list[dict] = []
    metadata = _build_default_youtube_metadata(result)
    existing_records = result.get("youtube_publish_records")
    if not isinstance(existing_records, list):
        existing_records = []
    for aspect in aspects:
        aspect_key = str(aspect or "").strip().lower()
        if aspect_key not in {"horizontal", "vertical"}:
            continue
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_key)
        upload_result = upload_video_to_youtube(
            YOUTUBE_TOKEN_STORE_PATH,
            video_path,
            title=metadata["title"],
            description=metadata["description"],
            tags=metadata["tags"],
            privacy_status=privacy_status,
            category_id=category_id,
            made_for_kids=False,
        )
        record = {
            "job_id": f"auto_opennews_{aspect_key}_{int(time.time())}",
            "history_id": output_dir.name,
            "aspect_ratio": aspect_key,
            "video_path": str(video_path),
            "created_at": time.time(),
            **upload_result,
        }
        existing_records.insert(0, record)
        records.append(record)
    if records:
        result["youtube_publish_records"] = existing_records[:20]
        result["youtube_publish_latest"] = records[-1]
        _save_result_to_output_dir(output_dir, result)
    return records


def _opennews_material_review_status(result: dict) -> dict:
    segments = result.get("segments") if isinstance(result, dict) else []
    fallback_items: list[dict] = []
    source_counts: dict[str, int] = {}
    if not isinstance(segments, list):
        segments = []
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        quality = segment.get("material_quality") if isinstance(segment.get("material_quality"), dict) else {}
        if quality.get("strict_source_fallback_used"):
            fallback_items.append({
                "segment_index": segment.get("index") or segment.get("segment_index") or len(fallback_items) + 1,
                "reason": "5090 AI图片完全不可用，使用了严格新闻源兜底素材",
                "quality": quality,
            })
        for item in segment.get("material_items") or []:
            if not isinstance(item, dict):
                continue
            source = str(item.get("source") or "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
            if item.get("strict_fallback") or source == "opennews_source":
                fallback_items.append({
                    "segment_index": segment.get("index") or segment.get("segment_index") or len(fallback_items) + 1,
                    "source": source,
                    "path": item.get("path") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": item.get("fallback_reason") or "使用了严格新闻源兜底素材",
                })
    return {
        "requires_human_review": False,
        "auto_publish_allowed": True,
        "uses_strict_source_fallback": bool(fallback_items),
        "reason": "5090 AI图片完全不可用，成片使用了严格新闻源兜底素材，已允许自动发布。" if fallback_items else "",
        "fallback_items": fallback_items[:30],
        "source_counts": source_counts,
    }


def _run_opennews_collection_job(job_id: str) -> None:
    try:
        _attach_opennews_collection_intro(job_id, message_suffix="正在生成合集...")
        build_collection_video(OPENNEWS_COLLECTION_DIR, OUTPUT_DIR, job_id)
    except Exception as exc:
        update_collection_job(
            OPENNEWS_COLLECTION_DIR,
            job_id,
            status="failed",
            message=str(exc),
            error=str(exc),
        )


def _collection_download_url(request: Request, job_id: str) -> str:
    return f"{_get_public_base_url(request)}/api/opennews/collections/{quote(job_id)}/download"


def _serialize_opennews_collection_job(job: dict, request: Request) -> dict:
    payload = dict(job or {})
    result = payload.get("result")
    if isinstance(result, dict) and result.get("video_path"):
        payload["download_url"] = _collection_download_url(request, str(payload.get("job_id") or result.get("job_id") or ""))
    return payload


def _short_opennews_collection_title(items: list[dict], *, prefix: str = "OpenNews合集") -> str:
    today = time.strftime("%Y-%m-%d")
    titles = [re.sub(r"\s+", "", str(item.get("title") or "")).strip("｜| -_") for item in items if item.get("title")]
    title_parts: list[str] = []
    for title in titles:
        if not title:
            continue
        compact = title[:16]
        candidate = "、".join(title_parts + [compact])
        full = f"{prefix}：{candidate}｜{today}"
        if len(full) > 96:
            break
        title_parts.append(compact)
        if len(title_parts) >= 3:
            break
    if not title_parts:
        return f"{prefix}｜{today}"
    return f"{prefix}：{'、'.join(title_parts)}｜{today}"[:100]


def _opennews_collection_description(items: list[dict], aspect_ratio: str) -> str:
    lines = [
        f"OpenNews 新闻合集（{aspect_ratio}）",
        "本合集由 iHouse OpenNews 自动整理生成，包含以下短新闻：",
        "",
    ]
    for index, item in enumerate(items, start=1):
        title = str(item.get("title") or "OpenNews 新闻").strip()
        source = str(item.get("source_name") or "").strip()
        published_at = str(item.get("published_at") or "").strip()
        suffix = "｜".join(part for part in [source, published_at] if part)
        lines.append(f"{index}. {title}{'｜' + suffix if suffix else ''}")
    lines.extend(["", "频道：ihouse AI Lab", "类型：OpenNews 自动新闻合集"])
    return "\n".join(lines)[:5000]


def _collection_item_title(item: dict) -> str:
    return str(item.get("title") or item.get("topic") or "OpenNews 新闻").strip()


def _collection_item_category(item: dict) -> str:
    raw = f"{item.get('title') or ''} {item.get('topic') or ''} {item.get('summary') or ''}".lower()
    if any(keyword in raw for keyword in ("robot", "robotics", "humanoid", "机器人", "人形机器人")):
        return "机器人"
    if any(keyword in raw for keyword in (" ai ", "artificial intelligence", "openai", "anthropic", "人工智能", "大模型")):
        return "AI"
    if any(keyword in raw for keyword in ("stock", "market", "fed", "oil", "finance", "股市", "金融", "油价")):
        return "金融"
    if any(keyword in raw for keyword in ("house", "home", "mortgage", "real estate", "房产", "住宅")):
        return "房产"
    if any(keyword in raw for keyword in ("war", "missile", "military", "defense", "iran", "军事", "导弹")):
        return "军事"
    if any(keyword in raw for keyword in ("election", "government", "trump", "policy", "政治", "政府")):
        return "政治"
    return "国际"


def _font_path_candidates() -> list[str]:
    return [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]


def _load_cover_font(size: int):
    from PIL import ImageFont

    for path in _font_path_candidates():
        try:
            if Path(path).exists():
                return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _draw_wrapped_text(draw, text: str, xy: tuple[int, int], *, font, fill: tuple[int, int, int], max_width: int, line_spacing: int = 10, max_lines: int = 3) -> int:
    chars = list(str(text or "").strip())
    lines: list[str] = []
    current = ""
    trailing_punctuation = "，。！？、；：,.!?;:"
    for char in chars:
        if char in trailing_punctuation and not current and lines:
            lines[-1] += char
            continue
        trial = current + char
        bbox = draw.textbbox((0, 0), trial, font=font)
        if bbox[2] - bbox[0] <= max_width or not current:
            current = trial
            continue
        lines.append(current)
        current = char
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    for index in range(1, len(lines)):
        while lines[index] and lines[index][0] in trailing_punctuation:
            lines[index - 1] += lines[index][0]
            lines[index] = lines[index][1:]
    if len(lines) > 1 and len(lines[-1]) <= 2:
        lines[-2] = lines[-2] + lines[-1]
        lines.pop()
    if chars and "".join(lines) != "".join(chars):
        lines[-1] = lines[-1].rstrip("，。,. ") + "..."
    x, y = xy
    for line in lines[:max_lines]:
        draw.text((x, y), line, font=font, fill=fill)
        bbox = draw.textbbox((x, y), line, font=font)
        y = bbox[3] + line_spacing
    return y


def _generate_opennews_collection_thumbnail(job: dict, result: dict) -> Path | None:
    if os.getenv("OPENNEWS_COLLECTION_THUMBNAIL_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return None
    try:
        from PIL import Image, ImageDraw, ImageFilter

        items = list(result.get("items") or job.get("items") or [])
        if not items:
            return None
        video_path = Path(str(result.get("video_path") or ""))
        output_dir = video_path.parent if video_path.exists() else (OPENNEWS_COLLECTION_DIR / "collections" / str(job.get("job_id") or "unknown"))
        output_dir.mkdir(parents=True, exist_ok=True)
        cover_path = output_dir / "youtube_thumbnail.jpg"

        width, height = 1920, 1080
        image = Image.new("RGB", (width, height), "#071421")
        draw = ImageDraw.Draw(image)
        for y in range(height):
            draw.line([(0, y), (width, y)], fill=(int(6 + y / height * 16), int(18 + y / height * 24), int(34 + y / height * 40)))
        for x in range(-height, width, 86):
            draw.line([(x, 0), (x + height, height)], fill=(24, 72, 102), width=1)
        for y in range(110, height, 96):
            draw.line([(0, y), (width, y)], fill=(18, 54, 80), width=1)

        glow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow)
        glow_draw.ellipse((980, -240, 2200, 860), fill=(0, 154, 180, 70))
        glow_draw.ellipse((-420, 560, 820, 1380), fill=(220, 63, 43, 58))
        image = Image.alpha_composite(image.convert("RGBA"), glow.filter(ImageFilter.GaussianBlur(55))).convert("RGB")
        draw = ImageDraw.Draw(image)

        title_font = _load_cover_font(84)
        main_font = _load_cover_font(70)
        sub_font = _load_cover_font(36)
        small_font = _load_cover_font(30)
        tag_font = _load_cover_font(28)
        date_font = _load_cover_font(34)

        draw.rounded_rectangle((96, 82, 610, 152), radius=12, fill=(214, 50, 42))
        draw.text((124, 98), "OPENNEWS", font=date_font, fill=(255, 255, 255))
        draw.text((640, 101), "每日热点", font=date_font, fill=(190, 224, 240))
        draw.text((1500, 98), time.strftime("%Y.%m.%d"), font=date_font, fill=(226, 238, 244))
        draw.text((96, 210), "今日十大英文热点", font=title_font, fill=(255, 255, 255))

        y = _draw_wrapped_text(draw, _collection_item_title(items[0]), (96, 330), font=main_font, fill=(255, 237, 205), max_width=1120, line_spacing=16, max_lines=3)
        y = max(y + 24, 620)
        for index, subtitle in enumerate([_collection_item_title(item) for item in items[1:4]], start=1):
            draw.rounded_rectangle((112, y + 7, 154, y + 49), radius=21, fill=(22, 118, 142))
            draw.text((126, y + 10), str(index), font=tag_font, fill=(255, 255, 255))
            _draw_wrapped_text(draw, subtitle, (174, y), font=sub_font, fill=(226, 238, 244), max_width=940, line_spacing=8, max_lines=1)
            y += 66

        categories: list[str] = []
        for item in items:
            category = _collection_item_category(item)
            if category not in categories:
                categories.append(category)
            if len(categories) >= 5:
                break
        draw.rounded_rectangle((96, 934, 1188, 1006), radius=18, fill=(255, 255, 255))
        draw.text((128, 953), "  ·  ".join(categories or ["AI", "金融", "国际"]), font=tag_font, fill=(10, 35, 48))

        draw.rounded_rectangle((1300, 238, 1818, 862), radius=34, fill=(239, 246, 248), outline=(90, 180, 196), width=4)
        draw.text((1350, 292), "每日热点", font=title_font, fill=(8, 32, 46))
        draw.text((1354, 405), "10条", font=main_font, fill=(214, 50, 42))
        draw.text((1524, 432), "国际新闻", font=sub_font, fill=(10, 35, 48))
        draw.text((1358, 540), "中文解读", font=sub_font, fill=(10, 35, 48))
        draw.text((1358, 604), "快速看懂", font=sub_font, fill=(10, 35, 48))
        draw.line((1350, 660, 1768, 660), fill=(28, 91, 113), width=3)
        draw.text((1358, 704), "OpenNews 每日热点", font=small_font, fill=(28, 91, 113))
        draw.text((1358, 754), "全球资讯 · 中文整理", font=small_font, fill=(28, 91, 113))

        image.save(cover_path, "JPEG", quality=94, optimize=True)
        return cover_path
    except Exception as exc:
        print(f"[OpenNews thumbnail] failed: {exc}", flush=True)
        return None


def _build_opennews_collection_intro_script(items: list[dict]) -> str:
    titles = [_collection_item_title(item) for item in items if _collection_item_title(item)]
    focus_titles = titles[:4]
    if not focus_titles:
        return "欢迎观看 OpenNews 每日热点。接下来，我们用几分钟带你快速了解今天值得关注的国际新闻。"
    focus_text = "、".join(focus_titles)
    return (
        "欢迎观看 OpenNews 每日热点。"
        f"今天值得关注的重点包括：{focus_text}。"
        "接下来，我们用几分钟带你快速看懂今天的国际热点。"
    )


def _opennews_intro_subtitle_chunks(text: str) -> list[str]:
    raw = re.sub(r"\s+", "", str(text or "").strip())
    if not raw:
        return []
    parts = [part for part in re.split(r"(?<=[。！？；])", raw) if part]
    chunks: list[str] = []
    for part in parts:
        while len(part) > 22:
            split_at = 22
            for mark in ("，", "、", "：", "；"):
                pos = part.rfind(mark, 0, 22)
                if pos >= 8:
                    split_at = pos + 1
                    break
            chunks.append(part[:split_at])
            part = part[split_at:]
        if part:
            chunks.append(part)
    return chunks or [raw]


def _format_srt_timestamp(seconds: float) -> str:
    total_ms = int(round(max(0.0, seconds) * 1000))
    hours = total_ms // 3600000
    minutes = (total_ms % 3600000) // 60000
    secs = (total_ms % 60000) // 1000
    millis = total_ms % 1000
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def _probe_intro_media_duration(path: Path) -> float:
    try:
        result = subprocess.run(
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
            timeout=20,
        )
        if result.returncode == 0:
            return max(0.1, float((result.stdout or "0").strip() or 0))
    except Exception:
        return 0.0
    return 0.0


def _burn_opennews_intro_subtitles(video_path: Path, script_text: str, output_path: Path) -> Path:
    duration = _probe_intro_media_duration(video_path)
    if duration <= 0:
        shutil.copy2(video_path, output_path)
        return output_path
    chunks = _opennews_intro_subtitle_chunks(script_text)
    if not chunks:
        shutil.copy2(video_path, output_path)
        return output_path
    srt_path = output_path.with_suffix(".srt")
    total_chars = max(1, sum(len(chunk) for chunk in chunks))
    cursor = 0.0
    lines: list[str] = []
    for index, chunk in enumerate(chunks, start=1):
        share = max(1.25, duration * (len(chunk) / total_chars))
        start = cursor
        end = duration if index == len(chunks) else min(duration, cursor + share)
        if end <= start:
            end = min(duration, start + 1.0)
        lines.extend([str(index), f"{_format_srt_timestamp(start)} --> {_format_srt_timestamp(end)}", chunk, ""])
        cursor = end
    srt_path.write_text("\n".join(lines), encoding="utf-8")
    escaped = srt_path.as_posix().replace("\\", "/").replace(":", r"\:").replace("'", r"\'")
    style = (
        "FontName=Noto Sans CJK SC,"
        "FontSize=26,"
        "Bold=1,"
        "PrimaryColour=&H0038F7FF,"
        "OutlineColour=&H0010192E,"
        "BackColour=&H00000000,"
        "BorderStyle=1,"
        "Outline=3.2,"
        "Shadow=1.6,"
        "Alignment=2,"
        "MarginV=42,"
        "MarginL=60,"
        "MarginR=60"
    )
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(video_path),
            "-vf",
            f"subtitles='{escaped}':force_style='{style}'",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "copy",
            "-movflags",
            "+faststart",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )
    return output_path


def _generate_opennews_collection_intro_digital_human(
    *,
    job_id: str,
    image_url: str,
    image_path: str,
    audio_url: str,
    audio_path: str,
    output_path: str,
    prompt: str,
) -> tuple[str, str, list[dict]]:
    attempts: list[dict] = []
    local_engines = [engine for engine in OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES if engine == INFINITETALK_ENGINE_ID]
    if OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED:
        for engine_id in local_engines:
            local_output_path = str(Path(output_path).with_name(f"{Path(output_path).stem}_{engine_id}.mp4"))
            try:
                video_result = _run_omnihuman_job_with_retry(
                    task_id=job_id,
                    job_id=f"{job_id}:collection_intro:{engine_id}",
                    label=f"OpenNews 合集数字人开场：{_digital_human_engine_label(engine_id)}",
                    tracker=None,
                    retries=1,
                    runner=lambda engine_id=engine_id, local_output_path=local_output_path: _generate_digital_human_video_by_engine(
                        engine_id=engine_id,
                        image_url=image_url,
                        image_path=image_path,
                        audio_url=audio_url,
                        audio_path=audio_path,
                        output_path=local_output_path,
                        prompt=prompt,
                        task_id=job_id,
                        segment_index=0,
                    ),
                )
                attempts.append({"engine": engine_id, "ok": True, "path": str(video_result)})
                return str(video_result), engine_id, attempts
            except Exception as exc:
                attempts.append({"engine": engine_id, "ok": False, "error": str(exc)})
                print(f"[opennews_collection_intro] 5090数字人失败，准备尝试下一个引擎：{engine_id}｜{exc}")

    video_result = _run_omnihuman_job_with_retry(
        task_id=job_id,
        job_id=f"{job_id}:collection_intro:{VOLC_ENGINE_ID}",
        label="OpenNews 合集数字人开场：火山 OmniHuman",
        tracker=None,
        runner=lambda: _generate_digital_human_video_by_engine(
            engine_id=VOLC_ENGINE_ID,
            image_url=image_url,
            image_path=image_path,
            audio_url=audio_url,
            audio_path=audio_path,
            output_path=output_path,
            prompt=prompt,
            task_id=job_id,
            segment_index=0,
        ),
    )
    attempts.append({"engine": VOLC_ENGINE_ID, "ok": True, "path": str(video_result)})
    return str(video_result), VOLC_ENGINE_ID, attempts


def _create_opennews_collection_intro_video(job: dict, output_root: Path) -> dict:
    if not OPENNEWS_COLLECTION_INTRO_ENABLED:
        return {"ok": False, "skipped": True, "reason": "intro_disabled"}
    if not OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH.exists():
        return {"ok": False, "skipped": True, "reason": "anchor_missing", "anchor_path": str(OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH)}
    items = list(job.get("items") or [])
    if not items:
        return {"ok": False, "skipped": True, "reason": "no_items"}

    from tos_uploader import upload_file_and_get_url

    job_id = str(job.get("job_id") or f"collection_intro_{int(time.time())}")
    intro_dir = OPENNEWS_COLLECTION_DIR / "collections" / job_id / "intro"
    intro_dir.mkdir(parents=True, exist_ok=True)
    script_text = _build_opennews_collection_intro_script(items)
    audio_path = intro_dir / "opennews_intro_audio.mp3"
    video_path = intro_dir / "opennews_intro_omnihuman.mp4"
    subtitled_video_path = intro_dir / "opennews_intro_omnihuman_subtitled.mp4"

    from generate_audio import generate_audio

    intro_voice_preset = _get_voice_preset("mandarin_female", "cn")
    _, intro_tts_provider = _generate_audio_for_workflow(
        script_text=script_text,
        audio_path=str(audio_path),
        voice=intro_voice_preset.get("voice_id") or "Chinese (Mandarin)_Warm_Bestie",
        speed=1.05,
        volume=1.25,
        language=intro_voice_preset.get("language") or "zh",
        workflow_config={
            "opennews": True,
            "opennews_material_only": True,
            "digital_human_engine": "opennews_material_only",
            "source": {"kind": "opennews"},
        },
        generate_audio_fn=generate_audio,
        log=lambda message: print(f"[opennews_collection_intro] {message}"),
    )
    image_url = upload_file_and_get_url(str(OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH), key_prefix="opennews/collection_intro/image")
    audio_url = upload_file_and_get_url(str(audio_path), key_prefix="opennews/collection_intro/audio")

    prompt = (
        "专业中文新闻女主播坐在演播桌前，面向镜头播报 OpenNews 每日热点开场。"
        "表情自然可信，口型清晰，轻微点头，动作克制，新闻栏目质感。"
        "不要改变背景中的 OpenNews 每日热点标识，不要添加额外文字。"
    )
    video_result, intro_engine, engine_attempts = _generate_opennews_collection_intro_digital_human(
        job_id=job_id,
        image_url=image_url,
        image_path=str(OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH),
        audio_url=audio_url,
        audio_path=str(audio_path),
        output_path=str(video_path),
        prompt=prompt,
    )
    final_intro_path = _burn_opennews_intro_subtitles(Path(str(video_result)), script_text, subtitled_video_path)
    return {
        "ok": True,
        "intro_script": script_text,
        "intro_audio_path": str(audio_path),
        "intro_video_path": str(final_intro_path),
        "intro_raw_video_path": str(video_result),
        "intro_subtitle_path": str(subtitled_video_path.with_suffix(".srt")),
        "anchor_path": str(OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH),
        "engine": intro_engine,
        "engine_attempts": engine_attempts,
        "tts_provider": intro_tts_provider,
    }


def _attach_opennews_collection_intro(job_id: str, *, message_suffix: str = "正在生成合集...") -> dict:
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id) or {"job_id": job_id}
    try:
        update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, message="正在生成数字人开场片头...")
        intro_result = _create_opennews_collection_intro_video(job, OUTPUT_DIR)
        if intro_result.get("ok") and intro_result.get("intro_video_path"):
            update_collection_job(
                OPENNEWS_COLLECTION_DIR,
                job_id,
                intro_video_path=intro_result.get("intro_video_path"),
                intro_script=intro_result.get("intro_script"),
                intro_result=intro_result,
                message=f"数字人开场片头已生成，{message_suffix}",
            )
        else:
            update_collection_job(
                OPENNEWS_COLLECTION_DIR,
                job_id,
                intro_result=intro_result,
                message=f"数字人开场片头已跳过，{message_suffix}",
            )
        return intro_result
    except Exception as intro_exc:
        update_collection_job(
            OPENNEWS_COLLECTION_DIR,
            job_id,
            intro_error=str(intro_exc),
            message=f"数字人开场生成失败，已跳过：{intro_exc}",
        )
        return {"ok": False, "error": str(intro_exc)}


def _publish_opennews_collection_to_youtube(job_id: str, *, privacy_status: str = "public") -> dict:
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id)
    if not job:
        raise YouTubePublishError("合集任务不存在")
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    video_path = Path(str(result.get("video_path") or ""))
    if str(job.get("status") or "") not in {"done", "publishing_youtube"} or not video_path.exists():
        raise YouTubePublishError("合集成片尚未生成完成，不能发布 YouTube")
    items = list(result.get("items") or job.get("items") or [])
    aspect_ratio = str(result.get("aspect_ratio") or job.get("aspect_ratio") or "")
    title = _short_opennews_collection_title(items)
    description = _opennews_collection_description(items, aspect_ratio)
    thumbnail_path = _generate_opennews_collection_thumbnail(job, result)
    upload_result = upload_video_to_youtube(
        YOUTUBE_TOKEN_STORE_PATH,
        video_path,
        title=title,
        description=description,
        tags=["OpenNews", "iHouse", "新闻合集"],
        privacy_status=privacy_status,
        category_id="25",
        made_for_kids=False,
        thumbnail_path=thumbnail_path,
    )
    record = {
        "job_id": f"opennews_collection_youtube_{int(time.time())}",
        "collection_id": job_id,
        "aspect_ratio": aspect_ratio,
        "video_path": str(video_path),
        "privacy_status": privacy_status,
        "thumbnail_path": str(thumbnail_path) if thumbnail_path else "",
        "created_at": time.time(),
        **upload_result,
    }
    records = result.get("youtube_publish_records")
    if not isinstance(records, list):
        records = []
    records.insert(0, record)
    result["youtube_publish_records"] = records[:20]
    result["youtube_publish_latest"] = record
    result["youtube_title"] = title
    result["youtube_description"] = description
    result["youtube_error"] = ""
    update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, result=result, youtube_error="")
    return record


def _auto_build_opennews_collections_if_ready(reason: str = "") -> None:
    if os.getenv("OPENNEWS_COLLECTION_AUTO_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return
    if not OPENNEWS_COLLECTION_AUTO_LOCK.acquire(blocking=False):
        return
    try:
        batch_size = max(2, min(int(os.getenv("OPENNEWS_COLLECTION_BATCH_SIZE", "10") or 10), 20))
        privacy_status = os.getenv("OPENNEWS_COLLECTION_YOUTUBE_PRIVACY", "public").strip() or "public"
        auto_started_at = ensure_collection_auto_started_at(OPENNEWS_COLLECTION_DIR)
        while True:
            pool = list_collection_pool(
                OPENNEWS_COLLECTION_DIR,
                OUTPUT_DIR,
                limit=200,
                include_used=False,
                min_created_at=auto_started_at,
            )
            if len(pool) < batch_size:
                return
            selected = sorted(pool, key=lambda item: float(item.get("created_at") or 0))[:batch_size]
            history_ids = [str(item.get("history_id") or "") for item in selected if item.get("history_id")]
            if len(history_ids) < batch_size:
                return
            base_title = _short_opennews_collection_title(selected)
            jobs = []
            for aspect_ratio in ("horizontal",):
                job = create_collection_job(
                    OPENNEWS_COLLECTION_DIR,
                    OUTPUT_DIR,
                    history_ids=history_ids,
                    aspect_ratio=aspect_ratio,
                    title=f"{base_title}_{aspect_ratio}",
                    username="auto_opennews",
                )
                update_collection_job(
                    OPENNEWS_COLLECTION_DIR,
                    str(job.get("job_id") or ""),
                    auto_created=True,
                    auto_reason=reason,
                    message="自动合集任务已创建，等待生成...",
                )
                jobs.append(job)
            success_count = 0
            for job in jobs:
                job_id = str(job.get("job_id") or "")
                try:
                    _attach_opennews_collection_intro(job_id, message_suffix="正在生成横屏合集...")
                    build_collection_video(OPENNEWS_COLLECTION_DIR, OUTPUT_DIR, job_id)
                    update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, status="publishing_youtube", message="合集已生成，正在自动发布 YouTube...")
                    record = _publish_opennews_collection_to_youtube(job_id, privacy_status=privacy_status)
                    update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, status="done", message="自动合集已生成并发布 YouTube", youtube_latest=record)
                    success_count += 1
                except Exception as exc:
                    update_collection_job(
                        OPENNEWS_COLLECTION_DIR,
                        job_id,
                        status="failed",
                        message=f"自动合集失败：{exc}",
                        error=str(exc),
                    )
            if success_count == 0:
                return
            # Continue scanning in case more than 10 new clips are already waiting.
    finally:
        OPENNEWS_COLLECTION_AUTO_LOCK.release()


def _trigger_opennews_collection_auto_check(reason: str = "") -> None:
    thread = threading.Thread(target=_auto_build_opennews_collections_if_ready, args=(reason,), daemon=True)
    thread.start()


def _build_and_publish_opennews_collection(history_ids: list[str], *, reason: str = "", privacy_status: str = "public") -> dict:
    clean_ids = [str(history_id or "").strip() for history_id in history_ids if str(history_id or "").strip()]
    if len(clean_ids) < max(2, min(int(os.getenv("OPENNEWS_COLLECTION_BATCH_SIZE", "10") or 10), 20)):
        raise RuntimeError("自动合集素材不足，等待更多短片完成。")
    pool = list_collection_pool(
        OPENNEWS_COLLECTION_DIR,
        OUTPUT_DIR,
        limit=300,
        include_used=False,
        min_created_at=0,
    )
    pool_by_id = {str(item.get("history_id") or ""): item for item in pool}
    selected = [pool_by_id[history_id] for history_id in clean_ids if history_id in pool_by_id]
    if len(selected) < len(clean_ids):
        missing = [history_id for history_id in clean_ids if history_id not in pool_by_id]
        raise RuntimeError(f"自动合集素材不可用或已入合集：{', '.join(missing[:3])}")
    base_title = _short_opennews_collection_title(selected)
    job = create_collection_job(
        OPENNEWS_COLLECTION_DIR,
        OUTPUT_DIR,
        history_ids=clean_ids,
        aspect_ratio="horizontal",
        title=f"{base_title}_horizontal",
        username="auto_opennews_collection",
    )
    job_id = str(job.get("job_id") or "")
    update_collection_job(
        OPENNEWS_COLLECTION_DIR,
        job_id,
        auto_created=True,
        auto_reason=reason,
        message="自动合集任务已创建，正在生成横屏合集...",
    )
    _attach_opennews_collection_intro(job_id, message_suffix="正在生成横屏合集...")
    build_collection_video(OPENNEWS_COLLECTION_DIR, OUTPUT_DIR, job_id)
    update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, status="publishing_youtube", message="合集已生成，正在自动发布 YouTube...")
    record = _publish_opennews_collection_to_youtube(job_id, privacy_status=privacy_status)
    update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, status="done", message="自动合集已生成并发布 YouTube", youtube_latest=record)
    updated = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id) or job
    return {"job_id": job_id, "job": updated, "youtube_record": record}


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
    handoff = request.query_params.get("handoff") or request.query_params.get("token")
    if handoff:
        return _complete_jclaw_handoff_login(request, handoff)
    return templates.TemplateResponse(request, "index.html")


@app.get("/sso/login", response_class=HTMLResponse)
async def jclaw_sso_login(request: Request):
    handoff = request.query_params.get("handoff") or request.query_params.get("token")
    if not handoff:
        return _jclaw_sso_error_response("缺少 handoff token")
    return _complete_jclaw_handoff_login(request, handoff)


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


@app.post("/api/app/auth/login")
async def app_auth_login(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    profile = USERS.get(username)
    if not profile or profile.get("password") != password:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=401)
    token_payload = _create_app_api_token(username)
    return {"ok": True, "user": _public_user(username, profile), **token_payload}


@app.post("/api/app/auth/logout")
async def app_auth_logout(request: Request):
    # Bearer token is stateless; the client only needs to discard it locally.
    request.session.clear()
    return {"ok": True}


@app.get("/api/app/me")
async def app_me(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"ok": True, "user": user}


def _app_task_status_payload(task_id: str, task: dict) -> dict:
    tracker = task.get("tracker")
    messages = []
    if tracker and getattr(tracker, "messages", None):
        messages = [
            {
                "message": item.get("message", ""),
                "step": item.get("step", 0),
                "total_steps": item.get("total_steps", 0),
                "time": item.get("time", 0),
            }
            for item in tracker.messages[-80:]
        ]
    result = task.get("result") or {}
    output_dir = str(task.get("output_dir") or "")
    return {
        "task_id": task_id,
        "mode": task.get("mode") or "full",
        "topic": task.get("topic") or "",
        "status": getattr(tracker, "status", "unknown") if tracker else "unknown",
        "step": getattr(tracker, "step", 0) if tracker else 0,
        "total_steps": getattr(tracker, "total_steps", 0) if tracker else 0,
        "created_at": task.get("created_at") or 0,
        "history_id": Path(output_dir).name if output_dir else "",
        "output_dir": output_dir,
        "messages": messages,
        "result_ready": bool(result),
        "result_summary": {
            "title": result.get("title") or "",
            "total_duration": result.get("total_duration") or 0,
            "segment_count": result.get("segment_count") or 0,
        } if isinstance(result, dict) else {},
    }


@app.get("/api/app/bootstrap")
async def app_bootstrap(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {
        "ok": True,
        "user": user,
        "options": {
            "voice_presets": VOICE_PRESETS,
            "avatars": _list_avatar_options(),
            "interface_languages": INTERFACE_LANGUAGES,
            "departments": DEPARTMENTS,
            "target_markets": TARGET_MARKETS,
            "composition_transitions": COMPOSITION_TRANSITIONS,
            "subtitle_templates": SUBTITLE_TEMPLATES,
            "digital_human_engines": _digital_human_engine_options_for_user(user),
            "script_models": _script_model_options_for_user(user),
            "property_bgm_tracks": _property_bgm_track_payloads(),
        },
        "active_tasks": _build_active_tasks_payload(user),
    }


@app.get("/api/app/tasks")
async def app_tasks(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    items = []
    for task_id, task in tasks.items():
        if _user_can_access_task(user, task):
            items.append(_app_task_status_payload(task_id, task))
    items.sort(key=lambda item: float(item.get("created_at") or 0), reverse=True)
    return {"ok": True, "items": items, "count": len(items)}


@app.get("/api/app/tasks/{task_id}")
async def app_task_status(task_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    task = tasks.get(task_id)
    if not task:
        return JSONResponse({"ok": False, "error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, task):
        return _forbidden_error()
    return {"ok": True, "task": _app_task_status_payload(task_id, task)}


@app.get("/api/app/history")
async def app_history(request: Request, limit: int = 50):
    user, error = _require_user(request)
    if error:
        return error
    items = _list_history_items(user)
    max_items = max(1, min(int(limit or 50), 200))
    return {"ok": True, "items": items[:max_items], "count": min(len(items), max_items), "total_count": len(items)}


@app.get("/api/app/history/{history_id}")
async def app_history_detail(history_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    return {
        "ok": True,
        "history": _serialize_result_for_ui(str(output_dir), result, result.get("topic", "")),
        "files": _build_file_entries(str(output_dir)),
    }


@app.get("/api/app/ready-videos")
async def app_ready_videos(request: Request, limit: int = 50, video_type: str = "all"):
    user, error = _require_user(request)
    if error:
        return error
    requested_type = str(video_type or "all").strip().lower()
    if requested_type not in {"all", "digital_human", "property_video", "opennews"}:
        return JSONResponse({"ok": False, "error": "video_type 只支持 all、digital_human、property_video、opennews"}, status_code=400)
    videos: list[dict] = []
    max_items = max(1, min(int(limit or 50), 200))
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir) or {}
        if not _history_visible_to_user(result, user):
            continue
        payload = None
        if _is_opennews_result(result):
            payload = _external_opennews_video_payload(request, output_dir, result)
            if payload:
                payload["type"] = "opennews"
                payload["type_label"] = "OpenNews 新闻视频"
        else:
            payload = _external_general_video_payload(request, output_dir, result)
        if not payload:
            continue
        if requested_type != "all" and payload.get("type") != requested_type:
            continue
        videos.append(payload)
        if len(videos) >= max_items:
            break
    return {"ok": True, "videos": videos, "count": len(videos)}


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


@app.get("/api/admin/opennews/sources")
async def admin_opennews_sources(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return {"sources": opennews_source_payloads(), "categories": opennews_category_payloads()}


@app.get("/api/youtube/status")
async def youtube_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = youtube_env_config()
    configured = bool(config.get("client_id") and config.get("client_secret") and (config.get("refresh_token") or YOUTUBE_TOKEN_STORE_PATH.exists()))
    payload = {
        "configured": configured,
        "client_id_configured": bool(config.get("client_id")),
        "client_secret_configured": bool(config.get("client_secret")),
        "redirect_uri": config.get("redirect_uri") or "",
        "refresh_token_configured": bool(config.get("refresh_token") or YOUTUBE_TOKEN_STORE_PATH.exists()),
        "channel": None,
        "error": "",
    }
    if configured:
        try:
            payload["channel"] = get_youtube_channel(YOUTUBE_TOKEN_STORE_PATH)
        except Exception as exc:
            payload["error"] = str(exc)
    return payload


@app.get("/api/youtube/oauth/start")
async def youtube_oauth_start(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = youtube_env_config()
    if not config.get("client_id") or not config.get("redirect_uri"):
        return JSONResponse({"error": "未配置 GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_REDIRECT_URI"}, status_code=500)
    state = hashlib.sha256(f"{user.get('username')}:{time.time()}:{uuid.uuid4()}".encode("utf-8")).hexdigest()
    request.session["youtube_oauth_state"] = state
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={quote(config['client_id'], safe='')}"
        f"&redirect_uri={quote(config['redirect_uri'], safe='')}"
        "&response_type=code"
        f"&scope={quote(YOUTUBE_SCOPE, safe='')}"
        "&access_type=offline"
        "&prompt=consent"
        f"&state={quote(state, safe='')}"
    )
    return RedirectResponse(auth_url)


@app.get("/api/youtube/oauth/callback")
async def youtube_oauth_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<h2>YouTube 授权失败</h2><p>{error}</p>", status_code=400)
    expected_state = request.session.get("youtube_oauth_state")
    if expected_state and state and not hmac.compare_digest(str(expected_state), str(state)):
        return HTMLResponse("<h2>YouTube 授权失败</h2><p>state 校验失败。</p>", status_code=400)
    if not code:
        return HTMLResponse("<h2>YouTube 授权失败</h2><p>缺少 code。</p>", status_code=400)
    try:
        tokens = exchange_youtube_code_for_tokens(code)
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            return HTMLResponse("<h2>YouTube 授权成功但没有返回 refresh_token</h2><p>如果之前授权过，请撤销应用授权后重新绑定。</p>", status_code=400)
        save_youtube_refresh_token(YOUTUBE_TOKEN_STORE_PATH, refresh_token, {"token_response": {k: v for k, v in tokens.items() if k != "refresh_token"}})
        channel = get_youtube_channel(YOUTUBE_TOKEN_STORE_PATH)
    except Exception as exc:
        return HTMLResponse(f"<h2>YouTube 授权失败</h2><p>{exc}</p>", status_code=500)
    return HTMLResponse(
        "<h2>YouTube 授权成功</h2>"
        f"<p>频道：{channel.get('title') or ''}</p>"
        f"<p>channel_id：{channel.get('channel_id') or ''}</p>"
        "<p>现在可以回到 iHouse 系统发布视频。</p>"
    )


@app.post("/api/youtube/upload")
async def youtube_upload(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    history_id = str(payload.get("history_id") or "").strip()
    if not history_id:
        return JSONResponse({"error": "缺少 history_id"}, status_code=400)
    output_dir, result, resolve_error = _resolve_history_for_user(history_id, user)
    if resolve_error:
        return resolve_error
    assert output_dir is not None and result is not None
    aspect_ratio = str(payload.get("aspect_ratio") or "vertical").strip().lower()
    try:
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_ratio)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    meta = _build_default_youtube_metadata(
        result,
        title=str(payload.get("title") or ""),
        description=str(payload.get("description") or ""),
        tags=payload.get("tags"),
    )
    job_id = f"youtube_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job = {
        "job_id": job_id,
        "status": "queued",
        "message": "YouTube 上传任务已创建",
        "created_at": time.time(),
        "updated_at": time.time(),
        "owner_username": user.get("username") or "",
        "history_id": history_id,
        "output_dir": str(output_dir),
        "video_path": str(video_path),
        "aspect_ratio": aspect_ratio,
        "title": meta["title"],
        "description": meta["description"],
        "tags": meta["tags"],
        "privacy_status": str(payload.get("privacy_status") or "unlisted"),
        "category_id": str(payload.get("category_id") or "25"),
        "made_for_kids": bool(payload.get("made_for_kids", False)),
        "publish_at": str(payload.get("publish_at") or ""),
    }
    with YOUTUBE_UPLOAD_LOCK:
        YOUTUBE_UPLOAD_JOBS[job_id] = dict(job)
    thread = threading.Thread(target=_run_youtube_upload_job, args=(job_id,), daemon=True)
    thread.start()
    return {"ok": True, "job_id": job_id, "job": job}


@app.get("/api/youtube/jobs/{job_id}")
async def youtube_upload_job(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with YOUTUBE_UPLOAD_LOCK:
        job = dict(YOUTUBE_UPLOAD_JOBS.get(job_id) or {})
    if not job:
        return JSONResponse({"error": "YouTube 上传任务不存在"}, status_code=404)
    return {"job": job}


@app.get("/api/opennews/sources")
async def opennews_sources(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {
        "sources": opennews_source_payloads(),
        "categories": opennews_category_payloads(),
        "trend_categories": opennews_trend_category_payloads(),
        "trend_time_ranges": opennews_trend_time_range_payloads(),
    }


@app.post("/api/admin/opennews/search")
async def admin_opennews_search(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    query = str(payload.get("query") or "").strip()
    source_ids = payload.get("source_ids") or []
    category = str(payload.get("category") or "all").strip() or "all"
    if isinstance(source_ids, str):
        source_ids = [part.strip() for part in source_ids.split(",") if part.strip()]
    search_result = search_opennews_candidates_with_stats(query, source_ids=source_ids, category=category)
    candidates = search_result.get("candidates", [])
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "search", {"query": query, "source_ids": source_ids, "category": category, "candidates": candidates, "stats": search_result.get("stats", []), "recent_window": search_result.get("recent_window", ""), "user": user.get("username")})
    return {"candidates": candidates, "count": len(candidates), "stats": search_result.get("stats", []), "raw_count": search_result.get("raw_count", 0), "deduped_count": search_result.get("deduped_count", 0), "recent_count": search_result.get("recent_count", 0), "recent_window": search_result.get("recent_window", ""), "missing_timestamp_checked": search_result.get("missing_timestamp_checked", 0)}


@app.post("/api/opennews/search")
async def opennews_search(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    query = str(payload.get("query") or "").strip()
    source_ids = payload.get("source_ids") or []
    category = str(payload.get("category") or "all").strip() or "all"
    if isinstance(source_ids, str):
        source_ids = [part.strip() for part in source_ids.split(",") if part.strip()]
    search_result = search_opennews_candidates_with_stats(query, source_ids=source_ids, category=category)
    candidates = search_result.get("candidates", [])
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "search", {"query": query, "source_ids": source_ids, "category": category, "candidates": candidates, "stats": search_result.get("stats", []), "recent_window": search_result.get("recent_window", ""), "user": user.get("username")})
    return {"candidates": candidates, "count": len(candidates), "stats": search_result.get("stats", []), "raw_count": search_result.get("raw_count", 0), "deduped_count": search_result.get("deduped_count", 0), "recent_count": search_result.get("recent_count", 0), "recent_window": search_result.get("recent_window", ""), "missing_timestamp_checked": search_result.get("missing_timestamp_checked", 0)}


@app.post("/api/opennews/trends/search")
async def opennews_trends_search(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    category = str(payload.get("category") or "all").strip() or "all"
    time_range = str(payload.get("time_range") or "6h").strip() or "6h"
    keyword = str(payload.get("query") or payload.get("keyword") or "").strip()
    try:
        search_result = search_english_trends(category=category, time_range=time_range, keyword=keyword)
    except Exception as exc:
        return JSONResponse({"error": f"英文热点抓取失败：{exc}"}, status_code=500)
    candidates = search_result.get("candidates", [])
    save_opennews_payload(
        OPENNEWS_ADMIN_DIR,
        "trends",
        {
            "query": keyword,
            "category": category,
            "time_range": time_range,
            "candidates": candidates,
            "stats": search_result.get("stats", []),
            "recent_window": search_result.get("recent_window", ""),
            "user": user.get("username"),
        },
    )
    return {
        "candidates": candidates,
        "count": len(candidates),
        "stats": search_result.get("stats", []),
        "raw_count": search_result.get("raw_count", 0),
        "deduped_count": search_result.get("deduped_count", 0),
        "recent_count": search_result.get("recent_count", 0),
        "recent_window": search_result.get("recent_window", ""),
        "time_range": search_result.get("time_range", ""),
        "source_errors": search_result.get("source_errors", []),
    }


@app.get("/api/opennews/batches/config")
async def opennews_batches_config(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"config": load_opennews_batch_config(OPENNEWS_BATCH_DIR), "is_admin": _is_admin(user)}


@app.post("/api/opennews/batches/config")
async def opennews_batches_config_update(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    config = save_opennews_batch_config(
        OPENNEWS_BATCH_DIR,
        {
            "enabled": bool(payload.get("enabled")),
            "interval_minutes": payload.get("interval_minutes"),
            "category": payload.get("category") or "all",
            "time_range": payload.get("time_range") or "6h",
            "limit": payload.get("limit") or 20,
        },
    )
    return {"config": config}


@app.post("/api/opennews/batches/run-now")
async def opennews_batches_run_now(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    override = {
        "category": payload.get("category") or None,
        "time_range": payload.get("time_range") or None,
        "limit": payload.get("limit") or None,
    }
    result = run_opennews_batch_fetch_once(
        OPENNEWS_BATCH_DIR,
        triggered_by=user.get("username") or "manual",
        override=override,
    )
    status_code = 202 if result.get("running") else 200
    return JSONResponse(result, status_code=status_code)


@app.get("/api/opennews/batches")
async def opennews_batches(request: Request, limit: int = 20):
    user, error = _require_user(request)
    if error:
        return error
    batches = list_opennews_batches(OPENNEWS_BATCH_DIR, limit=max(1, min(int(limit or 20), 60)))
    return {"batches": batches, "count": len(batches)}


@app.post("/api/opennews/batches/produce")
async def opennews_batches_produce(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    item_ids = payload.get("item_ids") or []
    if isinstance(item_ids, str):
        item_ids = [part.strip() for part in item_ids.split(",") if part.strip()]
    if not isinstance(item_ids, list) or not item_ids:
        return JSONResponse({"error": "请先勾选要制作的视频新闻。"}, status_code=400)
    item_ids = [str(item or "").strip() for item in item_ids if str(item or "").strip()]
    if len(item_ids) > 8:
        return JSONResponse({"error": "一次最多批量制作 8 条新闻，避免后台任务拥堵。"}, status_code=400)
    items = find_opennews_batch_items(OPENNEWS_BATCH_DIR, item_ids)
    if not items:
        return JSONResponse({"error": "未找到已勾选的批次新闻。"}, status_code=404)
    items = [item for item in items if str(item.get("status") or "") != "auto_producing"]
    if not items:
        return JSONResponse({"error": "勾选的新闻已经进入自动合集生产任务，请选择其他新闻。"}, status_code=400)
    target_market = str(payload.get("target_market") or user.get("target_market") or "cn")
    voice_preset_id = str(payload.get("voice_preset_id") or "")
    aspect_ratio = str(payload.get("aspect_ratio") or "horizontal")
    youtube_aspects = payload.get("youtube_aspects") or ["horizontal", "vertical"]
    if isinstance(youtube_aspects, str):
        youtube_aspects = ["horizontal", "vertical"] if youtube_aspects == "both" else [part.strip() for part in youtube_aspects.split(",") if part.strip()]
    elif isinstance(youtube_aspects, list):
        youtube_aspects = [str(part).strip() for part in youtube_aspects if str(part).strip()]
    else:
        youtube_aspects = ["horizontal", "vertical"]
    job = create_opennews_batch_job(
        OPENNEWS_BATCH_DIR,
        username=user.get("username") or "",
        items=items,
        options={
            "target_market": target_market,
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": voice_preset_id,
            "aspect_ratio": aspect_ratio,
            "notes": str(payload.get("notes") or ""),
            "youtube_auto_publish": bool(payload.get("youtube_auto_publish")),
            "youtube_privacy_status": str(payload.get("youtube_privacy_status") or "public"),
            "youtube_aspects": youtube_aspects or ["horizontal", "vertical"],
        },
    )
    thread = threading.Thread(
        target=_run_opennews_external_produce_job,
        kwargs={
            "job_id": job.get("job_id"),
            "user": dict(user),
            "public_base_url": _get_public_base_url(request),
        },
        daemon=True,
    )
    thread.start()
    return {"job": job, "job_id": job.get("job_id")}


@app.get("/api/opennews/batches/jobs/{job_id}")
async def opennews_batches_job_status(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id)
    if not job:
        return JSONResponse({"error": "批量生产任务不存在"}, status_code=404)
    if job.get("username") != user.get("username") and not _is_admin(user):
        return _forbidden_error()
    return {"job": job}


@app.get("/api/opennews/batches/jobs")
async def opennews_batches_jobs(request: Request, limit: int = 10):
    user, error = _require_user(request)
    if error:
        return error
    jobs = list_opennews_batch_jobs(
        OPENNEWS_BATCH_DIR,
        limit=max(1, min(int(limit or 10), 30)),
        username=str(user.get("username") or ""),
        include_all=_is_admin(user),
    )
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/opennews/collections/pool")
async def opennews_collections_pool(request: Request, limit: int = 80, include_used: bool = False):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以管理 OpenNews 合集")
    items = list_collection_pool(
        OPENNEWS_COLLECTION_DIR,
        OUTPUT_DIR,
        limit=max(1, min(int(limit or 80), 200)),
        include_used=bool(include_used),
    )
    return {"items": items, "count": len(items)}


@app.post("/api/opennews/collections/build")
async def opennews_collections_build(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以制作 OpenNews 合集")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    history_ids = payload.get("history_ids") or []
    if isinstance(history_ids, str):
        history_ids = [part.strip() for part in history_ids.split(",") if part.strip()]
    if not isinstance(history_ids, list) or not history_ids:
        return JSONResponse({"error": "请先选择要加入合集的成片视频。"}, status_code=400)
    try:
        job = create_collection_job(
            OPENNEWS_COLLECTION_DIR,
            OUTPUT_DIR,
            history_ids=[str(item or "").strip() for item in history_ids if str(item or "").strip()],
            aspect_ratio=str(payload.get("aspect_ratio") or "horizontal"),
            title=str(payload.get("title") or ""),
            username=str(user.get("username") or ""),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    thread = threading.Thread(target=_run_opennews_collection_job, args=(str(job.get("job_id") or ""),), daemon=True)
    thread.start()
    return {"job": _serialize_opennews_collection_job(job, request), "job_id": job.get("job_id")}


@app.get("/api/opennews/collections/jobs")
async def opennews_collections_jobs(request: Request, limit: int = 20):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以查看 OpenNews 合集")
    jobs = list_collection_jobs(OPENNEWS_COLLECTION_DIR, limit=max(1, min(int(limit or 20), 100)))
    return {"jobs": [_serialize_opennews_collection_job(job, request) for job in jobs], "count": len(jobs)}


@app.get("/api/opennews/collections/jobs/{job_id}")
async def opennews_collections_job(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以查看 OpenNews 合集")
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id)
    if not job:
        return JSONResponse({"error": "合集任务不存在"}, status_code=404)
    return {"job": _serialize_opennews_collection_job(job, request)}


@app.get("/api/opennews/collections/{job_id}/download")
async def opennews_collections_download(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以下载 OpenNews 合集")
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id)
    result = (job or {}).get("result") if isinstance(job, dict) else {}
    video_path = Path(str((result or {}).get("video_path") or ""))
    if not job or not video_path.exists():
        return JSONResponse({"error": "合集成片不存在或尚未生成完成"}, status_code=404)
    return FileResponse(video_path, media_type="video/mp4", filename=video_path.name)


@app.post("/api/opennews/collections/{job_id}/publish-youtube")
async def opennews_collections_publish_youtube(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发布 OpenNews 合集")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id)
    result = (job or {}).get("result") if isinstance(job, dict) else {}
    video_path = Path(str((result or {}).get("video_path") or ""))
    if not job or job.get("status") != "done" or not video_path.exists():
        return JSONResponse({"error": "合集成片尚未生成完成，不能发布 YouTube"}, status_code=400)
    privacy_status = str(payload.get("privacy_status") or "public")
    try:
        record = _publish_opennews_collection_to_youtube(job_id, privacy_status=privacy_status)
    except Exception as exc:
        update_collection_job(OPENNEWS_COLLECTION_DIR, job_id, youtube_error=str(exc))
        return JSONResponse({"error": str(exc)}, status_code=500)
    updated = load_collection_job(OPENNEWS_COLLECTION_DIR, job_id) or job
    return {"ok": True, "job": _serialize_opennews_collection_job(updated, request), "record": record}


@app.get("/api/opennews/localtok/status")
async def opennews_localtok_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"localtok": localtok_status()}


@app.get("/api/opennews/localtok/proposals")
async def opennews_localtok_proposals(request: Request, limit: int = 20):
    user, error = _require_user(request)
    if error:
        return error
    proposals = list_localtok_proposals(OPENNEWS_LOCALTOK_DIR, limit=max(1, min(int(limit or 20), 60)))
    if not _is_admin(user):
        proposals = [item for item in proposals if item.get("username") == user.get("username")]
    return {"proposals": proposals, "count": len(proposals), "localtok": localtok_status()}


@app.post("/api/opennews/localtok/propose")
async def opennews_localtok_propose(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    item_ids = payload.get("item_ids") or []
    if isinstance(item_ids, str):
        item_ids = [part.strip() for part in item_ids.split(",") if part.strip()]
    if not isinstance(item_ids, list) or not item_ids:
        return JSONResponse({"error": "请先勾选要提交 LocalTok 审核的新闻。"}, status_code=400)
    item_ids = [str(item or "").strip() for item in item_ids if str(item or "").strip()]
    if len(item_ids) > 10:
        return JSONResponse({"error": "LocalTok 一次最多提交 10 条候选新闻。"}, status_code=400)
    items = find_opennews_batch_items(OPENNEWS_BATCH_DIR, item_ids)
    if not items:
        return JSONResponse({"error": "未找到已勾选的批次新闻。"}, status_code=404)
    try:
        used_titles = get_localtok_used_titles()
        filtered_items, titles, summary, options, local_dup_titles = _build_localtok_proposal_payload(items, used_titles)
        if not filtered_items:
            return JSONResponse({"error": "勾选新闻都已存在于 LocalTok 已用标题清单，请换一批。", "dup_titles": local_dup_titles}, status_code=409)
        response = propose_localtok_news(titles=titles, summary=summary, options=options)
    except (LocalTokError, requests.RequestException) as exc:
        return JSONResponse({"error": f"LocalTok 提案提交失败：{exc}"}, status_code=502)
    local_proposal_id = make_local_proposal_id(user.get("username") or "")
    dup_titles = list(dict.fromkeys(local_dup_titles + [str(item) for item in (response.get("dup_titles") or []) if str(item).strip()]))
    proposal = create_localtok_proposal(
        OPENNEWS_LOCALTOK_DIR,
        {
            "local_proposal_id": local_proposal_id,
            "proposal_id": str(response.get("id") or ""),
            "username": user.get("username") or "",
            "status": "pending",
            "message": "LocalTok 提案已提交，等待对方审核。",
            "titles": titles,
            "summary": summary,
            "options": options,
            "items": filtered_items,
            "dup_titles": dup_titles,
            "settings": {
                "target_market": str(payload.get("target_market") or user.get("target_market") or "cn"),
                "department_id": str(user.get("department_id") or "real_estate"),
                "voice_preset_id": str(payload.get("voice_preset_id") or ""),
                "notes": str(payload.get("notes") or ""),
            },
        },
    )
    return {"proposal": proposal, "local_proposal_id": local_proposal_id, "proposal_id": response.get("id"), "dup_titles": dup_titles}


@app.post("/api/opennews/localtok/proposals/{local_proposal_id}/check")
async def opennews_localtok_proposal_check(local_proposal_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    proposal = load_localtok_proposal(OPENNEWS_LOCALTOK_DIR, local_proposal_id)
    if not proposal:
        return JSONResponse({"error": "LocalTok 提案记录不存在。"}, status_code=404)
    if proposal.get("username") != user.get("username") and not _is_admin(user):
        return _forbidden_error()
    if proposal.get("status") in {"generating", "published"}:
        return {"proposal": proposal, "message": proposal.get("message") or "提案已进入后续流程。"}
    if proposal.get("status") == "failed" and proposal.get("task_id"):
        return {"proposal": proposal, "message": proposal.get("message") or "提案处理失败。"}
    try:
        decision = get_localtok_decision(proposal.get("proposal_id") or "")
    except (LocalTokError, requests.RequestException) as exc:
        updated = update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "status": "pending",
                "message": f"LocalTok 审核状态暂时无法获取：{exc}",
                "error": str(exc),
            }),
        )
        return JSONResponse({"error": updated.get("message"), "proposal": updated}, status_code=502)
    status = str(decision.get("status") or "").strip().lower()
    if status != "decided":
        updated = update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "status": "pending",
                "message": "LocalTok 还未审核，稍后再检查。",
                "decision": decision,
                "error": "",
            }),
        )
        return {"proposal": updated, "decision": decision}
    updated = update_localtok_proposal(
        OPENNEWS_LOCALTOK_DIR,
        local_proposal_id,
        lambda payload: payload.update({
            "status": "decided",
            "message": "LocalTok 已审核，准备自动生成并发布视频。",
            "decision": decision,
            "error": "",
        }),
    )
    thread = threading.Thread(
        target=_run_localtok_decided_production,
        kwargs={
            "local_proposal_id": local_proposal_id,
            "user": dict(user),
            "public_base_url": _get_public_base_url(request),
        },
        daemon=True,
    )
    thread.start()
    return {"proposal": updated, "decision": decision}


@app.get("/api/opennews/auto/config")
async def opennews_auto_config(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    config = load_opennews_auto_config(OPENNEWS_AUTO_DIR)
    return {"config": config, "is_admin": _is_admin(user)}


def _external_download_url(request: Request, history_id: str, output_dir: Path, value: str) -> str:
    rel = _history_relpath_from_value(str(output_dir), value)
    if not rel:
        return ""
    base_url = _get_public_base_url(request).rstrip("/")
    return f"{base_url}/api/external/opennews/videos/{quote(history_id, safe='')}/download/{quote(rel, safe='/')}"


def _external_download_url_for_base(public_base_url: str, history_id: str, output_dir: Path, value: str) -> str:
    rel = _history_relpath_from_value(str(output_dir), value)
    if not rel:
        return ""
    base_url = _normalize_public_base_url(public_base_url or os.getenv("PUBLIC_BASE_URL") or "")
    if not base_url:
        base_url = "https://aiagent.office.ihousejapan.cn"
    return f"{base_url}/api/external/opennews/videos/{quote(history_id, safe='')}/download/{quote(rel, safe='/')}"


def _external_video_urls_for_result(public_base_url: str, output_dir: Path, result: dict) -> dict:
    history_id = output_dir.name
    variants: dict[str, dict] = {}
    raw_variants = result.get("final_video_variants")
    if isinstance(raw_variants, dict):
        for aspect, variant in raw_variants.items():
            if not isinstance(variant, dict):
                continue
            video_path = str(variant.get("final_video_path") or "")
            rel = _history_relpath_from_value(str(output_dir), video_path)
            if not rel or not (output_dir / rel).exists():
                continue
            variants[str(aspect)] = {
                "aspect_ratio": str(variant.get("compose_aspect_ratio") or aspect),
                "name": Path(video_path).name or f"final_video_{aspect}.mp4",
                "download_url": _external_download_url_for_base(public_base_url, history_id, output_dir, video_path),
                "size": (output_dir / rel).stat().st_size,
            }
    final_video_path = str(result.get("final_video_path") or "")
    final_rel = _history_relpath_from_value(str(output_dir), final_video_path)
    if final_rel and (output_dir / final_rel).exists() and not variants:
        aspect = str((result.get("workflow_config") or {}).get("compose_aspect_ratio") or "default")
        variants[aspect] = {
            "aspect_ratio": aspect,
            "name": Path(final_video_path).name or "final_video.mp4",
            "download_url": _external_download_url_for_base(public_base_url, history_id, output_dir, final_video_path),
            "size": (output_dir / final_rel).stat().st_size,
        }
    return {
        "history_id": history_id,
        "variants": variants,
        "vertical_url": (variants.get("vertical") or {}).get("download_url") or "",
        "horizontal_url": (variants.get("horizontal") or {}).get("download_url") or "",
    }


def _is_property_video_result(result: Optional[dict]) -> bool:
    if not result:
        return False
    workflow_config = result.get("workflow_config") or {}
    return bool(
        result.get("mode") == "property_video"
        or workflow_config.get("property_video_mode")
        or str(result.get("topic") or "") == "房源实拍成片"
    )


def _is_digital_human_result(result: Optional[dict]) -> bool:
    if not result or _is_opennews_result(result) or _is_property_video_result(result):
        return False
    workflow_config = result.get("workflow_config") or {}
    segments = result.get("segments")
    return bool(
        result.get("final_video_path")
        and (
            workflow_config.get("digital_human_engine")
            or isinstance(segments, list)
            or result.get("segment_count")
        )
    )


def _external_general_download_url(request: Request, history_id: str, output_dir: Path, value: str) -> str:
    rel = _history_relpath_from_value(str(output_dir), value)
    if not rel:
        return ""
    base_url = _get_public_base_url(request).rstrip("/")
    return f"{base_url}/api/external/videos/{quote(history_id, safe='')}/download/{quote(rel, safe='/')}"


def _external_final_video_paths(result: dict) -> set[str]:
    paths: set[str] = set()
    final_video_path = str(result.get("final_video_path") or "").strip()
    if final_video_path:
        paths.add(final_video_path)
    variants = result.get("final_video_variants")
    if isinstance(variants, dict):
        for variant in variants.values():
            if isinstance(variant, dict):
                variant_path = str(variant.get("final_video_path") or "").strip()
                if variant_path:
                    paths.add(variant_path)
    return paths


def _external_general_video_payload(request: Request, output_dir: Path, result: dict) -> Optional[dict]:
    if _is_property_video_result(result):
        video_type = "property_video"
        video_type_label = "房源实拍成片"
    elif _is_digital_human_result(result):
        video_type = "digital_human"
        video_type_label = "数字人视频"
    else:
        return None

    history_id = output_dir.name
    variants: dict[str, dict] = {}
    raw_variants = result.get("final_video_variants")
    if isinstance(raw_variants, dict):
        for aspect, variant in raw_variants.items():
            if not isinstance(variant, dict):
                continue
            video_path = str(variant.get("final_video_path") or "")
            rel = _history_relpath_from_value(str(output_dir), video_path)
            if not rel or not (output_dir / rel).exists():
                continue
            variants[str(aspect)] = {
                "aspect_ratio": str(variant.get("compose_aspect_ratio") or aspect),
                "name": Path(video_path).name or f"final_video_{aspect}.mp4",
                "download_url": _external_general_download_url(request, history_id, output_dir, video_path),
                "size": (output_dir / rel).stat().st_size,
            }

    final_video_path = str(result.get("final_video_path") or "")
    final_rel = _history_relpath_from_value(str(output_dir), final_video_path)
    if final_rel and (output_dir / final_rel).exists() and not variants:
        aspect = str((result.get("workflow_config") or {}).get("compose_aspect_ratio") or "vertical")
        variants[aspect] = {
            "aspect_ratio": aspect,
            "name": Path(final_video_path).name or "final_video.mp4",
            "download_url": _external_general_download_url(request, history_id, output_dir, final_video_path),
            "size": (output_dir / final_rel).stat().st_size,
        }

    if not variants:
        return None

    preferred = variants.get("vertical") or variants.get("9:16") or next(iter(variants.values()))
    return {
        "id": history_id,
        "history_id": history_id,
        "title": result.get("title") or (result.get("script") or {}).get("title") or result.get("topic") or video_type_label,
        "type": video_type,
        "type_label": video_type_label,
        "completed_at": int(output_dir.stat().st_mtime),
        "created_at": int(output_dir.stat().st_mtime),
        "duration": result.get("total_duration") or 0,
        "vertical_url": preferred.get("download_url") or "",
        "final_video_url": preferred.get("download_url") or "",
        "final_video_name": preferred.get("name") or "",
        "variants": variants,
    }


def _external_opennews_job_result_payload(job: dict) -> dict:
    items = []
    for item in job.get("items", []) or []:
        items.append({
            "id": item.get("batch_item_id") or "",
            "title": item.get("title") or _opennews_article_title(item.get("article") or {}),
            "status": item.get("status") or "",
            "message": item.get("message") or "",
            "task_id": item.get("task_id") or "",
            "history_id": item.get("history_id") or "",
            "created_at": (item.get("video") or {}).get("created_at") if isinstance(item.get("video"), dict) else item.get("created_at") or 0,
            "completed_at": (item.get("video") or {}).get("completed_at") if isinstance(item.get("video"), dict) else item.get("completed_at") or 0,
            "vertical_url": item.get("vertical_url") or ((item.get("video") or {}).get("vertical_url") if isinstance(item.get("video"), dict) else ""),
            "horizontal_url": item.get("horizontal_url") or ((item.get("video") or {}).get("horizontal_url") if isinstance(item.get("video"), dict) else ""),
            "video": item.get("video") or {},
            "youtube_records": item.get("youtube_records") or [],
            "youtube_error": item.get("youtube_error") or "",
            "youtube_urls": [
                record.get("youtube_url")
                for record in (item.get("youtube_records") or [])
                if isinstance(record, dict) and record.get("youtube_url")
            ],
            "error": item.get("error") or "",
        })
    total_count = len(items)
    completed_count = sum(1 for item in items if item.get("status") == "completed")
    failed_count = sum(1 for item in items if item.get("status") == "failed")
    publishing_count = sum(1 for item in items if item.get("status") == "publishing_youtube")
    running_count = sum(1 for item in items if item.get("status") not in {"completed", "failed"})
    return {
        "ok": str(job.get("status") or "") in {"done", "partial"},
        "job_id": job.get("job_id") or "",
        "status": job.get("status") or "",
        "message": job.get("message") or "",
        "total_count": total_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "publishing_count": publishing_count,
        "running_count": running_count,
        "items": items,
    }


def _notify_external_opennews_callback(callback_url: str, payload: dict) -> None:
    callback_url = str(callback_url or "").strip()
    if not callback_url:
        return
    try:
        parsed = urlparse(callback_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("callback_url 不是有效 HTTP/HTTPS URL")
        requests.post(
            callback_url,
            json=payload,
            headers={"X-Token": os.getenv("EXTERNAL_NEWS_API_TOKEN", "")},
            timeout=20,
        ).raise_for_status()
    except Exception as exc:
        print(f"[external_opennews_callback_failed] {callback_url}｜{exc}")


def _external_opennews_video_payload(request: Request, output_dir: Path, result: dict) -> Optional[dict]:
    if not _is_opennews_result(result):
        return None
    history_id = output_dir.name
    workflow_config = result.get("workflow_config") or {}
    variants: dict[str, dict] = {}
    raw_variants = result.get("final_video_variants")
    if isinstance(raw_variants, dict):
        for aspect, variant in raw_variants.items():
            if not isinstance(variant, dict):
                continue
            video_path = str(variant.get("final_video_path") or "")
            rel = _history_relpath_from_value(str(output_dir), video_path)
            if not rel or not (output_dir / rel).exists():
                continue
            variants[str(aspect)] = {
                "aspect_ratio": str(variant.get("compose_aspect_ratio") or aspect),
                "name": Path(video_path).name or f"final_video_{aspect}.mp4",
                "download_url": _external_download_url(request, history_id, output_dir, video_path),
                "size": (output_dir / rel).stat().st_size,
            }
    final_video_path = str(result.get("final_video_path") or "")
    final_rel = _history_relpath_from_value(str(output_dir), final_video_path)
    if final_rel and (output_dir / final_rel).exists() and "default" not in variants:
        variants.setdefault(
            str(workflow_config.get("compose_aspect_ratio") or "default"),
            {
                "aspect_ratio": str(workflow_config.get("compose_aspect_ratio") or "default"),
                "name": Path(final_video_path).name or "final_video.mp4",
                "download_url": _external_download_url(request, history_id, output_dir, final_video_path),
                "size": (output_dir / final_rel).stat().st_size,
            },
        )
    if not variants:
        return None
    source = (workflow_config.get("source") or {}).get("article") or {}
    completed_at = int(output_dir.stat().st_mtime)
    return {
        "id": history_id,
        "history_id": history_id,
        "title": result.get("title") or (result.get("script") or {}).get("title") or result.get("topic") or "OpenNews 新闻",
        "topic": result.get("topic") or "",
        "created_at": completed_at,
        "completed_at": completed_at,
        "duration": result.get("total_duration") or 0,
        "language": (_get_target_market(str(workflow_config.get("target_market") or "cn")).get("content_language") or ""),
        "target_market": workflow_config.get("target_market") or "",
        "source_name": source.get("source_name") or source.get("trend_domain") or "",
        "source_url": source.get("url") or "",
        "published_at": source.get("published_at") or "",
        "summary": source.get("summary_zh") or source.get("translated_summary") or source.get("summary") or "",
        "variants": variants,
        "vertical_url": (variants.get("vertical") or {}).get("download_url") or "",
        "horizontal_url": (variants.get("horizontal") or {}).get("download_url") or "",
        "youtube_records": result.get("youtube_publish_records") if isinstance(result.get("youtube_publish_records"), list) else [],
        "youtube_latest": result.get("youtube_publish_latest") if isinstance(result.get("youtube_publish_latest"), dict) else {},
        "youtube_urls": [
            record.get("youtube_url")
            for record in (result.get("youtube_publish_records") or [])
            if isinstance(record, dict) and record.get("youtube_url")
        ],
    }


def _external_news_user() -> dict:
    username = os.getenv("EXTERNAL_NEWS_OWNER_USERNAME", "admin").strip() or "admin"
    if username not in USERS:
        username = "admin"
    return _public_user(username, USERS[username])


def _opennews_auto_collection_mix_counts() -> dict[str, int]:
    def read_count(name: str, default: int) -> int:
        try:
            return max(0, min(int(os.getenv(name, str(default)) or default), 20))
        except Exception:
            return default

    return {
        "ai": read_count("OPENNEWS_BATCH_COLLECTION_AI_COUNT", 4),
        "robotics": read_count("OPENNEWS_BATCH_COLLECTION_ROBOTICS_COUNT", 1),
        "other": read_count("OPENNEWS_BATCH_COLLECTION_OTHER_COUNT", 5),
    }


def _opennews_batch_item_score(item: dict) -> tuple[float, float]:
    try:
        trend_score = float(item.get("trend_score") or 0)
    except Exception:
        trend_score = 0.0
    try:
        published_ts = float(item.get("published_ts") or item.get("batch_fetched_at") or 0)
    except Exception:
        published_ts = 0.0
    return trend_score, published_ts


def _opennews_batch_item_text(item: dict) -> str:
    fields = [
        item.get("title"),
        item.get("title_zh"),
        item.get("translated_title"),
        item.get("summary"),
        item.get("summary_zh"),
        item.get("translated_summary"),
        item.get("category"),
        item.get("category_name"),
        item.get("source_name"),
        item.get("trend_domain"),
    ]
    return " ".join(str(field or "") for field in fields).lower()


_OPENNEWS_ROBOTICS_KEYWORDS = {
    "robot",
    "robots",
    "robotic",
    "robotics",
    "humanoid",
    "automation",
    "automated",
    "autonomous robot",
    "warehouse robot",
    "industrial robot",
    "boston dynamics",
    "unitree",
    "figure ai",
    "optimus",
    "机器人",
    "人形机器人",
    "自动化",
}

_OPENNEWS_AI_KEYWORDS = {
    " ai ",
    "artificial intelligence",
    "generative ai",
    "openai",
    "anthropic",
    "chatgpt",
    "claude",
    "gemini",
    "llm",
    "large language model",
    "machine learning",
    "neural",
    "nvidia",
    "gpu",
    "data center",
    "datacenter",
    "semiconductor",
    "chip",
    "人工智能",
    "大模型",
    "生成式",
    "英伟达",
    "芯片",
    "算力",
}


def _opennews_auto_collection_bucket(item: dict) -> str:
    forced = str(item.get("auto_collection_bucket") or "").strip().lower()
    if forced in {"ai", "robotics", "other"}:
        return forced
    text = f" {_opennews_batch_item_text(item)} "
    if any(keyword in text for keyword in _OPENNEWS_ROBOTICS_KEYWORDS):
        return "robotics"
    if re.search(r"(?<![a-z])ai(?![a-z])", text) or any(keyword in text for keyword in _OPENNEWS_AI_KEYWORDS):
        return "ai"
    return "other"


def _opennews_auto_collection_item_key(item: dict) -> str:
    title = _opennews_article_title(item).strip().lower()
    url = str(item.get("url") or item.get("source_url") or "").strip().lower()
    item_id = str(item.get("batch_item_id") or item.get("id") or "").strip().lower()
    return url or title or item_id


def _opennews_collection_bucket_counts(items: list[dict]) -> dict[str, int]:
    counts = {"ai": 0, "robotics": 0, "other": 0}
    for item in items:
        bucket_name = _opennews_auto_collection_bucket(item)
        counts[bucket_name] = counts.get(bucket_name, 0) + 1
    return counts


def _opennews_auto_collection_supplement_specs(bucket_name: str) -> list[tuple[str, str]]:
    if bucket_name == "robotics":
        return [
            ("technology", "robotics humanoid robot automation industrial robot"),
            ("ai", "humanoid robot robotics automation embodied AI"),
            ("technology", "warehouse robot autonomous robot robotics startup"),
        ]
    if bucket_name == "ai":
        return [
            ("ai", ""),
            ("technology", "artificial intelligence OpenAI Anthropic Nvidia AI chip"),
            ("ai", "generative AI large language model data center"),
        ]
    return [
        ("finance", ""),
        ("real_estate", ""),
        ("military", ""),
        ("politics", ""),
        ("immigration", ""),
        ("technology", ""),
    ]


def _supplement_opennews_auto_collection_items(
    base_items: list[dict],
    *,
    required_counts: dict[str, int],
    time_range: str,
) -> list[dict]:
    expanded = [dict(item) for item in base_items if isinstance(item, dict)]
    seen = {_opennews_auto_collection_item_key(item) for item in expanded if _opennews_auto_collection_item_key(item)}

    for bucket_name in ("ai", "robotics", "other"):
        current_counts = _opennews_collection_bucket_counts(expanded)
        missing = max(0, int(required_counts.get(bucket_name) or 0) - int(current_counts.get(bucket_name) or 0))
        if missing <= 0:
            continue
        for category, keyword in _opennews_auto_collection_supplement_specs(bucket_name):
            if missing <= 0:
                break
            try:
                result = search_english_trends(
                    category=category,
                    time_range=time_range or "6h",
                    keyword=keyword,
                    limit=max(8, missing * 8),
                )
            except Exception as exc:
                print(
                    f"[OpenNews auto collection] supplement fetch failed bucket={bucket_name} "
                    f"category={category} keyword={keyword!r}: {exc}",
                    flush=True,
                )
                continue
            for candidate in result.get("candidates") or []:
                if missing <= 0:
                    break
                if not isinstance(candidate, dict):
                    continue
                item = dict(candidate)
                key = _opennews_auto_collection_item_key(item)
                if not key or key in seen:
                    continue
                natural_bucket = _opennews_auto_collection_bucket(item)
                if bucket_name == "other" and natural_bucket != "other":
                    continue
                # Targeted补抓来的 AI/机器人新闻按补抓目标归类，避免标题里没有直接出现
                # "robot" 等关键词时又被误放到 other。
                if bucket_name in {"ai", "robotics"}:
                    item["auto_collection_bucket"] = bucket_name
                item["auto_collection_supplement"] = True
                item["auto_collection_supplement_source"] = {
                    "bucket": bucket_name,
                    "category": category,
                    "keyword": keyword,
                }
                if not item.get("batch_item_id"):
                    item["batch_item_id"] = str(item.get("id") or key)[:80]
                if not item.get("batch_category"):
                    item["batch_category"] = f"supplement_{bucket_name}"
                expanded.append(item)
                seen.add(key)
                missing -= 1
    return expanded


def _select_opennews_auto_collection_items(items: list[dict], *, time_range: str = "6h") -> list[dict]:
    counts = _opennews_auto_collection_mix_counts()
    total = sum(counts.values())
    if total <= 0:
        return []

    items = _supplement_opennews_auto_collection_items(items, required_counts=counts, time_range=time_range)
    ranked = sorted(
        [item for item in items if isinstance(item, dict)],
        key=_opennews_batch_item_score,
        reverse=True,
    )
    buckets: dict[str, list[dict]] = {"ai": [], "robotics": [], "other": []}
    for item in ranked:
        buckets[_opennews_auto_collection_bucket(item)].append(item)

    selected: list[dict] = []
    selected_keys: set[str] = set()

    def item_key(item: dict) -> str:
        return str(item.get("batch_item_id") or item.get("id") or _opennews_article_title(item)).strip()

    def take_from(bucket_name: str, count: int) -> None:
        if count <= 0:
            return
        for item in buckets.get(bucket_name, []):
            key = item_key(item)
            if not key or key in selected_keys:
                continue
            selected.append(item)
            selected_keys.add(key)
            if len([existing for existing in selected if _opennews_auto_collection_bucket(existing) == bucket_name]) >= count:
                break

    take_from("ai", counts["ai"])
    take_from("robotics", counts["robotics"])
    take_from("other", counts["other"])

    selected_counts = _opennews_collection_bucket_counts(selected)
    missing = {
        key: max(0, int(counts.get(key) or 0) - int(selected_counts.get(key) or 0))
        for key in counts
    }
    if any(value > 0 for value in missing.values()):
        for bucket_name in ("ai", "robotics", "other"):
            while missing.get(bucket_name, 0) > 0:
                fallback_item = None
                for item in ranked:
                    key = item_key(item)
                    if key and key not in selected_keys:
                        fallback_item = dict(item)
                        selected_keys.add(key)
                        break
                if not fallback_item:
                    break
                fallback_item["auto_collection_bucket"] = bucket_name
                fallback_item["auto_collection_emergency_fill"] = True
                fallback_item["auto_collection_emergency_reason"] = (
                    f"{bucket_name} 补抓数量不足，使用剩余高热度新闻补位，保证合集流程不中断。"
                )
                selected.append(fallback_item)
                missing[bucket_name] = max(0, int(missing.get(bucket_name) or 0) - 1)
        selected_counts = _opennews_collection_bucket_counts(selected)
        missing = {
            key: max(0, int(counts.get(key) or 0) - int(selected_counts.get(key) or 0))
            for key in counts
        }
    if any(value > 0 for value in missing.values()):
        print(
            "[OpenNews auto collection] skip batch: strict mix not satisfied "
            f"required={counts}, selected={selected_counts}, missing={missing}, "
            f"available={{'ai': {len(buckets['ai'])}, 'robotics': {len(buckets['robotics'])}, 'other': {len(buckets['other'])}}}",
            flush=True,
        )
        return []

    return selected[:total]


def _handle_opennews_batch_after_fetch(root: Path, payload: dict) -> None:
    if os.getenv("OPENNEWS_BATCH_AUTO_COLLECTION_PRODUCE", "1").strip().lower() in {"0", "false", "no", "off"}:
        return
    config = load_opennews_batch_config(root)
    triggered_by = str(payload.get("triggered_by") or "")
    if triggered_by != "scheduler" and not config.get("enabled"):
        return
    items = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
    selected = _select_opennews_auto_collection_items(
        items,
        time_range=str(payload.get("time_range") or config.get("time_range") or "6h"),
    )
    if not selected:
        return
    selected_ids = [str(item.get("batch_item_id") or item.get("id") or "").strip() for item in selected]
    selected_ids = [item_id for item_id in selected_ids if item_id]
    if not selected_ids or not OPENNEWS_BATCH_AUTO_PRODUCE_LOCK.acquire(blocking=False):
        return
    try:
        user = _external_news_user()
        job = create_opennews_batch_job(
            root,
            username="auto_opennews_collection",
            items=selected,
            options={
                "target_market": os.getenv("OPENNEWS_BATCH_AUTO_TARGET_MARKET", "cn"),
                "department_id": user.get("department_id") or "real_estate",
                "voice_preset_id": os.getenv("OPENNEWS_BATCH_AUTO_VOICE_PRESET_ID", ""),
                "aspect_ratio": os.getenv("OPENNEWS_BATCH_AUTO_PREVIEW_ASPECT", "horizontal"),
                "notes": "自动抓取批次按 AI 4、机器人 1、其他热点 5 的比例生成新闻合集素材。每条短片只作为合集片段，不单独发布 YouTube。",
                "youtube_auto_publish": False,
                "youtube_privacy_status": os.getenv("OPENNEWS_BATCH_AUTO_YOUTUBE_PRIVACY", "public"),
                "youtube_aspects": ["horizontal"],
                "auto_collection_batch_id": payload.get("batch_id") or "",
                "auto_collection_item_ids": selected_ids,
                "auto_collection_mix_counts": _opennews_auto_collection_mix_counts(),
                "auto_collection_direct": True,
            },
        )
        mark_opennews_batch_items(
            root,
            selected_ids,
            {
                "status": "auto_producing",
                "auto_produce_job_id": job.get("job_id") or "",
                "auto_produce_selected_at": time.time(),
                "auto_produce_reason": "hourly_collection_mix",
                "auto_collection_mix_counts": _opennews_auto_collection_mix_counts(),
            },
        )
        thread = threading.Thread(
            target=_run_opennews_external_produce_job,
            kwargs={
                "job_id": job.get("job_id"),
                "user": dict(user),
                "public_base_url": os.getenv("PUBLIC_BASE_URL", "https://aiagent.office.ihousejapan.cn"),
            },
            daemon=True,
        )
        thread.start()
    finally:
        OPENNEWS_BATCH_AUTO_PRODUCE_LOCK.release()


def _external_candidate_payload(item: dict) -> dict:
    return {
        "id": str(item.get("batch_item_id") or item.get("id") or ""),
        "batch_item_id": str(item.get("batch_item_id") or item.get("id") or ""),
        "batch_id": str(item.get("batch_id") or ""),
        "title": _opennews_article_title(item),
        "title_original": item.get("title") or "",
        "summary": item.get("summary_zh") or item.get("translated_summary") or item.get("summary") or "",
        "source_name": item.get("source_name") or item.get("trend_domain") or "",
        "source_url": item.get("url") or "",
        "published_at": item.get("published_at") or "",
        "category": item.get("category_name") or item.get("category") or item.get("batch_category") or "",
        "trend_score": item.get("trend_score") or "",
    }


def _external_ready_video_titles() -> list[str]:
    titles: list[str] = []
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        if not _is_opennews_result(result):
            continue
        title = str((result or {}).get("title") or ((result or {}).get("script") or {}).get("title") or "").strip()
        if title:
            titles.append(title)
    return list(dict.fromkeys(titles))


def _wait_for_opennews_task_done(task_id: str, *, timeout_seconds: int = 5400) -> dict:
    deadline = time.time() + max(60, timeout_seconds)
    while time.time() < deadline:
        task = tasks.get(task_id) or {}
        tracker = task.get("tracker")
        tracker_status = getattr(tracker, "status", "")
        output_dir = task.get("output_dir")
        if output_dir and not task.get("result"):
            result_path = Path(str(output_dir)) / "result.json"
            if result_path.exists():
                try:
                    task["result"] = json.loads(result_path.read_text(encoding="utf-8"))
                    tasks[task_id] = task
                except Exception:
                    pass
        if tracker_status == "done" and task.get("result") and task.get("output_dir"):
            return task
        # Some long OpenNews jobs can finish writing result.json before the
        # in-memory tracker flips to done. Only continue once material files
        # are present, otherwise compose can race ahead of material download.
        if (
            task.get("result")
            and task.get("output_dir")
            and tracker_status not in {"error", "cancelled"}
            and _opennews_result_has_material_assets(task.get("result") or {}, Path(str(task.get("output_dir"))))
        ):
            return task
        if tracker_status in {"error", "cancelled"}:
            messages = getattr(tracker, "messages", []) or []
            last_message = messages[-1].get("message") if messages else "任务失败"
            raise RuntimeError(str(last_message))
        time.sleep(10)
    raise RuntimeError("等待 OpenNews 视频任务完成超时。")


def _run_opennews_external_produce_job(job_id: str, *, user: dict, public_base_url: str) -> None:
    def set_job_status(status: str, message: str) -> None:
        update_opennews_batch_job(
            OPENNEWS_BATCH_DIR,
            job_id,
            lambda job: job.update({"status": status, "message": message}),
        )

    def sync_batch_item(item_id: str, **updates: Any) -> None:
        if not item_id:
            return
        payload = dict(updates)
        payload["auto_produce_job_id"] = job_id
        mark_opennews_batch_items(OPENNEWS_BATCH_DIR, [item_id], payload)

    job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id)
    if not job:
        return
    options = dict(job.get("options") or {})
    target_market = str(options.get("target_market") or user.get("target_market") or "cn")
    department_id = str(options.get("department_id") or user.get("department_id") or "real_estate")
    voice_preset_id = str(options.get("voice_preset_id") or "")
    preferred_aspect_ratio = str(options.get("aspect_ratio") or "vertical")
    notes = str(options.get("notes") or "")
    external_request = dict(options.get("external_request") or {})
    callback_url = str(external_request.get("callback_url") or options.get("callback_url") or "").strip()
    youtube_auto_publish = bool(options.get("youtube_auto_publish") or external_request.get("youtube_auto_publish"))
    youtube_privacy_status = str(options.get("youtube_privacy_status") or external_request.get("youtube_privacy_status") or "public")
    youtube_aspects_raw = options.get("youtube_aspects") or external_request.get("youtube_aspects") or ["horizontal", "vertical"]
    if isinstance(youtube_aspects_raw, str):
        if youtube_aspects_raw == "both":
            youtube_aspects = ["horizontal", "vertical"]
        else:
            youtube_aspects = [part.strip() for part in youtube_aspects_raw.split(",") if part.strip()]
    elif isinstance(youtube_aspects_raw, list):
        youtube_aspects = [str(part).strip() for part in youtube_aspects_raw if str(part).strip()]
    else:
        youtube_aspects = ["horizontal", "vertical"]
    set_job_status("running", "外部审核已确认，正在一站式生成新闻视频成片...")

    total_items = len(job.get("items") or [])
    for index, item in enumerate(job.get("items") or []):
        item_id = str(item.get("batch_item_id") or "")
        if item.get("status") == "completed":
            update_opennews_batch_job(
                OPENNEWS_BATCH_DIR,
                job_id,
                lambda payload, idx=index: payload.update({"message": f"外部审核视频生产进度：{idx + 1}/{total_items}"}),
            )
            continue

        def mark_item(**updates: Any) -> None:
            def updater(payload: dict) -> None:
                for existing in payload.get("items", []) or []:
                    if str(existing.get("batch_item_id") or "") == item_id:
                        existing.update(updates)
                        break
            update_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id, updater)

        try:
            article = dict(item.get("article") or {})
            mark_item(status="drafting", message="正在生成新闻口播稿...")
            draft = generate_opennews_draft(article=article, target_market=target_market, notes=notes)
            mark_item(status="producing", message="正在生成配音和匹配素材...", draft=draft)
            task_result = _create_opennews_material_task(
                user=user,
                public_base_url=public_base_url,
                article=article,
                draft=draft,
                target_market=target_market,
                department_id=department_id,
                voice_preset_id=voice_preset_id,
                aspect_ratio=preferred_aspect_ratio,
            )
            task_id = str(task_result.get("task_id") or "")
            mark_item(task_id=task_id, message=f"视频生产任务已提交：{task_id}，等待中间产物完成...")
            task = _wait_for_opennews_task_done(task_id)
            mark_item(status="composing", message="中间产物完成，正在自动合成横屏和竖屏成片...")
            composed_result = _compose_opennews_task_video(task_id, preferred_aspect_ratio=preferred_aspect_ratio)
            material_review = _opennews_material_review_status(composed_result)
            if material_review.get("uses_strict_source_fallback"):
                composed_result["material_review"] = material_review
                _save_result_to_output_dir(Path(task.get("output_dir") or ""), composed_result)
            output_dir = Path(task.get("output_dir") or "")
            video_payload = _external_video_urls_for_result(public_base_url, output_dir, composed_result)
            youtube_records: list[dict] = []
            youtube_error = ""
            if youtube_auto_publish:
                try:
                    publish_message = "成片完成，正在自动发布到 YouTube..."
                    if material_review.get("uses_strict_source_fallback"):
                        publish_message = "成片使用严格新闻源兜底素材，安全过滤通过，正在自动发布到 YouTube..."
                    mark_item(status="publishing_youtube", message=publish_message, material_review=material_review)
                    youtube_records = _publish_opennews_result_to_youtube(
                        output_dir,
                        composed_result,
                        aspects=youtube_aspects,
                        privacy_status=youtube_privacy_status,
                    )
                except Exception as youtube_exc:
                    youtube_error = str(youtube_exc)
            final_status = "completed"
            final_message = (
                "成片已完成，可直接下载。"
                if not youtube_auto_publish
                else ("成片已完成，YouTube 已发布。" if not youtube_error else f"成片已完成，但 YouTube 发布失败：{youtube_error}")
            )
            mark_item(
                status=final_status,
                message=final_message,
                history_id=output_dir.name,
                video=video_payload,
                vertical_url=video_payload.get("vertical_url", ""),
                horizontal_url=video_payload.get("horizontal_url", ""),
                youtube_records=youtube_records,
                youtube_error=youtube_error,
                material_review=material_review,
                error="",
            )
            sync_batch_item(
                item_id,
                status=final_status,
                message=final_message,
                history_id=output_dir.name,
                video=video_payload,
                vertical_url=video_payload.get("vertical_url", ""),
                horizontal_url=video_payload.get("horizontal_url", ""),
                youtube_records=youtube_records,
                youtube_error=youtube_error,
                material_review=material_review,
                error="",
                completed_at=time.time(),
            )
        except Exception as exc:
            mark_item(status="failed", message=f"生成失败：{exc}", error=str(exc))
            sync_batch_item(item_id, status="failed", message=f"生成失败：{exc}", error=str(exc), completed_at=time.time())
        update_opennews_batch_job(
            OPENNEWS_BATCH_DIR,
            job_id,
            lambda payload, idx=index: payload.update({"message": f"外部审核视频生产进度：{idx + 1}/{total_items}"}),
        )

    final_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id) or {}
    failed = sum(1 for item in final_job.get("items", []) or [] if item.get("status") == "failed")
    completed = sum(1 for item in final_job.get("items", []) or [] if item.get("status") == "completed")
    set_job_status("done" if failed == 0 else "partial", f"外部审核视频已完成：{completed} 条成功，{failed} 条失败。")
    if completed and options.get("auto_collection_direct"):
        history_ids = [
            str(item.get("history_id") or "").strip()
            for item in final_job.get("items", []) or []
            if item.get("status") == "completed" and str(item.get("history_id") or "").strip()
        ]
        try:
            collection_result = _build_and_publish_opennews_collection(
                history_ids,
                reason=f"batch_job:{job_id}",
                privacy_status=youtube_privacy_status,
            )
            update_opennews_batch_job(
                OPENNEWS_BATCH_DIR,
                job_id,
                lambda job: job.update({
                    "collection_status": "done",
                    "collection_job_id": collection_result.get("job_id") or "",
                    "collection_message": "自动合集已生成并发布 YouTube。",
                    "collection_youtube_record": collection_result.get("youtube_record") or {},
                    "message": f"自动合集已完成：{completed} 条短片入合集，已发布 YouTube。",
                }),
            )
        except Exception as collection_exc:
            update_opennews_batch_job(
                OPENNEWS_BATCH_DIR,
                job_id,
                lambda job: job.update({
                    "collection_status": "failed",
                    "collection_error": str(collection_exc),
                    "collection_message": f"自动合集失败：{collection_exc}",
                    "message": f"短片已完成 {completed} 条，但自动合集失败：{collection_exc}",
                }),
            )
    elif completed:
        _trigger_opennews_collection_auto_check(f"batch_job:{job_id}")
    if callback_url:
        latest_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id) or final_job
        _notify_external_opennews_callback(callback_url, _external_opennews_job_result_payload(latest_job))


@app.get("/api/external/opennews/health")
async def external_opennews_health(request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    batch_config = load_opennews_batch_config(OPENNEWS_BATCH_DIR)
    youtube_config = youtube_env_config()
    return {
        "ok": True,
        "service": "ihouse-opennews",
        "time": int(time.time()),
        "auto_fetch": {
            "enabled": bool(batch_config.get("enabled")),
            "interval_minutes": batch_config.get("interval_minutes"),
            "limit": batch_config.get("limit"),
            "last_run_at": batch_config.get("last_run_at"),
            "next_run_at": batch_config.get("next_run_at"),
            "last_run_message": batch_config.get("last_run_message") or "",
            "last_run_error": batch_config.get("last_run_error") or "",
        },
        "youtube": {
            "configured": bool(youtube_config.get("client_id") and youtube_config.get("client_secret") and (youtube_config.get("refresh_token") or YOUTUBE_TOKEN_STORE_PATH.exists())),
            "default_auto_publish": True,
            "default_privacy_status": "public",
            "default_aspects": ["horizontal", "vertical"],
        },
    }


@app.get("/api/external/opennews/used-titles")
async def external_opennews_used_titles(request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    titles = _external_ready_video_titles()
    return {"titles": titles, "count": len(titles)}


@app.get("/api/external/opennews/candidate-batches")
async def external_opennews_candidate_batches(request: Request, limit: int = 10, exclude_used: bool = True):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    used_keys = {_normal_title_key(title) for title in _external_ready_video_titles()} if exclude_used else set()
    max_batches = max(1, min(int(limit or 10), 50))
    batches = []
    for batch in list_opennews_batches(OPENNEWS_BATCH_DIR, limit=max_batches):
        items = []
        for item in batch.get("items") or []:
            payload = _external_candidate_payload(item)
            if exclude_used and _normal_title_key(payload.get("title")) in used_keys:
                continue
            if payload.get("id"):
                items.append(payload)
        batches.append({
            "batch_id": batch.get("batch_id") or "",
            "started_at": batch.get("started_at") or 0,
            "finished_at": batch.get("finished_at") or 0,
            "category": batch.get("category") or "",
            "time_range": batch.get("time_range") or "",
            "triggered_by": batch.get("triggered_by") or "",
            "message": batch.get("message") or "",
            "raw_count": batch.get("raw_count") or 0,
            "duplicate_count": batch.get("duplicate_count") or 0,
            "items": items,
            "count": len(items),
        })
    return {"batches": batches, "count": len(batches)}


@app.post("/api/external/opennews/produce-selected")
async def external_opennews_produce_selected(request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    item_ids = payload.get("item_ids") or payload.get("ids") or []
    if isinstance(item_ids, str):
        item_ids = [part.strip() for part in item_ids.split(",") if part.strip()]
    if not isinstance(item_ids, list) or not item_ids:
        return JSONResponse({"error": "缺少 item_ids，请提交要生成的视频新闻 id。"}, status_code=400)
    item_ids = [str(item or "").strip() for item in item_ids if str(item or "").strip()]
    if len(item_ids) > 8:
        return JSONResponse({"error": "一次最多触发 8 条新闻生成。"}, status_code=400)
    items = find_opennews_batch_items(OPENNEWS_BATCH_DIR, item_ids)
    if not items:
        return JSONResponse({
            "error": "未找到对应候选新闻。",
            "requested_count": len(item_ids),
            "accepted_count": 0,
            "missing_item_ids": item_ids,
        }, status_code=404)
    found_ids = {
        str(item.get("batch_item_id") or item.get("id") or "").strip()
        for item in items
        if str(item.get("batch_item_id") or item.get("id") or "").strip()
    }
    missing_item_ids = [item_id for item_id in item_ids if item_id not in found_ids]
    allow_partial = bool(payload.get("allow_partial"))
    if missing_item_ids and not allow_partial:
        return JSONResponse({
            "error": "部分候选新闻 id 未找到，本次未启动生成。请重新从 candidate-batches 返回的 id/batch_item_id 中提交。",
            "requested_count": len(item_ids),
            "accepted_count": len(items),
            "missing_item_ids": missing_item_ids,
            "accepted_item_ids": sorted(found_ids),
        }, status_code=400)
    user = _external_news_user()
    target_market = str(payload.get("target_market") or "cn")
    voice_preset_id = str(payload.get("voice_preset_id") or "")
    aspect_ratio = str(payload.get("aspect_ratio") or "vertical")
    feedback = str(payload.get("feedback") or payload.get("notes") or "")
    callback_url = str(payload.get("callback_url") or "").strip()
    youtube_auto_publish = payload.get("youtube_auto_publish")
    if youtube_auto_publish is None:
        youtube_auto_publish = True
    else:
        youtube_auto_publish = bool(youtube_auto_publish)
    youtube_aspects = payload.get("youtube_aspects") or ["horizontal", "vertical"]
    if isinstance(youtube_aspects, str):
        youtube_aspects = ["horizontal", "vertical"] if youtube_aspects == "both" else [part.strip() for part in youtube_aspects.split(",") if part.strip()]
    elif isinstance(youtube_aspects, list):
        youtube_aspects = [str(part).strip() for part in youtube_aspects if str(part).strip()]
    else:
        youtube_aspects = ["horizontal", "vertical"]
    wait_until_done = bool(payload.get("wait") or payload.get("wait_until_done") or payload.get("sync"))
    try:
        wait_timeout_seconds = int(payload.get("wait_timeout_seconds") or 900)
    except Exception:
        wait_timeout_seconds = 900
    wait_timeout_seconds = max(30, min(wait_timeout_seconds, 1800))
    public_base_url = _get_public_base_url(request)
    job = create_opennews_batch_job(
        OPENNEWS_BATCH_DIR,
        username=user.get("username") or "admin",
        items=items,
        options={
            "target_market": target_market,
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": voice_preset_id,
            "aspect_ratio": aspect_ratio,
            "notes": feedback,
            "youtube_auto_publish": youtube_auto_publish,
            "youtube_privacy_status": str(payload.get("youtube_privacy_status") or "public"),
            "youtube_aspects": youtube_aspects or ["horizontal", "vertical"],
            "external_trigger": True,
            "external_request": {
                "item_ids": item_ids,
                "feedback": feedback,
                "callback_url": callback_url,
            },
        },
    )
    thread = threading.Thread(
        target=_run_opennews_external_produce_job,
        kwargs={
            "job_id": job.get("job_id"),
            "user": dict(user),
            "public_base_url": public_base_url,
        },
        daemon=True,
    )
    thread.start()
    external_base_url = public_base_url.rstrip("/")
    status_url = f"{external_base_url}/api/external/opennews/jobs/{quote(str(job.get('job_id') or ''), safe='')}"
    response_payload = {
        "ok": True,
        "job_id": job.get("job_id"),
        "job": job,
        "status_url": status_url,
        "ready_videos_url": f"{external_base_url}/api/external/opennews/ready-videos?limit=50",
        "youtube_auto_publish": youtube_auto_publish,
        "youtube_privacy_status": str(payload.get("youtube_privacy_status") or "public"),
        "youtube_aspects": youtube_aspects or ["horizontal", "vertical"],
        "requested_count": len(item_ids),
        "accepted_count": len(items),
        "missing_item_ids": missing_item_ids,
        "accepted_item_ids": sorted(found_ids),
        "mode": "sync" if wait_until_done else "async",
        "message": "已接收外部审核选择，开始自动生成新闻视频；完成后 job.items 会返回 vertical_url、horizontal_url 和 youtube_records。",
    }
    if not wait_until_done:
        return response_payload

    deadline = time.time() + wait_timeout_seconds
    while time.time() < deadline:
        latest_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, str(job.get("job_id") or ""))
        if latest_job and latest_job.get("status") in {"done", "partial", "failed", "error"}:
            result_payload = _external_opennews_job_result_payload(latest_job)
            status_code = 200 if result_payload.get("status") in {"done", "partial"} else 500
            return JSONResponse(result_payload, status_code=status_code)
        await asyncio.sleep(5)
    latest_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, str(job.get("job_id") or "")) or job
    timeout_payload = _external_opennews_job_result_payload(latest_job)
    timeout_payload.update({
        "ok": False,
        "status_url": status_url,
        "message": "视频仍在生成中，请继续查询 status_url；完成后会返回 vertical_url 和 horizontal_url。",
    })
    return JSONResponse(timeout_payload, status_code=202)


@app.get("/api/external/opennews/jobs/{job_id}")
async def external_opennews_job_status(job_id: str, request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id)
    if not job:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    payload = _external_opennews_job_result_payload(job)
    payload["job"] = job
    return payload


@app.get("/api/external/opennews/ready-videos")
async def external_opennews_ready_videos(request: Request, limit: int = 50):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    videos: list[dict] = []
    max_items = max(1, min(int(limit or 50), 200))
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        payload = _external_opennews_video_payload(request, output_dir, result or {})
        if not payload:
            continue
        videos.append(payload)
        if len(videos) >= max_items:
            break
    return {"videos": videos, "count": len(videos)}


@app.get("/api/external/opennews/videos/{history_id}")
async def external_opennews_video_detail(history_id: str, request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    output_dir = _resolve_history_output_dir(history_id)
    if not output_dir:
        return JSONResponse({"error": "视频不存在"}, status_code=404)
    result = _load_result_from_output_dir(output_dir)
    payload = _external_opennews_video_payload(request, output_dir, result or {})
    if not payload:
        return JSONResponse({"error": "这条记录不是已完成的 OpenNews 成片"}, status_code=404)
    return {"video": payload}


@app.get("/api/external/opennews/videos/{history_id}/download/{file_path:path}")
async def external_opennews_video_download(history_id: str, file_path: str, request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    output_dir = _resolve_history_output_dir(history_id)
    if not output_dir:
        return JSONResponse({"error": "视频不存在"}, status_code=404)
    result = _load_result_from_output_dir(output_dir)
    if not _is_opennews_result(result):
        return JSONResponse({"error": "这条记录不是 OpenNews 成片"}, status_code=404)
    base = output_dir.resolve()
    target = (output_dir / file_path).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if not target.exists() or not target.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(target), filename=target.name, media_type="video/mp4")


@app.get("/api/external/ready-videos")
async def external_ready_videos(request: Request, limit: int = 50, video_type: str = "all"):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    requested_type = str(video_type or "all").strip().lower()
    if requested_type not in {"all", "digital_human", "property_video"}:
        return JSONResponse({"error": "video_type 只支持 all、digital_human、property_video"}, status_code=400)
    videos: list[dict] = []
    try:
        max_items = max(1, min(int(limit or 50), 200))
    except Exception:
        max_items = 50
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        payload = _external_general_video_payload(request, output_dir, result or {})
        if not payload:
            continue
        if requested_type != "all" and payload.get("type") != requested_type:
            continue
        videos.append(payload)
        if len(videos) >= max_items:
            break
    return {"videos": videos, "count": len(videos)}


@app.get("/api/external/videos/{history_id}/download/{file_path:path}")
async def external_ready_video_download(history_id: str, file_path: str, request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    output_dir = _resolve_history_output_dir(history_id)
    if not output_dir:
        return JSONResponse({"error": "视频不存在"}, status_code=404)
    result = _load_result_from_output_dir(output_dir)
    if not (_is_digital_human_result(result) or _is_property_video_result(result)):
        return JSONResponse({"error": "这条记录不是数字人或房源最终成片"}, status_code=404)
    base = output_dir.resolve()
    target = (output_dir / file_path).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if not target.exists() or not target.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    allowed_paths = set()
    for video_path in _external_final_video_paths(result or {}):
        rel = _history_relpath_from_value(str(output_dir), video_path)
        if rel:
            allowed_paths.add((output_dir / rel).resolve())
    if target not in allowed_paths:
        return JSONResponse({"error": "只允许下载最终成片文件"}, status_code=403)
    return FileResponse(str(target), filename=target.name, media_type="video/mp4")


@app.post("/api/external/opennews/proposals/decision")
async def external_opennews_proposal_decision(request: Request):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    local_proposal_id = str(payload.get("local_proposal_id") or payload.get("id") or "").strip()
    if not local_proposal_id:
        return JSONResponse({"error": "缺少 local_proposal_id"}, status_code=400)
    proposal = load_localtok_proposal(OPENNEWS_LOCALTOK_DIR, local_proposal_id)
    if not proposal:
        return JSONResponse({"error": "提案不存在"}, status_code=404)
    username = str(proposal.get("username") or "admin")
    profile = USERS.get(username) or USERS.get("admin") or {}
    user = _public_user(username if username in USERS else "admin", profile)
    decision = {
        "status": str(payload.get("status") or "decided"),
        "choice": str(payload.get("choice") or ""),
        "feedback": str(payload.get("feedback") or ""),
    }
    updated = update_localtok_proposal(
        OPENNEWS_LOCALTOK_DIR,
        local_proposal_id,
        lambda row: row.update({
            "status": "decided",
            "message": "外部系统已回传审核决定，准备自动生成并发布视频。",
            "decision": decision,
            "error": "",
        }),
    )
    thread = threading.Thread(
        target=_run_localtok_decided_production,
        kwargs={
            "local_proposal_id": local_proposal_id,
            "user": dict(user),
            "public_base_url": _get_public_base_url(request),
        },
        daemon=True,
    )
    thread.start()
    return {"ok": True, "proposal": updated}


@app.post("/api/opennews/auto/config")
async def opennews_auto_config_update(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    config = save_opennews_auto_config(
        OPENNEWS_AUTO_DIR,
        {
            "enabled": bool(payload.get("enabled")),
            "interval_minutes": payload.get("interval_minutes"),
            "categories": payload.get("categories") or [],
            "time_range": payload.get("time_range") or "6h",
            "limit": payload.get("limit") or 20,
        },
    )
    return {"config": config}


@app.post("/api/opennews/auto/run-now")
async def opennews_auto_run_now(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    result = run_auto_fetch_once(OPENNEWS_AUTO_DIR, triggered_by=user.get("username") or "manual")
    status_code = 202 if result.get("running") else 200
    return JSONResponse(result, status_code=status_code)


@app.get("/api/opennews/auto/candidates")
async def opennews_auto_candidates(request: Request, status: str = "pending"):
    user, error = _require_user(request)
    if error:
        return error
    candidates = list_opennews_auto_candidates(OPENNEWS_AUTO_DIR, status=status or "pending", limit=160)
    return {"candidates": candidates, "count": len(candidates)}


@app.post("/api/opennews/auto/candidates/{candidate_id}/status")
async def opennews_auto_candidate_status(candidate_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    status = str(payload.get("status") or "").strip()
    try:
        candidate = update_auto_candidate_status(
            OPENNEWS_AUTO_DIR,
            candidate_id,
            status,
            username=user.get("username") or "",
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if not candidate:
        return JSONResponse({"error": "自动候选不存在"}, status_code=404)
    return {"candidate": candidate}


@app.post("/api/admin/opennews/draft")
async def admin_opennews_draft(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    article = payload.get("article") or {}
    if not article.get("url"):
        return JSONResponse({"error": "缺少新闻链接"}, status_code=400)
    try:
        draft = generate_opennews_draft(
            article=article,
            target_market=str(payload.get("target_market") or "cn"),
            notes=str(payload.get("notes") or ""),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "draft", {"article": article, "draft": draft, "user": user.get("username")})
    return {"draft": draft}


@app.post("/api/opennews/draft")
async def opennews_draft(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    article = payload.get("article") or {}
    if not article.get("url"):
        return JSONResponse({"error": "缺少新闻链接"}, status_code=400)
    try:
        draft = generate_opennews_draft(
            article=article,
            target_market=str(payload.get("target_market") or user.get("target_market") or "cn"),
            notes=str(payload.get("notes") or ""),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "draft", {"article": article, "draft": draft, "user": user.get("username")})
    return {"draft": draft}


def _set_opennews_draft_job(job_id: str, updates: dict[str, Any]) -> None:
    with OPENNEWS_DRAFT_LOCK:
        job = OPENNEWS_DRAFT_JOBS.get(job_id, {})
        job.update(updates)
        OPENNEWS_DRAFT_JOBS[job_id] = job


def _run_opennews_draft_job(job_id: str, *, article: dict, target_market: str, notes: str, username: str) -> None:
    _set_opennews_draft_job(job_id, {
        "status": "running",
        "message": "正在读取原文、提取原站素材并生成新闻稿...",
        "updated_at": time.time(),
    })
    try:
        draft = generate_opennews_draft(
            article=article,
            target_market=target_market,
            notes=notes,
        )
        save_opennews_payload(OPENNEWS_ADMIN_DIR, "draft", {"article": article, "draft": draft, "user": username})
        _set_opennews_draft_job(job_id, {
            "status": "done",
            "message": "新闻稿已生成",
            "draft": draft,
            "updated_at": time.time(),
        })
    except Exception as exc:
        _set_opennews_draft_job(job_id, {
            "status": "failed",
            "message": str(exc),
            "error": str(exc),
            "updated_at": time.time(),
        })


@app.post("/api/opennews/draft/start")
async def opennews_draft_start(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    article = payload.get("article") or {}
    if not article.get("url"):
        return JSONResponse({"error": "缺少新闻链接"}, status_code=400)
    job_id = uuid.uuid4().hex[:12]
    username = str(user.get("username") or "")
    target_market = str(payload.get("target_market") or user.get("target_market") or "cn")
    notes = str(payload.get("notes") or "")
    with OPENNEWS_DRAFT_LOCK:
        OPENNEWS_DRAFT_JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "message": "新闻稿任务已提交",
            "article": article,
            "username": username,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
    thread = threading.Thread(
        target=_run_opennews_draft_job,
        kwargs={
            "job_id": job_id,
            "article": article,
            "target_market": target_market,
            "notes": notes,
            "username": username,
        },
        daemon=True,
    )
    thread.start()
    return {"job_id": job_id, "status": "queued", "message": "新闻稿任务已提交，正在后台生成"}


@app.get("/api/opennews/draft/status/{job_id}")
async def opennews_draft_status(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    with OPENNEWS_DRAFT_LOCK:
        job = dict(OPENNEWS_DRAFT_JOBS.get(job_id) or {})
    if not job:
        return JSONResponse({"error": "新闻稿任务不存在或已过期"}, status_code=404)
    if job.get("username") != user.get("username") and not _is_admin(user):
        return _forbidden_error()
    return {
        "job_id": job_id,
        "status": job.get("status") or "unknown",
        "message": job.get("message") or "",
        "error": job.get("error") or "",
        "draft": job.get("draft"),
    }


def _pick_compatible_avatar_for_opennews(target_market: str, voice_preset: dict, requested_avatar_id: str = "") -> Optional[dict]:
    if requested_avatar_id:
        avatar = _get_avatar_option(requested_avatar_id, target_market_id=target_market)
        if avatar and _is_avatar_voice_compatible(avatar, voice_preset):
            return avatar
    for avatar in _list_avatar_options(target_market_id=target_market):
        enriched = _get_avatar_option(avatar.get("id"), target_market_id=target_market)
        if enriched and _is_avatar_voice_compatible(enriched, voice_preset):
            return enriched
    return _get_avatar_option(None, target_market_id=target_market)


def _create_opennews_material_task(
    *,
    user: dict,
    public_base_url: str,
    article: dict,
    draft: dict,
    target_market: str,
    department_id: str,
    voice_preset_id: str,
    aspect_ratio: str,
) -> dict:
    target_market = str(target_market or user.get("target_market") or "cn").strip() or "cn"
    if target_market not in {item["id"] for item in TARGET_MARKETS}:
        target_market = "cn"
    department_id = str(department_id or user.get("department_id") or "real_estate").strip() or "real_estate"
    voice_preset_id = str(voice_preset_id or _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female").strip()
    if voice_preset_id not in _get_visible_voice_preset_ids(target_market):
        voice_preset_id = _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female"
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    voice_preset["selected_speed"] = float(voice_preset.get("default_speed") or 1.1)
    aspect_ratio = str(aspect_ratio or "horizontal").strip().lower()
    if aspect_ratio not in {"vertical", "horizontal"}:
        aspect_ratio = "horizontal"
    script_data = build_opennews_script_data(draft=draft, article=article, target_market=target_market)
    topic = f"OpenNews：{script_data.get('title') or article.get('title') or '新闻视频'}"
    submission_key = _make_produce_submission_key(
        owner_username=user.get("username", ""),
        topic=topic,
        script_data=script_data,
        voice_preset_id=voice_preset.get("id", voice_preset_id),
        avatar_id="",
        speed=float(voice_preset.get("selected_speed") or 1.1),
        web_search_enabled=False,
        target_market=target_market,
        department_id=department_id,
        script_model=SCRIPT_MODEL_CLAUDE,
        digital_human_engine="opennews_material_only",
        compose_aspect_ratio=aspect_ratio,
    )
    reusable_task = _find_reusable_running_task(owner_username=user.get("username", ""), submission_key=submission_key)
    if reusable_task:
        return {
            "task_id": reusable_task.get("id", ""),
            "reused_existing": True,
            "script": script_data,
            "message": "OpenNews 新闻视频任务已在后台执行",
        }
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": topic,
        "image_path": "",
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": public_base_url,
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "submission_key": submission_key,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id", voice_preset_id),
            "avatar_id": "",
            "speed": voice_preset.get("selected_speed", 1.1),
            "web_search_enabled": False,
            "target_market": target_market,
            "department_id": department_id,
            "compose_transition_id": "fade",
            "subtitle_template_id": "property_clear",
            "compose_aspect_ratio": aspect_ratio,
            "source": {"kind": "opennews", "article": article},
            "script_model": SCRIPT_MODEL_CLAUDE,
            "digital_human_engine": "opennews_material_only",
            "opennews": True,
            "opennews_material_only": True,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("OpenNews 新闻视频任务已创建，准备进入素材成片链路...")
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, "", public_base_url, script_data, voice_preset, None),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id, "reused_existing": False, "script": script_data}


def _run_opennews_batch_produce_job(job_id: str, *, user: dict, public_base_url: str) -> None:
    def set_job_status(status: str, message: str) -> None:
        update_opennews_batch_job(
            OPENNEWS_BATCH_DIR,
            job_id,
            lambda job: job.update({"status": status, "message": message}),
        )

    job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id)
    if not job:
        return
    options = dict(job.get("options") or {})
    target_market = str(options.get("target_market") or user.get("target_market") or "cn")
    department_id = str(options.get("department_id") or user.get("department_id") or "real_estate")
    voice_preset_id = str(options.get("voice_preset_id") or "")
    aspect_ratio = str(options.get("aspect_ratio") or "horizontal")
    notes = str(options.get("notes") or "")
    set_job_status("running", "正在批量生成新闻稿并提交成片任务...")
    for index, item in enumerate(job.get("items") or []):
        item_id = str(item.get("batch_item_id") or "")

        def mark_item(**updates: Any) -> None:
            def updater(payload: dict) -> None:
                for existing in payload.get("items", []) or []:
                    if str(existing.get("batch_item_id") or "") == item_id:
                        existing.update(updates)
                        break
            update_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id, updater)

        try:
            article = dict(item.get("article") or {})
            mark_item(status="drafting", message="正在生成新闻稿...")
            draft = generate_opennews_draft(article=article, target_market=target_market, notes=notes)
            mark_item(status="submitting", message="正在提交视频生产任务...", draft=draft)
            result = _create_opennews_material_task(
                user=user,
                public_base_url=public_base_url,
                article=article,
                draft=draft,
                target_market=target_market,
                department_id=department_id,
                voice_preset_id=voice_preset_id,
                aspect_ratio=aspect_ratio,
            )
            mark_item(
                status="submitted",
                message="已提交到当前任务",
                task_id=result.get("task_id", ""),
                reused_existing=bool(result.get("reused_existing")),
            )
        except Exception as exc:
            mark_item(status="failed", message=str(exc), error=str(exc))
        update_opennews_batch_job(
            OPENNEWS_BATCH_DIR,
            job_id,
            lambda payload, idx=index: payload.update({"message": f"批量生产进度：{idx + 1}/{len(payload.get('items') or [])}"}),
        )
    final_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id) or {}
    failed = sum(1 for item in final_job.get("items", []) or [] if item.get("status") == "failed")
    submitted = sum(1 for item in final_job.get("items", []) or [] if item.get("task_id"))
    set_job_status("done" if failed == 0 else "partial", f"批量生产已提交：{submitted} 条成功，{failed} 条失败。")


def _normal_title_key(title: Any) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip().lower())


def _opennews_article_title(article: dict) -> str:
    return str(article.get("title_zh") or article.get("translated_title") or article.get("title") or "OpenNews 新闻").strip()


def _build_localtok_proposal_payload(items: list[dict], used_titles: list[str]) -> tuple[list[dict], list[str], str, list[str], list[str]]:
    used_keys = {_normal_title_key(title) for title in used_titles if _normal_title_key(title)}
    filtered_items: list[dict] = []
    duplicate_titles: list[str] = []
    for item in items:
        title = _opennews_article_title(item)
        if _normal_title_key(title) in used_keys:
            duplicate_titles.append(title)
            continue
        filtered_items.append(dict(item))
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    titles = [_opennews_article_title(item) for item in filtered_items]
    source_names = [str(item.get("source_name") or item.get("trend_domain") or "英文热点源") for item in filtered_items]
    summary = (
        f"本轮从 OpenNews 热点批次中选出 {len(titles)} 条候选新闻，"
        f"来源包括：{'、'.join(sorted(set(source_names))[:5])}。请审核选择适合制作 LocalTok 新闻短视频的主题。"
    )
    options = [f"{letters[index]}:{title}" for index, title in enumerate(titles[:26])]
    return filtered_items[:26], titles[:26], summary, options, duplicate_titles


def _localtok_choice_index(choice: str) -> int:
    match = re.match(r"\s*([A-Za-z])\s*[:：]", str(choice or ""))
    if not match:
        return -1
    return ord(match.group(1).upper()) - ord("A")


def _opennews_result_has_material_assets(result: dict, output_path: Path) -> bool:
    for segment in result.get("segments") or []:
        if not isinstance(segment, dict) or segment.get("type") != "material":
            continue
        for item in segment.get("material_items") or []:
            if not isinstance(item, dict):
                continue
            raw_path = str(item.get("path") or "").strip()
            if not raw_path:
                continue
            path = Path(raw_path)
            if not path.is_absolute():
                path = output_path / raw_path
            if path.exists() and path.stat().st_size > 0:
                return True
        for raw_path in segment.get("material_paths") or []:
            path = Path(str(raw_path))
            if not path.is_absolute():
                path = output_path / str(raw_path)
            if path.exists() and path.stat().st_size > 0:
                return True
    return False


def _compose_opennews_task_video(task_id: str, *, preferred_aspect_ratio: str = "vertical") -> dict:
    task = tasks.get(task_id) or {}
    output_dir = task.get("output_dir")
    result = task.get("result")
    if not output_dir or not result:
        raise RuntimeError("OpenNews 任务尚未生成可合成的中间结果。")
    output_path = Path(output_dir)
    if not output_path.exists():
        raise RuntimeError("OpenNews 输出目录不存在。")
    if not _opennews_result_has_material_assets(result, output_path):
        raise RuntimeError("OpenNews 成片中止：没有通过安全过滤的可用素材，已阻止生成白底占位视频。请更换新闻或等待下一轮素材匹配。")
    workflow_config = result.get("workflow_config") or {}
    transition_id = str(workflow_config.get("compose_transition_id") or "fade")
    subtitle_template_id = "property_clear"
    aspect_ratio = str(preferred_aspect_ratio or "vertical").strip().lower()
    if aspect_ratio not in {"vertical", "horizontal"}:
        aspect_ratio = "vertical"
    from video_composer import compose_history_video

    variant_results: dict[str, dict] = {}
    for variant_aspect in ("horizontal", "vertical"):
        variant_results[variant_aspect] = compose_history_video(
            str(output_path),
            result,
            transition_id=transition_id,
            subtitle_template_id=subtitle_template_id,
            aspect_ratio=variant_aspect,
            output_stem=f"final_video_{variant_aspect}",
        )
    compose_result = dict(variant_results.get(aspect_ratio) or variant_results["vertical"])
    compose_result["final_video_variants"] = variant_results
    workflow_config["compose_transition_id"] = transition_id
    workflow_config["subtitle_template_id"] = subtitle_template_id
    workflow_config["compose_aspect_ratio"] = aspect_ratio
    result["workflow_config"] = workflow_config
    result.update(compose_result)
    _record_history_cost(
        output_dir=output_path,
        result=result,
        user=None,
        event_type="compose_video",
        amount=_estimate_compose_cost(result.get("total_duration", 0)),
        provider=COST_RULES["compose_video"]["provider"],
        topic=result.get("topic", ""),
        meta={
            "transition_id": transition_id,
            "subtitle_template_id": subtitle_template_id,
            "aspect_ratio": aspect_ratio,
            "generated_aspect_ratios": ["horizontal", "vertical"],
            "scope": "localtok_publish",
        },
    )
    task["result"] = result
    _persist_task_result(task)
    _sync_live_task_result(str(output_path), result)
    return result


def _run_localtok_decided_production(local_proposal_id: str, *, user: dict, public_base_url: str) -> None:
    proposal = load_localtok_proposal(OPENNEWS_LOCALTOK_DIR, local_proposal_id)
    if not proposal:
        return
    try:
        decision = proposal.get("decision") or {}
        choice_index = _localtok_choice_index(decision.get("choice") or "")
        items = list(proposal.get("items") or [])
        if choice_index < 0 or choice_index >= len(items):
            raise RuntimeError(f"LocalTok 审核选择无法匹配候选项：{decision.get('choice') or ''}")
        selected = dict(items[choice_index])
        settings = dict(proposal.get("settings") or {})
        feedback = str(decision.get("feedback") or "").strip()
        notes = str(settings.get("notes") or "").strip()
        combined_notes = "\n".join([part for part in (notes, f"LocalTok 审核反馈：{feedback}" if feedback else "") if part])
        update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "status": "generating",
                "message": "LocalTok 已审核，正在生成新闻稿、配音和竖屏视频...",
                "selected_item": selected,
            }),
        )
        draft = generate_opennews_draft(
            article=selected,
            target_market=str(settings.get("target_market") or user.get("target_market") or "cn"),
            notes=combined_notes,
        )
        result = _create_opennews_material_task(
            user=user,
            public_base_url=public_base_url,
            article=selected,
            draft=draft,
            target_market=str(settings.get("target_market") or user.get("target_market") or "cn"),
            department_id=str(settings.get("department_id") or user.get("department_id") or "real_estate"),
            voice_preset_id=str(settings.get("voice_preset_id") or ""),
            aspect_ratio="vertical",
        )
        task_id = str(result.get("task_id") or "")
        update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "task_id": task_id,
                "message": f"视频生产任务已提交：{task_id}，等待中间产物完成...",
            }),
        )
        deadline = time.time() + 60 * 60
        while time.time() < deadline:
            task = tasks.get(task_id) or {}
            tracker = task.get("tracker")
            tracker_status = getattr(tracker, "status", "")
            if tracker_status == "done" and task.get("result") and task.get("output_dir"):
                break
            if tracker_status in {"error", "cancelled"}:
                messages = getattr(tracker, "messages", []) or []
                last_message = messages[-1].get("message") if messages else "任务失败"
                raise RuntimeError(str(last_message))
            time.sleep(10)
        else:
            raise RuntimeError("等待 OpenNews 视频任务完成超时。")

        composed_result = _compose_opennews_task_video(task_id, preferred_aspect_ratio="vertical")
        output_dir = Path(tasks[task_id].get("output_dir") or "")
        variants = composed_result.get("final_video_variants") or {}
        vertical_path = (variants.get("vertical") or {}).get("final_video_path") or composed_result.get("final_video_path")
        if not vertical_path:
            vertical_path = str(output_dir / "final_edit" / "final_video_vertical.mp4")
        video_path = Path(vertical_path)
        if not video_path.exists():
            raise RuntimeError("竖屏成片文件未找到，无法推送 LocalTok。")
        title = _opennews_article_title(selected)
        publish_name = f"opennews_{time.strftime('%Y%m%d_%H%M%S')}_{_make_safe_name(title, fallback='news')[:36]}"
        update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "history_id": output_dir.name,
                "message": "竖屏成片已生成，正在推送 LocalTok 展示系统...",
            }),
        )
        publish_result = publish_localtok_video(video_path=video_path, name=publish_name, title=title)
        update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "status": "published",
                "message": "已发布到 LocalTok 展示系统。",
                "publish_result": publish_result,
                "history_id": output_dir.name,
                "error": "",
            }),
        )
    except Exception as exc:
        update_localtok_proposal(
            OPENNEWS_LOCALTOK_DIR,
            local_proposal_id,
            lambda payload: payload.update({
                "status": "failed",
                "message": f"LocalTok 发布链路失败：{exc}",
                "error": str(exc),
            }),
        )


@app.post("/api/admin/opennews/produce")
async def admin_opennews_produce(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    article = payload.get("article") or {}
    draft = payload.get("draft") or {}
    target_market = str(payload.get("target_market") or "cn").strip() or "cn"
    if target_market not in {item["id"] for item in TARGET_MARKETS}:
        target_market = "cn"
    department_id = str(payload.get("department_id") or "real_estate").strip() or "real_estate"
    voice_preset_id = str(payload.get("voice_preset_id") or "mandarin_male").strip() or "mandarin_male"
    if voice_preset_id not in _get_visible_voice_preset_ids(target_market):
        voice_preset_id = _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female"
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    voice_preset["selected_speed"] = float(payload.get("speed") or voice_preset.get("default_speed") or 1.1)
    aspect_ratio = str(payload.get("aspect_ratio") or "horizontal").strip().lower()
    if aspect_ratio not in {"vertical", "horizontal"}:
        aspect_ratio = "horizontal"
    try:
        script_data = build_opennews_script_data(draft=draft, article=article, target_market=target_market)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    topic = f"OpenNews：{script_data.get('title') or article.get('title') or '新闻视频'}"
    submission_key = _make_produce_submission_key(
        owner_username=user.get("username", ""),
        topic=topic,
        script_data=script_data,
        voice_preset_id=voice_preset.get("id", voice_preset_id),
        avatar_id="",
        speed=float(voice_preset.get("selected_speed") or 1.1),
        web_search_enabled=False,
        target_market=target_market,
        department_id=department_id,
        script_model=SCRIPT_MODEL_CLAUDE,
        digital_human_engine="opennews_material_only",
        compose_aspect_ratio=aspect_ratio,
    )
    reusable_task = _find_reusable_running_task(owner_username=user.get("username", ""), submission_key=submission_key)
    if reusable_task:
        return {"task_id": reusable_task.get("id", ""), "reused_existing": True, "message": "OpenNews 新闻视频任务已在后台执行"}

    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": topic,
        "image_path": "",
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "submission_key": submission_key,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id", voice_preset_id),
            "avatar_id": "",
            "speed": voice_preset.get("selected_speed", 1.1),
            "web_search_enabled": False,
            "target_market": target_market,
            "department_id": department_id,
                "compose_transition_id": "fade",
                "subtitle_template_id": "property_clear",
                "compose_aspect_ratio": aspect_ratio,
                "source": {"kind": "opennews", "article": article},
            "script_model": SCRIPT_MODEL_CLAUDE,
            "digital_human_engine": "opennews_material_only",
            "opennews": True,
            "opennews_material_only": True,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("OpenNews 新闻视频任务已创建，准备进入素材成片链路...")
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "produce", {"article": article, "draft": draft, "script": script_data, "task_id": task_id, "user": user.get("username"), "aspect_ratio": aspect_ratio})
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, "", tasks[task_id]["public_base_url"], script_data, voice_preset, None),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id, "reused_existing": False, "script": script_data}


@app.post("/api/opennews/produce")
async def opennews_produce(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    article = payload.get("article") or {}
    draft = payload.get("draft") or {}
    target_market = str(payload.get("target_market") or user.get("target_market") or "cn").strip() or "cn"
    if target_market not in {item["id"] for item in TARGET_MARKETS}:
        target_market = "cn"
    department_id = str(payload.get("department_id") or user.get("department_id") or "real_estate").strip() or "real_estate"
    voice_preset_id = str(payload.get("voice_preset_id") or _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female").strip()
    if voice_preset_id not in _get_visible_voice_preset_ids(target_market):
        voice_preset_id = _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female"
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    voice_preset["selected_speed"] = float(payload.get("speed") or voice_preset.get("default_speed") or 1.1)
    aspect_ratio = str(payload.get("aspect_ratio") or "horizontal").strip().lower()
    if aspect_ratio not in {"vertical", "horizontal"}:
        aspect_ratio = "horizontal"
    try:
        script_data = build_opennews_script_data(draft=draft, article=article, target_market=target_market)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    topic = f"OpenNews：{script_data.get('title') or article.get('title') or '新闻视频'}"
    submission_key = _make_produce_submission_key(
        owner_username=user.get("username", ""),
        topic=topic,
        script_data=script_data,
        voice_preset_id=voice_preset.get("id", voice_preset_id),
        avatar_id="",
        speed=float(voice_preset.get("selected_speed") or 1.1),
        web_search_enabled=False,
        target_market=target_market,
        department_id=department_id,
        script_model=SCRIPT_MODEL_CLAUDE,
        digital_human_engine="opennews_material_only",
        compose_aspect_ratio=aspect_ratio,
    )
    reusable_task = _find_reusable_running_task(owner_username=user.get("username", ""), submission_key=submission_key)
    if reusable_task:
        return {"task_id": reusable_task.get("id", ""), "reused_existing": True, "message": "OpenNews 新闻视频任务已在后台执行"}

    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "topic": topic,
        "image_path": "",
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "submission_key": submission_key,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id", voice_preset_id),
            "avatar_id": "",
            "speed": voice_preset.get("selected_speed", 1.1),
            "web_search_enabled": False,
            "target_market": target_market,
            "department_id": department_id,
                "compose_transition_id": "fade",
                "subtitle_template_id": "property_clear",
                "compose_aspect_ratio": aspect_ratio,
                "source": {"kind": "opennews", "article": article},
            "script_model": SCRIPT_MODEL_CLAUDE,
            "digital_human_engine": "opennews_material_only",
            "opennews": True,
            "opennews_material_only": True,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("OpenNews 新闻视频任务已创建，准备进入素材成片链路...")
    save_opennews_payload(OPENNEWS_ADMIN_DIR, "produce", {"article": article, "draft": draft, "script": script_data, "task_id": task_id, "user": user.get("username"), "aspect_ratio": aspect_ratio})
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, "", tasks[task_id]["public_base_url"], script_data, voice_preset, None),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id, "reused_existing": False, "script": script_data}


def _floorplan_nav_job_payload(job: dict) -> dict:
    payload = dict(job or {})
    job_id = str(payload.get("job_id") or "")
    video = dict(payload.get("video") or {})
    if video.get("path"):
        video["url"] = f"/api/admin/floorplan-nav/jobs/{quote(job_id)}/file/video"
    floorplans = []
    for item in payload.get("floorplans") or []:
        floorplan = dict(item or {})
        floorplan["url"] = f"/api/admin/floorplan-nav/jobs/{quote(job_id)}/file/floorplan/{int(floorplan.get('index') or 0)}"
        floorplan.pop("path", None)
        floorplans.append(floorplan)
    payload["video"] = video
    payload["floorplans"] = floorplans
    analysis = dict(payload.get("analysis") or {})
    frames = []
    for item in analysis.get("frames") or []:
        frame = dict(item or {})
        frame["url"] = f"/api/admin/floorplan-nav/jobs/{quote(job_id)}/file/frame/{int(frame.get('index') or 0)}"
        frame.pop("path", None)
        frames.append(frame)
    analysis["frames"] = frames
    payload["analysis"] = analysis
    return payload


@app.post("/api/admin/floorplan-nav/jobs")
async def admin_floorplan_nav_create(
    request: Request,
    video: UploadFile = File(...),
    floorplans: list[UploadFile] = File(...),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    video_suffix = Path(video.filename or "").suffix.lower()
    if video_suffix not in FLOORPLAN_NAV_VIDEO_SUFFIXES:
        return JSONResponse({"error": "请上传 mp4、mov、m4v 或 webm 视频"}, status_code=400)
    if not floorplans:
        return JSONResponse({"error": "请至少上传一张户型图"}, status_code=400)
    for item in floorplans:
        if Path(item.filename or "").suffix.lower() not in FLOORPLAN_NAV_IMAGE_SUFFIXES:
            return JSONResponse({"error": "户型图仅支持 jpg、jpeg、png、webp"}, status_code=400)
    job = create_floorplan_nav_job(
        jobs_root=FLOORPLAN_NAV_JOBS_DIR,
        video_file=video,
        floorplan_files=floorplans,
        notes=notes,
        owner=user,
    )
    run_floorplan_nav_job_async(FLOORPLAN_NAV_JOBS_DIR, str(job["job_id"]))
    return _floorplan_nav_job_payload(job)


@app.get("/api/admin/floorplan-nav/jobs/{job_id}")
async def admin_floorplan_nav_status(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    job = load_floorplan_nav_job(FLOORPLAN_NAV_JOBS_DIR, job_id)
    if not job:
        return JSONResponse({"error": "户型图联动任务不存在"}, status_code=404)
    return _floorplan_nav_job_payload(job)


@app.post("/api/admin/floorplan-nav/jobs/{job_id}/points")
async def admin_floorplan_nav_save_points(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    job = load_floorplan_nav_job(FLOORPLAN_NAV_JOBS_DIR, job_id)
    if not job:
        return JSONResponse({"error": "户型图联动任务不存在"}, status_code=404)
    try:
        body = await request.json()
    except Exception:
        body = {}
    points = body.get("points")
    if not isinstance(points, list):
        return JSONResponse({"error": "点位数据格式错误"}, status_code=400)
    normalized = []
    for item in points:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "segment_index": int(item.get("segment_index") or 0),
            "floorplan_index": int(item.get("floorplan_index") or 0),
            "x": max(0.0, min(1.0, float(item.get("x") or 0))),
            "y": max(0.0, min(1.0, float(item.get("y") or 0))),
            "room": str(item.get("room") or ""),
        })
    job["points"] = normalized
    job["message"] = "户型图点位已保存"
    save_floorplan_nav_job(FLOORPLAN_NAV_JOBS_DIR, job)
    return _floorplan_nav_job_payload(job)


@app.get("/api/admin/floorplan-nav/jobs/{job_id}/file/{kind}/{index}")
async def admin_floorplan_nav_file_indexed(job_id: str, kind: str, index: int, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    job = load_floorplan_nav_job(FLOORPLAN_NAV_JOBS_DIR, job_id)
    if not job:
        return JSONResponse({"error": "户型图联动任务不存在"}, status_code=404)
    path = None
    if kind == "floorplan":
        items = job.get("floorplans") or []
        if 0 <= index < len(items):
            path = Path(str(items[index].get("path") or ""))
    elif kind == "frame":
        items = (job.get("analysis") or {}).get("frames") or []
        if 0 <= index < len(items):
            path = Path(str(items[index].get("path") or ""))
    if not path or not path.exists():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(path))


@app.get("/api/admin/floorplan-nav/jobs/{job_id}/file/video")
async def admin_floorplan_nav_video(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    job = load_floorplan_nav_job(FLOORPLAN_NAV_JOBS_DIR, job_id)
    if not job:
        return JSONResponse({"error": "户型图联动任务不存在"}, status_code=404)
    path = Path(str((job.get("video") or {}).get("path") or ""))
    if not path.exists():
        return JSONResponse({"error": "视频不存在"}, status_code=404)
    return FileResponse(str(path), media_type="video/mp4")


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
        "property_bgm_tracks": _property_bgm_track_payloads(),
        "current_user": user,
        "current_task": _build_current_task_payload(user),
        "active_tasks": _build_active_tasks_payload(user),
    }


@app.get("/api/material-library")
async def material_library_items(
    request: Request,
    q: str = "",
    kind: str = "",
    category: str = "",
    uploader: str = "",
):
    user, error = _require_user(request)
    if error:
        return error
    approved_rows = [
        item
        for item in list_material_library_items(status="approved")
        if material_item_matches_filters(item, q=q, kind=kind, category=category, uploader=uploader)
    ]
    approved = [_material_library_item_payload(item, user) for item in approved_rows]
    pending = []
    if _is_admin(user):
        pending_rows = [
            item
            for item in list_material_library_items(status="pending")
            if material_item_matches_filters(item, q=q, kind=kind, category=category, uploader=uploader)
        ]
        pending = [_material_library_item_payload(item, user) for item in pending_rows]
    uploaders = []
    uploader_seen = set()
    for item in list_material_library_items():
        uploader_name = str(item.get("uploader_display_name") or item.get("uploader_username") or "").strip()
        if not uploader_name:
            continue
        lowered = uploader_name.lower()
        if lowered in uploader_seen:
            continue
        uploader_seen.add(lowered)
        uploaders.append(uploader_name)
    uploaders.sort()
    return {
        "items": approved,
        "pending_items": pending,
        "categories": MATERIAL_CATEGORIES,
        "uploaders": uploaders,
    }


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
        if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v", ".webm", ".mp3", ".wav", ".m4a", ".aac", ".ogg"}:
            return JSONResponse({"error": "仅支持上传图片、视频或音频素材"}, status_code=400)
        temp_path = upload_dir / f"{uuid.uuid4().hex[:12]}_{index}{suffix}"
        with temp_path.open("wb") as f:
            shutil.copyfileobj(upload.file, f)
        try:
            item = register_material_file(
                temp_path=str(temp_path),
                original_filename=original_name,
                title=Path(original_name).stem,
                category="背景音乐" if suffix in AUDIO_SUFFIXES else "",
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
    category: str = Form(""),
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
                "category": category.strip(),
                "notes": notes.strip(),
                "reviewed_at": time.time(),
                "reviewed_by_username": user.get("username", ""),
                "reviewed_by_display_name": user.get("display_name", ""),
            },
        )
    except FileNotFoundError:
        return JSONResponse({"error": "素材不存在"}, status_code=404)
    return {"ok": True, "item": _material_library_item_payload(updated, user)}


@app.post("/api/material-library/review-batch")
async def review_material_library_items_batch(
    request: Request,
    action: str = Form(...),
    item_ids: str = Form(""),
    category: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以批量处理素材")
    normalized_action = str(action or "").strip().lower()
    selected_ids = [value.strip() for value in str(item_ids or "").split(",") if value.strip()]
    if not selected_ids:
        return JSONResponse({"error": "请先选择素材"}, status_code=400)
    if normalized_action == "delete":
        deleted = batch_delete_material_library_items(selected_ids)
        return {"ok": True, "deleted_count": len(deleted), "items": [_material_library_item_payload(item, user) for item in deleted]}
    if normalized_action not in {"approved", "rejected"}:
        return JSONResponse({"error": "批量操作非法"}, status_code=400)
    updates = {
        "status": normalized_action,
        "reviewed_at": time.time(),
        "reviewed_by_username": user.get("username", ""),
        "reviewed_by_display_name": user.get("display_name", ""),
    }
    if category.strip():
        updates["category"] = category.strip()
    updated = batch_update_material_library_items(selected_ids, updates)
    return {"ok": True, "updated_count": len(updated), "items": [_material_library_item_payload(item, user) for item in updated]}


@app.get("/api/material-library/harvest")
async def material_harvest_payload(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return {"jobs": [], "candidates": []}
    jobs = [_harvest_job_payload(item) for item in list_harvest_jobs()]
    candidates = [_harvest_candidate_payload(item, user) for item in list_harvest_candidates()]
    return {
        "jobs": jobs,
        "candidates": candidates,
        "presets": [
            {"category": category, "topic": data.get("topic", ""), "notes": data.get("notes", ""), "tags": data.get("tags", [])}
            for category, data in NEWS_HARVEST_PRESETS.items()
        ],
    }


@app.post("/api/material-library/harvest/jobs")
async def create_material_harvest_job(
    request: Request,
    topic: str = Form(""),
    category: str = Form(""),
    source_text: str = Form(""),
    search_notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发起采集任务")
    job = create_harvest_job(
        topic=topic,
        category=category,
        source_text=source_text,
        search_notes=search_notes,
        created_by_username=user.get("username", ""),
        created_by_display_name=user.get("display_name", ""),
    )
    run_harvest_job_async(job["id"])
    return {"ok": True, "job": _harvest_job_payload(job)}


@app.post("/api/material-library/harvest/candidates/{candidate_id}/import")
async def import_material_harvest_candidate(
    candidate_id: str,
    request: Request,
    category: str = Form(""),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以导入候选素材")
    try:
        item = import_harvest_candidate_to_material_library(
            candidate_id,
            uploader_username=user.get("username", ""),
            uploader_display_name=user.get("display_name", ""),
            category=category.strip(),
            notes=notes.strip(),
        )
        updated = update_material_library_item(
            str(item.get("id")),
            {
                "status": "approved",
                "reviewed_at": time.time(),
                "reviewed_by_username": user.get("username", ""),
                "reviewed_by_display_name": user.get("display_name", ""),
            },
        )
    except FileNotFoundError:
        return JSONResponse({"error": "候选素材不存在"}, status_code=404)
    except Exception as exc:
        return JSONResponse({"error": f"导入失败：{exc}"}, status_code=400)
    return {"ok": True, "item": _material_library_item_payload(updated, user)}


@app.post("/api/material-library/harvest/candidates/{candidate_id}/reject")
async def reject_material_harvest_candidate(candidate_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以拒绝候选素材")
    try:
        candidate = update_harvest_candidate(candidate_id, {"status": "rejected"})
    except FileNotFoundError:
        return JSONResponse({"error": "候选素材不存在"}, status_code=404)
    return {"ok": True, "candidate": _harvest_candidate_payload(candidate, user)}


@app.delete("/api/material-library/harvest/candidates")
async def clear_material_harvest_candidates(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以清空候选素材")
    removed_count = clear_harvest_candidates(keep_imported=True)
    return {"ok": True, "removed_count": removed_count}


@app.delete("/api/material-library/harvest/candidates/{candidate_id}")
async def delete_material_harvest_candidate(candidate_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以删除候选素材")
    try:
        candidate = delete_harvest_candidate(candidate_id)
    except FileNotFoundError:
        return JSONResponse({"error": "候选素材不存在"}, status_code=404)
    return {"ok": True, "candidate": _harvest_candidate_payload(candidate, user)}


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
    script_model: str = Form(SCRIPT_MODEL_API_RELAY),
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
    script_model: str = Form(SCRIPT_MODEL_API_RELAY),
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


@app.post("/api/property-video/jobs")
async def start_property_video_job(
    request: Request,
    videos: list[UploadFile] = File(...),
    script_text: str = Form(...),
    voice_preset_id: str = Form(...),
    speed: float = Form(1.1),
    target_market: str = Form("cn"),
    bgm_item_id: str = Form(""),
    bgm_volume: float = Form(0.10),
    timeline_segments: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error

    script_text = (script_text or "").strip()
    if not script_text:
        return JSONResponse({"error": "请先填写房源解说文案"}, status_code=400)
    if not videos:
        return JSONResponse({"error": "请至少上传一个房源视频"}, status_code=400)

    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    if voice_preset.get("enabled") is False:
        return JSONResponse({"error": "当前音色还未配置，暂时不可用"}, status_code=400)
    bgm_item_id = str(bgm_item_id or "").strip()
    if bgm_item_id and not _get_approved_bgm_path(bgm_item_id):
        return JSONResponse({"error": "选择的背景音乐不存在或还未审核通过"}, status_code=400)
    bgm_volume = max(0.0, min(float(bgm_volume or 0.10), 0.30))
    parsed_timeline_segments: list[dict] = []
    if str(timeline_segments or "").strip():
        try:
            parsed = json.loads(timeline_segments)
            if isinstance(parsed, list):
                parsed_timeline_segments = [item for item in parsed if isinstance(item, dict)]
        except Exception:
            return JSONResponse({"error": "一镜到底分段文案格式错误，请重新分析视频后再试"}, status_code=400)

    task_id = str(uuid.uuid4())[:8]
    output_dir = Path(_create_output_dir("property_video", "房源实拍成片"))
    incoming_dir = output_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []
    try:
        for index, upload in enumerate(videos, start=1):
            original_name = Path(upload.filename or f"clip_{index:02d}.mp4").name
            suffix = Path(original_name).suffix.lower()
            if suffix not in PROPERTY_VIDEO_EXTENSIONS:
                return JSONResponse({"error": f"只支持上传视频文件：{', '.join(sorted(PROPERTY_VIDEO_EXTENSIONS))}"}, status_code=400)
            destination = incoming_dir / f"{index:02d}_{uuid.uuid4().hex[:8]}{suffix}"
            with destination.open("wb") as out:
                shutil.copyfileobj(upload.file, out)
            saved_paths.append(str(destination))
    except Exception as exc:
        return JSONResponse({"error": f"视频上传保存失败：{exc}"}, status_code=500)

    voice_preset["selected_speed"] = speed
    tracker = ProgressTracker(task_id)
    tracker.total_steps = 4
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "mode": "property_video",
        "topic": "房源实拍成片",
        "image_path": "",
        "tracker": tracker,
        "output_dir": str(output_dir),
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id"),
            "speed": speed,
            "target_market": target_market,
            "voice_preset": voice_preset,
            "bgm_item_id": bgm_item_id,
            "bgm_volume": bgm_volume,
            "property_video_mode": "one_take_timeline" if parsed_timeline_segments else "real_shot_voiceover",
            "timeline_segments": parsed_timeline_segments,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("房源实拍成片任务已创建，准备开始...")
    _push_live_event("task_created", "创建了房源实拍成片任务", tasks[task_id])
    thread = threading.Thread(
        target=run_property_video_with_progress,
        args=(task_id, saved_paths, script_text, voice_preset, target_market, speed, bgm_item_id, bgm_volume, parsed_timeline_segments),
        daemon=True,
    )
    thread.start()
    return {"task_id": task_id}


@app.post("/api/property-video/analyze")
async def analyze_property_video(
    request: Request,
    videos: list[UploadFile] = File(...),
    target_market: str = Form("cn"),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not videos:
        return JSONResponse({"error": "请至少上传一个房源视频"}, status_code=400)

    analysis_id = str(uuid.uuid4())[:8]
    output_dir = Path(_create_output_dir("property_analysis", "房源视觉分析"))
    incoming_dir = output_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[Path] = []
    try:
        for index, upload in enumerate(videos, start=1):
            original_name = Path(upload.filename or f"clip_{index:02d}.mp4").name
            suffix = Path(original_name).suffix.lower()
            if suffix not in PROPERTY_VIDEO_EXTENSIONS:
                return JSONResponse({"error": f"只支持上传视频文件：{', '.join(sorted(PROPERTY_VIDEO_EXTENSIONS))}"}, status_code=400)
            destination = incoming_dir / f"{index:02d}_{uuid.uuid4().hex[:8]}{suffix}"
            with destination.open("wb") as out:
                shutil.copyfileobj(upload.file, out)
            saved_paths.append(destination)
    except Exception as exc:
        return JSONResponse({"error": f"视频上传保存失败：{exc}"}, status_code=500)

    try:
        analysis = analyze_property_video_with_openai(
            video_paths=saved_paths,
            work_dir=output_dir / "analysis",
            target_market=target_market,
            user_notes=notes,
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    result = {
        "analysis_id": analysis_id,
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "target_market": target_market,
        "notes": notes,
        "created_at": time.time(),
        "analysis": analysis,
    }
    (output_dir / "analysis_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {
        "analysis_id": analysis_id,
        "overall_summary": analysis.get("overall_summary", ""),
        "suggested_script": analysis.get("suggested_script", ""),
        "total_video_duration": analysis.get("total_video_duration", 0),
        "target_script_chars": analysis.get("target_script_chars", ""),
        "estimated_narration_seconds": analysis.get("estimated_narration_seconds", 0),
        "clip_durations": analysis.get("clip_durations", []),
        "timeline_segments": analysis.get("timeline_segments", []),
        "clips": analysis.get("clips", []),
        "warnings": analysis.get("warnings", []),
        "model": analysis.get("model", ""),
        "usage": analysis.get("usage", {}),
    }


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
                    last_message = tracker.messages[-1]["message"] if tracker.messages else ""
                    result_data = {
                        "mode": tasks[task_id].get("mode", "full"),
                        "output_dir": output_dir,
                        "history_id": Path(output_dir).name if output_dir else "",
                        "can_retry": bool(lifecycle.get("can_resume_production")),
                        "error": last_message.replace("出错了：", "", 1),
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
            "compose_aspect_ratio": workflow_config.get("compose_aspect_ratio") or workflow_config.get("aspect_ratio") or "vertical",
            "source": workflow_config.get("source") or {},
            "opennews": bool(workflow_config.get("opennews")),
            "opennews_material_only": bool(workflow_config.get("opennews_material_only")),
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
    script_model: str = Form(SCRIPT_MODEL_API_RELAY),
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
        audio_path_str, tts_provider = _generate_audio_for_workflow(
            script_text=script_text,
            audio_path=str(audio_path),
            voice=tts_voice,
            speed=tts_speed,
            volume=tts_volume,
            language=tts_language,
            workflow_config=workflow_config,
            generate_audio_fn=generate_audio,
        )
    except Exception as exc:
        return JSONResponse({"error": f"重新生成配音失败：{exc}"}, status_code=500)

    segment["audio_path"] = audio_path_str
    segment["tts_provider"] = tts_provider
    try:
        segment["audio_url"] = upload_file_and_get_url(audio_path_str, key_prefix="full/audio")
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
        amount=_estimate_tts_cost(script_text, audio_path_str),
        provider=tts_provider,
        topic=result.get("topic", ""),
        meta={"segment_index": segment_index, "audio_path": audio_path_str, "scope": "regenerate_audio"},
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

    workflow_config = result.get("workflow_config") or {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    transition_id = str(workflow_config.get("compose_transition_id") or "fade")
    subtitle_template_id = str(workflow_config.get("subtitle_template_id") or "classic")
    requested_aspect_ratio = str((payload or {}).get("aspect_ratio") or "").strip().lower()
    if requested_aspect_ratio not in {"vertical", "horizontal"}:
        requested_aspect_ratio = ""
    is_opennews_result = bool(
        workflow_config.get("opennews")
        or workflow_config.get("opennews_material_only")
        or str(workflow_config.get("digital_human_engine") or "") == "opennews_material_only"
        or str(result.get("topic") or "").startswith("OpenNews")
    )
    default_aspect_ratio = "horizontal" if is_opennews_result else "vertical"
    compose_aspect_ratio = str(
        requested_aspect_ratio
        or workflow_config.get("compose_aspect_ratio")
        or workflow_config.get("aspect_ratio")
        or default_aspect_ratio
    ).strip().lower()
    if compose_aspect_ratio not in {"vertical", "horizontal"}:
        compose_aspect_ratio = default_aspect_ratio
    if is_opennews_result:
        subtitle_template_id = "property_clear"

    try:
        from video_composer import compose_history_video
        if is_opennews_result:
            variant_results: dict[str, dict] = {}
            for variant_aspect in ("horizontal", "vertical"):
                variant_results[variant_aspect] = compose_history_video(
                    str(output_dir),
                    result,
                    transition_id=transition_id,
                    subtitle_template_id=subtitle_template_id,
                    aspect_ratio=variant_aspect,
                    output_stem=f"final_video_{variant_aspect}",
                )
            compose_result = dict(variant_results.get(compose_aspect_ratio) or variant_results["horizontal"])
            compose_result["final_video_variants"] = variant_results
        else:
            compose_result = compose_history_video(
                str(output_dir),
                result,
                transition_id=transition_id,
                subtitle_template_id=subtitle_template_id,
                aspect_ratio=compose_aspect_ratio,
            )
    except Exception as exc:
        return JSONResponse({"error": f"自动成片失败：{exc}"}, status_code=500)

    workflow_config["compose_transition_id"] = transition_id
    workflow_config["subtitle_template_id"] = subtitle_template_id
    workflow_config["compose_aspect_ratio"] = compose_aspect_ratio
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
        meta={
            "transition_id": transition_id,
            "subtitle_template_id": subtitle_template_id,
            "aspect_ratio": compose_aspect_ratio,
            "generated_aspect_ratios": ["horizontal", "vertical"] if is_opennews_result else [compose_aspect_ratio],
        },
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
