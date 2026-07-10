"""
iHouse 视频自动化生产系统 - Web 应用
FastAPI + SSE 实时进度推送
"""

import csv
import base64
import asyncio
import copy
from collections import deque
import hashlib
import hmac
import io
import json
import os
import re
import sqlite3
import subprocess
import threading
import requests
import shutil
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Optional
from urllib.parse import quote, urlparse

from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.sessions import SessionMiddleware

load_dotenv(override=False)
for _key, _value in dotenv_values().items():
    if _key in {
        "FORCE_SCRIPT_MODEL_PROVIDER",
        "SCRIPT_MODEL_FORCE_PROVIDER",
        "FORCE_SCRIPT_MODEL_LABEL",
        "FORCE_SCRIPT_MODEL_DESCRIPTION",
        "OPENAI_RELAY_API_KEY",
        "SUB2API_API_KEY",
        "API_RELAY_OPENAI_API_KEY",
        "OPENAI_RELAY_BASE_URL",
        "OPENAI_RELAY_MODEL",
        "OPENAI_RELAY_REASONING_EFFORT",
        "OPENAI_RELAY_MERGE_INSTRUCTIONS_INTO_INPUT",
        "OPENNEWS_MODEL_PROVIDER",
        "OPENNEWS_TEXT_MODEL_PROVIDER",
        "OPENNEWS_RELAY_MODEL",
        "OPENAI_RELAY_OPENNEWS_MODEL",
        "OPENNEWS_RELAY_REASONING_EFFORT",
        "OPENAI_RELAY_OPENNEWS_REASONING_EFFORT",
    } and _value is not None:
        os.environ[_key] = _value

from material_library import (
    MATERIAL_LIBRARY_DIR,
    AUDIO_SUFFIXES,
    delete_material_library_item,
    list_material_library_items,
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
    _local_opennews_language_fallback,
    save_opennews_payload,
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
    _candidate_event_key as opennews_candidate_event_key,
    _candidate_event_tokens as opennews_candidate_event_tokens,
    _candidate_title_compact as opennews_candidate_title_compact,
    _candidate_title_similar as opennews_candidate_title_similar,
    _is_duplicate_event as opennews_is_duplicate_event,
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
    update_batch_job as update_opennews_batch_job,
)
from opennews_collections import (
    audit_result_image_duplicates,
    image_material_fingerprint,
    load_collection_job,
    update_collection_job,
)
from source_ingest import analyze_topic_fields
from facebook_publisher import (
    FACEBOOK_SCOPE,
    FacebookPublishError,
    build_facebook_authorization_url,
    exchange_facebook_code_for_tokens,
    exchange_facebook_long_lived_user_token,
    facebook_env_config,
    get_facebook_pages,
    get_facebook_video_metrics,
    get_facebook_page,
    load_facebook_token_store,
    save_facebook_authorization,
    upload_video_to_facebook_page,
)
from youtube_publisher import (
    YOUTUBE_SCOPE,
    YouTubePublishError,
    exchange_youtube_code_for_tokens,
    find_recent_youtube_upload,
    get_youtube_channel,
    get_youtube_video_metrics,
    save_youtube_oauth_app_config,
    save_youtube_refresh_token,
    set_youtube_thumbnail,
    upload_video_to_youtube,
    youtube_env_config,
    youtube_oauth_app_source,
)
from x_publisher import (
    X_SCOPE,
    XPublishError,
    build_x_authorization_url,
    exchange_x_code_for_tokens,
    generate_x_pkce_pair,
    get_x_post_metrics,
    get_x_user,
    load_x_token_store,
    save_x_tokens,
    upload_video_to_x,
    x_env_config,
)
from x_browser_publisher import (
    XBrowserPublishError,
    x_browser_auth_ready,
    x_browser_profile_dir,
    publish_video_to_x_browser,
    x_browser_env_config,
)
from x_browser_login_manager import (
    start_x_browser_login,
    stop_x_browser_login,
    x_browser_login_env_config,
    x_browser_login_status,
)
import topic_auto
import property_auto

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
AUTO_DIGITAL_BATCH_JOBS: dict[str, dict[str, Any]] = {}
AUTO_DIGITAL_BATCH_LOCK = threading.Lock()
OPENNEWS_DRAFT_JOBS: dict[str, dict[str, Any]] = {}
OPENNEWS_DRAFT_LOCK = threading.Lock()
YOUTUBE_UPLOAD_JOBS: dict[str, dict[str, Any]] = {}
YOUTUBE_UPLOAD_LOCK = threading.Lock()
X_UPLOAD_JOBS: dict[str, dict[str, Any]] = {}
X_UPLOAD_LOCK = threading.Lock()
FACEBOOK_UPLOAD_JOBS: dict[str, dict[str, Any]] = {}
FACEBOOK_UPLOAD_LOCK = threading.Lock()
FACEBOOK_PUBLISH_STATE_LOCK = threading.RLock()
FACEBOOK_PUBLISH_SERIAL_LOCK = threading.Lock()
OPENNEWS_BATCH_AUTO_PRODUCE_LOCK = threading.Lock()
OPENNEWS_CHANNEL_CONFIG_LOCK = threading.Lock()
RESULT_FILE_WRITE_LOCK = threading.RLock()
OPENNEWS_YOUTUBE_PUBLISH_LEDGER_LOCK = threading.RLock()
ASSETS_DIR = BASE_DIR / "assets"
ASSETS_DIR.mkdir(exist_ok=True)
AVATAR_LIBRARY_MANIFEST_PATH = ASSETS_DIR / "avatar_library_manifest.json"
AVATAR_LIBRARY_LOCK = threading.Lock()
MATERIAL_LIBRARY_PUBLIC_DIR = MATERIAL_LIBRARY_DIR

AVATAR_DISPLAY_NAME_MAP = {
    "avatar_test_0cd3d70a.png": "女主播A",
    "avatar_host_c.png": "男主播A",
    "avatar_host_d.png": "女主播C",
    "avatar_ultraman.png": "奥特曼",
    "avatar_test_new_01.png": "男主播B",
    "avatar_test_aec9a0f0.png": "女主播B",
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
            "tw": "mandarin_male",
            "jp": "mandarin_male",
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
AUTO_DIGITAL_BATCH_DIR = OUTPUT_DIR / "auto_digital_batches"
TOPIC_AUTO_DIR = OUTPUT_DIR / "topic_auto"
PROPERTY_AUTO_DIR = OUTPUT_DIR / "property_auto"
FLOORPLAN_NAV_JOBS_DIR = OUTPUT_DIR / "admin_floorplan_nav_jobs"
OPENNEWS_ADMIN_DIR = OUTPUT_DIR / "admin_opennews"
OPENNEWS_AUTO_DIR = OUTPUT_DIR / "opennews_auto"
OPENNEWS_BATCH_DIR = OUTPUT_DIR / "opennews_batches"
OPENNEWS_CHANNELS_CONFIG_PATH = OPENNEWS_BATCH_DIR / "channels_config.json"
OPENNEWS_COLLECTION_DIR = OUTPUT_DIR / "opennews_collections"
YOUTUBE_AUTH_DIR = OUTPUT_DIR / "youtube_auth"
YOUTUBE_TOKEN_STORE_PATH = YOUTUBE_AUTH_DIR / "youtube_token.json"
YOUTUBE_THUMBNAIL_RETRY_DIR = OUTPUT_DIR / "youtube_thumbnail_retries"
OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR = OUTPUT_DIR / "opennews_youtube_publish_ledger"
YOUTUBE_THUMBNAIL_COOLDOWN_PATH = YOUTUBE_AUTH_DIR / "thumbnail_cooldown.json"
X_AUTH_DIR = OUTPUT_DIR / "x_auth"
X_TOKEN_STORE_PATH = X_AUTH_DIR / "x_token.json"
FACEBOOK_AUTH_DIR = OUTPUT_DIR / "facebook_auth"
FACEBOOK_TOKEN_STORE_PATH = FACEBOOK_AUTH_DIR / "facebook_token.json"
FACEBOOK_PUBLISH_STATE_PATH = FACEBOOK_AUTH_DIR / "publish_state.json"
AUTO_DIGITAL_BATCH_DIR.mkdir(parents=True, exist_ok=True)
FLOORPLAN_NAV_JOBS_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_AUTO_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_BATCH_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_COLLECTION_DIR.mkdir(parents=True, exist_ok=True)
YOUTUBE_AUTH_DIR.mkdir(parents=True, exist_ok=True)
YOUTUBE_THUMBNAIL_RETRY_DIR.mkdir(parents=True, exist_ok=True)
OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR.mkdir(parents=True, exist_ok=True)
X_AUTH_DIR.mkdir(parents=True, exist_ok=True)
FACEBOOK_AUTH_DIR.mkdir(parents=True, exist_ok=True)
COMPOSE_READY_RECOVERY_STARTED = False

MATERIAL_VECTOR_SERVICE_URL = os.getenv("OPENNEWS_MATERIAL_VECTOR_URL", "http://192.168.0.34:8897").strip().rstrip("/")
MATERIAL_VECTOR_SYNC_ENABLED = (
    os.getenv("MATERIAL_VECTOR_SYNC_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_STALE_JOB_TIMEOUT_HOURS = max(1, int(os.getenv("OPENNEWS_STALE_JOB_TIMEOUT_HOURS", "3") or "3"))
COMPOSE_READY_RECOVERY_POLL_SECONDS = max(
    20,
    int(os.getenv("COMPOSE_READY_RECOVERY_POLL_SECONDS", "60") or "60"),
)
COMPOSE_READY_RECOVERY_BATCH_SIZE = max(
    1,
    int(os.getenv("COMPOSE_READY_RECOVERY_BATCH_SIZE", "2") or "2"),
)
YOUTUBE_THUMBNAIL_RATE_LIMIT_COOLDOWN_SECONDS = max(
    600,
    int(os.getenv("YOUTUBE_THUMBNAIL_RATE_LIMIT_COOLDOWN_SECONDS", str(6 * 60 * 60)) or str(6 * 60 * 60)),
)
YOUTUBE_THUMBNAIL_RETRY_INTERVAL_SECONDS = max(
    300,
    int(os.getenv("YOUTUBE_THUMBNAIL_RETRY_INTERVAL_SECONDS", str(30 * 60)) or str(30 * 60)),
)
YOUTUBE_THUMBNAIL_RETRY_MAX_ATTEMPTS = max(
    1,
    int(os.getenv("YOUTUBE_THUMBNAIL_RETRY_MAX_ATTEMPTS", "12") or "12"),
)
YOUTUBE_THUMBNAIL_RETRY_WORKER_STARTED = False
YOUTUBE_THUMBNAIL_RETRY_LOCK = threading.Lock()


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default) or default).strip().lower() not in {"0", "false", "no", "off", ""}


def _opennews_x_auto_publish_default() -> bool:
    return _env_flag("OPENNEWS_X_AUTO_PUBLISH_ENABLED", "1")


def _opennews_x_auto_publish_disabled() -> bool:
    return _env_flag("OPENNEWS_X_AUTO_PUBLISH_DISABLED", "0")


def _opennews_x_single_shorts_enabled() -> bool:
    return _env_flag("OPENNEWS_X_PUBLISH_SINGLE_SHORTS_ENABLED", "1")


def _opennews_x_collection_enabled() -> bool:
    return _env_flag("OPENNEWS_X_PUBLISH_COLLECTION_ENABLED", "0")


def _opennews_facebook_auto_publish_default() -> bool:
    return _env_flag("OPENNEWS_FACEBOOK_AUTO_PUBLISH_ENABLED", "1")


def _opennews_facebook_auto_publish_disabled() -> bool:
    return _env_flag("OPENNEWS_FACEBOOK_AUTO_PUBLISH_DISABLED", "0")


def _opennews_facebook_single_shorts_enabled() -> bool:
    return _env_flag("OPENNEWS_FACEBOOK_PUBLISH_SINGLE_SHORTS_ENABLED", "1")


def _opennews_facebook_collection_enabled() -> bool:
    return _env_flag("OPENNEWS_FACEBOOK_PUBLISH_COLLECTION_ENABLED", "0")


def _opennews_youtube_auto_publish_default() -> bool:
    return _env_flag("OPENNEWS_YOUTUBE_AUTO_PUBLISH_ENABLED", "1")

def _opennews_youtube_auto_publish_disabled() -> bool:
    return _env_flag("OPENNEWS_YOUTUBE_AUTO_PUBLISH_DISABLED", "0")

def _opennews_youtube_publish_language_versions_enabled() -> bool:
    return _env_flag("OPENNEWS_YOUTUBE_PUBLISH_LANGUAGE_VERSIONS_ENABLED", "0")


def _opennews_x_publish_language_versions_enabled() -> bool:
    return _env_flag("OPENNEWS_X_PUBLISH_LANGUAGE_VERSIONS_ENABLED", "1")


def _opennews_x_publish_mode() -> str:
    mode = (os.getenv("OPENNEWS_X_PUBLISH_MODE", "browser") or "browser").strip().lower()
    if mode not in {"browser", "api"}:
        mode = "browser"
    return mode


def _opennews_x_publish_mode_label() -> str:
    return "浏览器自动化" if _opennews_x_publish_mode() == "browser" else "API"


def _opennews_facebook_publish_language_versions_enabled() -> bool:
    # 默认关闭:FB 只发主语言(频道首个语言,通常中文)一帖,避免中/日/英各发一帖触发频率封锁。
    return _env_flag("OPENNEWS_FACEBOOK_PUBLISH_LANGUAGE_VERSIONS_ENABLED", "0")


# ===== Facebook 发帖节流 + 368 频率封锁自动冷却 =====
def _facebook_min_post_interval_seconds() -> int:
    try:
        return max(0, int(os.getenv("OPENNEWS_FACEBOOK_MIN_POST_INTERVAL_SECONDS", "1800") or "1800"))
    except Exception:
        return 1800


def _facebook_frequency_cooldown_seconds(block_count: int = 1) -> int:
    try:
        base = max(3600, int(os.getenv("OPENNEWS_FACEBOOK_FREQUENCY_COOLDOWN_SECONDS", "86400") or "86400"))
        maximum = max(base, int(os.getenv("OPENNEWS_FACEBOOK_FREQUENCY_MAX_COOLDOWN_SECONDS", "259200") or "259200"))
        return min(maximum, base * (2 ** max(0, int(block_count or 1) - 1)))
    except Exception:
        return 86400


def _load_facebook_publish_state() -> dict:
    with FACEBOOK_PUBLISH_STATE_LOCK:
        try:
            if FACEBOOK_PUBLISH_STATE_PATH.exists():
                data = json.loads(FACEBOOK_PUBLISH_STATE_PATH.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def _save_facebook_publish_state(state: dict) -> None:
    with FACEBOOK_PUBLISH_STATE_LOCK:
        try:
            FACEBOOK_PUBLISH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = FACEBOOK_PUBLISH_STATE_PATH.with_name(
                f".{FACEBOOK_PUBLISH_STATE_PATH.name}.{threading.get_ident()}.tmp"
            )
            tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(FACEBOOK_PUBLISH_STATE_PATH)
        except Exception as exc:
            print(f"[facebook throttle] 写入发帖状态失败: {exc}", flush=True)


def _is_facebook_frequency_block(error_text: str) -> bool:
    text = str(error_text or "")
    lowered = text.lower()
    return (
        '"code":368' in lowered
        or "code': 368" in lowered
        or "1390008" in text
        or "免受垃圾信息打扰" in text
        or "限制了你" in text
        or ("368" in text and "oauth" in lowered)
    )


def _facebook_page_state_bucket(state: dict, page_id: str) -> dict:
    """按 Page 分桶节流/冷却:不同 Page(OpenNews / iHouse株式会社)互不影响。
    兼容旧的顶层扁平字段(迁移到 default 桶)。"""
    key = str(page_id or "").strip() or "default"
    pages = state.get("pages")
    if not isinstance(pages, dict):
        pages = {}
        # 迁移旧的扁平字段
        legacy = {}
        if state.get("last_post_at"):
            legacy["last_post_at"] = state.get("last_post_at")
        if state.get("cooldown_until"):
            legacy["cooldown_until"] = state.get("cooldown_until")
            legacy["cooldown_reason"] = state.get("cooldown_reason")
        if legacy:
            pages["default"] = legacy
        state["pages"] = pages
    bucket = pages.get(key)
    if not isinstance(bucket, dict):
        bucket = {}
        pages[key] = bucket
    return bucket


def _facebook_publish_cooldown_remaining(page_id: str = "") -> float:
    with FACEBOOK_PUBLISH_STATE_LOCK:
        state = _load_facebook_publish_state()
        bucket = _facebook_page_state_bucket(state, page_id)
        until = float(bucket.get("cooldown_until") or 0)
        cooldown_set_at = float(bucket.get("cooldown_set_at") or 0)
        cooldown_reason = str(bucket.get("cooldown_reason") or "")
        # 旧版本只冷却 2 小时。首次读取时把仍有效的 368 状态迁移到新的安全窗口，
        # 避免部署后立刻再次撞 Meta 限制。
        if (
            cooldown_set_at
            and not bucket.get("frequency_block_count")
            and _is_facebook_frequency_block(cooldown_reason)
        ):
            migrated_until = max(until, cooldown_set_at + _facebook_frequency_cooldown_seconds(1))
            if migrated_until > until:
                until = migrated_until
                bucket["cooldown_until"] = until
                bucket["frequency_block_count"] = 1
                bucket["cooldown_migrated_at"] = time.time()
                _save_facebook_publish_state(state)
        return max(0.0, until - time.time())


def _trigger_facebook_publish_cooldown(reason: str = "", page_id: str = "") -> float:
    with FACEBOOK_PUBLISH_STATE_LOCK:
        state = _load_facebook_publish_state()
        bucket = _facebook_page_state_bucket(state, page_id)
        block_count = max(0, int(bucket.get("frequency_block_count") or 0)) + 1
        cooldown = _facebook_frequency_cooldown_seconds(block_count)
        until = time.time() + cooldown
        bucket["frequency_block_count"] = block_count
        bucket["cooldown_until"] = until
        bucket["cooldown_reason"] = str(reason or "")[:300]
        bucket["cooldown_set_at"] = time.time()
        _save_facebook_publish_state(state)
    print(f"[facebook throttle] Page={page_id or 'default'} 触发 368 冷却，暂停 {int(cooldown/60)} 分钟", flush=True)
    return until


def _facebook_publish_throttle_remaining(page_id: str = "") -> float:
    interval = _facebook_min_post_interval_seconds()
    if interval <= 0:
        return 0.0
    with FACEBOOK_PUBLISH_STATE_LOCK:
        state = _load_facebook_publish_state()
        bucket = _facebook_page_state_bucket(state, page_id)
        last = float(bucket.get("last_post_at") or 0)
        return max(0.0, (last + interval) - time.time())


def _record_facebook_publish_success(page_id: str = "") -> None:
    with FACEBOOK_PUBLISH_STATE_LOCK:
        state = _load_facebook_publish_state()
        bucket = _facebook_page_state_bucket(state, page_id)
        bucket["last_post_at"] = time.time()
        bucket["frequency_block_count"] = 0
        # 发成功即清掉该 Page 的冷却(说明已解封)。
        bucket.pop("cooldown_until", None)
        bucket.pop("cooldown_reason", None)
        _save_facebook_publish_state(state)


def _mark_facebook_publish_pending(result: dict, *, retry_at: float, reason: str) -> None:
    result["facebook_publish_pending_at"] = max(time.time() + 30, float(retry_at or 0))
    result["facebook_publish_pending_reason"] = str(reason or "Facebook 发布等待重试")[:300]


def _clear_facebook_publish_pending(result: dict) -> None:
    result.pop("facebook_publish_pending_at", None)
    result.pop("facebook_publish_pending_reason", None)
    result.pop("facebook_publish_throttled", None)


def _facebook_publish_pending_due(result: dict, now_ts: Optional[float] = None) -> bool:
    try:
        retry_at = float((result or {}).get("facebook_publish_pending_at") or 0)
    except Exception:
        return False
    return bool(retry_at and retry_at <= float(now_ts if now_ts is not None else time.time()))


def _opennews_material_review_blocks_publish() -> bool:
    return _env_flag("OPENNEWS_MATERIAL_REVIEW_BLOCKS_AUTO_PUBLISH", "0")


OPENNEWS_CHANNEL_LANGUAGE_IDS = ("cn", "jp", "en")
OPENNEWS_CHANNEL_PLATFORM_IDS = ("x", "facebook", "youtube")
OPENNEWS_DEFAULT_CHANNELS = [
    {
        "id": "technology",
        "name": "科技新闻",
        "category": "technology",
        "keyword": "AI chip semiconductor robotics startup software",
        "enabled": True,
    },
    {
        "id": "military",
        "name": "军事新闻",
        "category": "military",
        "keyword": "defense missile drone navy air force Ukraine Taiwan Strait",
        "enabled": True,
    },
    {
        "id": "politics",
        "name": "政治新闻",
        "category": "politics",
        "keyword": "White House Congress election sanctions foreign policy",
        "enabled": True,
    },
    {
        "id": "finance",
        "name": "金融新闻",
        "category": "finance",
        "keyword": "Federal Reserve interest rates stocks market oil dollar inflation",
        "enabled": True,
    },
    {
        "id": "general",
        "name": "综合新闻",
        "category": "all",
        "keyword": "",
        "enabled": False,
    },
]
OPENNEWS_CHANNEL_SCHEDULER_STARTED = False


def _safe_opennews_channel_id(value: Any, fallback: str = "general") -> str:
    text = re.sub(r"[^a-z0-9_-]+", "_", str(value or "").strip().lower()).strip("_")
    return text or fallback


def _opennews_channel_language_name(language_id: str) -> str:
    return {
        "cn": "中文",
        "jp": "日文",
        "en": "英文",
    }.get(str(language_id or ""), str(language_id or ""))


def _default_opennews_language_account(language_id: str) -> dict:
    publish_enabled = str(language_id or "").strip().lower() == "cn"
    return {
        "language": language_id,
        "x": {
            "enabled": publish_enabled,
            "binding_mode": "global",
            "account_label": "",
            "handle": "",
            "profile_dir": "",
        },
        "facebook": {
            "enabled": publish_enabled,
            "binding_mode": "global",
            "page_name": "",
            "page_id": "",
            "page_access_token": "",
        },
        "youtube": {
            "enabled": False,
            "channel_name": "",
            "token_store_path": "",
        },
    }


def _default_opennews_channels_config() -> dict:
    now = time.time()
    channels = []
    for item in OPENNEWS_DEFAULT_CHANNELS:
        channel = {
            "id": item["id"],
            "name": item["name"],
            "enabled": bool(item.get("enabled")),
            "category": item["category"],
            "keyword": item.get("keyword") or "",
            "time_range": "6h",
            "interval_minutes": 120,
            "limit": 20,
            "produce_limit": 6,
            "languages": list(OPENNEWS_CHANNEL_LANGUAGE_IDS),
            "platforms": {"x": True, "facebook": True, "youtube": False},
            "accounts": {
                language_id: _default_opennews_language_account(language_id)
                for language_id in OPENNEWS_CHANNEL_LANGUAGE_IDS
            },
            "last_run_at": 0,
            "next_run_at": 0,
            "last_run_message": "",
            "last_run_error": "",
            "created_at": now,
            "updated_at": now,
        }
        channels.append(channel)
    return {
        "version": 1,
        "scheduler_enabled": False,
        "updated_at": now,
        "channels": channels,
    }


def _read_opennews_channels_raw() -> dict:
    try:
        if OPENNEWS_CHANNELS_CONFIG_PATH.exists():
            payload = json.loads(OPENNEWS_CHANNELS_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return _default_opennews_channels_config()


def _normalize_opennews_platform_account(platform: str, raw: Any, existing: dict | None = None) -> dict:
    existing = existing if isinstance(existing, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    platform = str(platform or "").strip()
    if platform == "x":
        binding_mode = str(raw.get("binding_mode") or existing.get("binding_mode") or "").strip().lower()
        if binding_mode not in {"global", "custom"}:
            binding_mode = "custom" if any(raw.get(key) or existing.get(key) for key in ("account_label", "label", "handle", "profile_dir", "user_data_dir")) else "global"
        if binding_mode == "global":
            return {
                "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", True)),
                "binding_mode": "global",
                "account_label": "",
                "handle": "",
                "profile_dir": "",
            }
        return {
            "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", True)),
            "binding_mode": "custom",
            "account_label": str(raw.get("account_label") or raw.get("label") or existing.get("account_label") or "").strip(),
            "handle": str(raw.get("handle") or existing.get("handle") or "").strip().lstrip("@"),
            "profile_dir": str(raw.get("profile_dir") or raw.get("user_data_dir") or existing.get("profile_dir") or "").strip(),
        }
    if platform == "facebook":
        binding_mode = str(raw.get("binding_mode") or existing.get("binding_mode") or "").strip().lower()
        if binding_mode not in {"global", "custom"}:
            binding_mode = "custom" if any(raw.get(key) or existing.get(key) for key in ("page_name", "page_id", "page_access_token")) else "global"
        if binding_mode == "global":
            return {
                "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", True)),
                "binding_mode": "global",
                "page_name": "",
                "page_id": "",
                "page_access_token": "",
            }
        incoming_token = str(raw.get("page_access_token") or "").strip()
        return {
            "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", True)),
            "binding_mode": "custom",
            "page_name": str(raw.get("page_name") or existing.get("page_name") or "").strip(),
            "page_id": str(raw.get("page_id") or existing.get("page_id") or "").strip(),
            "page_access_token": incoming_token or str(existing.get("page_access_token") or "").strip(),
        }
    if platform == "youtube":
        return {
            "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", False)),
            "channel_name": str(raw.get("channel_name") or existing.get("channel_name") or "").strip(),
            "token_store_path": str(raw.get("token_store_path") or existing.get("token_store_path") or "").strip(),
        }
    return {}


def _normalize_opennews_language_accounts(raw: Any, existing: dict | None = None) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    existing = existing if isinstance(existing, dict) else {}
    normalized: dict[str, dict] = {}
    for language_id in OPENNEWS_CHANNEL_LANGUAGE_IDS:
        raw_item = raw.get(language_id) if isinstance(raw.get(language_id), dict) else {}
        existing_item = existing.get(language_id) if isinstance(existing.get(language_id), dict) else {}
        normalized[language_id] = {
            "language": language_id,
            "x": _normalize_opennews_platform_account("x", raw_item.get("x"), existing_item.get("x")),
            "facebook": _normalize_opennews_platform_account("facebook", raw_item.get("facebook"), existing_item.get("facebook")),
            "youtube": _normalize_opennews_platform_account("youtube", raw_item.get("youtube"), existing_item.get("youtube")),
        }
    return normalized


def _normalize_opennews_channel(raw: Any, existing: dict | None = None) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    existing = existing if isinstance(existing, dict) else {}
    channel_id = _safe_opennews_channel_id(raw.get("id") or existing.get("id"))
    valid_categories = {item["id"] for item in opennews_trend_category_payloads()}
    category = str(raw.get("category") or existing.get("category") or channel_id or "all").strip().lower()
    if category not in valid_categories:
        category = "all"
    raw_languages = raw.get("languages") if isinstance(raw.get("languages"), list) else existing.get("languages")
    languages = []
    for item in raw_languages if isinstance(raw_languages, list) else list(OPENNEWS_CHANNEL_LANGUAGE_IDS):
        language_id = str(item or "").strip().lower()
        if language_id in OPENNEWS_CHANNEL_LANGUAGE_IDS and language_id not in languages:
            languages.append(language_id)
    if not languages:
        languages = list(OPENNEWS_CHANNEL_LANGUAGE_IDS)
    platforms_raw = raw.get("platforms") if isinstance(raw.get("platforms"), dict) else {}
    platforms_existing = existing.get("platforms") if isinstance(existing.get("platforms"), dict) else {}
    platforms = {
        "x": _parse_bool_form(platforms_raw.get("x")) if "x" in platforms_raw else _parse_bool_form(platforms_existing.get("x", True)),
        "facebook": _parse_bool_form(platforms_raw.get("facebook")) if "facebook" in platforms_raw else _parse_bool_form(platforms_existing.get("facebook", True)),
        "youtube": _parse_bool_form(platforms_raw.get("youtube")) if "youtube" in platforms_raw else _parse_bool_form(platforms_existing.get("youtube", False)),
    }
    interval_minutes = int(raw.get("interval_minutes") or existing.get("interval_minutes") or 120)
    if interval_minutes not in {5, 15, 30, 60, 120, 180, 360}:
        interval_minutes = 120
    time_range = str(raw.get("time_range") or existing.get("time_range") or "6h").strip().lower()
    if time_range not in {"1h", "6h", "24h"}:
        time_range = "6h"
    try:
        limit = max(5, min(int(raw.get("limit") or existing.get("limit") or 20), 60))
    except Exception:
        limit = 20
    try:
        produce_limit = max(1, min(int(raw.get("produce_limit") or existing.get("produce_limit") or 6), 12))
    except Exception:
        produce_limit = 6
    return {
        "id": channel_id,
        "name": str(raw.get("name") or existing.get("name") or channel_id).strip() or channel_id,
        "enabled": _parse_bool_form(raw.get("enabled")) if "enabled" in raw else _parse_bool_form(existing.get("enabled", False)),
        "category": category,
        "keyword": str(raw.get("keyword") if "keyword" in raw else existing.get("keyword") or "").strip(),
        "time_range": time_range,
        "interval_minutes": interval_minutes,
        "limit": limit,
        "produce_limit": produce_limit,
        "languages": languages,
        "platforms": platforms,
        "accounts": _normalize_opennews_language_accounts(raw.get("accounts"), existing.get("accounts")),
        "last_run_at": float(existing.get("last_run_at") or raw.get("last_run_at") or 0),
        "next_run_at": float(existing.get("next_run_at") or raw.get("next_run_at") or 0),
        "last_run_message": str(existing.get("last_run_message") or raw.get("last_run_message") or ""),
        "last_run_error": str(existing.get("last_run_error") or raw.get("last_run_error") or ""),
        "created_at": float(existing.get("created_at") or raw.get("created_at") or time.time()),
        "updated_at": time.time(),
    }


def _normalize_opennews_channels_config(raw: Any, existing: dict | None = None) -> dict:
    default_config = _default_opennews_channels_config()
    raw = raw if isinstance(raw, dict) else {}
    existing = existing if isinstance(existing, dict) else {}
    existing_by_id = {
        str(item.get("id") or ""): item
        for item in (existing.get("channels") or [])
        if isinstance(item, dict) and item.get("id")
    }
    raw_channels = raw.get("channels") if isinstance(raw.get("channels"), list) else None
    if raw_channels is None:
        raw_channels = default_config["channels"]
    channels: list[dict] = []
    seen: set[str] = set()
    for item in raw_channels:
        channel = _normalize_opennews_channel(item, existing_by_id.get(str((item or {}).get("id") or "")))
        if channel["id"] in seen:
            continue
        seen.add(channel["id"])
        channels.append(channel)
    for item in default_config["channels"]:
        if item["id"] not in seen:
            channels.append(_normalize_opennews_channel(item, existing_by_id.get(item["id"])))
    return {
        "version": 1,
        "scheduler_enabled": _parse_bool_form(raw.get("scheduler_enabled")) if "scheduler_enabled" in raw else _parse_bool_form(existing.get("scheduler_enabled", False)),
        "updated_at": time.time(),
        "channels": channels,
    }


def _write_opennews_channels_config(config: dict) -> dict:
    OPENNEWS_CHANNELS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = OPENNEWS_CHANNELS_CONFIG_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(config, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp_path.replace(OPENNEWS_CHANNELS_CONFIG_PATH)
    return config


def _load_opennews_channels_config(*, include_secrets: bool = False) -> dict:
    with OPENNEWS_CHANNEL_CONFIG_LOCK:
        config = _normalize_opennews_channels_config(_read_opennews_channels_raw())
        if not OPENNEWS_CHANNELS_CONFIG_PATH.exists():
            _write_opennews_channels_config(config)
    return config if include_secrets else _public_opennews_channels_config(config)


def _save_opennews_channels_config(payload: dict) -> dict:
    with OPENNEWS_CHANNEL_CONFIG_LOCK:
        existing = _normalize_opennews_channels_config(_read_opennews_channels_raw())
        config = _normalize_opennews_channels_config(payload, existing=existing)
        _write_opennews_channels_config(config)
    return config


def _opennews_channel_account_token_path(channel_id: str, language: str, platform: str) -> Path:
    """为某频道×语言×平台生成独立的凭据文件路径，实现按频道发布到不同账号。"""
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", f"{channel_id}_{language}").strip("_") or "channel"
    if platform == "youtube":
        return YOUTUBE_AUTH_DIR / f"youtube_token_{safe}.json"
    if platform == "facebook":
        return FACEBOOK_AUTH_DIR / f"facebook_token_{safe}.json"
    raise ValueError(f"未知平台：{platform}")


def _opennews_bind_channel_account(channel_id: str, language: str, platform: str, account_patch: dict) -> bool:
    """把某平台授权/登录结果合并写入指定频道/语言的账号槽（加锁）。返回是否命中该频道。"""
    with OPENNEWS_CHANNEL_CONFIG_LOCK:
        config = _normalize_opennews_channels_config(_read_opennews_channels_raw())
        hit = False
        for channel in config.get("channels", []):
            if str(channel.get("id")) != str(channel_id):
                continue
            accounts = channel.setdefault("accounts", {})
            slot = accounts.get(language)
            if not isinstance(slot, dict):
                slot = _default_opennews_language_account(language)
                accounts[language] = slot
            current = slot.get(platform) if isinstance(slot.get(platform), dict) else {}
            slot[platform] = {**current, **account_patch}
            channel["updated_at"] = time.time()
            hit = True
            break
        if hit:
            _write_opennews_channels_config(config)
        return hit


def _opennews_x_profile_logged_in(profile_dir: str) -> bool:
    """X 浏览器 profile 是否真登录成功：Cookies 里存在 x.com/twitter.com 的 auth_token。
    面板据此显示“已登录”而不是仅仅“建了档”，避免误判为已绑定。"""
    pdir = str(profile_dir or "").strip()
    if not pdir:
        return False
    base = Path(pdir)
    if not base.exists():
        return False
    try:
        cookies_files = list(base.rglob("Cookies"))[:6]
    except Exception:
        return False
    for cookies in cookies_files:
        try:
            con = sqlite3.connect(f"file:{cookies}?mode=ro&immutable=1", uri=True)
            row = con.execute(
                "SELECT 1 FROM cookies WHERE name='auth_token' "
                "AND (host_key LIKE '%x.com' OR host_key LIKE '%twitter.com') LIMIT 1"
            ).fetchone()
            con.close()
            if row:
                return True
        except Exception:
            continue
    return False


def _public_opennews_channels_config(config: dict) -> dict:
    public_config = copy.deepcopy(config)
    for channel in public_config.get("channels") or []:
        for account in (channel.get("accounts") or {}).values():
            if not isinstance(account, dict):
                continue
            facebook = account.get("facebook")
            if isinstance(facebook, dict):
                token = str(facebook.get("page_access_token") or "").strip()
                facebook["page_access_token_configured"] = bool(token)
                facebook["page_access_token"] = ""
            x_account = account.get("x")
            if isinstance(x_account, dict):
                x_account["logged_in"] = _opennews_x_profile_logged_in(x_account.get("profile_dir"))
    return public_config


def _find_opennews_channel(channel_id: str, *, include_secrets: bool = True) -> dict:
    config = _load_opennews_channels_config(include_secrets=include_secrets)
    wanted = _safe_opennews_channel_id(channel_id)
    for channel in config.get("channels") or []:
        if str(channel.get("id") or "") == wanted:
            return dict(channel)
    for channel in config.get("channels") or []:
        if str(channel.get("id") or "") == "general":
            return dict(channel)
    return {}


def _opennews_publish_account_for(channel_id: str, target_market: str, platform: str) -> dict:
    channel = _find_opennews_channel(channel_id, include_secrets=True)
    platform = str(platform or "").strip().lower()
    target_market = str(target_market or "cn").strip().lower() or "cn"
    platforms = channel.get("platforms") if isinstance(channel.get("platforms"), dict) else {}
    accounts = channel.get("accounts") if isinstance(channel.get("accounts"), dict) else {}
    language_account = accounts.get(target_market) if isinstance(accounts.get(target_market), dict) else {}
    platform_account = language_account.get(platform) if isinstance(language_account.get(platform), dict) else {}
    enabled = _parse_bool_form(platforms.get(platform, platform in {"x", "facebook"})) and _parse_bool_form(platform_account.get("enabled", True))
    public_account = copy.deepcopy(platform_account)
    if platform == "facebook":
        public_account["page_access_token_configured"] = bool(public_account.get("page_access_token"))
        public_account.pop("page_access_token", None)
    return {
        "enabled": enabled,
        "channel_id": str(channel.get("id") or channel_id or "general"),
        "channel_name": str(channel.get("name") or channel_id or "综合新闻"),
        "target_market": target_market,
        "target_market_label": _opennews_channel_language_name(target_market),
        "platform": platform,
        "account": platform_account,
        "public_account": public_account,
    }


def _opennews_result_channel_id(result: dict) -> str:
    workflow_config = result.get("workflow_config") if isinstance(result, dict) else {}
    if not isinstance(workflow_config, dict):
        workflow_config = {}
    channel_obj = workflow_config.get("opennews_channel") if isinstance(workflow_config.get("opennews_channel"), dict) else {}
    return _safe_opennews_channel_id(workflow_config.get("opennews_channel_id") or channel_obj.get("id") or "general")


def _opennews_channel_scheduler_due_channels(config: dict) -> list[dict]:
    if not config.get("scheduler_enabled"):
        return []
    now = time.time()
    due = []
    for channel in config.get("channels") or []:
        if not isinstance(channel, dict) or not channel.get("enabled"):
            continue
        next_run_at = float(channel.get("next_run_at") or 0)
        if not next_run_at or now >= next_run_at:
            due.append(channel)
    return due


def _youtube_thumbnail_error_is_rate_limited(error: str) -> bool:
    text = str(error or "").lower()
    return "uploadratelimitexceeded" in text or "too many thumbnails" in text or " 429" in text or "：429" in text


def _youtube_thumbnail_cooldown_until() -> float:
    try:
        if not YOUTUBE_THUMBNAIL_COOLDOWN_PATH.exists():
            return 0.0
        payload = json.loads(YOUTUBE_THUMBNAIL_COOLDOWN_PATH.read_text(encoding="utf-8"))
        return float(payload.get("until") or 0)
    except Exception:
        return 0.0


def _youtube_thumbnail_cooldown_message() -> str:
    until = _youtube_thumbnail_cooldown_until()
    if until <= time.time():
        return ""
    return f"YouTube 封面上传限流冷却中，预计 {time.strftime('%Y-%m-%d %H:%M:%S JST', time.localtime(until))} 后再试。"


def _set_youtube_thumbnail_cooldown(error: str = "") -> float:
    until = time.time() + YOUTUBE_THUMBNAIL_RATE_LIMIT_COOLDOWN_SECONDS
    payload = {
        "until": until,
        "reason": "uploadRateLimitExceeded",
        "error": str(error or "")[:1000],
        "updated_at": time.time(),
    }
    YOUTUBE_THUMBNAIL_COOLDOWN_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = YOUTUBE_THUMBNAIL_COOLDOWN_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(YOUTUBE_THUMBNAIL_COOLDOWN_PATH)
    return until


def _youtube_thumbnail_retry_path(video_id: str) -> Path:
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", str(video_id or "").strip())[:80]
    return YOUTUBE_THUMBNAIL_RETRY_DIR / f"{safe_id or uuid.uuid4().hex}.json"


def _remember_youtube_thumbnail_retry(
    *,
    video_id: str,
    thumbnail_path: Path,
    collection_id: str = "",
    title: str = "",
    youtube_url: str = "",
    error: str = "",
) -> dict:
    video_id = str(video_id or "").strip()
    if not video_id:
        return {}
    now = time.time()
    if _youtube_thumbnail_error_is_rate_limited(error):
        _set_youtube_thumbnail_cooldown(error)
    cooldown_until = _youtube_thumbnail_cooldown_until()
    retry_path = _youtube_thumbnail_retry_path(video_id)
    existing: dict = {}
    try:
        if retry_path.exists():
            existing = json.loads(retry_path.read_text(encoding="utf-8"))
    except Exception:
        existing = {}
    payload = {
        **existing,
        "video_id": video_id,
        "thumbnail_path": str(thumbnail_path),
        "collection_id": collection_id,
        "title": title,
        "youtube_url": youtube_url,
        "status": "pending",
        "attempts": int(existing.get("attempts") or 0),
        "last_error": str(error or "")[:1500],
        "next_attempt_at": max(cooldown_until, now + YOUTUBE_THUMBNAIL_RETRY_INTERVAL_SECONDS),
        "updated_at": now,
        "created_at": float(existing.get("created_at") or now),
    }
    tmp_path = retry_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(retry_path)
    return payload


def _update_collection_youtube_thumbnail_status(collection_id: str, video_id: str, thumbnail_result: dict) -> None:
    collection_id = str(collection_id or "").strip()
    video_id = str(video_id or "").strip()
    if not collection_id or not video_id:
        return
    job = load_collection_job(OPENNEWS_COLLECTION_DIR, collection_id)
    if not job:
        return
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    for rec in result.get("youtube_publish_records") or []:
        if isinstance(rec, dict) and str(rec.get("video_id") or "") == video_id:
            rec["thumbnail"] = thumbnail_result
            rec["thumbnail_retry_updated_at"] = time.time()
    latest = result.get("youtube_publish_latest")
    if isinstance(latest, dict) and str(latest.get("video_id") or "") == video_id:
        latest["thumbnail"] = thumbnail_result
        latest["thumbnail_retry_updated_at"] = time.time()
    update_collection_job(OPENNEWS_COLLECTION_DIR, collection_id, result=result)


def _retry_pending_youtube_thumbnails_once() -> int:
    now = time.time()
    if _youtube_thumbnail_cooldown_until() > now:
        return 0
    retried = 0
    for retry_path in sorted(YOUTUBE_THUMBNAIL_RETRY_DIR.glob("*.json"), key=lambda path: path.stat().st_mtime):
        try:
            payload = json.loads(retry_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("status") or "") != "pending":
            continue
        if float(payload.get("next_attempt_at") or 0) > now:
            continue
        attempts = int(payload.get("attempts") or 0)
        if attempts >= YOUTUBE_THUMBNAIL_RETRY_MAX_ATTEMPTS:
            payload["status"] = "failed"
            payload["updated_at"] = now
            retry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            continue
        video_id = str(payload.get("video_id") or "").strip()
        thumbnail_path = Path(str(payload.get("thumbnail_path") or ""))
        result = set_youtube_thumbnail(YOUTUBE_TOKEN_STORE_PATH, video_id, thumbnail_path)
        attempts += 1
        payload["attempts"] = attempts
        payload["last_result"] = result
        payload["updated_at"] = time.time()
        if result.get("ok"):
            payload["status"] = "done"
            payload["completed_at"] = time.time()
            _update_collection_youtube_thumbnail_status(
                str(payload.get("collection_id") or ""),
                video_id,
                {"ok": True, "raw": result.get("raw"), "retried": True, "attempts": attempts},
            )
            print(f"[YouTube thumbnail retry] success video_id={video_id}", flush=True)
        else:
            error = str(result.get("error") or "")
            payload["last_error"] = error[:1500]
            if _youtube_thumbnail_error_is_rate_limited(error):
                cooldown_until = _set_youtube_thumbnail_cooldown(error)
                payload["next_attempt_at"] = cooldown_until
            else:
                payload["next_attempt_at"] = time.time() + YOUTUBE_THUMBNAIL_RETRY_INTERVAL_SECONDS
            if attempts >= YOUTUBE_THUMBNAIL_RETRY_MAX_ATTEMPTS:
                payload["status"] = "failed"
            print(f"[YouTube thumbnail retry] failed video_id={video_id}: {error[:240]}", flush=True)
        retry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        retried += 1
        if retried >= 1:
            break
    return retried


def _youtube_thumbnail_retry_worker() -> None:
    time.sleep(30)
    while True:
        try:
            _retry_pending_youtube_thumbnails_once()
        except Exception as exc:
            print(f"[YouTube thumbnail retry] worker error: {exc}", flush=True)
        time.sleep(YOUTUBE_THUMBNAIL_RETRY_INTERVAL_SECONDS)


def _start_youtube_thumbnail_retry_worker() -> None:
    global YOUTUBE_THUMBNAIL_RETRY_WORKER_STARTED
    with YOUTUBE_THUMBNAIL_RETRY_LOCK:
        if YOUTUBE_THUMBNAIL_RETRY_WORKER_STARTED:
            return
        YOUTUBE_THUMBNAIL_RETRY_WORKER_STARTED = True
    threading.Thread(target=_youtube_thumbnail_retry_worker, daemon=True, name="youtube-thumbnail-retry").start()


def _cleanup_stale_opennews_batch_jobs() -> None:
    jobs_dir = OPENNEWS_BATCH_DIR / "batch_jobs"
    if not jobs_dir.exists():
        return
    now = time.time()
    active_statuses = {"queued", "drafting", "producing", "composing", "publishing_youtube", "publishing_x", "running"}
    cleaned = 0
    for job_path in jobs_dir.glob("opennews_batch_*.json"):
        try:
            if (now - job_path.stat().st_mtime) / 3600 < OPENNEWS_STALE_JOB_TIMEOUT_HOURS:
                continue
            job = json.loads(job_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(job.get("status") or "") not in {"running", "queued"}:
            continue
        touched = str(job.get("status") or "") == "queued"
        for item in job.get("items", []) or []:
            if str(item.get("status") or "") not in active_statuses:
                continue
            item["status"] = "failed"
            item["message"] = f"任务超过 {OPENNEWS_STALE_JOB_TIMEOUT_HOURS} 小时未更新，已自动标记为中断。"
            item["error"] = "stale_opennews_batch_item_timeout"
            item["completed_at"] = now
            item["updated_at"] = now
            touched = True
        if not touched:
            continue
        counts: dict[str, int] = {}
        for item in job.get("items", []) or []:
            status = str(item.get("status") or "")
            counts[status] = counts.get(status, 0) + 1
        job["status"] = "partial" if counts.get("completed") else "failed"
        job["message"] = "存在长时间未更新的任务，已自动清理。"
        job["updated_at"] = now
        tmp_path = job_path.with_suffix(job_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(job_path)
        cleaned += 1
    if cleaned:
        print(f"🧹 已清理 OpenNews 历史超时批次：{cleaned} 个")


def _update_opennews_channel_after_run(channel_id: str, result: dict) -> None:
    channel_id = _safe_opennews_channel_id(channel_id)
    with OPENNEWS_CHANNEL_CONFIG_LOCK:
        config = _normalize_opennews_channels_config(_read_opennews_channels_raw())
        for channel in config.get("channels") or []:
            if str(channel.get("id") or "") != channel_id:
                continue
            finished_at = float(result.get("finished_at") or time.time())
            channel["last_run_at"] = finished_at
            channel["next_run_at"] = finished_at + int(channel.get("interval_minutes") or 120) * 60 if config.get("scheduler_enabled") and channel.get("enabled") else 0
            channel["last_run_message"] = str(result.get("message") or "")
            channel["last_run_error"] = "" if result.get("ok") else str(result.get("error") or result.get("message") or "")
            channel["updated_at"] = time.time()
            break
        config["updated_at"] = time.time()
        _write_opennews_channels_config(config)


def _run_opennews_channel_fetch(channel: dict, *, triggered_by: str = "channel_scheduler") -> dict:
    channel_id = _safe_opennews_channel_id(channel.get("id"))
    result = run_opennews_batch_fetch_once(
        OPENNEWS_BATCH_DIR,
        triggered_by=triggered_by,
        override={
            "channel_id": channel_id,
            "channel_name": channel.get("name") or channel_id,
            "category": channel.get("category") or "all",
            "keyword": channel.get("keyword") or "",
            "time_range": channel.get("time_range") or "6h",
            "limit": channel.get("limit") or 20,
        },
    )
    if not result.get("running"):
        _update_opennews_channel_after_run(channel_id, result)
    return result


def _start_opennews_channel_scheduler(poll_seconds: int = 20) -> None:
    global OPENNEWS_CHANNEL_SCHEDULER_STARTED
    if OPENNEWS_CHANNEL_SCHEDULER_STARTED:
        return
    OPENNEWS_CHANNEL_SCHEDULER_STARTED = True
    _load_opennews_channels_config(include_secrets=True)
    print(f"[OpenNews channel scheduler] started root={OPENNEWS_BATCH_DIR} poll_seconds={poll_seconds}", flush=True)

    def loop() -> None:
        while True:
            try:
                config = _load_opennews_channels_config(include_secrets=True)
                for channel in _opennews_channel_scheduler_due_channels(config):
                    print(
                        "[OpenNews channel scheduler] due, running fetch "
                        f"channel={channel.get('id')} next_run_at={channel.get('next_run_at') or 0} now={time.time()}",
                        flush=True,
                    )
                    result = _run_opennews_channel_fetch(channel, triggered_by="channel_scheduler")
                    if result.get("running"):
                        break
                    time.sleep(2)
            except Exception as exc:
                print(f"[OpenNews channel scheduler] loop error: {exc!r}", flush=True)
            time.sleep(max(10, int(poll_seconds)))

    threading.Thread(target=loop, name="opennews-channel-scheduler", daemon=True).start()


# 已在本进程内触发过"补发布"的成片目录名，避免同一条被重复触发发布。
_OPENNEWS_PUBLISH_RECOVERY_ATTEMPTED: set[str] = set()
# 本进程内已尝试过重合成的目录:避免"永远合成失败"(如缺配音)的旧目录被无限重做，堵死合成/GPU 管线。
_COMPOSE_READY_RECOVERY_ATTEMPTED: set[str] = set()


_STALE_PUBLISH_ERROR_MARKERS = ("还没有可上传的成片", "请先生成成片", "没有可上传的成片")


def _is_stale_publish_error(text: Any) -> bool:
    """判断某条发布错误是否为“成片还没就绪”这类会随合成完成而自愈的临时错误。
    这类错误不应永久挡住补发——合成完成后成片已存在，必须允许恢复机制重试发布。"""
    t = str(text or "")
    return bool(t) and any(marker in t for marker in _STALE_PUBLISH_ERROR_MARKERS)


def _recover_ready_compose_histories_once(max_items: int = COMPOSE_READY_RECOVERY_BATCH_SIZE) -> int:
    candidates: list[Path] = []
    try:
        candidates = sorted(
            [path for path in OUTPUT_DIR.iterdir() if path.is_dir() and (path / "result.json").exists()],
            key=lambda path: (path / "result.json").stat().st_mtime,
            reverse=True,
        )
    except Exception as exc:
        print(f"[compose-ready recovery] list error: {exc!r}", flush=True)
        return 0

    recovered = 0
    now_ts = time.time()
    recovery_max_age = float(os.getenv("COMPOSE_READY_RECOVERY_MAX_AGE_SECONDS", str(2 * 86400)) or (2 * 86400))
    for output_dir in candidates:
        if recovered >= max(1, int(max_items)):
            break
        # 只恢复较新的成片：目录名前缀是原始产出时间戳，超过 recovery_max_age(默认2天)的旧积压不再
        # 重新合成/补发，避免把早期别的频道/通用自动残留的旧新闻(非当前配置频道)又制作出来。
        try:
            dir_ts = float(str(output_dir.name).split("_", 1)[0])
        except Exception:
            dir_ts = now_ts
        history_too_old = (now_ts - dir_ts) > recovery_max_age
        result_path = output_dir / "result.json"
        try:
            result = _load_result_from_output_dir(output_dir)
            if not isinstance(result, dict):
                continue
        except Exception as exc:
            print(f"[compose-ready recovery] read error dir={output_dir.name} err={exc!r}", flush=True)
            continue
        if history_too_old and not result.get("facebook_publish_pending_at"):
            continue
        # 外部自动批次仍由主线程负责合成/发布时，恢复线程不得抢跑。
        if _opennews_external_produce_active(output_dir):
            continue
        facebook_retry_due = _facebook_publish_pending_due(result, now_ts)
        # 已发布过的 OpenNews 成片视为完成：不再重合成或补发，避免重合成丢记录后又重新上传同一条。
        if _history_is_opennews_result(result) and (
            result.get("youtube_publish_records")
            or result.get("facebook_publish_records")
            or result.get("x_publish_records")
        ) and not facebook_retry_due:
            continue
        lifecycle = _build_history_lifecycle(output_dir, result)
        if lifecycle.get("live_task_id"):
            continue
        if not lifecycle.get("can_compose"):
            # 已合成（或无需再合成）的 OpenNews 成片：若有可发布视频但从未发布过，
            # 补触发一次发布。覆盖 produce 自身发布未成功、或历史遗留未发的情况。
            try:
                # 跳过刚改动过(最近 15 分钟)的成片：它多半正被自己的生产流程发布，
                # 恢复工人此时插手会和主发布并发，导致同一条视频重复上传。恢复只管“旧的卡住的”。
                try:
                    recently_touched = (time.time() - result_path.stat().st_mtime) < 900
                except Exception:
                    recently_touched = False
                if (
                    (facebook_retry_due or not recently_touched)
                    and (facebook_retry_due or output_dir.name not in _OPENNEWS_PUBLISH_RECOVERY_ATTEMPTED)
                    and _history_is_opennews_result(result)
                    and _opennews_result_has_publishable_video(output_dir, result)
                ):
                    already_published = bool(
                        result.get("x_publish_records")
                        or result.get("facebook_publish_records")
                        or result.get("youtube_publish_records")
                    ) and not facebook_retry_due
                    # 只把“真实失败”的错误当作阻断；“成片未就绪”这类临时错误现在成片已存在，应放行补发。
                    already_errored = any(
                        v and not _is_stale_publish_error(v)
                        for key, v in (
                            ("x", result.get("x_auto_publish_error")),
                            ("facebook", result.get("facebook_auto_publish_error")),
                            ("youtube", result.get("youtube_auto_publish_error")),
                        )
                        if not (facebook_retry_due and key == "facebook")
                    )
                    if not facebook_retry_due and _opennews_youtube_publish_claim_active(result, now_ts=now_ts):
                        continue
                    if not already_published and not already_errored:
                        if not facebook_retry_due:
                            _OPENNEWS_PUBLISH_RECOVERY_ATTEMPTED.add(output_dir.name)
                        reason = "facebook-retry" if facebook_retry_due else "unpublished"
                        print(f"[publish-ready recovery] auto-publish dir={output_dir.name} reason={reason}", flush=True)
                        _schedule_opennews_post_compose_publish("", str(output_dir), result)
                        recovered += 1
            except Exception as pub_exc:
                print(f"[publish-ready recovery] trigger failed dir={output_dir.name} err={pub_exc!r}", flush=True)
            continue
        # 本进程内同一目录只重合成一次:缺配音等永久错误不再无限重做,避免堵死管线。
        if output_dir.name in _COMPOSE_READY_RECOVERY_ATTEMPTED:
            continue
        _COMPOSE_READY_RECOVERY_ATTEMPTED.add(output_dir.name)
        try:
            print(
                f"[compose-ready recovery] composing dir={output_dir.name} topic={str(result.get('topic') or '')[:80]}",
                flush=True,
            )
            composed_result = _compose_history_result(
                output_dir,
                result,
                user=None,
                requested_aspect_ratio="",
                cost_scope="compose_ready_recovery",
            )
            recovered += 1
            # 合成成功后，对 OpenNews 结果触发自动发布（X/Facebook/YouTube）。
            # produce 主流程若因 TTS 等中途失败不会发布，recovery 补齐成片后必须接上发布，
            # 否则视频产好却一直不发。带"未发过才发"守卫，避免重复发。
            try:
                pub_result = composed_result if isinstance(composed_result, dict) else result
                if _history_is_opennews_result(pub_result) and _opennews_result_has_publishable_video(output_dir, pub_result):
                    already_published = bool(
                        pub_result.get("x_publish_records")
                        or pub_result.get("facebook_publish_records")
                        or pub_result.get("youtube_publish_records")
                    )
                    if not already_published:
                        print(f"[compose-ready recovery] auto-publish dir={output_dir.name}", flush=True)
                        _schedule_opennews_post_compose_publish("", str(output_dir), pub_result)
            except Exception as pub_exc:
                print(
                    f"[compose-ready recovery] auto-publish trigger failed dir={output_dir.name} err={pub_exc!r}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"[compose-ready recovery] compose failed dir={output_dir.name} err={exc!r}",
                flush=True,
            )
    if recovered:
        print(f"[compose-ready recovery] recovered={recovered}", flush=True)
    return recovered


def _start_compose_ready_recovery_worker(poll_seconds: int = COMPOSE_READY_RECOVERY_POLL_SECONDS) -> None:
    global COMPOSE_READY_RECOVERY_STARTED
    if COMPOSE_READY_RECOVERY_STARTED:
        return
    COMPOSE_READY_RECOVERY_STARTED = True
    print(
        "[compose-ready recovery] started "
        f"root={OUTPUT_DIR} poll_seconds={poll_seconds} batch_size={COMPOSE_READY_RECOVERY_BATCH_SIZE}",
        flush=True,
    )

    def loop() -> None:
        while True:
            try:
                _recover_ready_compose_histories_once(COMPOSE_READY_RECOVERY_BATCH_SIZE)
            except Exception as exc:
                print(f"[compose-ready recovery] loop error: {exc!r}", flush=True)
            time.sleep(max(20, int(poll_seconds)))

    threading.Thread(target=loop, name="compose-ready-recovery", daemon=True).start()


@app.on_event("startup")
async def _start_opennews_batch_scheduler() -> None:
    _cleanup_stale_opennews_batch_jobs()
    if os.getenv("YOUTUBE_FEATURE_ENABLED", "0").strip().lower() not in {"0", "false", "no", "off"}:
        _start_youtube_thumbnail_retry_worker()
    set_opennews_batch_after_fetch_callback(_handle_opennews_batch_after_fetch)
    save_opennews_auto_config(
        OPENNEWS_AUTO_DIR,
        {
            "enabled": False,
            "interval_minutes": 120,
            "categories": ["all"],
            "time_range": "6h",
            "limit": 20,
        },
    )
    save_opennews_batch_config(
        OPENNEWS_BATCH_DIR,
        {
            "enabled": False,
            "interval_minutes": 120,
            "category": "all",
            "time_range": "6h",
            "limit": 20,
        },
    )
    _start_opennews_channel_scheduler(poll_seconds=20)
    _recover_pending_auto_digital_batches()
    _start_compose_ready_recovery_worker()
    _start_topic_auto_scheduler(poll_seconds=60)
    _start_property_auto_scheduler(poll_seconds=60)

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
        "voice_id": (
            os.getenv("VOICE_TAIWAN_CLONE", "moss_audio_08266d37-33be-11f1-a17e-22a10454a6ba").strip()
            or "moss_audio_08266d37-33be-11f1-a17e-22a10454a6ba"
        ),
        "default_speed": 1.1,
        "default_volume": 1.0,
        "tags": ["女声", "台湾", "克隆"],
        "sample_text": "嗨，今天用更自然亲切的语气，陪你快速看懂这个主题。",
        "enabled": True,
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
    {
        "id": "english_female",
        "name": "自然英语女声",
        "subtitle": "英语",
        "gender": "female",
        "language": "en-US",
        "style": "适合国际新闻、科技资讯、英文市场解说内容",
        "voice_id": (
            os.getenv("VOICE_ENGLISH_FEMALE", "").strip()
            or os.getenv("VOICE_MANDARIN_FEMALE", "").strip()
            or "Chinese (Mandarin)_Warm_Bestie"
        ),
        "default_speed": 1.05,
        "default_volume": 1.0,
        "tags": ["女声", "英语", "国际"],
        "sample_text": "Hello, here is a quick and clear breakdown of today's biggest story.",
    },
]


INTERFACE_LANGUAGES = [
    {"id": "zh-CN", "name": "简体中文"},
    {"id": "zh-TW", "name": "繁體中文"},
    {"id": "ja-JP", "name": "日本語"},
    {"id": "en-US", "name": "English"},
]

DEPARTMENTS = [
    {"id": "real_estate", "name": "房地产"},
    {"id": "robotics", "name": "机器人"},
]

TARGET_MARKETS = [
    {"id": "cn", "name": "中国市场", "content_language": "简体中文", "default_voice_preset_id": "mandarin_female"},
    {"id": "tw", "name": "台湾市场", "content_language": "繁體中文", "default_voice_preset_id": "taiwan_clone"},
    {"id": "jp", "name": "日本市场", "content_language": "日语", "default_voice_preset_id": "japanese_female"},
    {"id": "en", "name": "英语市场", "content_language": "English", "default_voice_preset_id": "english_female"},
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
QWEN_TTS_MAX_CONCURRENT = max(1, int(os.getenv("OPENNEWS_QWEN_TTS_MAX_CONCURRENT", "1") or "1"))
QWEN_TTS_QUEUE_CONDITION = threading.Condition()
QWEN_TTS_WAITING_JOBS: list[dict] = []
QWEN_TTS_RUNNING_JOBS = 0
QWEN_TTS_RUNNING_ITEMS: list[dict] = []
GPU_RESOURCE_LOCK = threading.RLock()
GPU_RESOURCE_CONDITION = threading.Condition()
GPU_RESOURCE_WAITING_JOBS: list[dict] = []
GPU_RESOURCE_ACTIVE_JOB: Optional[dict] = None
HUNYUAN_ENGINE_ID = "hunyuan_local"
INFINITETALK_ENGINE_ID = "infinitetalk_local"
VOLC_ENGINE_ID = "volc_omnihuman"
LOCAL_DIGITAL_HUMAN_GPU_PROFILE = os.getenv("LOCAL_DIGITAL_HUMAN_GPU_PROFILE", "digital_intro").strip() or "digital_intro"
LOCAL_DIGITAL_HUMAN_RESTORE_PROFILE = os.getenv("LOCAL_DIGITAL_HUMAN_RESTORE_PROFILE", "material").strip()


def _digital_keep_warm_enabled() -> bool:
    # 保持 InfiniteTalk 常驻:数字人任务做完后不立刻切回 restore 档(不停服务),
    # 让连续的数字人段/条之间 InfiniteTalk 一直在,避免反复 stop/start 打断+重载模型。
    # 后续的 TTS 任务需要显存时会自行切档停掉它。默认开启;置 0 可回退旧行为。
    return (os.getenv("LOCAL_DIGITAL_HUMAN_KEEP_WARM", "1").strip().lower() not in {"0", "false", "no", "off"})
SCRIPT_MODEL_CLAUDE = "claude"
SCRIPT_MODEL_API_RELAY = "api_relay"
SCRIPT_MODEL_LOCAL_QWEN = "local_qwen"
SCRIPT_MODEL_FORCE_ENV_KEYS = ("FORCE_SCRIPT_MODEL_PROVIDER", "SCRIPT_MODEL_FORCE_PROVIDER")
DIGITAL_HUMAN_ENGINES = [
    {
        "id": INFINITETALK_ENGINE_ID,
        "name": "5090 本地 InfiniteTalk",
        "description": "数字人统一生产通道：5090 本地 InfiniteTalk。配音仍使用 MiniMax API。",
        "admin_only": False,
        "default": True,
    },
]
SCRIPT_MODEL_OPTIONS = [
    {
        "id": SCRIPT_MODEL_CLAUDE,
        "name": "Claude",
        "description": "主文案模型：用于普通数字人文案、新闻转写和口播稿生成。联网检索入口已暂停。",
        "admin_only": False,
        "default": True,
    },
    {
        "id": SCRIPT_MODEL_LOCAL_QWEN,
        "name": "5090 本地 Qwen",
        "description": "本地 5090 文案模型，适合批量选题与中国市场短视频文案。",
        "admin_only": True,
        "default": False,
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
    "digital_human_generate": {"provider": "infinitetalk_5090", "base": 0.0, "per_second": round(1.0 / FX_CNY_PER_USD, 6)},
    "material_fetch": {"provider": "pexels", "base": 0.0, "per_segment": 0.0},
    "tos_upload": {"provider": "volc_tos", "minimum": 0.0, "per_mb": 0.0},
    "compose_video": {"provider": "ffmpeg", "base": 0.0, "per_second": 0.0},
}
OPENNEWS_QWEN_TTS_ENABLED = (os.getenv("OPENNEWS_QWEN_TTS_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_QWEN_TTS_BASE_URL = (os.getenv("OPENNEWS_QWEN_TTS_BASE_URL") or "http://192.168.0.34:8895").strip().rstrip("/")
OPENNEWS_QWEN_TTS_TOKEN = os.getenv("OPENNEWS_QWEN_TTS_TOKEN", "local-qwen3-tts-5090").strip()
# 单个分片 TTS 调用的重试次数：TTS 在请求预算回收(每36次)或瞬时不可用时会短暂失败,
# 单片重试可避免"一片失败导致整段配音失败 -> 没有可用配音文件 -> 合成失败"。
OPENNEWS_QWEN_TTS_RETRY_ATTEMPTS = max(1, min(8, int(os.getenv("OPENNEWS_QWEN_TTS_RETRY_ATTEMPTS", "4") or "4")))
OPENNEWS_QWEN_TTS_SPEAKER = os.getenv("OPENNEWS_QWEN_TTS_SPEAKER", "serena").strip() or "serena"
OPENNEWS_QWEN_TTS_FEMALE_SPEAKER = os.getenv("OPENNEWS_QWEN_TTS_FEMALE_SPEAKER", "serena").strip() or "serena"
OPENNEWS_QWEN_TTS_MALE_SPEAKER = os.getenv("OPENNEWS_QWEN_TTS_MALE_SPEAKER", "aiden").strip() or "aiden"
OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_SPEAKER = os.getenv(
    "OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_SPEAKER",
    OPENNEWS_QWEN_TTS_FEMALE_SPEAKER,
).strip() or OPENNEWS_QWEN_TTS_FEMALE_SPEAKER
OPENNEWS_QWEN_TTS_JAPANESE_MALE_SPEAKER = os.getenv(
    "OPENNEWS_QWEN_TTS_JAPANESE_MALE_SPEAKER",
    OPENNEWS_QWEN_TTS_MALE_SPEAKER,
).strip() or OPENNEWS_QWEN_TTS_MALE_SPEAKER
OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_SPEAKER = os.getenv(
    "OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_SPEAKER",
    OPENNEWS_QWEN_TTS_FEMALE_SPEAKER,
).strip() or OPENNEWS_QWEN_TTS_FEMALE_SPEAKER
OPENNEWS_QWEN_TTS_ENGLISH_MALE_SPEAKER = os.getenv(
    "OPENNEWS_QWEN_TTS_ENGLISH_MALE_SPEAKER",
    OPENNEWS_QWEN_TTS_MALE_SPEAKER,
).strip() or OPENNEWS_QWEN_TTS_MALE_SPEAKER
OPENNEWS_QWEN_TTS_LANGUAGE = os.getenv("OPENNEWS_QWEN_TTS_LANGUAGE", "chinese").strip() or "chinese"
OPENNEWS_QWEN_TTS_TIMEOUT = max(15, int(os.getenv("OPENNEWS_QWEN_TTS_TIMEOUT", "180") or "180"))
OPENNEWS_QWEN_TTS_RETRY_INTERVAL_SECONDS = max(5, int(os.getenv("OPENNEWS_QWEN_TTS_RETRY_INTERVAL_SECONDS", "30") or "30"))
OPENNEWS_QWEN_TTS_GPU_UNAVAILABLE_RETRY_INTERVAL_SECONDS = max(
    OPENNEWS_QWEN_TTS_RETRY_INTERVAL_SECONDS,
    int(os.getenv("OPENNEWS_QWEN_TTS_GPU_UNAVAILABLE_RETRY_INTERVAL_SECONDS", "120") or "120"),
)
OPENNEWS_QWEN_TTS_HEALTH_TIMEOUT = max(3, int(os.getenv("OPENNEWS_QWEN_TTS_HEALTH_TIMEOUT", "8") or "8"))
OPENNEWS_QWEN_TTS_RESTART_ON_FAILURE = (
    os.getenv("OPENNEWS_QWEN_TTS_RESTART_ON_FAILURE", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_QWEN_TTS_MAX_CHARS = max(60, int(os.getenv("OPENNEWS_QWEN_TTS_MAX_CHARS", "180") or "180"))
OPENNEWS_QWEN_TTS_CHUNK_PAUSE_SECONDS = max(0.0, float(os.getenv("OPENNEWS_QWEN_TTS_CHUNK_PAUSE_SECONDS", "2.0") or "2.0"))
OPENNEWS_QWEN_TTS_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_INSTRUCT",
    "用自然、清晰、专业的中文新闻女主播语气朗读，节奏稳定，声音有亲和力。",
).strip()
OPENNEWS_QWEN_TTS_FEMALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_FEMALE_INSTRUCT",
    "用自然、清晰、专业的中文新闻女主播语气朗读，节奏稳定，声音有亲和力。",
).strip()
OPENNEWS_QWEN_TTS_MALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_MALE_INSTRUCT",
    "用沉稳、清晰、专业的中文新闻男主播语气朗读，节奏稳定，信息感强。",
).strip()
OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_INSTRUCT",
    "自然で聞き取りやすい日本語ニュース女性アナウンサーの口調で読み上げてください。落ち着いて、明瞭で、テンポは安定させてください。",
).strip()
OPENNEWS_QWEN_TTS_JAPANESE_MALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_JAPANESE_MALE_INSTRUCT",
    "落ち着きがあり聞き取りやすい日本語ニュース男性アナウンサーの口調で読み上げてください。明瞭で安定したテンポを保ってください。",
).strip()
OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_INSTRUCT",
    "Read in a clear, natural, professional English female news anchor tone with steady pacing and trustworthy delivery.",
).strip()
OPENNEWS_QWEN_TTS_ENGLISH_MALE_INSTRUCT = os.getenv(
    "OPENNEWS_QWEN_TTS_ENGLISH_MALE_INSTRUCT",
    "Read in a clear, calm, professional English male news anchor tone with steady pacing and strong information delivery.",
).strip()
OPENNEWS_MINIMAX_FALLBACK_VOICE_PRESET_ID = os.getenv("OPENNEWS_MINIMAX_FALLBACK_VOICE_PRESET_ID", "mandarin_female").strip() or "mandarin_female"
OPENNEWS_MINIMAX_FALLBACK_FEMALE_VOICE_PRESET_ID = os.getenv("OPENNEWS_MINIMAX_FALLBACK_FEMALE_VOICE_PRESET_ID", "mandarin_female").strip() or "mandarin_female"
OPENNEWS_MINIMAX_FALLBACK_MALE_VOICE_PRESET_ID = os.getenv("OPENNEWS_MINIMAX_FALLBACK_MALE_VOICE_PRESET_ID", "mandarin_male").strip() or "mandarin_male"
OPENNEWS_MINIMAX_FALLBACK_JAPANESE_FEMALE_VOICE_PRESET_ID = os.getenv(
    "OPENNEWS_MINIMAX_FALLBACK_JAPANESE_FEMALE_VOICE_PRESET_ID",
    "japanese_female",
).strip() or "japanese_female"
OPENNEWS_MINIMAX_FALLBACK_ENGLISH_FEMALE_VOICE_PRESET_ID = os.getenv(
    "OPENNEWS_MINIMAX_FALLBACK_ENGLISH_FEMALE_VOICE_PRESET_ID",
    "english_female",
).strip() or "english_female"
# Collection intro is intentionally short because digital-human generation is slow.
OPENNEWS_COLLECTION_INTRO_ENABLED = (os.getenv("OPENNEWS_COLLECTION_INTRO_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_COLLECTION_INTRO_ANCHOR_PATH = ASSETS_DIR / os.getenv("OPENNEWS_COLLECTION_INTRO_ANCHOR_FILENAME", "opennews_anchor_daily.png").strip()
OPENNEWS_COLLECTION_INTRO_FEMALE_ANCHOR_PATH = ASSETS_DIR / os.getenv("OPENNEWS_COLLECTION_INTRO_FEMALE_ANCHOR_FILENAME", "opennews_anchor_daily.png").strip()
OPENNEWS_COLLECTION_INTRO_MALE_ANCHOR_PATH = ASSETS_DIR / os.getenv("OPENNEWS_COLLECTION_INTRO_MALE_ANCHOR_FILENAME", "opennews_anchor_daily_male.png").strip()
OPENNEWS_PRESENTER_STATE_PATH = OPENNEWS_BATCH_DIR / "presenter_state.json"
OPENNEWS_PRESENTER_STATE_LOCK = threading.Lock()
GPU_ORCHESTRATOR_ENABLED = (os.getenv("GPU_ORCHESTRATOR_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
GPU_ORCHESTRATOR_URL = os.getenv("GPU_ORCHESTRATOR_URL", "http://192.168.0.34:8898").strip().rstrip("/")
GPU_ORCHESTRATOR_TOKEN = os.getenv("GPU_ORCHESTRATOR_TOKEN", "local-gpu-orchestrator-5090").strip()
GPU_ORCHESTRATOR_TIMEOUT_SECONDS = max(3, int(os.getenv("GPU_ORCHESTRATOR_TIMEOUT_SECONDS", "25") or "25"))
OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED = (
    (os.getenv("OPENNEWS_COLLECTION_INTRO_LOCAL_DIGITAL_ENABLED", "1") or "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES = [
    part.strip()
    for part in (os.getenv("OPENNEWS_COLLECTION_INTRO_LOCAL_ENGINES", INFINITETALK_ENGINE_ID) or "").split(",")
    if part.strip()
]


def _opennews_presenter_config(gender: str = "female") -> dict:
    gender = str(gender or "female").strip().lower()
    if gender not in {"female", "male"}:
        gender = "female"
    if gender == "male":
        return {
            "gender": "male",
            "label": "男主播",
            "anchor_filename": OPENNEWS_COLLECTION_INTRO_MALE_ANCHOR_PATH.name,
            "anchor_path": str(OPENNEWS_COLLECTION_INTRO_MALE_ANCHOR_PATH),
            "qwen_speaker": OPENNEWS_QWEN_TTS_MALE_SPEAKER,
            "qwen_instruct": OPENNEWS_QWEN_TTS_MALE_INSTRUCT,
            "minimax_voice_preset_id": OPENNEWS_MINIMAX_FALLBACK_MALE_VOICE_PRESET_ID,
            "voice_preset_id": "mandarin_male",
            "digital_human_prompt": (
                "专业中文新闻男主播坐在演播桌前，面向镜头播报 OpenNews 每日热点开场。"
                "表情沉稳可信，口型清晰，轻微点头，动作克制，新闻栏目质感。"
                "不要改变背景中的 OpenNews 每日热点标识，不要添加额外文字。"
            ),
        }
    return {
        "gender": "female",
        "label": "女主播",
        "anchor_filename": OPENNEWS_COLLECTION_INTRO_FEMALE_ANCHOR_PATH.name,
        "anchor_path": str(OPENNEWS_COLLECTION_INTRO_FEMALE_ANCHOR_PATH),
        "qwen_speaker": OPENNEWS_QWEN_TTS_FEMALE_SPEAKER,
        "qwen_instruct": OPENNEWS_QWEN_TTS_FEMALE_INSTRUCT,
        "minimax_voice_preset_id": OPENNEWS_MINIMAX_FALLBACK_FEMALE_VOICE_PRESET_ID,
        "voice_preset_id": "mandarin_female",
        "digital_human_prompt": (
            "专业中文新闻女主播坐在演播桌前，面向镜头播报 OpenNews 每日热点开场。"
            "表情自然可信，口型清晰，轻微点头，动作克制，新闻栏目质感。"
            "不要改变背景中的 OpenNews 每日热点标识，不要添加额外文字。"
        ),
    }


def _opennews_presenter_config_for_market(target_market: str = "cn", gender: str = "female") -> dict:
    target_market = str(target_market or "cn").strip().lower() or "cn"
    presenter = _opennews_presenter_config(gender)
    gender = presenter.get("gender", "female")
    if target_market == "jp":
        presenter["qwen_speaker"] = (
            OPENNEWS_QWEN_TTS_JAPANESE_MALE_SPEAKER if gender == "male" else OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_SPEAKER
        )
        presenter["qwen_instruct"] = (
            OPENNEWS_QWEN_TTS_JAPANESE_MALE_INSTRUCT if gender == "male" else OPENNEWS_QWEN_TTS_JAPANESE_FEMALE_INSTRUCT
        )
        presenter["minimax_voice_preset_id"] = OPENNEWS_MINIMAX_FALLBACK_JAPANESE_FEMALE_VOICE_PRESET_ID
        presenter["voice_preset_id"] = "japanese_female"
        return presenter
    if target_market == "en":
        presenter["qwen_speaker"] = (
            OPENNEWS_QWEN_TTS_ENGLISH_MALE_SPEAKER if gender == "male" else OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_SPEAKER
        )
        presenter["qwen_instruct"] = (
            OPENNEWS_QWEN_TTS_ENGLISH_MALE_INSTRUCT if gender == "male" else OPENNEWS_QWEN_TTS_ENGLISH_FEMALE_INSTRUCT
        )
        presenter["minimax_voice_preset_id"] = OPENNEWS_MINIMAX_FALLBACK_ENGLISH_FEMALE_VOICE_PRESET_ID
        presenter["voice_preset_id"] = "english_female"
        return presenter
    if target_market == "tw":
        presenter["voice_preset_id"] = "taiwan_clone" if str(os.getenv("VOICE_TAIWAN_CLONE", "")).strip() else "taiwan_female"
    return presenter


def _normalize_opennews_presenter_config(config: Optional[dict]) -> dict:
    if not isinstance(config, dict):
        return _opennews_presenter_config("female")
    gender = str(config.get("gender") or "female").strip().lower()
    base = _opennews_presenter_config(gender)
    for key in ("qwen_speaker", "qwen_instruct", "minimax_voice_preset_id", "voice_preset_id", "anchor_path", "anchor_filename"):
        value = str(config.get(key) or "").strip()
        if value:
            base[key] = value
    return base


def _next_opennews_batch_presenter_config() -> dict:
    with OPENNEWS_PRESENTER_STATE_LOCK:
        state: dict = {}
        try:
            if OPENNEWS_PRESENTER_STATE_PATH.exists():
                state = json.loads(OPENNEWS_PRESENTER_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            state = {}
        last_gender = str(state.get("last_gender") or "").strip().lower()
        next_gender = "male" if last_gender == "female" else "female"
        next_count = int(state.get("batch_count") or 0) + 1
        updated = {
            "last_gender": next_gender,
            "batch_count": next_count,
            "updated_at": time.time(),
        }
        try:
            OPENNEWS_PRESENTER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = OPENNEWS_PRESENTER_STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(OPENNEWS_PRESENTER_STATE_PATH)
        except Exception as exc:
            print(f"[opennews_presenter] failed to persist presenter state: {exc}", flush=True)
        config = _opennews_presenter_config(next_gender)
        config["batch_count"] = next_count
        return config


def _opennews_presenter_state_snapshot() -> dict:
    with OPENNEWS_PRESENTER_STATE_LOCK:
        state: dict = {}
        try:
            if OPENNEWS_PRESENTER_STATE_PATH.exists():
                state = json.loads(OPENNEWS_PRESENTER_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    last_gender = str(state.get("last_gender") or "").strip().lower()
    if last_gender not in {"female", "male"}:
        last_gender = ""
    next_gender = "male" if last_gender == "female" else "female"
    try:
        batch_count = int(state.get("batch_count") or 0)
    except Exception:
        batch_count = 0
    try:
        updated_at = float(state.get("updated_at") or 0)
    except Exception:
        updated_at = 0
    next_config = _opennews_presenter_config(next_gender)
    last_config = _opennews_presenter_config(last_gender) if last_gender else {}
    return {
        "last_gender": last_gender,
        "last_label": last_config.get("label") or "",
        "next_gender": next_gender,
        "next_label": next_config.get("label") or "",
        "batch_count": batch_count,
        "updated_at": updated_at,
    }


def _switch_5090_gpu_profile(profile: str, *, reason: str = "") -> dict:
    profile = str(profile or "").strip().lower()
    if not GPU_ORCHESTRATOR_ENABLED or not GPU_ORCHESTRATOR_URL or not profile:
        return {"ok": False, "skipped": True, "reason": "orchestrator_disabled"}
    try:
        with GPU_RESOURCE_LOCK:
            response = requests.post(
                f"{GPU_ORCHESTRATOR_URL}/profile",
                headers={"X-Token": GPU_ORCHESTRATOR_TOKEN, "Content-Type": "application/json"},
                json={"profile": profile, "reason": reason},
                timeout=GPU_ORCHESTRATOR_TIMEOUT_SECONDS,
            )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            print(f"[gpu_orchestrator] profile={profile} returned non-ok: {payload}", flush=True)
        return payload
    except Exception as exc:
        print(f"[gpu_orchestrator] failed to switch profile={profile}: {exc}", flush=True)
        return {"ok": False, "error": str(exc), "profile": profile}


def _gpu_resource_snapshot() -> dict:
    with GPU_RESOURCE_CONDITION:
        active = dict(GPU_RESOURCE_ACTIVE_JOB or {})
        waiting = [dict(item) for item in GPU_RESOURCE_WAITING_JOBS]
    return {
        "active": active,
        "waiting": waiting,
        "active_kind": active.get("kind", ""),
        "active_label": active.get("label", ""),
        "waiting_count": len(waiting),
    }


def _run_with_5090_gpu_resource(
    *,
    kind: str,
    label: str,
    task_id: str = "",
    profile: str = "",
    reason: str = "",
    log=None,
    runner,
):
    global GPU_RESOURCE_ACTIVE_JOB

    task_key = str(task_id or "")
    job_id = f"gpu:{kind}:{task_key or 'manual'}:{time.time_ns()}"
    task = tasks.get(task_key, {}) if task_key and isinstance(tasks, dict) else {}
    queue_item = {
        "job_id": job_id,
        "kind": kind,
        "label": label,
        "task_id": task_key,
        "topic": task.get("topic", ""),
        "mode": task.get("mode", ""),
        "owner_username": task.get("owner_username", ""),
        "owner_display_name": task.get("owner_display_name") or task.get("owner_username") or "",
        "created_at": time.time(),
    }
    waiting_logged = False
    with GPU_RESOURCE_CONDITION:
        GPU_RESOURCE_WAITING_JOBS.append(queue_item)
        while True:
            if task_key and _is_task_cancel_requested(task_key):
                GPU_RESOURCE_WAITING_JOBS[:] = [
                    item for item in GPU_RESOURCE_WAITING_JOBS if item.get("job_id") != job_id
                ]
                GPU_RESOURCE_CONDITION.notify_all()
                raise TaskCancelled(f"已停止当前任务，未继续等待 5090 GPU 资源：{label}")
            try:
                ahead = next(
                    (idx for idx, item in enumerate(GPU_RESOURCE_WAITING_JOBS) if item.get("job_id") == job_id),
                    0,
                )
            except ValueError:
                ahead = 0
            if ahead == 0 and not GPU_RESOURCE_ACTIVE_JOB:
                GPU_RESOURCE_WAITING_JOBS.pop(0)
                GPU_RESOURCE_ACTIVE_JOB = dict(queue_item)
                break
            if log and not waiting_logged:
                active_label = (GPU_RESOURCE_ACTIVE_JOB or {}).get("label") or "其他 5090 任务"
                log(f"{label}等待 5090 GPU 资源，当前占用：{active_label}，前方还有 {ahead} 个任务")
                waiting_logged = True
            GPU_RESOURCE_CONDITION.wait(timeout=2)

    try:
        if log and waiting_logged:
            log(f"{label}开始占用 5090 GPU")
        if task_key:
            _raise_if_task_cancel_requested(task_key, f"已停止当前任务，未继续执行 5090 GPU 任务：{label}")
        with GPU_RESOURCE_LOCK:
            if profile:
                _switch_5090_gpu_profile(profile, reason=reason or label)
            return runner()
    finally:
        with GPU_RESOURCE_CONDITION:
            GPU_RESOURCE_ACTIVE_JOB = None
            GPU_RESOURCE_CONDITION.notify_all()


def _run_with_5090_digital_profile(engine_id: str, *, task_id: str = "", segment_index: int = 0, runner):
    if engine_id not in {INFINITETALK_ENGINE_ID, HUNYUAN_ENGINE_ID}:
        return runner()
    reason = f"{engine_id} task={task_id or '-'} segment={segment_index}"

    def _runner_with_cancel_mapping():
        try:
            return _run_with_5090_digital_restore(reason, runner)
        except TaskCancelled:
            raise
        except Exception as exc:
            if task_id and _is_task_cancel_requested(task_id):
                raise TaskCancelled("已停止当前任务，5090 数字人生成已取消") from exc
            raise

    return _run_with_5090_gpu_resource(
        kind="digital_human",
        label=f"5090 InfiniteTalk 数字人（第 {segment_index or '-'} 段）",
        task_id=task_id,
        profile=LOCAL_DIGITAL_HUMAN_GPU_PROFILE,
        reason=reason,
        runner=_runner_with_cancel_mapping,
    )


def _run_with_5090_digital_restore(reason: str, runner):
    try:
        return runner()
    finally:
        # keep-warm:数字人任务做完后不切回 restore 档(保持 InfiniteTalk 常驻),
        # 避免连续数字人段/条之间反复 stop/start InfiniteTalk(打断在跑的生成 + 重载模型)。
        # 后面若有 TTS 任务需要显存,那个任务会自行切档停掉 InfiniteTalk。
        if _digital_keep_warm_enabled():
            return
        if LOCAL_DIGITAL_HUMAN_RESTORE_PROFILE:
            _switch_5090_gpu_profile(LOCAL_DIGITAL_HUMAN_RESTORE_PROFILE, reason=f"finished {reason}")


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


def _sync_material_item_to_vector_library(item: dict) -> None:
    if not MATERIAL_VECTOR_SYNC_ENABLED or not MATERIAL_VECTOR_SERVICE_URL:
        return
    if str(item.get("kind") or "").lower() != "image":
        return
    filename = Path(str(item.get("filename") or "")).name
    file_path = (MATERIAL_LIBRARY_DIR / filename).resolve()
    library_root = MATERIAL_LIBRARY_DIR.resolve()
    if not str(file_path).startswith(str(library_root)) or not file_path.exists():
        return
    material_id = f"prod_{item.get('id') or file_path.stem}"
    title = " | ".join(
        part
        for part in [
            str(item.get("title") or file_path.stem),
            str(item.get("category") or ""),
            " ".join(map(str, item.get("tags") or [])),
            " ".join(map(str, item.get("news_topics") or [])),
        ]
        if part.strip()
    )
    try:
        with file_path.open("rb") as handle:
            response = requests.post(
                f"{MATERIAL_VECTOR_SERVICE_URL}/analyze-upload",
                data={"material_id": material_id, "title": title},
                files={"file": (filename, handle, "application/octet-stream")},
                timeout=360,
            )
        response.raise_for_status()
        payload = response.json()
        analysis = payload.get("analysis") if isinstance(payload, dict) else {}
        if isinstance(analysis, dict):
            ai_tags = []
            for key in ("category", "entities", "scenes", "concepts", "visible_text"):
                value = analysis.get(key)
                if isinstance(value, list):
                    ai_tags.extend(str(item or "") for item in value if str(item or "").strip())
                elif value:
                    ai_tags.append(str(value))
            update_material_library_item(
                str(item.get("id") or ""),
                {
                    "ai_provider": "5090-qwen3-vl-bge-m3",
                    "ai_summary": str(analysis.get("description") or "").strip(),
                    "ai_tags": ai_tags,
                    "safety_status": str(analysis.get("safety_status") or item.get("safety_status") or "safe"),
                },
            )
    except Exception as exc:
        print(f"  ⚠️ 素材向量库同步失败：{item.get('id') or filename}: {exc}")


def _sync_material_item_to_vector_library_async(item: dict) -> None:
    threading.Thread(
        target=_sync_material_item_to_vector_library,
        args=(dict(item or {}),),
        daemon=True,
        name=f"material-vector-sync-{item.get('id') or uuid.uuid4().hex[:6]}",
    ).start()


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


def _cancel_active_infinitetalk_jobs(task_id: str) -> dict:
    normalized = str(task_id or "").strip()
    if not normalized:
        return {"ok": False, "skipped": True, "reason": "missing_task_id"}
    try:
        from infinitetalk_avatar_client import cancel_infinitetalk_jobs
    except Exception as exc:
        return {"ok": False, "error": f"import_failed: {exc}"}
    try:
        result = cancel_infinitetalk_jobs(normalized)
        jobs = result.get("jobs") if isinstance(result, dict) else []
        if isinstance(result, dict) and isinstance(jobs, list) and "cancelled_job_ids" not in result:
            result["cancelled_job_ids"] = [str(item.get("job_id") or "") for item in jobs if isinstance(item, dict)]
        return result
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _history_id_from_output_dir(output_dir: Optional[str]) -> str:
    if not output_dir:
        return ""
    return Path(output_dir).resolve().name


def _get_target_market(target_market_id: Optional[str]) -> dict:
    for market in TARGET_MARKETS:
        if market["id"] == target_market_id:
            return dict(market)
    return dict(TARGET_MARKETS[0])


OPENNEWS_EXTRA_TARGET_MARKET_IDS = ("jp", "en")


def _normalize_opennews_extra_target_markets(raw: Any, primary_target_market: str = "cn") -> list[str]:
    primary = str(primary_target_market or "cn").strip().lower() or "cn"
    if isinstance(raw, str):
        requested = [part.strip().lower() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, (list, tuple, set)):
        requested = [str(part or "").strip().lower() for part in raw if str(part or "").strip()]
    else:
        requested = []
    valid_market_ids = {item["id"] for item in TARGET_MARKETS}
    normalized: list[str] = []
    for market_id in requested:
        if market_id == primary:
            continue
        if market_id not in valid_market_ids:
            continue
        if market_id not in OPENNEWS_EXTRA_TARGET_MARKET_IDS:
            continue
        if market_id not in normalized:
            normalized.append(market_id)
    return normalized


def _opennews_multilingual_enabled() -> bool:
    return _env_flag("OPENNEWS_MULTI_LANGUAGE_ENABLED", "1")


def _opennews_extra_target_markets_for_primary(primary_target_market: str, configured: Any = None) -> list[str]:
    if configured is None:
        configured = os.getenv("OPENNEWS_MULTI_LANGUAGE_TARGET_MARKETS", ",".join(OPENNEWS_EXTRA_TARGET_MARKET_IDS))
    normalized = _normalize_opennews_extra_target_markets(configured, primary_target_market)
    if normalized:
        return normalized
    primary = str(primary_target_market or "cn").strip().lower() or "cn"
    return [market_id for market_id in OPENNEWS_EXTRA_TARGET_MARKET_IDS if market_id != primary]


def _language_version_group_id() -> str:
    return f"opennews_lang_{int(time.time())}_{uuid.uuid4().hex[:10]}"


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
    base_ids = {"ricky_clone"}
    if target_market_id == "tw":
        return {"mandarin_female", "mandarin_male", "taiwan_female", "taiwan_clone", "japanese_female", "english_female"} | base_ids
    if target_market_id == "jp":
        return {"mandarin_female", "mandarin_male", "japanese_female", "english_female"} | base_ids
    if target_market_id == "en":
        return {"english_female", "japanese_female", "mandarin_female", "mandarin_male"} | base_ids
    return {"mandarin_female", "mandarin_male", "taiwan_clone", "english_female"} | base_ids


def _is_avatar_voice_compatible(avatar_option: Optional[dict], voice_preset: Optional[dict]) -> bool:
    if not avatar_option or not voice_preset:
        return True
    avatar_gender = (avatar_option.get("gender") or "").strip().lower()
    voice_gender = (voice_preset.get("gender") or "").strip().lower()
    if not avatar_gender or not voice_gender:
        return True
    return avatar_gender == voice_gender


def _allows_local_digital_human_engine(user: Optional[dict]) -> bool:
    if not isinstance(user, dict):
        return False
    workflow_config = user.get("workflow_config") or {}
    return bool(user.get("allow_local_digital_human") or workflow_config.get("allow_local_digital_human"))


def _normalize_digital_human_engine(engine_id: str | None, user: Optional[dict] = None) -> str:
    requested = (engine_id or INFINITETALK_ENGINE_ID).strip()
    if requested == "opennews_material_only":
        return "opennews_material_only"
    return INFINITETALK_ENGINE_ID


def _digital_human_engine_label(engine_id: str | None) -> str:
    if (engine_id or "").strip() == "opennews_material_only":
        return "无数字人（素材成片）"
    normalized = _normalize_digital_human_engine(engine_id, {"role": "admin"})
    for item in DIGITAL_HUMAN_ENGINES:
        if item["id"] == normalized:
            return item["name"]
    return "5090 本地 InfiniteTalk"


def _digital_human_engine_options_for_user(user: Optional[dict]) -> list[dict]:
    return [dict(item) for item in DIGITAL_HUMAN_ENGINES]


def _local_qwen_script_model_available() -> bool:
    disabled = str(os.getenv("LOCAL_QWEN_DISABLED") or "").strip().lower()
    if disabled in {"1", "true", "yes", "on"}:
        return False
    return bool(
        str(
            os.getenv("LOCAL_QWEN_BASE_URL")
            or os.getenv("OPENNEWS_LOCAL_LLM_BASE_URL")
            or os.getenv("OPENNEWS_QWEN_LLM_BASE_URL")
            or "http://192.168.0.34:11434/v1"
        ).strip()
    )


def _forced_script_model_provider() -> str:
    requested = ""
    for env_key in SCRIPT_MODEL_FORCE_ENV_KEYS:
        requested = str(os.getenv(env_key) or "").strip().lower()
        if requested:
            break
    if requested in {"api", "relay", "api_relay", "openai_relay", "office_api", "unified"}:
        return SCRIPT_MODEL_API_RELAY
    if requested in {"local", "local_qwen", "qwen", "qwen_local", "ollama"}:
        return SCRIPT_MODEL_LOCAL_QWEN
    if requested in {"claude", "anthropic"}:
        return SCRIPT_MODEL_CLAUDE
    return ""


def _normalize_script_model(model_id: str | None, user: Optional[dict] = None) -> str:
    forced = _forced_script_model_provider()
    if forced:
        return forced
    requested = str(model_id or "").strip().lower()
    if requested == SCRIPT_MODEL_LOCAL_QWEN and _is_admin(user) and _local_qwen_script_model_available():
        return SCRIPT_MODEL_LOCAL_QWEN
    if requested == SCRIPT_MODEL_API_RELAY and _is_admin(user):
        return SCRIPT_MODEL_API_RELAY
    return SCRIPT_MODEL_CLAUDE


def _script_model_label(model_id: str | None) -> str:
    normalized = _normalize_script_model(model_id, {"role": "admin"} if model_id == SCRIPT_MODEL_API_RELAY else None)
    if _forced_script_model_provider() == SCRIPT_MODEL_API_RELAY:
        return os.getenv("FORCE_SCRIPT_MODEL_LABEL", "统一接口模型").strip() or "统一接口模型"
    for item in SCRIPT_MODEL_OPTIONS:
        if item["id"] == normalized:
            return item["name"]
    return "Claude"


def _script_model_options_for_user(user: Optional[dict]) -> list[dict]:
    forced = _forced_script_model_provider()
    if forced:
        for item in SCRIPT_MODEL_OPTIONS:
            if item["id"] == forced:
                payload = dict(item)
                payload["admin_only"] = False
                payload["default"] = True
                if forced == SCRIPT_MODEL_API_RELAY:
                    payload["name"] = os.getenv("FORCE_SCRIPT_MODEL_LABEL", "统一接口模型").strip() or "统一接口模型"
                    payload["description"] = os.getenv(
                        "FORCE_SCRIPT_MODEL_DESCRIPTION",
                        "所有文案生成统一走 api.office.ihousejapan.cn 接口模型。",
                    ).strip() or "所有文案生成统一走统一接口模型。"
                return [payload]
        return []
    items: list[dict] = []
    for item in SCRIPT_MODEL_OPTIONS:
        if item["id"] == SCRIPT_MODEL_LOCAL_QWEN and not _local_qwen_script_model_available():
            continue
        if item.get("admin_only") and not _is_admin(user):
            continue
        items.append(dict(item))
    return items


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
    error_message = str((result or {}).get("error") or "").strip()
    if (
        not error_message
        and output_dir
        and _history_is_opennews_result(result)
        and has_script
        and not compose_ready
        and not _opennews_result_has_material_assets({"segments": segments}, output_dir)
    ):
        try:
            _ensure_opennews_fallback_material_assets(result or {}, output_dir)
        except Exception as exc:
            print(f"[opennews material fallback] lifecycle recovery failed: {exc!r}", flush=True)
        if not _opennews_result_has_material_assets({"segments": segments}, output_dir):
            error_message = "OpenNews 成片中止：免费素材 API 没有拿到可用素材，新闻图卡保底也未生成成功。请稍后重试。"

    if compose_ready:
        status = "completed"
        stage_key = "compose"
    elif error_message:
        status = "failed"
        if materials_ready:
            stage_key = "compose"
        elif digital_human_ready:
            stage_key = "materials"
        elif audio_ready:
            stage_key = "digital_human"
        elif has_script:
            stage_key = "audio"
        else:
            stage_key = "script"
    elif materials_ready and digital_human_ready:
        status = "ready_compose"
        stage_key = "compose"
    elif audio_ready and not digital_human_ready:
        status = "interrupted"
        stage_key = "digital_human"
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
        "can_resume_production": bool(has_script and not (materials_ready and digital_human_ready) and not compose_ready),
        "can_compose": bool(materials_ready and digital_human_ready and not compose_ready),
        "live_task_id": "",
        "error_message": error_message,
    }


def _history_is_opennews_result(result: Optional[dict]) -> bool:
    payload = result if isinstance(result, dict) else {}
    workflow_config = payload.get("workflow_config") if isinstance(payload.get("workflow_config"), dict) else {}
    return bool(
        workflow_config.get("opennews")
        or workflow_config.get("opennews_material_only")
        or str(workflow_config.get("digital_human_engine") or "") == "opennews_material_only"
        or str(payload.get("topic") or "").startswith("OpenNews")
    )


def _write_history_result(output_dir: Path, result: dict) -> None:
    _save_result_to_output_dir(output_dir, result)


def _compose_history_result(
    output_dir: Path,
    result: dict,
    *,
    user: Optional[dict] = None,
    requested_aspect_ratio: str = "",
    cost_scope: str = "manual_history_compose",
) -> dict:
    workflow_config = result.get("workflow_config") or {}
    requested_aspect_ratio = str(requested_aspect_ratio or "").strip().lower()
    if requested_aspect_ratio not in {"vertical", "horizontal"}:
        requested_aspect_ratio = ""

    is_opennews_result = _history_is_opennews_result(result)
    default_aspect_ratio = "horizontal" if is_opennews_result else "vertical"
    compose_aspect_ratio = str(
        requested_aspect_ratio
        or workflow_config.get("compose_aspect_ratio")
        or workflow_config.get("aspect_ratio")
        or default_aspect_ratio
    ).strip().lower()
    if compose_aspect_ratio not in {"vertical", "horizontal"}:
        compose_aspect_ratio = default_aspect_ratio

    transition_id = str(workflow_config.get("compose_transition_id") or "fade")
    subtitle_template_id = str(workflow_config.get("subtitle_template_id") or "classic")
    if is_opennews_result:
        subtitle_template_id = "property_clear"
        try:
            composed_result = _compose_opennews_result(
                output_dir,
                result,
                preferred_aspect_ratio=compose_aspect_ratio,
                user=user,
                cost_scope=cost_scope,
            )
        except Exception as exc:
            result["error"] = str(exc)
            try:
                result["material_review"] = _opennews_material_review_status(result, output_dir)
            except Exception:
                pass
            _write_history_result(output_dir, result)
            _sync_live_task_result(str(output_dir), result)
            raise
    else:
        from video_composer import compose_history_video

        composed_payload = compose_history_video(
            str(output_dir),
            result,
            transition_id=transition_id,
            subtitle_template_id=subtitle_template_id,
            aspect_ratio=compose_aspect_ratio,
        )
        workflow_config["compose_transition_id"] = transition_id
        workflow_config["subtitle_template_id"] = subtitle_template_id
        workflow_config["compose_aspect_ratio"] = compose_aspect_ratio
        result["workflow_config"] = workflow_config
        result.update(composed_payload)
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
                "generated_aspect_ratios": [compose_aspect_ratio],
                "scope": cost_scope,
            },
        )
        composed_result = result

    _write_history_result(output_dir, composed_result)
    _sync_live_task_result(str(output_dir), composed_result)
    return composed_result


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
    engine_id = _normalize_digital_human_engine(engine_id, {"role": "admin"})
    if engine_id == "opennews_material_only":
        raise RuntimeError("素材成片任务不应进入数字人生成")
    if engine_id == HUNYUAN_ENGINE_ID:
        from hunyuan_avatar_client import generate_hunyuan_avatar_video

        return _run_with_5090_digital_profile(
            engine_id,
            task_id=task_id,
            segment_index=segment_index,
            runner=lambda: generate_hunyuan_avatar_video(
                image_path=image_path,
                audio_path=audio_path,
                output_path=output_path,
                prompt=prompt,
                external_task_id=task_id,
                segment_index=segment_index,
            ),
        )
    if engine_id == INFINITETALK_ENGINE_ID:
        from infinitetalk_avatar_client import generate_infinitetalk_avatar_video

        return _run_with_5090_digital_profile(
            engine_id,
            task_id=task_id,
            segment_index=segment_index,
            runner=lambda: generate_infinitetalk_avatar_video(
                image_path=image_path,
                audio_path=audio_path,
                output_path=output_path,
                prompt=prompt,
                external_task_id=task_id,
                segment_index=segment_index,
            ),
        )

    raise RuntimeError(f"不支持的数字人引擎：{engine_id}")


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
        from generate_script import generate_script
        from tos_uploader import upload_file_and_get_url

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
        opennews_persistence_config = {
            "opennews_channel_id": workflow_config.get("opennews_channel_id") or "",
            "opennews_channel_name": workflow_config.get("opennews_channel_name") or "",
            "opennews_language_markets": workflow_config.get("opennews_language_markets"),
            "opennews_presenter": workflow_config.get("opennews_presenter") or {},
            "material_strategy": workflow_config.get("material_strategy") or "",
            "batch_job_id": workflow_config.get("batch_job_id") or "",
            "external_produce_managed": bool(workflow_config.get("external_produce_managed")),
            "x_auto_publish": _parse_bool_form(workflow_config.get("x_auto_publish")) if "x_auto_publish" in workflow_config else _opennews_x_auto_publish_default(),
            "facebook_auto_publish": _parse_bool_form(workflow_config.get("facebook_auto_publish")) if "facebook_auto_publish" in workflow_config else _opennews_facebook_auto_publish_default(),
            "youtube_auto_publish": _parse_bool_form(workflow_config.get("youtube_auto_publish")) if "youtube_auto_publish" in workflow_config else _opennews_youtube_auto_publish_default(),
            "x_aspects": workflow_config.get("x_aspects") or ["vertical"],
            "facebook_aspects": workflow_config.get("facebook_aspects") or ["vertical"],
            "youtube_aspects": workflow_config.get("youtube_aspects") or ["vertical"],
        }

        output_dir = _create_output_dir("full", topic)
        task["output_dir"] = output_dir
        if opennews_persistence_config.get("external_produce_managed"):
            _mark_opennews_external_produce_active(
                Path(output_dir),
                task_id=task_id,
                batch_job_id=str(opennews_persistence_config.get("batch_job_id") or ""),
            )

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
                "allow_local_digital_human": bool(workflow_config.get("allow_local_digital_human")),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
                **opennews_persistence_config,
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
                task_id=task_id,
            )
            seg_with_audio = dict(seg)
            seg_with_audio["audio_path"] = audio_path
            try:
                seg_with_audio["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
            except Exception as _audio_upload_exc:
                # 火山 TOS 上传失败不应中断配音流程：本地合成只需 audio_path，
                # audio_url 仅用于远程预览，缺失可容忍。之前此处异常会中断整段配音循环，
                # 导致段落 audio_path 未回填 -> 合成报"没有可用的配音文件"。
                seg_with_audio["audio_url"] = ""
                print(f"[opennews audio] TOS 上传失败(不影响合成) seg={index}: {_audio_upload_exc!r}", flush=True)
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
                "allow_local_digital_human": bool(workflow_config.get("allow_local_digital_human")),
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
                **opennews_persistence_config,
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
                tracker.log("OpenNews 免费素材 API 未命中，成片前将生成新闻图卡保底素材。")
            tracker.log(f"素材匹配完成，共 {material_group_count} 组素材")
        except TaskCancelled:
            raise
        except Exception as exc:
            if workflow_config.get("opennews") or workflow_config.get("opennews_material_only") or digital_human_engine == "opennews_material_only":
                tracker.log(f"OpenNews 素材匹配异常：{exc}；已按宽松策略继续成片，避免影响 X/Facebook 自动发布。")
                final_segments = segments_with_dh
                for seg in final_segments:
                    if isinstance(seg, dict):
                        seg["material_warning"] = str(exc)
                        seg.setdefault("material_paths", [])
                material_group_count = 0
                tracker.log(f"素材匹配降级完成，共 {material_group_count} 组素材")
            else:
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
                "compose_aspect_ratio": workflow_config.get("compose_aspect_ratio") or workflow_config.get("aspect_ratio") or "vertical",
                "source": workflow_config.get("source") or {},
                "opennews": bool(workflow_config.get("opennews")),
                "opennews_material_only": bool(workflow_config.get("opennews_material_only")),
                "auto_publish_to_x_and_facebook": bool(workflow_config.get("auto_publish_to_x_and_facebook")),
                "allow_local_digital_human": bool(workflow_config.get("allow_local_digital_human")),
                "x_auto_publish": _parse_bool_form(workflow_config.get("x_auto_publish")) if "x_auto_publish" in workflow_config else _opennews_x_auto_publish_default(),
                "facebook_auto_publish": _parse_bool_form(workflow_config.get("facebook_auto_publish")) if "facebook_auto_publish" in workflow_config else _opennews_facebook_auto_publish_default(),
                "x_aspects": workflow_config.get("x_aspects") or ["vertical"],
                "facebook_aspects": workflow_config.get("facebook_aspects") or ["vertical"],
                "digital_human_engine": digital_human_engine,
                "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
                "opennews_channel_id": workflow_config.get("opennews_channel_id") or "",
                "opennews_channel_name": workflow_config.get("opennews_channel_name") or "",
                "opennews_language_markets": workflow_config.get("opennews_language_markets"),
                "opennews_presenter": workflow_config.get("opennews_presenter") or {},
                "material_strategy": workflow_config.get("material_strategy") or "",
                "batch_job_id": workflow_config.get("batch_job_id") or "",
                **opennews_persistence_config,
            },
            "cost_entries": task.get("cost_entries", []),
            "cost_summary": task.get("cost_summary", _empty_cost_summary()),
        }
        if (
            result_data.get("workflow_config", {}).get("opennews")
            or result_data.get("workflow_config", {}).get("opennews_material_only")
            or result_data.get("workflow_config", {}).get("digital_human_engine") == "opennews_material_only"
        ):
            try:
                if _ensure_opennews_fallback_material_assets(result_data, Path(output_dir)):
                    tracker.log("已生成 OpenNews 新闻图卡保底素材，继续成片和自动发布。")
            except Exception as exc:
                tracker.log(f"OpenNews 新闻图卡保底素材生成失败：{exc}")

        if (
            _opennews_multilingual_enabled()
            and (
                workflow_config.get("opennews")
                or workflow_config.get("opennews_material_only")
                or digital_human_engine == "opennews_material_only"
            )
        ):
            configured_markets = workflow_config.get("opennews_language_markets")
            if configured_markets is not None:
                extra_market_ids = _normalize_opennews_extra_target_markets(configured_markets, target_market)
            else:
                extra_market_ids = _opennews_extra_target_markets_for_primary(target_market)
            language_versions: list[dict] = []
            if extra_market_ids:
                tracker.log(f"正在派生多语言版本：{' / '.join(extra_market_ids)}...", step=5)
            for extra_market_id in extra_market_ids:
                try:
                    version_payload = _build_opennews_language_script_only_version(
                        source_topic=topic,
                        source_script=script_data,
                        source_segments=final_segments,
                        primary_workflow_config=result_data.get("workflow_config") or {},
                        target_market=extra_market_id,
                        department_id=department_id,
                        provider=script_model,
                    )
                    language_versions.append(version_payload)
                except Exception as exc:
                    language_versions.append(
                        {
                            "target_market": extra_market_id,
                            "error": str(exc),
                        }
                    )
            if language_versions:
                result_data["language_version_group_id"] = _language_version_group_id()
                result_data["language_versions"] = language_versions


        task["result"] = result_data
        _persist_task_result(task)
        tracker.finish(result_data)
        if (
            result_data.get("workflow_config", {}).get("opennews")
            or result_data.get("workflow_config", {}).get("opennews_material_only")
            or result_data.get("workflow_config", {}).get("digital_human_engine") == "opennews_material_only"
        ) and not result_data.get("workflow_config", {}).get("external_produce_managed"):
            tracker.log("OpenNews 成片已保存，正在后台自动发布到 X / Facebook / YouTube...")
            _schedule_opennews_post_compose_publish(task_id, output_dir, result_data)
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
        # 房源自动化任务：做完自动发 YouTube（与话题共用账号，只发 Shorts）+ Facebook（共用话题 Page，中文单帖）
        if (task.get("workflow_config") or {}).get("property_auto"):
            try:
                _maybe_publish_property_video_to_youtube(Path(task["output_dir"]), result, title_hint=task.get("topic") or "")
            except Exception as _prop_pub_exc:
                print(f"[property-auto youtube] hook error: {_prop_pub_exc!r}", flush=True)
            try:
                _maybe_publish_property_video_to_facebook(Path(task["output_dir"]), result, title_hint=task.get("topic") or "")
            except Exception as _prop_fb_exc:
                print(f"[property-auto facebook] hook error: {_prop_fb_exc!r}", flush=True)
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


def _apply_opennews_material_strategy(script_data: dict, *, strategy: str = "", batch_job_id: str = "") -> dict:
    applied_strategy = str(strategy or "").strip().lower()
    applied_batch_job_id = str(batch_job_id or "").strip()
    if not isinstance(script_data, dict):
        return script_data
    segments = script_data.get("segments") or []
    if not isinstance(segments, list):
        return script_data
    for segment in segments:
        if not isinstance(segment, dict) or segment.get("type") != "material":
            continue
        if applied_strategy:
            segment["material_strategy"] = applied_strategy
        if applied_batch_job_id:
            segment["batch_job_id"] = applied_batch_job_id
    return script_data


def run_resume_pipeline_with_progress(task_id: str):
    tracker = tasks[task_id]["tracker"]
    try:
        _raise_if_task_cancel_requested(task_id)
        from generate_audio import generate_audio
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
                try:
                    seg["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
                except Exception as _seg_audio_upload_exc:
                    seg["audio_url"] = seg.get("audio_url", "")
                    print(f"[opennews audio] TOS 上传失败(不影响合成): {_seg_audio_upload_exc!r}", flush=True)
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
                    task_id=task_id,
                )
                seg["audio_path"] = audio_path
                try:
                    seg["audio_url"] = upload_file_and_get_url(audio_path, key_prefix="full/audio")
                except Exception as _seg_audio_upload_exc:
                    seg["audio_url"] = seg.get("audio_url", "")
                    print(f"[opennews audio] TOS 上传失败(不影响合成): {_seg_audio_upload_exc!r}", flush=True)
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
            "allow_local_digital_human": bool(workflow_config.get("allow_local_digital_human")),
            "auto_publish_to_x_and_facebook": bool(workflow_config.get("auto_publish_to_x_and_facebook")) or bool(workflow_config.get("opennews")) or bool(workflow_config.get("opennews_material_only")),
            "x_auto_publish": _parse_bool_form(workflow_config.get("x_auto_publish")) if "x_auto_publish" in workflow_config else _opennews_x_auto_publish_default(),
            "facebook_auto_publish": _parse_bool_form(workflow_config.get("facebook_auto_publish")) if "facebook_auto_publish" in workflow_config else _opennews_facebook_auto_publish_default(),
            "x_aspects": workflow_config.get("x_aspects") or ["vertical"],
            "facebook_aspects": workflow_config.get("facebook_aspects") or ["vertical"],
            "digital_human_engine": digital_human_engine,
            "digital_human_engine_name": _digital_human_engine_label(digital_human_engine),
        }
        if (
            result.get("workflow_config", {}).get("opennews")
            or result.get("workflow_config", {}).get("opennews_material_only")
            or result.get("workflow_config", {}).get("digital_human_engine") == "opennews_material_only"
        ):
            try:
                if _ensure_opennews_fallback_material_assets(result, Path(output_dir)):
                    tracker.log("已生成 OpenNews 新闻图卡保底素材，继续成片和自动发布。")
            except Exception as exc:
                tracker.log(f"OpenNews 新闻图卡保底素材生成失败：{exc}")
        result["cost_entries"] = task.get("cost_entries", result.get("cost_entries", []))
        result["cost_summary"] = task.get("cost_summary", result.get("cost_summary", _empty_cost_summary()))

        task["result"] = result
        _persist_task_result(task)
        tracker.finish(result)
        if (
            result.get("workflow_config", {}).get("opennews")
            or result.get("workflow_config", {}).get("opennews_material_only")
            or result.get("workflow_config", {}).get("digital_human_engine") == "opennews_material_only"
        ):
            tracker.log("OpenNews 继续生产已完成，正在后台自动发布到 X / Facebook...")
            _schedule_opennews_post_compose_publish(task_id, output_dir, result)
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
            label=f"数字人单段测试：{_digital_human_engine_label(INFINITETALK_ENGINE_ID)}",
            tracker=tracker,
            runner=lambda: _generate_digital_human_video_by_engine(
                engine_id=INFINITETALK_ENGINE_ID,
                image_url=image_url,
                image_path=image_path,
                audio_url=audio_url,
                audio_path=audio_path,
                output_path=os.path.join(video_dir, "avatar_test.mp4"),
                prompt="",
                task_id=task_id,
                segment_index=1,
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
            "workflow_config": {
                "digital_human_engine": INFINITETALK_ENGINE_ID,
                "digital_human_engine_name": _digital_human_engine_label(INFINITETALK_ENGINE_ID),
                "tts_provider": COST_RULES["tts_generate"]["provider"],
            },
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


def _qwen_tts_queue_snapshot() -> dict:
    with QWEN_TTS_QUEUE_CONDITION:
        running = [dict(item) for item in QWEN_TTS_RUNNING_ITEMS]
        waiting = [dict(item) for item in QWEN_TTS_WAITING_JOBS]
    return {
        "max_concurrent": QWEN_TTS_MAX_CONCURRENT,
        "running_count": len(running),
        "waiting_count": len(waiting),
        "running": running,
        "waiting": waiting,
        "current_owner_username": running[0].get("owner_username") if running else "",
        "current_owner_display_name": running[0].get("owner_display_name") if running else "",
    }


def _cancel_waiting_qwen_tts_jobs(task_id: str) -> int:
    normalized = str(task_id or "")
    if not normalized:
        return 0
    with QWEN_TTS_QUEUE_CONDITION:
        before = len(QWEN_TTS_WAITING_JOBS)
        QWEN_TTS_WAITING_JOBS[:] = [
            item for item in QWEN_TTS_WAITING_JOBS if str(item.get("task_id") or "") != normalized
        ]
        removed = before - len(QWEN_TTS_WAITING_JOBS)
        if removed:
            QWEN_TTS_QUEUE_CONDITION.notify_all()
        return removed


def _qwen_tts_health_check() -> dict:
    if not OPENNEWS_QWEN_TTS_BASE_URL:
        raise RuntimeError("OpenNews Qwen3-TTS 未配置 base_url")
    response = requests.get(
        f"{OPENNEWS_QWEN_TTS_BASE_URL}/health",
        headers={"X-Token": OPENNEWS_QWEN_TTS_TOKEN},
        timeout=OPENNEWS_QWEN_TTS_HEALTH_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Qwen3-TTS health returned not ok: {payload}")
    return payload


def _recover_qwen_tts_service(reason: str = "") -> dict:
    if not OPENNEWS_QWEN_TTS_RESTART_ON_FAILURE:
        return {"ok": False, "skipped": True, "reason": "restart_disabled"}
    with GPU_RESOURCE_LOCK:
        profile_result = _switch_5090_gpu_profile("idle", reason=f"qwen3-tts recovery: {reason[:120]}")
        if profile_result.get("ok"):
            return profile_result
        return _switch_5090_gpu_profile("material", reason=f"qwen3-tts recovery fallback: {reason[:120]}")


def _qwen_tts_gpu_unavailable(recover_result: Any, exc: Exception | str = "") -> bool:
    text = str(exc or "").strip().lower()
    if any(token in text for token in ("gpu unavailable", "fallen off the bus", "xid", "unspecified launch failure")):
        return True
    if not isinstance(recover_result, dict):
        return False
    if "gpu-unavailable" in str(recover_result).lower():
        return True
    for item in recover_result.get("results") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("active") or "").strip().lower() == "gpu-unavailable":
            return True
        if "fallen off the bus" in str(item.get("stderr") or "").lower():
            return True
        readiness = item.get("readiness") if isinstance(item.get("readiness"), dict) else {}
        if str(readiness.get("error") or "").strip().lower() == "gpu-unavailable":
            return True
    return False


def _run_qwen_tts_job(
    *,
    task_id: str,
    label: str,
    target_market: str,
    speaker: str,
    runner,
    log=None,
):
    global QWEN_TTS_RUNNING_JOBS
    task_key = str(task_id or "")
    task = tasks.get(task_key, {}) if task_key and isinstance(tasks, dict) else {}
    job_id = f"qwen-tts:{task_key or 'manual'}:{time.time_ns()}"
    queue_item = {
        "job_id": job_id,
        "task_id": task_key,
        "label": label,
        "target_market": target_market,
        "speaker": speaker,
        "topic": task.get("topic", ""),
        "mode": task.get("mode", "opennews"),
        "owner_username": task.get("owner_username", ""),
        "owner_display_name": task.get("owner_display_name") or task.get("owner_username") or "",
        "created_at": time.time(),
    }
    waiting_logged = False
    with QWEN_TTS_QUEUE_CONDITION:
        QWEN_TTS_WAITING_JOBS.append(queue_item)
        while True:
            if task_key and _is_task_cancel_requested(task_key):
                QWEN_TTS_WAITING_JOBS[:] = [item for item in QWEN_TTS_WAITING_JOBS if item.get("job_id") != job_id]
                QWEN_TTS_QUEUE_CONDITION.notify_all()
                raise TaskCancelled("已停止当前任务，未继续等待 5090 Qwen3-TTS 配音")
            try:
                ahead = next((idx for idx, item in enumerate(QWEN_TTS_WAITING_JOBS) if item.get("job_id") == job_id), 0)
            except ValueError:
                ahead = 0
            can_run = ahead == 0 and QWEN_TTS_RUNNING_JOBS < QWEN_TTS_MAX_CONCURRENT
            if can_run:
                QWEN_TTS_WAITING_JOBS.pop(0)
                QWEN_TTS_RUNNING_JOBS += 1
                QWEN_TTS_RUNNING_ITEMS.append(queue_item)
                break
            if log and not waiting_logged:
                log(f"5090 Qwen3-TTS 配音排队中，前方还有 {ahead} 个配音任务")
                waiting_logged = True
            QWEN_TTS_QUEUE_CONDITION.wait(timeout=2)
    try:
        if log and waiting_logged:
            log("5090 Qwen3-TTS 配音开始执行")
        if task_key:
            _raise_if_task_cancel_requested(task_key, "已停止当前任务，未继续生成配音")
        return _run_with_5090_gpu_resource(
            kind="qwen_tts",
            label=label or "5090 Qwen3-TTS 配音",
            task_id=task_key,
            profile="material",
            reason=f"qwen3-tts task={task_key or '-'} label={label}",
            log=log,
            runner=lambda: (_qwen_tts_health_check(), runner())[1],
        )
    finally:
        with QWEN_TTS_QUEUE_CONDITION:
            QWEN_TTS_RUNNING_JOBS = max(0, QWEN_TTS_RUNNING_JOBS - 1)
            QWEN_TTS_RUNNING_ITEMS[:] = [item for item in QWEN_TTS_RUNNING_ITEMS if item.get("job_id") != job_id]
            QWEN_TTS_QUEUE_CONDITION.notify_all()


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
    # 确定性输入错误：音频过短/缺失、图片缺失等，重试再多也不会成功。
    # 必须先于下面的 retry_tokens 判断（否则会被 "infinitetalk" 关键词误判为可重试而无限重试）。
    non_retryable_tokens = [
        "length not satisfies frame nums",
        "aduio file not exists",  # 上游断言原文（拼写如此）
        "audio file not exists",
        "not satisfies frame",
    ]
    if any(token in text for token in non_retryable_tokens):
        return False
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
    retry_forever = os.getenv("OMNIHUMAN_STAGE_RETRY_FOREVER", "1").strip().lower() not in {"0", "false", "no", "off"}
    attempts = max(1, int(retries or os.getenv("OMNIHUMAN_STAGE_RETRIES", "3")))
    base_delay = max(1, int(retry_delay_seconds or os.getenv("OMNIHUMAN_STAGE_RETRY_DELAY_SECONDS", "5")))
    max_delay = max(base_delay, int(os.getenv("OMNIHUMAN_STAGE_RETRY_MAX_DELAY_SECONDS", "60") or "60"))
    last_error: Optional[Exception] = None
    attempt = 0

    while retry_forever or attempt < attempts:
        attempt += 1
        attempt_job_id = job_id if attempt == 1 else f"{job_id}:retry:{attempt}"
        try:
            if attempt > 1 and tracker:
                tracker.log(f"{label}重试中（第 {attempt} 次）" if retry_forever else f"{label}重试中（{attempt}/{attempts}）")
            return _run_omnihuman_job(job_id=attempt_job_id, label=label, tracker=tracker, runner=runner)
        except TaskCancelled:
            raise
        except Exception as exc:
            last_error = exc
            if not _is_retryable_omnihuman_error(exc):
                raise
            if not retry_forever and attempt >= attempts:
                raise
            wait_seconds = min(max_delay, base_delay * attempt)
            retry_message = (
                f"{label}失败，{wait_seconds} 秒后等待 5090 恢复并继续重试（第 {attempt} 次）：{exc}"
                if retry_forever
                else f"{label}失败，{wait_seconds} 秒后重试（{attempt}/{attempts}）"
            )
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
                    "retry_forever": retry_forever,
                    "error": str(exc),
                },
            )
            waited = 0
            while waited < wait_seconds:
                _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续等待 5090 数字人恢复")
                sleep_step = min(5, wait_seconds - waited)
                time.sleep(sleep_step)
                waited += sleep_step

    if last_error:
        raise last_error
    raise RuntimeError(f"{label}重试失败")


def _persist_production_checkpoint(task: dict, result: dict, stage: Optional[str] = None):
    if stage:
        task["production_stage"] = stage
    task["result"] = result
    _persist_task_result(task)


def _auto_digital_batch_path(batch_id: str) -> Path:
    return AUTO_DIGITAL_BATCH_DIR / f"{batch_id}.json"


def _save_auto_digital_batch_job(job: dict) -> None:
    batch_id = str(job.get("batch_id") or "").strip()
    if not batch_id:
        return
    path = _auto_digital_batch_path(batch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    with AUTO_DIGITAL_BATCH_LOCK:
        AUTO_DIGITAL_BATCH_JOBS[batch_id] = copy.deepcopy(job)


def _load_auto_digital_batch_job(batch_id: str) -> Optional[dict]:
    normalized = str(batch_id or "").strip()
    if not normalized:
        return None
    with AUTO_DIGITAL_BATCH_LOCK:
        cached = AUTO_DIGITAL_BATCH_JOBS.get(normalized)
    if cached:
        return copy.deepcopy(cached)
    path = _auto_digital_batch_path(normalized)
    if not path.exists():
        return None
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    with AUTO_DIGITAL_BATCH_LOCK:
        AUTO_DIGITAL_BATCH_JOBS[normalized] = copy.deepcopy(job)
    return job


def _list_auto_digital_batch_jobs_for_user(user: Optional[dict], limit: int = 12) -> list[dict]:
    if not AUTO_DIGITAL_BATCH_DIR.exists():
        return []
    jobs: list[dict] = []
    for path in sorted(AUTO_DIGITAL_BATCH_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not _is_admin(user) and str(job.get("owner_username") or "") != str((user or {}).get("username") or ""):
            continue
        jobs.append(job)
        if len(jobs) >= max(1, min(limit, 50)):
            break
    return jobs


def _find_running_auto_digital_batch_for_user(user: dict) -> Optional[dict]:
    for job in _list_auto_digital_batch_jobs_for_user(user, limit=50):
        if str(job.get("status") or "") in {"queued", "running"}:
            return job
    return None


def _auto_digital_batch_payload(job: Optional[dict]) -> Optional[dict]:
    if not isinstance(job, dict):
        return None
    items = []
    for item in job.get("items") or []:
        if not isinstance(item, dict):
            continue
        items.append(
            {
                "index": int(item.get("index") or len(items) + 1),
                "topic": str(item.get("topic") or ""),
                "angle": str(item.get("angle") or ""),
                "status": str(item.get("status") or "queued"),
                "task_id": str(item.get("task_id") or ""),
                "history_id": str(item.get("history_id") or ""),
                "error": str(item.get("error") or ""),
                "updated_at": float(item.get("updated_at") or 0),
            }
        )
    done_count = sum(1 for item in items if item.get("status") == "done")
    failed_count = sum(1 for item in items if item.get("status") == "error")
    running_count = sum(1 for item in items if item.get("status") == "running")
    return {
        "batch_id": str(job.get("batch_id") or ""),
        "seed_topic": str(job.get("seed_topic") or ""),
        "status": str(job.get("status") or "queued"),
        "message": str(job.get("message") or ""),
        "created_at": float(job.get("created_at") or 0),
        "updated_at": float(job.get("updated_at") or 0),
        "owner_username": str(job.get("owner_username") or ""),
        "owner_display_name": str(job.get("owner_display_name") or ""),
        "voice_preset_id": str(job.get("voice_preset_id") or ""),
        "avatar_id": str(job.get("avatar_id") or ""),
        "digital_human_engine": str(job.get("digital_human_engine") or ""),
        "script_model": str(job.get("script_model") or ""),
        "task_count": len(items),
        "done_count": done_count,
        "failed_count": failed_count,
        "running_count": running_count,
        "items": items,
    }


def _create_auto_digital_single_task(
    *,
    owner: dict,
    request: Request,
    topic: str,
    voice_preset: dict,
    avatar_option: dict,
    speed: float,
    target_market: str,
    department_id: str,
    script_model: str,
    digital_human_engine: str,
    batch_id: str,
    batch_index: int,
) -> str:
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    image_path = avatar_option.get("image_path", "")
    voice_preset = dict(voice_preset or {})
    voice_preset["selected_speed"] = speed
    task_user = {
        "username": owner.get("username"),
        "display_name": owner.get("display_name"),
        "role": owner.get("role"),
        "workflow_config": {"allow_local_digital_human": True},
        "allow_local_digital_human": True,
    }
    tasks[task_id] = {
        "owner_username": owner.get("username"),
        "owner_display_name": owner.get("display_name"),
        "owner_role": owner.get("role"),
        "id": task_id,
        "mode": "auto_digital_batch",
        "topic": topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id"),
            "avatar_id": avatar_option.get("id"),
            "speed": speed,
            "web_search_enabled": False,
            "target_market": target_market,
            "department_id": department_id,
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
            "compose_aspect_ratio": "vertical",
            "script_model": script_model,
            "digital_human_engine": _normalize_digital_human_engine(digital_human_engine, task_user),
            "allow_local_digital_human": True,
            "auto_digital_batch_id": batch_id,
            "auto_digital_batch_index": batch_index,
        },
        "allow_local_digital_human": True,
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log(f"批量数字人任务已创建，准备开始第 {batch_index} 条...")
    _push_live_event("task_created", f"创建了批量数字人任务第 {batch_index} 条", tasks[task_id])
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, image_path, tasks[task_id]["public_base_url"], None, dict(voice_preset), dict(avatar_option)),
        daemon=True,
    )
    thread.start()
    return task_id


def _wait_for_task_terminal_state(task_id: str, timeout_seconds: Optional[int] = None) -> tuple[str, Optional[dict]]:
    start_time = time.time()
    while True:
        task = tasks.get(task_id)
        tracker = task.get("tracker") if task else None
        if tracker and tracker.status in {"done", "error", "cancelled"}:
            return tracker.status, task
        if timeout_seconds is not None and time.time() - start_time >= timeout_seconds:
            return "timeout", task
        time.sleep(2)


def _run_auto_digital_batch(batch_id: str) -> None:
    job = _load_auto_digital_batch_job(batch_id)
    if not job:
        return
    owner = {
        "username": job.get("owner_username"),
        "display_name": job.get("owner_display_name"),
        "role": job.get("owner_role", "user"),
    }
    voice_preset = _get_voice_preset(job.get("voice_preset_id"), job.get("target_market"))
    avatar_option = _get_avatar_option(job.get("avatar_id"), target_market_id=job.get("target_market"))
    if not avatar_option:
        job["status"] = "error"
        job["message"] = "默认主播不存在，批量任务无法启动"
        job["updated_at"] = time.time()
        _save_auto_digital_batch_job(job)
        return
    speed = float(job.get("speed") or voice_preset.get("default_speed") or 1.1)
    request_data = job.get("request_context") or {}

    job["status"] = "running"
    job["message"] = "批量任务开始顺序生产"
    job["updated_at"] = time.time()
    _save_auto_digital_batch_job(job)

    class _BatchRequest:
        def __init__(self, base_url: str):
            parsed = urlparse(base_url)
            self.headers = {"host": parsed.netloc}
            self.url = type("URL", (), {"scheme": parsed.scheme or "https", "netloc": parsed.netloc})()

    batch_request = _BatchRequest(str(request_data.get("public_base_url") or "https://aiagent.office.ihousejapan.cn"))

    total_items = len(job.get("items") or [])
    for position in range(total_items):
        latest_job = _load_auto_digital_batch_job(batch_id) or job
        if str(latest_job.get("status") or "") == "cancelled":
            latest_job["message"] = "批量任务已停止"
            latest_job["updated_at"] = time.time()
            _save_auto_digital_batch_job(latest_job)
            return
        job = latest_job
        items = job.get("items") or []
        if position >= len(items):
            break
        item = items[position]
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") == "done":
            continue
        item["status"] = "running"
        item["updated_at"] = time.time()
        job["message"] = f"正在生成第 {item.get('index')} / {len(job.get('items') or [])} 条"
        job["updated_at"] = time.time()
        _save_auto_digital_batch_job(job)
        try:
            task_id = _create_auto_digital_single_task(
                owner=owner,
                request=batch_request,
                topic=str(item.get("topic") or ""),
                voice_preset=voice_preset,
                avatar_option=avatar_option,
                speed=speed,
                target_market=str(job.get("target_market") or "cn"),
                department_id=str(job.get("department_id") or "real_estate"),
                script_model=str(job.get("script_model") or SCRIPT_MODEL_LOCAL_QWEN),
                digital_human_engine=str(job.get("digital_human_engine") or INFINITETALK_ENGINE_ID),
                batch_id=batch_id,
                batch_index=int(item.get("index") or 0),
            )
            item["task_id"] = task_id
            _save_auto_digital_batch_job(job)
            status, task = _wait_for_task_terminal_state(task_id)
            output_dir = Path(str((task or {}).get("output_dir") or ""))
            if status == "done":
                item["status"] = "done"
                item["updated_at"] = time.time()
                item["history_id"] = output_dir.name if output_dir.exists() else ""
                item["error"] = ""
                # 话题自动化批次：单条做完后自动发到话题 YouTube 账号 + Facebook Page（若已配置并开启）
                if str(job.get("source") or "") == "topic_auto" and output_dir.exists():
                    try:
                        _maybe_publish_topic_video_to_youtube(output_dir)
                    except Exception as _topic_pub_exc:
                        print(f"[topic-auto youtube] hook error: {_topic_pub_exc!r}", flush=True)
                    try:
                        _maybe_publish_topic_video_to_facebook(output_dir)
                    except Exception as _topic_fb_exc:
                        print(f"[topic-auto facebook] hook error: {_topic_fb_exc!r}", flush=True)
            elif status == "cancelled":
                item["status"] = "cancelled"
                item["updated_at"] = time.time()
                item["error"] = "任务已停止"
                job["status"] = "cancelled"
                job["message"] = "批量任务已停止"
                _save_auto_digital_batch_job(job)
                return
            else:
                tracker = (task or {}).get("tracker")
                error_message = ""
                if tracker and getattr(tracker, "messages", None):
                    error_message = str(tracker.messages[-1].get("message") or "")
                item["status"] = "error"
                item["updated_at"] = time.time()
                item["history_id"] = output_dir.name if output_dir.exists() else ""
                item["error"] = error_message or ("批量任务超时" if status == "timeout" else "生成失败")
        except Exception as exc:
            item["status"] = "error"
            item["updated_at"] = time.time()
            item["error"] = str(exc)
        _save_auto_digital_batch_job(job)

    statuses = [str(item.get("status") or "") for item in job.get("items") or [] if isinstance(item, dict)]
    if statuses and all(status == "done" for status in statuses):
        job["status"] = "done"
        job["message"] = f"{len(statuses)} 条批量数字人视频已全部生成完成"
    elif any(status == "running" for status in statuses):
        job["status"] = "running"
        job["message"] = "批量任务仍在运行中"
    elif any(status == "done" for status in statuses):
        job["status"] = "partial"
        job["message"] = "部分视频已生成完成，请查看失败条目"
    else:
        job["status"] = "error"
        job["message"] = "批量任务未成功生成可用视频"
    job["updated_at"] = time.time()
    _save_auto_digital_batch_job(job)


def _recover_pending_auto_digital_batches(max_recovered: int = 3) -> None:
    if not AUTO_DIGITAL_BATCH_DIR.exists():
        return
    recovered = 0
    for path in sorted(AUTO_DIGITAL_BATCH_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        batch_id = str(job.get("batch_id") or path.stem).strip()
        if not batch_id or str(job.get("status") or "") not in {"queued", "running"}:
            continue
        for item in job.get("items") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "") == "running":
                if item.get("task_id"):
                    item["previous_task_id"] = item.get("task_id")
                item["task_id"] = ""
                item["status"] = "queued"
                item["error"] = "服务重启后已恢复排队"
                item["updated_at"] = time.time()
        job["status"] = "queued"
        job["message"] = "服务启动后已恢复批量数字人任务，继续从未完成条目生产"
        job["updated_at"] = time.time()
        _save_auto_digital_batch_job(job)
        threading.Thread(
            target=_run_auto_digital_batch,
            args=(batch_id,),
            daemon=True,
            name=f"auto-digital-recover-{batch_id[:12]}",
        ).start()
        recovered += 1
        if recovered >= max(1, int(max_recovered or 1)):
            break
    if recovered:
        print(f"🔁 已恢复批量数字人自动生成任务：{recovered} 个")


# ── 话题接口自动化：从话题采集接口拉取选题，复用现有"批量数字人"管线制作 ──

TOPIC_AUTO_OWNER = {"username": "topic_auto", "display_name": "话题自动化", "role": "admin"}


def _topic_auto_config_path() -> Path:
    return TOPIC_AUTO_DIR / "config.json"


def _load_topic_auto_config() -> dict:
    defaults = {
        "scheduler_enabled": False,
        "produce_limit": 5,        # 每次触发最多创建几条（逐条顺序生产）；剩余的下一轮继续，等于持续清空
        "interval_minutes": 10,     # 轮询间隔（分钟）：抓不定时新内容用小间隔
        "max_attempts": 3,          # 单条失败最多重试次数，达到后跳过并记录
        "youtube": {},              # 话题视频的 YouTube 发布账号：{enabled, channel_name, token_store_path}
        "youtube_auto_publish": False,  # 每条话题视频做完是否自动发 YouTube
        "facebook": {},             # 话题视频的 Facebook 发布 Page：{enabled, page_name, page_id, page_access_token}
        "facebook_auto_publish": False, # 每条话题视频做完是否自动发 Facebook（只发中文单帖）
        "next_run_at": 0,
        "last_run_at": 0,
        "last_run_message": "",
    }
    path = _topic_auto_config_path()
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for key in defaults:
                    if key in data:
                        defaults[key] = data[key]
        except Exception:
            pass
    return defaults


def _save_topic_auto_config(config: dict) -> None:
    TOPIC_AUTO_DIR.mkdir(parents=True, exist_ok=True)
    _topic_auto_config_path().write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _find_running_topic_auto_batch() -> Optional[dict]:
    for job in _list_auto_digital_batch_jobs_for_user(TOPIC_AUTO_OWNER, limit=20):
        if job.get("source") == "topic_auto" and job.get("status") in {"queued", "running"}:
            return job
    return None


def _maybe_publish_topic_video_to_youtube(output_dir: Path) -> None:
    """话题视频做完后，若已绑定话题 YouTube 账号且开启自动发布，则发布到该账号。失败记录不影响主流程。"""
    cfg = _load_topic_auto_config()
    yt = cfg.get("youtube") if isinstance(cfg.get("youtube"), dict) else {}
    if not (cfg.get("youtube_auto_publish") and yt.get("enabled") and yt.get("token_store_path")):
        return
    token_path = Path(str(yt.get("token_store_path")))
    if not token_path.exists():
        return
    result = _load_result_from_output_dir(output_dir)
    if not result or result.get("youtube_publish_records"):
        return  # 无结果或已发过，避免重复
    try:
        video = _resolve_youtube_publish_video(output_dir, result, aspect_ratio="vertical")
    except Exception:
        return  # 还没有成片
    try:
        meta = _build_youtube_shorts_metadata(_build_default_youtube_metadata(result))
        up = upload_video_to_youtube(
            token_path, video,
            title=meta["title"], description=meta["description"], tags=meta["tags"],
            privacy_status="public", category_id="22", made_for_kids=False,
            thumbnail_path=_resolve_youtube_thumbnail(output_dir, result, aspect_ratio="vertical"),
        )
        rec = {"job_id": f"topic_yt_{int(time.time())}", "history_id": output_dir.name,
               "aspect_ratio": "vertical", "video_path": str(video), "created_at": time.time(), **up}
        result["youtube_publish_records"] = [rec] + list(result.get("youtube_publish_records") or [])
        result["youtube_publish_latest"] = rec
        result.pop("youtube_auto_publish_error", None)
        _save_result_to_output_dir(output_dir, result)
        print(f"[topic-auto youtube] published {output_dir.name}: {up.get('youtube_url')}", flush=True)
    except Exception as exc:
        result["youtube_auto_publish_error"] = str(exc)
        _save_result_to_output_dir(output_dir, result)
        print(f"[topic-auto youtube] publish failed {output_dir.name}: {exc!r}", flush=True)


def _maybe_publish_topic_video_to_facebook(output_dir: Path) -> None:
    """话题视频做完后，若已绑定 FB Page 且开启自动发布，则发到该 Page（只发中文单帖，带节流/368冷却）。"""
    cfg = _load_topic_auto_config()
    fb = cfg.get("facebook") if isinstance(cfg.get("facebook"), dict) else {}
    if not (cfg.get("facebook_auto_publish") and fb.get("enabled") and fb.get("page_id") and fb.get("page_access_token")):
        return
    page_id = str(fb.get("page_id"))
    page_token = str(fb.get("page_access_token"))
    result = _load_result_from_output_dir(output_dir)
    if not result or result.get("facebook_publish_records"):
        return  # 无结果或已发过，避免重复
    if _facebook_publish_cooldown_remaining(page_id) > 0:
        result["facebook_auto_publish_error"] = "Facebook 368 频率封锁冷却中，已跳过本条。"
        _save_result_to_output_dir(output_dir, result)
        return
    if _facebook_publish_throttle_remaining(page_id) > 0:
        result["facebook_publish_throttled"] = "距上次 Facebook 发帖不足最小间隔，已跳过本条。"
        _save_result_to_output_dir(output_dir, result)
        return
    try:
        video = _resolve_youtube_publish_video(output_dir, result, aspect_ratio="vertical")
    except Exception:
        return  # 还没有成片
    try:
        title = str(result.get("title") or result.get("topic") or "iHouse 话题")
        desc = _build_default_facebook_post_text(result)
        up = upload_video_to_facebook_page(
            FACEBOOK_TOKEN_STORE_PATH, video,
            description=desc, title=title, page_id=page_id, page_access_token=page_token,
        )
        _record_facebook_publish_success(page_id)
        rec = {"job_id": f"topic_fb_{int(time.time())}", "history_id": output_dir.name,
               "aspect_ratio": "vertical", "video_path": str(video), "created_at": time.time(), **up}
        result["facebook_publish_records"] = [rec] + list(result.get("facebook_publish_records") or [])
        result["facebook_publish_latest"] = rec
        result.pop("facebook_auto_publish_error", None)
        _save_result_to_output_dir(output_dir, result)
        print(f"[topic-auto facebook] published {output_dir.name}: {up.get('facebook_url')}", flush=True)
    except Exception as exc:
        if _is_facebook_frequency_block(str(exc)):
            _trigger_facebook_publish_cooldown(str(exc), page_id=page_id)
        result["facebook_auto_publish_error"] = str(exc)
        _save_result_to_output_dir(output_dir, result)
        print(f"[topic-auto facebook] publish failed {output_dir.name}: {exc!r}", flush=True)


def _start_topic_auto_batch(selected: list[dict]) -> dict:
    """用给定选题（已抽取，含 topic/angle/record_id）创建并启动一个话题数字人批次。
    自动流程和面板手动勾选制作共用。返回 {ok, batch_id} 或 {ok:False, error}。"""
    if not selected:
        return {"ok": False, "error": "没有可制作的选题"}
    voice_preset = _get_voice_preset("mandarin_female", "cn")
    avatar_option = _get_avatar_option("avatar_host_d.png", target_market_id="cn")
    if not avatar_option:
        return {"ok": False, "error": "默认女主播C(avatar_host_d.png)不存在，请先恢复"}
    items: list[dict] = []
    for i, topic in enumerate(selected, start=1):
        items.append({
            "index": i,
            "topic": topic["topic"],
            "angle": topic.get("angle") or "",
            "status": "queued",
            "task_id": "",
            "history_id": "",
            "error": "",
            "topic_record_id": topic["record_id"],
            "topic_tags": topic.get("tags") or [],
            "created_at": time.time(),
            "updated_at": time.time(),
        })
    batch_id = f"tadb_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job = {
        "batch_id": batch_id,
        "source": "topic_auto",
        "seed_topic": "",
        "status": "queued",
        "message": "话题数字人批次已创建，等待启动",
        "created_at": time.time(),
        "updated_at": time.time(),
        "owner_username": TOPIC_AUTO_OWNER["username"],
        "owner_display_name": TOPIC_AUTO_OWNER["display_name"],
        "owner_role": TOPIC_AUTO_OWNER["role"],
        "target_market": "cn",
        "department_id": "real_estate",
        "voice_preset_id": voice_preset.get("id"),
        "avatar_id": avatar_option.get("id"),
        "speed": float(voice_preset.get("default_speed") or 1.1),
        "script_model": SCRIPT_MODEL_LOCAL_QWEN,
        "digital_human_engine": INFINITETALK_ENGINE_ID,
        "request_context": {"public_base_url": os.getenv("PUBLIC_BASE_URL", "https://aiagent.office.ihousejapan.cn")},
        "items": items,
    }
    _save_auto_digital_batch_job(job)
    threading.Thread(target=_run_auto_digital_batch, args=(batch_id,), daemon=True).start()
    return {"ok": True, "batch_id": batch_id}


def _reconcile_topic_auto_state(state: dict, *, max_attempts: int, limit: int = 30) -> bool:
    """把最近已结束的话题批次逐条对账进 state：成功=done(不再做)，失败=attempts+1(可重试)。返回是否有更新。"""
    changed = False
    reconciled = set(state.get("reconciled_batches") or [])
    try:
        jobs = _list_auto_digital_batch_jobs_for_user(TOPIC_AUTO_OWNER, limit=limit)
    except Exception:
        jobs = []
    for job in jobs:
        if job.get("source") != "topic_auto":
            continue
        batch_id = str(job.get("batch_id") or "")
        if not batch_id or batch_id in reconciled:
            continue
        if str(job.get("status") or "") in {"queued", "running"}:
            continue  # 批次未结束，下一轮再对账
        full = _load_auto_digital_batch_job(batch_id) or job
        for item in (full.get("items") or []):
            rid = str(item.get("topic_record_id") or "")
            if not rid:
                continue
            istatus = str(item.get("status") or "")
            if istatus == "done":
                topic_auto.mark_record_result(state, rid, success=True, batch_id=batch_id, topic=item.get("topic") or "", max_attempts=max_attempts)
                changed = True
            elif istatus in {"error", "cancelled"}:
                topic_auto.mark_record_result(state, rid, success=False, batch_id=batch_id, topic=item.get("topic") or "", max_attempts=max_attempts)
                changed = True
        reconciled.add(batch_id)
        changed = True
    if changed:
        state["reconciled_batches"] = sorted(reconciled)[-300:]
    return changed


def _run_topic_auto_produce_once(*, limit: Optional[int] = None, triggered_by: str = "manual") -> dict:
    """拉取话题接口 -> 对账历史批次(成功才去重/失败重试) -> 选待做 -> 复用批量数字人管线逐条生产。"""
    config = _load_topic_auto_config()
    max_attempts = max(1, int(config.get("max_attempts") or 3))
    produce_limit = int(limit if limit is not None else (config.get("produce_limit") or 5))
    produce_limit = max(1, min(50, produce_limit))
    if not topic_auto.topic_auto_is_configured():
        return {"ok": False, "error": "未配置 TOPIC_COLLECTOR_API_TOKEN，无法拉取话题选题"}
    # 先对账已结束的历史批次：成功的标记 done，失败的累计重试次数（达上限才跳过）
    state = topic_auto.load_topic_state(str(TOPIC_AUTO_DIR))
    if _reconcile_topic_auto_state(state, max_attempts=max_attempts):
        topic_auto.save_topic_state(str(TOPIC_AUTO_DIR), state)
    running = _find_running_topic_auto_batch()
    if running:
        return {"ok": True, "running": True, "batch_id": running.get("batch_id"), "message": "已有话题数字人批次在运行，本次不重复触发。"}
    try:
        records = topic_auto.fetch_topic_records()
    except Exception as exc:
        return {"ok": False, "error": f"拉取话题接口失败：{exc}"}
    selected = topic_auto.select_pending_topics(records, state, max_attempts=max_attempts, limit=produce_limit)
    if not selected:
        msg = f"拉取 {len(records)} 条话题，无新选题（均已制作过）。"
        config["last_run_at"] = time.time()
        config["last_run_message"] = msg
        _save_topic_auto_config(config)
        return {"ok": True, "produced": 0, "total_records": len(records), "message": msg}
    started = _start_topic_auto_batch(selected)
    if not started.get("ok"):
        return started
    batch_id = started["batch_id"]
    # 不在提交时去重：成功/失败由下一轮对账写入 topic_state（成功才不再做、失败可重试）
    msg = f"已从话题接口选取 {len(selected)} 条待做选题，创建数字人批次 {batch_id} 并开始逐条制作。"
    config["last_run_at"] = time.time()
    config["last_run_message"] = msg
    _save_topic_auto_config(config)
    print(f"[topic-auto] {triggered_by}: {msg}", flush=True)
    return {
        "ok": True,
        "produced": len(selected),
        "total_records": len(records),
        "batch_id": batch_id,
        "topics": [t["topic"] for t in selected],
        "message": msg,
    }


def _start_topic_auto_scheduler(poll_seconds: int = 60) -> None:
    """话题自动化定时器：仅当 config.scheduler_enabled 为真时按 interval 拉取+制作。默认关闭。"""
    def loop() -> None:
        while True:
            try:
                config = _load_topic_auto_config()
                if config.get("scheduler_enabled"):
                    now = time.time()
                    next_run_at = float(config.get("next_run_at") or 0)
                    if now >= next_run_at:
                        _run_topic_auto_produce_once(triggered_by="scheduler")
                        config = _load_topic_auto_config()
                        config["next_run_at"] = now + max(10, int(config.get("interval_minutes") or 120)) * 60
                        _save_topic_auto_config(config)
            except Exception as exc:
                print(f"[topic-auto scheduler] loop error: {exc!r}", flush=True)
            time.sleep(max(30, int(poll_seconds)))

    threading.Thread(target=loop, name="topic-auto-scheduler", daemon=True).start()


# ────────────── 物件(房源)自动化：拉接口 → 下载实拍视频 → AI 写文案 → 房源实拍成片 ──────────────
PROPERTY_AUTO_OWNER = {"username": "property_auto", "display_name": "房源自动化", "role": "admin"}


def _property_auto_config_path() -> Path:
    return PROPERTY_AUTO_DIR / "config.json"


def _load_property_auto_config() -> dict:
    defaults = {
        "scheduler_enabled": False,
        "interval_minutes": 15,
        "max_attempts": 3,
        "youtube_auto_publish": True,   # 房源视频做完自动发 YouTube（共用话题绑定的账号，只发 Shorts）
        "facebook_auto_publish": True,  # 房源视频做完自动发 Facebook（共用话题绑定的 Page，只发中文单帖）
        "next_run_at": 0,
        "last_run_at": 0,
        "last_run_message": "",
    }
    path = _property_auto_config_path()
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for key in defaults:
                    if key in data:
                        defaults[key] = data[key]
        except Exception:
            pass
    return defaults


def _save_property_auto_config(config: dict) -> None:
    PROPERTY_AUTO_DIR.mkdir(parents=True, exist_ok=True)
    _property_auto_config_path().write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _find_running_property_auto_task() -> Optional[str]:
    """是否有房源自动化的任务正在制作（一次只做一条）。"""
    for tid, task in list(tasks.items()):
        if not isinstance(task, dict):
            continue
        if task.get("owner_username") != PROPERTY_AUTO_OWNER["username"]:
            continue
        tracker = task.get("tracker")
        if tracker is not None and getattr(tracker, "status", "") in {"running", "pending", "queued"}:
            return tid
    return None


def _property_video_final_exists(output_dir: Path) -> bool:
    try:
        for p in output_dir.rglob("*.mp4"):
            if "final" in p.name.lower() or "final" in str(p.parent.name).lower():
                return True
    except Exception:
        pass
    return False


def _maybe_publish_property_video_to_youtube(output_dir: Path, result: Optional[dict] = None, title_hint: str = "") -> None:
    """房源视频做完后，若开启，则发布到（与话题共用的）YouTube 账号，只发 Shorts。"""
    pcfg = _load_property_auto_config()
    if not pcfg.get("youtube_auto_publish", True):
        return
    tcfg = _load_topic_auto_config()  # 共用话题绑定的同一个 YouTube 账号
    yt = tcfg.get("youtube") if isinstance(tcfg.get("youtube"), dict) else {}
    token_path = Path(str(yt.get("token_store_path") or ""))
    if not (yt.get("enabled") and token_path.exists()):
        return
    result = result if isinstance(result, dict) else (_load_result_from_output_dir(output_dir) or {})
    if result.get("youtube_publish_records"):
        return
    video = None
    fvp = str(result.get("final_video_path") or "")
    if fvp and Path(fvp).exists():
        video = Path(fvp)
    else:
        for p in sorted(output_dir.rglob("*.mp4")):
            if "final" in p.name.lower():
                video = p
                break
    if not video or not video.exists():
        return
    try:
        base_title = (title_hint or result.get("title") or output_dir.name or "房源实拍").strip()
        desc = str(result.get("social_post") or result.get("script_text") or base_title)
        meta = _build_youtube_shorts_metadata({"title": base_title, "description": desc, "tags": ["房源", "日本房产", "iHouse"]})
        up = upload_video_to_youtube(
            token_path, video,
            title=meta["title"], description=meta["description"], tags=meta["tags"],
            privacy_status="public", category_id="22", made_for_kids=False, thumbnail_path=None,
        )
        rec = {"job_id": f"property_yt_{int(time.time())}", "history_id": output_dir.name,
               "aspect_ratio": "vertical", "video_path": str(video), "created_at": time.time(), **up}
        result["youtube_publish_records"] = [rec] + list(result.get("youtube_publish_records") or [])
        result["youtube_publish_latest"] = rec
        result.pop("youtube_auto_publish_error", None)
        _save_result_to_output_dir(output_dir, result)
        print(f"[property-auto youtube] published {output_dir.name}: {up.get('youtube_url')}", flush=True)
    except Exception as exc:
        result["youtube_auto_publish_error"] = str(exc)
        _save_result_to_output_dir(output_dir, result)
        print(f"[property-auto youtube] publish failed {output_dir.name}: {exc!r}", flush=True)


def _maybe_publish_property_video_to_facebook(output_dir: Path, result: Optional[dict] = None, title_hint: str = "") -> None:
    """房源视频做完后，若开启，则发到（与话题共用的）FB Page，只发中文单帖，带节流/368冷却。"""
    pcfg = _load_property_auto_config()
    if not pcfg.get("facebook_auto_publish", True):
        return
    tcfg = _load_topic_auto_config()  # 共用话题绑定的同一个 FB Page
    fb = tcfg.get("facebook") if isinstance(tcfg.get("facebook"), dict) else {}
    if not (fb.get("enabled") and fb.get("page_id") and fb.get("page_access_token")):
        return
    page_id = str(fb.get("page_id"))
    page_token = str(fb.get("page_access_token"))
    result = result if isinstance(result, dict) else (_load_result_from_output_dir(output_dir) or {})
    if result.get("facebook_publish_records"):
        return
    if _facebook_publish_cooldown_remaining(page_id) > 0 or _facebook_publish_throttle_remaining(page_id) > 0:
        return
    video = None
    fvp = str(result.get("final_video_path") or "")
    if fvp and Path(fvp).exists():
        video = Path(fvp)
    else:
        for p in sorted(output_dir.rglob("*.mp4")):
            if "final" in p.name.lower():
                video = p
                break
    if not video or not video.exists():
        return
    try:
        base_title = (title_hint or result.get("title") or output_dir.name or "房源实拍").strip()
        desc = str(result.get("social_post") or result.get("script_text") or base_title)
        up = upload_video_to_facebook_page(
            FACEBOOK_TOKEN_STORE_PATH, video,
            description=desc, title=base_title, page_id=page_id, page_access_token=page_token,
        )
        _record_facebook_publish_success(page_id)
        rec = {"job_id": f"property_fb_{int(time.time())}", "history_id": output_dir.name,
               "aspect_ratio": "vertical", "video_path": str(video), "created_at": time.time(), **up}
        result["facebook_publish_records"] = [rec] + list(result.get("facebook_publish_records") or [])
        result["facebook_publish_latest"] = rec
        result.pop("facebook_auto_publish_error", None)
        _save_result_to_output_dir(output_dir, result)
        print(f"[property-auto facebook] published {output_dir.name}: {up.get('facebook_url')}", flush=True)
    except Exception as exc:
        if _is_facebook_frequency_block(str(exc)):
            _trigger_facebook_publish_cooldown(str(exc), page_id=page_id)
        result["facebook_auto_publish_error"] = str(exc)
        _save_result_to_output_dir(output_dir, result)
        print(f"[property-auto facebook] publish failed {output_dir.name}: {exc!r}", flush=True)


def _create_property_auto_video(prop: dict) -> dict:
    """下载房源视频 + AI 写文案 + 创建房源实拍成片任务。返回 {ok, task_id, output_dir} 或 {ok:False, error}。"""
    voice_preset = _get_voice_preset("mandarin_female", "cn")  # 温柔女声
    if not voice_preset or voice_preset.get("enabled") is False:
        return {"ok": False, "error": "默认温柔女声(mandarin_female)不可用"}
    task_id = str(uuid.uuid4())[:8]
    safe_name = re.sub(r"[^\w一-鿿-]+", "", str(prop.get("name") or ""))[:20]
    output_dir = Path(_create_output_dir("property_video", f"房源实拍成片-{safe_name or prop.get('record_id')}"))
    incoming = output_dir / "incoming"
    saved_paths = property_auto.download_videos(prop.get("video_urls") or [], incoming)
    if not saved_paths:
        return {"ok": False, "error": "房源实拍视频下载失败或无视频"}
    try:
        analysis = analyze_property_video_with_openai(
            video_paths=[Path(p) for p in saved_paths],
            work_dir=output_dir / "analysis",
            target_market="cn",
            user_notes=prop.get("notes_text") or "",
        )
    except Exception as exc:
        return {"ok": False, "error": f"AI 文案生成失败：{exc}"}
    script_text = str(analysis.get("suggested_script") or "").strip()
    if not script_text:
        return {"ok": False, "error": "AI 没有生成文案"}
    timeline = analysis.get("timeline_segments") if isinstance(analysis.get("timeline_segments"), list) else []
    speed = float(voice_preset.get("default_speed") or 1.1)
    voice_preset = dict(voice_preset)
    voice_preset["selected_speed"] = speed
    tracker = ProgressTracker(task_id)
    tracker.total_steps = 4
    tasks[task_id] = {
        "owner_username": PROPERTY_AUTO_OWNER["username"],
        "owner_display_name": PROPERTY_AUTO_OWNER["display_name"],
        "owner_role": PROPERTY_AUTO_OWNER["role"],
        "id": task_id,
        "mode": "property_video",
        "topic": f"房源实拍成片-{prop.get('name') or ''}",
        "image_path": "",
        "tracker": tracker,
        "output_dir": str(output_dir),
        "result": None,
        "public_base_url": os.getenv("PUBLIC_BASE_URL", "https://aiagent.office.ihousejapan.cn"),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "property_auto_record_id": prop.get("record_id"),
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id"),
            "speed": speed,
            "target_market": "cn",
            "voice_preset": voice_preset,
            "bgm_item_id": "",
            "bgm_volume": 0.0,
            "property_video_mode": "one_take_timeline" if timeline else "real_shot_voiceover",
            "timeline_segments": timeline,
            "property_auto": True,
            "property_record_id": prop.get("record_id"),
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("房源实拍成片任务已创建（房源自动化），准备开始...")
    threading.Thread(
        target=run_property_video_with_progress,
        args=(task_id, saved_paths, script_text, voice_preset, "cn", speed, "", 0.0, timeline),
        daemon=True,
    ).start()
    return {"ok": True, "task_id": task_id, "output_dir": str(output_dir)}


def _reconcile_property_auto_state(state: dict, *, max_attempts: int) -> bool:
    """把 producing 中的房源记录对账：成片存在=done；任务已结束且无成片=失败。"""
    changed = False
    for rid, entry in list((state.get("records") or {}).items()):
        if not isinstance(entry, dict) or entry.get("status") != "producing":
            continue
        od = entry.get("output_dir")
        tid = entry.get("last_task")
        if od and _property_video_final_exists(Path(od)):
            property_auto.mark_record_result(state, rid, success=True, task_id=tid or "", name=entry.get("name") or "", max_attempts=max_attempts)
            changed = True
            continue
        # 任务不在运行了（内存里没有或已结束）且没成片 → 失败
        task = tasks.get(tid) if tid else None
        tracker = (task or {}).get("tracker") if isinstance(task, dict) else None
        still_running = tracker is not None and getattr(tracker, "status", "") in {"running", "pending", "queued"}
        if not still_running:
            property_auto.mark_record_result(state, rid, success=False, task_id=tid or "", name=entry.get("name") or "", max_attempts=max_attempts)
            changed = True
    return changed


def _run_property_auto_produce_once(*, triggered_by: str = "manual", record_ids: Optional[list] = None) -> dict:
    """拉房源接口 → 对账 → 选一条待做 → 下载视频+AI文案+制作。一次只做一条（房源视频较重）。"""
    config = _load_property_auto_config()
    max_attempts = max(1, int(config.get("max_attempts") or 3))
    if not property_auto.property_auto_is_configured():
        return {"ok": False, "error": "未配置 PROPERTY_API_TOKEN，无法拉取房源数据"}
    state = property_auto.load_property_state(str(PROPERTY_AUTO_DIR))
    if _reconcile_property_auto_state(state, max_attempts=max_attempts):
        property_auto.save_property_state(str(PROPERTY_AUTO_DIR), state)
    running = _find_running_property_auto_task()
    if running:
        return {"ok": True, "running": True, "task_id": running, "message": "已有房源视频在制作中，本次不重复触发。"}
    try:
        records = property_auto.fetch_property_records()
    except Exception as exc:
        return {"ok": False, "error": f"拉取房源接口失败：{exc}"}
    if record_ids:
        wanted = {str(x).strip() for x in record_ids if str(x).strip()}
        pending = []
        for r in records:
            ex = property_auto.extract_property(r)
            if ex and ex["record_id"] in wanted:
                pending.append(ex)
    else:
        pending = property_auto.select_pending_properties(records, state, max_attempts=max_attempts, limit=1)
    if not pending:
        msg = f"拉取 {len(records)} 个房源，无待做（均已制作或无视频）。"
        config["last_run_at"] = time.time(); config["last_run_message"] = msg
        _save_property_auto_config(config)
        return {"ok": True, "produced": 0, "total_records": len(records), "message": msg}
    prop = pending[0]
    started = _create_property_auto_video(prop)
    if not started.get("ok"):
        # 立即失败也计一次尝试
        property_auto.mark_record_result(state, prop["record_id"], success=False, name=prop.get("name") or "", max_attempts=max_attempts)
        property_auto.save_property_state(str(PROPERTY_AUTO_DIR), state)
        return {"ok": False, "error": started.get("error")}
    # 标记为 producing（成功/失败由下一轮对账写入）
    rec = state.setdefault("records", {}).setdefault(prop["record_id"], {"attempts": 0})
    rec.update({"status": "producing", "last_task": started["task_id"], "output_dir": started["output_dir"],
                "name": prop.get("name") or "", "updated_at": time.time()})
    property_auto.save_property_state(str(PROPERTY_AUTO_DIR), state)
    msg = f"开始制作房源实拍成片：{prop.get('name') or prop['record_id']}（任务 {started['task_id']}）。"
    config["last_run_at"] = time.time(); config["last_run_message"] = msg
    _save_property_auto_config(config)
    print(f"[property-auto] {triggered_by}: {msg}", flush=True)
    return {"ok": True, "produced": 1, "total_records": len(records), "task_id": started["task_id"], "message": msg}


def _start_property_auto_scheduler(poll_seconds: int = 60) -> None:
    def loop() -> None:
        while True:
            try:
                config = _load_property_auto_config()
                if config.get("scheduler_enabled"):
                    now = time.time()
                    if now >= float(config.get("next_run_at") or 0):
                        _run_property_auto_produce_once(triggered_by="scheduler")
                        config = _load_property_auto_config()
                        config["next_run_at"] = now + max(5, int(config.get("interval_minutes") or 15)) * 60
                        _save_property_auto_config(config)
            except Exception as exc:
                print(f"[property-auto scheduler] loop error: {exc!r}", flush=True)
            time.sleep(max(30, int(poll_seconds)))

    threading.Thread(target=loop, name="property-auto-scheduler", daemon=True).start()


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


def _is_opennews_tts_workflow(workflow_config: dict) -> bool:
    source = workflow_config.get("source") or {}
    source_kind = str(source.get("kind") or "").strip().lower() if isinstance(source, dict) else ""
    engine = str(workflow_config.get("digital_human_engine") or "").strip().lower()
    return (
        bool(workflow_config.get("opennews"))
        or bool(workflow_config.get("opennews_material_only"))
        or engine == "opennews_material_only"
        or source_kind == "opennews"
    )


def _is_digital_human_tts_workflow(workflow_config: dict) -> bool:
    if bool(workflow_config.get("auto_digital_batch_id")):
        return True
    if _is_opennews_tts_workflow(workflow_config):
        return False
    engine = str(workflow_config.get("digital_human_engine") or "").strip().lower()
    return engine in {INFINITETALK_ENGINE_ID, HUNYUAN_ENGINE_ID, VOLC_ENGINE_ID}


def _should_use_qwen_tts_for_workflow(workflow_config: dict) -> bool:
    # Sales digital-human jobs use MiniMax for voiceover. Keep 5090 GPU memory
    # reserved for InfiniteTalk instead of competing with local Qwen3-TTS.
    if _is_digital_human_tts_workflow(workflow_config):
        return False
    if not OPENNEWS_QWEN_TTS_ENABLED:
        return False
    if _is_opennews_tts_workflow(workflow_config):
        return True
    return bool(workflow_config.get("force_qwen_tts"))


def _opennews_qwen_tts_language_enabled(target_market: str) -> bool:
    target_market = str(target_market or "cn").strip().lower() or "cn"
    return target_market in {"cn", "tw", "jp", "en"}


def _opennews_qwen_tts_language_for_market(target_market: str) -> str:
    target_market = str(target_market or "cn").strip().lower() or "cn"
    if target_market == "jp":
        return "japanese"
    if target_market == "en":
        return "english"
    return OPENNEWS_QWEN_TTS_LANGUAGE


def _sanitize_opennews_qwen_tts_text(script_text: str, target_market: str) -> str:
    text = str(script_text or "").strip()
    if not text:
        return ""
    if target_market in {"cn", "tw"}:
        replacements = {
            "H-1B": "H一1B",
            "h-1b": "H一1B",
            "AI": "A I",
            "2026至2027": "2026年到2027年",
            "2026-2027": "2026年到2027年",
            "75%到80%": "百分之七十五到百分之八十",
            "21%到33%": "百分之二十一到百分之三十三",
            "10万美元": "十万美元",
        }
        for source, target in replacements.items():
            text = text.replace(source, target)
        text = re.sub(r"(\d{4})-(\d{4})", r"\1年到\2年", text)
        text = re.sub(r"([A-Za-z])\-([A-Za-z0-9])", r"\1 \2", text)
    elif target_market == "jp":
        text = text.replace("H-1B", "H 1 B")
    elif target_market == "en":
        text = text.replace("H-1B", "H 1 B")
    return re.sub(r"\s+", " ", text).strip()


def _split_opennews_qwen_tts_text(script_text: str, target_market: str, max_chars: int = 120) -> list[str]:
    text = _sanitize_opennews_qwen_tts_text(script_text, target_market)
    if not text:
        return []
    separators = "。！？!?；;：:"
    chunks: list[str] = []
    buffer = ""
    for char in text:
        buffer += char
        if len(buffer) >= max_chars and char in separators:
            chunks.append(buffer.strip())
            buffer = ""
    if buffer.strip():
        chunks.append(buffer.strip())
    normalized: list[str] = []
    for chunk in chunks:
        if len(chunk) <= max_chars:
            normalized.append(chunk)
            continue
        start = 0
        while start < len(chunk):
            normalized.append(chunk[start:start + max_chars].strip())
            start += max_chars
    return [item for item in normalized if item]


def _generate_opennews_qwen_tts_audio(
    script_text: str,
    output_path: str,
    presenter_config: Optional[dict] = None,
    *,
    target_market: str = "cn",
    task_id: str = "",
    log=None,
) -> str:
    if not OPENNEWS_QWEN_TTS_BASE_URL or not OPENNEWS_QWEN_TTS_TOKEN:
        raise RuntimeError("OpenNews Qwen3-TTS 未配置 base_url 或 token")
    presenter = _normalize_opennews_presenter_config(presenter_config)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    chunks = _split_opennews_qwen_tts_text(script_text, target_market, max_chars=OPENNEWS_QWEN_TTS_MAX_CHARS)
    if not chunks:
        raise RuntimeError("OpenNews Qwen3-TTS 文本为空")
    headers = {
        "X-Token": OPENNEWS_QWEN_TTS_TOKEN,
        "Content-Type": "application/json",
    }
    wav_paths: list[Path] = []
    for index, chunk_text in enumerate(chunks, start=1):
        if task_id:
            _raise_if_task_cancel_requested(task_id, "已停止当前任务，未继续生成配音")
        if log and len(chunks) > 1:
            log(f"5090 Qwen3-TTS 分片配音中（{index}/{len(chunks)}）")
        payload = {
            "text": chunk_text,
            "language": _opennews_qwen_tts_language_for_market(target_market),
            "speaker": presenter.get("qwen_speaker") or OPENNEWS_QWEN_TTS_SPEAKER,
            "instruct": presenter.get("qwen_instruct") or OPENNEWS_QWEN_TTS_INSTRUCT,
        }
        wav_path = output.with_suffix(f".qwen.part{index:02d}.wav")
        last_tts_error: Exception | None = None
        for tts_attempt in range(OPENNEWS_QWEN_TTS_RETRY_ATTEMPTS):
            try:
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
                wav_path.write_bytes(wav_response.content)
                last_tts_error = None
                break
            except Exception as tts_exc:
                last_tts_error = tts_exc
                if tts_attempt + 1 < OPENNEWS_QWEN_TTS_RETRY_ATTEMPTS:
                    if log:
                        log(f"5090 Qwen3-TTS 分片 {index} 第 {tts_attempt + 1} 次失败，重试：{tts_exc}")
                    time.sleep(min(30, 6.0 * (tts_attempt + 1)))
                    continue
        if last_tts_error is not None:
            raise last_tts_error
        wav_paths.append(wav_path)
        if OPENNEWS_QWEN_TTS_CHUNK_PAUSE_SECONDS > 0 and index < len(chunks):
            time.sleep(OPENNEWS_QWEN_TTS_CHUNK_PAUSE_SECONDS)
    if len(wav_paths) == 1:
        merge_input = wav_paths[0]
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(merge_input),
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
    else:
        concat_list = output.with_suffix(".qwen.concat.txt")
        concat_list.write_text("".join(f"file '{path.name}'\n" for path in wav_paths), encoding="utf-8")
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_list),
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
            timeout=180,
            cwd=str(output.parent),
        )
        try:
            concat_list.unlink()
        except Exception:
            pass
    for wav_path in wav_paths:
        try:
            wav_path.unlink()
        except Exception:
            pass
    return str(output)


def _opennews_minimax_fallback_voice(presenter_config: Optional[dict] = None) -> tuple[str, str, str]:
    presenter = _normalize_opennews_presenter_config(presenter_config)
    preset_id = str(presenter.get("minimax_voice_preset_id") or OPENNEWS_MINIMAX_FALLBACK_VOICE_PRESET_ID).strip()
    preset = _get_voice_preset(preset_id, "cn")
    voice_id = str(preset.get("voice_id") or "").strip()
    if not voice_id:
        voice_id = "Chinese (Mandarin)_Gentleman" if presenter.get("gender") == "male" else "Chinese (Mandarin)_Warm_Bestie"
    language = str(preset.get("language") or "zh").strip() or "zh"
    return voice_id, language, preset_id


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
    task_id: str = "",
) -> tuple[str, str]:
    target_market = str(workflow_config.get("target_market") or "cn").strip().lower() or "cn"
    opennews_workflow = _should_use_qwen_tts_for_workflow(workflow_config)
    if opennews_workflow:
        if not OPENNEWS_QWEN_TTS_ENABLED:
            raise RuntimeError("OpenNews 已配置为只使用 5090 Qwen3-TTS，但 OPENNEWS_QWEN_TTS_ENABLED 当前未开启")
        if not _opennews_qwen_tts_language_enabled(target_market):
            raise RuntimeError(f"OpenNews 只允许使用 5090 Qwen3-TTS，当前市场不支持：{target_market}")
        presenter_config = _normalize_opennews_presenter_config(workflow_config.get("opennews_presenter"))
        status_task_id = str(task_id or workflow_config.get("task_id") or workflow_config.get("source_task_id") or "").strip()
        speaker = str(presenter_config.get("qwen_speaker") or OPENNEWS_QWEN_TTS_SPEAKER)
        workflow_label = "批量数字人" if workflow_config.get("auto_digital_batch_id") else "OpenNews"
        attempt = 0
        while True:
            attempt += 1
            try:
                if log:
                    log(
                        f"{workflow_label} 使用 5090 Qwen3-TTS 本地配音："
                        f"{presenter_config.get('qwen_speaker')} / {_opennews_qwen_tts_language_for_market(target_market)}"
                        f"（第 {attempt} 次）"
                    )
                _run_qwen_tts_job(
                    task_id=status_task_id,
                    label=f"OpenNews Qwen3-TTS {target_market}",
                    target_market=target_market,
                    speaker=speaker,
                    log=log,
                    runner=lambda: _generate_opennews_qwen_tts_audio(
                        script_text,
                        audio_path,
                        presenter_config=presenter_config,
                        target_market=target_market,
                        task_id=status_task_id,
                        log=log,
                    ),
                )
                return audio_path, "qwen3-tts"
            except TaskCancelled:
                raise
            except Exception as exc:
                recover_result = _recover_qwen_tts_service(str(exc))
                gpu_unavailable = _qwen_tts_gpu_unavailable(recover_result, exc)
                retry_delay = (
                    OPENNEWS_QWEN_TTS_GPU_UNAVAILABLE_RETRY_INTERVAL_SECONDS
                    if gpu_unavailable
                    else OPENNEWS_QWEN_TTS_RETRY_INTERVAL_SECONDS
                )
                if log:
                    log(
                        (
                            "Qwen3-TTS 配音失败，检测到 5090 GPU 异常，建议确认 5090 是否已重启；"
                            if gpu_unavailable
                            else "Qwen3-TTS 配音失败，不使用 MiniMax 兜底；"
                        )
                        + f"{retry_delay} 秒后继续重试：{exc}"
                        + (f"；恢复动作：{recover_result.get('profile') or recover_result.get('reason') or recover_result.get('error') or '已触发'}" if isinstance(recover_result, dict) else "")
                    )
                time.sleep(retry_delay)
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


def _jclaw_lab_token_from_request(request: Request) -> str:
    bearer = _bearer_token_from_request(request)
    if bearer:
        return bearer
    return str(request.headers.get("X-JClaw-Lab-Token") or "").strip()


def _jclaw_lab_secret_keys() -> list[tuple[str, bytes]]:
    secrets = [
        os.getenv("JCLAW_LAB_TOKEN_SECRET", ""),
        os.getenv("JCLAW_AI_AGENT_HANDOFF_SECRET", ""),
    ]
    keys: list[tuple[str, bytes]] = []
    seen: set[bytes] = set()
    for secret in secrets:
        secret = str(secret or "").strip()
        if not secret:
            continue
        for mode, key in _jclaw_handoff_secret_keys(secret):
            if key in seen:
                continue
            seen.add(key)
            keys.append((mode, key))
    return keys


def _verify_jclaw_lab_token(token: str) -> Optional[dict[str, Any]]:
    parts = str(token or "").split(".")
    if len(parts) != 3 or not all(parts):
        return None
    secret_keys = _jclaw_lab_secret_keys()
    if not secret_keys:
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
    signature_ok = False
    for _, secret_key in secret_keys:
        expected = _jwt_hs256_signature_with_key(signing_input, secret_key)
        if hmac.compare_digest(signature, expected):
            signature_ok = True
            break
    if not signature_ok:
        return None
    now = int(time.time())
    if int(payload.get("exp") or 0) <= now:
        return None
    if payload.get("nbf") and int(payload.get("nbf") or 0) > now + 30:
        return None
    expected_app = os.getenv("JCLAW_LAB_APP_KEY", "").strip()
    if expected_app and str(payload.get("app") or "").strip() != expected_app:
        return None
    issuer = str(payload.get("iss") or "").strip()
    if issuer and issuer != "jclaw-lab":
        return None
    return payload


def _resolve_jclaw_lab_user(payload: dict[str, Any]) -> str:
    lab_username = str(payload.get("sub") or payload.get("username") or payload.get("uid") or "").strip()
    normalized_payload = dict(payload)
    normalized_payload.setdefault("uid", lab_username)
    normalized_payload.setdefault("username", lab_username)
    return _resolve_jclaw_user(normalized_payload)


def _get_current_user_or_jclaw_lab(request: Request) -> Optional[dict]:
    user = _get_current_user(request)
    if user:
        return user
    payload = _verify_jclaw_lab_token(_jclaw_lab_token_from_request(request))
    if not payload:
        return None
    try:
        username = _resolve_jclaw_lab_user(payload)
    except Exception as exc:
        print(f"JClaw Lab token user mapping failed: {exc}", flush=True)
        return None
    profile = USERS.get(username)
    if not profile:
        return None
    user = _public_user(username, profile)
    user["lab_source"] = "jclaw-lab"
    user["lab_sub"] = str(payload.get("sub") or "")
    user["lab_app"] = str(payload.get("app") or "")
    return user


def _get_current_jclaw_lab_user(request: Request) -> Optional[dict]:
    payload = _verify_jclaw_lab_token(_jclaw_lab_token_from_request(request))
    if not payload:
        return None
    try:
        username = _resolve_jclaw_lab_user(payload)
    except Exception as exc:
        print(f"JClaw Lab token user mapping failed: {exc}", flush=True)
        return None
    profile = USERS.get(username)
    if not profile:
        return None
    user = _public_user(username, profile)
    user["lab_source"] = "jclaw-lab"
    user["lab_sub"] = str(payload.get("sub") or "")
    user["lab_app"] = str(payload.get("app") or "")
    return user


def _require_lab_or_user(request: Request) -> tuple[Optional[dict], Optional[JSONResponse]]:
    user = _get_current_user_or_jclaw_lab(request)
    if not user:
        return None, _auth_error("请先通过 JClaw 小程序或网页登录")
    return user, None


def _require_jclaw_lab_user(request: Request) -> tuple[Optional[dict], Optional[JSONResponse]]:
    user = _get_current_jclaw_lab_user(request)
    if not user:
        return None, JSONResponse(
            {
                "error": "这个地址是 JClaw Lab 小程序正式入口，请通过同事 App 打开。浏览器预览请使用 /lab/opennews。",
                "preview_url": "/lab/opennews",
            },
            status_code=401,
        )
    return user, None


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


def _require_admin_user(request: Request, message: str = "只有管理员可以使用该功能") -> tuple[Optional[dict], Optional[JSONResponse]]:
    user, error = _require_user(request)
    if error:
        return None, error
    if not _is_admin(user):
        return None, _forbidden_error(message)
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


def _restore_result_generated_segment_paths(output_dir: Path, result: dict) -> bool:
    """用输出目录里的确定性文件名补回被旧检查点覆盖的分段产物路径。"""
    changed = False
    workflow_config = result.get("workflow_config") if isinstance(result.get("workflow_config"), dict) else {}
    digital_human_engine = str(workflow_config.get("digital_human_engine") or "")
    segments = result.get("segments") if isinstance(result.get("segments"), list) else []
    for index, segment in enumerate(segments):
        if not isinstance(segment, dict):
            continue
        audio_path = str(segment.get("audio_path") or "").strip()
        if not audio_path or not Path(audio_path).exists():
            audio_candidates = sorted((Path(output_dir) / "audio").glob(f"segment_{index:02d}_*.mp3"))
            if audio_candidates:
                segment["audio_path"] = str(audio_candidates[0])
                changed = True
        if str(segment.get("type") or "") != "digital_human":
            continue
        video_path = str(segment.get("video_path") or "").strip()
        candidate = Path(output_dir) / "digital_human" / f"dh_{index:02d}.mp4"
        if (not video_path or not Path(video_path).exists()) and candidate.exists() and candidate.stat().st_size > 0:
            segment["video_path"] = str(candidate)
            if digital_human_engine:
                segment["digital_human_engine"] = digital_human_engine
            changed = True
    return changed


def _opennews_batch_publish_state_for_history(history_id: str, *, limit: int = 200) -> dict:
    """Recover platform receipts that survived in a batch job after result.json was overwritten."""
    history_id = str(history_id or "").strip()
    jobs_dir = OPENNEWS_BATCH_DIR / "batch_jobs"
    if not history_id or not jobs_dir.exists():
        return {}
    recovered: dict[str, Any] = {}
    try:
        job_paths = sorted(
            jobs_dir.glob("opennews_batch_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[: max(20, int(limit or 200))]
    except Exception:
        return {}
    for job_path in job_paths:
        try:
            job = json.loads(job_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for item in (job.get("items") or []) if isinstance(job, dict) else []:
            if not isinstance(item, dict) or str(item.get("history_id") or "").strip() != history_id:
                continue
            for records_key, batch_records_key in (
                ("youtube_publish_records", "youtube_records"),
                ("facebook_publish_records", "facebook_records"),
                ("x_publish_records", "x_records"),
            ):
                records = item.get(records_key) or item.get(batch_records_key)
                if isinstance(records, list) and records:
                    recovered.setdefault(records_key, []).extend(copy.deepcopy(records))
            if recovered:
                recovered["publish_receipts_recovered_from_batch_job"] = job_path.stem
    return recovered


def _load_result_from_output_dir(output_dir: Path) -> Optional[dict]:
    result_path = Path(output_dir) / "result.json"
    if not result_path.exists():
        return None
    try:
        with RESULT_FILE_WRITE_LOCK:
            result = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    batch_publish_state = _opennews_batch_publish_state_for_history(Path(output_dir).name)
    if batch_publish_state:
        result = _merge_result_for_persistence(result, batch_publish_state)
    _restore_result_generated_segment_paths(Path(output_dir), result)
    if not isinstance(result.get("cost_entries"), list):
        result["cost_entries"] = []
    if not result.get("cost_summary"):
        result["cost_summary"] = _summarize_cost_entries(result["cost_entries"])
    return result


_RESULT_PLATFORM_RECORD_KEYS = (
    "youtube_publish_records",
    "facebook_publish_records",
    "x_publish_records",
)
_RESULT_WORKFLOW_IDENTITY_KEYS = (
    "source",
    "opennews_channel_id",
    "opennews_channel_name",
    "opennews_language_markets",
    "opennews_presenter",
    "material_strategy",
    "batch_job_id",
)


def _publish_record_identity(record: Any) -> str:
    if not isinstance(record, dict):
        return str(record)
    for key in ("video_id", "post_id", "tweet_id", "id", "youtube_url", "facebook_url", "x_url", "job_id"):
        value = str(record.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)


def _publish_record_created_at(record: dict) -> float:
    try:
        return float((record or {}).get("created_at") or 0)
    except Exception:
        return 0.0


_RESULT_SEGMENT_ARTIFACT_KEYS = (
    "audio_path",
    "audio_url",
    "tts_provider",
    "video_path",
    "digital_human_engine",
    "material_paths",
    "material_items",
    "material_quality",
)


def _merge_result_segments(existing: Any, incoming: Any) -> list[dict]:
    existing_segments = existing if isinstance(existing, list) else []
    incoming_segments = incoming if isinstance(incoming, list) else []
    if not incoming_segments:
        return copy.deepcopy(existing_segments)
    merged_segments = copy.deepcopy(incoming_segments)
    for index, segment in enumerate(merged_segments):
        if not isinstance(segment, dict) or index >= len(existing_segments):
            continue
        previous = existing_segments[index]
        if not isinstance(previous, dict):
            continue
        if str(previous.get("type") or "") != str(segment.get("type") or ""):
            continue
        for key in _RESULT_SEGMENT_ARTIFACT_KEYS:
            if segment.get(key) in (None, "", [], {}) and previous.get(key) not in (None, "", [], {}):
                segment[key] = copy.deepcopy(previous[key])
    return merged_segments


def _merge_result_for_persistence(existing: Optional[dict], incoming: dict) -> dict:
    """合并可能来自主生产、恢复合成和发布线程的结果，保住路由与平台回执。"""
    existing = existing if isinstance(existing, dict) else {}
    incoming = incoming if isinstance(incoming, dict) else {}
    merged = copy.deepcopy(existing)
    merged.update(copy.deepcopy(incoming))

    existing_workflow = existing.get("workflow_config") if isinstance(existing.get("workflow_config"), dict) else {}
    incoming_workflow = incoming.get("workflow_config") if isinstance(incoming.get("workflow_config"), dict) else {}
    workflow = copy.deepcopy(existing_workflow)
    workflow.update(copy.deepcopy(incoming_workflow))
    for key in _RESULT_WORKFLOW_IDENTITY_KEYS:
        incoming_value = incoming_workflow.get(key)
        existing_value = existing_workflow.get(key)
        routed_to_general = (
            key == "opennews_channel_id"
            and str(incoming_value or "") == "general"
            and str(existing_value or "") not in {"", "general"}
        )
        if (incoming_value in (None, "", [], {}) or routed_to_general) and existing_value not in (None, "", [], {}):
            workflow[key] = copy.deepcopy(existing_workflow[key])
    if workflow:
        merged["workflow_config"] = workflow

    if isinstance(existing.get("segments"), list) or isinstance(incoming.get("segments"), list):
        merged["segments"] = _merge_result_segments(existing.get("segments"), incoming.get("segments"))
        merged["segment_count"] = len(merged["segments"])

    for records_key in _RESULT_PLATFORM_RECORD_KEYS:
        combined: list[dict] = []
        seen: set[str] = set()
        for record in list(incoming.get(records_key) or []) + list(existing.get(records_key) or []):
            if not isinstance(record, dict):
                continue
            identity = _publish_record_identity(record)
            if identity in seen:
                continue
            seen.add(identity)
            combined.append(copy.deepcopy(record))
        if combined:
            combined.sort(key=_publish_record_created_at, reverse=True)
            merged[records_key] = combined[:20]
            latest_key = records_key.replace("_records", "_latest")
            merged[latest_key] = copy.deepcopy(combined[0])
        if incoming.get(records_key):
            platform = records_key.split("_", 1)[0]
            merged.pop(f"{platform}_auto_publish_error", None)
            merged.pop(f"{platform}_publish_error", None)
            if platform == "facebook":
                merged.pop("facebook_publish_pending_at", None)
                merged.pop("facebook_publish_pending_reason", None)
                merged.pop("facebook_publish_throttled", None)

    return merged


def _persist_result_file(output_dir: Path, result: dict) -> dict:
    output_dir = Path(output_dir)
    path = output_dir / "result.json"
    with RESULT_FILE_WRITE_LOCK:
        existing: dict = {}
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    existing = loaded
            except Exception:
                existing = {}
        merged = _merge_result_for_persistence(existing, result)
        _restore_result_generated_segment_paths(output_dir, merged)
        output_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
        tmp_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp_path.replace(path)
        result.clear()
        result.update(copy.deepcopy(merged))
        return merged


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
    digital_human_engine: str = INFINITETALK_ENGINE_ID,
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
        "digital_human_engine": digital_human_engine or INFINITETALK_ENGINE_ID,
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


_OPENNEWS_EXTERNAL_PRODUCE_MARKER = ".opennews-external-produce.json"


def _mark_opennews_external_produce_active(output_dir: Path, *, task_id: str, batch_job_id: str) -> None:
    path = Path(output_dir) / _OPENNEWS_EXTERNAL_PRODUCE_MARKER
    payload = {
        "task_id": str(task_id or ""),
        "batch_job_id": str(batch_job_id or ""),
        "updated_at": time.time(),
    }
    try:
        tmp_path = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(path)
    except Exception as exc:
        print(f"[OpenNews produce owner] marker write failed dir={Path(output_dir).name} err={exc!r}", flush=True)


def _opennews_external_produce_active(output_dir: Path) -> bool:
    path = Path(output_dir) / _OPENNEWS_EXTERNAL_PRODUCE_MARKER
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        updated_at = float((payload or {}).get("updated_at") or path.stat().st_mtime)
    except Exception:
        updated_at = path.stat().st_mtime
    try:
        ttl = max(1800, int(os.getenv("OPENNEWS_EXTERNAL_PRODUCE_MARKER_TTL_SECONDS", "10800") or "10800"))
    except Exception:
        ttl = 10800
    return (time.time() - updated_at) < ttl


def _clear_opennews_external_produce_active(output_dir: Optional[Path]) -> None:
    if not output_dir:
        return
    try:
        (Path(output_dir) / _OPENNEWS_EXTERNAL_PRODUCE_MARKER).unlink()
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"[OpenNews produce owner] marker cleanup failed dir={Path(output_dir).name} err={exc!r}", flush=True)


def _acquire_publish_lock(output_dir: Path, ttl_seconds: int = 1800) -> bool:
    """为某条成片获取“正在发布”互斥锁（原子创建锁文件）。已被占用则返回 False。
    防止生产流程自身的发布与后台恢复工人并发发布同一条视频，导致重复上传。"""
    lock = output_dir / ".publish.lock"
    try:
        fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, str(time.time()).encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        # 处理僵尸锁（进程异常退出未释放）：超过 TTL 视为过期，可抢占
        try:
            ts = float((lock.read_text(encoding="utf-8") or "0").strip() or 0)
        except Exception:
            ts = 0.0
        if time.time() - ts > ttl_seconds:
            try:
                lock.write_text(str(time.time()), encoding="utf-8")
                return True
            except Exception:
                return False
        return False
    except Exception:
        return True  # 锁机制自身异常不应阻断正常发布


def _release_publish_lock(output_dir: Path) -> None:
    try:
        (output_dir / ".publish.lock").unlink()
    except Exception:
        pass


def _schedule_opennews_post_compose_publish(task_id: str, output_dir: str, result_data: dict) -> None:
    """在 OpenNews 流水线尾部自动发布到 X、Facebook 与 YouTube。"""
    def _runner() -> None:
        path = Path(output_dir)
        if not _acquire_publish_lock(path):
            print(f"[opennews_auto_publish] 跳过 {path.name}：已有发布流程在进行（避免重复发布）", flush=True)
            return
        try:
            result = _load_result_from_output_dir(path) or result_data
            material_review = _opennews_material_review_status(result, path)
            if material_review.get("reason"):
                result["material_review"] = material_review
            if not _opennews_result_has_publishable_video(path, result):
                return
            publish_result = _auto_publish_opennews_result_data(path, result)
            final = _load_result_from_output_dir(path) or result
            if material_review:
                final["material_review"] = material_review
            for platform in ("x", "facebook", "youtube"):
                error_value = str(publish_result.get(f"{platform}_error") or "")
                records = publish_result.get(f"{platform}_records") or []
                if error_value:
                    final[f"{platform}_auto_publish_error"] = error_value
                elif records:
                    final.pop(f"{platform}_auto_publish_error", None)
                    final.pop(f"{platform}_publish_error", None)
            _save_result_to_output_dir(path, final)
            try:
                task = tasks.get(task_id)
                if task is not None:
                    task["result"] = final
                    _persist_task_result(task)
            except Exception:
                pass
        except Exception as exc:
            print(f"[opennews_auto_publish] task {task_id} failed: {exc!r}")
        finally:
            _release_publish_lock(path)

    threading.Thread(target=_runner, name=f"opennews-auto-publish-{task_id}", daemon=True).start()

def _persist_task_result(task: dict):
    output_dir = task.get("output_dir")
    result = task.get("result")
    if not output_dir or not result:
        return
    task["result"] = _persist_result_file(Path(output_dir), result)


def _bundle_root_name(history_id: str, result: dict) -> str:
    label = _make_safe_name(result.get("topic") or result.get("title") or history_id, fallback="content_bundle")
    return f"{history_id}_{label}"


def _build_timeline_rows(result: dict) -> list[dict]:
    rows = []
    for index, segment in enumerate(result.get("segments") or [], start=1):
        material_files = []
        for material_index, material_path in enumerate(segment.get("material_paths") or [], start=1):
            suffix = Path(str(material_path)).suffix or ".jpg"
            material_files.append(f"{index:02d}_material_{material_index:02d}{suffix}")
        rows.append({
            "index": index,
            "type": segment.get("type", ""),
            "start": segment.get("start", 0),
            "end": segment.get("end", 0),
            "duration": segment.get("duration", 0),
            "script": segment.get("script", ""),
            "action": segment.get("action", ""),
            "material_keyword": segment.get("material_keyword", ""),
            "material_desc": segment.get("material_desc", ""),
            "audio_file": f"{index:02d}_{segment.get('type', 'segment')}.mp3" if segment.get("audio_path") else "",
            "video_file": f"{index:02d}_digital_human.mp4" if segment.get("video_path") else "",
            "material_files": "|".join(material_files),
        })
    return rows


def _build_bundle_readme(result: dict, history_id: str) -> str:
    lines = [
        "iHouse 剪辑交付包",
        "=" * 40,
        f"任务ID：{history_id}",
        f"选题：{result.get('topic', '')}",
        f"标题：{result.get('title', '')}",
        f"封面标题：{result.get('cover_title', '')}",
        f"总时长：{result.get('total_duration', 0)}秒",
        f"段落数量：{len(result.get('segments') or [])}",
        "",
        "文件夹说明",
        "- 01_脚本：脚本与时间轴说明",
        "- 02_配音：按段落顺序命名的配音文件",
        "- 03_数字人视频：按段落顺序命名的数字人视频",
        "- 04_素材：按段落顺序命名的素材图片",
        "- 05_SNS：SNS 文案",
        "- 06_剪辑时间轴数据：timeline.csv，可直接对应剪辑软件时间轴",
        "- 07_成片：完整视频、封面和字幕",
        "",
        "段落顺序说明",
    ]
    for row in _build_timeline_rows(result):
        lines.append(f"第{row['index']}段 | {row['type']} | {row['start']}s-{row['end']}s | {row['duration']}s")
        if row["audio_file"]:
            lines.append(f"  配音：{row['audio_file']}")
        if row["video_file"]:
            lines.append(f"  数字人视频：{row['video_file']}")
        if row["material_files"]:
            lines.append(f"  素材：{row['material_files']}")
        if row["action"]:
            lines.append(f"  动作提示：{row['action']}")
        if row["material_keyword"] or row["material_desc"]:
            lines.append(f"  素材说明：{row['material_keyword']} {row['material_desc']}")
        lines.append(f"  文案：{row['script']}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_timeline_csv_bytes(result: dict) -> bytes:
    buffer = io.StringIO()
    fieldnames = [
        "index", "type", "start", "end", "duration", "script", "action",
        "material_keyword", "material_desc", "audio_file", "video_file", "material_files",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(_build_timeline_rows(result))
    return buffer.getvalue().encode("utf-8-sig")


def _resolve_bundle_file(output_dir: Path, value: Any) -> Optional[Path]:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    candidates = [path] if path.is_absolute() else [output_dir / path, BASE_DIR / path]
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    return None


def _build_history_bundle_zip(output_dir: Path, result: dict) -> Path:
    output_dir = Path(output_dir).resolve()
    history_id = output_dir.name
    bundle_path = Path("/tmp") / f"{history_id}_bundle.zip"
    temporary_path = Path("/tmp") / f".{history_id}_bundle_{uuid.uuid4().hex}.tmp"
    root = _bundle_root_name(history_id, result)

    with zipfile.ZipFile(temporary_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{root}/00_项目说明/README.txt", _build_bundle_readme(result, history_id))
        archive.writestr(f"{root}/06_剪辑时间轴数据/timeline.csv", _build_timeline_csv_bytes(result))

        for source_name, archive_name in (
            ("script.json", "01_脚本/script.json"),
            ("script_readable.txt", "01_脚本/script_readable.txt"),
            ("social_posts.txt", "05_SNS/social_posts.txt"),
        ):
            source = output_dir / source_name
            if source.is_file():
                archive.write(source, f"{root}/{archive_name}")

        for result_key, archive_stem, fallback_suffix in (
            ("final_video_path", "07_成片/final_video", ".mp4"),
            ("cover_image_path", "07_成片/cover", ".jpg"),
            ("subtitle_path", "07_成片/timeline_subtitles", ".srt"),
        ):
            source = _resolve_bundle_file(output_dir, result.get(result_key))
            if source:
                archive.write(source, f"{root}/{archive_stem}{source.suffix or fallback_suffix}")

        for index, segment in enumerate(result.get("segments") or [], start=1):
            audio_path = _resolve_bundle_file(output_dir, segment.get("audio_path"))
            if audio_path:
                archive.write(audio_path, f"{root}/02_配音/{index:02d}_{segment.get('type', 'segment')}{audio_path.suffix or '.mp3'}")

            video_path = _resolve_bundle_file(output_dir, segment.get("video_path"))
            if video_path:
                archive.write(video_path, f"{root}/03_数字人视频/{index:02d}_digital_human{video_path.suffix or '.mp4'}")

            for material_index, material_value in enumerate(segment.get("material_paths") or [], start=1):
                material_path = _resolve_bundle_file(output_dir, material_value)
                if material_path:
                    archive.write(
                        material_path,
                        f"{root}/04_素材/{index:02d}_material_{material_index:02d}{material_path.suffix or '.jpg'}",
                    )

    temporary_path.replace(bundle_path)
    return bundle_path


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


def _opennews_result_has_publishable_video(output_dir: Path, result: dict, aspect_ratio: str = "vertical") -> bool:
    try:
        _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_ratio)
        return True
    except Exception:
        return False


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
    payload["language_versions"] = _serialize_language_versions_for_ui(output_dir, payload)
    return payload


def _serialize_language_versions_for_ui(output_dir: str, result: dict) -> list[dict]:
    raw_versions = result.get("language_versions")
    if not isinstance(raw_versions, list):
        return []
    serialized: list[dict] = []
    for item in raw_versions:
        if not isinstance(item, dict):
            continue
        workflow_config = item.get("workflow_config") if isinstance(item.get("workflow_config"), dict) else {}
        target_market = str(item.get("target_market") or workflow_config.get("target_market") or "").strip()
        market = _get_target_market(target_market or "cn")
        script_payload = item.get("script") if isinstance(item.get("script"), dict) else {}
        segment_items = item.get("segments") if isinstance(item.get("segments"), list) else []
        segment_scripts = [
            {
                "type": str(seg.get("type") or ""),
                "script": str(seg.get("script") or "").strip(),
            }
            for seg in segment_items
            if isinstance(seg, dict) and str(seg.get("script") or "").strip()
        ]
        script_text = "\n\n".join(part.get("script") or "" for part in segment_scripts).strip()
        if not script_text and isinstance(script_payload, dict):
            script_text = "\n\n".join(
                str(seg.get("script") or "").strip()
                for seg in (script_payload.get("segments") or [])
                if isinstance(seg, dict) and str(seg.get("script") or "").strip()
            ).strip()
        variant_payload = {
            "target_market": target_market,
            "language_label": market.get("content_language") or target_market,
            "title": str(item.get("title") or "").strip(),
            "cover_title": str(item.get("cover_title") or "").strip(),
            "social_post": str(item.get("social_post") or "").strip(),
            "history_id": result.get("history_id") if isinstance(result.get("history_id"), str) else "",
            "script_text": script_text,
            "script_segments": segment_scripts,
            "script_generation_mode": str(item.get("script_generation_mode") or "").strip(),
            "error": str(item.get("error") or "").strip(),
        }
        final_video_url = _history_file_url(output_dir, item.get("final_video_path", ""))
        if final_video_url:
            variant_payload["final_video"] = {
                "url": final_video_url,
                "name": Path(str(item.get("final_video_path", ""))).name or "final_video.mp4",
            }
        raw_video_variants = item.get("final_video_variants")
        if isinstance(raw_video_variants, dict):
            final_video_variants = {}
            for variant_key, variant_data in raw_video_variants.items():
                if not isinstance(variant_data, dict):
                    continue
                variant_video_url = _history_file_url(output_dir, variant_data.get("final_video_path", ""))
                if not variant_video_url:
                    continue
                final_video_variants[str(variant_key)] = {
                    "url": variant_video_url,
                    "name": Path(str(variant_data.get("final_video_path", ""))).name or f"final_video_{variant_key}.mp4",
                    "aspect_ratio": str(variant_data.get("compose_aspect_ratio") or variant_key),
                }
            if final_video_variants:
                variant_payload["final_video_variants"] = final_video_variants
        serialized.append(variant_payload)
    return serialized


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


def _save_result_to_output_dir(output_dir: Path, result: dict) -> dict:
    return _persist_result_file(Path(output_dir), result)


def _platform_metrics_path(output_dir: Path) -> Path:
    return output_dir / "platform_metrics.json"


def _load_platform_metrics(output_dir: Path) -> dict[str, Any]:
    path = _platform_metrics_path(output_dir)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_platform_metrics(output_dir: Path, payload: dict[str, Any]) -> None:
    _platform_metrics_path(output_dir).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _latest_publish_record(records: Any) -> dict[str, Any]:
    if not isinstance(records, list):
        return {}
    for record in records:
        if isinstance(record, dict):
            return record
    return {}


def _collect_history_platform_metrics(output_dir: Path, result: dict, *, force_refresh: bool = False) -> dict[str, Any]:
    existing = _load_platform_metrics(output_dir)
    if existing and not force_refresh:
        return existing
    payload: dict[str, Any] = {
        "history_id": output_dir.name,
        "updated_at": int(time.time()),
        "platforms": {},
    }
    youtube_record = _latest_publish_record(result.get("youtube_publish_records"))
    if youtube_record.get("video_id"):
        try:
            payload["platforms"]["youtube"] = {
                "ok": True,
                "record": youtube_record,
                "metrics": get_youtube_video_metrics(YOUTUBE_TOKEN_STORE_PATH, str(youtube_record.get("video_id") or "")),
            }
        except Exception as exc:
            payload["platforms"]["youtube"] = {"ok": False, "record": youtube_record, "error": str(exc)}
    x_record = _latest_publish_record(result.get("x_publish_records"))
    post_id = str(x_record.get("post_id") or "").strip()
    if post_id:
        try:
            payload["platforms"]["x"] = {
                "ok": True,
                "record": x_record,
                "metrics": get_x_post_metrics(X_TOKEN_STORE_PATH, post_id),
            }
        except Exception as exc:
            payload["platforms"]["x"] = {"ok": False, "record": x_record, "error": str(exc)}
    facebook_record = _latest_publish_record(result.get("facebook_publish_records"))
    facebook_video_id = str(facebook_record.get("video_id") or "").strip()
    if facebook_video_id:
        try:
            payload["platforms"]["facebook"] = {
                "ok": True,
                "record": facebook_record,
                "metrics": get_facebook_video_metrics(FACEBOOK_TOKEN_STORE_PATH, facebook_video_id),
            }
        except Exception as exc:
            payload["platforms"]["facebook"] = {"ok": False, "record": facebook_record, "error": str(exc)}
    language_versions_payload: list[dict[str, Any]] = []
    for version in result.get("language_versions") or []:
        if not isinstance(version, dict):
            continue
        version_payload: dict[str, Any] = {
            "target_market": str(version.get("target_market") or ""),
            "title": str(version.get("title") or ""),
            "platforms": {},
        }
        youtube_record = _latest_publish_record(version.get("youtube_publish_records"))
        if youtube_record.get("video_id"):
            try:
                version_payload["platforms"]["youtube"] = {
                    "ok": True,
                    "record": youtube_record,
                    "metrics": get_youtube_video_metrics(YOUTUBE_TOKEN_STORE_PATH, str(youtube_record.get("video_id") or "")),
                }
            except Exception as exc:
                version_payload["platforms"]["youtube"] = {"ok": False, "record": youtube_record, "error": str(exc)}
        x_record = _latest_publish_record(version.get("x_publish_records"))
        post_id = str(x_record.get("post_id") or "").strip()
        if post_id:
            try:
                version_payload["platforms"]["x"] = {
                    "ok": True,
                    "record": x_record,
                    "metrics": get_x_post_metrics(X_TOKEN_STORE_PATH, post_id),
                }
            except Exception as exc:
                version_payload["platforms"]["x"] = {"ok": False, "record": x_record, "error": str(exc)}
        facebook_record = _latest_publish_record(version.get("facebook_publish_records"))
        facebook_video_id = str(facebook_record.get("video_id") or "").strip()
        if facebook_video_id:
            try:
                version_payload["platforms"]["facebook"] = {
                    "ok": True,
                    "record": facebook_record,
                    "metrics": get_facebook_video_metrics(FACEBOOK_TOKEN_STORE_PATH, facebook_video_id),
                }
            except Exception as exc:
                version_payload["platforms"]["facebook"] = {"ok": False, "record": facebook_record, "error": str(exc)}
        if version_payload["platforms"]:
            language_versions_payload.append(version_payload)
    if language_versions_payload:
        payload["language_versions"] = language_versions_payload
    _save_platform_metrics(output_dir, payload)
    return payload


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


def _resolve_youtube_thumbnail(output_dir: Path, result: dict, aspect_ratio: str = "vertical") -> Path | None:
    aspect_ratio = (aspect_ratio or "vertical").strip().lower()
    variants = result.get("final_video_variants")
    if isinstance(variants, dict):
        preferred = variants.get(aspect_ratio) if isinstance(variants.get(aspect_ratio), dict) else None
        if preferred and preferred.get("cover_image_path"):
            rel = _history_relpath_from_value(str(output_dir), str(preferred.get("cover_image_path") or ""))
            if rel and (output_dir / rel).exists():
                return output_dir / rel
    cover_image_path = str(result.get("cover_image_path") or "")
    rel = _history_relpath_from_value(str(output_dir), cover_image_path)
    if rel and (output_dir / rel).exists():
        return output_dir / rel
    return None


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


def _build_youtube_shorts_metadata(metadata: dict) -> dict:
    title = str((metadata or {}).get("title") or "iHouse OpenNews").strip()
    description = str((metadata or {}).get("description") or "").strip()
    tags = list((metadata or {}).get("tags") or [])

    shorts_marker = "#Shorts"
    if shorts_marker.lower() not in title.lower():
        suffix = f" {shorts_marker}"
        title = f"{title[: max(0, 100 - len(suffix))].rstrip()}{suffix}".strip()
    title = title[:100] or "iHouse OpenNews #Shorts"

    if shorts_marker.lower() not in description.lower():
        description = (description + "\n\n" + shorts_marker).strip()
    description = description[:5000]

    for tag in ("Shorts", "YouTube Shorts"):
        if tag not in tags:
            tags.append(tag)

    return {
        "title": title,
        "description": description,
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


def _opennews_youtube_token_path_for(channel_id: str, target_market: str):
    """取某频道某语言绑定的 YouTube token 路径；频道未单独配置时回退到全局 token。
    这样不同频道/语言可以发布到不同 YouTube 账号，只需在频道配置里设置各自的 token_store_path。"""
    account = _opennews_publish_account_for(channel_id, target_market, "youtube").get("account") or {}
    token_path = str(account.get("token_store_path") or "").strip()
    return Path(token_path) if token_path else YOUTUBE_TOKEN_STORE_PATH


def _opennews_youtube_publish_identity(
    result: dict,
    *,
    channel_id: str,
    target_market: str,
    language_version: str,
    aspect_ratio: str,
) -> dict:
    source_url = _opennews_result_source_url(result)
    event_key = _opennews_event_identity_dedupe_key(_opennews_item_event_identity(result))
    title = re.sub(r"\s+", " ", str(result.get("title") or result.get("topic") or "").strip().lower())
    content_identity = f"url:{source_url}" if source_url else event_key or f"title:{title}"
    account = _opennews_publish_account_for(channel_id, target_market, "youtube").get("account") or {}
    account_scope = str(account.get("channel_name") or account.get("token_store_path") or "default").strip().lower()
    return {
        "platform": "youtube",
        "channel_id": _safe_opennews_channel_id(channel_id),
        "account_scope": account_scope,
        "target_market": str(target_market or "cn").strip().lower(),
        "language_version": str(language_version or "primary").strip().lower(),
        "aspect_ratio": str(aspect_ratio or "vertical").strip().lower(),
        "content_identity": content_identity,
        "source_url": source_url,
    }


def _opennews_youtube_publish_ledger_path(identity: dict) -> Path:
    raw = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR / f"{digest}.json"


def _read_opennews_youtube_publish_ledger(identity: dict) -> dict:
    path = _opennews_youtube_publish_ledger_path(identity)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_opennews_youtube_publish_ledger(identity: dict, payload: dict) -> dict:
    path = _opennews_youtube_publish_ledger_path(identity)
    with OPENNEWS_YOUTUBE_PUBLISH_LEDGER_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        saved = {
            "schema_version": 1,
            "identity": copy.deepcopy(identity),
            **copy.deepcopy(payload),
            "updated_at": time.time(),
        }
        tmp_path = path.with_name(f".{path.name}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp")
        tmp_path.write_text(json.dumps(saved, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp_path.replace(path)
        return saved


def _claim_opennews_youtube_publish(identity: dict, *, history_id: str) -> dict:
    stale_seconds = _opennews_youtube_publish_claim_ttl_seconds()
    with OPENNEWS_YOUTUBE_PUBLISH_LEDGER_LOCK:
        existing = _read_opennews_youtube_publish_ledger(identity)
        if existing.get("status") == "published" and isinstance(existing.get("record"), dict):
            return {"acquired": False, "status": "published", "record": copy.deepcopy(existing["record"])}
        try:
            age_seconds = time.time() - float(existing.get("updated_at") or 0)
        except Exception:
            age_seconds = stale_seconds + 1
        if existing and age_seconds < stale_seconds:
            return {
                "acquired": False,
                "status": str(existing.get("status") or "publishing"),
                "age_seconds": max(0, age_seconds),
            }
        claim_id = uuid.uuid4().hex
        saved = _write_opennews_youtube_publish_ledger(
            identity,
            {
                "status": "publishing",
                "claim_id": claim_id,
                "history_id": str(history_id or ""),
                "started_at": time.time(),
                "takeover": bool(existing),
            },
        )
        return {
            "acquired": True,
            "status": "publishing",
            "claim_id": claim_id,
            "takeover": bool(existing),
            "ledger": saved,
        }


def _opennews_youtube_publish_claim_ttl_seconds() -> int:
    try:
        return max(
            300,
            int(os.getenv("OPENNEWS_YOUTUBE_PUBLISH_CLAIM_TTL_SECONDS", "3600") or "3600"),
        )
    except Exception:
        return 3600


def _opennews_youtube_publish_claim_active(result: dict, *, now_ts: Optional[float] = None) -> bool:
    workflow_config = result.get("workflow_config") if isinstance(result, dict) else {}
    workflow_config = workflow_config if isinstance(workflow_config, dict) else {}
    channel_id = _opennews_result_channel_id(result)
    target_market = str(workflow_config.get("target_market") or "cn")
    aspects_raw = workflow_config.get("youtube_aspects") or ["vertical"]
    if isinstance(aspects_raw, str):
        aspects = [part.strip() for part in aspects_raw.split(",") if part.strip()]
    else:
        aspects = [str(part or "").strip() for part in aspects_raw if str(part or "").strip()]
    now_ts = time.time() if now_ts is None else float(now_ts)
    ttl_seconds = _opennews_youtube_publish_claim_ttl_seconds()
    for aspect in aspects or ["vertical"]:
        identity = _opennews_youtube_publish_identity(
            result,
            channel_id=channel_id,
            target_market=target_market,
            language_version="primary",
            aspect_ratio=aspect,
        )
        state = _read_opennews_youtube_publish_ledger(identity)
        if str(state.get("status") or "") not in {"publishing", "uncertain"}:
            continue
        try:
            age_seconds = now_ts - float(state.get("updated_at") or 0)
        except Exception:
            age_seconds = ttl_seconds + 1
        if age_seconds < ttl_seconds:
            return True
    return False


def _complete_opennews_youtube_publish(identity: dict, record: dict, *, claim_id: str = "") -> None:
    with OPENNEWS_YOUTUBE_PUBLISH_LEDGER_LOCK:
        existing = _read_opennews_youtube_publish_ledger(identity)
        existing_claim_id = str(existing.get("claim_id") or "")
        if claim_id and existing_claim_id and claim_id != existing_claim_id:
            return
        _write_opennews_youtube_publish_ledger(
            identity,
            {
                "status": "published",
                "claim_id": claim_id or existing_claim_id,
                "history_id": str(record.get("history_id") or existing.get("history_id") or ""),
                "published_at": time.time(),
                "record": copy.deepcopy(record),
            },
        )


def _mark_opennews_youtube_publish_uncertain(identity: dict, *, claim_id: str, error: str) -> None:
    with OPENNEWS_YOUTUBE_PUBLISH_LEDGER_LOCK:
        existing = _read_opennews_youtube_publish_ledger(identity)
        if str(existing.get("claim_id") or "") not in {"", str(claim_id or "")}:
            return
        _write_opennews_youtube_publish_ledger(
            identity,
            {
                **existing,
                "status": "uncertain",
                "claim_id": str(claim_id or existing.get("claim_id") or ""),
                "error": str(error or "")[:1000],
            },
        )


def _youtube_record_for_history(record: dict, output_dir: Path) -> dict:
    adapted = copy.deepcopy(record)
    original_history_id = str(adapted.get("history_id") or "").strip()
    if original_history_id and original_history_id != output_dir.name:
        adapted["deduplicated_publish"] = True
        adapted["deduplicated_from_history_id"] = original_history_id
        adapted["history_id"] = output_dir.name
    return adapted


def _publish_opennews_youtube_once(
    output_dir: Path,
    identity_result: dict,
    *,
    channel_id: str,
    target_market: str,
    language_version: str,
    aspect_ratio: str,
    video_path: Path,
    thumbnail_path: Optional[Path],
    metadata: dict,
    privacy_status: str,
    category_id: str,
) -> dict:
    identity = _opennews_youtube_publish_identity(
        identity_result,
        channel_id=channel_id,
        target_market=target_market,
        language_version=language_version,
        aspect_ratio=aspect_ratio,
    )
    claim = _claim_opennews_youtube_publish(identity, history_id=output_dir.name)
    if claim.get("status") == "published" and isinstance(claim.get("record"), dict):
        return _youtube_record_for_history(claim["record"], output_dir)

    token_path = _opennews_youtube_token_path_for(channel_id, target_market)
    source_url = str(identity.get("source_url") or "")
    try:
        remote_record = find_recent_youtube_upload(
            token_path,
            title=str(metadata.get("title") or ""),
            source_url=source_url,
        )
    except Exception as exc:
        if claim.get("takeover"):
            _mark_opennews_youtube_publish_uncertain(
                identity,
                claim_id=str(claim.get("claim_id") or ""),
                error=f"reconcile_failed: {exc}",
            )
            raise YouTubePublishError(f"YouTube 防重核对失败，已暂停重试以避免重复发布：{exc}") from exc
        remote_record = {}
        print(f"[youtube dedup] recent upload lookup failed, fresh publish continues: {exc}", flush=True)

    if remote_record:
        record = {
            "job_id": f"reconciled_opennews_{aspect_ratio}_{int(time.time())}",
            "history_id": output_dir.name,
            "aspect_ratio": aspect_ratio,
            "youtube_format": "shorts" if aspect_ratio == "vertical" else "standard",
            "language_version": language_version,
            "target_market": target_market,
            "video_path": str(video_path),
            "thumbnail_path": str(thumbnail_path) if thumbnail_path else "",
            "created_at": time.time(),
            "privacy_status": privacy_status,
            "recovered_from_youtube": True,
            **remote_record,
        }
        _complete_opennews_youtube_publish(identity, record, claim_id=str(claim.get("claim_id") or ""))
        print(
            f"[youtube dedup] restored existing upload dir={output_dir.name} video_id={record.get('video_id')}",
            flush=True,
        )
        return record

    if not claim.get("acquired"):
        print(
            f"[youtube dedup] publish already in progress dir={output_dir.name} identity={identity.get('content_identity')}",
            flush=True,
        )
        return {}

    try:
        upload_result = upload_video_to_youtube(
            token_path,
            video_path,
            title=metadata["title"],
            description=metadata["description"],
            tags=metadata["tags"],
            privacy_status=privacy_status,
            category_id=category_id,
            made_for_kids=False,
            thumbnail_path=thumbnail_path,
        )
    except Exception as exc:
        _mark_opennews_youtube_publish_uncertain(
            identity,
            claim_id=str(claim.get("claim_id") or ""),
            error=str(exc),
        )
        raise

    record = {
        "job_id": f"auto_opennews_{language_version}_{aspect_ratio}_{int(time.time())}",
        "history_id": output_dir.name,
        "aspect_ratio": aspect_ratio,
        "youtube_format": "shorts" if aspect_ratio == "vertical" else "standard",
        "language_version": language_version,
        "target_market": target_market,
        "video_path": str(video_path),
        "thumbnail_path": str(thumbnail_path) if thumbnail_path else "",
        "created_at": time.time(),
        **upload_result,
    }
    _complete_opennews_youtube_publish(identity, record, claim_id=str(claim.get("claim_id") or ""))
    return record


def _publish_opennews_result_to_youtube(
    output_dir: Path,
    result: dict,
    *,
    aspects: list[str] | tuple[str, ...] = ("horizontal", "vertical"),
    privacy_status: str = "public",
    category_id: str = "25",
    include_language_versions: bool = False,
) -> list[dict]:
    records: list[dict] = []
    metadata = _build_default_youtube_metadata(result)
    existing_records = result.get("youtube_publish_records")
    if not isinstance(existing_records, list):
        existing_records = []
    channel_id = _opennews_result_channel_id(result)
    for aspect in aspects:
        aspect_key = str(aspect or "").strip().lower()
        if aspect_key not in {"horizontal", "vertical"}:
            continue
        target_market = str((result.get("workflow_config") or {}).get("target_market") or "cn")
        if not _opennews_publish_account_for(channel_id, target_market, "youtube").get("enabled", True):
            continue
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_key)
        thumbnail_path = _resolve_youtube_thumbnail(output_dir, result, aspect_ratio=aspect_key)
        upload_metadata = _build_youtube_shorts_metadata(metadata) if aspect_key == "vertical" else metadata
        record = _publish_opennews_youtube_once(
            output_dir,
            result,
            channel_id=channel_id,
            target_market=target_market,
            language_version="primary",
            aspect_ratio=aspect_key,
            video_path=video_path,
            thumbnail_path=thumbnail_path,
            metadata=upload_metadata,
            privacy_status=privacy_status,
            category_id=category_id,
        )
        if not record:
            continue
        existing_records.insert(0, record)
        records.append(record)
    if records:
        result["youtube_publish_records"] = existing_records[:20]
        result["youtube_publish_latest"] = records[-1]
    if include_language_versions:
        for version in result.get("language_versions") or []:
            if not isinstance(version, dict) or version.get("error"):
                continue
            target_market = str(version.get("target_market") or "").strip()
            if not target_market:
                continue
            if not _opennews_publish_account_for(channel_id, target_market, "youtube").get("enabled", True):
                continue
            version_metadata = _build_default_youtube_metadata(
                version,
                title=str(version.get("title") or metadata.get("title") or ""),
                description=str(version.get("social_post") or metadata.get("description") or ""),
                tags=["OpenNews", "iHouse", target_market.upper()],
            )
            version_records = version.get("youtube_publish_records")
            if not isinstance(version_records, list):
                version_records = []
            for aspect in aspects:
                aspect_key = str(aspect or "").strip().lower()
                if aspect_key not in {"horizontal", "vertical"}:
                    continue
                video_path = _resolve_youtube_publish_video(output_dir, version, aspect_ratio=aspect_key)
                thumbnail_path = _resolve_youtube_thumbnail(output_dir, version, aspect_ratio=aspect_key)
                version_upload_metadata = _build_youtube_shorts_metadata(version_metadata) if aspect_key == "vertical" else version_metadata
                record = _publish_opennews_youtube_once(
                    output_dir,
                    result,
                    channel_id=channel_id,
                    target_market=target_market,
                    language_version=target_market,
                    aspect_ratio=aspect_key,
                    video_path=video_path,
                    thumbnail_path=thumbnail_path,
                    metadata=version_upload_metadata,
                    privacy_status=privacy_status,
                    category_id=category_id,
                )
                if not record:
                    continue
                version_records.insert(0, record)
                records.append(record)
            if version_records:
                version["youtube_publish_records"] = version_records[:20]
                version["youtube_publish_latest"] = version_records[0]
    _save_result_to_output_dir(output_dir, result)
    return records


def _fit_x_post_text(title: str, *, source_name: str = "", source_url: str = "", suffix: str = "#OpenNews #iHouse") -> str:
    title = re.sub(r"\s+", " ", str(title or "").strip()) or "iHouse OpenNews"
    source_name = re.sub(r"\s+", " ", str(source_name or "").strip())
    source_url = str(source_url or "").strip()
    suffix = str(suffix or "").strip()
    trailing_lines = []
    if source_name:
        trailing_lines.append(f"来源：{source_name}")
    if source_url:
        trailing_lines.append(source_url)
    if suffix:
        trailing_lines.append(suffix)
    trailing = "\n".join(trailing_lines)
    text = f"{title}\n{trailing}" if trailing else title
    if len(text) <= 280:
        return text
    available = 280 - len(trailing) - (1 if trailing else 0)
    if available < 24:
        available = 24
        trailing = "\n".join(line for line in trailing_lines if not line.startswith("来源："))
    trimmed_title = title[: max(1, available - 3)].rstrip() + "..."
    text = f"{trimmed_title}\n{trailing}" if trailing else trimmed_title
    return text[:280]


def _build_default_x_post_text(result: dict, *, title: str = "", suffix: str = "#OpenNews #iHouse") -> str:
    workflow_config = result.get("workflow_config") or {}
    source = (workflow_config.get("source") or {}).get("article") or {}
    default_title = (
        title
        or result.get("title")
        or ((result.get("script") or {}).get("title") if isinstance(result.get("script"), dict) else "")
        or result.get("topic")
        or "iHouse OpenNews"
    )
    return _fit_x_post_text(
        str(default_title),
        source_name=str(source.get("source_name") or source.get("trend_domain") or ""),
        source_url=str(source.get("url") or ""),
        suffix=suffix,
    )


def _fit_facebook_post_text(
    title: str,
    *,
    summary: str = "",
    source_name: str = "",
    source_url: str = "",
    suffix: str = "#OpenNews #iHouse",
) -> str:
    title = re.sub(r"\s+", " ", str(title or "").strip()) or "iHouse OpenNews"
    summary = re.sub(r"\s+", " ", str(summary or "").strip())
    source_name = re.sub(r"\s+", " ", str(source_name or "").strip())
    source_url = str(source_url or "").strip()
    suffix = str(suffix or "").strip()
    parts = [title]
    if summary:
        parts.append(summary[:400])
    if source_name:
        parts.append(f"来源：{source_name}")
    if source_url:
        parts.append(source_url)
    if suffix:
        parts.append(suffix)
    return "\n".join(part for part in parts if part).strip()[:5000]


def _build_default_facebook_post_text(result: dict, *, title: str = "", suffix: str = "#OpenNews #iHouse") -> str:
    workflow_config = result.get("workflow_config") or {}
    source = (workflow_config.get("source") or {}).get("article") or {}
    default_title = (
        title
        or result.get("title")
        or ((result.get("script") or {}).get("title") if isinstance(result.get("script"), dict) else "")
        or result.get("topic")
        or "iHouse OpenNews"
    )
    summary = ""
    script = result.get("script") if isinstance(result.get("script"), dict) else {}
    if isinstance(script, dict):
        summary = str(script.get("summary") or script.get("social_post") or "").strip()
    return _fit_facebook_post_text(
        str(default_title),
        summary=summary,
        source_name=str(source.get("source_name") or source.get("trend_domain") or ""),
        source_url=str(source.get("url") or ""),
        suffix=suffix,
    )


def _update_x_upload_job(job_id: str, **updates: Any) -> dict:
    with X_UPLOAD_LOCK:
        job = X_UPLOAD_JOBS.get(job_id, {})
        job.update(updates)
        job["updated_at"] = time.time()
        X_UPLOAD_JOBS[job_id] = job
        return dict(job)


def _run_x_upload_job(job_id: str) -> None:
    job = _update_x_upload_job(job_id, status="running", message="正在上传到 X...")
    try:
        output_dir = Path(job.get("output_dir") or "")
        result = _load_result_from_output_dir(output_dir)
        if not result:
            raise XPublishError("历史结果不存在，无法上传 X")
        upload_result = _upload_video_to_opennews_x(
            Path(job.get("video_path") or ""),
            text=str(job.get("text") or ""),
            made_with_ai=bool(job.get("made_with_ai", True)),
        )
        publish_record = {
            "job_id": job_id,
            "history_id": output_dir.name,
            "aspect_ratio": job.get("aspect_ratio") or "",
            "video_path": job.get("video_path") or "",
            "created_at": time.time(),
            **upload_result,
        }
        records = result.get("x_publish_records")
        if not isinstance(records, list):
            records = []
        records.insert(0, publish_record)
        result["x_publish_records"] = records[:20]
        result["x_publish_latest"] = publish_record
        _save_result_to_output_dir(output_dir, result)
        _update_x_upload_job(
            job_id,
            status="done",
            message="X 发布完成",
            result=upload_result,
            x_url=upload_result.get("x_url", ""),
            post_id=upload_result.get("post_id", ""),
        )
    except Exception as exc:
        _update_x_upload_job(job_id, status="failed", message=str(exc), error=str(exc))


def _upload_video_to_opennews_x(
    video_path: Path,
    *,
    text: str,
    made_with_ai: bool = True,
    account_config: Optional[dict] = None,
) -> dict:
    account_config = account_config if isinstance(account_config, dict) else {}
    mode = _opennews_x_publish_mode()
    if mode == "browser":
        try:
            login_status = x_browser_login_status()
            if login_status.get("running"):
                raise XBrowserPublishError("X 可视化登录窗口仍在运行，请先结束登录会话再自动发布。")
            if login_status.get("degraded") or login_status.get("profile_in_use"):
                stop_x_browser_login()
            result = publish_video_to_x_browser(
                video_path,
                text=text,
                made_with_ai=made_with_ai,
                user_data_dir=account_config.get("profile_dir") or None,
            )
            result["publish_mode"] = "browser"
            return result
        except XBrowserPublishError:
            raise
        except Exception as exc:
            raise XBrowserPublishError(f"X 浏览器自动发布失败：{exc}") from exc
    result = upload_video_to_x(
        X_TOKEN_STORE_PATH,
        video_path,
        text=text,
        made_with_ai=made_with_ai,
    )
    result["publish_mode"] = "api"
    return result


def _publish_opennews_result_to_x(
    output_dir: Path,
    result: dict,
    *,
    aspects: list[str] | tuple[str, ...] = ("vertical",),
    text: str = "",
    include_language_versions: bool = True,
) -> list[dict]:
    records: list[dict] = []
    existing_records = result.get("x_publish_records")
    if not isinstance(existing_records, list):
        existing_records = []
    channel_id = _opennews_result_channel_id(result)
    for aspect in aspects:
        aspect_key = str(aspect or "").strip().lower()
        if aspect_key not in {"horizontal", "vertical"}:
            continue
        target_market = str((result.get("workflow_config") or {}).get("target_market") or "cn")
        publish_target = _opennews_publish_account_for(channel_id, target_market, "x")
        if not publish_target.get("enabled", True):
            continue
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_key)
        post_text = str(text or "").strip() or _build_default_x_post_text(result)
        upload_result = _upload_video_to_opennews_x(
            video_path,
            text=post_text,
            made_with_ai=True,
            account_config=publish_target.get("account") or {},
        )
        record = {
            "job_id": f"auto_opennews_x_{aspect_key}_{int(time.time())}",
            "history_id": output_dir.name,
            "aspect_ratio": aspect_key,
            "language_version": "primary",
            "target_market": target_market,
            "opennews_channel_id": publish_target.get("channel_id") or channel_id,
            "opennews_channel_name": publish_target.get("channel_name") or "",
            "publish_account": publish_target.get("public_account") or {},
            "video_path": str(video_path),
            "created_at": time.time(),
            **upload_result,
        }
        existing_records.insert(0, record)
        records.append(record)
    if records:
        result["x_publish_records"] = existing_records[:20]
        result["x_publish_latest"] = records[-1]
        result.pop("x_auto_publish_error", None)
        result.pop("x_publish_error", None)
    if include_language_versions:
        for version in result.get("language_versions") or []:
            if not isinstance(version, dict) or version.get("error"):
                continue
            target_market = str(version.get("target_market") or "").strip()
            if not target_market:
                continue
            version_records = version.get("x_publish_records")
            if not isinstance(version_records, list):
                version_records = []
            version_error = ""
            for aspect in aspects:
                aspect_key = str(aspect or "").strip().lower()
                if aspect_key not in {"horizontal", "vertical"}:
                    continue
                try:
                    publish_target = _opennews_publish_account_for(channel_id, target_market, "x")
                    if not publish_target.get("enabled", True):
                        continue
                    video_path = _resolve_youtube_publish_video(output_dir, version, aspect_ratio=aspect_key)
                    post_text = str(text or "").strip() or _build_default_x_post_text(version, title=str(version.get("title") or ""))
                    upload_result = _upload_video_to_opennews_x(
                        video_path,
                        text=post_text,
                        made_with_ai=True,
                        account_config=publish_target.get("account") or {},
                    )
                    record = {
                        "job_id": f"auto_opennews_x_{target_market}_{aspect_key}_{int(time.time())}",
                        "history_id": output_dir.name,
                        "aspect_ratio": aspect_key,
                        "language_version": target_market,
                        "target_market": target_market,
                        "opennews_channel_id": publish_target.get("channel_id") or channel_id,
                        "opennews_channel_name": publish_target.get("channel_name") or "",
                        "publish_account": publish_target.get("public_account") or {},
                        "video_path": str(video_path),
                        "created_at": time.time(),
                        **upload_result,
                    }
                    version_records.insert(0, record)
                    records.append(record)
                except Exception as exc:
                    version_error = str(exc)
            if version_records:
                version["x_publish_records"] = version_records[:20]
                version["x_publish_latest"] = version_records[0]
                version.pop("x_auto_publish_error", None)
                version.pop("x_publish_error", None)
            elif version_error:
                version["x_auto_publish_error"] = version_error
                version["x_publish_error"] = version_error
    _save_result_to_output_dir(output_dir, result)
    return records


def _update_facebook_upload_job(job_id: str, **updates: Any) -> dict:
    with FACEBOOK_UPLOAD_LOCK:
        job = FACEBOOK_UPLOAD_JOBS.get(job_id, {})
        job.update(updates)
        job["updated_at"] = time.time()
        FACEBOOK_UPLOAD_JOBS[job_id] = job
        return dict(job)


def _run_facebook_upload_job(job_id: str) -> None:
    job = _update_facebook_upload_job(job_id, status="running", message="正在上传到 Facebook...")
    try:
        output_dir = Path(job.get("output_dir") or "")
        result = _load_result_from_output_dir(output_dir)
        if not result:
            raise FacebookPublishError("历史结果不存在，无法上传 Facebook")
        upload_result = upload_video_to_facebook_page(
            FACEBOOK_TOKEN_STORE_PATH,
            Path(job.get("video_path") or ""),
            description=str(job.get("text") or ""),
            title=str(job.get("title") or ""),
        )
        publish_record = {
            "job_id": job_id,
            "history_id": output_dir.name,
            "aspect_ratio": job.get("aspect_ratio") or "",
            "video_path": job.get("video_path") or "",
            "created_at": time.time(),
            **upload_result,
        }
        records = result.get("facebook_publish_records")
        if not isinstance(records, list):
            records = []
        records.insert(0, publish_record)
        result["facebook_publish_records"] = records[:20]
        result["facebook_publish_latest"] = publish_record
        _save_result_to_output_dir(output_dir, result)
        _update_facebook_upload_job(
            job_id,
            status="done",
            message="Facebook 发布完成",
            result=upload_result,
            facebook_url=upload_result.get("facebook_url", ""),
            video_id=upload_result.get("video_id", ""),
        )
    except Exception as exc:
        _update_facebook_upload_job(job_id, status="failed", message=str(exc), error=str(exc))


def _publish_opennews_result_to_facebook(
    output_dir: Path,
    result: dict,
    *,
    aspects: list[str] | tuple[str, ...] = ("vertical",),
    text: str = "",
    include_language_versions: bool = True,
) -> list[dict]:
    with FACEBOOK_PUBLISH_SERIAL_LOCK:
        return _publish_opennews_result_to_facebook_locked(
            output_dir,
            result,
            aspects=aspects,
            text=text,
            include_language_versions=include_language_versions,
        )


def _publish_opennews_result_to_facebook_locked(
    output_dir: Path,
    result: dict,
    *,
    aspects: list[str] | tuple[str, ...] = ("vertical",),
    text: str = "",
    include_language_versions: bool = True,
) -> list[dict]:
    records: list[dict] = []
    existing_records = result.get("facebook_publish_records")
    if not isinstance(existing_records, list):
        existing_records = []
    channel_id = _opennews_result_channel_id(result)
    # 该频道主语言用哪个 Page(用于按 Page 分桶节流/冷却)。
    _primary_market = str((result.get("workflow_config") or {}).get("target_market") or "cn")
    _primary_pt = _opennews_publish_account_for(channel_id, _primary_market, "facebook")
    if not _primary_pt.get("enabled", True):
        return records
    fb_page_key = str((_primary_pt.get("account") or {}).get("page_id") or "")
    # 368 频率封锁冷却中:直接跳过 FB 发布,记明原因,不硬撞。
    cooldown_remaining = _facebook_publish_cooldown_remaining(fb_page_key)
    if cooldown_remaining > 0:
        note = f"Facebook 频率封锁(368)冷却中，约 {int(cooldown_remaining/60)} 分钟后自动补发。"
        _mark_facebook_publish_pending(result, retry_at=time.time() + cooldown_remaining, reason=note)
        _save_result_to_output_dir(output_dir, result)
        print(f"[facebook throttle] 跳过 {output_dir.name}：{note}", flush=True)
        return records
    for aspect in aspects:
        aspect_key = str(aspect or "").strip().lower()
        if aspect_key not in {"horizontal", "vertical"}:
            continue
        target_market = str((result.get("workflow_config") or {}).get("target_market") or "cn")
        publish_target = _opennews_publish_account_for(channel_id, target_market, "facebook")
        if not publish_target.get("enabled", True):
            continue
        account_config = publish_target.get("account") or {}
        page_key = str(account_config.get("page_id") or "") or fb_page_key
        # 发帖最小间隔节流:距上次成功发帖太近则本条跳过(不算失败),让发帖节奏拉开避免被封。
        throttle_remaining = _facebook_publish_throttle_remaining(page_key)
        if throttle_remaining > 0:
            note = f"距上次 Facebook 发帖不足最小间隔，约 {int(throttle_remaining/60)} 分钟后自动补发。"
            result["facebook_publish_throttled"] = note
            _mark_facebook_publish_pending(result, retry_at=time.time() + throttle_remaining, reason=note)
            print(f"[facebook throttle] 跳过 {output_dir.name}：{note}", flush=True)
            continue
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_key)
        post_text = str(text or "").strip() or _build_default_facebook_post_text(result)
        try:
            upload_result = upload_video_to_facebook_page(
                FACEBOOK_TOKEN_STORE_PATH,
                video_path,
                description=post_text,
                title=str(result.get("title") or result.get("topic") or "OpenNews"),
                page_id=str(account_config.get("page_id") or ""),
                page_access_token=str(account_config.get("page_access_token") or ""),
            )
        except Exception as exc:
            if _is_facebook_frequency_block(str(exc)):
                retry_at = _trigger_facebook_publish_cooldown(str(exc), page_id=page_key)
                _mark_facebook_publish_pending(result, retry_at=retry_at, reason="Facebook 368 限制，已排队等待自动补发。")
            raise
        _record_facebook_publish_success(page_key)
        record = {
            "job_id": f"auto_opennews_facebook_{aspect_key}_{int(time.time())}",
            "history_id": output_dir.name,
            "aspect_ratio": aspect_key,
            "language_version": "primary",
            "target_market": target_market,
            "opennews_channel_id": publish_target.get("channel_id") or channel_id,
            "opennews_channel_name": publish_target.get("channel_name") or "",
            "publish_account": publish_target.get("public_account") or {},
            "video_path": str(video_path),
            "created_at": time.time(),
            **upload_result,
        }
        existing_records.insert(0, record)
        records.append(record)
    if records:
        result["facebook_publish_records"] = existing_records[:20]
        result["facebook_publish_latest"] = records[-1]
        _clear_facebook_publish_pending(result)
        result.pop("facebook_auto_publish_error", None)
        result.pop("facebook_publish_error", None)
    if include_language_versions:
        for version in result.get("language_versions") or []:
            if not isinstance(version, dict) or version.get("error"):
                continue
            target_market = str(version.get("target_market") or "").strip()
            if not target_market:
                continue
            version_records = version.get("facebook_publish_records")
            if not isinstance(version_records, list):
                version_records = []
            version_error = ""
            for aspect in aspects:
                aspect_key = str(aspect or "").strip().lower()
                if aspect_key not in {"horizontal", "vertical"}:
                    continue
                try:
                    if _facebook_publish_cooldown_remaining() > 0 or _facebook_publish_throttle_remaining() > 0:
                        continue
                    publish_target = _opennews_publish_account_for(channel_id, target_market, "facebook")
                    if not publish_target.get("enabled", True):
                        continue
                    video_path = _resolve_youtube_publish_video(output_dir, version, aspect_ratio=aspect_key)
                    post_text = str(text or "").strip() or _build_default_facebook_post_text(version, title=str(version.get("title") or ""))
                    account_config = publish_target.get("account") or {}
                    try:
                        upload_result = upload_video_to_facebook_page(
                            FACEBOOK_TOKEN_STORE_PATH,
                            video_path,
                            description=post_text,
                            title=str(version.get("title") or ""),
                            page_id=str(account_config.get("page_id") or ""),
                            page_access_token=str(account_config.get("page_access_token") or ""),
                        )
                    except Exception as up_exc:
                        if _is_facebook_frequency_block(str(up_exc)):
                            _trigger_facebook_publish_cooldown(str(up_exc))
                        raise
                    _record_facebook_publish_success()
                    record = {
                        "job_id": f"auto_opennews_facebook_{target_market}_{aspect_key}_{int(time.time())}",
                        "history_id": output_dir.name,
                        "aspect_ratio": aspect_key,
                        "language_version": target_market,
                        "target_market": target_market,
                        "opennews_channel_id": publish_target.get("channel_id") or channel_id,
                        "opennews_channel_name": publish_target.get("channel_name") or "",
                        "publish_account": publish_target.get("public_account") or {},
                        "video_path": str(video_path),
                        "created_at": time.time(),
                        **upload_result,
                    }
                    version_records.insert(0, record)
                    records.append(record)
                except Exception as exc:
                    version_error = str(exc)
            if version_records:
                version["facebook_publish_records"] = version_records[:20]
                version["facebook_publish_latest"] = version_records[0]
                version.pop("facebook_auto_publish_error", None)
                version.pop("facebook_publish_error", None)
            elif version_error:
                version["facebook_auto_publish_error"] = version_error
                version["facebook_publish_error"] = version_error
    _save_result_to_output_dir(output_dir, result)
    return records


def _opennews_material_review_status(result: dict, output_dir: Path | None = None) -> dict:
    segments = result.get("segments") if isinstance(result, dict) else []
    fallback_items: list[dict] = []
    unsafe_items: list[dict] = []
    source_counts: dict[str, int] = {}
    duplicate_image_audit = {"ok": True, "duplicate_group_count": 0, "duplicate_groups": []}
    if not isinstance(segments, list):
        segments = []
    if output_dir and output_dir.exists():
        try:
            duplicate_image_audit = audit_result_image_duplicates(result, output_dir)
        except Exception:
            duplicate_image_audit = {"ok": True, "duplicate_group_count": 0, "duplicate_groups": []}
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        quality = segment.get("material_quality") if isinstance(segment.get("material_quality"), dict) else {}
        if quality.get("strict_source_fallback_used"):
            if quality.get("source_unreviewed_count"):
                unsafe_items.append({
                    "segment_index": segment.get("index") or segment.get("segment_index") or len(unsafe_items) + 1,
                    "reason": "网络新闻源素材未全部经过 Qwen3-VL 语义/安全审核",
                    "quality": quality,
                })
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
                if source == "opennews_source" and item.get("qwen_review_score") is None:
                    unsafe_items.append({
                        "segment_index": segment.get("index") or segment.get("segment_index") or len(unsafe_items) + 1,
                        "source": source,
                        "path": item.get("path") or "",
                        "source_url": item.get("source_url") or "",
                        "title": item.get("title") or "",
                        "reason": "网络新闻源素材缺少 Qwen3-VL 审核记录",
                    })
                fallback_items.append({
                    "segment_index": segment.get("index") or segment.get("segment_index") or len(fallback_items) + 1,
                    "source": source,
                    "path": item.get("path") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": item.get("fallback_reason") or "使用了严格新闻源兜底素材",
                })
    duplicate_image_blocking = _env_flag("OPENNEWS_AUTO_PUBLISH_DUPLICATE_IMAGE_BLOCKING", "0")
    review_blocks_publish = _opennews_material_review_blocks_publish()
    duplicate_images_detected = not bool(duplicate_image_audit.get("ok", True))
    blocking_issue_detected = bool(unsafe_items) or (duplicate_images_detected and duplicate_image_blocking)
    auto_publish_allowed = (not blocking_issue_detected) or (not review_blocks_publish)
    review_reason = ""
    if unsafe_items and review_blocks_publish:
        review_reason = "网络新闻源素材缺少 Qwen3-VL 审核，已禁止自动发布。"
    elif unsafe_items:
        review_reason = "网络新闻源素材缺少 Qwen3-VL 审核记录，已按宽松策略记录警告但不阻断自动发布。"
    elif duplicate_images_detected and duplicate_image_blocking and review_blocks_publish:
        review_reason = "素材审核发现重复图片，已禁止自动发布。"
    elif duplicate_images_detected and duplicate_image_blocking:
        review_reason = "素材审核发现重复图片，已按宽松策略记录警告但不阻断自动发布。"
    elif duplicate_images_detected:
        review_reason = "素材审核发现重复图片，已按宽松策略记录警告但不阻断自动发布。"
    elif fallback_items:
        review_reason = "5090 AI图片完全不可用，成片使用了严格新闻源兜底素材，且已通过 Qwen3-VL 审核。"
    return {
        "requires_human_review": blocking_issue_detected and review_blocks_publish,
        "auto_publish_allowed": auto_publish_allowed,
        "review_blocks_publish": review_blocks_publish,
        "uses_strict_source_fallback": bool(fallback_items),
        "reason": review_reason,
        "fallback_items": fallback_items[:30],
        "unsafe_items": unsafe_items[:30],
        "source_counts": source_counts,
        "duplicate_images_detected": duplicate_images_detected,
        "duplicate_image_blocking": duplicate_image_blocking,
        "duplicate_image_group_count": int(duplicate_image_audit.get("duplicate_group_count") or 0),
        "duplicate_image_groups": list(duplicate_image_audit.get("duplicate_groups") or [])[:30],
    }


def _opennews_publish_records_have_aspect(records: Any, aspect_ratio: str) -> bool:
    aspect_key = str(aspect_ratio or "").strip().lower()
    if not aspect_key:
        return False
    return any(
        isinstance(record, dict) and str(record.get("aspect_ratio") or "").strip().lower() == aspect_key
        for record in (records or [])
    )


def _load_opennews_history_result_for_publish(history_id: str) -> tuple[Path, dict]:
    output_dir = _resolve_history_output_dir(history_id)
    if not output_dir:
        raise RuntimeError(f"历史记录不存在：{history_id}")
    result = _load_result_from_output_dir(output_dir)
    if not result or not _is_opennews_result(result):
        raise RuntimeError(f"历史记录不是可发布的 OpenNews 成片：{history_id}")
    return output_dir, result


def _list_avatar_options(target_market_id: Optional[str] = None, include_all: bool = False) -> list[dict]:
    from PIL import Image

    items = []
    manifest = _load_avatar_library_manifest()
    preferred_order = {
        "avatar_test_0cd3d70a.png": 0,
        "avatar_host_c.png": 1,
        "avatar_host_d.png": 2,
        "avatar_ultraman.png": 3,
        "avatar_test_new_01.png": 4,
        "avatar_test_aec9a0f0.png": 5,
        "avatar_custom_林晨专属_male_manual.png": 6,
    }
    for path in sorted(ASSETS_DIR.iterdir() if ASSETS_DIR.exists() else [], key=lambda p: (preferred_order.get(p.name, 999), p.name)):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        if path.name in AVATAR_OPTION_EXCLUDE_FILENAMES or path.stem in {"ihouse-logo"}:
            continue
        lower_name = path.name.lower()
        if lower_name.startswith("opennews_anchor_daily"):
            continue
        try:
            with Image.open(path) as image:
                width, height = image.size
            if width >= height:
                continue
        except Exception:
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
    tts_queue = _qwen_tts_queue_snapshot()
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
            tts_waiting_hit = next((item for item in tts_queue.get("waiting", []) if item.get("owner_username") == username), None)
            tts_running_hit = next((item for item in tts_queue.get("running", []) if item.get("owner_username") == username), None)
            if running_hit:
                status = "数字人生成中"
            elif waiting_hit:
                status = "数字人排队中"
            elif tts_running_hit:
                status = "5090 配音生成中"
            elif tts_waiting_hit:
                status = "5090 配音排队中"
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
                "qwen_tts_queue": {
                    "waiting": bool(next((item for item in tts_queue.get("waiting", []) if item.get("task_id") == current_task.get("id")), None)),
                    "running": bool(next((item for item in tts_queue.get("running", []) if item.get("task_id") == current_task.get("id")), None)),
                },
            })
    return {
        "summary": {
            "running_task_count": len(active_tasks),
            "waiting_queue_count": queue.get("waiting_count", 0),
            "qwen_tts_waiting_count": tts_queue.get("waiting_count", 0),
            "qwen_tts_running_count": tts_queue.get("running_count", 0),
            "current_owner_username": queue.get("current_owner_username", ""),
            "current_owner_display_name": queue.get("current_owner_display_name", ""),
            "completed_today": completed_today,
        },
        "queue": queue,
        "qwen_tts_queue": tts_queue,
        "users": users,
        "active_tasks": active_tasks,
        "recent_events": _recent_live_events(),
    }


def _admin_service_card(
    key: str,
    title: str,
    *,
    level: str = "info",
    state_label: str = "",
    summary: str = "",
    detail: str = "",
    meta: Optional[list[dict[str, str]]] = None,
) -> dict:
    return {
        "key": key,
        "title": title,
        "level": level,
        "state_label": state_label,
        "summary": summary,
        "detail": detail,
        "meta": meta or [],
    }


def _admin_service_warning(level: str, title: str, detail: str, *, service_key: str = "") -> dict:
    return {
        "level": level,
        "title": title,
        "detail": detail,
        "service_key": service_key,
    }


def _x_browser_profile_snapshot() -> dict:
    profile_dir = x_browser_profile_dir()
    file_count = 0
    size_bytes = 0
    for path in profile_dir.rglob("*"):
        if not path.is_file():
            continue
        file_count += 1
        try:
            size_bytes += path.stat().st_size
        except Exception:
            pass
    return {
        "profile_dir": str(profile_dir),
        "file_count": file_count,
        "size_bytes": size_bytes,
        "auth_ready": x_browser_auth_ready(),
    }


def _latest_x_browser_account_context(limit: int = 50) -> dict:
    env = x_browser_env_config()
    state_dir = Path(str(env.get("state_dir") or (OUTPUT_DIR / "x_browser"))).expanduser().resolve()
    debug_dir = Path(os.getenv("X_BROWSER_DEBUG_DIR", str(state_dir / "debug"))).expanduser().resolve()
    if not debug_dir.exists():
        return {}
    try:
        debug_logs = sorted(
            debug_dir.glob("x_publish_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[:limit]
    except Exception:
        return {}
    for path in debug_logs:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        for event in reversed(payload):
            if not isinstance(event, dict) or event.get("stage") != "account_context":
                continue
            handle = str(event.get("expected_handle") or "").strip().lstrip("@")
            if not handle:
                continue
            try:
                updated_at = path.stat().st_mtime
            except Exception:
                updated_at = 0.0
            return {
                "handle": handle,
                "debug_log": str(path),
                "updated_at": updated_at,
                "source": "browser_debug",
            }
    return {}


def _build_opennews_bound_accounts_summary() -> dict:
    x_publish_mode = _opennews_x_publish_mode()
    x_config = x_env_config()
    x_profile = _x_browser_profile_snapshot()
    x_login = x_browser_login_status()
    x_store = load_x_token_store(X_TOKEN_STORE_PATH) if X_TOKEN_STORE_PATH.exists() else {}
    x_user = x_store.get("user") if isinstance(x_store.get("user"), dict) else {}
    x_debug = _latest_x_browser_account_context()
    x_handle = str(x_debug.get("handle") or x_user.get("username") or "").strip().lstrip("@")
    x_display_name = str(x_user.get("name") or "").strip()
    if not x_display_name and x_handle:
        x_display_name = f"@{x_handle}"
    x_configured = (
        bool(x_profile.get("auth_ready"))
        if x_publish_mode == "browser"
        else bool(
            x_config.get("client_id")
            and x_config.get("redirect_uri")
            and (x_config.get("refresh_token") or X_TOKEN_STORE_PATH.exists())
        )
    )

    facebook_config = facebook_env_config()
    facebook_store = load_facebook_token_store(FACEBOOK_TOKEN_STORE_PATH) if FACEBOOK_TOKEN_STORE_PATH.exists() else {}
    facebook_user = facebook_store.get("user") if isinstance(facebook_store.get("user"), dict) else {}
    facebook_configured = bool(
        facebook_config.get("app_id")
        and facebook_config.get("app_secret")
        and facebook_config.get("redirect_uri")
        and (
            (facebook_config.get("page_id") and facebook_config.get("page_access_token"))
            or FACEBOOK_TOKEN_STORE_PATH.exists()
        )
    )
    facebook_page_id = str(facebook_store.get("page_id") or facebook_config.get("page_id") or "").strip()
    facebook_page_name = str(facebook_store.get("page_name") or "").strip()

    return {
        "ok": True,
        "generated_at": time.time(),
        "x": {
            "configured": x_configured,
            "publish_mode": x_publish_mode,
            "publish_mode_label": _opennews_x_publish_mode_label(),
            "auth_ready": bool(x_profile.get("auth_ready")),
            "handle": x_handle,
            "display_name": x_display_name,
            "profile_dir": str(x_profile.get("profile_dir") or ""),
            "browser_profile": x_profile,
            "browser_login": x_login,
            "detected_from": str(x_debug.get("source") or ("oauth_token" if x_user else "")),
            "last_detected_at": float(x_debug.get("updated_at") or 0),
            "status_label": "已绑定" if x_configured else "未绑定",
            "account_label": f"@{x_handle}" if x_handle else (x_display_name or ""),
        },
        "facebook": {
            "configured": facebook_configured,
            "page_name": facebook_page_name,
            "page_id": facebook_page_id,
            "user_name": str(facebook_user.get("name") or "").strip(),
            "user_id": str(facebook_user.get("id") or "").strip(),
            "page_access_token_configured": bool(
                facebook_store.get("page_access_token") or facebook_config.get("page_access_token")
            ),
            "status_label": "已绑定" if facebook_configured else "未绑定",
            "account_label": facebook_page_name or facebook_page_id,
        },
    }


def _build_admin_services_status() -> dict:
    live = _build_admin_live_status()
    queue = live.get("queue") or {}
    tts_queue = live.get("qwen_tts_queue") or _qwen_tts_queue_snapshot()
    gpu_snapshot = _gpu_resource_snapshot()
    batch_config = load_opennews_batch_config(OPENNEWS_BATCH_DIR)
    channels_config = _load_opennews_channels_config(include_secrets=False)
    enabled_channels = [
        channel
        for channel in (channels_config.get("channels") or [])
        if isinstance(channel, dict) and channel.get("enabled")
    ]
    due_channels = _opennews_channel_scheduler_due_channels(channels_config)
    x_config = x_env_config()
    x_publish_mode = _opennews_x_publish_mode()
    x_profile = _x_browser_profile_snapshot()
    x_login = x_browser_login_status()
    x_store = load_x_token_store(X_TOKEN_STORE_PATH) if X_TOKEN_STORE_PATH.exists() else {}
    x_configured = (
        x_profile.get("auth_ready", False)
        if x_publish_mode == "browser"
        else bool(
            x_config.get("client_id")
            and x_config.get("redirect_uri")
            and (x_config.get("refresh_token") or X_TOKEN_STORE_PATH.exists())
        )
    )
    x_user = x_store.get("user") if isinstance(x_store.get("user"), dict) else None
    x_error = ""

    facebook_config = facebook_env_config()
    facebook_store = load_facebook_token_store(FACEBOOK_TOKEN_STORE_PATH) if FACEBOOK_TOKEN_STORE_PATH.exists() else {}
    facebook_configured = bool(
        facebook_config.get("app_id")
        and facebook_config.get("app_secret")
        and facebook_config.get("redirect_uri")
        and (
            (facebook_config.get("page_id") and facebook_config.get("page_access_token"))
            or FACEBOOK_TOKEN_STORE_PATH.exists()
        )
    )
    facebook_page = {
        "id": str(facebook_store.get("page_id") or facebook_config.get("page_id") or "").strip(),
        "name": str(facebook_store.get("page_name") or "").strip(),
    }
    facebook_error = ""

    youtube_config = youtube_env_config()
    youtube_store = {}
    if YOUTUBE_TOKEN_STORE_PATH.exists():
        try:
            youtube_store = json.loads(YOUTUBE_TOKEN_STORE_PATH.read_text(encoding="utf-8"))
        except Exception:
            youtube_store = {}
    youtube_configured = bool(
        youtube_config.get("client_id")
        and youtube_config.get("client_secret")
        and (youtube_config.get("refresh_token") or YOUTUBE_TOKEN_STORE_PATH.exists())
    )
    youtube_channel = (
        youtube_store.get("channel") if isinstance(youtube_store.get("channel"), dict) else {}
    )
    youtube_error = ""

    warnings: list[dict] = []
    if not channels_config.get("scheduler_enabled"):
        warnings.append(
            _admin_service_warning(
                "warning",
                "OpenNews 频道调度器未开启",
                "当前不会自动按频道抓取并批量生产新闻视频。",
                service_key="opennews_channels",
            )
        )
    if batch_config.get("last_run_error"):
        warnings.append(
            _admin_service_warning(
                "error",
                "OpenNews 抓取器最近一次运行报错",
                str(batch_config.get("last_run_error") or ""),
                service_key="opennews_fetcher",
            )
        )
    for channel in enabled_channels:
        if channel.get("last_run_error"):
            warnings.append(
                _admin_service_warning(
                    "error",
                    f"频道「{channel.get('name') or channel.get('id') or '未命名频道'}」最近一次运行失败",
                    str(channel.get("last_run_error") or ""),
                    service_key="opennews_channels",
                )
            )
    if x_publish_mode == "browser" and not x_profile.get("auth_ready"):
        warnings.append(
            _admin_service_warning(
                "warning",
                "X 浏览器自动发布尚未就绪",
                "浏览器 profile 为空，当前无法自动发 X 视频。",
                service_key="x_publish",
            )
        )
    if not facebook_configured:
        warnings.append(
            _admin_service_warning(
                "warning",
                "Facebook 发布未配置完成",
                "缺少 App/Page 授权信息，自动发布到 Facebook 会跳过。",
                service_key="facebook_publish",
            )
        )
    elif facebook_error:
        warnings.append(
            _admin_service_warning(
                "error",
                "Facebook 发布通道异常",
                facebook_error,
                service_key="facebook_publish",
            )
        )
    if not youtube_configured:
        warnings.append(
            _admin_service_warning(
                "info",
                "YouTube 目前未配置",
                "当前更适合只观察 X / Facebook 发布链路，YouTube 可后续单独恢复。",
                service_key="youtube_publish",
            )
        )
    elif youtube_error:
        warnings.append(
            _admin_service_warning(
                "error",
                "YouTube 授权存在异常",
                youtube_error,
                service_key="youtube_publish",
            )
        )

    services = [
        _admin_service_card(
            "web_app",
            "主服务",
            level="success",
            state_label="运行中",
            summary="FastAPI 主应用正在提供页面与接口。",
            detail="这个页面本身就说明主服务仍可响应。",
            meta=[
                {"label": "活跃任务", "value": str(live.get("summary", {}).get("running_task_count", 0))},
                {"label": "最近事件", "value": str(len(live.get("recent_events") or []))},
            ],
        ),
        _admin_service_card(
            "opennews_channels",
            "OpenNews 频道调度器",
            level="success" if channels_config.get("scheduler_enabled") else "warning",
            state_label="已开启" if channels_config.get("scheduler_enabled") else "未开启",
            summary=f"已启用频道 {len(enabled_channels)} 个，当前待执行 {len(due_channels)} 个。",
            detail="频道化调度负责科技、军事、政治、金融等不同栏目自动抓取与批量制作。",
            meta=[
                {"label": "调度器", "value": "开启" if channels_config.get("scheduler_enabled") else "关闭"},
                {"label": "启用频道", "value": str(len(enabled_channels))},
                {"label": "到点待跑", "value": str(len(due_channels))},
            ],
        ),
        _admin_service_card(
            "opennews_fetcher",
            "OpenNews 抓取引擎",
            level="error" if batch_config.get("last_run_error") else "success",
            state_label="异常" if batch_config.get("last_run_error") else "正常",
            summary=str(batch_config.get("last_run_message") or "最近还没有抓取记录。"),
            detail="这里看热点抓取器最近一次运行反馈，便于判断为什么没出新闻任务。",
            meta=[
                {"label": "间隔", "value": f"{int(batch_config.get('interval_minutes') or 0)} 分钟"},
                {"label": "时间范围", "value": str(batch_config.get("time_range") or "6h")},
                {"label": "抓取上限", "value": str(batch_config.get("limit") or 0)},
            ],
        ),
        _admin_service_card(
            "gpu_5090",
            "5090 GPU 资源",
            level="warning" if gpu_snapshot.get("waiting_count") else ("success" if gpu_snapshot.get("active") else "info"),
            state_label="占用中" if gpu_snapshot.get("active") else "空闲",
            summary=gpu_snapshot.get("active_label") or "当前没有任务占用 5090 GPU。",
            detail="数字人和 5090 本地链路会共用这层资源编排。",
            meta=[
                {"label": "当前类型", "value": str(gpu_snapshot.get("active_kind") or "无")},
                {"label": "等待队列", "value": str(gpu_snapshot.get("waiting_count") or 0)},
            ],
        ),
        _admin_service_card(
            "digital_human_queue",
            "数字人队列",
            level="warning" if queue.get("waiting_count") else ("success" if queue.get("running_count") else "info"),
            state_label="生成中" if queue.get("running_count") else "空闲",
            summary=queue.get("current_owner_display_name") or "当前没有数字人任务在运行。",
            detail="这里判断 InfiniteTalk / 火山链路是不是在正常接任务。",
            meta=[
                {"label": "运行中", "value": str(queue.get("running_count") or 0)},
                {"label": "排队中", "value": str(queue.get("waiting_count") or 0)},
                {"label": "并发上限", "value": str(queue.get("max_concurrent") or 0)},
            ],
        ),
        _admin_service_card(
            "qwen_tts",
            "5090 配音队列",
            level="warning" if tts_queue.get("waiting_count") else ("success" if tts_queue.get("running_count") else "info"),
            state_label="生成中" if tts_queue.get("running_count") else "空闲",
            summary=tts_queue.get("current_owner_display_name") or "当前没有 5090 配音任务在运行。",
            detail="主要观察本地 Qwen3-TTS 是否拥堵，避免新闻配音拖住后续流程。",
            meta=[
                {"label": "运行中", "value": str(tts_queue.get("running_count") or 0)},
                {"label": "排队中", "value": str(tts_queue.get("waiting_count") or 0)},
                {"label": "并发上限", "value": str(tts_queue.get("max_concurrent") or 0)},
            ],
        ),
        _admin_service_card(
            "x_publish",
            "X 自动发布",
            level="success" if x_configured else "warning",
            state_label="就绪" if x_configured else "待登录",
            summary=(
                f"当前模式：{_opennews_x_publish_mode_label()}，浏览器登录面板 {'已启动' if x_login.get('running') else '未启动'}。"
                if x_publish_mode == "browser"
                else "当前使用 API 模式发布 X。"
            ),
            detail=x_error or ("浏览器模式下依赖 profile 持久登录态，单独登录面板只用于人工补登录。"),
            meta=[
                {"label": "发布模式", "value": _opennews_x_publish_mode_label()},
                {"label": "Profile 文件", "value": str(x_profile.get("file_count") or 0)},
                {"label": "语言扩展", "value": "开启" if _opennews_x_publish_language_versions_enabled() else "关闭"},
            ],
        ),
        _admin_service_card(
            "facebook_publish",
            "Facebook 自动发布",
            level="success" if facebook_configured else "warning",
            state_label="就绪" if facebook_configured else "待配置",
            summary=(facebook_page.get("name") if isinstance(facebook_page, dict) and facebook_page.get("name") else "当前还没有可用的 Facebook Page 授权。"),
            detail="竖屏新闻视频会沿这条链路自动发布到对应主页。",
            meta=[
                {"label": "Page ID", "value": str((facebook_page or {}).get("id") or facebook_config.get("page_id") or "未设置")},
                {"label": "自动发布", "value": "开启" if _opennews_facebook_auto_publish_default() and not _opennews_facebook_auto_publish_disabled() else "关闭"},
                {"label": "语言扩展", "value": "开启" if _opennews_facebook_publish_language_versions_enabled() else "关闭"},
            ],
        ),
        _admin_service_card(
            "youtube_publish",
            "YouTube 发布",
            level="success" if youtube_configured else "info",
            state_label="就绪" if youtube_configured else "未配置",
            summary=(youtube_channel or {}).get("title") or ("已配置授权，但当前未缓存频道名。" if youtube_configured else "当前未启用 YouTube 自动发布。"),
            detail="这条链路目前可以先保留观察，不强制参与新闻自动分发。",
            meta=[
                {"label": "Refresh Token", "value": "已配置" if youtube_config.get("refresh_token") or YOUTUBE_TOKEN_STORE_PATH.exists() else "未配置"},
                {"label": "频道版本发布", "value": "开启" if _opennews_youtube_publish_language_versions_enabled() else "关闭"},
            ],
        ),
    ]

    channels = []
    for channel in channels_config.get("channels") or []:
        if not isinstance(channel, dict):
            continue
        accounts = channel.get("accounts") if isinstance(channel.get("accounts"), dict) else {}
        languages = list(channel.get("languages") or [])
        x_enabled_languages = 0
        facebook_enabled_languages = 0
        youtube_enabled_languages = 0
        for language_id in languages:
            account = accounts.get(language_id) if isinstance(accounts.get(language_id), dict) else {}
            x_account = account.get("x") if isinstance(account.get("x"), dict) else {}
            facebook_account = account.get("facebook") if isinstance(account.get("facebook"), dict) else {}
            youtube_account = account.get("youtube") if isinstance(account.get("youtube"), dict) else {}
            if _parse_bool_form(x_account.get("enabled", True)):
                x_enabled_languages += 1
            if _parse_bool_form(facebook_account.get("enabled", True)):
                facebook_enabled_languages += 1
            if _parse_bool_form(youtube_account.get("enabled", False)):
                youtube_enabled_languages += 1
        channels.append(
            {
                "id": str(channel.get("id") or ""),
                "name": str(channel.get("name") or channel.get("id") or "未命名频道"),
                "enabled": bool(channel.get("enabled")),
                "category": str(channel.get("category") or "all"),
                "keyword": str(channel.get("keyword") or ""),
                "time_range": str(channel.get("time_range") or "6h"),
                "interval_minutes": int(channel.get("interval_minutes") or 0),
                "limit": int(channel.get("limit") or 0),
                "produce_limit": int(channel.get("produce_limit") or 0),
                "languages": languages,
                "platforms": dict(channel.get("platforms") or {}),
                "last_run_at": float(channel.get("last_run_at") or 0),
                "next_run_at": float(channel.get("next_run_at") or 0),
                "last_run_message": str(channel.get("last_run_message") or ""),
                "last_run_error": str(channel.get("last_run_error") or ""),
                "account_summary": {
                    "x_enabled_languages": x_enabled_languages,
                    "facebook_enabled_languages": facebook_enabled_languages,
                    "youtube_enabled_languages": youtube_enabled_languages,
                },
            }
        )

    return {
        "ok": True,
        "generated_at": time.time(),
        "summary": {
            "running_task_count": live.get("summary", {}).get("running_task_count", 0),
            "digital_human_waiting_count": queue.get("waiting_count", 0),
            "qwen_tts_waiting_count": tts_queue.get("waiting_count", 0),
            "gpu_waiting_count": gpu_snapshot.get("waiting_count", 0),
            "enabled_channel_count": len(enabled_channels),
            "due_channel_count": len(due_channels),
            "warning_count": len(warnings),
        },
        "services": services,
        "warnings": warnings,
        "channels": channels,
        "active_tasks": live.get("active_tasks") or [],
        "recent_events": live.get("recent_events") or [],
        "queues": {
            "digital_human": queue,
            "qwen_tts": tts_queue,
            "gpu_5090": gpu_snapshot,
        },
        "publish_channels": {
            "x": {
                "configured": x_configured,
                "publish_mode": x_publish_mode,
                "publish_mode_label": _opennews_x_publish_mode_label(),
                "browser_profile": x_profile,
                "browser_login": x_login,
                "user": x_user,
                "error": x_error,
            },
            "facebook": {
                "configured": facebook_configured,
                "page": facebook_page,
                "error": facebook_error,
            },
            "youtube": {
                "configured": youtube_configured,
                "channel": youtube_channel,
                "error": youtube_error,
            },
        },
        "routes": {
            "workbench": "/",
            "admin_dashboard": "/admin/dashboard",
            "admin_services": "/admin/services",
            "service_api": "/api/admin/services-status",
        },
        "api_reference": [
            {"name": "后台服务聚合状态", "path": "/api/admin/services-status"},
            {"name": "管理员实时状态", "path": "/api/admin/live-status"},
            {"name": "数字人 / 配音 / GPU 队列", "path": "/api/omnihuman-queue"},
            {"name": "YouTube 状态", "path": "/api/youtube/status"},
            {"name": "Facebook 状态", "path": "/api/facebook/status"},
            {"name": "X 状态", "path": "/api/x/status"},
            {"name": "X 浏览器登录面板状态", "path": "/api/x/browser-login/status"},
        ],
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
    tts_queue = _qwen_tts_queue_snapshot()
    task_id = str(current_task.get("id", ""))
    tts_waiting_hit = next((item for item in tts_queue.get("waiting", []) if item.get("task_id") == task_id), None)
    tts_running_hit = next((item for item in tts_queue.get("running", []) if item.get("task_id") == task_id), None)
    return {
        "task_id": task_id,
        "topic": current_task.get("topic", ""),
        "mode": current_task.get("mode", "full"),
        "digital_human_engine": engine_id,
        "digital_human_engine_name": _digital_human_engine_label(engine_id),
        "step": getattr(tracker, "step", 0),
        "total_steps": getattr(tracker, "total_steps", 0),
        "status": getattr(tracker, "status", "running"),
        "latest_message": latest_message,
        "output_dir": current_task.get("output_dir") or "",
        "qwen_tts_queue": {
            "waiting": bool(tts_waiting_hit),
            "running": bool(tts_running_hit),
            "waiting_count": tts_queue.get("waiting_count", 0),
            "running_count": tts_queue.get("running_count", 0),
        },
    }


def _build_active_tasks_payload(user: Optional[dict]) -> list[dict]:
    if not user:
        return []
    username = user.get("username", "")
    queue = _omnihuman_queue_snapshot()
    tts_queue = _qwen_tts_queue_snapshot()
    waiting_task_ids = {str(item.get("task_id", "")) for item in (queue.get("waiting") or []) if item.get("task_id")}
    running_task_ids = {str(item.get("task_id", "")) for item in (queue.get("running") or []) if item.get("task_id")}
    tts_waiting_task_ids = {str(item.get("task_id", "")) for item in (tts_queue.get("waiting") or []) if item.get("task_id")}
    tts_running_task_ids = {str(item.get("task_id", "")) for item in (tts_queue.get("running") or []) if item.get("task_id")}
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
        elif task_id in tts_waiting_task_ids:
            status_group = "queued"
            stage_key = "qwen_tts_waiting"
        elif task_id in tts_running_task_ids:
            status_group = "running"
            stage_key = "qwen_tts_running"
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
            "auto_digital_batch_id": str(workflow_config.get("auto_digital_batch_id") or ""),
            "auto_digital_batch_index": int(workflow_config.get("auto_digital_batch_index") or 0),
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
            "qwen_tts_queue": {
                "waiting": task_id in tts_waiting_task_ids,
                "running": task_id in tts_running_task_ids,
                "waiting_count": tts_queue.get("waiting_count", 0),
                "running_count": tts_queue.get("running_count", 0),
            },
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


@app.get("/opennews/data-center", response_class=HTMLResponse)
async def opennews_data_center_page(request: Request):
    return templates.TemplateResponse(request, "opennews_data_center.html")


@app.get("/lab/opennews", response_class=HTMLResponse)
async def lab_opennews_page(request: Request):
    return templates.TemplateResponse(request, "lab_opennews.html")


@app.get("/lab/apps/opennews", response_class=HTMLResponse)
async def lab_opennews_private_app(request: Request):
    user, error = _require_jclaw_lab_user(request)
    if error:
        return error
    return templates.TemplateResponse(request, "lab_opennews.html", {"lab_user": user})


@app.get("/lab/opennews/manifest.json")
async def lab_opennews_manifest(request: Request):
    base_url = _get_public_base_url(request).rstrip("/")
    return {
        "key": "ihouse-opennews",
        "appKey": "ihouse-opennews",
        "name": "OpenNews 新闻视频",
        "description": "抓取热点新闻，勾选后自动生成 OpenNews 三语竖屏新闻视频并发布 X/Facebook。",
        "entry_url": f"{base_url}/lab/apps/opennews",
        "preview_url": f"{base_url}/lab/opennews",
        "icon_url": f"{base_url}/public/assets/ihouse-logo.webp",
        "type": "web",
        "network": "public",
        "private": True,
        "scopes": ["auth.read", "auth.token"],
        "backend": {
            "base_url": base_url,
            "auth": "Authorization: Bearer <JClaw Lab JWT>",
            "health_url": f"{base_url}/api/lab/opennews/me",
        },
        "notes": [
            "entry_url 是正式小程序入口，宿主打开时需要附带 JClaw Lab JWT。",
            "preview_url 只用于普通浏览器预览页面外观，不代表已登录小程序环境。",
        ],
    }


@app.get("/sso/login", response_class=HTMLResponse)
async def jclaw_sso_login(request: Request):
    handoff = request.query_params.get("handoff") or request.query_params.get("token")
    if not handoff:
        return _jclaw_sso_error_response("缺少 handoff token")
    return _complete_jclaw_handoff_login(request, handoff)


@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard_page(request: Request):
    return templates.TemplateResponse(request, "admin.html")


@app.get("/admin/services", response_class=HTMLResponse)
async def admin_services_page(request: Request):
    user = _get_current_user(request)
    if not user or not _is_admin(user):
        return RedirectResponse(url=f"/?logged_out={int(time.time())}", status_code=302)
    return templates.TemplateResponse(request, "admin_services.html")


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
    return JSONResponse({"error": "管理员费用统计已停用"}, status_code=410)


@app.get("/api/admin/live-status")
async def admin_live_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_admin_live_status()


@app.get("/api/admin/services-status")
async def admin_services_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_admin_services_status()


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


@app.get("/api/youtube/oauth-app")
async def youtube_oauth_app_get(request: Request):
    """读取当前 YouTube 授权应用凭据配置（不返回密钥明文，只返回是否已配置）。"""
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = youtube_env_config()
    return {
        "ok": True,
        "client_id": config.get("client_id") or "",
        "client_secret_configured": bool(config.get("client_secret")),
        "redirect_uri": config.get("redirect_uri") or "",
        "source": youtube_oauth_app_source(),
    }


@app.post("/api/youtube/oauth-app")
async def youtube_oauth_app_save(request: Request):
    """在面板上填写并保存 YouTube 授权应用的客户端 ID / 密钥。保存后立即生效，无需重启。"""
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    client_id = str(payload.get("client_id") or "").strip()
    client_secret = str(payload.get("client_secret") or "").strip()
    redirect_uri = str(payload.get("redirect_uri") or "").strip()
    if not client_id:
        return JSONResponse({"error": "请填写客户端 ID"}, status_code=400)
    # 密钥留空表示“沿用已保存的密钥”（避免前端不显示明文时误清空）
    if not client_secret:
        existing = youtube_env_config()
        client_secret = existing.get("client_secret") or ""
        if not client_secret:
            return JSONResponse({"error": "请填写客户端密钥"}, status_code=400)
    try:
        save_youtube_oauth_app_config(client_id, client_secret, redirect_uri)
    except Exception as exc:
        return JSONResponse({"error": f"保存失败：{exc}"}, status_code=500)
    saved = youtube_env_config()
    return {
        "ok": True,
        "client_id": saved.get("client_id") or "",
        "client_secret_configured": bool(saved.get("client_secret")),
        "redirect_uri": saved.get("redirect_uri") or "",
        "source": youtube_oauth_app_source(),
    }


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
    # 按频道授权：带上 channel/language 则把凭据绑定到该频道账号槽；否则维持全局绑定行为。
    oauth_channel = str(request.query_params.get("channel") or "").strip()
    oauth_language = str(request.query_params.get("language") or "").strip()
    if oauth_channel and oauth_language:
        request.session["youtube_oauth_channel"] = oauth_channel
        request.session["youtube_oauth_language"] = oauth_language
    else:
        request.session.pop("youtube_oauth_channel", None)
        request.session.pop("youtube_oauth_language", None)
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={quote(config['client_id'], safe='')}"
        f"&redirect_uri={quote(config['redirect_uri'], safe='')}"
        "&response_type=code"
        f"&scope={quote(YOUTUBE_SCOPE, safe='')}"
        "&access_type=offline"
        # select_account：每次都让选 Google 账号（对接多个账号时能选对）；
        # 若所选账号名下有多个 YouTube 子频道(品牌账号)，Google 会接着让选具体频道。
        "&prompt=select_account%20consent"
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
        meta = {"token_response": {k: v for k, v in tokens.items() if k != "refresh_token"}}
        oauth_channel = str(request.session.pop("youtube_oauth_channel", "") or "").strip()
        oauth_language = str(request.session.pop("youtube_oauth_language", "") or "").strip()
        if oauth_channel == "topic_auto":
            # 话题自动化专属 YouTube 账号绑定
            token_path = YOUTUBE_AUTH_DIR / "youtube_token_topic_auto.json"
            save_youtube_refresh_token(token_path, refresh_token, meta)
            channel = get_youtube_channel(token_path)
            cfg = _load_topic_auto_config()
            cfg["youtube"] = {"enabled": True, "channel_name": channel.get("title") or "", "token_store_path": str(token_path)}
            cfg["youtube_auto_publish"] = True
            _save_topic_auto_config(cfg)
            return HTMLResponse(
                "<h2>YouTube 授权成功</h2>"
                f"<p>已绑定为<b>话题自动化</b>的发布账号。</p>"
                f"<p>YouTube 频道：{channel.get('title') or ''}</p>"
                "<p>以后每条话题视频做完会自动发到这个频道。可关闭本页回到面板刷新。</p>"
                "<script>try{window.opener&&window.opener.postMessage({type:'ihouse-oauth-done',platform:'youtube'},'*');}catch(e){}</script>"
            )
        if oauth_channel and oauth_language:
            # 按频道绑定：凭据存到该频道独立文件，并写入该频道账号槽。
            token_path = _opennews_channel_account_token_path(oauth_channel, oauth_language, "youtube")
            save_youtube_refresh_token(token_path, refresh_token, meta)
            channel = get_youtube_channel(token_path)
            bound = _opennews_bind_channel_account(oauth_channel, oauth_language, "youtube", {
                "enabled": True,
                "channel_name": channel.get("title") or "",
                "token_store_path": str(token_path),
            })
            return HTMLResponse(
                "<h2>YouTube 授权成功</h2>"
                f"<p>已绑定到频道 <b>{oauth_channel}</b> / 语言 <b>{oauth_language}</b>{'' if bound else '（未找到该频道，凭据已保存）'}</p>"
                f"<p>YouTube 频道：{channel.get('title') or ''}</p>"
                f"<p>channel_id：{channel.get('channel_id') or ''}</p>"
                "<p>可以关闭本页，回到 iHouse 面板刷新查看绑定状态。</p>"
                "<script>try{window.opener&&window.opener.postMessage({type:'ihouse-oauth-done',platform:'youtube'},'*');}catch(e){}</script>"
            )
        save_youtube_refresh_token(YOUTUBE_TOKEN_STORE_PATH, refresh_token, meta)
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


@app.get("/api/facebook/status")
async def facebook_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = facebook_env_config()
    configured = bool(
        config.get("app_id")
        and config.get("app_secret")
        and config.get("redirect_uri")
        and (
            (config.get("page_id") and config.get("page_access_token"))
            or FACEBOOK_TOKEN_STORE_PATH.exists()
        )
    )
    payload = {
        "configured": configured,
        "app_id_configured": bool(config.get("app_id")),
        "app_secret_configured": bool(config.get("app_secret")),
        "redirect_uri": config.get("redirect_uri") or "",
        "page_id_configured": bool(config.get("page_id")),
        "page_access_token_configured": bool(config.get("page_access_token") or FACEBOOK_TOKEN_STORE_PATH.exists()),
        "auto_publish_enabled": _opennews_facebook_auto_publish_default(),
        "auto_publish_disabled": _opennews_facebook_auto_publish_disabled(),
        "scope": FACEBOOK_SCOPE,
        "page": None,
        "error": "",
    }
    if configured:
        try:
            payload["page"] = get_facebook_page(FACEBOOK_TOKEN_STORE_PATH)
        except Exception as exc:
            payload["error"] = str(exc)
    return payload


@app.get("/api/facebook/oauth/start")
async def facebook_oauth_start(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = facebook_env_config()
    if not config.get("app_id") or not config.get("redirect_uri"):
        return JSONResponse({"error": "未配置 FACEBOOK_APP_ID / FACEBOOK_REDIRECT_URI"}, status_code=500)
    state = hashlib.sha256(f"facebook:{user.get('username')}:{time.time()}:{uuid.uuid4()}".encode("utf-8")).hexdigest()
    request.session["facebook_oauth_state"] = state
    request.session["facebook_oauth_scope"] = FACEBOOK_SCOPE
    oauth_channel = str(request.query_params.get("channel") or "").strip()
    oauth_language = str(request.query_params.get("language") or "").strip()
    if oauth_channel and oauth_language:
        request.session["facebook_oauth_channel"] = oauth_channel
        request.session["facebook_oauth_language"] = oauth_language
    else:
        request.session.pop("facebook_oauth_channel", None)
        request.session.pop("facebook_oauth_language", None)
    try:
        auth_url = build_facebook_authorization_url(state=state, scope=FACEBOOK_SCOPE)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return RedirectResponse(auth_url)


@app.get("/api/facebook/oauth/callback")
async def facebook_oauth_callback(
    request: Request,
    code: str = "",
    state: str = "",
    error: str = "",
    error_reason: str = "",
    error_description: str = "",
    error_code: str = "",
    error_message: str = "",
):
    # Facebook 授权失败时可能只回传 error_code/error_message(而非 error),这里一并捕获,避免误报“缺少 code”。
    fb_error = error or error_message or error_description or error_reason
    if fb_error:
        def _esc(v: str) -> str:
            return str(v).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        detail = _esc(error_message or error_description or error or error_reason)
        code_hint = f"（错误码 {_esc(error_code)}）" if error_code else ""
        tip = ""
        if "invalid scopes" in str(fb_error).lower() or "pages_manage_posts" in str(fb_error).lower():
            tip = (
                "<p style='color:#555'>原因:该 Facebook 应用尚未启用 "
                "<b>pages_show_list / pages_read_engagement / pages_manage_posts</b> 这三个 Page 权限。"
                "请到应用后台把这三个权限加上(见下),并确保用应用管理员账号授权。</p>"
            )
        return HTMLResponse(f"<h2>Facebook 授权失败</h2><p>{code_hint}{detail}</p>{tip}", status_code=400)
    expected_state = request.session.get("facebook_oauth_state")
    if expected_state and state and not hmac.compare_digest(str(expected_state), str(state)):
        return HTMLResponse("<h2>Facebook 授权失败</h2><p>state 校验失败。</p>", status_code=400)
    if not code:
        return HTMLResponse("<h2>Facebook 授权失败</h2><p>缺少 code(Facebook 未返回授权码,通常是权限或回调地址配置问题)。</p>", status_code=400)
    try:
        short_lived = exchange_facebook_code_for_tokens(code)
        long_lived = exchange_facebook_long_lived_user_token(str(short_lived.get("access_token") or ""))
        user_token = str(long_lived.get("access_token") or short_lived.get("access_token") or "")
        user_expires = float(long_lived.get("expires_at") or short_lived.get("expires_at") or 0.0)
        meta = {
            "scope": request.session.get("facebook_oauth_scope") or FACEBOOK_SCOPE,
            "short_lived_token_response": short_lived.get("raw") or {},
            "long_lived_token_response": long_lived.get("raw") or {},
        }
        oauth_channel = str(request.session.pop("facebook_oauth_channel", "") or "").strip()
        oauth_language = str(request.session.pop("facebook_oauth_language", "") or "").strip()
        request.session.pop("facebook_oauth_state", None)
        request.session.pop("facebook_oauth_scope", None)
        if oauth_channel and oauth_language:
            token_path = _opennews_channel_account_token_path(oauth_channel, oauth_language, "facebook")
            saved = save_facebook_authorization(
                token_path, user_access_token=user_token, user_token_expires_at=user_expires, meta=meta,
            )
            _opennews_bind_channel_account(oauth_channel, oauth_language, "facebook", {
                "enabled": True,
                "binding_mode": "custom",
                "page_name": saved.get("page_name") or "",
                "page_id": saved.get("page_id") or "",
                "page_access_token": saved.get("page_access_token") or "",
            })
            pages = saved.get("pages") if isinstance(saved.get("pages"), list) else get_facebook_pages(user_token)
            picker = ""
            valid_pages = [p for p in (pages or []) if isinstance(p, dict) and p.get("id")]
            if len(valid_pages) > 1:
                links = "".join(
                    '<li><a href="/api/facebook/oauth/select-page?channel={c}&language={l}&page_id={pid}">{name}</a>{cur}</li>'.format(
                        c=quote(oauth_channel, safe=""), l=quote(oauth_language, safe=""),
                        pid=quote(str(p.get("id") or ""), safe=""),
                        name=(p.get("name") or p.get("id")),
                        cur="（当前）" if str(p.get("id")) == str(saved.get("page_id")) else "",
                    )
                    for p in valid_pages
                )
                picker = f"<p>该账号可管理多个 Page，已默认绑定 <b>{saved.get('page_name') or ''}</b>。如需换成其它 Page 请点击：</p><ul>{links}</ul>"
            return HTMLResponse(
                "<h2>Facebook 授权成功</h2>"
                f"<p>已绑定到频道 <b>{oauth_channel}</b> / 语言 <b>{oauth_language}</b></p>"
                f"<p>Page：{saved.get('page_name') or ''}（{saved.get('page_id') or ''}）</p>"
                + picker +
                "<p>可以关闭本页，回到 iHouse 面板刷新查看绑定状态。</p>"
                "<script>try{window.opener&&window.opener.postMessage({type:'ihouse-oauth-done',platform:'facebook'},'*');}catch(e){}</script>"
            )
        saved = save_facebook_authorization(
            FACEBOOK_TOKEN_STORE_PATH, user_access_token=user_token, user_token_expires_at=user_expires, meta=meta,
        )
        page = get_facebook_page(FACEBOOK_TOKEN_STORE_PATH)
    except Exception as exc:
        return HTMLResponse(f"<h2>Facebook 授权失败</h2><p>{exc}</p>", status_code=500)
    return HTMLResponse(
        "<h2>Facebook 授权成功</h2>"
        f"<p>Page：{page.get('name') or saved.get('page_name') or ''}</p>"
        f"<p>page_id：{page.get('id') or saved.get('page_id') or ''}</p>"
        "<p>现在可以回到 iHouse 系统自动发布到 Facebook。</p>"
    )


@app.get("/api/facebook/oauth/select-page")
async def facebook_oauth_select_page(request: Request, channel: str = "", language: str = "", page_id: str = ""):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    channel = str(channel or "").strip()
    language = str(language or "").strip()
    page_id = str(page_id or "").strip()
    if not (channel and language and page_id):
        return HTMLResponse("<h2>参数不全</h2>", status_code=400)
    try:
        token_path = _opennews_channel_account_token_path(channel, language, "facebook")
        stored = json.loads(token_path.read_text(encoding="utf-8")) if token_path.exists() else {}
        user_token = str(stored.get("user_access_token") or "").strip()
        if not user_token:
            return HTMLResponse("<h2>找不到已保存的用户令牌，请重新授权。</h2>", status_code=400)
        saved = save_facebook_authorization(
            token_path, user_access_token=user_token, preferred_page_id=page_id, meta=stored.get("meta") or {},
        )
        _opennews_bind_channel_account(channel, language, "facebook", {
            "enabled": True,
            "binding_mode": "custom",
            "page_name": saved.get("page_name") or "",
            "page_id": saved.get("page_id") or "",
            "page_access_token": saved.get("page_access_token") or "",
        })
    except Exception as exc:
        return HTMLResponse(f"<h2>切换 Page 失败</h2><p>{exc}</p>", status_code=500)
    return HTMLResponse(
        "<h2>已切换 Facebook Page</h2>"
        f"<p>频道 <b>{channel}</b> / 语言 <b>{language}</b> 现绑定：{saved.get('page_name') or ''}（{saved.get('page_id') or ''}）</p>"
        "<p>可以关闭本页，回到 iHouse 面板刷新查看。</p>"
        "<script>try{window.opener&&window.opener.postMessage({type:'ihouse-oauth-done',platform:'facebook'},'*');}catch(e){}</script>"
    )


@app.post("/api/opennews/channel-account/unbind")
async def opennews_channel_account_unbind(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    channel = str(payload.get("channel") or "").strip()
    language = str(payload.get("language") or "").strip()
    platform = str(payload.get("platform") or "").strip()
    if platform not in {"x", "facebook", "youtube"} or not (channel and language):
        return JSONResponse({"error": "参数不全或平台不支持"}, status_code=400)
    reset = dict((_default_opennews_language_account(language).get(platform) or {}))
    reset["enabled"] = False
    ok = _opennews_bind_channel_account(channel, language, platform, reset)
    return {"ok": bool(ok)}


@app.post("/api/facebook/upload")
async def facebook_upload(request: Request):
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
    text = str(payload.get("text") or "").strip() or _build_default_facebook_post_text(result)
    title = str(payload.get("title") or "").strip() or str(result.get("title") or result.get("topic") or "OpenNews")
    job_id = f"facebook_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job = {
        "job_id": job_id,
        "status": "queued",
        "message": "Facebook 发布任务已创建",
        "created_at": time.time(),
        "updated_at": time.time(),
        "owner_username": user.get("username") or "",
        "history_id": history_id,
        "output_dir": str(output_dir),
        "video_path": str(video_path),
        "aspect_ratio": aspect_ratio,
        "text": text,
        "title": title[:255],
    }
    with FACEBOOK_UPLOAD_LOCK:
        FACEBOOK_UPLOAD_JOBS[job_id] = dict(job)
    thread = threading.Thread(target=_run_facebook_upload_job, args=(job_id,), daemon=True)
    thread.start()
    return {"ok": True, "job_id": job_id, "job": job}


@app.get("/api/facebook/jobs/{job_id}")
async def facebook_upload_job(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with FACEBOOK_UPLOAD_LOCK:
        job = dict(FACEBOOK_UPLOAD_JOBS.get(job_id) or {})
    if not job:
        return JSONResponse({"error": "Facebook 发布任务不存在"}, status_code=404)
    return {"job": job}


@app.get("/api/x/status")
async def x_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = x_env_config()
    publish_mode = _opennews_x_publish_mode()
    configured = x_browser_auth_ready() if publish_mode == "browser" else bool(config.get("client_id") and config.get("redirect_uri") and (config.get("refresh_token") or X_TOKEN_STORE_PATH.exists()))
    payload = {
        "configured": configured,
        "publish_mode": publish_mode,
        "publish_mode_label": _opennews_x_publish_mode_label(),
        "browser": {
            **x_browser_env_config(),
            "auth_ready": x_browser_auth_ready(),
        },
        "consumer_key_configured": bool(config.get("consumer_key")),
        "consumer_secret_configured": bool(config.get("consumer_secret")),
        "bearer_token_configured": bool(config.get("bearer_token")),
        "client_id_configured": bool(config.get("client_id")),
        "client_secret_configured": bool(config.get("client_secret")),
        "redirect_uri": config.get("redirect_uri") or "",
        "refresh_token_configured": bool(config.get("refresh_token") or X_TOKEN_STORE_PATH.exists()),
        "auto_publish_enabled": _opennews_x_auto_publish_default(),
        "auto_publish_disabled": _opennews_x_auto_publish_disabled(),
        "scope": X_SCOPE,
        "user": None,
        "error": "",
    }
    if configured and publish_mode != "browser":
        try:
            payload["user"] = get_x_user(X_TOKEN_STORE_PATH)
        except Exception as exc:
            payload["error"] = str(exc)
    return payload


@app.get("/api/x/browser-profile/export")
async def x_browser_profile_export(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    profile_dir = x_browser_profile_dir()
    if not profile_dir.exists():
        return JSONResponse({"error": "X 浏览器 profile 不存在"}, status_code=404)
    temp_dir = OUTPUT_DIR / "_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    archive_path = temp_dir / f"x_browser_profile_{int(time.time())}.zip"
    if archive_path.exists():
        archive_path.unlink()
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in profile_dir.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(profile_dir)
            zf.write(path, rel.as_posix())
    return FileResponse(str(archive_path), media_type="application/zip", filename=archive_path.name)


@app.post("/api/x/browser-profile/import")
async def x_browser_profile_import(request: Request, file: UploadFile = File(...)):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    filename = str(file.filename or "").lower()
    if not filename.endswith(".zip"):
        return JSONResponse({"error": "请上传 zip 文件"}, status_code=400)
    profile_dir = x_browser_profile_dir()
    temp_dir = OUTPUT_DIR / "_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_zip = temp_dir / f"x_browser_profile_import_{uuid.uuid4().hex}.zip"
    import_dir = temp_dir / f"x_browser_profile_import_{uuid.uuid4().hex}"
    data = await file.read()
    temp_zip.write_bytes(data)
    extracted = 0
    try:
        import_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(temp_zip, "r") as zf:
            for member in zf.infolist():
                name = member.filename or ""
                if not name or name.startswith("/") or ".." in PurePosixPath(name).parts:
                    continue
                zf.extract(member, import_dir)
        if profile_dir.exists():
            shutil.rmtree(profile_dir, ignore_errors=True)
        profile_dir.mkdir(parents=True, exist_ok=True)
        for path in import_dir.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(import_dir)
            target = profile_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            extracted += 1
    except zipfile.BadZipFile:
        return JSONResponse({"error": "无效的 zip 文件"}, status_code=400)
    finally:
        try:
            temp_zip.unlink()
        except Exception:
            pass
        try:
            shutil.rmtree(import_dir, ignore_errors=True)
        except Exception:
            pass
    return {
        "ok": True,
        "message": "X 浏览器 profile 已导入",
        "file_count": extracted,
        "auth_ready": x_browser_auth_ready(),
        "profile_dir": str(profile_dir),
    }


@app.post("/api/x/browser-profile/clear")
async def x_browser_profile_clear(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    profile_dir = x_browser_profile_dir()
    if profile_dir.exists():
        shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    return {"ok": True, "message": "X 浏览器 profile 已清空", "auth_ready": x_browser_auth_ready()}


@app.get("/api/x/browser-profile/status")
async def x_browser_profile_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    profile_dir = x_browser_profile_dir()
    file_count = 0
    total_bytes = 0
    for path in profile_dir.rglob("*"):
        if not path.is_file():
            continue
        file_count += 1
        try:
            total_bytes += path.stat().st_size
        except Exception:
            pass
    return {
        "ok": True,
        "auth_ready": x_browser_auth_ready(),
        "profile_dir": str(profile_dir),
        "file_count": file_count,
        "size_bytes": total_bytes,
        "publish_mode": _opennews_x_publish_mode(),
        "publish_mode_label": _opennews_x_publish_mode_label(),
        "browser": x_browser_env_config(),
    }


@app.get("/api/x/browser-login/status")
async def x_browser_login_status_api(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return {"ok": True, **x_browser_login_status(), "env": x_browser_login_env_config()}


@app.post("/api/x/browser-login/start")
async def x_browser_login_start_api(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    channel = str((payload or {}).get("channel") or "").strip()
    language = str((payload or {}).get("language") or "").strip()
    profile_dir = None
    if channel and language:
        # 按频道登录：为该频道×语言分配独立浏览器 profile，并写入账号槽（发布时用此 profile）。
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", f"{channel}_{language}").strip("_") or "channel"
        state_dir = Path(str(x_browser_env_config().get("state_dir") or (OUTPUT_DIR / "x_browser")))
        profile_dir = state_dir / "profiles" / safe
        _opennews_bind_channel_account(channel, language, "x", {
            "enabled": True,
            "binding_mode": "custom",
            "account_label": f"{channel}/{language}",
            "profile_dir": str(profile_dir),
        })
    try:
        status = start_x_browser_login(profile_dir)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return {
        "ok": True,
        "channel": channel,
        "language": language,
        "profile_dir": str(profile_dir) if profile_dir else "",
        **status,
        "env": x_browser_login_env_config(),
    }


@app.post("/api/x/browser-login/stop")
async def x_browser_login_stop_api(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        status = stop_x_browser_login()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return {"ok": True, **status, "env": x_browser_login_env_config()}


@app.get("/api/x/oauth/start")
async def x_oauth_start(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    config = x_env_config()
    if not config.get("client_id") or not config.get("redirect_uri"):
        return JSONResponse({"error": "未配置 X_CLIENT_ID / X_REDIRECT_URI"}, status_code=500)
    state = hashlib.sha256(f"x:{user.get('username')}:{time.time()}:{uuid.uuid4()}".encode("utf-8")).hexdigest()
    code_verifier, code_challenge = generate_x_pkce_pair()
    request.session["x_oauth_state"] = state
    request.session["x_oauth_code_verifier"] = code_verifier
    request.session["x_oauth_scope"] = X_SCOPE
    try:
        auth_url = build_x_authorization_url(state=state, code_challenge=code_challenge, scope=X_SCOPE)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return RedirectResponse(auth_url)


@app.get("/api/x/oauth/callback")
async def x_oauth_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<h2>X 授权失败</h2><p>{error}</p>", status_code=400)
    expected_state = request.session.get("x_oauth_state")
    if expected_state and state and not hmac.compare_digest(str(expected_state), str(state)):
        return HTMLResponse("<h2>X 授权失败</h2><p>state 校验失败。</p>", status_code=400)
    if not code:
        return HTMLResponse("<h2>X 授权失败</h2><p>缺少 code。</p>", status_code=400)
    code_verifier = str(request.session.get("x_oauth_code_verifier") or "")
    if not code_verifier:
        return HTMLResponse("<h2>X 授权失败</h2><p>缺少 PKCE 会话，请从 /api/x/oauth/start 重新开始。</p>", status_code=400)
    try:
        tokens = exchange_x_code_for_tokens(code, code_verifier)
        refresh_token = str(tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            return HTMLResponse("<h2>X 授权成功但没有返回 refresh_token</h2><p>请确认 OAuth scope 包含 offline.access，并重新授权。</p>", status_code=400)
        save_x_tokens(
            X_TOKEN_STORE_PATH,
            tokens,
            {
                "scope": request.session.get("x_oauth_scope") or X_SCOPE,
                "token_response": {k: v for k, v in tokens.items() if k not in {"access_token", "refresh_token"}},
            },
        )
        request.session.pop("x_oauth_state", None)
        request.session.pop("x_oauth_code_verifier", None)
        request.session.pop("x_oauth_scope", None)
        x_user = get_x_user(X_TOKEN_STORE_PATH)
    except Exception as exc:
        return HTMLResponse(f"<h2>X 授权失败</h2><p>{exc}</p>", status_code=500)
    return HTMLResponse(
        "<h2>X 授权成功</h2>"
        f"<p>账号：@{x_user.get('username') or ''}</p>"
        f"<p>name：{x_user.get('name') or ''}</p>"
        "<p>现在可以回到 iHouse 系统自动发布到 X。</p>"
    )


@app.post("/api/x/upload")
async def x_upload(request: Request):
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
    text = str(payload.get("text") or "").strip() or _build_default_x_post_text(result)
    job_id = f"x_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    job = {
        "job_id": job_id,
        "status": "queued",
        "message": "X 发布任务已创建",
        "created_at": time.time(),
        "updated_at": time.time(),
        "owner_username": user.get("username") or "",
        "history_id": history_id,
        "output_dir": str(output_dir),
        "video_path": str(video_path),
        "aspect_ratio": aspect_ratio,
        "text": text[:280],
        "made_with_ai": bool(payload.get("made_with_ai", True)),
    }
    with X_UPLOAD_LOCK:
        X_UPLOAD_JOBS[job_id] = dict(job)
    thread = threading.Thread(target=_run_x_upload_job, args=(job_id,), daemon=True)
    thread.start()
    return {"ok": True, "job_id": job_id, "job": job}


@app.get("/api/x/jobs/{job_id}")
async def x_upload_job(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    with X_UPLOAD_LOCK:
        job = dict(X_UPLOAD_JOBS.get(job_id) or {})
    if not job:
        return JSONResponse({"error": "X 发布任务不存在"}, status_code=404)
    return {"job": job}


@app.post("/api/x/browser-publish-test")
async def x_browser_publish_test(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    if _opennews_x_publish_mode() != "browser":
        return JSONResponse({"error": "当前 X 发布模式不是 browser"}, status_code=400)
    if not x_browser_auth_ready():
        return JSONResponse({"error": "当前服务器 X 浏览器 profile 未登录，请先导入已登录 profile"}, status_code=400)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    history_id = str(payload.get("history_id") or "").strip()
    if not history_id:
        return JSONResponse({"error": "缺少 history_id"}, status_code=400)
    aspect_ratio = str(payload.get("aspect_ratio") or "vertical").strip().lower()
    output_dir, result, resolve_error = _resolve_history_for_user(history_id, user)
    if resolve_error:
        return resolve_error
    assert output_dir is not None and result is not None
    try:
        video_path = _resolve_youtube_publish_video(output_dir, result, aspect_ratio=aspect_ratio)
    except Exception as exc:
        return JSONResponse({"error": f"找不到可发布视频：{exc}"}, status_code=400)
    text = str(payload.get("text") or "").strip() or _build_default_x_post_text(result)
    try:
        upload_result = publish_video_to_x_browser(video_path, text=text, made_with_ai=True)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    publish_record = {
        "job_id": f"x_browser_test_{aspect_ratio}_{int(time.time())}",
        "history_id": output_dir.name,
        "aspect_ratio": aspect_ratio,
        "language_version": "primary",
        "target_market": str((result.get("workflow_config") or {}).get("target_market") or "cn"),
        "video_path": str(video_path),
        "created_at": time.time(),
        **upload_result,
    }
    records = result.get("x_publish_records")
    if not isinstance(records, list):
        records = []
    records.insert(0, publish_record)
    result["x_publish_records"] = records[:20]
    result["x_publish_latest"] = publish_record
    _save_result_to_output_dir(output_dir, result)
    return {"ok": True, "record": publish_record, "history_id": output_dir.name}


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
    return {
        "config": load_opennews_batch_config(OPENNEWS_BATCH_DIR),
        "channels_config": _load_opennews_channels_config(include_secrets=False),
        "accounts_summary": _build_opennews_bound_accounts_summary() if _is_admin(user) else {},
        "presenter_state": _opennews_presenter_state_snapshot(),
        "is_admin": _is_admin(user),
    }


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
    previous_config = load_opennews_batch_config(OPENNEWS_BATCH_DIR)
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
    should_kick_now = bool(config.get("enabled")) and (
        not previous_config.get("enabled")
        or any(
            config.get(key) != previous_config.get(key)
            for key in ("interval_minutes", "category", "time_range", "limit")
        )
    )
    if should_kick_now:
        threading.Thread(
            target=run_opennews_batch_fetch_once,
            kwargs={
                "root": OPENNEWS_BATCH_DIR,
                "triggered_by": f"config_update:{user.get('username') or 'admin'}",
            },
            daemon=True,
        ).start()
    return {"config": config, "presenter_state": _opennews_presenter_state_snapshot()}


@app.get("/api/opennews/channels/config")
async def opennews_channels_config(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {
        "config": _load_opennews_channels_config(include_secrets=False),
        "accounts_summary": _build_opennews_bound_accounts_summary() if _is_admin(user) else {},
        "is_admin": _is_admin(user),
    }


@app.get("/api/opennews/accounts/summary")
async def opennews_accounts_summary(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return _build_opennews_bound_accounts_summary()


@app.post("/api/opennews/channels/config")
async def opennews_channels_config_update(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    previous_config = _load_opennews_channels_config(include_secrets=True)
    config = _save_opennews_channels_config(payload)
    should_kick_channels = []
    previous_by_id = {
        str(item.get("id") or ""): item
        for item in previous_config.get("channels") or []
        if isinstance(item, dict)
    }
    if config.get("scheduler_enabled"):
        for channel in config.get("channels") or []:
            if not isinstance(channel, dict) or not channel.get("enabled"):
                continue
            previous = previous_by_id.get(str(channel.get("id") or "")) or {}
            if (
                not previous_config.get("scheduler_enabled")
                or not previous.get("enabled")
                or any(channel.get(key) != previous.get(key) for key in ("category", "keyword", "time_range", "limit", "interval_minutes"))
            ):
                should_kick_channels.append(dict(channel))
    for channel in should_kick_channels:
        threading.Thread(
            target=_run_opennews_channel_fetch,
            kwargs={
                "channel": channel,
                "triggered_by": f"channel_config_update:{user.get('username') or 'admin'}",
            },
            daemon=True,
        ).start()
    if config.get("scheduler_enabled"):
        save_opennews_batch_config(
            OPENNEWS_BATCH_DIR,
            {
                "enabled": False,
                "interval_minutes": 120,
                "category": "all",
                "time_range": "6h",
                "limit": 20,
            },
        )
    return {
        "config": _public_opennews_channels_config(config),
        "accounts_summary": _build_opennews_bound_accounts_summary(),
        "presenter_state": _opennews_presenter_state_snapshot(),
    }


@app.post("/api/opennews/batches/run-now")
async def opennews_batches_run_now(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    channel_id = _safe_opennews_channel_id(payload.get("channel_id") or payload.get("opennews_channel_id") or "")
    channel = _find_opennews_channel(channel_id, include_secrets=True) if channel_id else {}
    override = {
        "channel_id": channel.get("id") or None,
        "channel_name": channel.get("name") or None,
        "category": payload.get("category") or channel.get("category") or None,
        "keyword": payload.get("keyword") if payload.get("keyword") is not None else channel.get("keyword"),
        "time_range": payload.get("time_range") or channel.get("time_range") or None,
        "limit": payload.get("limit") or channel.get("limit") or None,
    }
    if channel:
        run_channel = dict(channel)
        for key in ("category", "keyword", "time_range", "limit"):
            if override.get(key) is not None:
                run_channel[key] = override.get(key)
        result = _run_opennews_channel_fetch(run_channel, triggered_by=f"manual_channel:{user.get('username') or 'manual'}")
    else:
        result = run_opennews_batch_fetch_once(
            OPENNEWS_BATCH_DIR,
            triggered_by=user.get("username") or "manual",
            override=override,
        )
    status_code = 202 if result.get("running") else 200
    return JSONResponse(result, status_code=status_code)


@app.post("/api/opennews/batches/prepare-review")
async def opennews_batches_prepare_review(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse(
        {"error": "OpenNews 人工素材审核批次已停用；请使用 2 小时自动化或“直接制作并发布”。"},
        status_code=410,
    )


@app.post("/api/opennews/batches/run-manual-production")
async def opennews_batches_run_manual_production(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # One-click full OpenNews run: fetch one batch, produce the configured
    # clips, publish the top story as Shorts, then build/publish one horizontal
    # collection from the same batch.
    save_opennews_batch_config(
        OPENNEWS_BATCH_DIR,
        {
            "enabled": False,
            "interval_minutes": 180,
            "category": payload.get("category") or "all",
            "time_range": payload.get("time_range") or "6h",
            "limit": 20,
        },
    )
    result = run_opennews_batch_fetch_once(
        OPENNEWS_BATCH_DIR,
        triggered_by=f"manual_production:{user.get('username') or 'admin'}",
        override={
            "category": payload.get("category") or "all",
            "time_range": payload.get("time_range") or "6h",
            "limit": 20,
        },
    )
    if result.get("running"):
        return JSONResponse(result, status_code=202)
    if not result.get("ok"):
        return JSONResponse(result, status_code=500)

    items = [item for item in (result.get("items") or []) if isinstance(item, dict)]
    mix_counts = _opennews_auto_collection_mix_counts()
    target_collection_count = max(1, sum(int(value or 0) for value in mix_counts.values()))
    selected = _select_opennews_auto_collection_items(
        items,
        time_range=str(result.get("time_range") or payload.get("time_range") or "6h"),
    )
    if not selected:
        selected = sorted(items, key=_opennews_batch_item_score, reverse=True)[:target_collection_count]
    selected = selected[:target_collection_count]
    if not selected:
        return JSONResponse({
            **result,
            "ok": False,
            "error": "本轮没有可制作的新新闻，可能全部被重复过滤。",
        }, status_code=400)

    target_market = str(payload.get("target_market") or user.get("target_market") or "cn")
    presenter_config = _next_opennews_batch_presenter_config()
    voice_preset_id = str(payload.get("voice_preset_id") or presenter_config.get("voice_preset_id") or "")
    aspect_ratio = str(payload.get("aspect_ratio") or "horizontal")
    selected_ids = [
        str(item.get("batch_item_id") or item.get("id") or "").strip()
        for item in selected
        if str(item.get("batch_item_id") or item.get("id") or "").strip()
    ]
    top_item = _select_opennews_batch_top_item(selected)
    top_item_id = str((top_item or {}).get("batch_item_id") or (top_item or {}).get("id") or "").strip()
    job = create_opennews_batch_job(
        OPENNEWS_BATCH_DIR,
        username=user.get("username") or "admin",
        items=selected,
        options={
            "target_market": target_market,
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": voice_preset_id,
            "aspect_ratio": aspect_ratio,
            "notes": str(payload.get("notes") or "手动启动一轮完整 OpenNews 自动化：生成中/日/英竖屏新闻视频，并发布 X/Facebook。"),
            "youtube_auto_publish": False,
            "youtube_privacy_status": "public",
            "youtube_aspects": [],
            "x_auto_publish": _opennews_x_auto_publish_default(),
            "x_publish_single_shorts": _opennews_x_auto_publish_default(),
            "x_collection_auto_publish": False,
            "x_aspects": ["vertical"],
            "opennews_presenter": presenter_config,
            "auto_collection_direct": False,
            "auto_collection_item_ids": selected_ids,
            "auto_single_shorts_item_ids": [top_item_id] if top_item_id else [],
            "auto_collection_mix_counts": mix_counts,
            "manual_batch_production": True,
            "manual_batch_id": result.get("batch_id") or "",
            "material_strategy": "free_library_script_match",
        },
    )
    if selected_ids:
        mark_opennews_batch_items(
            OPENNEWS_BATCH_DIR,
            selected_ids,
            {
                "status": "manual_producing",
                "auto_produce_job_id": job.get("job_id") or "",
                "auto_produce_selected_at": time.time(),
                "auto_produce_reason": "manual_batch_x_facebook",
                "message": "已进入手动完整自动化：中/日/英竖屏视频将自动发布 X/Facebook。",
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
    return {
        **result,
        "ok": True,
        "selected_count": len(selected),
        "selected_item_ids": selected_ids,
        "job": job,
        "job_id": job.get("job_id"),
        "message": f"已抓取新批次并启动 {len(selected)} 条新闻完整自动化：中/日/英竖屏视频将自动发布 X/Facebook。",
    }


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
    aspect_ratio = "vertical"
    opennews_channel_id = _safe_opennews_channel_id(payload.get("opennews_channel_id") or payload.get("channel_id") or "")
    if not opennews_channel_id:
        for item in items:
            opennews_channel_id = _safe_opennews_channel_id(item.get("opennews_channel_id") or "")
            if opennews_channel_id:
                break
    opennews_channel = _find_opennews_channel(opennews_channel_id, include_secrets=True) if opennews_channel_id else {}
    opennews_language_markets = opennews_channel.get("languages") if isinstance(opennews_channel.get("languages"), list) else None
    youtube_account_state = _opennews_publish_account_for(opennews_channel.get("id") or opennews_channel_id, target_market, "youtube") if (opennews_channel or opennews_channel_id) else {"enabled": False}
    youtube_auto_publish = payload.get("youtube_auto_publish")
    youtube_auto_publish = (
        _opennews_youtube_auto_publish_default()
        and not _opennews_youtube_auto_publish_disabled()
        and bool(youtube_account_state.get("enabled"))
    ) if youtube_auto_publish is None else _parse_bool_form(youtube_auto_publish)
    if _opennews_youtube_auto_publish_disabled():
        youtube_auto_publish = False
    youtube_aspects: list[str] = ["vertical"] if youtube_auto_publish else []
    x_auto_publish = payload.get("x_auto_publish")
    x_auto_publish = _opennews_x_auto_publish_default() if x_auto_publish is None else _parse_bool_form(x_auto_publish)
    if _opennews_x_auto_publish_disabled():
        x_auto_publish = False
    x_aspects = payload.get("x_aspects") or ["vertical"]
    if isinstance(x_aspects, str):
        x_aspects = ["horizontal", "vertical"] if x_aspects == "both" else [part.strip() for part in x_aspects.split(",") if part.strip()]
    elif isinstance(x_aspects, list):
        x_aspects = [str(part).strip() for part in x_aspects if str(part).strip()]
    else:
        x_aspects = ["vertical"]
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
            "youtube_auto_publish": youtube_auto_publish,
            "youtube_privacy_status": str(payload.get("youtube_privacy_status") or "public"),
            "youtube_aspects": youtube_aspects,
            "x_auto_publish": x_auto_publish,
            "x_aspects": x_aspects or ["vertical"],
            "facebook_auto_publish": _opennews_facebook_auto_publish_default(),
            "facebook_aspects": ["vertical"],
            "opennews_channel_id": opennews_channel.get("id") or opennews_channel_id,
            "opennews_channel_name": opennews_channel.get("name") or "",
            "opennews_language_markets": opennews_language_markets or list(OPENNEWS_CHANNEL_LANGUAGE_IDS),
            "material_strategy": "free_library_script_match",
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


@app.post("/api/opennews/batches/jobs/{job_id}/continue")
async def opennews_batches_continue_after_review(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse(
        {"error": "OpenNews 人工素材审核批次已停用；请使用 2 小时自动化或“直接制作并发布”。"},
        status_code=410,
    )


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
    return {"job": _opennews_batch_job_payload_for_ui(job)}


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
    return {"jobs": [_opennews_batch_job_payload_for_ui(job) for job in jobs], "count": len(jobs)}


@app.get("/api/lab/opennews/me")
async def lab_opennews_me(request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    return {"user": user}


@app.post("/api/lab/opennews/batches/run-now")
async def lab_opennews_batches_run_now(request: Request):
    user, error = _require_lab_or_user(request)
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
        triggered_by=f"lab:{user.get('username') or 'user'}",
        override=override,
    )
    return JSONResponse(result, status_code=202 if result.get("running") else 200)


@app.get("/api/lab/opennews/batches")
async def lab_opennews_batches(request: Request, limit: int = 10, exclude_used: bool = True):
    user, error = _require_lab_or_user(request)
    if error:
        return error
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
                payload["status"] = item.get("status") or ""
                payload["auto_reason"] = item.get("auto_reason") or ""
                payload["message"] = item.get("message") or ""
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
    return {"batches": batches, "count": len(batches), "user": user}


@app.post("/api/lab/opennews/produce-selected")
async def lab_opennews_produce_selected(request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    item_ids = payload.get("item_ids") or payload.get("ids") or []
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
    found_ids = {
        str(item.get("batch_item_id") or item.get("id") or "").strip()
        for item in items
        if str(item.get("batch_item_id") or item.get("id") or "").strip()
    }
    missing_item_ids = [item_id for item_id in item_ids if item_id not in found_ids]
    if missing_item_ids:
        return JSONResponse({
            "error": "部分候选新闻 id 未找到，本次未启动生成。",
            "missing_item_ids": missing_item_ids,
            "accepted_item_ids": sorted(found_ids),
        }, status_code=400)
    target_market = str(payload.get("target_market") or user.get("target_market") or "cn")
    voice_preset_id = str(payload.get("voice_preset_id") or "")
    aspect_ratio = str(payload.get("aspect_ratio") or "vertical")
    youtube_aspects: list[str] = []
    youtube_auto_publish = False
    x_auto_publish = payload.get("x_auto_publish")
    x_auto_publish = _opennews_x_auto_publish_default() if x_auto_publish is None else _parse_bool_form(x_auto_publish)
    if _opennews_x_auto_publish_disabled():
        x_auto_publish = False
    x_aspects = payload.get("x_aspects") or ["vertical"]
    if isinstance(x_aspects, str):
        x_aspects = ["horizontal", "vertical"] if x_aspects == "both" else [part.strip() for part in x_aspects.split(",") if part.strip()]
    elif isinstance(x_aspects, list):
        x_aspects = [str(part).strip() for part in x_aspects if str(part).strip()]
    else:
        x_aspects = ["vertical"]
    job = create_opennews_batch_job(
        OPENNEWS_BATCH_DIR,
        username=user.get("username") or "",
        items=items,
        options={
            "target_market": target_market,
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": voice_preset_id,
            "aspect_ratio": aspect_ratio,
            "notes": str(payload.get("notes") or payload.get("feedback") or ""),
            "youtube_auto_publish": youtube_auto_publish,
            "youtube_privacy_status": str(payload.get("youtube_privacy_status") or "public"),
            "youtube_aspects": youtube_aspects,
            "x_auto_publish": x_auto_publish,
            "x_aspects": x_aspects or ["vertical"],
            "facebook_auto_publish": _opennews_facebook_auto_publish_default(),
            "facebook_aspects": ["vertical"],
            "lab_trigger": True,
            "lab_sub": user.get("lab_sub") or "",
            "material_strategy": "free_library_script_match",
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
    return {
        "ok": True,
        "job_id": job.get("job_id"),
        "job": job,
        "message": "已接收选中的新闻，开始一站式生成文案、配音、素材和三语竖屏成片，并按配置发布 X/Facebook。",
    }


@app.post("/api/lab/opennews/batches/prepare-review")
async def lab_opennews_batches_prepare_review(request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("当前账号暂不支持开启人工审核批次")
    return JSONResponse(
        {"error": "OpenNews 人工素材审核批次已停用；请使用 2 小时自动化或直接批量制作。"},
        status_code=410,
    )


@app.post("/api/lab/opennews/batches/jobs/{job_id}/continue")
async def lab_opennews_batches_continue_after_review(job_id: str, request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("当前账号暂不支持继续人工审核批次")
    return JSONResponse(
        {"error": "OpenNews 人工素材审核批次已停用；请使用 2 小时自动化或直接批量制作。"},
        status_code=410,
    )


@app.get("/api/lab/opennews/jobs")
async def lab_opennews_jobs(request: Request, limit: int = 10):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    jobs = list_opennews_batch_jobs(
        OPENNEWS_BATCH_DIR,
        limit=max(1, min(int(limit or 10), 30)),
        username=str(user.get("username") or ""),
        include_all=_is_admin(user),
    )
    return {
        "jobs": [_external_opennews_job_result_payload(job) | {"job": _opennews_batch_job_payload_for_ui(job)} for job in jobs],
        "count": len(jobs),
    }


@app.get("/api/lab/opennews/jobs/{job_id}")
async def lab_opennews_job_status(job_id: str, request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id)
    if not job:
        return JSONResponse({"error": "批量生产任务不存在"}, status_code=404)
    if job.get("username") != user.get("username") and not _is_admin(user):
        return _forbidden_error()
    payload = _external_opennews_job_result_payload(job)
    payload["job"] = _opennews_batch_job_payload_for_ui(job)
    return payload


@app.get("/api/lab/opennews/ready-videos")
async def lab_opennews_ready_videos(request: Request, limit: int = 50, refresh: int = 0):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    videos: list[dict] = []
    max_items = max(1, min(int(limit or 50), 200))
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        payload = _lab_opennews_video_payload(request, output_dir, result or {}, force_metrics_refresh=bool(refresh))
        if not payload:
            continue
        videos.append(payload)
        if len(videos) >= max_items:
            break
    return {"videos": videos, "count": len(videos)}


@app.get("/api/lab/opennews/videos/{history_id}/download/{file_path:path}")
async def lab_opennews_video_download(history_id: str, file_path: str, request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
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


def _lab_task_payload(task: dict) -> dict:
    tracker = task.get("tracker")
    messages = list(getattr(tracker, "messages", []) or [])
    status = getattr(tracker, "status", "") or ("done" if task.get("result") else "running")
    output_dir = task.get("output_dir") or ""
    result_payload = None
    if task.get("result"):
        result_payload = _serialize_result_for_ui(output_dir, task.get("result") or {}, task.get("topic", ""))
        history_id = Path(str(output_dir)).name if output_dir else ""
        if history_id:
            final_rel = _history_relpath_from_value(output_dir, str((task.get("result") or {}).get("final_video_path") or ""))
            if final_rel:
                result_payload["lab_final_video"] = {
                    "url": f"/api/lab/tasks/{task.get('id')}/download/{quote(final_rel, safe='/')}",
                    "name": Path(final_rel).name or "final_video.mp4",
                }
            raw_variants = (task.get("result") or {}).get("final_video_variants")
            if isinstance(raw_variants, dict):
                lab_variants = {}
                for variant_key, variant_data in raw_variants.items():
                    if not isinstance(variant_data, dict):
                        continue
                    variant_rel = _history_relpath_from_value(output_dir, str(variant_data.get("final_video_path") or ""))
                    if not variant_rel:
                        continue
                    lab_variants[str(variant_key)] = {
                        "url": f"/api/lab/tasks/{task.get('id')}/download/{quote(variant_rel, safe='/')}",
                        "name": Path(variant_rel).name or f"final_video_{variant_key}.mp4",
                        "aspect_ratio": str(variant_data.get("compose_aspect_ratio") or variant_key),
                    }
                if lab_variants:
                    result_payload["lab_final_video_variants"] = lab_variants
    return {
        "task_id": task.get("id") or "",
        "mode": task.get("mode") or "digital_human",
        "topic": task.get("topic") or "",
        "status": status,
        "step": getattr(tracker, "step", 0) if tracker else 0,
        "total_steps": getattr(tracker, "total_steps", 0) if tracker else 0,
        "created_at": task.get("created_at") or 0,
        "owner_username": task.get("owner_username") or "",
        "messages": messages[-80:],
        "latest_message": (messages[-1].get("message") if messages else ""),
        "has_result": bool(task.get("result")),
        "result": result_payload,
    }


def _default_lab_avatar_for_voice(target_market: str, voice_preset: dict) -> Optional[dict]:
    for avatar in _list_avatar_options(target_market_id=target_market):
        if _is_avatar_voice_compatible(avatar, voice_preset):
            return avatar
    avatars = _list_avatar_options(target_market_id=target_market)
    return avatars[0] if avatars else None


@app.get("/api/lab/workbench/options")
async def lab_workbench_options(request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    return {
        "voice_presets": VOICE_PRESETS,
        "avatars": _list_avatar_options(target_market_id=user.get("target_market") or "cn"),
        "departments": DEPARTMENTS,
        "target_markets": TARGET_MARKETS,
        "digital_human_engines": _digital_human_engine_options_for_user(user),
        "property_bgm_tracks": _property_bgm_track_payloads(),
        "current_user": user,
    }


@app.post("/api/lab/digital-human/jobs")
async def lab_digital_human_job(
    request: Request,
    topic: str = Form(""),
    target_market: str = Form(""),
    department_id: str = Form(""),
    voice_preset_id: str = Form(""),
    avatar_id: str = Form(""),
    speed: float = Form(1.1),
    use_web_search: str = Form("false"),
    digital_human_engine: str = Form(""),
):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    topic = str(topic or "").strip()
    if not topic:
        return JSONResponse({"error": "请先输入数字人视频选题"}, status_code=400)
    target_market = str(target_market or user.get("target_market") or "cn")
    department_id = str(department_id or user.get("department_id") or "real_estate")
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    visible_voice_ids = _get_visible_voice_preset_ids(target_market)
    if voice_preset.get("id") not in visible_voice_ids:
        voice_preset = _get_voice_preset(_get_target_market(target_market).get("default_voice_preset_id"), target_market)
    voice_preset["selected_speed"] = speed
    avatar_option = _get_avatar_option(avatar_id, target_market_id=target_market) if avatar_id else None
    if not avatar_option or not _is_avatar_voice_compatible(avatar_option, voice_preset):
        avatar_option = _default_lab_avatar_for_voice(target_market, voice_preset)
    if not avatar_option:
        return JSONResponse({"error": "当前市场没有可用主播图片"}, status_code=400)
    selected_engine = _normalize_digital_human_engine(digital_human_engine, user)
    task_id = str(uuid.uuid4())[:8]
    tracker = ProgressTracker(task_id)
    image_path = avatar_option.get("image_path", "")
    tasks[task_id] = {
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "id": task_id,
        "mode": "digital_human",
        "topic": topic,
        "image_path": image_path,
        "tracker": tracker,
        "output_dir": None,
        "result": None,
        "public_base_url": _get_public_base_url(request),
        "created_at": time.time(),
        "cancel_requested": False,
        "cancel_requested_at": None,
        "workflow_config": {
            "voice_preset_id": voice_preset.get("id"),
            "avatar_id": avatar_option.get("id"),
            "speed": speed,
            "web_search_enabled": _parse_bool_form(use_web_search),
            "target_market": target_market,
            "department_id": department_id,
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
            "script_model": _normalize_script_model(SCRIPT_MODEL_API_RELAY, user),
            "digital_human_engine": selected_engine,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("小程序数字人任务已创建，准备开始...")
    _push_live_event("task_created", "创建了小程序数字人任务", tasks[task_id])
    thread = threading.Thread(
        target=run_pipeline_with_progress,
        args=(task_id, topic, image_path, tasks[task_id]["public_base_url"], None, voice_preset, avatar_option),
        daemon=True,
    )
    thread.start()
    return {"ok": True, "task_id": task_id, "task": _lab_task_payload(tasks[task_id])}


@app.post("/api/lab/property-video/jobs")
async def lab_property_video_job(
    request: Request,
    videos: list[UploadFile] = File(...),
    script_text: str = Form(...),
    voice_preset_id: str = Form(""),
    speed: float = Form(1.1),
    target_market: str = Form(""),
    bgm_item_id: str = Form(""),
    bgm_volume: float = Form(0.10),
):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    script_text = (script_text or "").strip()
    if not script_text:
        return JSONResponse({"error": "请先填写房源解说文案"}, status_code=400)
    if not videos:
        return JSONResponse({"error": "请至少上传一个房源视频"}, status_code=400)
    target_market = str(target_market or user.get("target_market") or "cn")
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    if voice_preset.get("enabled") is False:
        return JSONResponse({"error": "当前音色还未配置，暂时不可用"}, status_code=400)
    bgm_item_id = str(bgm_item_id or "").strip()
    if bgm_item_id and not _get_approved_bgm_path(bgm_item_id):
        return JSONResponse({"error": "选择的背景音乐不存在或还未审核通过"}, status_code=400)
    bgm_volume = max(0.0, min(float(bgm_volume or 0.10), 0.30))

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
            "property_video_mode": "real_shot_voiceover",
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    tracker.log("小程序房源实拍成片任务已创建，准备开始...")
    _push_live_event("task_created", "创建了小程序房源实拍成片任务", tasks[task_id])
    thread = threading.Thread(
        target=run_property_video_with_progress,
        args=(task_id, saved_paths, script_text, voice_preset, target_market, speed, bgm_item_id, bgm_volume, []),
        daemon=True,
    )
    thread.start()
    return {"ok": True, "task_id": task_id, "task": _lab_task_payload(tasks[task_id])}


@app.get("/api/lab/tasks/active")
async def lab_active_tasks(request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    items = [
        _lab_task_payload(task)
        for task in tasks.values()
        if _user_can_access_task(user, task)
    ]
    items.sort(key=lambda item: float(item.get("created_at") or 0), reverse=True)
    return {"items": items}


@app.get("/api/lab/tasks/{task_id}")
async def lab_task_status(task_id: str, request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    task = tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, task):
        return _forbidden_error()
    return {"task": _lab_task_payload(task)}


@app.get("/api/lab/tasks/{task_id}/download/{file_path:path}")
async def lab_task_download(task_id: str, file_path: str, request: Request):
    user, error = _require_lab_or_user(request)
    if error:
        return error
    task = tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    if not _user_can_access_task(user, task):
        return _forbidden_error()
    output_dir = task.get("output_dir")
    if not output_dir:
        return JSONResponse({"error": "输出目录不存在"}, status_code=404)
    base = Path(output_dir).resolve()
    target = (base / file_path).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if not target.exists() or not target.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    return FileResponse(str(target), filename=target.name)


def _opennews_collection_disabled_response() -> JSONResponse:
    return JSONResponse(
        {
            "error": "OpenNews 合集制作已停用。当前只保留 2 小时自动化 OpenNews 短视频生产，并将三语竖屏成片发布到 X/Facebook。",
            "disabled": True,
        },
        status_code=410,
    )


@app.get("/api/opennews/collections/pool")
async def opennews_collections_pool(request: Request, limit: int = 80, include_used: bool = False):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以管理 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.post("/api/opennews/collections/build")
async def opennews_collections_build(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以制作 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.get("/api/opennews/collections/jobs")
async def opennews_collections_jobs(request: Request, limit: int = 20):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以查看 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.get("/api/opennews/collections/jobs/{job_id}")
async def opennews_collections_job(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以查看 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.get("/api/opennews/collections/{job_id}/download")
async def opennews_collections_download(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以下载 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.post("/api/opennews/collections/{job_id}/publish-youtube")
async def opennews_collections_publish_youtube(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发布 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.post("/api/opennews/collections/{job_id}/publish-x")
async def opennews_collections_publish_x(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发布 OpenNews 合集")
    return _opennews_collection_disabled_response()


@app.post("/api/opennews/collections/{job_id}/publish-facebook")
async def opennews_collections_publish_facebook(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发布 OpenNews 合集")
    return _opennews_collection_disabled_response()


def _localtok_disabled_response() -> JSONResponse:
    return JSONResponse(
        {
            "error": "LocalTok 审核链路已停用。OpenNews 当前只保留 2 小时自动化生产，以及 X/Facebook 自动发布。",
            "disabled": True,
        },
        status_code=410,
    )


@app.get("/api/opennews/localtok/status")
async def opennews_localtok_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _localtok_disabled_response()


@app.get("/api/opennews/localtok/proposals")
async def opennews_localtok_proposals(request: Request, limit: int = 20):
    user, error = _require_user(request)
    if error:
        return error
    return _localtok_disabled_response()


@app.post("/api/opennews/localtok/propose")
async def opennews_localtok_propose(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _localtok_disabled_response()


@app.post("/api/opennews/localtok/proposals/{local_proposal_id}/check")
async def opennews_localtok_proposal_check(local_proposal_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _localtok_disabled_response()


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


def _lab_opennews_download_url(request: Request, history_id: str, output_dir: Path, value: str) -> str:
    rel = _history_relpath_from_value(str(output_dir), value)
    if not rel:
        return ""
    base_url = _get_public_base_url(request).rstrip("/")
    return f"{base_url}/api/lab/opennews/videos/{quote(history_id, safe='')}/download/{quote(rel, safe='/')}"


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
        history_id = item.get("history_id") or ""
        platform_metrics: dict[str, Any] = {}
        if history_id:
            try:
                output_dir = _resolve_history_output_dir(str(history_id))
                result = _load_result_from_output_dir(output_dir) if output_dir else None
                if output_dir and result:
                    platform_metrics = _collect_history_platform_metrics(output_dir, result, force_refresh=False)
            except Exception:
                platform_metrics = {}
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
            "material_review": item.get("material_review") or {},
            "review_history_id": item.get("review_history_id") or "",
            "review_task_id": item.get("review_task_id") or "",
            "review_result": item.get("review_result") or {},
            "review_updated_at": item.get("review_updated_at") or 0,
            "x_records": item.get("x_records") or [],
            "x_error": item.get("x_error") or "",
            "x_urls": [
                record.get("x_url")
                for record in (item.get("x_records") or [])
                if isinstance(record, dict) and record.get("x_url")
            ],
            "facebook_records": item.get("facebook_records") or [],
            "facebook_error": item.get("facebook_error") or "",
            "facebook_urls": [
                record.get("facebook_url")
                for record in (item.get("facebook_records") or [])
                if isinstance(record, dict) and record.get("facebook_url")
            ],
            "platform_metrics": platform_metrics,
            "error": item.get("error") or "",
        })
    total_count = len(items)
    completed_count = sum(1 for item in items if item.get("status") == "completed")
    failed_count = sum(1 for item in items if item.get("status") == "failed")
    publishing_count = sum(1 for item in items if item.get("status") in {"publishing_youtube", "publishing_x", "publishing_facebook"})
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


def _opennews_batch_job_payload_for_ui(job: dict) -> dict:
    payload = dict(job or {})
    items: list[dict] = []
    review_pending_count = 0
    completed_count = 0
    failed_count = 0
    for item in payload.get("items", []) or []:
        item_payload = dict(item or {})
        status = str(item_payload.get("status") or "")
        if status == "review_pending":
            review_pending_count += 1
        elif status == "completed":
            completed_count += 1
        elif status in {"failed", "review_failed"}:
            failed_count += 1
        review_result = _build_opennews_manual_review_result_payload(payload, item_payload)
        if review_result:
            item_payload["review_result"] = review_result
        items.append(item_payload)
    payload["items"] = items
    payload["review_pending_count"] = review_pending_count
    payload["completed_count"] = completed_count
    payload["failed_count"] = failed_count
    return payload


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


def _external_opennews_video_payload(
    request: Request,
    output_dir: Path,
    result: dict,
    *,
    force_metrics_refresh: bool = False,
) -> Optional[dict]:
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
        "opennews_channel_id": workflow_config.get("opennews_channel_id") or "",
        "opennews_channel_name": workflow_config.get("opennews_channel_name") or "",
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
        "x_records": result.get("x_publish_records") if isinstance(result.get("x_publish_records"), list) else [],
        "x_latest": result.get("x_publish_latest") if isinstance(result.get("x_publish_latest"), dict) else {},
        "x_urls": [
            record.get("x_url")
            for record in (result.get("x_publish_records") or [])
            if isinstance(record, dict) and record.get("x_url")
        ],
        "facebook_records": result.get("facebook_publish_records") if isinstance(result.get("facebook_publish_records"), list) else [],
        "facebook_latest": result.get("facebook_publish_latest") if isinstance(result.get("facebook_publish_latest"), dict) else {},
        "facebook_urls": [
            record.get("facebook_url")
            for record in (result.get("facebook_publish_records") or [])
            if isinstance(record, dict) and record.get("facebook_url")
        ],
        "platform_metrics": _collect_history_platform_metrics(output_dir, result, force_refresh=force_metrics_refresh),
    }


def _lab_opennews_video_payload(
    request: Request,
    output_dir: Path,
    result: dict,
    *,
    force_metrics_refresh: bool = False,
) -> Optional[dict]:
    payload = _external_opennews_video_payload(request, output_dir, result, force_metrics_refresh=force_metrics_refresh)
    if not payload:
        return None
    payload = json.loads(json.dumps(payload, ensure_ascii=False))
    history_id = output_dir.name
    raw_variants = result.get("final_video_variants")
    if isinstance(raw_variants, dict):
        for aspect, variant in raw_variants.items():
            if not isinstance(variant, dict):
                continue
            video_path = str(variant.get("final_video_path") or "")
            rel = _history_relpath_from_value(str(output_dir), video_path)
            if not rel or not (output_dir / rel).exists():
                continue
            variant_payload = (payload.get("variants") or {}).get(str(aspect))
            if isinstance(variant_payload, dict):
                variant_payload["download_url"] = _lab_opennews_download_url(request, history_id, output_dir, video_path)
    final_video_path = str(result.get("final_video_path") or "")
    final_rel = _history_relpath_from_value(str(output_dir), final_video_path)
    if final_rel and (output_dir / final_rel).exists():
        default_key = str((result.get("workflow_config") or {}).get("compose_aspect_ratio") or "default")
        variant_payload = (payload.get("variants") or {}).get(default_key)
        if isinstance(variant_payload, dict):
            variant_payload["download_url"] = _lab_opennews_download_url(request, history_id, output_dir, final_video_path)
    variants = payload.get("variants") if isinstance(payload.get("variants"), dict) else {}
    payload["vertical_url"] = (variants.get("vertical") or {}).get("download_url") or ""
    payload["horizontal_url"] = (variants.get("horizontal") or {}).get("download_url") or ""
    return payload


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
        "ai": read_count("OPENNEWS_BATCH_COLLECTION_AI_COUNT", 3),
        "robotics": read_count("OPENNEWS_BATCH_COLLECTION_ROBOTICS_COUNT", 1),
        "other": read_count("OPENNEWS_BATCH_COLLECTION_OTHER_COUNT", 2),
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


def _opennews_item_event_identity(item: dict) -> dict:
    """Return the cross-source event identity used by auto production selection."""
    article = dict(item.get("article") or item)
    return {
        "event_key": opennews_candidate_event_key(article),
        "event_tokens": opennews_candidate_event_tokens(article),
        "title_compact": opennews_candidate_title_compact(article),
    }


def _opennews_is_duplicate_auto_event(item: dict, selected_identities: list[dict]) -> bool:
    identity = _opennews_item_event_identity(item)
    event_key = str(identity.get("event_key") or "")
    event_tokens = list(identity.get("event_tokens") or [])
    title_compact = str(identity.get("title_compact") or "")
    for existing in selected_identities:
        if event_key and event_key == str(existing.get("event_key") or ""):
            return True
        if title_compact and opennews_candidate_title_similar(title_compact, str(existing.get("title_compact") or "")):
            return True
        if event_tokens and opennews_is_duplicate_event(event_tokens, list(existing.get("event_tokens") or [])):
            return True
    return False


def _opennews_event_identity_dedupe_key(identity: dict) -> str:
    event_key = str(identity.get("event_key") or "").strip()
    if event_key:
        return f"event:{event_key}"
    title_compact = str(identity.get("title_compact") or "").strip()
    if title_compact:
        return f"title:{title_compact}"
    tokens = [str(token or "").strip() for token in (identity.get("event_tokens") or []) if str(token or "").strip()]
    return f"tokens:{' '.join(tokens[:10])}" if tokens else ""


def _opennews_recent_completed_event_identities(*, limit: int = 0, exclude_job_id: str = "", channel_id: str = "") -> list[dict]:
    # 去重只对比“最近 N 条”已完成新闻，而不是全部历史——历史越大越容易把同话题新新闻误判为重复。
    # channel_id 非空时：只对比“同一频道”的历史，避免不同频道(如科技/话题/综合的房产题材)
    # 把某频道(如房产频道)的新新闻误判为重复而整批跳过不生产。
    if not limit:
        limit = max(50, int(os.getenv("OPENNEWS_DEDUP_HISTORY_LIMIT", "200") or "200"))
    scope_channel = _safe_opennews_channel_id(channel_id) if channel_id else ""
    identities: list[dict] = []
    seen_keys: set[str] = set()

    def add_item(item: dict) -> None:
        if len(identities) >= limit or not isinstance(item, dict):
            return
        identity = _opennews_item_event_identity(item)
        identity_key = _opennews_event_identity_dedupe_key(identity)
        if not identity_key or identity_key in seen_keys:
            return
        seen_keys.add(identity_key)
        identities.append(identity)

    jobs_dir = OPENNEWS_BATCH_DIR / "batch_jobs"
    if jobs_dir.exists():
        for job_path in sorted(jobs_dir.glob("opennews_batch_*.json"), key=lambda path: path.stat().st_mtime, reverse=True):
            if len(identities) >= limit:
                break
            if exclude_job_id and job_path.stem == exclude_job_id:
                continue
            try:
                job_data = json.loads(job_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            job_channel = _safe_opennews_channel_id(job_data.get("opennews_channel_id") or job_data.get("channel_id") or "")
            for item in job_data.get("items", []) or []:
                if str(item.get("status") or "") != "completed":
                    continue
                if not str(item.get("history_id") or "").strip():
                    continue
                if scope_channel:
                    item_channel = _safe_opennews_channel_id(item.get("opennews_channel_id") or job_channel or "")
                    if item_channel != scope_channel:
                        continue
                add_item(item)
                if len(identities) >= limit:
                    break

    if len(identities) < limit and OUTPUT_DIR.exists():
        output_dirs = [path for path in OUTPUT_DIR.iterdir() if path.is_dir()]
        for output_dir in sorted(output_dirs, key=lambda path: path.stat().st_mtime, reverse=True):
            if len(identities) >= limit:
                break
            result_path = output_dir / "result.json"
            if not result_path.exists():
                continue
            try:
                result = json.loads(result_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not _is_opennews_result(result):
                continue
            if scope_channel and _opennews_result_channel_id(result) != scope_channel:
                continue
            source_article = (
                ((result.get("workflow_config") or {}).get("source") or {}).get("article")
                or ((result.get("source") or {}).get("article") if isinstance(result.get("source"), dict) else {})
                or {}
            )
            if isinstance(source_article, dict) and source_article:
                add_item({"article": source_article, "history_id": output_dir.name, "status": "completed"})
            else:
                add_item(result)

    return identities


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
    seen_identities = [_opennews_item_event_identity(item) for item in expanded]

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
                if _opennews_is_duplicate_auto_event(item, seen_identities):
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
                seen_identities.append(_opennews_item_event_identity(item))
                missing -= 1
    return expanded


def _select_opennews_auto_collection_items(items: list[dict], *, time_range: str = "6h", category: str = "") -> list[dict]:
    # ai/robotics/other 配比 + 补抓 只对科技类频道有意义;对房产/金融等频道会去补抓科技(ai/robotics)
    # 内容来填配比,把跨题材新闻塞进本频道 job,造成串台。所以非科技类目直接用已按题材过滤好的本频道料。
    category = str(category or "").strip().lower()
    if category and category not in {"technology", "ai", "all"}:
        ranked = sorted(
            [item for item in items if isinstance(item, dict)],
            key=_opennews_batch_item_score,
            reverse=True,
        )
        return ranked
    counts = _opennews_auto_collection_mix_counts()
    total = sum(counts.values())
    if total <= 0:
        return []

    minimum_total = max(
        3,
        min(
            total,
            int(os.getenv("OPENNEWS_AUTO_COLLECTION_MIN_ITEMS", "3") or "3"),
        ),
    )

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
    selected_identities: list[dict] = []

    def item_key(item: dict) -> str:
        return str(item.get("batch_item_id") or item.get("id") or _opennews_article_title(item)).strip()

    def take_from(bucket_name: str, count: int) -> None:
        if count <= 0:
            return
        for item in buckets.get(bucket_name, []):
            key = item_key(item)
            if not key or key in selected_keys:
                continue
            if _opennews_is_duplicate_auto_event(item, selected_identities):
                continue
            selected.append(item)
            selected_keys.add(key)
            selected_identities.append(_opennews_item_event_identity(item))
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
                    if key and key not in selected_keys and not _opennews_is_duplicate_auto_event(item, selected_identities):
                        fallback_item = dict(item)
                        selected_keys.add(key)
                        selected_identities.append(_opennews_item_event_identity(item))
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
            "[OpenNews auto collection] strict mix degraded "
            f"required={counts}, selected={selected_counts}, missing={missing}, minimum_total={minimum_total}, "
            f"available={{'ai': {len(buckets['ai'])}, 'robotics': {len(buckets['robotics'])}, 'other': {len(buckets['other'])}}}",
            flush=True,
        )
        if len(selected) < minimum_total:
            print(
                "[OpenNews auto collection] skip batch: insufficient items after degradation "
                f"selected={len(selected)} minimum_total={minimum_total}",
                flush=True,
            )
            return []

    return selected[:total]


def _select_opennews_batch_top_item(items: list[dict]) -> Optional[dict]:
    ranked = sorted(
        [item for item in items if isinstance(item, dict)],
        key=_opennews_batch_item_score,
        reverse=True,
    )
    return dict(ranked[0]) if ranked else None


# 各频道题材白名单:命中任一关键词才算"属于该频道题材"(中英都匹配)。
# 只对这些题材明确的频道过滤;综合/其它不在表内的频道不过滤。
_OPENNEWS_CHANNEL_TOPIC_KEYWORDS = {
    "real_estate_immigration": [
        "real estate", "real-estate", "housing", "house price", "home price", "home prices",
        "mortgage", "property", "properties", "apartment", "condo", "rent", "rental", "landlord",
        "tenant", "reit", "homebuilder", "home builder", "housing market", "realty", "home sales",
        "homebuyer", "home buyer", "home market", "luxury home", "commercial property", "residential",
        "immigration", "immigrant", "migrant", "visa", "citizenship", "green card",
        "permanent resident", "asylum", "border policy", "work permit", "international student",
        "住房", "住宅", "豪宅", "楼盘", "房产", "房价", "房贷", "楼市", "租金", "租房", "房东", "置业", "购房", "买房",
        "公寓", "地产", "不动产", "物业", "移民", "签证", "入籍", "绿卡", "永居", "居留", "留学生",
    ],
    "real_estate": [
        "real estate", "real-estate", "housing", "house price", "home price", "mortgage", "property",
        "apartment", "condo", "rent", "rental", "landlord", "tenant", "reit", "homebuilder",
        "housing market", "realty", "home sales", "homebuyer", "home market", "luxury home", "residential",
        "住房", "住宅", "豪宅", "楼盘", "房产", "房价", "房贷", "楼市", "租金", "租房", "房东", "置业", "购房", "买房", "公寓", "地产", "不动产", "物业",
    ],
    "immigration": [
        "immigration", "immigrant", "migrant", "visa", "citizenship", "green card",
        "permanent resident", "asylum", "border policy", "work permit", "international student",
        "移民", "签证", "入籍", "绿卡", "永居", "居留", "留学生", "国际学生",
    ],
    "technology": [
        "ai", "artificial intelligence", "chip", "semiconductor", "software", "robot", "robotics",
        "tech", "technology", "startup", "nvidia", "amd", "intel", "apple", "google", "microsoft",
        "amazon", "meta", "openai", "quantum", "data center", "data centre", "gpu", "cloud",
        "smartphone", "cyber", "blockchain", "chatbot", "algorithm", "5g", "satellite",
        "科技", "芯片", "半导体", "人工智能", "机器人", "软件", "算法", "量子", "数据中心", "智能", "互联网", "云计算",
    ],
    "ai": [
        "ai", "artificial intelligence", "generative", "openai", "anthropic", "nvidia", "chip",
        "machine learning", "large language model", "chatbot", "算法", "人工智能", "大模型", "智能", "芯片",
    ],
}


# 重叠类目(比较主次时跳过自己的子/父类目,避免自我竞争)
_OPENNEWS_TOPIC_OVERLAP = {
    "real_estate_immigration": {"real_estate", "immigration"},
    "real_estate": {"real_estate_immigration"},
    "immigration": {"real_estate_immigration"},
    "technology": {"ai"},
    "ai": {"technology"},
}


def _opennews_item_topic_text(item: dict) -> str:
    return " ".join(
        str(item.get(k) or "")
        for k in ("title", "title_zh", "topic", "topic_zh", "headline", "summary", "summary_zh")
    ).lower()


def _opennews_item_topic_score(text: str, keywords: list) -> int:
    return sum(1 for kw in set(str(k).lower() for k in keywords) if kw in text)


def _filter_items_for_channel_topic(items: list, channel: dict) -> list:
    """best-fit 题材过滤:一条新闻归给'题材词命中最多'的那一类。
    只有当本频道类目是这条的主导题材(命中数≥其它任何类目)时才保留,
    避免'蹭了一个词就串台'(如房产新闻蹭'科技从业者'被当科技)。"""
    category = str((channel or {}).get("category") or "").strip().lower()
    own_kws = _OPENNEWS_CHANNEL_TOPIC_KEYWORDS.get(category)
    if not own_kws:
        return items  # 无题材白名单的频道不过滤
    overlap = _OPENNEWS_TOPIC_OVERLAP.get(category, set())
    kept = []
    for it in items:
        text = _opennews_item_topic_text(it)
        if not text.strip():
            kept.append(it)  # 无文本不误杀
            continue
        own = _opennews_item_topic_score(text, own_kws)
        if own <= 0:
            continue  # 完全不含本频道题材 -> 丢
        best_other = 0
        for cat, kws in _OPENNEWS_CHANNEL_TOPIC_KEYWORDS.items():
            if cat == category or cat in overlap:
                continue
            best_other = max(best_other, _opennews_item_topic_score(text, kws))
        if own >= best_other:
            kept.append(it)
    dropped = len(items) - len(kept)
    if dropped:
        print(
            f"[topic filter] 频道题材={category} 丢弃非主导题材 {dropped} 条，保留 {len(kept)} 条",
            flush=True,
        )
    return kept


def _opennews_channel_published_event_keys(channel_id: str, *, platform: str = "youtube", exclude_dir: str = "", limit: int = 150) -> set:
    """扫描最近成片，收集'已发布到该频道该平台'的事件指纹集合(用于跨目录防重复发布)。"""
    channel_id = _safe_opennews_channel_id(channel_id)
    rec_field = {"youtube": "youtube_publish_records", "facebook": "facebook_publish_records", "x": "x_publish_records"}.get(platform, "youtube_publish_records")
    keys: set = set()
    if not OUTPUT_DIR.exists():
        return keys
    try:
        dirs = sorted(
            [p for p in OUTPUT_DIR.iterdir() if p.is_dir()],
            key=lambda p: p.stat().st_mtime, reverse=True,
        )[: max(20, limit)]
    except Exception:
        return keys
    for output_dir in dirs:
        if exclude_dir and output_dir.name == exclude_dir:
            continue
        result_path = output_dir / "result.json"
        if not result_path.exists():
            continue
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(result, dict) or not result.get(rec_field):
            continue
        if _opennews_result_channel_id(result) != channel_id:
            continue
        key = _opennews_event_identity_dedupe_key(_opennews_item_event_identity(result))
        if key:
            keys.add(key)
        u = _opennews_result_source_url(result)
        if u:
            keys.add("url:" + u)
    # 外部生产任务会先把平台回执写进 batch job；即使 result.json 后续被陈旧检查点覆盖，
    # 跨目录去重也必须能看到这份独立回执。
    batch_records_field = {
        "youtube": "youtube_records",
        "facebook": "facebook_records",
        "x": "x_records",
    }.get(platform, "youtube_records")
    jobs_dir = OPENNEWS_BATCH_DIR / "batch_jobs"
    try:
        job_paths = sorted(
            jobs_dir.glob("opennews_batch_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[: max(20, limit)]
    except Exception:
        job_paths = []
    for job_path in job_paths:
        try:
            job = json.loads(job_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        job_channel = _safe_opennews_channel_id(job.get("opennews_channel_id") or job.get("channel_id") or "")
        for item in job.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            if exclude_dir and str(item.get("history_id") or "") == exclude_dir:
                continue
            records = item.get(batch_records_field) or item.get(rec_field)
            if not isinstance(records, list) or not records:
                continue
            article = item.get("article") if isinstance(item.get("article"), dict) else item
            item_channel = _safe_opennews_channel_id(
                item.get("opennews_channel_id")
                or article.get("opennews_channel_id")
                or job_channel
                or ""
            )
            if item_channel != channel_id:
                continue
            key = _opennews_event_identity_dedupe_key(_opennews_item_event_identity(article))
            if key:
                keys.add(key)
            source_url = str(article.get("url") or article.get("source_url") or "").strip().lower()
            if source_url:
                keys.add("url:" + source_url)
    return keys


def _opennews_result_source_url(result: dict) -> str:
    if not isinstance(result, dict):
        return ""
    wc = result.get("workflow_config") if isinstance(result.get("workflow_config"), dict) else {}
    for src in (wc.get("source"), result.get("source")):
        if isinstance(src, dict):
            art = src.get("article")
            if isinstance(art, dict) and art.get("url"):
                return str(art.get("url")).strip().lower()
    return ""


def _handle_opennews_batch_after_fetch(root: Path, payload: dict) -> None:
    if os.getenv("OPENNEWS_BATCH_AUTO_COLLECTION_PRODUCE", "1").strip().lower() in {"0", "false", "no", "off"}:
        return
    config = load_opennews_batch_config(root)
    triggered_by = str(payload.get("triggered_by") or "")
    channel_id = _safe_opennews_channel_id(payload.get("opennews_channel_id") or payload.get("channel_id") or "")
    channel = _find_opennews_channel(channel_id, include_secrets=True) if channel_id else {}
    if not channel and triggered_by != "scheduler" and not config.get("enabled"):
        return
    items = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
    # 题材相关性过滤:只让属于本频道题材的内容进入生产,避免串台(如科技新闻发到房产频道)。
    if channel:
        items = _filter_items_for_channel_topic(items, channel)
    if not items:
        print(
            "[OpenNews auto collection] skip production: current batch has no fresh unique items "
            f"batch_id={payload.get('batch_id') or ''} duplicate_count={payload.get('duplicate_count') or 0}",
            flush=True,
        )
        return
    selected = _select_opennews_auto_collection_items(
        items,
        time_range=str(payload.get("time_range") or config.get("time_range") or "6h"),
        category=str((channel or {}).get("category") or payload.get("category") or ""),
    )
    if channel:
        selected = selected[: max(1, int(channel.get("produce_limit") or 6))]
    top_item = _select_opennews_batch_top_item(items) or _select_opennews_batch_top_item(selected)
    if not selected:
        return
    # 只与“同一频道”的历史去重:避免其它频道(科技/话题/综合)的同题材成片把本频道新料整批误判为重复。
    completed_history = _opennews_recent_completed_event_identities(channel_id=channel_id)
    job_items: list[dict] = []
    skipped_history_ids: list[str] = []
    for item in selected:
        item_id = str(item.get("batch_item_id") or item.get("id") or "").strip()
        if _opennews_is_duplicate_auto_event(item, completed_history):
            if item_id:
                skipped_history_ids.append(item_id)
            continue
        job_items.append(item)
        completed_history.append(_opennews_item_event_identity(item))
    if skipped_history_ids:
        mark_opennews_batch_items(
            root,
            skipped_history_ids,
            {
                "status": "skipped_duplicate",
                "auto_produce_skipped_at": time.time(),
                "auto_produce_reason": "already_completed_event",
                "message": "同一新闻事件已在历史成片中制作过，本批次自动跳过。",
            },
        )
        print(
            f"[OpenNews auto collection] skipped {len(skipped_history_ids)} already-completed duplicate events "
            f"batch_id={payload.get('batch_id') or ''}",
            flush=True,
        )
    if not job_items:
        print(
            "[OpenNews auto collection] skip production: selected items were all already completed duplicate events "
            f"batch_id={payload.get('batch_id') or ''}",
            flush=True,
        )
        return
    selected = job_items
    selected_ids = [str(item.get("batch_item_id") or item.get("id") or "").strip() for item in selected]
    selected_ids = [item_id for item_id in selected_ids if item_id]
    top_item_id = str((top_item or {}).get("batch_item_id") or (top_item or {}).get("id") or "").strip()
    if (
        top_item
        and top_item_id
        and top_item_id not in selected_ids
        and not _opennews_is_duplicate_auto_event(top_item, completed_history)
    ):
        job_items.insert(0, top_item)
        completed_history.append(_opennews_item_event_identity(top_item))
        selected_ids.insert(0, top_item_id)
    else:
        top_item = _select_opennews_batch_top_item(job_items)
        top_item_id = str((top_item or {}).get("batch_item_id") or (top_item or {}).get("id") or "").strip()
    if not selected_ids or not OPENNEWS_BATCH_AUTO_PRODUCE_LOCK.acquire(blocking=False):
        return
    try:
        user = _external_news_user()
        presenter_config = _next_opennews_batch_presenter_config()
        channel_languages = list(channel.get("languages") or []) if channel else []
        primary_market = (
            channel_languages[0]
            if channel_languages
            else os.getenv("OPENNEWS_BATCH_AUTO_TARGET_MARKET", "cn")
        )
        channel_name = str(channel.get("name") or payload.get("opennews_channel_name") or "OpenNews").strip()
        channel_platforms = channel.get("platforms") if isinstance(channel.get("platforms"), dict) else {}
        channel_x_auto_publish = (
            _opennews_x_auto_publish_default()
            and _parse_bool_form(channel_platforms.get("x", True))
        )
        channel_facebook_auto_publish = (
            _opennews_facebook_auto_publish_default()
            and _parse_bool_form(channel_platforms.get("facebook", True))
        )
        youtube_account_state = _opennews_publish_account_for(channel.get("id") or channel_id, primary_market, "youtube") if channel else {"enabled": False}
        channel_youtube_auto_publish = (
            _opennews_youtube_auto_publish_default()
            and not _opennews_youtube_auto_publish_disabled()
            and bool(youtube_account_state.get("enabled"))
        )
        job = create_opennews_batch_job(
            root,
            username="auto_opennews",
            items=job_items,
            options={
                "target_market": primary_market,
                "department_id": user.get("department_id") or "real_estate",
                "voice_preset_id": presenter_config.get("voice_preset_id") or os.getenv("OPENNEWS_BATCH_AUTO_VOICE_PRESET_ID", ""),
                "aspect_ratio": os.getenv("OPENNEWS_BATCH_AUTO_PREVIEW_ASPECT", "vertical"),
                "notes": f"{channel_name}自动抓取批次：生成配置语言的竖屏新闻视频，并按频道账号矩阵发布 X/Facebook"
                + (" 和 YouTube。" if channel_youtube_auto_publish else "。"),
                "youtube_auto_publish": channel_youtube_auto_publish,
                "youtube_privacy_status": os.getenv("OPENNEWS_BATCH_AUTO_YOUTUBE_PRIVACY", "public"),
                "youtube_aspects": ["vertical"] if channel_youtube_auto_publish else [],
                "x_auto_publish": channel_x_auto_publish,
                "x_publish_single_shorts": channel_x_auto_publish,
                "x_collection_auto_publish": False,
                "x_aspects": ["vertical"],
                "facebook_auto_publish": channel_facebook_auto_publish,
                "facebook_aspects": ["vertical"],
                "opennews_presenter": presenter_config,
                "opennews_channel_id": channel.get("id") or channel_id,
                "opennews_channel_name": channel_name,
                "opennews_language_markets": channel_languages or list(OPENNEWS_CHANNEL_LANGUAGE_IDS),
                "auto_collection_batch_id": payload.get("batch_id") or "",
                "auto_collection_item_ids": selected_ids,
                "auto_single_shorts_item_ids": [top_item_id] if top_item_id else [],
                "auto_collection_mix_counts": _opennews_auto_collection_mix_counts(),
                "auto_collection_direct": False,
                "material_strategy": "free_library_script_match",
            },
        )
        mark_opennews_batch_items(
            root,
            selected_ids,
            {
                "status": "auto_producing",
                "auto_produce_job_id": job.get("job_id") or "",
                "auto_produce_selected_at": time.time(),
                "auto_produce_reason": f"channel_{channel.get('id') or 'two_hour'}_automation",
                "auto_collection_mix_counts": _opennews_auto_collection_mix_counts(),
                "opennews_channel_id": channel.get("id") or channel_id,
                "opennews_channel_name": channel_name if channel else payload.get("opennews_channel_name") or "",
            },
        )
        if top_item_id:
            mark_opennews_batch_items(
                root,
                [top_item_id],
                {
                    "status": "auto_producing",
                    "auto_produce_job_id": job.get("job_id") or "",
                    "auto_produce_selected_at": time.time(),
                    "auto_produce_reason": "top_shorts",
                    "auto_single_shorts": True,
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
        "opennews_channel_id": item.get("opennews_channel_id") or "",
        "opennews_channel_name": item.get("opennews_channel_name") or "",
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


def _recover_opennews_task_from_output(task_id: str, expected_title: str = "") -> dict:
    expected_key = _make_safe_name(expected_title or "", fallback="").lower()
    candidates: list[Path] = []
    if OUTPUT_DIR.exists():
        candidates = sorted(
            [path for path in OUTPUT_DIR.iterdir() if path.is_dir() and (path / "result.json").exists()],
            key=lambda path: (path / "result.json").stat().st_mtime,
            reverse=True,
        )[:80]
    for output_dir in candidates:
        result_path = output_dir / "result.json"
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not _is_opennews_result(result):
            continue
        result_title = str(result.get("title") or ((result.get("script") or {}).get("title")) or "").strip()
        result_key = _make_safe_name(result_title, fallback="").lower()
        dir_key = output_dir.name.lower()
        if expected_key and expected_key not in result_key and expected_key not in dir_key:
            continue
        if _opennews_result_has_material_assets(result, output_dir):
            return {
                "id": task_id,
                "output_dir": str(output_dir),
                "result": result,
                "tracker": None,
                "recovered_from_output": True,
            }
    return {}


def _opennews_task_result_ready_for_compose(result: dict) -> bool:
    """判断一个 OpenNews 任务结果是否已具备合成条件（所有有台词的段都已生成配音）。
    生产过程中会写入中间检查点 result.json（音频只生成了一部分），不能据此就去合成，
    否则会拿到不完整音频 -> 合成报"没有可用的配音文件"。"""
    segments = (result or {}).get("segments") or []
    if not segments:
        return False
    for seg in segments:
        if not str((seg or {}).get("script") or "").strip():
            continue
        if not _segment_has_audio(seg):
            return False
    return True


def _wait_for_opennews_task_done(task_id: str, *, timeout_seconds: int = 5400, expected_title: str = "") -> dict:
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
        if not task:
            recovered_task = _recover_opennews_task_from_output(task_id, expected_title=expected_title)
            if recovered_task:
                return recovered_task
        if tracker_status == "done" and task.get("result") and task.get("output_dir"):
            return task
        # Some OpenNews jobs finish writing result.json before the in-memory
        # tracker flips to done. 但生产过程中也会写入"中间检查点"result.json
        # （音频只生成了一部分），若此时就返回，compose 会拿到不完整音频而失败。
        # 因此仅当结果已具备合成条件（所有段都已配音）时才提前返回。
        if (
            task.get("result")
            and task.get("output_dir")
            and tracker_status not in {"error", "cancelled"}
            and not bool((task.get("workflow_config") or {}).get("external_produce_managed"))
            and _opennews_task_result_ready_for_compose(task.get("result"))
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
    material_strategy = str(options.get("material_strategy") or "").strip().lower()
    opennews_channel_id = _safe_opennews_channel_id(options.get("opennews_channel_id") or "")
    opennews_channel_name = str(options.get("opennews_channel_name") or "").strip()
    opennews_language_markets = options.get("opennews_language_markets") if isinstance(options.get("opennews_language_markets"), list) else None
    presenter_config = _normalize_opennews_presenter_config(options.get("opennews_presenter"))
    external_request = dict(options.get("external_request") or {})
    callback_url = str(external_request.get("callback_url") or options.get("callback_url") or "").strip()
    youtube_auto_publish = False
    if "youtube_auto_publish" in external_request:
        youtube_auto_publish = _parse_bool_form(external_request.get("youtube_auto_publish"))
    if "youtube_auto_publish" in options:
        youtube_auto_publish = _parse_bool_form(options.get("youtube_auto_publish"))
    if _opennews_youtube_auto_publish_disabled():
        youtube_auto_publish = False
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
    x_auto_publish = _opennews_x_auto_publish_default()
    if "x_auto_publish" in external_request:
        x_auto_publish = _parse_bool_form(external_request.get("x_auto_publish"))
    if "x_auto_publish" in options:
        x_auto_publish = _parse_bool_form(options.get("x_auto_publish"))
    if _opennews_x_auto_publish_disabled():
        x_auto_publish = False
    facebook_auto_publish = _opennews_facebook_auto_publish_default()
    if "facebook_auto_publish" in external_request:
        facebook_auto_publish = _parse_bool_form(external_request.get("facebook_auto_publish"))
    if "facebook_auto_publish" in options:
        facebook_auto_publish = _parse_bool_form(options.get("facebook_auto_publish"))
    if _opennews_facebook_auto_publish_disabled():
        facebook_auto_publish = False
    x_aspects_raw = options.get("x_aspects") or external_request.get("x_aspects") or ["vertical"]
    if isinstance(x_aspects_raw, str):
        x_aspects = ["horizontal", "vertical"] if x_aspects_raw == "both" else [part.strip() for part in x_aspects_raw.split(",") if part.strip()]
    elif isinstance(x_aspects_raw, list):
        x_aspects = [str(part).strip() for part in x_aspects_raw if str(part).strip()]
    else:
        x_aspects = ["vertical"]
    set_job_status("running", "外部审核已确认，正在一站式生成新闻视频成片...")

    total_items = len(job.get("items") or [])
    completed_event_identities: list[dict] = _opennews_recent_completed_event_identities(exclude_job_id=job_id)
    for index, item in enumerate(job.get("items") or []):
        item_id = str(item.get("batch_item_id") or "")
        if _opennews_is_duplicate_auto_event(item, completed_event_identities):
            duplicate_message = "同一新闻事件已在历史成片或本批次中制作过，已跳过重复项。"
            def mark_duplicate(payload: dict, idx=index) -> None:
                for existing in payload.get("items", []) or []:
                    if str(existing.get("batch_item_id") or "") == item_id:
                        existing.update({
                            "status": "skipped_duplicate",
                            "message": duplicate_message,
                            "error": "",
                            "completed_at": time.time(),
                        })
                        break
                payload["message"] = f"外部审核视频生产进度：{idx + 1}/{total_items}"
            update_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id, mark_duplicate)
            sync_batch_item(item_id, status="skipped_duplicate", message=duplicate_message, error="", completed_at=time.time())
            continue
        if item.get("status") == "completed":
            completed_event_identities.append(_opennews_item_event_identity(item))
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

        output_dir: Optional[Path] = None
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
                presenter_config=presenter_config,
                material_strategy=material_strategy,
                batch_job_id=job_id,
                opennews_channel_id=opennews_channel_id,
                opennews_channel_name=opennews_channel_name,
                opennews_language_markets=opennews_language_markets,
                x_auto_publish=x_auto_publish,
                facebook_auto_publish=facebook_auto_publish,
                youtube_auto_publish=youtube_auto_publish,
                x_aspects=x_aspects,
                facebook_aspects=["vertical"],
                youtube_aspects=youtube_aspects,
                external_produce_managed=True,
            )
            task_id = str(task_result.get("task_id") or "")
            mark_item(task_id=task_id, message=f"视频生产任务已提交：{task_id}，等待中间产物完成...")
            task = _wait_for_opennews_task_done(
                task_id,
                expected_title=str(draft.get("video_title") or article.get("title") or ""),
            )
            output_dir_value = str(task.get("output_dir") or "").strip()
            if not output_dir_value:
                raise RuntimeError("OpenNews 任务没有生成输出目录。")
            output_dir = Path(output_dir_value)
            mark_item(status="composing", message="中间产物完成，正在自动合成横屏和竖屏成片...")
            composed_result = _compose_opennews_task_video(task_id, preferred_aspect_ratio=preferred_aspect_ratio)
            material_review = _opennews_material_review_status(composed_result, output_dir)
            if material_review.get("uses_strict_source_fallback"):
                composed_result["material_review"] = material_review
                _save_result_to_output_dir(Path(task.get("output_dir") or ""), composed_result)
            video_payload = _external_video_urls_for_result(public_base_url, output_dir, composed_result)
            youtube_records: list[dict] = []
            youtube_error = ""
            x_records: list[dict] = []
            x_error = ""
            facebook_records: list[dict] = []
            facebook_error = ""
            youtube_publish_this_item = youtube_auto_publish
            x_publish_this_item = x_auto_publish
            facebook_publish_this_item = facebook_auto_publish
            item_youtube_aspects = youtube_aspects or ["vertical"]
            item_x_aspects = x_aspects or ["vertical"]
            if youtube_publish_this_item:
                try:
                    mark_item(status="publishing_youtube", message="成片完成，正在自动发布到 YouTube...", material_review=material_review)
                    youtube_records = _publish_opennews_result_to_youtube(
                        output_dir,
                        composed_result,
                        aspects=item_youtube_aspects,
                        privacy_status=youtube_privacy_status,
                        include_language_versions=_opennews_youtube_publish_language_versions_enabled(),
                    )
                except Exception as youtube_exc:
                    youtube_error = str(youtube_exc)
            if x_publish_this_item:
                try:
                    publish_message = "成片完成，正在自动发布到 X..."
                    mark_item(status="publishing_x", message=publish_message, material_review=material_review)
                    x_records = _publish_opennews_result_to_x(
                        output_dir,
                        composed_result,
                        aspects=item_x_aspects,
                        include_language_versions=_opennews_x_publish_language_versions_enabled(),
                    )
                except Exception as x_exc:
                    x_error = str(x_exc)
            if facebook_publish_this_item:
                try:
                    mark_item(status="publishing_facebook", message="成片完成，正在自动发布到 Facebook...", material_review=material_review)
                    facebook_records = _publish_opennews_result_to_facebook(
                        output_dir,
                        composed_result,
                        aspects=["vertical"],
                        include_language_versions=_opennews_facebook_publish_language_versions_enabled(),
                    )
                except Exception as facebook_exc:
                    facebook_error = str(facebook_exc)
            if youtube_error:
                composed_result["youtube_auto_publish_error"] = youtube_error
                composed_result["youtube_publish_error"] = youtube_error
            if x_error:
                composed_result["x_auto_publish_error"] = x_error
                composed_result["x_publish_error"] = x_error
            if facebook_error:
                composed_result["facebook_auto_publish_error"] = facebook_error
                composed_result["facebook_publish_error"] = facebook_error
            _save_result_to_output_dir(output_dir, composed_result)
            facebook_pending_at = float(composed_result.get("facebook_publish_pending_at") or 0)
            facebook_pending_reason = str(composed_result.get("facebook_publish_pending_reason") or "").strip()
            final_status = "completed"
            published_platforms = []
            failed_parts = []
            skipped_parts = []
            if youtube_publish_this_item:
                if youtube_error:
                    failed_parts.append(f"YouTube 发布失败：{youtube_error}")
                elif youtube_records:
                    published_platforms.append("YouTube")
                else:
                    skipped_parts.append("YouTube 未返回发布记录，已保留成片等待恢复检查。")
            elif youtube_error:
                skipped_parts.append(f"YouTube 自动发布已跳过：{youtube_error}")
            if x_publish_this_item:
                if x_error:
                    failed_parts.append(f"X 发布失败：{x_error}")
                elif x_records:
                    published_platforms.append("X")
                else:
                    skipped_parts.append("X 未返回发布记录。")
            elif x_error:
                skipped_parts.append(f"X 自动发布已跳过：{x_error}")
            if facebook_publish_this_item:
                if facebook_pending_at:
                    skipped_parts.append(facebook_pending_reason or "Facebook 已排队等待自动补发。")
                elif facebook_error:
                    failed_parts.append(f"Facebook 发布失败：{facebook_error}")
                elif facebook_records:
                    published_platforms.append("Facebook")
                else:
                    skipped_parts.append("Facebook 未返回发布记录，已保留成片等待恢复检查。")
            elif facebook_error:
                skipped_parts.append(f"Facebook 自动发布已跳过：{facebook_error}")
            if published_platforms:
                final_message = f"成片已完成，{' / '.join(published_platforms)} 已发布。"
                if failed_parts:
                    final_message += " 但" + "；".join(failed_parts)
                if skipped_parts:
                    final_message += " " + "；".join(skipped_parts)
            elif failed_parts:
                final_message = "成片已完成，但" + "；".join(failed_parts)
            elif skipped_parts:
                final_message = "成片已完成，但" + "；".join(skipped_parts)
            else:
                final_message = "成片已完成，可直接下载。"
            mark_item(
                status=final_status,
                message=final_message,
                history_id=output_dir.name,
                video=video_payload,
                vertical_url=video_payload.get("vertical_url", ""),
                horizontal_url=video_payload.get("horizontal_url", ""),
                youtube_records=youtube_records,
                youtube_error=youtube_error,
                x_records=x_records,
                x_error=x_error,
                facebook_records=facebook_records,
                facebook_error=facebook_error,
                facebook_pending_at=facebook_pending_at,
                facebook_pending_reason=facebook_pending_reason,
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
                x_records=x_records,
                x_error=x_error,
                facebook_records=facebook_records,
                facebook_error=facebook_error,
                facebook_pending_at=facebook_pending_at,
                facebook_pending_reason=facebook_pending_reason,
                material_review=material_review,
                error="",
                completed_at=time.time(),
            )
            completed_event_identities.append(_opennews_item_event_identity(item))
        except Exception as exc:
            mark_item(status="failed", message=f"生成失败：{exc}", error=str(exc))
            sync_batch_item(item_id, status="failed", message=f"生成失败：{exc}", error=str(exc), completed_at=time.time())
        finally:
            _clear_opennews_external_produce_active(output_dir)
        update_opennews_batch_job(
            OPENNEWS_BATCH_DIR,
            job_id,
            lambda payload, idx=index: payload.update({"message": f"外部审核视频生产进度：{idx + 1}/{total_items}"}),
        )

    final_job = load_opennews_batch_job(OPENNEWS_BATCH_DIR, job_id) or {}
    failed = sum(1 for item in final_job.get("items", []) or [] if item.get("status") == "failed")
    completed = sum(1 for item in final_job.get("items", []) or [] if item.get("status") == "completed")
    set_job_status("done" if failed == 0 else "partial", f"外部审核视频已完成：{completed} 条成功，{failed} 条失败。")
    if completed:
        print(
            f"[OpenNews] collection build skipped for batch_job={job_id}; "
            "single-video X/Facebook publishing is the only active distribution path.",
            flush=True,
        )
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
    x_config = x_env_config()
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
            "auto_publish_disabled": os.getenv("OPENNEWS_YOUTUBE_AUTO_PUBLISH_DISABLED", "0").strip().lower() not in {"0", "false", "no", "off"},
            "default_auto_publish": False,
            "default_privacy_status": "public",
            "default_aspects": ["horizontal", "vertical"],
            "publish_language_versions_enabled": _opennews_youtube_publish_language_versions_enabled(),
        },
        "x": {
            "configured": x_browser_auth_ready() if _opennews_x_publish_mode() == "browser" else bool(x_config.get("client_id") and x_config.get("redirect_uri") and (x_config.get("refresh_token") or X_TOKEN_STORE_PATH.exists())),
            "publish_mode": _opennews_x_publish_mode(),
            "publish_mode_label": _opennews_x_publish_mode_label(),
            "browser": {
                **x_browser_env_config(),
                "auth_ready": x_browser_auth_ready(),
            },
            "client_id_configured": bool(x_config.get("client_id")),
            "client_secret_configured": bool(x_config.get("client_secret")),
            "redirect_uri": x_config.get("redirect_uri") or "",
            "refresh_token_configured": bool(x_config.get("refresh_token") or X_TOKEN_STORE_PATH.exists()),
            "auto_publish_enabled": _opennews_x_auto_publish_default(),
            "auto_publish_disabled": _opennews_x_auto_publish_disabled(),
            "publish_single_shorts_enabled": _opennews_x_single_shorts_enabled(),
            "publish_collection_enabled": _opennews_x_collection_enabled(),
            "default_aspects": ["vertical"],
            "publish_language_versions_enabled": _opennews_x_publish_language_versions_enabled(),
        },
        "facebook": {
            "configured": bool(
                facebook_env_config().get("app_id")
                and facebook_env_config().get("app_secret")
                and facebook_env_config().get("redirect_uri")
                and (
                    (facebook_env_config().get("page_id") and facebook_env_config().get("page_access_token"))
                    or FACEBOOK_TOKEN_STORE_PATH.exists()
                )
            ),
            "auto_publish_enabled": _opennews_facebook_auto_publish_default(),
            "auto_publish_disabled": _opennews_facebook_auto_publish_disabled(),
            "publish_single_shorts_enabled": _opennews_facebook_single_shorts_enabled(),
            "publish_collection_enabled": _opennews_facebook_collection_enabled(),
            "default_aspects": ["vertical"],
            "publish_language_versions_enabled": _opennews_facebook_publish_language_versions_enabled(),
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
    youtube_auto_publish = False
    youtube_aspects: list[str] = []
    x_auto_publish = payload.get("x_auto_publish")
    if x_auto_publish is None:
        x_auto_publish = _opennews_x_auto_publish_default()
    else:
        x_auto_publish = _parse_bool_form(x_auto_publish)
    if _opennews_x_auto_publish_disabled():
        x_auto_publish = False
    x_aspects = payload.get("x_aspects") or ["vertical"]
    if isinstance(x_aspects, str):
        x_aspects = ["horizontal", "vertical"] if x_aspects == "both" else [part.strip() for part in x_aspects.split(",") if part.strip()]
    elif isinstance(x_aspects, list):
        x_aspects = [str(part).strip() for part in x_aspects if str(part).strip()]
    else:
        x_aspects = ["vertical"]
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
            "youtube_aspects": youtube_aspects,
            "x_auto_publish": x_auto_publish,
            "x_aspects": x_aspects or ["vertical"],
            "facebook_auto_publish": _opennews_facebook_auto_publish_default(),
            "facebook_aspects": ["vertical"],
            "external_trigger": True,
            "external_request": {
                "item_ids": item_ids,
                "feedback": feedback,
                "callback_url": callback_url,
                "x_auto_publish": x_auto_publish,
                "x_aspects": x_aspects or ["vertical"],
                "facebook_auto_publish": _opennews_facebook_auto_publish_default(),
                "facebook_aspects": ["vertical"],
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
        "youtube_aspects": youtube_aspects,
        "x_auto_publish": x_auto_publish,
        "x_aspects": x_aspects or ["vertical"],
        "requested_count": len(item_ids),
        "accepted_count": len(items),
        "missing_item_ids": missing_item_ids,
        "accepted_item_ids": sorted(found_ids),
        "mode": "sync" if wait_until_done else "async",
        "message": "已接收外部审核选择，开始自动生成新闻视频；完成后 job.items 会返回 vertical_url、horizontal_url、x_records 和 facebook_records。",
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
    payload["job"] = _opennews_batch_job_payload_for_ui(job)
    return payload


@app.get("/api/external/opennews/ready-videos")
async def external_opennews_ready_videos(request: Request, limit: int = 50, refresh: int = 0):
    token_error = _require_external_news_token(request)
    if token_error:
        return token_error
    videos: list[dict] = []
    max_items = max(1, min(int(limit or 50), 200))
    for output_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True):
        result = _load_result_from_output_dir(output_dir)
        payload = _external_opennews_video_payload(request, output_dir, result or {}, force_metrics_refresh=bool(refresh))
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
    return _localtok_disabled_response()


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


def _resolve_opennews_voice_preset_id(target_market: str, requested_voice_preset_id: str = "") -> str:
    target_market = str(target_market or "cn").strip() or "cn"
    voice_preset_id = str(requested_voice_preset_id or "").strip()
    if voice_preset_id and voice_preset_id in _get_visible_voice_preset_ids(target_market):
        return voice_preset_id
    return _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female"


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
    presenter_config: Optional[dict] = None,
    material_strategy: str = "",
    batch_job_id: str = "",
    opennews_channel_id: str = "",
    opennews_channel_name: str = "",
    opennews_language_markets: Optional[list[str]] = None,
    x_auto_publish: Optional[bool] = None,
    facebook_auto_publish: Optional[bool] = None,
    youtube_auto_publish: Optional[bool] = None,
    x_aspects: Optional[list[str]] = None,
    facebook_aspects: Optional[list[str]] = None,
    youtube_aspects: Optional[list[str]] = None,
    external_produce_managed: bool = False,
) -> dict:
    presenter_config = _normalize_opennews_presenter_config(presenter_config)
    target_market = str(target_market or user.get("target_market") or "cn").strip() or "cn"
    if target_market not in {item["id"] for item in TARGET_MARKETS}:
        target_market = "cn"
    department_id = str(department_id or user.get("department_id") or "real_estate").strip() or "real_estate"
    voice_preset_id = str(
        voice_preset_id
        or presenter_config.get("voice_preset_id")
        or _get_target_market(target_market).get("default_voice_preset_id")
        or "mandarin_female"
    ).strip()
    if voice_preset_id not in _get_visible_voice_preset_ids(target_market):
        voice_preset_id = _get_target_market(target_market).get("default_voice_preset_id") or "mandarin_female"
    voice_preset = _get_voice_preset(voice_preset_id, target_market)
    voice_preset["selected_speed"] = float(voice_preset.get("default_speed") or 1.1)
    aspect_ratio = str(aspect_ratio or "horizontal").strip().lower()
    if aspect_ratio not in {"vertical", "horizontal"}:
        aspect_ratio = "horizontal"
    normalized_material_strategy = str(material_strategy or "free_library_script_match").strip().lower()
    opennews_channel_id = _safe_opennews_channel_id(opennews_channel_id or article.get("opennews_channel_id") or "")
    opennews_channel_name = str(opennews_channel_name or article.get("opennews_channel_name") or "").strip()
    requested_language_markets = None
    if opennews_language_markets is not None:
        normalized_language_markets = _normalize_opennews_extra_target_markets(opennews_language_markets, target_market)
        requested_language_markets = [target_market] + normalized_language_markets
    script_data = build_opennews_script_data(draft=draft, article=article, target_market=target_market)
    script_data = _apply_opennews_material_strategy(
        script_data,
        strategy=normalized_material_strategy,
        batch_job_id=batch_job_id,
    )
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
            "opennews_presenter": presenter_config,
            "material_strategy": normalized_material_strategy,
            "batch_job_id": str(batch_job_id or "").strip(),
            "opennews_channel_id": opennews_channel_id,
            "opennews_channel_name": opennews_channel_name,
            **({"opennews_language_markets": requested_language_markets} if requested_language_markets else {}),
            **({"x_auto_publish": bool(x_auto_publish)} if x_auto_publish is not None else {}),
            **({"facebook_auto_publish": bool(facebook_auto_publish)} if facebook_auto_publish is not None else {}),
            **({"youtube_auto_publish": bool(youtube_auto_publish)} if youtube_auto_publish is not None else {}),
            **({"x_aspects": list(x_aspects)} if x_aspects is not None else {}),
            **({"facebook_aspects": list(facebook_aspects)} if facebook_aspects is not None else {}),
            **({"youtube_aspects": list(youtube_aspects)} if youtube_aspects is not None else {}),
            "external_produce_managed": bool(external_produce_managed),
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


def _coerce_localized_opennews_script(source_script: dict, localized_script: dict) -> dict:
    source_timeline_segments = list(source_script.get("segments", []) or [])
    localized_segments = list(localized_script.get("segments", []) or []) if isinstance(localized_script, dict) else []
    payload = {
        "title": str((localized_script or {}).get("title") or (localized_script or {}).get("video_title") or source_script.get("title") or "").strip(),
        "cover_title": str((localized_script or {}).get("cover_title") or source_script.get("cover_title") or "").strip(),
        "total_duration": int(source_script.get("total_duration") or (localized_script or {}).get("total_duration") or 0),
        "segment_count": len(source_timeline_segments),
        "segments": [],
        "social_post": str((localized_script or {}).get("social_post") or source_script.get("social_post") or "").strip(),
    }
    for index, source_seg in enumerate(source_timeline_segments):
        localized_seg = localized_segments[index] if index < len(localized_segments) and isinstance(localized_segments[index], dict) else {}
        seg_type = str(source_seg.get("type") or localized_seg.get("type") or "material")
        normalized = {
            "type": seg_type,
            "start": source_seg.get("start"),
            "end": source_seg.get("end"),
            "duration": source_seg.get("duration"),
            "script": str(localized_seg.get("script") or source_seg.get("script") or "").strip(),
        }
        if seg_type == "digital_human":
            normalized["action"] = str(localized_seg.get("action") or source_seg.get("action") or "").strip()
        else:
            normalized["material_keyword"] = str(
                localized_seg.get("material_keyword")
                or source_seg.get("material_keyword")
                or normalized["script"]
            ).strip()
            normalized["material_search_keyword"] = str(
                localized_seg.get("material_search_keyword")
                or source_seg.get("material_search_keyword")
                or ""
            ).strip()
            normalized["material_desc"] = str(
                localized_seg.get("material_desc")
                or source_seg.get("material_desc")
                or ""
            ).strip()
        payload["segments"].append(normalized)
    return payload


def _opennews_language_text_blob(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    parts = [
        str(payload.get("title") or ""),
        str(payload.get("cover_title") or ""),
        str(payload.get("social_post") or ""),
    ]
    script_payload = payload.get("script") if isinstance(payload.get("script"), dict) else payload
    if isinstance(script_payload, dict):
        parts.extend([
            str(script_payload.get("title") or ""),
            str(script_payload.get("cover_title") or ""),
            str(script_payload.get("social_post") or ""),
        ])
        for segment in script_payload.get("segments") or []:
            if isinstance(segment, dict):
                parts.append(str(segment.get("script") or ""))
    for segment in payload.get("segments") or []:
        if isinstance(segment, dict):
            parts.append(str(segment.get("script") or ""))
    return "\n".join(part for part in parts if part).strip()


def _opennews_target_language_mismatch(payload: dict, target_market: str) -> bool:
    target_market = str(target_market or "cn").strip().lower() or "cn"
    text = _opennews_language_text_blob(payload)
    if not text:
        return True
    cjk_count = len(re.findall(r"[\u4e00-\u9fff]", text))
    kana_count = len(re.findall(r"[\u3040-\u30ff]", text))
    alpha_count = len(re.findall(r"[A-Za-z]", text))
    if target_market == "jp":
        return kana_count < 8
    if target_market == "en":
        return alpha_count < max(40, (cjk_count + kana_count) * 2)
    if target_market in {"cn", "tw"}:
        return cjk_count < max(20, alpha_count // 3)
    return False


def _prepare_opennews_language_script(
    *,
    source_topic: str,
    source_script: dict,
    primary_workflow_config: dict,
    target_market: str,
    department_id: str,
    provider: str,
) -> tuple[dict, dict[str, Any]]:
    source_article = {}
    source_cfg = (primary_workflow_config or {}).get("source") or {}
    if isinstance(source_cfg, dict):
        source_article = dict(source_cfg.get("article") or {})
    localized_meta: dict[str, Any] = {}
    native_error = ""

    if source_article:
        try:
            localized_draft = generate_opennews_draft(
                article=source_article,
                target_market=target_market,
                notes="",
            )
            localized_raw_script = build_opennews_script_data(
                draft=localized_draft,
                article=source_article,
                target_market=target_market,
            )
            native_script = _coerce_localized_opennews_script(source_script, localized_raw_script)
            if _opennews_target_language_mismatch(native_script, target_market):
                raise RuntimeError(f"{target_market} 原生新闻稿生成语种校验失败")
            localized_meta = {
                "generation_mode": "market_native_from_article",
                "draft_title": str(localized_draft.get("video_title") or "").strip(),
                "usage": (localized_draft.get("_meta") or {}).get("usage") or {},
            }
            return native_script, localized_meta
        except Exception as exc:
            native_error = str(exc)

    if source_article and native_error:
        localized_draft = _local_opennews_language_fallback(
            article=source_article,
            target_market=target_market,
            published_at=str(source_article.get("published_at") or ""),
        )
        localized_raw_script = build_opennews_script_data(
            draft=localized_draft,
            article=source_article,
            target_market=target_market,
        )
        native_fallback_script = _coerce_localized_opennews_script(source_script, localized_raw_script)
        if _opennews_target_language_mismatch(native_fallback_script, target_market):
            raise RuntimeError(f"OpenNews {target_market} 原生新闻稿生成失败：{native_error}")
        return native_fallback_script, {
            "generation_mode": "local_market_language_fallback",
            "draft_title": str(localized_draft.get("video_title") or "").strip(),
            "native_error": native_error,
        }

    from generate_script import translate_script_data

    try:
        translated_script = translate_script_data(
            source_topic,
            source_script,
            target_market=target_market,
            department_id=department_id,
            provider=provider,
        )
        localized_meta = translated_script.pop("_meta", {}) if isinstance(translated_script, dict) else {}
        localized_meta["generation_mode"] = localized_meta.get("generation_mode") or "timeline_translation"
        if _opennews_target_language_mismatch(translated_script, target_market):
            raise RuntimeError(f"{target_market} 翻译稿语种校验失败")
    except Exception as exc:
        if not source_article:
            raise
        try:
            localized_draft = generate_opennews_draft(
                article=source_article,
                target_market=target_market,
                notes="",
            )
        except Exception:
            localized_draft = _local_opennews_language_fallback(
                article=source_article,
                target_market=target_market,
                published_at=str(source_article.get("published_at") or ""),
            )
        localized_raw_script = build_opennews_script_data(
            draft=localized_draft,
            article=source_article,
            target_market=target_market,
        )
        translated_script = _coerce_localized_opennews_script(source_script, localized_raw_script)
        localized_meta = {
            "generation_mode": "market_native_fallback",
            "draft_title": str(localized_draft.get("video_title") or "").strip(),
            "translation_error": str(exc),
            "native_error": native_error,
        }
        if _opennews_target_language_mismatch(translated_script, target_market):
            localized_draft = _local_opennews_language_fallback(
                article=source_article,
                target_market=target_market,
                published_at=str(source_article.get("published_at") or ""),
            )
            localized_raw_script = build_opennews_script_data(
                draft=localized_draft,
                article=source_article,
                target_market=target_market,
            )
            translated_script = _coerce_localized_opennews_script(source_script, localized_raw_script)
            localized_meta.update(
                {
                    "generation_mode": "local_market_language_fallback",
                    "draft_title": str(localized_draft.get("video_title") or "").strip(),
                }
            )
            if _opennews_target_language_mismatch(translated_script, target_market):
                raise RuntimeError(f"{target_market} 备用新闻稿语种校验失败")
    return translated_script, localized_meta


def _build_opennews_language_script_only_version(
    *,
    source_topic: str,
    source_script: dict,
    source_segments: list[dict],
    primary_workflow_config: dict,
    target_market: str,
    department_id: str,
    provider: str,
) -> dict:
    translated_script, localized_meta = _prepare_opennews_language_script(
        source_topic=source_topic,
        source_script=source_script,
        primary_workflow_config=primary_workflow_config,
        target_market=target_market,
        department_id=department_id,
        provider=provider,
    )
    market = _get_target_market(target_market)
    primary_presenter = _normalize_opennews_presenter_config((primary_workflow_config or {}).get("opennews_presenter"))
    presenter_for_market = _opennews_presenter_config_for_market(
        target_market=target_market,
        gender=str(primary_presenter.get("gender") or "female"),
    )
    translated_segments: list[dict] = []
    for index, translated_seg in enumerate(translated_script.get("segments", []) or []):
        base_seg = source_segments[index] if index < len(source_segments) else {}
        seg_copy = copy.deepcopy(base_seg if isinstance(base_seg, dict) else {})
        seg_copy.update(
            {
                "type": translated_seg.get("type", seg_copy.get("type")),
                "start": translated_seg.get("start", seg_copy.get("start")),
                "end": translated_seg.get("end", seg_copy.get("end")),
                "duration": translated_seg.get("duration", seg_copy.get("duration")),
                "script": translated_seg.get("script", seg_copy.get("script", "")),
                "target_market": target_market,
                "department_id": department_id,
            }
        )
        if seg_copy.get("type") == "digital_human":
            seg_copy["action"] = translated_seg.get("action", seg_copy.get("action", ""))
        else:
            seg_copy["material_keyword"] = translated_seg.get("material_keyword", seg_copy.get("material_keyword", ""))
            seg_copy["material_search_keyword"] = translated_seg.get("material_search_keyword", seg_copy.get("material_search_keyword", ""))
            seg_copy["material_desc"] = translated_seg.get("material_desc", seg_copy.get("material_desc", ""))
        translated_segments.append(seg_copy)
    return {
        "target_market": target_market,
        "title": translated_script.get("title") or "",
        "cover_title": translated_script.get("cover_title") or "",
        "total_duration": translated_script.get("total_duration") or source_script.get("total_duration") or 0,
        "segment_count": len(translated_segments),
        "script": translated_script,
        "segments": translated_segments,
        "social_post": translated_script.get("social_post") or "",
        "workflow_config": {
            **(primary_workflow_config or {}),
            "target_market": target_market,
            "department_id": department_id,
            "compose_aspect_ratio": "vertical",
            "subtitle_template_id": "property_clear",
            "opennews": True,
            "opennews_material_only": True,
            "opennews_presenter": presenter_for_market,
        },
        "translation_usage": localized_meta.get("usage", {}),
        "script_generation_mode": localized_meta.get("generation_mode") or "timeline_translation",
        "language_label": market.get("content_language") or target_market,
        "audio_ready": False,
    }


def _build_opennews_language_version(
    *,
    output_dir: str,
    source_topic: str,
    source_script: dict,
    source_segments: list[dict],
    primary_workflow_config: dict,
    target_market: str,
    department_id: str,
    provider: str,
    user: Optional[dict],
    compose_videos: bool = True,
) -> dict:
    from generate_audio import generate_audio
    from tos_uploader import upload_file_and_get_url

    output_path = Path(output_dir)
    market = _get_target_market(target_market)
    voice_preset = _get_voice_preset(market.get("default_voice_preset_id"), target_market)
    tts_voice = voice_preset.get("voice_id")
    if not tts_voice:
        raise RuntimeError(f"{market.get('name') or target_market} 缺少可用配音方案")
    tts_speed = float(voice_preset.get("default_speed") or 1.05)
    tts_volume = float(voice_preset.get("default_volume") or 1.0)
    translated_script, localized_meta = _prepare_opennews_language_script(
        source_topic=source_topic,
        source_script=source_script,
        primary_workflow_config=primary_workflow_config,
        target_market=target_market,
        department_id=department_id,
        provider=provider,
    )
    primary_presenter = _normalize_opennews_presenter_config((primary_workflow_config or {}).get("opennews_presenter"))
    presenter_for_market = _opennews_presenter_config_for_market(
        target_market=target_market,
        gender=str(primary_presenter.get("gender") or "female"),
    )

    translated_segments: list[dict] = []
    for index, translated_seg in enumerate(translated_script.get("segments", []) or []):
        base_seg = source_segments[index] if index < len(source_segments) else {}
        seg_copy = copy.deepcopy(base_seg if isinstance(base_seg, dict) else {})
        seg_copy.update(
            {
                "type": translated_seg.get("type", seg_copy.get("type")),
                "start": translated_seg.get("start", seg_copy.get("start")),
                "end": translated_seg.get("end", seg_copy.get("end")),
                "duration": translated_seg.get("duration", seg_copy.get("duration")),
                "script": translated_seg.get("script", seg_copy.get("script", "")),
                "target_market": target_market,
                "department_id": department_id,
            }
        )
        if seg_copy.get("type") == "digital_human":
            seg_copy["action"] = translated_seg.get("action", seg_copy.get("action", ""))
        else:
            seg_copy["material_keyword"] = translated_seg.get("material_keyword", seg_copy.get("material_keyword", ""))
            seg_copy["material_search_keyword"] = translated_seg.get("material_search_keyword", seg_copy.get("material_search_keyword", ""))
            seg_copy["material_desc"] = translated_seg.get("material_desc", seg_copy.get("material_desc", ""))

        audio_dir = output_path / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        seg_type = str(seg_copy.get("type") or "segment")
        audio_path = audio_dir / f"segment_{index:02d}_{seg_type}_{target_market}.mp3"
        audio_path_str, tts_provider = _generate_audio_for_workflow(
            script_text=str(seg_copy.get("script") or "").strip(),
            audio_path=str(audio_path),
            voice=tts_voice,
            speed=tts_speed,
            volume=tts_volume,
            language=voice_preset.get("language", ""),
            workflow_config={
                **(primary_workflow_config or {}),
                "target_market": target_market,
                "opennews_presenter": presenter_for_market,
                "voice_preset": {
                    "id": voice_preset.get("id"),
                    "name": voice_preset.get("name"),
                    "subtitle": voice_preset.get("subtitle"),
                    "selected_speed": tts_speed,
                    "selected_volume": tts_volume,
                    "language": market.get("content_language", ""),
                },
                "source_task_id": str((primary_workflow_config or {}).get("task_id") or ""),
            },
            generate_audio_fn=generate_audio,
            task_id=str((primary_workflow_config or {}).get("task_id") or ""),
        )
        seg_copy["audio_path"] = audio_path_str
        try:
            seg_copy["audio_url"] = upload_file_and_get_url(audio_path_str, key_prefix="full/audio")
        except Exception:
            seg_copy["audio_url"] = seg_copy.get("audio_url", "")
        seg_copy["tts_provider"] = tts_provider
        translated_segments.append(seg_copy)
        _record_history_cost(
            output_dir=output_path,
            result={"topic": source_topic, "cost_entries": [], "cost_summary": _empty_cost_summary()},
            user=user,
            event_type="tts_generate",
            amount=_estimate_tts_cost(str(seg_copy.get("script") or ""), audio_path_str),
            provider=tts_provider,
            topic=source_topic,
            meta={"segment_index": index + 1, "audio_path": audio_path_str, "scope": f"opennews_translate_{target_market}"},
        )

    compose_input = {
        "topic": source_topic,
        "title": translated_script.get("title") or "",
        "cover_title": translated_script.get("cover_title") or "",
        "total_duration": translated_script.get("total_duration") or source_script.get("total_duration") or 0,
        "segment_count": len(translated_segments),
        "script": translated_script,
        "segments": translated_segments,
        "social_post": translated_script.get("social_post") or "",
        "workflow_config": {
            **(primary_workflow_config or {}),
            "voice_preset": {
                "id": voice_preset.get("id"),
                "name": voice_preset.get("name"),
                "subtitle": voice_preset.get("subtitle"),
                "selected_speed": tts_speed,
                "selected_volume": tts_volume,
                "language": market.get("content_language", ""),
            },
            "target_market": target_market,
            "department_id": department_id,
            "compose_aspect_ratio": "vertical",
            "subtitle_template_id": "property_clear",
            "opennews": True,
            "opennews_material_only": True,
            "opennews_presenter": presenter_for_market,
        },
        "cost_entries": [],
        "cost_summary": _empty_cost_summary(),
    }
    if not compose_videos:
        compose_input.update(
            {
                "target_market": target_market,
                "translation_usage": localized_meta.get("usage", {}),
                "script_generation_mode": localized_meta.get("generation_mode") or "market_native",
                "audio_ready": True,
            }
        )
        return compose_input

    from video_composer import compose_history_video
    variant_results: dict[str, dict] = {}
    for variant_aspect in ("horizontal", "vertical"):
        variant_results[variant_aspect] = compose_history_video(
            output_dir,
            compose_input,
            transition_id=str((primary_workflow_config or {}).get("compose_transition_id") or "fade"),
            subtitle_template_id="property_clear",
            aspect_ratio=variant_aspect,
            output_stem=f"final_video_{target_market}_{variant_aspect}",
        )
    composed = dict(variant_results.get("vertical") or variant_results.get("horizontal") or {})
    composed["final_video_variants"] = variant_results
    composed.update(
        {
            "target_market": target_market,
            "title": compose_input.get("title"),
            "cover_title": compose_input.get("cover_title"),
            "social_post": compose_input.get("social_post"),
            "workflow_config": compose_input.get("workflow_config"),
            "script": translated_script,
            "segments": translated_segments,
            "translation_usage": localized_meta.get("usage", {}),
            "script_generation_mode": localized_meta.get("generation_mode") or "market_native",
            "audio_ready": True,
        }
    )
    return composed


def _compose_opennews_language_versions(
    *,
    output_path: Path,
    result: dict,
    user: Optional[dict],
    source_topic: str,
    department_id: str,
    provider: str,
) -> None:
    language_versions = result.get("language_versions")
    if not isinstance(language_versions, list):
        language_versions = []
    if _opennews_multilingual_enabled():
        workflow_config = result.get("workflow_config") or {}
        primary_market = str(workflow_config.get("target_market") or "cn").strip() or "cn"
        configured_markets = workflow_config.get("opennews_language_markets")
        if configured_markets is not None:
            extra_market_ids = _normalize_opennews_extra_target_markets(configured_markets, primary_market)
        else:
            extra_market_ids = _opennews_extra_target_markets_for_primary(primary_market)

        existing_by_market: dict[str, dict] = {}
        extra_versions: list[dict] = []
        for item in language_versions:
            if not isinstance(item, dict):
                continue
            item_market = str(item.get("target_market") or ((item.get("workflow_config") or {}).get("target_market")) or "").strip()
            if item_market in extra_market_ids and item_market not in existing_by_market:
                existing_by_market[item_market] = item
            else:
                extra_versions.append(item)

        refreshed_versions: list[dict] = []
        legacy_generation_modes = {"market_native", "market_native_fallback"}
        for extra_market_id in extra_market_ids:
            existing_item = existing_by_market.get(extra_market_id)
            generation_mode = str((existing_item or {}).get("script_generation_mode") or "").strip()
            audio_ready = bool((existing_item or {}).get("audio_ready"))
            should_rebuild = (
                not isinstance(existing_item, dict)
                or bool(existing_item.get("error"))
                or not audio_ready
                or _opennews_target_language_mismatch(existing_item, extra_market_id)
                or generation_mode in legacy_generation_modes
                or not generation_mode
            )
            if should_rebuild:
                try:
                    refreshed_versions.append(
                        _build_opennews_language_version(
                            output_dir=str(output_path),
                            source_topic=source_topic,
                            source_script=result.get("script") or {},
                            source_segments=result.get("segments") or [],
                            primary_workflow_config=workflow_config,
                            target_market=extra_market_id,
                            department_id=department_id,
                            provider=provider,
                            user=user,
                            compose_videos=False,
                        )
                    )
                except Exception as exc:
                    refreshed_versions.append({"target_market": extra_market_id, "error": str(exc)})
            else:
                refreshed_versions.append(existing_item)

        language_versions = refreshed_versions + extra_versions
        if language_versions:
            result["language_version_group_id"] = result.get("language_version_group_id") or _language_version_group_id()
            result["language_versions"] = language_versions

    from video_composer import compose_history_video

    for item in language_versions:
        if not isinstance(item, dict) or item.get("error"):
            continue
        target_market = str(item.get("target_market") or ((item.get("workflow_config") or {}).get("target_market")) or "").strip()
        if not target_market:
            continue
        if isinstance(item.get("final_video_variants"), dict) and item.get("final_video_path"):
            continue
        variant_results: dict[str, dict] = {}
        for variant_aspect in ("horizontal", "vertical"):
            variant_results[variant_aspect] = compose_history_video(
                str(output_path),
                item,
                transition_id=str((result.get("workflow_config") or {}).get("compose_transition_id") or "fade"),
                subtitle_template_id="property_clear",
                aspect_ratio=variant_aspect,
                output_stem=f"final_video_{target_market}_{variant_aspect}",
            )
        composed = dict(variant_results.get("vertical") or variant_results.get("horizontal") or {})
        item.update(composed)
        item["final_video_variants"] = variant_results


def _build_opennews_manual_review_result_payload(job: dict, item: dict) -> dict:
    review_result = item.get("review_result") or {}
    payload = dict(review_result) if isinstance(review_result, dict) else {}
    if not payload:
        history_id = str(item.get("review_history_id") or "").strip()
        output_dir = _resolve_history_output_dir(history_id)
        result = _load_result_from_output_dir(output_dir) if output_dir else None
        if output_dir and result:
            payload = _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))
    if payload and not payload.get("history_id"):
        payload["history_id"] = str(item.get("review_history_id") or payload.get("history_id") or "")
    return payload


def _normal_title_key(title: Any) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip().lower())


def _opennews_article_title(article: dict) -> str:
    return str(article.get("title_zh") or article.get("translated_title") or article.get("title") or "OpenNews 新闻").strip()


def _opennews_result_has_material_assets(result: dict, output_path: Path) -> bool:
    material_segment_count = 0
    usable_asset_count = 0
    for segment in result.get("segments") or []:
        if not isinstance(segment, dict):
            continue
        segment_items = segment.get("material_items") or []
        segment_paths = segment.get("material_paths") or []
        if str(segment.get("type") or "") != "material" and not segment_items and not segment_paths:
            continue
        material_segment_count += 1
        segment_has_asset = False
        for item in segment_items:
            if not isinstance(item, dict):
                continue
            raw_path = str(item.get("path") or "").strip()
            if not raw_path:
                continue
            path = Path(raw_path)
            if not path.is_absolute():
                path = output_path / raw_path
            if path.exists() and path.stat().st_size > 0:
                segment_has_asset = True
                usable_asset_count += 1
                break
        if segment_has_asset:
            continue
        for raw_path in segment_paths:
            path = Path(str(raw_path))
            if not path.is_absolute():
                path = output_path / str(raw_path)
            if path.exists() and path.stat().st_size > 0:
                segment_has_asset = True
                usable_asset_count += 1
                break
    return material_segment_count > 0 and usable_asset_count > 0


def _ensure_opennews_fallback_material_assets(result: dict, output_path: Path) -> bool:
    if _opennews_result_has_material_assets(result, output_path):
        return False
    if not _history_is_opennews_result(result):
        return False
    try:
        from fetch_materials import (
            _opennews_generate_verified_news_card,
            _opennews_relevance_tokens,
            _opennews_visual_domain,
        )
    except Exception as exc:
        print(f"[opennews material fallback] helper import failed: {exc!r}")
        return False

    changed = False
    segments = result.get("segments")
    if not isinstance(segments, list):
        return False
    for segment_index, segment in enumerate(segments):
        if not isinstance(segment, dict):
            continue
        if str(segment.get("type") or "") != "material" and not (
            segment.get("opennews_material_only") or segment.get("disable_free_material_fallback")
        ):
            continue
        if _opennews_result_has_material_assets({"segments": [segment]}, output_path):
            continue
        existing_materials_dir = output_path / "materials"
        existing_material_paths: list[str] = []
        if existing_materials_dir.exists():
            for path in sorted(existing_materials_dir.glob(f"material_{segment_index:02d}_*")):
                if not path.is_file() or path.stat().st_size <= 0:
                    continue
                if path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v"}:
                    continue
                existing_material_paths.append(str(path))
        if existing_material_paths:
            segment["material_paths"] = existing_material_paths
            segment["material_items"] = [
                {
                    "path": path,
                    "kind": "video" if Path(path).suffix.lower() in {".mp4", ".mov", ".m4v"} else "image",
                    "source": "existing_opennews_material",
                    "title": str(segment.get("material_keyword") or segment.get("title_zh") or segment.get("title") or "OpenNews 新闻"),
                    "match_reason": "compose_time_existing_material_recovery",
                }
                for path in existing_material_paths
            ]
            segment["material_quality"] = {
                "strategy": "compose_time_existing_material_recovery",
                "auto_publish_allowed": True,
                "requires_human_review": False,
                "existing_material_count": len(existing_material_paths),
                "source_counts": {"existing_opennews_material": len(existing_material_paths)},
            }
            changed = True
            continue
        try:
            relevance_tokens = _opennews_relevance_tokens(segment)
            visual_domain = _opennews_visual_domain(segment, relevance_tokens) if relevance_tokens else "general"
        except Exception:
            visual_domain = "general"
        material_paths: list[str] = []
        material_items: list[dict] = []
        for card_index in range(3):
            try:
                card_path = _opennews_generate_verified_news_card(
                    segment,
                    str(output_path),
                    segment_index,
                    card_index,
                    visual_domain=visual_domain,
                )
            except Exception as exc:
                print(f"[opennews material fallback] news card generation failed: {exc!r}")
                continue
            material_paths.append(card_path)
            material_items.append(
                {
                    "path": card_path,
                    "kind": "image",
                    "source": "generated_news_card",
                    "title": str(segment.get("material_keyword") or segment.get("title_zh") or segment.get("title") or "OpenNews 新闻"),
                    "match_reason": "compose_time_news_card_fallback",
                }
            )
        if material_paths:
            segment["material_paths"] = material_paths
            segment["material_items"] = material_items
            segment["material_quality"] = {
                "strategy": "compose_time_news_card_fallback",
                "auto_publish_allowed": True,
                "requires_human_review": False,
                "generated_news_card_count": len(material_items),
                "source_counts": {"generated_news_card": len(material_items)},
            }
            changed = True
    return changed


def _compose_opennews_result(
    output_path: Path,
    result: dict,
    *,
    preferred_aspect_ratio: str = "vertical",
    user: Optional[dict] = None,
    cost_scope: str = "localtok_publish",
) -> dict:
    if not output_path.exists():
        raise RuntimeError("OpenNews 输出目录不存在。")
    if not _opennews_result_has_material_assets(result, output_path):
        _ensure_opennews_fallback_material_assets(result, output_path)
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
    result.pop("error", None)
    _compose_opennews_language_versions(
        output_path=output_path,
        result=result,
        user=user,
        source_topic=str(result.get("topic") or ""),
        department_id=str(workflow_config.get("department_id") or "real_estate"),
        provider=str(workflow_config.get("script_model") or SCRIPT_MODEL_CLAUDE),
    )
    _record_history_cost(
        output_dir=output_path,
        result=result,
        user=user,
        event_type="compose_video",
        amount=_estimate_compose_cost(result.get("total_duration", 0)),
        provider=COST_RULES["compose_video"]["provider"],
        topic=result.get("topic", ""),
        meta={
            "transition_id": transition_id,
            "subtitle_template_id": subtitle_template_id,
            "aspect_ratio": aspect_ratio,
            "generated_aspect_ratios": ["horizontal", "vertical"],
            "scope": cost_scope,
        },
    )
    return result


def _compose_opennews_task_video(task_id: str, *, preferred_aspect_ratio: str = "vertical") -> dict:
    task = tasks.get(task_id) or {}
    output_dir = task.get("output_dir")
    result = task.get("result")
    if not output_dir or not result:
        raise RuntimeError("OpenNews 任务尚未生成可合成的中间结果。")
    output_path = Path(output_dir)
    try:
        result = _compose_opennews_result(
            output_path,
            result,
            preferred_aspect_ratio=preferred_aspect_ratio,
            user=None,
            cost_scope="localtok_publish",
        )
    except Exception as exc:
        if isinstance(result, dict):
            result["error"] = str(exc)
            try:
                result["material_review"] = _opennews_material_review_status(result, output_path)
            except Exception:
                pass
            task["result"] = result
            _persist_task_result(task)
            _sync_live_task_result(str(output_path), result)
        raise
    task["result"] = result
    _persist_task_result(task)
    _sync_live_task_result(str(output_path), result)
    return result


def _auto_publish_opennews_result_data(
    output_dir: Path,
    result: dict,
    *,
    workflow_config_override: Optional[dict] = None,
) -> dict:
    workflow_config = workflow_config_override or result.get("workflow_config") or {}
    material_review = _opennews_material_review_status(result, output_dir)
    if material_review.get("uses_strict_source_fallback"):
        result["material_review"] = material_review
    x_records: list[dict] = []
    facebook_records: list[dict] = []
    youtube_records: list[dict] = []
    x_error = ""
    facebook_error = ""
    youtube_error = ""
    x_auto_publish = _parse_bool_form(workflow_config.get("x_auto_publish")) if "x_auto_publish" in workflow_config else _opennews_x_auto_publish_default()
    facebook_auto_publish = _parse_bool_form(workflow_config.get("facebook_auto_publish")) if "facebook_auto_publish" in workflow_config else _opennews_facebook_auto_publish_default()
    youtube_auto_publish = _parse_bool_form(workflow_config.get("youtube_auto_publish")) if "youtube_auto_publish" in workflow_config else _opennews_youtube_auto_publish_default()
    # 未归属任何配置频道的内容(channel_id=general,多为旧积压/恢复重合成)一律不自动发布,
    # 避免它走全局 token 发到别的频道(如把 general 里的房产/科技混合内容误发到某个频道)。
    _pub_cid = _opennews_result_channel_id(result)
    if _pub_cid == "general" or not _find_opennews_channel(_pub_cid):
        if x_auto_publish or facebook_auto_publish or youtube_auto_publish:
            print(f"[publish guard] 跳过未归属频道(channel={_pub_cid})的自动发布 dir={output_dir.name}", flush=True)
        x_auto_publish = facebook_auto_publish = youtube_auto_publish = False
    if _opennews_x_auto_publish_disabled():
        x_auto_publish = False
    if _opennews_facebook_auto_publish_disabled():
        facebook_auto_publish = False
    if _opennews_youtube_auto_publish_disabled():
        youtube_auto_publish = False
    # 幂等防重:重新读盘上的已发记录并合并,某平台已发过就不再重发。
    # 修复恢复worker重合成丢记录、或多个触发点并发导致同一条视频被多次上传的问题。
    _disk_result = _load_result_from_output_dir(output_dir) or {}
    for _rk in (
        "youtube_publish_records", "youtube_publish_latest",
        "facebook_publish_records", "facebook_publish_latest",
        "x_publish_records", "x_publish_latest",
    ):
        if _disk_result.get(_rk) and not result.get(_rk):
            result[_rk] = _disk_result[_rk]
    if result.get("youtube_publish_records"):
        youtube_auto_publish = False
    if result.get("facebook_publish_records"):
        facebook_auto_publish = False
    if result.get("x_publish_records"):
        x_auto_publish = False
    # 跨目录防重复发布:同一条新闻(按事件指纹 或 源文章URL)若已由别的成片发到本频道,则本条不再重发。
    _pub_channel_id = _opennews_result_channel_id(result)
    _this_event_key = _opennews_event_identity_dedupe_key(_opennews_item_event_identity(result))
    _this_url = _opennews_result_source_url(result)
    _this_url_key = ("url:" + _this_url) if _this_url else ""

    def _already_pub(platform: str) -> bool:
        published = _opennews_channel_published_event_keys(_pub_channel_id, platform=platform, exclude_dir=output_dir.name)
        return (bool(_this_event_key) and _this_event_key in published) or (bool(_this_url_key) and _this_url_key in published)

    if youtube_auto_publish and _already_pub("youtube"):
        youtube_auto_publish = False
        result["youtube_auto_publish_error"] = "同一新闻已在本频道发布过，跳过重复发布。"
        print(f"[dedup publish] 跳过 YouTube 重复发布 dir={output_dir.name} channel={_pub_channel_id}", flush=True)
    if facebook_auto_publish and _already_pub("facebook"):
        facebook_auto_publish = False
    youtube_aspects_raw = workflow_config.get("youtube_aspects") or ["vertical"]
    if isinstance(youtube_aspects_raw, str):
        youtube_aspects = ["horizontal", "vertical"] if youtube_aspects_raw == "both" else [part.strip() for part in youtube_aspects_raw.split(",") if part.strip()]
    else:
        youtube_aspects = [str(part).strip() for part in (youtube_aspects_raw or []) if str(part).strip()] or ["vertical"]
    x_aspects_raw = workflow_config.get("x_aspects") or ["vertical"]
    if isinstance(x_aspects_raw, str):
        x_aspects = ["horizontal", "vertical"] if x_aspects_raw == "both" else [part.strip() for part in x_aspects_raw.split(",") if part.strip()]
    else:
        x_aspects = [str(part).strip() for part in (x_aspects_raw or []) if str(part).strip()] or ["vertical"]
    facebook_aspects_raw = workflow_config.get("facebook_aspects") or ["vertical"]
    if isinstance(facebook_aspects_raw, str):
        facebook_aspects = ["horizontal", "vertical"] if facebook_aspects_raw == "both" else [part.strip() for part in facebook_aspects_raw.split(",") if part.strip()]
    else:
        facebook_aspects = [str(part).strip() for part in (facebook_aspects_raw or []) if str(part).strip()] or ["vertical"]
    if x_auto_publish:
        try:
            x_records = _publish_opennews_result_to_x(
                output_dir,
                result,
                aspects=x_aspects,
                include_language_versions=_opennews_x_publish_language_versions_enabled(),
            )
        except Exception as exc:
            x_error = str(exc)
    if facebook_auto_publish:
        try:
            facebook_records = _publish_opennews_result_to_facebook(
                output_dir,
                result,
                aspects=facebook_aspects,
                include_language_versions=_opennews_facebook_publish_language_versions_enabled(),
            )
        except Exception as exc:
            facebook_error = str(exc)
    if youtube_auto_publish:
        try:
            youtube_records = _publish_opennews_result_to_youtube(
                output_dir,
                result,
                aspects=youtube_aspects,
                privacy_status="public",
                include_language_versions=_opennews_youtube_publish_language_versions_enabled(),
            )
        except Exception as exc:
            youtube_error = str(exc)
    if x_records:
        result["x_publish_records"] = x_records + list(result.get("x_publish_records") or [])[len(x_records):]
        result["x_publish_latest"] = x_records[0]
        result.pop("x_auto_publish_error", None)
        result.pop("x_publish_error", None)
    elif x_error:
        result["x_auto_publish_error"] = x_error
        result["x_publish_error"] = x_error
    if facebook_records:
        result["facebook_publish_records"] = facebook_records + list(result.get("facebook_publish_records") or [])[len(facebook_records):]
        result["facebook_publish_latest"] = facebook_records[0]
        result.pop("facebook_auto_publish_error", None)
        result.pop("facebook_publish_error", None)
    elif facebook_error:
        result["facebook_auto_publish_error"] = facebook_error
        result["facebook_publish_error"] = facebook_error
    if youtube_records:
        result["youtube_publish_records"] = youtube_records + list(result.get("youtube_publish_records") or [])[len(youtube_records):]
        result["youtube_publish_latest"] = youtube_records[0]
        result.pop("youtube_auto_publish_error", None)
        result.pop("youtube_publish_error", None)
    elif youtube_error:
        result["youtube_auto_publish_error"] = youtube_error
        result["youtube_publish_error"] = youtube_error
    _save_result_to_output_dir(output_dir, result)
    return {
        "x_records": x_records,
        "facebook_records": facebook_records,
        "youtube_records": youtube_records,
        "x_error": x_error,
        "facebook_error": facebook_error,
        "youtube_error": youtube_error,
        "material_review": material_review,
    }


def _auto_publish_opennews_task_result(task_id: str) -> dict:
    task = tasks.get(task_id) or {}
    result = task.get("result") or {}
    output_dir_value = str(task.get("output_dir") or "").strip()
    if not output_dir_value or not isinstance(result, dict):
        return {"x_records": [], "facebook_records": [], "x_error": "missing_result", "facebook_error": "missing_result"}
    output_dir = Path(output_dir_value)
    publish_result = _auto_publish_opennews_result_data(
        output_dir,
        result,
        workflow_config_override=result.get("workflow_config") or task.get("workflow_config") or {},
    )
    task["result"] = result
    _persist_task_result(task)
    _sync_live_task_result(str(output_dir), result)
    return publish_result


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
            "auto_publish_to_x_and_facebook": True,
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
            "auto_publish_to_x_and_facebook": True,
            "x_auto_publish": _opennews_x_auto_publish_default(),
            "facebook_auto_publish": _opennews_facebook_auto_publish_default(),
            "x_aspects": ["vertical"],
            "facebook_aspects": ["vertical"],
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


@app.post("/api/admin/avatar-lab/generate")
async def admin_generate_avatar(request: Request, reference_image: UploadFile = File(...), avatar_name: str = Form("新主播"), gender: str = Form("female"), target_markets: str = Form("cn,tw,jp"), style_note: str = Form(""), candidate_count: int = Form(3)):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


@app.get("/api/admin/avatar-lab/jobs/latest")
async def admin_avatar_latest_job_status(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


@app.get("/api/admin/avatar-lab/jobs/{job_id}")
async def admin_avatar_job_status(job_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


@app.get("/api/admin/avatar-lab/jobs/{job_id}/download/{file_path:path}")
async def admin_avatar_job_download(job_id: str, file_path: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


@app.post("/api/admin/avatar-lab/jobs/{job_id}/import")
async def admin_avatar_job_import(job_id: str, request: Request, filename: str = Form(...), display_name: str = Form(""), gender: str = Form("female"), target_markets: str = Form("cn,tw,jp"), style_note: str = Form("")):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


@app.delete("/api/admin/avatar-lab/jobs/{job_id}/candidates/{filename}")
async def admin_avatar_job_delete_candidate(job_id: str, filename: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error()
    return JSONResponse({"error": "主播图实验室已停用"}, status_code=410)


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
        "can_use_auto_digital_batch": _is_admin(user),
        "auto_digital_batch": (
            _auto_digital_batch_payload(_find_running_auto_digital_batch_for_user(user))
            if _is_admin(user)
            else None
        ),
    }


@app.get("/api/property-video/bgm")
async def property_video_bgm_tracks(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {"ok": True, "tracks": _property_bgm_track_payloads()}


@app.post("/api/property-video/bgm/upload")
async def upload_property_video_bgm_tracks(
    request: Request,
    files: list[UploadFile] = File(...),
    notes: str = Form(""),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以管理房源实拍 BGM")
    upload_dir = OUTPUT_DIR / "property_bgm_uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    created = []
    for index, upload in enumerate(files or [], start=1):
        original_name = Path(upload.filename or "").name
        suffix = Path(original_name).suffix.lower()
        if suffix not in AUDIO_SUFFIXES:
            return JSONResponse({"error": "BGM 只支持 mp3、wav、m4a、aac、ogg 音频"}, status_code=400)
        temp_path = upload_dir / f"{uuid.uuid4().hex[:12]}_{index}{suffix}"
        with temp_path.open("wb") as f:
            shutil.copyfileobj(upload.file, f)
        try:
            item = register_material_file(
                temp_path=str(temp_path),
                original_filename=original_name,
                title=Path(original_name).stem,
                category="背景音乐",
                notes=notes,
                uploader_username=user.get("username", ""),
                uploader_display_name=user.get("display_name", ""),
                source="property_bgm_upload",
                safety_status="safe",
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
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        created.append(_material_library_item_payload(updated, user))
    return {"ok": True, "items": created, "tracks": _property_bgm_track_payloads()}


@app.delete("/api/property-video/bgm/{item_id}")
async def delete_property_video_bgm_track(item_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以管理房源实拍 BGM")
    try:
        existing = next((item for item in list_material_library_items() if str(item.get("id")) == str(item_id)), None)
        if not existing:
            return JSONResponse({"error": "BGM 不存在"}, status_code=404)
        if str(existing.get("kind") or "") != "audio":
            return JSONResponse({"error": "只能删除 BGM 音频"}, status_code=400)
        deleted = delete_material_library_item(item_id)
    except FileNotFoundError:
        return JSONResponse({"error": "BGM 不存在"}, status_code=404)
    return {
        "ok": True,
        "deleted": _material_library_item_payload(deleted, user),
        "tracks": _property_bgm_track_payloads(),
    }


def _legacy_material_library_disabled_response() -> JSONResponse:
    return JSONResponse(
        {
            "error": "素材库页面已精简下线。房源实拍 BGM 请使用 /api/property-video/bgm；OpenNews 和普通数字人只走免费素材 API。",
            "disabled": True,
        },
        status_code=410,
    )


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
    return _legacy_material_library_disabled_response()


@app.post("/api/material-library/upload")
async def upload_material_library_items(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _legacy_material_library_disabled_response()


@app.post("/api/material-library/{item_id}/review")
async def review_material_library_item(item_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以审核素材")
    return _legacy_material_library_disabled_response()


@app.post("/api/material-library/review-batch")
async def review_material_library_items_batch(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以批量处理素材")
    return _legacy_material_library_disabled_response()


@app.get("/api/material-library/harvest")
async def material_harvest_payload(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return {
        "jobs": [],
        "candidates": [],
        "presets": [],
        "topic_presets": [],
        "disabled": True,
        "message": "素材采集候选池已停用。OpenNews 和普通数字人只走免费素材 API；房源实拍 BGM 保留素材库音频。",
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
    return JSONResponse({"error": "素材采集候选池已停用。"}, status_code=410)


@app.post("/api/material-library/harvest/hotspots")
async def create_material_hotspot_harvest_jobs(
    request: Request,
    days: int = Form(7),
    max_topics: int = Form(10),
    per_topic: int = Form(8),
):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以发起热点补库")
    return JSONResponse({"error": "素材热点补库已停用。"}, status_code=410)


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
    return JSONResponse({"error": "素材采集候选池已停用。"}, status_code=410)


@app.post("/api/material-library/harvest/candidates/{candidate_id}/reject")
async def reject_material_harvest_candidate(candidate_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以拒绝候选素材")
    return JSONResponse({"error": "素材采集候选池已停用。"}, status_code=410)


@app.delete("/api/material-library/harvest/candidates")
async def clear_material_harvest_candidates(request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以清空候选素材")
    return {"ok": True, "removed_count": 0, "disabled": True}


@app.delete("/api/material-library/harvest/candidates/{candidate_id}")
async def delete_material_harvest_candidate(candidate_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    if not _is_admin(user):
        return _forbidden_error("只有管理员可以删除候选素材")
    return JSONResponse({"error": "素材采集候选池已停用。"}, status_code=410)


@app.delete("/api/material-library/{item_id}")
async def delete_material_library_item_endpoint(item_id: str, request: Request):
    user, error = _require_user(request)
    if error:
        return error
    return _legacy_material_library_disabled_response()


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


@app.post("/api/auto-digital/topics")
async def auto_digital_generate_topics(
    request: Request,
    seed_topic: str = Form(""),
    count: int = Form(10),
):
    user, error = _require_admin_user(request, "只有管理员可以使用批量数字人自动生成")
    if error:
        return error
    seed = str(seed_topic or "").strip()
    if not seed:
        return JSONResponse({"error": "请先输入一个批量主题方向"}, status_code=400)
    from generate_script import generate_topic_ideas

    try:
        payload = _run_script_ai_job(
            job_id=f"auto-digital-topics:{user.get('username', 'guest')}:{time.time_ns()}",
            label="批量选题生成",
            runner=lambda: generate_topic_ideas(
                seed,
                count=max(1, min(int(count or 10), 10)),
                target_market="cn",
                department_id="real_estate",
                provider=SCRIPT_MODEL_LOCAL_QWEN,
            ),
        )
    except Exception as exc:
        message, status_code = _friendly_ai_error_message(exc, "批量选题生成")
        return JSONResponse({"error": f"{message}：{exc}"}, status_code=status_code)
    return {
        "ok": True,
        "seed_topic": payload.get("seed_topic") or seed,
        "topics": payload.get("topics") or [],
    }


@app.post("/api/auto-digital/batches")
async def auto_digital_create_batch(
    request: Request,
    seed_topic: str = Form(""),
    topics_json: str = Form(""),
):
    user, error = _require_admin_user(request, "只有管理员可以创建批量数字人任务")
    if error:
        return error
    running_job = _find_running_auto_digital_batch_for_user(user)
    if running_job:
        return {
            "ok": True,
            "reused_existing": True,
            "batch": _auto_digital_batch_payload(running_job),
            "message": "当前账号已有批量数字人任务在运行",
        }
    seed = str(seed_topic or "").strip()
    try:
        raw_topics = json.loads(topics_json or "[]")
    except json.JSONDecodeError:
        return JSONResponse({"error": "选题数据格式错误"}, status_code=400)
    if not isinstance(raw_topics, list):
        return JSONResponse({"error": "选题数据必须是数组"}, status_code=400)
    if not raw_topics:
        if not seed:
            return JSONResponse({"error": "请先输入一个批量主题方向"}, status_code=400)
        from generate_script import generate_topic_ideas

        try:
            generated_payload = _run_script_ai_job(
                job_id=f"auto-digital-batch-topics:{user.get('username', 'guest')}:{time.time_ns()}",
                label="批量选题生成",
                runner=lambda: generate_topic_ideas(
                    seed,
                    count=10,
                    target_market="cn",
                    department_id="real_estate",
                    provider=SCRIPT_MODEL_LOCAL_QWEN,
                ),
            )
        except Exception as exc:
            message, status_code = _friendly_ai_error_message(exc, "批量选题生成")
            return JSONResponse({"error": f"{message}：{exc}"}, status_code=status_code)
        raw_topics = generated_payload.get("topics") or []
    items: list[dict] = []
    seen: set[str] = set()
    for raw in raw_topics:
        if isinstance(raw, str):
            topic = raw.strip()
            angle = ""
        elif isinstance(raw, dict):
            topic = str(raw.get("topic") or raw.get("title") or "").strip()
            angle = str(raw.get("angle") or "").strip()
        else:
            continue
        topic = re.sub(r"\s+", " ", topic).strip()
        if not topic:
            continue
        key = re.sub(r"\s+", "", topic).lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "index": len(items) + 1,
                "topic": topic[:100],
                "angle": angle[:160],
                "status": "queued",
                "task_id": "",
                "history_id": "",
                "error": "",
                "created_at": time.time(),
                "updated_at": time.time(),
            }
        )
        if len(items) >= 10:
            break
    if not items:
        return JSONResponse({"error": "请至少保留 1 条有效选题"}, status_code=400)
    voice_preset = _get_voice_preset("mandarin_female", "cn")
    avatar_option = _get_avatar_option("avatar_host_d.png", target_market_id="cn")
    if not avatar_option:
        return JSONResponse({"error": "默认女主播C不存在，请先恢复 avatar_host_d.png"}, status_code=400)
    batch_id = f"adb_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    public_base_url = _get_public_base_url(request)
    job = {
        "batch_id": batch_id,
        "seed_topic": seed,
        "status": "queued",
        "message": "批量任务已创建，等待启动",
        "created_at": time.time(),
        "updated_at": time.time(),
        "owner_username": user.get("username"),
        "owner_display_name": user.get("display_name"),
        "owner_role": user.get("role"),
        "target_market": "cn",
        "department_id": "real_estate",
        "voice_preset_id": voice_preset.get("id"),
        "avatar_id": avatar_option.get("id"),
        "speed": float(voice_preset.get("default_speed") or 1.1),
        "script_model": SCRIPT_MODEL_LOCAL_QWEN,
        "digital_human_engine": INFINITETALK_ENGINE_ID,
        "request_context": {"public_base_url": public_base_url},
        "items": items,
    }
    _save_auto_digital_batch_job(job)
    threading.Thread(target=_run_auto_digital_batch, args=(batch_id,), daemon=True).start()
    return {"ok": True, "reused_existing": False, "batch": _auto_digital_batch_payload(job)}


@app.get("/api/auto-digital/batches")
async def auto_digital_batches(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以查看批量数字人任务")
    if error:
        return error
    return {"items": [_auto_digital_batch_payload(job) for job in _list_auto_digital_batch_jobs_for_user(user, limit=12)]}


@app.get("/api/auto-digital/batches/{batch_id}")
async def auto_digital_batch_status(batch_id: str, request: Request):
    user, error = _require_admin_user(request, "只有管理员可以查看批量数字人任务")
    if error:
        return error
    job = _load_auto_digital_batch_job(batch_id)
    if not job:
        return JSONResponse({"error": "批量任务不存在"}, status_code=404)
    if not _is_admin(user) and str(job.get("owner_username") or "") != str(user.get("username") or ""):
        return _forbidden_error()
    return {"batch": _auto_digital_batch_payload(job)}


@app.post("/api/auto-digital/batches/{batch_id}/cancel")
async def auto_digital_cancel_batch(batch_id: str, request: Request):
    user, error = _require_admin_user(request, "只有管理员可以停止批量数字人任务")
    if error:
        return error
    job = _load_auto_digital_batch_job(batch_id)
    if not job:
        return JSONResponse({"error": "批量任务不存在"}, status_code=404)
    if not _is_admin(user) and str(job.get("owner_username") or "") != str(user.get("username") or ""):
        return _forbidden_error()
    job["status"] = "cancelled"
    job["message"] = "已请求停止批量任务"
    job["updated_at"] = time.time()
    for item in job.get("items") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") == "running" and item.get("task_id"):
            task = tasks.get(str(item.get("task_id")))
            if task and _user_can_access_task(user, task):
                task["cancel_requested"] = True
                task["cancel_requested_at"] = time.time()
                _cancel_waiting_omnihuman_jobs(str(item.get("task_id")))
                _cancel_waiting_qwen_tts_jobs(str(item.get("task_id")))
                _cancel_active_infinitetalk_jobs(str(item.get("task_id")))
        if str(item.get("status") or "") in {"queued", "running"}:
            item["status"] = "cancelled"
            item["updated_at"] = time.time()
    _save_auto_digital_batch_job(job)
    return {"ok": True, "batch": _auto_digital_batch_payload(job)}


# ── 话题接口自动化 路由（管理员）──

@app.post("/api/topic-auto/run-now")
async def topic_auto_run_now(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以触发话题自动化")
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    limit = payload.get("limit") if isinstance(payload, dict) else None
    result = _run_topic_auto_produce_once(
        limit=int(limit) if limit else None,
        triggered_by=f"manual:{user.get('username') or 'admin'}",
    )
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@app.get("/api/topic-auto/status")
async def topic_auto_status(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以查看话题自动化状态")
    if error:
        return error
    config = _load_topic_auto_config()
    state = topic_auto.load_topic_state(str(TOPIC_AUTO_DIR))
    rec = state.get("records") or {}
    done_count = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "done")
    skipped_count = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "skipped")
    failed_pending = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "failed")
    recent = sorted(
        [dict(v, record_id=k) for k, v in rec.items() if isinstance(v, dict)],
        key=lambda v: v.get("updated_at") or 0, reverse=True,
    )[:20]
    batches = [
        _auto_digital_batch_payload(job)
        for job in _list_auto_digital_batch_jobs_for_user(TOPIC_AUTO_OWNER, limit=10)
        if job.get("source") == "topic_auto"
    ]
    running = _find_running_topic_auto_batch()
    yt = config.get("youtube") if isinstance(config.get("youtube"), dict) else {}
    fb = config.get("facebook") if isinstance(config.get("facebook"), dict) else {}
    return {
        "ok": True,
        "configured": topic_auto.topic_auto_is_configured(),
        "produced_count": done_count,
        "done_count": done_count,
        "skipped_count": skipped_count,
        "failed_pending_count": failed_pending,
        "running_batch_id": (running or {}).get("batch_id") if running else "",
        "youtube": {
            "bound": bool(yt.get("token_store_path")),
            "enabled": bool(yt.get("enabled")),
            "channel_name": yt.get("channel_name") or "",
            "auto_publish": bool(config.get("youtube_auto_publish")),
        },
        "facebook": {
            "bound": bool(fb.get("page_id") and fb.get("page_access_token")),
            "enabled": bool(fb.get("enabled")),
            "page_name": fb.get("page_name") or "",
            "auto_publish": bool(config.get("facebook_auto_publish")),
        },
        "config": _sanitize_topic_auto_config_for_client(config),
        "recent_records": [
            {"record_id": v.get("record_id"), "topic": v.get("topic") or "", "status": v.get("status") or "",
             "attempts": v.get("attempts") or 0, "updated_at": v.get("updated_at") or 0}
            for v in recent
        ],
        "recent_batches": batches,
    }


@app.get("/api/topic-auto/config")
async def topic_auto_get_config(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以查看话题自动化配置")
    if error:
        return error
    return {"ok": True, "configured": topic_auto.topic_auto_is_configured(), "config": _sanitize_topic_auto_config_for_client(_load_topic_auto_config())}


def _sanitize_topic_auto_config_for_client(config: dict) -> dict:
    """给前端的配置副本:抹掉 Facebook page_access_token 等敏感凭据,只留是否已绑定/页名。"""
    safe = copy.deepcopy(config) if isinstance(config, dict) else {}
    fb = safe.get("facebook") if isinstance(safe.get("facebook"), dict) else {}
    if fb:
        fb["page_access_token_configured"] = bool(fb.get("page_access_token"))
        fb.pop("page_access_token", None)
        safe["facebook"] = fb
    return safe


@app.post("/api/topic-auto/config")
async def topic_auto_set_config(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以修改话题自动化配置")
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return JSONResponse({"error": "配置格式错误"}, status_code=400)
    config = _load_topic_auto_config()
    if "scheduler_enabled" in payload:
        config["scheduler_enabled"] = bool(payload.get("scheduler_enabled"))
        if config["scheduler_enabled"]:
            config["next_run_at"] = 0  # 开启后尽快跑一次
    if "produce_limit" in payload:
        config["produce_limit"] = max(1, min(50, int(payload.get("produce_limit") or 5)))
    if "interval_minutes" in payload:
        config["interval_minutes"] = max(5, int(payload.get("interval_minutes") or 10))
    if "max_attempts" in payload:
        config["max_attempts"] = max(1, min(10, int(payload.get("max_attempts") or 3)))
    if "youtube_auto_publish" in payload:
        config["youtube_auto_publish"] = bool(payload.get("youtube_auto_publish"))
    if "facebook_auto_publish" in payload:
        config["facebook_auto_publish"] = bool(payload.get("facebook_auto_publish"))
    _save_topic_auto_config(config)
    return {"ok": True, "config": config}


@app.get("/api/topic-auto/topics")
async def topic_auto_topics(request: Request):
    """列出接口拉到的全部话题 + 每条状态（供页面浏览、勾选制作）。"""
    user, error = _require_admin_user(request, "只有管理员可以查看话题列表")
    if error:
        return error
    if not topic_auto.topic_auto_is_configured():
        return JSONResponse({"error": "未配置 TOPIC_COLLECTOR_API_TOKEN"}, status_code=400)
    try:
        records = topic_auto.fetch_topic_records()
    except Exception as exc:
        return JSONResponse({"error": f"拉取话题接口失败：{exc}"}, status_code=502)
    state = topic_auto.load_topic_state(str(TOPIC_AUTO_DIR))
    rec_state = state.get("records") or {}
    topics = []
    for r in records:
        ex = topic_auto.extract_topic_from_record(r)
        if not ex:
            continue
        st = rec_state.get(ex["record_id"]) or {}
        status = str(st.get("status") or "pending")
        topics.append({
            "record_id": ex["record_id"],
            "topic": ex["topic"],
            "angle": ex.get("angle") or "",
            "tags": ex.get("tags") or [],
            "created_at": ex.get("created_at") or "",
            "status": status if status in {"done", "failed", "skipped"} else "pending",
            "attempts": int(st.get("attempts") or 0),
        })
    running = _find_running_topic_auto_batch()
    return {"ok": True, "total": len(topics), "running_batch_id": (running or {}).get("batch_id") if running else "", "topics": topics}


@app.post("/api/topic-auto/produce-selected")
async def topic_auto_produce_selected(request: Request):
    """把页面勾选的话题走一站式(批量数字人)制作。"""
    user, error = _require_admin_user(request, "只有管理员可以制作话题视频")
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    ids = payload.get("record_ids") if isinstance(payload, dict) else None
    wanted = {str(x).strip() for x in (ids or []) if str(x).strip()}
    if not wanted:
        return JSONResponse({"error": "请先勾选要制作的话题"}, status_code=400)
    running = _find_running_topic_auto_batch()
    if running:
        return JSONResponse({"ok": True, "running": True, "batch_id": running.get("batch_id"), "message": "已有话题数字人批次在运行，请等它完成后再制作。"}, status_code=200)
    if not topic_auto.topic_auto_is_configured():
        return JSONResponse({"error": "未配置 TOPIC_COLLECTOR_API_TOKEN"}, status_code=400)
    try:
        records = topic_auto.fetch_topic_records()
    except Exception as exc:
        return JSONResponse({"error": f"拉取话题接口失败：{exc}"}, status_code=502)
    selected: list[dict] = []
    seen: set = set()
    for r in records:
        ex = topic_auto.extract_topic_from_record(r)
        if not ex or ex["record_id"] not in wanted or ex["record_id"] in seen:
            continue
        seen.add(ex["record_id"])
        selected.append(ex)
    if not selected:
        return JSONResponse({"error": "勾选的话题在接口里找不到（可能已被删除）"}, status_code=400)
    started = _start_topic_auto_batch(selected[:50])
    if not started.get("ok"):
        return JSONResponse(started, status_code=400)
    return {"ok": True, "produced": len(selected), "batch_id": started["batch_id"],
            "message": f"已选 {len(selected)} 条话题，创建数字人批次并开始逐条制作。"}


@app.get("/api/property-auto/status")
async def property_auto_status(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以查看房源自动化状态")
    if error:
        return error
    config = _load_property_auto_config()
    state = property_auto.load_property_state(str(PROPERTY_AUTO_DIR))
    rec = state.get("records") or {}
    done_count = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "done")
    skipped_count = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "skipped")
    failed_pending = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "failed")
    producing = sum(1 for v in rec.values() if isinstance(v, dict) and v.get("status") == "producing")
    recent = sorted([dict(v, record_id=k) for k, v in rec.items() if isinstance(v, dict)],
                    key=lambda v: v.get("updated_at") or 0, reverse=True)[:20]
    running = _find_running_property_auto_task()
    tcfg = _load_topic_auto_config()
    tyt = tcfg.get("youtube") if isinstance(tcfg.get("youtube"), dict) else {}
    tfb = tcfg.get("facebook") if isinstance(tcfg.get("facebook"), dict) else {}
    return {
        "ok": True,
        "configured": property_auto.property_auto_is_configured(),
        "done_count": done_count, "skipped_count": skipped_count,
        "failed_pending_count": failed_pending, "producing_count": producing,
        "running_task_id": running or "",
        "youtube": {
            "shared_bound": bool(tyt.get("token_store_path")),
            "shared_channel_name": tyt.get("channel_name") or "",
            "auto_publish": bool(config.get("youtube_auto_publish", True)),
        },
        "facebook": {
            "shared_bound": bool(tfb.get("page_id") and tfb.get("page_access_token")),
            "shared_page_name": tfb.get("page_name") or "",
            "auto_publish": bool(config.get("facebook_auto_publish", True)),
        },
        "config": config,
        "recent_records": [
            {"record_id": v.get("record_id"), "name": v.get("name") or "", "status": v.get("status") or "",
             "attempts": v.get("attempts") or 0, "updated_at": v.get("updated_at") or 0}
            for v in recent
        ],
    }


@app.post("/api/property-auto/config")
async def property_auto_set_config(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以修改房源自动化配置")
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return JSONResponse({"error": "配置格式错误"}, status_code=400)
    config = _load_property_auto_config()
    if "scheduler_enabled" in payload:
        config["scheduler_enabled"] = bool(payload.get("scheduler_enabled"))
        if config["scheduler_enabled"]:
            config["next_run_at"] = 0
    if "interval_minutes" in payload:
        config["interval_minutes"] = max(5, int(payload.get("interval_minutes") or 15))
    if "max_attempts" in payload:
        config["max_attempts"] = max(1, min(10, int(payload.get("max_attempts") or 3)))
    if "youtube_auto_publish" in payload:
        config["youtube_auto_publish"] = bool(payload.get("youtube_auto_publish"))
    if "facebook_auto_publish" in payload:
        config["facebook_auto_publish"] = bool(payload.get("facebook_auto_publish"))
    _save_property_auto_config(config)
    return {"ok": True, "config": config}


@app.get("/api/property-auto/properties")
async def property_auto_properties(request: Request):
    """列出房源接口全部房源 + 每个状态（供页面浏览、勾选制作）。"""
    user, error = _require_admin_user(request, "只有管理员可以查看房源列表")
    if error:
        return error
    if not property_auto.property_auto_is_configured():
        return JSONResponse({"error": "未配置 PROPERTY_API_TOKEN"}, status_code=400)
    try:
        records = property_auto.fetch_property_records()
    except Exception as exc:
        return JSONResponse({"error": f"拉取房源接口失败：{exc}"}, status_code=502)
    state = property_auto.load_property_state(str(PROPERTY_AUTO_DIR))
    rec_state = state.get("records") or {}
    props = []
    for r in records:
        ex = property_auto.extract_property(r)
        if not ex:
            continue
        st = rec_state.get(ex["record_id"]) or {}
        status = str(st.get("status") or "pending")
        props.append({
            "record_id": ex["record_id"], "name": ex["name"],
            "video_count": len(ex.get("video_urls") or []),
            "status": status if status in {"done", "failed", "skipped", "producing"} else "pending",
            "attempts": int(st.get("attempts") or 0),
        })
    running = _find_running_property_auto_task()
    return {"ok": True, "total": len(props), "running_task_id": running or "", "properties": props}


@app.post("/api/property-auto/produce-selected")
async def property_auto_produce_selected(request: Request):
    """把勾选的房源走一站式房源实拍成片（一次做一条，其余排队等下次）。"""
    user, error = _require_admin_user(request, "只有管理员可以制作房源视频")
    if error:
        return error
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    ids = payload.get("record_ids") if isinstance(payload, dict) else None
    wanted = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not wanted:
        return JSONResponse({"error": "请先勾选要制作的房源"}, status_code=400)
    result = _run_property_auto_produce_once(triggered_by=f"manual:{user.get('username') or 'admin'}", record_ids=wanted)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@app.post("/api/property-auto/run-now")
async def property_auto_run_now(request: Request):
    user, error = _require_admin_user(request, "只有管理员可以触发房源自动化")
    if error:
        return error
    result = _run_property_auto_produce_once(triggered_by=f"manual:{user.get('username') or 'admin'}")
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


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
    digital_human_engine: str = Form(INFINITETALK_ENGINE_ID),
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
    digital_human_engine: str = Form(INFINITETALK_ENGINE_ID),
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
    digital_human_queue = _omnihuman_queue_snapshot()
    qwen_tts_queue = _qwen_tts_queue_snapshot()
    gpu_queue = _gpu_resource_snapshot()
    payload = dict(digital_human_queue)
    payload["digital_human"] = digital_human_queue
    payload["qwen_tts"] = qwen_tts_queue
    payload["gpu_5090"] = gpu_queue
    return payload


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
    removed_qwen_jobs = _cancel_waiting_qwen_tts_jobs(task_id)
    infinitetalk_cancel_result = _cancel_active_infinitetalk_jobs(task_id)

    if tracker.step <= 0:
        tracker.step = 1
    tracker.log("已收到停止请求，系统会尽快停止当前任务")
    if removed_jobs:
        tracker.log(f"已从数字人队列移除 {removed_jobs} 个待执行任务")
    if removed_qwen_jobs:
        tracker.log(f"已从 5090 配音队列移除 {removed_qwen_jobs} 个待执行任务")
    if infinitetalk_cancel_result.get("ok"):
        cancelled_jobs = infinitetalk_cancel_result.get("cancelled_job_ids") or []
        if cancelled_jobs:
            tracker.log(f"已通知 5090 停止 {len(cancelled_jobs)} 个 InfiniteTalk 任务")
    _push_live_event(
        "task_cancel_requested",
        "任务已请求停止",
        task,
        {
            "removed_waiting_jobs": removed_jobs,
            "removed_qwen_tts_jobs": removed_qwen_jobs,
            "infinitetalk_cancel_result": infinitetalk_cancel_result,
        },
    )

    return {
        "task_id": task_id,
        "status": "cancelling",
        "message": "已收到停止请求，系统会尽快停止当前任务",
        "removed_waiting_jobs": removed_jobs,
        "removed_qwen_tts_jobs": removed_qwen_jobs,
        "infinitetalk_cancel_result": infinitetalk_cancel_result,
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
            "allow_local_digital_human": bool(workflow_config.get("allow_local_digital_human")),
            "digital_human_engine": _normalize_digital_human_engine(
                workflow_config.get("digital_human_engine"),
                {**user, "workflow_config": workflow_config},
            ),
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
    existing_fingerprints: set[str] = set()
    for existing_item in material_items:
        if not isinstance(existing_item, dict):
            continue
        if str(existing_item.get("kind") or "image").lower() == "video":
            continue
        existing_path = Path(str(existing_item.get("path") or ""))
        if not existing_path.is_absolute():
            existing_path = output_dir / existing_path
        if not existing_path.exists() or not existing_path.is_file():
            continue
        fingerprint = image_material_fingerprint(existing_path)
        if fingerprint:
            existing_fingerprints.add(fingerprint[:32])
    for upload in images:
        if not upload.filename:
            continue
        ext = Path(upload.filename).suffix or ".jpg"
        filename = f"material_{segment_index:02d}_manual_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}{ext}"
        output_path = material_dir / filename
        with open(output_path, "wb") as f:
            f.write(await upload.read())
        kind = "video" if ext.lower() in {".mp4", ".mov", ".m4v", ".webm"} else "image"
        if kind != "video":
            uploaded_fingerprint = image_material_fingerprint(output_path)
            if uploaded_fingerprint and uploaded_fingerprint[:32] in existing_fingerprints:
                output_path.unlink(missing_ok=True)
                continue
            if uploaded_fingerprint:
                existing_fingerprints.add(uploaded_fingerprint[:32])
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
            task_id="",
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
    payload = _serialize_result_for_ui(str(output_dir), result, result.get("topic", ""))
    payload["platform_metrics"] = _collect_history_platform_metrics(output_dir, result, force_refresh=False)
    return payload


@app.get("/api/history/{history_id}/platform-metrics")
async def history_platform_metrics(history_id: str, request: Request, refresh: int = 0):
    user, error = _require_user(request)
    if error:
        return error
    output_dir, result, access_error = _resolve_history_for_user(history_id, user)
    if access_error:
        return access_error
    payload = _collect_history_platform_metrics(output_dir, result, force_refresh=bool(refresh))
    return payload


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

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    requested_aspect_ratio = str((payload or {}).get("aspect_ratio") or "").strip().lower()
    if requested_aspect_ratio not in {"vertical", "horizontal"}:
        requested_aspect_ratio = ""
    try:
        result = _compose_history_result(
            output_dir,
            result,
            user=user,
            requested_aspect_ratio=requested_aspect_ratio,
            cost_scope="manual_history_compose",
        )
    except Exception as exc:
        return JSONResponse({"error": f"自动成片失败：{exc}"}, status_code=500)
    publish_result = None
    if _is_opennews_result(result) and _opennews_result_has_publishable_video(output_dir, result):
        publish_result = _auto_publish_opennews_result_data(output_dir, result)
    return {
        "ok": True,
        "result": _serialize_result_for_ui(str(output_dir), result, result.get("topic", "")),
        "publish": publish_result,
    }


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
