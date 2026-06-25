"""OpenNews batched hot-topic fetching and production queue helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from opennews_trends import search_english_trends


DEFAULT_BATCH_CONFIG = {
    "enabled": False,
    "interval_minutes": 180,
    "category": "all",
    "time_range": "6h",
    "limit": 20,
    "last_run_at": 0,
    "next_run_at": 0,
    "last_run_message": "自动抓取尚未启动。",
    "last_run_error": "",
}

VALID_INTERVALS = {5, 15, 30, 60, 120, 180, 360}
VALID_TIME_RANGES = {"1h", "6h", "24h"}
VALID_CATEGORIES = {
    "all",
    "ai",
    "real_estate",
    "immigration",
    "technology",
    "finance",
    "military",
    "politics",
}

_FILE_LOCK = threading.Lock()
_RUN_LOCK = threading.Lock()
_SCHEDULER_STARTED = False
_AFTER_FETCH_CALLBACK = None
RETENTION_SECONDS = max(3600, int(os.getenv("OPENNEWS_BATCH_RETENTION_SECONDS", str(2 * 24 * 60 * 60)) or str(2 * 24 * 60 * 60)))
DEDUPE_MEMORY_SECONDS = max(
    RETENTION_SECONDS,
    int(os.getenv("OPENNEWS_BATCH_DEDUPE_MEMORY_SECONDS", str(14 * 24 * 60 * 60)) or str(14 * 24 * 60 * 60)),
)
EVENT_DUPLICATE_TOKEN_OVERLAP = max(
    0.42,
    min(0.95, float(os.getenv("OPENNEWS_BATCH_EVENT_DUPLICATE_TOKEN_OVERLAP", "0.58") or "0.58")),
)
EVENT_DUPLICATE_MIN_TOKENS = max(
    4,
    min(12, int(os.getenv("OPENNEWS_BATCH_EVENT_DUPLICATE_MIN_TOKENS", "5") or "5")),
)
FETCH_OVERFETCH_MULTIPLIER = max(
    1,
    min(8, int(os.getenv("OPENNEWS_BATCH_FETCH_OVERFETCH_MULTIPLIER", "4") or "4")),
)
FETCH_OVERFETCH_MIN = max(
    20,
    min(120, int(os.getenv("OPENNEWS_BATCH_FETCH_OVERFETCH_MIN", "80") or "80")),
)
FETCH_OVERFETCH_MAX = max(
    FETCH_OVERFETCH_MIN,
    min(160, int(os.getenv("OPENNEWS_BATCH_FETCH_OVERFETCH_MAX", "100") or "100")),
)

TITLE_DEDUPE_STOPWORDS = {
    "about", "after", "again", "against", "amid", "and", "are", "as", "at", "back",
    "be", "been", "before", "being", "by", "can", "could", "day", "days", "for",
    "from", "has", "have", "how", "in", "into", "is", "it", "its", "latest",
    "live", "may", "more", "new", "news", "not", "of", "on", "over", "said",
    "says", "say", "than", "that", "the", "their", "this", "to", "top", "update",
    "updates", "video", "watch", "what", "when", "where", "why", "will", "with",
    "would", "报道", "最新", "视频", "新闻", "更新",
    "according", "reportedly", "breaking", "exclusive", "analysis", "opinion", "explainer",
    "today", "yesterday", "monday", "tuesday", "wednesday", "thursday", "friday",
    "saturday", "sunday", "jst", "gmt", "utc",
}


def _config_path(root: Path) -> Path:
    return root / "batch_config.json"


def _seen_path(root: Path) -> Path:
    return root / "seen.json"


def _batches_dir(root: Path) -> Path:
    return root / "batches"


def _jobs_dir(root: Path) -> Path:
    return root / "batch_jobs"


def _ensure_root(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _batches_dir(root).mkdir(parents=True, exist_ok=True)
    _jobs_dir(root).mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _parse_news_timestamp(value: Any) -> float:
    if value in (None, "", 0, "0"):
        return 0.0
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000
        return ts if ts > 0 else 0.0
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return _parse_news_timestamp(float(text))
    except Exception:
        pass
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _candidate_reference_timestamp(item: dict) -> float:
    published_ts = _parse_news_timestamp(item.get("published_ts"))
    if published_ts:
        return published_ts
    published_at = _parse_news_timestamp(item.get("published_at") or item.get("news_time") or item.get("date"))
    if published_at:
        return published_at
    return _safe_float(item.get("batch_fetched_at") or item.get("fetched_at") or item.get("created_at"), 0)


def _is_recent_candidate(item: dict, *, now: float | None = None) -> bool:
    now = now or time.time()
    ts = _candidate_reference_timestamp(item)
    if not ts:
        return True
    # Allow a small future clock skew from upstream feeds.
    if ts > now + 6 * 60 * 60:
        return True
    return ts >= now - RETENTION_SECONDS


def _prune_old_batches_locked(root: Path, *, now: float | None = None) -> dict:
    now = now or time.time()
    removed_batches = 0
    removed_items = 0
    kept_keys: set[str] = set()
    for path in sorted(_batches_dir(root).glob("batch_*.json")):
        payload = _read_json(path, {})
        if not isinstance(payload, dict):
            continue
        items = payload.get("items") or []
        if not isinstance(items, list):
            items = []
        fresh_items = [item for item in items if isinstance(item, dict) and _is_recent_candidate(item, now=now)]
        removed_items += max(0, len(items) - len(fresh_items))
        for item in fresh_items:
            key = str(item.get("batch_item_id") or item.get("id") or "").strip()
            if key:
                kept_keys.add(key)
        if not fresh_items and items:
            try:
                path.unlink()
                removed_batches += 1
            except FileNotFoundError:
                pass
            continue
        if len(fresh_items) != len(items):
            payload["items"] = fresh_items
            payload["retention_pruned_at"] = now
            payload["retention_policy_seconds"] = RETENTION_SECONDS
            _write_json(path, payload)

    seen = _read_json(_seen_path(root), {})
    if isinstance(seen, dict):
        fresh_seen = {}
        for key, value in seen.items():
            if not isinstance(value, dict):
                continue
            last_seen_at = _safe_float(value.get("last_seen_at") or value.get("first_seen_at"), 0)
            if (last_seen_at and last_seen_at >= now - DEDUPE_MEMORY_SECONDS) or str(key) in kept_keys:
                fresh_seen[key] = value
        if len(fresh_seen) != len(seen):
            _write_json(_seen_path(root), fresh_seen)
    return {"removed_batches": removed_batches, "removed_items": removed_items}


def cleanup_old_batches(root: Path) -> dict:
    _ensure_root(root)
    with _FILE_LOCK:
        return _prune_old_batches_locked(root)


def _normalize_config(config: dict | None) -> dict:
    clean = dict(DEFAULT_BATCH_CONFIG)
    if isinstance(config, dict):
        clean.update(config)
    clean["enabled"] = bool(clean.get("enabled"))
    try:
        interval = int(clean.get("interval_minutes") or DEFAULT_BATCH_CONFIG["interval_minutes"])
    except Exception:
        interval = DEFAULT_BATCH_CONFIG["interval_minutes"]
    clean["interval_minutes"] = interval if interval in VALID_INTERVALS else DEFAULT_BATCH_CONFIG["interval_minutes"]
    category = str(clean.get("category") or "all").strip().lower()
    clean["category"] = category if category in VALID_CATEGORIES else "all"
    time_range = str(clean.get("time_range") or "6h").strip().lower()
    clean["time_range"] = time_range if time_range in VALID_TIME_RANGES else "6h"
    try:
        clean["limit"] = max(5, min(int(clean.get("limit") or 20), 60))
    except Exception:
        clean["limit"] = 20
    for key in ("last_run_at", "next_run_at"):
        clean[key] = _safe_float(clean.get(key), 0)
    clean["last_run_message"] = str(clean.get("last_run_message") or "")
    clean["last_run_error"] = str(clean.get("last_run_error") or "")
    return clean


def load_batch_config(root: Path) -> dict:
    _ensure_root(root)
    with _FILE_LOCK:
        return _normalize_config(_read_json(_config_path(root), {}))


def save_batch_config(root: Path, config: dict) -> dict:
    _ensure_root(root)
    current = load_batch_config(root)
    current.update(config or {})
    clean = _normalize_config(current)
    now = time.time()
    if clean.get("enabled") and not clean.get("next_run_at"):
        clean["next_run_at"] = now + clean["interval_minutes"] * 60
    if not clean.get("enabled"):
        clean["next_run_at"] = 0
    with _FILE_LOCK:
        _write_json(_config_path(root), clean)
    return clean


def _candidate_key(candidate: dict) -> str:
    url = str(candidate.get("url") or "").strip().lower()
    title = str(candidate.get("title") or candidate.get("title_zh") or "").strip().lower()
    source = str(candidate.get("source_name") or candidate.get("trend_domain") or "").strip().lower()
    # Use source + normalized title as the stable event key; URL alone is too noisy
    # across news aggregators and syndicated copies.
    title_norm = " ".join(title.split())
    basis = f"{source}|{title_norm or url}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:18]


def _candidate_url_key(candidate: dict) -> str:
    url = str(candidate.get("url") or candidate.get("source_url") or "").strip().lower()
    if not url:
        return ""
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"[?&](?:utm_[^=&]+|fbclid|gclid|mc_cid|mc_eid|ref|ref_src|cmpid|outputType)=[^&]*", "", url)
    url = re.sub(r"[?&]+$", "", url)
    url = url.rstrip("/")
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:18] if url else ""


def _normalize_dedupe_word(token: str) -> str:
    token = str(token or "").lower().strip("._-")
    if len(token) > 5 and token.endswith("ies"):
        token = token[:-3] + "y"
    elif len(token) > 5 and token.endswith("ing"):
        token = token[:-3]
    elif len(token) > 4 and token.endswith("ed"):
        token = token[:-2]
    elif len(token) > 4 and token.endswith("es"):
        token = token[:-2]
    elif len(token) > 3 and token.endswith("s"):
        token = token[:-1]
    return token


def _candidate_dedupe_text(candidate: dict) -> str:
    fields = [
        candidate.get("title"),
        candidate.get("title_zh"),
        candidate.get("translated_title"),
        candidate.get("original_title"),
        candidate.get("english_title"),
        candidate.get("summary"),
        candidate.get("summary_zh"),
        candidate.get("translated_summary"),
        candidate.get("description"),
        candidate.get("content"),
    ]
    text = " ".join(str(field or "") for field in fields)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[\u2018\u2019\u201c\u201d]", "'", text)
    return text.lower()


def _candidate_title_key(candidate: dict) -> str:
    compact = _candidate_title_compact(candidate)
    return hashlib.sha1(compact.encode("utf-8")).hexdigest()[:18] if compact else ""


def _candidate_title_compact(candidate: dict) -> str:
    title = str(
        candidate.get("title")
        or candidate.get("title_zh")
        or candidate.get("translated_title")
        or candidate.get("original_title")
        or candidate.get("english_title")
        or ""
    ).lower()
    title = re.sub(r"<[^>]+>", " ", title)
    title = re.sub(r"https?://\S+", " ", title)
    title = re.sub(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\.?\s+\d{1,2},?\s+\d{4}\b", " ", title)
    title = re.sub(r"\b20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}日?\b", " ", title)
    title = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", title)
    words = []
    for raw in title.split():
        word = _normalize_dedupe_word(raw)
        if len(word) < 2 or word in TITLE_DEDUPE_STOPWORDS:
            continue
        words.append(word)
    return " ".join(words[:24]).strip()


def _candidate_event_tokens(candidate: dict) -> list[str]:
    text = _candidate_dedupe_text(candidate)
    aliases = {
        "u.s.": "us",
        "u.s": "us",
        "united states": "us",
        "wall street": "wallstreet",
        "federal reserve": "fed",
        "federal open market committee": "fed",
        "artificial intelligence": "ai",
        "generative artificial intelligence": "ai",
        "ai chip": "aichip",
        "ai chips": "aichip",
        "semiconductor": "chip",
        "semiconductors": "chip",
        "donald trump": "trump",
        "president trump": "trump",
        "jensen huang": "jensenhuang",
        "nvidia ceo": "jensenhuang",
        "elon musk": "elonmusk",
        "x ai": "xai",
        "amazon web services": "aws",
        "amazon bedrock": "bedrock",
        "aws bedrock": "bedrock",
        "grok 4.3": "grok43",
        "grok-4.3": "grok43",
        "grok4.3": "grok43",
        "grok 4 3": "grok43",
        "white house": "whitehouse",
        "middle east": "middleeast",
        "stock market": "stockmarket",
        "oil price": "oilprice",
        "oil prices": "oilprice",
        "mortgage rate": "mortgagerate",
        "mortgage rates": "mortgagerate",
        "real estate": "realestate",
        "housing market": "housingmarket",
        "人工智能": " ai ",
        "英伟达": " nvidia ",
        "黃仁勳": " jensenhuang ",
        "黄仁勋": " jensenhuang ",
        "特朗普": " trump ",
        "白宫": " whitehouse ",
        "白宮": " whitehouse ",
        "美联储": " fed ",
        "聯準會": " fed ",
        "房地产": " realestate ",
        "房产": " realestate ",
        "房價": " homeprice ",
        "房价": " homeprice ",
        "油价": " oilprice ",
        "油價": " oilprice ",
        "伊朗": " iran ",
        "以色列": " israel ",
        "乌克兰": " ukraine ",
        "烏克蘭": " ukraine ",
        "俄罗斯": " russia ",
        "俄羅斯": " russia ",
        "中国": " china ",
        "中國": " china ",
        "台湾": " taiwan ",
        "台灣": " taiwan ",
        "北约": " nato ",
        "北約": " nato ",
        "马斯克": " elonmusk ",
        "馬斯克": " elonmusk ",
        "亚马逊": " amazon aws ",
        "亞馬遜": " amazon aws ",
    }
    for source, replacement in aliases.items():
        text = text.replace(source, f" {replacement} ")
    raw_tokens = re.findall(r"[a-z0-9][a-z0-9.+-]{1,}|[\u4e00-\u9fff]{2,}", text)
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in raw_tokens:
        token = _normalize_dedupe_word(raw)
        if len(token) < 2 or token in TITLE_DEDUPE_STOPWORDS:
            continue
        if token.isdigit() and len(token) < 4:
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
        if len(tokens) >= 40:
            break
    return tokens


def _candidate_canonical_event_key(candidate: dict) -> str:
    text = _candidate_dedupe_text(candidate)
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff.]+", " ", text.lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return ""

    def has(pattern: str) -> bool:
        return bool(re.search(pattern, normalized, flags=re.I))

    # Product-version stories are the easiest to repeat under many headlines.
    # Give them stable event clusters before the softer token overlap logic.
    if has(r"\bgrok\s*[-_ ]*4\s*(?:\.|[-_ ])\s*3\b|\bgrok4\.?3\b"):
        if has(r"\b(?:aws|amazon|bedrock)\b|亚马逊|亞馬遜"):
            return "ai:grok_4_3_aws_bedrock"
        return "ai:grok_4_3"
    if has(r"\bclaude\s*(?:sonnet\s*)?4\s*(?:\.|[-_ ])\s*6\b|\bclaude4\.?6\b"):
        return "ai:claude_sonnet_4_6"
    if has(r"\bgpt\s*[-_ ]?5(?:\.\d+)?\b|\bchatgpt\s*5(?:\.\d+)?\b"):
        version = re.search(r"\b(?:gpt|chatgpt)\s*[-_ ]?5(?:\.(\d+))?\b", normalized, flags=re.I)
        suffix = f"_{version.group(1)}" if version and version.group(1) else ""
        return f"ai:gpt_5{suffix}"
    if has(r"\bgemini\s*3(?:\.\d+)?\b"):
        return "ai:gemini_3"

    # Obituaries often appear under rewritten headlines that omit "Fed" while
    # still describing the same person/event. Anchor the person + death signal
    # before the broader Federal Reserve branch so cross-source copies collapse.
    if has(r"\b(?:alan greenspan|greenspan)\b|格林斯潘") and has(r"\b(?:die|dies|died|dead|death|decease|obituary)\b|去世|逝世|離世|离世|享年"):
        return "obit:alan_greenspan_death"

    if has(r"\b(?:nvidia|jensen huang|jensenhuang)\b|英伟达|黃仁勳|黄仁勋"):
        if has(r"\b(?:china|export|restriction|ban|chip|semiconductor|aichip)\b|出口|限制|芯片|晶片"):
            return "ai:nvidia_china_chip_export_controls"
        if has(r"\b(?:data center|datacenter|energy|electricity|power)\b|数据中心|資料中心|能源|电力|電力"):
            return "ai:nvidia_ai_datacenter_energy"

    if has(r"\b(?:fed|federal reserve|powell|jerome powell)\b|美联储|聯準會"):
        if has(r"\b(?:rate|interest|inflation|cut|hold)\b|利率|降息|通胀|通膨"):
            return "finance:fed_rates_inflation"

    if has(r"\b(?:iran|israel|strait of hormuz|hormuz)\b|伊朗|以色列|霍尔木兹|霍爾木茲"):
        return "military:iran_israel_hormuz_conflict"

    return ""


def _candidate_event_signature_tokens(candidate: dict) -> list[str]:
    tokens = _candidate_event_tokens(candidate)
    canonical_key = _candidate_canonical_event_key(candidate)
    if canonical_key:
        tokens = [canonical_key.replace(":", "_")] + tokens
    if not tokens:
        return []
    priority_patterns = (
        r"^(?:ai|aichip|chip|nvidia|jensenhuang|openai|anthropic|google|microsoft|meta|apple|tesla|robot|robotic|robotics)$",
        r"^(?:xai|grok|grok43|aws|amazon|bedrock|claude|gemini|chatgpt|gpt)$",
        r"^(?:ai_grok_4_3|ai_grok_4_3_aws_bedrock|ai_claude_sonnet_4_6|ai_gpt_5|ai_gpt_5_[0-9]+|ai_gemini_3)$",
        r"^(?:trump|whitehouse|congress|senate|fed|powell|iran|israel|ukraine|russia|china|taiwan|nato)$",
        r"^(?:stockmarket|market|stock|oilprice|oil|dollar|inflation|tariff|rate|mortgagerate|housingmarket|realestate|homeprice)$",
        r"^[0-9]{4}$",
        r"^[0-9]+(?:\.[0-9]+)?(?:bn|billion|mn|million|trillion|%)?$",
    )
    priority: list[str] = []
    regular: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        bucket = priority if any(re.search(pattern, token) for pattern in priority_patterns) else regular
        if token not in seen:
            bucket.append(token)
            seen.add(token)
    combined = priority + regular
    return sorted(combined[:24])


def _candidate_event_key(candidate: dict) -> str:
    canonical_key = _candidate_canonical_event_key(candidate)
    if canonical_key:
        return hashlib.sha1(canonical_key.encode("utf-8")).hexdigest()[:18]
    tokens = _candidate_event_signature_tokens(candidate)
    if not tokens:
        return ""
    basis = " ".join(tokens)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:18]


STRONG_EVENT_ANCHOR_TOKENS = {
    "ai", "aichip", "chip", "nvidia", "jensenhuang", "openai", "anthropic", "google", "microsoft", "meta",
    "apple", "tesla", "trump", "whitehouse", "congress", "senate", "fed", "powell", "iran", "israel",
    "ukraine", "russia", "china", "taiwan", "nato", "stockmarket", "oilprice", "mortgagerate",
    "housingmarket", "realestate", "homeprice", "elonmusk", "spacex", "xai", "grok", "grok43",
    "aws", "amazon", "bedrock", "claude", "gemini", "chatgpt", "gpt",
    "ai_grok_4_3", "ai_grok_4_3_aws_bedrock", "ai_claude_sonnet_4_6", "ai_gpt_5", "ai_gemini_3",
}


def _strong_event_anchor_overlap(candidate_tokens: set[str], existing_tokens: set[str]) -> int:
    return len((candidate_tokens & existing_tokens) & STRONG_EVENT_ANCHOR_TOKENS)


def _has_grok43_signature(tokens: set[str]) -> bool:
    return "grok43" in tokens or ("grok" in tokens and ("4.3" in tokens or "43" in tokens))


def _has_aws_bedrock_signature(tokens: set[str]) -> bool:
    return bool(tokens & {"aws", "amazon", "bedrock"})


def _candidate_title_similar(left: str, right: str) -> bool:
    left_tokens = [token for token in left.split() if token]
    right_tokens = [token for token in right.split() if token]
    if len(left_tokens) < 3 or len(right_tokens) < 3:
        return False
    left_set = set(left_tokens)
    right_set = set(right_tokens)
    overlap = len(left_set & right_set)
    if overlap >= 5:
        return True
    denominator = max(1, min(len(left_set), len(right_set)))
    return overlap / denominator >= 0.62


def _is_duplicate_event(candidate_tokens: list[str], existing_tokens: list[str]) -> bool:
    candidate_set = set(candidate_tokens)
    existing_set = set(existing_tokens)
    if _has_grok43_signature(candidate_set) and _has_grok43_signature(existing_set):
        if _has_aws_bedrock_signature(candidate_set) or _has_aws_bedrock_signature(existing_set):
            return True
    if len(candidate_tokens) < EVENT_DUPLICATE_MIN_TOKENS or len(existing_tokens) < EVENT_DUPLICATE_MIN_TOKENS:
        return _strong_event_anchor_overlap(candidate_set, existing_set) >= 3
    overlap = len(candidate_set & existing_set)
    if _strong_event_anchor_overlap(candidate_set, existing_set) >= 3:
        return True
    if overlap >= 7:
        return True
    denominator = max(1, min(len(candidate_set), len(existing_set)))
    return (overlap / denominator) >= EVENT_DUPLICATE_TOKEN_OVERLAP


def _batch_path(root: Path, batch_id: str) -> Path:
    return _batches_dir(root) / f"{batch_id}.json"


def _job_path(root: Path, job_id: str) -> Path:
    return _jobs_dir(root) / f"{job_id}.json"


def set_after_fetch_callback(callback) -> None:
    global _AFTER_FETCH_CALLBACK
    _AFTER_FETCH_CALLBACK = callback


def _notify_after_fetch(root: Path, payload: dict) -> None:
    callback = _AFTER_FETCH_CALLBACK
    if not callback or not payload.get("ok"):
        return
    try:
        callback(root, payload)
    except Exception:
        pass


def _candidate_payload(candidate: dict, *, batch_id: str, category: str, fetched_at: float) -> dict:
    item = dict(candidate)
    item_id = _candidate_key(item)
    item["id"] = str(item.get("id") or item_id)
    item["batch_item_id"] = item_id
    item["batch_id"] = batch_id
    item["batch_category"] = category
    item["batch_fetched_at"] = fetched_at
    item["status"] = str(item.get("status") or "pending")
    return item


def run_batch_fetch_once(root: Path, *, triggered_by: str = "manual", override: dict | None = None) -> dict:
    _ensure_root(root)
    if not _RUN_LOCK.acquire(blocking=False):
        return {"ok": False, "running": True, "message": "热点批次抓取正在执行中，请稍后刷新。"}
    started_at = time.time()
    batch_id = time.strftime("batch_%Y%m%d_%H%M%S")
    config = load_batch_config(root)
    if isinstance(override, dict):
        config.update({k: v for k, v in override.items() if v not in (None, "", [])})
        config = _normalize_config(config)
    category = str(config.get("category") or "all")
    time_range = str(config.get("time_range") or "6h")
    limit = int(config.get("limit") or 20)
    payload = {
        "batch_id": batch_id,
        "triggered_by": triggered_by,
        "started_at": started_at,
        "finished_at": 0,
        "category": category,
        "time_range": time_range,
        "limit": limit,
        "items": [],
        "duplicate_count": 0,
        "raw_count": 0,
        "source_errors": [],
        "message": "",
    }
    try:
        fetch_limit = max(limit, min(FETCH_OVERFETCH_MAX, max(FETCH_OVERFETCH_MIN, limit * FETCH_OVERFETCH_MULTIPLIER)))
        result = search_english_trends(category=category, time_range=time_range, keyword="", limit=fetch_limit)
        candidates = result.get("candidates") or []
        payload["raw_count"] = len(candidates)
        payload["source_limit"] = fetch_limit
        payload["source_errors"] = result.get("source_errors", [])
        with _FILE_LOCK:
            _prune_old_batches_locked(root, now=started_at)
            seen = _read_json(_seen_path(root), {})
            if not isinstance(seen, dict):
                seen = {}
            seen_title_keys = {
                str(value.get("title_key") or "")
                for value in seen.values()
                if isinstance(value, dict) and value.get("title_key")
            }
            seen_url_keys = {
                str(value.get("url_key") or "")
                for value in seen.values()
                if isinstance(value, dict) and value.get("url_key")
            }
            seen_event_keys = {
                str(value.get("event_key") or "")
                for value in seen.values()
                if isinstance(value, dict) and value.get("event_key")
            }
            seen_event_tokens = [
                list(value.get("event_tokens") or [])
                for value in seen.values()
                if isinstance(value, dict) and isinstance(value.get("event_tokens"), list)
            ]
            seen_title_compacts = [
                str(value.get("title_compact") or "")
                for value in seen.values()
                if isinstance(value, dict) and value.get("title_compact")
            ]
            new_items = []
            duplicate_count = 0
            duplicate_event_count = 0
            duplicate_reason_counts: dict[str, int] = {}
            for candidate in candidates:
                key = _candidate_key(candidate)
                url_key = _candidate_url_key(candidate)
                title_key = _candidate_title_key(candidate)
                title_compact = _candidate_title_compact(candidate)
                event_key = _candidate_event_key(candidate)
                event_tokens = _candidate_event_tokens(candidate)
                duplicate_reason = ""
                if key in seen:
                    duplicate_reason = "same_source_title"
                elif url_key and url_key in seen_url_keys:
                    duplicate_reason = "same_url"
                elif title_key and title_key in seen_title_keys:
                    duplicate_reason = "same_title"
                elif title_compact and any(_candidate_title_similar(title_compact, existing) for existing in seen_title_compacts):
                    duplicate_reason = "similar_title"
                elif event_key and event_key in seen_event_keys:
                    duplicate_reason = "same_event_key"
                elif any(_is_duplicate_event(event_tokens, existing_tokens) for existing_tokens in seen_event_tokens):
                    duplicate_reason = "similar_event_tokens"
                if duplicate_reason:
                    duplicate_count += 1
                    duplicate_reason_counts[duplicate_reason] = duplicate_reason_counts.get(duplicate_reason, 0) + 1
                    if duplicate_reason != "same_source_title":
                        duplicate_event_count += 1
                    existing_key = key if key in seen else ""
                    if not existing_key:
                        for seen_key, seen_value in seen.items():
                            if not isinstance(seen_value, dict):
                                continue
                            if url_key and seen_value.get("url_key") == url_key:
                                existing_key = str(seen_key)
                                break
                            if title_key and seen_value.get("title_key") == title_key:
                                existing_key = str(seen_key)
                                break
                            if title_compact and _candidate_title_similar(title_compact, str(seen_value.get("title_compact") or "")):
                                existing_key = str(seen_key)
                                break
                            if event_key and seen_value.get("event_key") == event_key:
                                existing_key = str(seen_key)
                                break
                            if _is_duplicate_event(event_tokens, list(seen_value.get("event_tokens") or [])):
                                existing_key = str(seen_key)
                                break
                    if existing_key and isinstance(seen.get(existing_key), dict):
                        seen[existing_key]["last_seen_at"] = started_at
                        seen[existing_key]["seen_count"] = int(seen[existing_key].get("seen_count") or 1) + 1
                        seen[existing_key]["last_duplicate_reason"] = duplicate_reason
                    continue
                item = _candidate_payload(candidate, batch_id=batch_id, category=category, fetched_at=started_at)
                item["dedupe_key"] = key
                item["url_key"] = url_key
                item["title_key"] = title_key
                item["title_compact"] = title_compact
                item["event_key"] = event_key
                new_items.append(item)
                seen[key] = {
                    "key": key,
                    "url_key": url_key,
                    "title_key": title_key,
                    "title_compact": title_compact,
                    "event_key": event_key,
                    "event_tokens": event_tokens,
                    "title": item.get("title") or item.get("title_zh") or "",
                    "url": item.get("url") or "",
                    "first_seen_at": started_at,
                    "last_seen_at": started_at,
                    "seen_count": 1,
                    "batch_id": batch_id,
                }
                if url_key:
                    seen_url_keys.add(url_key)
                if title_key:
                    seen_title_keys.add(title_key)
                if event_key:
                    seen_event_keys.add(event_key)
                if event_tokens:
                    seen_event_tokens.append(event_tokens)
                if title_compact:
                    seen_title_compacts.append(title_compact)
            payload["items"] = new_items[:limit]
            payload["duplicate_count"] = duplicate_count
            payload["duplicate_event_count"] = duplicate_event_count
            payload["duplicate_reason_counts"] = duplicate_reason_counts
            payload["finished_at"] = time.time()
            payload["message"] = (
                f"抓取完成：源头候选 {len(candidates)} 条，新增 {len(payload['items'])} 条，过滤重复 {duplicate_count} 条"
                f"（跨来源/相似事件 {duplicate_event_count} 条）。"
            )
            _write_json(_seen_path(root), seen)
            _write_json(_batch_path(root, batch_id), payload)
            config["last_run_at"] = payload["finished_at"]
            config["next_run_at"] = payload["finished_at"] + int(config.get("interval_minutes") or DEFAULT_BATCH_CONFIG["interval_minutes"]) * 60 if config.get("enabled") else 0
            config["last_run_message"] = payload["message"]
            config["last_run_error"] = ""
            _write_json(_config_path(root), _normalize_config(config))
        result_payload = {"ok": True, **payload}
        _notify_after_fetch(root, result_payload)
        return result_payload
    except Exception as exc:
        payload["finished_at"] = time.time()
        payload["message"] = f"抓取失败：{exc}"
        with _FILE_LOCK:
            config["last_run_at"] = payload["finished_at"]
            config["next_run_at"] = payload["finished_at"] + int(config.get("interval_minutes") or DEFAULT_BATCH_CONFIG["interval_minutes"]) * 60 if config.get("enabled") else 0
            config["last_run_error"] = str(exc)
            config["last_run_message"] = payload["message"]
            _write_json(_config_path(root), _normalize_config(config))
            _write_json(_batch_path(root, batch_id), payload)
        return {"ok": False, **payload}
    finally:
        _RUN_LOCK.release()


def list_batches(root: Path, *, limit: int = 20) -> list[dict]:
    _ensure_root(root)
    with _FILE_LOCK:
        _prune_old_batches_locked(root)
        paths = sorted(_batches_dir(root).glob("batch_*.json"), key=lambda p: p.name, reverse=True)
        batches = []
        for path in paths[: max(1, min(limit, 80))]:
            payload = _read_json(path, {})
            if isinstance(payload, dict):
                batches.append(payload)
        return batches


def find_batch_items(root: Path, item_ids: list[str]) -> list[dict]:
    wanted = {str(item or "").strip() for item in item_ids if str(item or "").strip()}
    if not wanted:
        return []
    found: list[dict] = []
    for batch in list_batches(root, limit=80):
        for item in batch.get("items", []) or []:
            if str(item.get("batch_item_id") or item.get("id") or "") in wanted:
                found.append(dict(item))
    return found


def _remember_candidate_seen_locked(root: Path, candidate: dict, *, reason: str = "seen") -> None:
    if not isinstance(candidate, dict):
        return
    seen = _read_json(_seen_path(root), {})
    if not isinstance(seen, dict):
        seen = {}
    now = time.time()
    key = str(candidate.get("dedupe_key") or _candidate_key(candidate) or candidate.get("batch_item_id") or candidate.get("id") or "").strip()
    if not key:
        return
    url_key = str(candidate.get("url_key") or _candidate_url_key(candidate) or "")
    title_key = str(candidate.get("title_key") or _candidate_title_key(candidate) or "")
    title_compact = str(candidate.get("title_compact") or _candidate_title_compact(candidate) or "")
    event_key = str(candidate.get("event_key") or _candidate_event_key(candidate) or "")
    event_tokens = _candidate_event_tokens(candidate)
    existing = seen.get(key) if isinstance(seen.get(key), dict) else {}
    seen[key] = {
        **existing,
        "key": key,
        "url_key": url_key,
        "title_key": title_key,
        "title_compact": title_compact,
        "event_key": event_key,
        "event_tokens": event_tokens,
        "title": candidate.get("title_zh") or candidate.get("translated_title") or candidate.get("title") or existing.get("title") or "",
        "url": candidate.get("url") or existing.get("url") or "",
        "first_seen_at": _safe_float(existing.get("first_seen_at"), now) if existing else now,
        "last_seen_at": now,
        "seen_count": int(existing.get("seen_count") or 0) + 1 if existing else 1,
        "batch_id": candidate.get("batch_id") or existing.get("batch_id") or "",
        "remember_reason": reason,
    }
    _write_json(_seen_path(root), seen)


def mark_batch_items(root: Path, item_ids: list[str], updates: dict) -> None:
    wanted = {str(item or "").strip() for item in item_ids if str(item or "").strip()}
    if not wanted:
        return
    _ensure_root(root)
    with _FILE_LOCK:
        for path in sorted(_batches_dir(root).glob("batch_*.json")):
            payload = _read_json(path, {})
            if not isinstance(payload, dict):
                continue
            changed = False
            for item in payload.get("items", []) or []:
                key = str(item.get("batch_item_id") or item.get("id") or "")
                if key in wanted:
                    item.update(updates or {})
                    if str((updates or {}).get("status") or "").lower() in {"completed", "done", "published"}:
                        _remember_candidate_seen_locked(root, item, reason=str((updates or {}).get("status") or "completed"))
                    changed = True
            if changed:
                payload["updated_at"] = time.time()
                _write_json(path, payload)


def create_batch_job(root: Path, *, username: str, items: list[dict], options: dict) -> dict:
    _ensure_root(root)
    job_id = f"opennews_batch_{int(time.time())}_{hashlib.sha1((username + str(time.time())).encode()).hexdigest()[:8]}"
    now = time.time()
    job = {
        "job_id": job_id,
        "username": username,
        "status": "queued",
        "message": "批量生产任务已提交",
        "created_at": now,
        "updated_at": now,
        "options": options,
        "items": [
            {
                "batch_item_id": item.get("batch_item_id") or item.get("id"),
                "title": item.get("title_zh") or item.get("translated_title") or item.get("title") or "OpenNews 新闻",
                "article": item,
                "status": "queued",
                "message": "等待生成",
                "task_id": "",
                "error": "",
            }
            for item in items
        ],
    }
    with _FILE_LOCK:
        _write_json(_job_path(root, job_id), job)
    return job


def update_batch_job(root: Path, job_id: str, updater) -> dict:
    _ensure_root(root)
    with _FILE_LOCK:
        job = _read_json(_job_path(root, job_id), {})
        if not isinstance(job, dict):
            job = {"job_id": job_id, "items": []}
        updater(job)
        job["updated_at"] = time.time()
        _write_json(_job_path(root, job_id), job)
        return job


def load_batch_job(root: Path, job_id: str) -> dict | None:
    _ensure_root(root)
    with _FILE_LOCK:
        job = _read_json(_job_path(root, job_id), None)
    return job if isinstance(job, dict) else None


def list_batch_jobs(root: Path, *, limit: int = 10, username: str = "", include_all: bool = False) -> list[dict]:
    _ensure_root(root)
    with _FILE_LOCK:
        paths = sorted(_jobs_dir(root).glob("opennews_batch_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        jobs: list[dict] = []
        for path in paths:
            payload = _read_json(path, {})
            if not isinstance(payload, dict):
                continue
            if not include_all and username and payload.get("username") != username:
                continue
            jobs.append(payload)
            if len(jobs) >= max(1, min(int(limit or 10), 50)):
                break
        return jobs


def start_batch_scheduler(root: Path, *, poll_seconds: int = 20) -> None:
    global _SCHEDULER_STARTED
    if _SCHEDULER_STARTED:
        return
    _SCHEDULER_STARTED = True
    _ensure_root(root)

    def loop() -> None:
        while True:
            try:
                config = load_batch_config(root)
                if config.get("enabled"):
                    next_run_at = float(config.get("next_run_at") or 0)
                    if not next_run_at or time.time() >= next_run_at:
                        run_batch_fetch_once(root, triggered_by="scheduler")
            except Exception:
                pass
            time.sleep(max(10, int(poll_seconds)))

    threading.Thread(target=loop, name="opennews-batch-scheduler", daemon=True).start()
