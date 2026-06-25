#!/usr/bin/env python3
"""
Build a reusable OpenNews material bank from recent news topics.

Unlike per-news harvesting, this script first analyzes recent OpenNews batches,
selects high-frequency reusable visual topics, and then downloads topic-level
images to Desktop folders for manual review.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus, unquote, urlparse

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
TOOLS_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from ai_material_harvester import (  # noqa: E402
    CATEGORY_SEED_SOURCE_URLS,
    NEWS_TOPIC_HARVEST_PRESETS,
    _fetch_candidate_rows,
    discover_source_urls,
)
from harvest_recent_opennews_assets_to_desktop import (  # noqa: E402
    DEFAULT_DESKTOP,
    DEFAULT_PROD_BASE_URL,
    _dedupe,
    _download_image,
    _existing_material_fingerprints,
    _hash_file,
    _image_is_usable,
    _item_summary,
    _item_title,
    _safe_suffix,
    _slugify,
    load_recent_news_items,
)
from opennews_material_sources import TOPIC_OFFICIAL_SOURCE_URLS  # noqa: E402
from source_ingest import DEFAULT_HEADERS  # noqa: E402


EVERGREEN_TOPIC_IDS = [
    "ai_nvidia_chip",
    "ai_model_companies",
    "data_center_servers",
    "robotics_humanoid",
    "white_house_us_politics",
    "trump_us_election",
    "military_conflict",
    "middle_east_iran_israel",
    "oil_energy",
    "fed_inflation_markets",
    "real_estate_us_housing",
    "immigration_visa",
    "general_press_briefing",
]

TOPIC_QUERY_EXPANSIONS = {
    "ai_nvidia_chip": [
        "site:nvidia.com newsroom Jensen Huang GPU AI chip photo",
        "Nvidia GPU server rack data center official photo",
        "semiconductor wafer GPU chip data center news photo",
    ],
    "ai_model_companies": [
        "OpenAI Anthropic Google Gemini AI model official product demo",
        "generative AI software interface conference official news photo",
        "AI company office data center product launch photo",
    ],
    "data_center_servers": [
        "data center server racks cooling system official photo",
        "AI infrastructure GPU server room official image",
        "cloud computing server racks power cooling news photo",
    ],
    "robotics_humanoid": [
        "humanoid robot laboratory official press photo",
        "industrial robot arm factory automation official photo",
        "warehouse robotics automation b-roll image",
    ],
    "white_house_us_politics": [
        "White House press briefing room official photo",
        "US Congress government meeting official photo",
        "US politics policy press conference b-roll",
    ],
    "trump_us_election": [
        "Donald Trump policy speech official news photo",
        "Trump campaign rally press conference photo",
        "US election campaign stage news image",
    ],
    "military_conflict": [
        "NATO military exercise warship fighter jet official photo",
        "drone missile defense system military b-roll",
        "army exercise defense news official photo",
    ],
    "middle_east_iran_israel": [
        "Middle East map diplomacy press conference official photo",
        "Strait of Hormuz oil tanker warship news photo",
        "Iran Israel diplomacy military b-roll image",
    ],
    "oil_energy": [
        "crude oil refinery tanker energy market photo",
        "oil price gas station pipeline news photo",
        "OPEC energy infrastructure official image",
    ],
    "fed_inflation_markets": [
        "site:federalreserve.gov Federal Reserve building press conference official photo",
        "Wall Street stock market trading floor red board photo",
        "central bank inflation interest rate trading screen news image",
    ],
    "real_estate_us_housing": [
        "US housing market suburb homes real estate sign photo",
        "apartment building mortgage housing news image",
        "city skyline residential property market b-roll",
    ],
    "immigration_visa": [
        "visa passport airport immigration office news photo",
        "border control government office immigration image",
        "students visa application passport b-roll",
    ],
    "general_press_briefing": [
        "press conference podium official building news photo",
        "newsroom city street data screen b-roll",
        "public statement government press briefing image",
    ],
}

CURATED_PROVIDER_QUERIES = {
    "ai_nvidia_chip": ["Nvidia GPU", "Jensen Huang", "semiconductor wafer", "GPU server"],
    "ai_model_companies": ["OpenAI", "Anthropic Claude", "Google Gemini AI", "generative artificial intelligence"],
    "data_center_servers": ["data center server racks", "cloud data center", "server room cooling"],
    "robotics_humanoid": ["humanoid robot", "industrial robot arm", "warehouse robot"],
    "white_house_us_politics": ["White House press briefing", "United States Congress", "government press conference"],
    "trump_us_election": ["Donald Trump speech", "Donald Trump rally", "Donald Trump press conference"],
    "military_conflict": ["warship", "fighter jet", "military exercise", "military drone", "missile defense"],
    "middle_east_iran_israel": ["Strait of Hormuz oil tanker", "Iran Israel diplomacy", "Middle East map"],
    "oil_energy": ["oil refinery", "crude oil tanker", "oil pipeline", "gas station"],
    "fed_inflation_markets": ["Federal Reserve building", "Jerome Powell", "Wall Street trading floor", "stock market board"],
    "real_estate_us_housing": ["suburban houses", "real estate sign", "apartment building", "mortgage documents"],
    "immigration_visa": ["passport visa", "airport immigration", "border control", "USCIS office"],
    "general_press_briefing": ["press conference podium", "newsroom", "city street", "official building"],
}

NASA_RELEVANT_TOPIC_IDS = {
    "ai_nvidia_chip",
    "data_center_servers",
    "military_conflict",
    "oil_energy",
    "general_press_briefing",
}

TOPIC_STRICT_IDENTITY_TERMS = {
    "ai_nvidia_chip": [["nvidia", "jensen", "huang", "英伟达", "黄仁勋", "黃仁勳"], ["gpu", "semiconductor", "chip", "data center"]],
    "openai_chatgpt": [["openai", "chatgpt", "gpt"]],
    "anthropic_claude": [["anthropic", "claude"]],
    "google_gemini_ai": [["google", "gemini", "deepmind", "alphabet"]],
    "meta_ai": [["meta", "llama", "facebook"]],
    "microsoft_copilot_ai": [["microsoft", "copilot", "azure"]],
    "deepseek_ai": [["deepseek"]],
    "xai_grok_bedrock": [["xai", "x.ai", "grok"], ["amazon", "aws", "bedrock"]],
    "data_center_servers": [["data center", "datacenter", "server", "server rack", "cloud", "gpu", "cooling"]],
    "robotics_humanoid": [["robot", "robotics", "humanoid", "automation", "机器人", "人形机器人"]],
    "white_house_us_politics": [["white house", "白宫", "白宮"], ["congress", "government", "press briefing"]],
    "trump_us_election": [["trump", "donald trump", "特朗普"]],
    "military_conflict": [["military", "defense", "warship", "fighter", "missile", "drone", "nato", "army", "navy", "军", "导弹", "无人机"]],
    "middle_east_iran_israel": [["iran", "israel", "hormuz", "middle east", "伊朗", "以色列", "霍尔木兹"]],
    "oil_energy": [["oil", "crude", "refinery", "tanker", "pipeline", "opec", "energy", "石油", "油价"]],
    "fed_inflation_markets": [["federal reserve", "fed", "powell", "inflation", "interest rate", "wall street", "stock market", "美联储", "股市"]],
    "real_estate_us_housing": [["real estate", "housing", "mortgage", "home", "apartment", "property", "房产", "住宅"]],
    "immigration_visa": [["immigration", "visa", "passport", "airport", "border", "uscis", "dhs", "移民", "签证", "护照"]],
    "general_press_briefing": [["press conference", "press briefing", "podium", "newsroom", "official building", "记者会"]],
}

OFF_TOPIC_HINTS = {
    "avatar",
    "profile",
    "logo",
    "icon",
    "sprite",
    "thumbnail",
    "placeholder",
    "celebrity",
    "sports",
    "football",
    "baseball",
    "basketball",
    "fashion",
    "recipe",
    "wedding",
    "movie",
    "music",
}

BAD_ASSET_URL_HINTS = {
    "avatar",
    "profile",
    "logo",
    "icon",
    "sprite",
    "placeholder",
    "transparent",
    "1x1",
    "pixel",
    "favicon",
    "gravatar",
    "ads",
    "advert",
    "banner",
    "tracking",
}

PREFERRED_IMAGE_HOSTS = {
    "gettyimages",
    "apimages",
    "reuters",
    "bloomberg",
    "afp",
    "epa",
    "shutterstock",
    "alamy",
    "wikimedia",
    "commons.wikimedia",
    "flickr",
    "defense.gov",
    "dvidshub.net",
    "nato.int",
    "whitehouse.gov",
    "federalreserve.gov",
    "nvidia.com",
    "openai.com",
    "google",
    "microsoft.com",
    "redfin.com",
    "zillow.com",
}

TOPIC_ALLOWED_HOST_HINTS = {
    "ai_nvidia_chip": {"nvidia.com", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages", "afp", "shutterstock", "alamy"},
    "ai_model_companies": {"openai.com", "anthropic.com", "blog.google", "googleblog.com", "microsoft.com", "about.meta.com", "wikimedia", "reuters", "bloomberg", "gettyimages"},
    "data_center_servers": {"nvidia.com", "microsoft.com", "google", "amazon", "wikimedia", "reuters", "bloomberg", "gettyimages", "alamy"},
    "robotics_humanoid": {"bostondynamics.com", "unitree", "figure.ai", "tesla.com", "wikimedia", "reuters", "bloomberg", "gettyimages", "alamy"},
    "white_house_us_politics": {"whitehouse.gov", "congress.gov", "state.gov", "gov", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages"},
    "trump_us_election": {"whitehouse.gov", "donaldjtrump.com", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages"},
    "military_conflict": {"defense.gov", "dvidshub.net", "nato.int", "army.mil", "navy.mil", "af.mil", "wikimedia", "reuters", "gettyimages", "apimages"},
    "middle_east_iran_israel": {"defense.gov", "state.gov", "nato.int", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages"},
    "oil_energy": {"energy.gov", "eia.gov", "opec.org", "wikimedia", "reuters", "bloomberg", "gettyimages", "alamy"},
    "fed_inflation_markets": {"federalreserve.gov", "nyse.com", "nasdaq.com", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages"},
    "real_estate_us_housing": {"nar.realtor", "redfin.com", "zillow.com", "wikimedia", "reuters", "bloomberg", "gettyimages", "alamy"},
    "immigration_visa": {"uscis.gov", "dhs.gov", "state.gov", "cbp.gov", "canada.ca", "wikimedia", "reuters", "gettyimages", "alamy"},
    "general_press_briefing": {"whitehouse.gov", "state.gov", "gov", "wikimedia", "reuters", "bloomberg", "gettyimages", "apimages"},
}

TRUSTED_SOURCE_HINTS = {
    "gov",
    "nasa.gov",
    "defense.gov",
    "dvidshub.net",
    "nato.int",
    "whitehouse.gov",
    "state.gov",
    "federalreserve.gov",
    "ecb.europa.eu",
    "nist.gov",
    "energy.gov",
    "nvidia.com",
    "openai.com",
    "anthropic.com",
    "googleblog.com",
    "blog.google",
    "microsoft.com",
    "about.meta.com",
    "nar.realtor",
    "redfin.com",
    "zillow.com",
    "uscis.gov",
    "dhs.gov",
}

TOPIC_SIGNAL_RULES = {
    "ai_nvidia_chip": {
        "positive": ["nvidia", "jensen", "huang", "gpu", "chip", "semiconductor", "data center", "server", "英伟达", "黄仁勋"],
        "negative": ["tesla", "spacex", "robotaxi", "sports", "football"],
        "min_score": 10,
    },
    "ai_model_companies": {
        "positive": ["openai", "anthropic", "claude", "chatgpt", "gemini", "google", "meta", "llama", "microsoft", "copilot", "ai model", "generative ai"],
        "negative": ["tesla", "spacex", "football", "recipe", "fashion"],
        "min_score": 10,
    },
    "data_center_servers": {
        "positive": ["data center", "datacenter", "server", "server rack", "cloud", "gpu", "power", "cooling", "数据中心", "服务器"],
        "negative": ["phone", "smartphone", "tesla", "spacex", "sports"],
        "min_score": 10,
    },
    "robotics_humanoid": {
        "positive": ["robot", "robotics", "humanoid", "automation", "industrial robot", "warehouse", "机器人", "自动化"],
        "negative": ["sports", "movie", "anime", "toy"],
        "min_score": 10,
    },
    "white_house_us_politics": {
        "positive": ["white house", "press briefing", "congress", "government", "president", "policy", "白宫", "国会"],
        "negative": ["sports", "celebrity", "recipe", "fashion"],
        "min_score": 10,
    },
    "trump_us_election": {
        "positive": ["trump", "donald trump", "campaign", "rally", "white house", "election", "特朗普"],
        "negative": ["sports", "celebrity", "recipe"],
        "min_score": 10,
    },
    "military_conflict": {
        "positive": ["military", "defense", "warship", "fighter", "missile", "drone", "army", "navy", "nato", "军", "导弹", "无人机"],
        "negative": ["game", "sports", "movie", "anime"],
        "min_score": 10,
    },
    "middle_east_iran_israel": {
        "positive": ["iran", "israel", "hormuz", "middle east", "tanker", "oil", "diplomacy", "伊朗", "以色列"],
        "negative": ["sports", "celebrity", "recipe"],
        "min_score": 10,
    },
    "oil_energy": {
        "positive": ["oil", "crude", "energy", "refinery", "tanker", "opec", "gas station", "pipeline", "石油", "油价"],
        "negative": ["cooking oil", "essential oil", "sports", "fashion"],
        "min_score": 10,
    },
    "fed_inflation_markets": {
        "positive": ["federal reserve", "fed", "powell", "inflation", "interest rate", "stock market", "wall street", "trading floor", "美联储", "股市"],
        "negative": ["spacex", "tesla", "sports", "football", "recipe"],
        "min_score": 10,
    },
    "real_estate_us_housing": {
        "positive": ["real estate", "housing", "mortgage", "home", "apartment", "property", "residential", "房产", "住宅"],
        "negative": ["sports", "celebrity", "recipe", "hotel travel"],
        "min_score": 10,
    },
    "immigration_visa": {
        "positive": ["immigration", "visa", "passport", "airport", "border", "uscis", "dhs", "移民", "签证", "护照"],
        "negative": ["sports", "celebrity", "recipe"],
        "min_score": 10,
    },
    "general_press_briefing": {
        "positive": ["press conference", "press briefing", "podium", "official building", "newsroom", "city street", "data screen", "记者会"],
        "negative": ["sports", "celebrity", "recipe", "fashion"],
        "min_score": 8,
    },
}


def _news_text(item: dict) -> str:
    return " ".join(
        part
        for part in [
            _item_title(item),
            item.get("original_title") or item.get("english_title") or item.get("title") or "",
            _item_summary(item),
            item.get("category") or "",
            item.get("batch_category") or "",
            item.get("source_name") or "",
        ]
        if str(part or "").strip()
    )


def analyze_topic_bank(news_items: list[dict], *, limit: int, include_evergreen: bool) -> list[dict]:
    rows = []
    texts = [_news_text(item) for item in news_items if isinstance(item, dict)]
    for index, preset in enumerate(NEWS_TOPIC_HARVEST_PRESETS):
        patterns = list(preset.get("patterns") or [])
        examples = []
        hit_count = 0
        for item, text in zip(news_items, texts):
            if patterns and any(re.search(pattern, text, flags=re.I) for pattern in patterns):
                hit_count += 1
                if len(examples) < 5:
                    examples.append(_item_title(item))
        if hit_count <= 0 and not (include_evergreen and preset.get("id") in EVERGREEN_TOPIC_IDS):
            continue
        evergreen_bonus = 25 if preset.get("id") in EVERGREEN_TOPIC_IDS else 0
        score = hit_count * 100 + evergreen_bonus + max(0, 20 - index)
        rows.append(
            {
                "id": preset.get("id", ""),
                "name": preset.get("name", ""),
                "category": preset.get("category", ""),
                "topic": preset.get("topic", ""),
                "notes": preset.get("notes", ""),
                "tags": list(preset.get("tags") or []),
                "patterns": patterns,
                "hit_count": hit_count,
                "score": score,
                "examples": examples,
                "preset": preset,
            }
        )
    rows.sort(key=lambda row: (int(row.get("score") or 0), int(row.get("hit_count") or 0)), reverse=True)
    return rows[: max(1, int(limit or 12))]


def _topic_terms(topic: dict) -> list[str]:
    values = [
        topic.get("name", ""),
        topic.get("category", ""),
        topic.get("topic", ""),
        " ".join(topic.get("tags") or []),
    ]
    terms = []
    for value in values:
        terms.extend(part for part in re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", str(value or "")) if len(part) >= 3)
    return _dedupe(terms)


def _candidate_score(row: dict, topic: dict) -> int:
    blob = " ".join(
        [
            str(row.get("asset_url") or ""),
            str(row.get("source_url") or ""),
            str(row.get("source_site") or ""),
            str(row.get("page_title") or ""),
            str(row.get("image_title") or ""),
            str(row.get("landing_url") or ""),
            str(row.get("page_excerpt") or ""),
            str(row.get("query") or ""),
        ]
    ).lower()
    score = 0
    topic_id = str(topic.get("id") or "")
    rules = TOPIC_SIGNAL_RULES.get(topic_id, {})
    for term in rules.get("positive") or []:
        if str(term).lower() in blob:
            score += 14
    for term in rules.get("negative") or []:
        if str(term).lower() in blob:
            score -= 26
    for term in _topic_terms(topic):
        key = term.lower()
        if key in blob:
            score += 4
    for tag in topic.get("tags") or []:
        if str(tag).lower() in blob:
            score += 6
    if any(hint in blob for hint in OFF_TOPIC_HINTS):
        score -= 20
    source_site = str(row.get("source_site") or "")
    if source_site:
        score += 2
    source_host = urlparse(str(row.get("source_url") or "")).netloc.lower().replace("www.", "")
    asset_host = urlparse(str(row.get("asset_url") or "")).netloc.lower().replace("www.", "")
    landing_host = urlparse(str(row.get("landing_url") or "")).netloc.lower().replace("www.", "")
    if any(hint in source_host or hint in asset_host for hint in TRUSTED_SOURCE_HINTS):
        score += 10
    if any(hint in source_host or hint in asset_host or hint in landing_host for hint in PREFERRED_IMAGE_HOSTS):
        score += 8
    if _candidate_host_allowed_for_topic(row, topic):
        score += 14
    source_type = str(row.get("source_type") or "")
    if source_type in {"wikimedia_commons", "nasa_images"}:
        score += 16
    if source_type == "bing_image_search":
        score += 8
    if str(row.get("query") or "") == "topic_official_source":
        score += 18
    if str(row.get("query") or "") == "category_seed_source":
        score += 4
    if any(hint in str(row.get("asset_url") or "").lower() for hint in BAD_ASSET_URL_HINTS):
        score -= 40
    return score


def _candidate_score_reason(row: dict, topic: dict) -> str:
    blob = " ".join(
        [
            str(row.get("asset_url") or ""),
            str(row.get("source_url") or ""),
            str(row.get("source_site") or ""),
            str(row.get("page_title") or ""),
            str(row.get("image_title") or ""),
            str(row.get("landing_url") or ""),
            str(row.get("page_excerpt") or ""),
            str(row.get("query") or ""),
        ]
    ).lower()
    rules = TOPIC_SIGNAL_RULES.get(str(topic.get("id") or ""), {})
    positives = [term for term in rules.get("positive") or [] if str(term).lower() in blob]
    negatives = [term for term in rules.get("negative") or [] if str(term).lower() in blob]
    host = urlparse(str(row.get("source_url") or row.get("asset_url") or "")).netloc.lower().replace("www.", "")
    trusted = [hint for hint in TRUSTED_SOURCE_HINTS if hint in host][:3]
    allowed = _candidate_host_allowed_for_topic(row, topic)
    return f"positive={','.join(positives[:6])}; negative={','.join(negatives[:4])}; trusted={','.join(trusted)}; allowed_host={allowed}"


def _candidate_has_required_signal(row: dict, topic: dict) -> bool:
    rules = TOPIC_SIGNAL_RULES.get(str(topic.get("id") or ""), {})
    positives = [str(term).lower() for term in (rules.get("positive") or []) if str(term).strip()]
    if not positives:
        return True
    blob = " ".join(
        [
            str(row.get("asset_url") or ""),
            str(row.get("source_url") or ""),
            str(row.get("source_site") or ""),
            str(row.get("page_title") or ""),
            str(row.get("image_title") or ""),
            str(row.get("landing_url") or ""),
            str(row.get("page_excerpt") or ""),
            str(row.get("query") or ""),
        ]
    ).lower()
    return any(term in blob for term in positives)


def _candidate_context_blob(row: dict) -> str:
    return " ".join(
        [
            str(row.get("asset_url") or ""),
            str(row.get("source_url") or ""),
            str(row.get("source_site") or ""),
            str(row.get("page_title") or ""),
            str(row.get("image_title") or ""),
            str(row.get("landing_url") or ""),
            str(row.get("page_excerpt") or ""),
            str(row.get("query") or ""),
        ]
    ).lower()


def _candidate_matches_strict_topic_identity(row: dict, topic: dict) -> bool:
    groups = TOPIC_STRICT_IDENTITY_TERMS.get(str(topic.get("id") or ""), [])
    if not groups:
        return True
    blob = _candidate_context_blob(row)
    # A candidate must hit at least one strong identity group. This is the most
    # important difference from keyword scraping: category similarity is not
    # enough for local library construction.
    return any(any(str(term).lower() in blob for term in group) for group in groups)


def _candidate_host_allowed_for_topic(row: dict, topic: dict) -> bool:
    allowed = TOPIC_ALLOWED_HOST_HINTS.get(str(topic.get("id") or ""), set())
    if not allowed:
        return True
    hosts = [
        urlparse(str(row.get("source_url") or "")).netloc.lower().replace("www.", ""),
        urlparse(str(row.get("asset_url") or "")).netloc.lower().replace("www.", ""),
        urlparse(str(row.get("landing_url") or "")).netloc.lower().replace("www.", ""),
    ]
    return any(hint in host for hint in allowed for host in hosts if host)


def _candidate_has_strong_context(row: dict, topic: dict) -> bool:
    # For direct image search results, require either a topic-allowed host or a
    # title/landing page that clearly contains the topic signal. This blocks
    # generic CDN images whose URL happens to match one weak word.
    if str(row.get("source_type") or "") != "bing_image_search":
        return True
    if _candidate_host_allowed_for_topic(row, topic):
        return True
    title_blob = " ".join(
        [
            str(row.get("image_title") or ""),
            str(row.get("page_title") or ""),
            str(row.get("landing_url") or ""),
            str(row.get("source_site") or ""),
        ]
    ).lower()
    rules = TOPIC_SIGNAL_RULES.get(str(topic.get("id") or ""), {})
    positives = [str(term).lower() for term in (rules.get("positive") or []) if len(str(term).strip()) >= 3]
    return sum(1 for term in positives if term in title_blob) >= 2


def _topic_min_score(topic: dict) -> int:
    topic_id = str(topic.get("id") or "")
    rules = TOPIC_SIGNAL_RULES.get(topic_id, {})
    try:
        return int(rules.get("min_score", 8))
    except Exception:
        return 8


def _is_bad_asset_url(asset_url: str) -> bool:
    lowered = str(asset_url or "").lower()
    if not lowered.startswith(("http://", "https://")):
        return True
    if lowered.startswith("data:"):
        return True
    suffix = Path(urlparse(lowered).path).suffix.lower()
    if suffix in {".svg", ".ico", ".gif"}:
        return True
    return any(hint in lowered for hint in BAD_ASSET_URL_HINTS)


def _downloaded_image_passes_bank_quality(path: Path, meta: dict) -> tuple[bool, str]:
    try:
        size = path.stat().st_size
    except Exception:
        size = 0
    if size and size < 35 * 1024:
        return False, f"too_small_file:{size}"
    width = int(meta.get("width") or 0)
    height = int(meta.get("height") or 0)
    if width and height:
        ratio = width / max(height, 1)
        if ratio > 4.2 or ratio < 0.24:
            return False, f"extreme_aspect:{width}x{height}"
    return True, "ok"


def _decode_bing_image_url(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = text.replace("\\/", "/")
    text = text.encode("utf-8").decode("unicode_escape", errors="ignore")
    return unquote(text).strip()


def _extract_jsonish_field(text: str, field: str) -> str:
    patterns = [
        rf'"{re.escape(field)}"\s*:\s*"([^"]*)"',
        rf"&quot;{re.escape(field)}&quot;\s*:\s*&quot;([^&]*)&quot;",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return _decode_bing_image_url(match.group(1))
    return ""


def _discover_bing_image_candidates(query: str, topic: dict, *, limit: int = 40) -> list[dict]:
    if not str(query or "").strip():
        return []
    search_url = (
        "https://www.bing.com/images/search?"
        f"q={quote_plus(query)}&form=HDRSC2&first=1&tsc=ImageBasicHover&safeSearch=Strict"
    )
    response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=25, allow_redirects=True)
    response.raise_for_status()
    text = response.text or ""
    candidates = []
    seen = set()
    # Bing stores original image URLs in murl fields. This is far cleaner than
    # scraping every image from a news page because it avoids avatars and ads.
    patterns = [
        r'"murl"\s*:\s*"([^"]+)"',
        r"&quot;murl&quot;\s*:\s*&quot;([^&]+)&quot;",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            asset_url = _decode_bing_image_url(match.group(1))
            key = asset_url.split("?")[0].lower()
            if not key or key in seen or _is_bad_asset_url(asset_url):
                continue
            seen.add(key)
            window = text[max(0, match.start() - 900): min(len(text), match.end() + 1800)]
            image_title = _extract_jsonish_field(window, "t") or query
            landing_url = _extract_jsonish_field(window, "purl")
            if landing_url and _is_bad_asset_url(landing_url):
                landing_url = ""
            host = urlparse(asset_url).netloc.lower().replace("www.", "")
            landing_host = urlparse(landing_url).netloc.lower().replace("www.", "")
            row = {
                "topic": topic.get("topic") or topic.get("name") or "",
                "category": topic.get("category") or "",
                "tags": list(topic.get("tags") or []),
                "kind": "image",
                "title": topic.get("name") or query,
                "page_title": image_title,
                "image_title": image_title,
                "page_excerpt": f"Direct image search result for {query}",
                "source_url": landing_url or search_url,
                "landing_url": landing_url,
                "asset_url": asset_url,
                "source_site": landing_host or host,
                "source_type": "bing_image_search",
                "safety_status": "needs_review",
                "status": "pending",
                "query": query,
            }
            row["relevance_score"] = _candidate_score(row, topic)
            row["score_reason"] = _candidate_score_reason(row, topic)
            candidates.append(row)
            if len(candidates) >= limit:
                return candidates
    return candidates


def _strip_markup(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _curated_provider_row(
    topic: dict,
    *,
    asset_url: str,
    source_url: str,
    source_site: str,
    source_type: str,
    title: str,
    query: str,
    excerpt: str = "",
) -> dict:
    row = {
        "topic": topic.get("topic") or topic.get("name") or "",
        "category": topic.get("category") or "",
        "tags": list(topic.get("tags") or []),
        "kind": "image",
        "title": topic.get("name") or query,
        "page_title": title or query,
        "image_title": title or query,
        "page_excerpt": excerpt or f"Curated provider result for {query}",
        "source_url": source_url,
        "landing_url": source_url,
        "asset_url": asset_url,
        "source_site": source_site,
        "source_type": source_type,
        "safety_status": "needs_review",
        "status": "pending",
        "query": query,
    }
    row["relevance_score"] = _candidate_score(row, topic)
    row["score_reason"] = _candidate_score_reason(row, topic)
    return row


def _search_wikimedia_commons(query: str, topic: dict, *, limit: int = 30) -> list[dict]:
    if not str(query or "").strip():
        return []
    params = {
        "action": "query",
        "generator": "search",
        "gsrnamespace": "6",
        "gsrsearch": query,
        "gsrlimit": str(max(1, min(limit, 50))),
        "prop": "imageinfo",
        "iiprop": "url|mime|size|extmetadata",
        "format": "json",
        "origin": "*",
    }
    response = requests.get("https://commons.wikimedia.org/w/api.php", params=params, headers=DEFAULT_HEADERS, timeout=25)
    response.raise_for_status()
    pages = (response.json().get("query") or {}).get("pages") or {}
    rows = []
    for page in pages.values():
        info = (page.get("imageinfo") or [{}])[0]
        asset_url = str(info.get("url") or "")
        if _is_bad_asset_url(asset_url):
            continue
        metadata = info.get("extmetadata") or {}
        title = _strip_markup((metadata.get("ObjectName") or {}).get("value") or page.get("title") or query)
        desc = _strip_markup((metadata.get("ImageDescription") or {}).get("value") or "")
        source_url = str(info.get("descriptionurl") or asset_url)
        rows.append(
            _curated_provider_row(
                topic,
                asset_url=asset_url,
                source_url=source_url,
                source_site="commons.wikimedia.org",
                source_type="wikimedia_commons",
                title=title,
                query=query,
                excerpt=desc[:500],
            )
        )
    return rows


def _nasa_original_asset_url(collection_url: str) -> str:
    if not collection_url:
        return ""
    response = requests.get(collection_url, headers=DEFAULT_HEADERS, timeout=20)
    response.raise_for_status()
    assets = response.json()
    if not isinstance(assets, list):
        return ""
    image_assets = [
        str(asset)
        for asset in assets
        if str(asset).lower().split("?")[0].endswith((".jpg", ".jpeg", ".png", ".webp"))
    ]
    for needle in ("~orig", "~large", "~medium"):
        for asset in image_assets:
            if needle in asset.lower():
                return asset
    return image_assets[0] if image_assets else ""


def _search_nasa_images(query: str, topic: dict, *, limit: int = 20) -> list[dict]:
    if str(topic.get("id") or "") not in NASA_RELEVANT_TOPIC_IDS:
        return []
    response = requests.get(
        "https://images-api.nasa.gov/search",
        params={"q": query, "media_type": "image", "page_size": str(max(1, min(limit, 50)))},
        headers=DEFAULT_HEADERS,
        timeout=25,
    )
    response.raise_for_status()
    items = ((response.json().get("collection") or {}).get("items") or [])[:limit]
    rows = []
    for item in items:
        data = (item.get("data") or [{}])[0]
        title = _strip_markup(data.get("title") or query)
        desc = _strip_markup(data.get("description") or "")
        asset_url = ""
        try:
            asset_url = _nasa_original_asset_url(str(item.get("href") or ""))
        except Exception:
            asset_url = ""
        if not asset_url:
            links = item.get("links") or []
            asset_url = str((links[0] if links else {}).get("href") or "")
        if _is_bad_asset_url(asset_url):
            continue
        rows.append(
            _curated_provider_row(
                topic,
                asset_url=asset_url,
                source_url=str(item.get("href") or asset_url),
                source_site="images.nasa.gov",
                source_type="nasa_images",
                title=title,
                query=query,
                excerpt=desc[:500],
            )
        )
    return rows


def _curated_topic_queries(topic: dict) -> list[str]:
    topic_id = str(topic.get("id") or "")
    queries = list(CURATED_PROVIDER_QUERIES.get(topic_id, []))
    if not queries:
        queries = [
            str(topic.get("name") or ""),
            str(topic.get("topic") or ""),
            " ".join(topic.get("tags") or []),
        ]
    return _dedupe([query for query in queries if str(query or "").strip()])


def _topic_queries(topic: dict) -> list[str]:
    topic_id = str(topic.get("id") or "")
    queries = [
        str(topic.get("topic") or ""),
        f"{topic.get('name')} official news photo b-roll",
        f"{' '.join(topic.get('tags') or [])} news photo official image",
    ]
    queries.extend(TOPIC_QUERY_EXPANSIONS.get(topic_id, []))
    return _dedupe([query for query in queries if query])


def _collect_topic_candidates(topic: dict, *, source_limit: int, query_source_limit: int, source_mode: str = "curated") -> list[dict]:
    category = str(topic.get("category") or "")
    candidates = []
    mode = str(source_mode or "curated").strip().lower()

    if mode in {"curated", "mixed"}:
        for query in _curated_topic_queries(topic):
            try:
                candidates.extend(_search_wikimedia_commons(query, topic, limit=max(12, query_source_limit * 6)))
            except Exception:
                continue
            try:
                candidates.extend(_search_nasa_images(query, topic, limit=max(8, query_source_limit * 4)))
            except Exception:
                continue

    # Optional broad search. Keep this out of the default path because it is the
    # main source of noisy celebrity, article-thumbnail, and unrelated CDN images.
    if mode in {"mixed", "bing"}:
        for query in _topic_queries(topic)[:5]:
            try:
                candidates.extend(_discover_bing_image_candidates(query, topic, limit=max(12, query_source_limit * 8)))
            except Exception:
                continue

    sources: list[tuple[str, str]] = []
    for source_url in TOPIC_OFFICIAL_SOURCE_URLS.get(str(topic.get("id") or ""), []):
        sources.append((source_url, "topic_official_source"))
    if mode in {"mixed", "bing"}:
        for source_url in CATEGORY_SEED_SOURCE_URLS.get(category, [])[:4]:
            sources.append((source_url, "category_seed_source"))
    if mode in {"mixed", "bing"}:
        for query in _topic_queries(topic):
            try:
                for discovered_url in discover_source_urls(query, str(topic.get("notes") or ""), limit=query_source_limit, category=category):
                    sources.append((discovered_url, query))
            except Exception:
                continue
            if len(sources) >= source_limit:
                break
    deduped_sources = []
    seen_sources = set()
    for source_url, query in sources:
        key = str(source_url or "").split("?")[0].lower()
        if not key or key in seen_sources:
            continue
        seen_sources.add(key)
        deduped_sources.append((source_url, query))
        if len(deduped_sources) >= source_limit:
            break
    for source_url, query in deduped_sources:
        try:
            rows = _fetch_candidate_rows(
                str(topic.get("topic") or topic.get("name") or ""),
                source_url,
                category=category,
                tags=list(topic.get("tags") or []),
            )
        except Exception:
            continue
        for row in rows:
            if _is_bad_asset_url(str(row.get("asset_url") or "")):
                continue
            row["query"] = query
            row["topic_id"] = topic.get("id", "")
            row["topic_name"] = topic.get("name", "")
            row["relevance_score"] = _candidate_score(row, topic)
            row["score_reason"] = _candidate_score_reason(row, topic)
            candidates.append(row)
    candidates.sort(key=lambda row: int(row.get("relevance_score") or 0), reverse=True)
    deduped = []
    seen_assets = set()
    host_counts: dict[str, int] = {}
    for row in candidates:
        if str(row.get("kind") or "image") != "image":
            continue
        key = str(row.get("asset_url") or "").split("?")[0].lower()
        if not key or key in seen_assets:
            continue
        host = urlparse(str(row.get("asset_url") or "")).netloc.lower()
        if host_counts.get(host, 0) >= 10:
            continue
        if int(row.get("relevance_score") or 0) < _topic_min_score(topic):
            continue
        if not _candidate_has_required_signal(row, topic):
            continue
        if not _candidate_matches_strict_topic_identity(row, topic):
            continue
        if not _candidate_has_strong_context(row, topic):
            continue
        seen_assets.add(key)
        host_counts[host] = host_counts.get(host, 0) + 1
        deduped.append(row)
    return deduped


def harvest_topic(
    topic: dict,
    output_root: Path,
    index: int,
    args: argparse.Namespace,
    *,
    existing_hashes: set[str],
    existing_source_urls: set[str],
    run_hashes: set[str],
) -> list[dict]:
    topic_dir = output_root / f"{index:02d}_{_slugify(topic.get('category') or 'news')}_{_slugify(topic.get('name') or topic.get('id') or 'topic')}"
    topic_dir.mkdir(parents=True, exist_ok=True)
    rows = _collect_topic_candidates(
        topic,
        source_limit=args.source_limit,
        query_source_limit=args.query_source_limit,
        source_mode=args.source_mode,
    )
    manifest_rows = []
    downloaded = 0
    print(f"\n== {index:02d}. {topic.get('name')} ==")
    print(f"命中新闻：{topic.get('hit_count', 0)}，候选：{len(rows)}，目标下载：{args.per_topic}")
    for row_index, row in enumerate(rows, start=1):
        if downloaded >= args.per_topic:
            break
        asset_url = str(row.get("asset_url") or "").strip()
        source_key = str(row.get("source_url") or "").strip().split("?")[0].lower()
        if source_key and source_key in existing_source_urls:
            status = "skipped"
            reason = "source_url_exists"
            filename = ""
            meta = {"width": 0, "height": 0}
        else:
            suffix = _safe_suffix(asset_url)
            filename = f"{downloaded + 1:03d}_{_slugify(row.get('page_title') or row.get('title') or topic.get('name'), 'image')}{suffix}"
            if len(filename) > 130:
                filename = f"{downloaded + 1:03d}_{_slugify(topic.get('name'), 'image')}{suffix}"
            path = topic_dir / filename
            ok, reason, _ = _download_image(asset_url, path)
            meta = {"width": 0, "height": 0}
            if ok:
                usable, reason, meta = _image_is_usable(path, min_width=args.min_width, min_height=args.min_height)
                ok = usable
            if ok:
                quality_ok, quality_reason = _downloaded_image_passes_bank_quality(path, meta)
                if not quality_ok:
                    ok = False
                    reason = quality_reason
            if ok:
                file_hash = _hash_file(path)
                if file_hash in existing_hashes:
                    path.unlink(missing_ok=True)
                    ok = False
                    reason = "file_hash_exists_in_material_library"
                elif file_hash in run_hashes:
                    path.unlink(missing_ok=True)
                    ok = False
                    reason = "duplicate_in_this_run"
                else:
                    run_hashes.add(file_hash)
            if ok:
                status = "downloaded"
                downloaded += 1
                print(f"  + {downloaded:02d}/{args.per_topic} score={row.get('relevance_score', 0)} {filename}")
            else:
                status = "rejected"
                path.unlink(missing_ok=True)
                filename = ""
        manifest_rows.append(
            {
                "topic_id": topic.get("id", ""),
                "topic_name": topic.get("name", ""),
                "category": topic.get("category", ""),
                "status": status,
                "reason": reason,
                "filename": filename,
                "asset_url": asset_url,
                "source_url": row.get("source_url", ""),
                "source_site": row.get("source_site", ""),
                "page_title": row.get("page_title", ""),
                "image_title": row.get("image_title", ""),
                "landing_url": row.get("landing_url", ""),
                "page_excerpt": row.get("page_excerpt", ""),
                "query": row.get("query", ""),
                "relevance_score": row.get("relevance_score", 0),
                "score_reason": row.get("score_reason", ""),
                "allowed_host": _candidate_host_allowed_for_topic(row, topic),
                "width": meta.get("width", 0),
                "height": meta.get("height", 0),
                "tags": "、".join(topic.get("tags") or []),
                "topic_hit_count": topic.get("hit_count", 0),
                "topic_examples": " || ".join(topic.get("examples") or []),
            }
        )
        if row_index % 40 == 0:
            print(f"  checked {row_index}/{len(rows)} candidates...")
    fieldnames = list(manifest_rows[0].keys()) if manifest_rows else [
        "topic_id", "topic_name", "category", "status", "reason", "filename", "asset_url",
        "source_url", "source_site", "page_title", "image_title", "landing_url", "page_excerpt",
        "query", "relevance_score", "score_reason", "allowed_host",
        "width", "height", "tags", "topic_hit_count", "topic_examples",
    ]
    with (topic_dir / "manifest.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest_rows)
    (topic_dir / "topic.json").write_text(json.dumps(topic, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return manifest_rows


def write_analysis_report(output_root: Path, topics: list[dict], news_items: list[dict]) -> None:
    csv_path = output_root / "analysis_report.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["rank", "topic_id", "topic_name", "category", "hit_count", "score", "examples", "topic"],
        )
        writer.writeheader()
        for index, topic in enumerate(topics, start=1):
            writer.writerow(
                {
                    "rank": index,
                    "topic_id": topic.get("id", ""),
                    "topic_name": topic.get("name", ""),
                    "category": topic.get("category", ""),
                    "hit_count": topic.get("hit_count", 0),
                    "score": topic.get("score", 0),
                    "examples": " || ".join(topic.get("examples") or []),
                    "topic": topic.get("topic", ""),
                }
            )
    (output_root / "analysis_report.json").write_text(
        json.dumps(
            {
                "created_at": time.time(),
                "news_count": len(news_items),
                "topics": [
                    {
                        key: value
                        for key, value in topic.items()
                        if key not in {"preset", "patterns"}
                    }
                    for topic in topics
                ],
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze recent OpenNews batches and harvest reusable topic-bank images to Desktop.")
    parser.add_argument("--output", default="", help="Output folder. Default: Desktop/OpenNews高频专题素材补库_TIMESTAMP")
    parser.add_argument("--batch-dir", default=str(ROOT_DIR / "output" / "opennews_batches" / "batches"), help="Local OpenNews batch JSON folder.")
    parser.add_argument("--base-url", default=DEFAULT_PROD_BASE_URL, help="Production base URL for pulling latest batches when local data is absent.")
    parser.add_argument("--token", default="", help="External OpenNews X-Token. Or set OPENNEWS_EXTERNAL_TOKEN.")
    parser.add_argument("--days", type=int, default=7, help="Analyze news from the last N days when timestamps are present.")
    parser.add_argument("--max-batches", type=int, default=36, help="Max recent batches to inspect.")
    parser.add_argument("--limit-news", type=int, default=240, help="Max news items to analyze.")
    parser.add_argument("--max-topics", type=int, default=14, help="Max reusable topics to harvest.")
    parser.add_argument("--per-topic", type=int, default=45, help="Max images to download per selected topic.")
    parser.add_argument("--source-limit", type=int, default=22, help="Max source pages per topic.")
    parser.add_argument("--query-source-limit", type=int, default=5, help="Max source pages per generated query.")
    parser.add_argument(
        "--source-mode",
        choices=["curated", "mixed", "bing"],
        default="curated",
        help="curated=structured/official sources only; mixed=curated plus broad web search; bing=legacy broad image/page search.",
    )
    parser.add_argument("--min-width", type=int, default=560, help="Minimum image width.")
    parser.add_argument("--min-height", type=int, default=320, help="Minimum image height.")
    parser.add_argument("--no-evergreen", action="store_true", help="Do not fill with evergreen high-frequency topics when recent hits are thin.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output).expanduser() if args.output else DEFAULT_DESKTOP / f"OpenNews高频专题素材补库_{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    news_items = load_recent_news_items(args)
    if not news_items:
        raise SystemExit("No recent OpenNews items found. Provide --token or make sure local output/opennews_batches/batches exists.")
    topics = analyze_topic_bank(news_items, limit=args.max_topics, include_evergreen=not args.no_evergreen)
    write_analysis_report(output_root, topics, news_items)
    print(f"分析新闻：{len(news_items)} 条，选中专题：{len(topics)} 个，输出：{output_root}")
    for index, topic in enumerate(topics, start=1):
        print(f"{index:02d}. {topic.get('name')} hit={topic.get('hit_count')} score={topic.get('score')}")

    existing_hashes, existing_source_urls = _existing_material_fingerprints()
    run_hashes: set[str] = set()
    all_rows: list[dict] = []
    for index, topic in enumerate(topics, start=1):
        all_rows.extend(
            harvest_topic(
                topic,
                output_root,
                index,
                args,
                existing_hashes=existing_hashes,
                existing_source_urls=existing_source_urls,
                run_hashes=run_hashes,
            )
        )
    if all_rows:
        with (output_root / "manifest_all.csv").open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
    summary = {
        "created_at": time.time(),
        "news_count": len(news_items),
        "topic_count": len(topics),
        "candidate_count": len(all_rows),
        "downloaded_count": sum(1 for row in all_rows if row.get("status") == "downloaded"),
        "output_root": str(output_root),
    }
    (output_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n完成：{output_root}")
    print(f"下载成功：{summary['downloaded_count']} / 候选记录：{summary['candidate_count']}")
    print("先看 analysis_report.csv，再在各专题文件夹里删掉不满意图片，最后导入素材库。")


if __name__ == "__main__":
    main()
