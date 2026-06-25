#!/usr/bin/env python3
"""
Harvest image assets for recent OpenNews items into Desktop review folders.

This is intentionally offline-first: it only downloads candidate images to the
local Desktop. Admins can delete bad files by hand, then import the remaining
files with tools/import_harvested_images_to_material_library.py.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from PIL import Image, ImageStat

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from ai_material_harvester import (  # noqa: E402
    NEWS_TOPIC_HARVEST_PRESETS,
    create_harvest_job,
    _fetch_candidate_rows,
    discover_source_urls,
    list_harvest_candidates,
    run_harvest_job,
)
from opennews_material_sources import TOPIC_OFFICIAL_SOURCE_URLS  # noqa: E402
from material_library import MATERIAL_LIBRARY_DIR, list_material_library_items  # noqa: E402
from source_ingest import DEFAULT_HEADERS  # noqa: E402


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
DEFAULT_DESKTOP = Path.home() / "Desktop"
DEFAULT_PROD_BASE_URL = "https://aiagent.office.ihousejapan.cn"


ENTITY_PRESETS = [
    ("spacex", ["spacex", "space x", "starship", "falcon 9", "falcon heavy", "dragon spacecraft", "星舰"], ["SpaceX", "Starship", "Falcon 9 rocket"], []),
    ("tesla", ["tesla", "特斯拉"], ["Tesla", "Tesla factory", "Elon Musk"], []),
    ("xai_grok", ["grok", "xai", "elon musk", "musk", "马斯克"], ["xAI", "Grok", "马斯克"], ["xai_grok_bedrock"]),
    ("amazon_bedrock", ["bedrock", "amazon", "aws"], ["Amazon Bedrock", "AWS"], ["xai_grok_bedrock"]),
    ("openai_chatgpt", ["openai", "chatgpt", "sam altman", "altman"], ["OpenAI", "ChatGPT", "Sam Altman"], ["openai_chatgpt"]),
    ("anthropic_claude", ["anthropic", "claude"], ["Anthropic", "Claude"], ["anthropic_claude"]),
    ("nvidia_huang", ["nvidia", "黄仁勋", "huang", "jensen"], ["Nvidia", "黄仁勋"], ["ai_nvidia_chip"]),
    ("google_gemini", ["google", "gemini", "alphabet"], ["Google", "Gemini"], ["google_gemini_ai"]),
    ("meta_ai", ["meta", "llama", "facebook", "zuckerberg"], ["Meta", "Llama", "扎克伯格"], ["meta_ai"]),
    ("apple_ai", ["apple", "siri", "iphone", "wwdc"], ["Apple", "Siri", "iPhone"], []),
    ("microsoft_copilot", ["microsoft", "copilot", "azure"], ["Microsoft", "Copilot", "Azure"], ["microsoft_copilot_ai"]),
    ("deepseek", ["deepseek"], ["DeepSeek"], ["deepseek_ai"]),
    ("robotics", ["robot", "robotics", "humanoid", "机器人"], ["机器人", "humanoid robot"], ["robotics_humanoid"]),
    ("white_house", ["white house", "trump", "biden", "白宫", "特朗普"], ["White House", "Trump"], ["white_house_us_politics", "trump_us_election"]),
    ("oil_energy", ["oil", "crude", "energy", "opec", "油价", "能源"], ["oil market", "energy infrastructure"], ["oil_energy"]),
    ("fed_markets", ["fed", "federal reserve", "jerome powell", "powell", "美联储", "鲍威尔"], ["Federal Reserve", "Jerome Powell"], ["fed_inflation_markets"]),
    ("real_estate", ["real estate", "housing", "mortgage", "房产", "房地产"], ["real estate", "housing market"], ["real_estate_us_housing"]),
]

EVENT_PRESETS = [
    (
        "stock_down",
        ["stock falls", "stock drops", "shares fall", "shares drop", "stock down", "股价下跌", "股票下跌", "下跌", "走低", "跌破"],
        ["falling stock chart", "red stock market board", "Wall Street trading screen", "stock market decline"],
        ["股价下跌", "股票", "行情屏", "金融市场"],
    ),
    (
        "stock_up",
        ["stock rises", "stock jumps", "shares rise", "shares jump", "stock up", "股价上涨", "上涨", "走高"],
        ["rising stock chart", "green stock market board", "Wall Street trading floor", "stock market rally"],
        ["股价上涨", "股票", "行情屏", "金融市场"],
    ),
    (
        "earnings_finance",
        ["earnings", "revenue", "profit", "ipo", "market value", "估值", "财报", "营收", "利润", "上市"],
        ["financial report chart", "stock exchange trading floor", "business finance newsroom"],
        ["财报", "金融", "股票", "商业"],
    ),
    (
        "rocket_space",
        ["rocket", "launch", "spacecraft", "starship", "falcon", "火箭", "发射", "航天"],
        ["rocket launch", "SpaceX rocket", "spacecraft launch pad", "Starship rocket"],
        ["火箭", "航天", "SpaceX"],
    ),
    (
        "ai_model",
        ["ai model", "large language model", "llm", "人工智能模型", "大模型", "模型发布"],
        ["AI model data center", "artificial intelligence server room", "AI software interface"],
        ["AI", "大模型", "数据中心"],
    ),
]

OFFTOPIC_TERMS = [
    "celebrity", "sports", "football", "baseball", "basketball", "crime scene",
    "wedding", "fashion", "recipe", "travel", "movie", "music",
]


def _slugify(value: str, fallback: str = "news") -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", str(value or "").strip()).strip("_")
    return slug[:88] or fallback


def _safe_suffix(url: str, content_type: str = "") -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in IMAGE_SUFFIXES:
        return suffix
    content_type = str(content_type or "").lower()
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    return ".jpg"


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _existing_material_fingerprints() -> tuple[set[str], set[str]]:
    hashes: set[str] = set()
    source_urls: set[str] = set()
    for item in list_material_library_items():
        source_url = str(item.get("source_url") or "").strip().split("?")[0].lower()
        if source_url:
            source_urls.add(source_url)
        filename = Path(str(item.get("filename") or "")).name
        path = MATERIAL_LIBRARY_DIR / filename
        if path.exists() and path.suffix.lower() in IMAGE_SUFFIXES:
            try:
                hashes.add(_hash_file(path))
            except Exception:
                pass
    return hashes, source_urls


def _image_is_usable(path: Path, *, min_width: int, min_height: int) -> tuple[bool, str, dict]:
    try:
        with Image.open(path) as image:
            image = image.convert("RGB")
            width, height = image.size
            if width < min_width or height < min_height:
                return False, f"too_small:{width}x{height}", {"width": width, "height": height}
            stat = ImageStat.Stat(image.resize((64, 64)))
            mean = sum(stat.mean) / 3
            variance = sum(stat.var) / 3
            if mean > 246 and variance < 22:
                return False, "blank_or_white", {"width": width, "height": height}
            if variance < 8:
                return False, "flat_or_logo_like", {"width": width, "height": height}
            return True, "ok", {"width": width, "height": height}
    except Exception as exc:
        return False, f"invalid_image:{exc}", {"width": 0, "height": 0}


def _download_image(url: str, output_path: Path, *, timeout: int = 35) -> tuple[bool, str, str]:
    try:
        with requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, stream=True, allow_redirects=True) as response:
            response.raise_for_status()
            content_type = str(response.headers.get("content-type") or "").lower()
            if content_type and "image" not in content_type and "octet-stream" not in content_type:
                return False, f"not_image:{content_type}", content_type
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("wb") as handle:
                size = 0
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > 14 * 1024 * 1024:
                        return False, "too_large", content_type
                    handle.write(chunk)
        return True, "downloaded", content_type
    except Exception as exc:
        return False, str(exc), ""


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _item_title(item: dict) -> str:
    return _clean_text(
        item.get("title_zh")
        or item.get("translated_title")
        or item.get("title")
        or item.get("original_title")
        or item.get("english_title")
    )


def _item_english_title(item: dict) -> str:
    return _clean_text(item.get("original_title") or item.get("english_title") or item.get("title") or "")


def _item_summary(item: dict) -> str:
    return _clean_text(
        item.get("summary_zh")
        or item.get("translated_summary")
        or item.get("summary")
        or item.get("description")
        or item.get("content")
    )


def _item_url(item: dict) -> str:
    return str(item.get("url") or item.get("source_url") or "").strip()


def _match_entity_queries(text: str) -> tuple[list[str], list[str]]:
    lowered = text.lower()
    tags: list[str] = []
    queries: list[str] = []
    for _, keywords, labels, _topic_ids in ENTITY_PRESETS:
        if any(keyword.lower() in lowered for keyword in keywords):
            tags.extend(labels)
            queries.extend([f"{label} news photo official press image" for label in labels[:2]])
    return _dedupe(queries), _dedupe(tags)


def _match_event_queries(text: str) -> tuple[list[str], list[str]]:
    lowered = text.lower()
    queries: list[str] = []
    tags: list[str] = []
    for _, keywords, labels, event_tags in EVENT_PRESETS:
        if any(keyword.lower() in lowered for keyword in keywords):
            queries.extend([f"{label} news image b-roll" for label in labels])
            tags.extend(event_tags)
    return _dedupe(queries), _dedupe(tags)


def _match_preset_queries(text: str, category: str) -> tuple[list[str], list[str]]:
    lowered = text.lower()
    queries: list[str] = []
    tags: list[str] = []
    for preset in NEWS_TOPIC_HARVEST_PRESETS:
        haystacks = [
            preset.get("id", ""),
            preset.get("name", ""),
            preset.get("topic", ""),
            " ".join(preset.get("tags") or []),
        ]
        if any(part and str(part).lower() in lowered for part in haystacks):
            queries.append(str(preset.get("topic") or preset.get("name") or ""))
            tags.extend(preset.get("tags") or [])
    return _dedupe([query for query in queries if query]), _dedupe(tags)


def _dedupe(values: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _news_queries(item: dict) -> tuple[list[str], list[str]]:
    title = _item_title(item)
    english_title = _item_english_title(item)
    summary = _item_summary(item)
    category = str(item.get("category") or item.get("batch_category") or "").strip()
    source_name = str(item.get("source_name") or item.get("trend_domain") or "").strip()
    text = " ".join([title, english_title, summary, source_name, category])
    entity_queries, entity_tags = _match_entity_queries(text)
    event_queries, event_tags = _match_event_queries(text)
    preset_queries, preset_tags = _match_preset_queries(text, category)
    base_queries = [
        f"{english_title or title} {source_name} news photo",
        f"{english_title or title} official press image",
    ]
    if entity_queries and event_queries:
        entity_blob = " ".join(entity_tags[:3])
        event_blob = " ".join(event_tags[:3])
        base_queries.insert(1, f"{entity_blob} {event_blob} news visual")
    return _dedupe(base_queries + entity_queries + event_queries + preset_queries)[:12], _dedupe(entity_tags + event_tags + preset_tags + [category])


def _profile_terms(item: dict) -> dict:
    title = _item_title(item)
    english_title = _item_english_title(item)
    summary = _item_summary(item)
    text = " ".join([title, english_title, summary])
    lowered = text.lower()
    entity_terms: list[str] = []
    event_terms: list[str] = []
    tags: list[str] = []
    matched_topic_ids: list[str] = []
    for entity_id, keywords, labels, topic_ids in ENTITY_PRESETS:
        if any(keyword.lower() in lowered for keyword in keywords):
            entity_terms.append(entity_id)
            entity_terms.extend(keywords)
            entity_terms.extend(labels)
            matched_topic_ids.extend(topic_ids)
            tags.extend(labels)
    for _, keywords, labels, event_tags in EVENT_PRESETS:
        if any(keyword.lower() in lowered for keyword in keywords):
            event_terms.extend(keywords)
            event_terms.extend(labels)
            tags.extend(event_tags)
    title_terms = [
        part
        for part in re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", f"{english_title} {title}")
        if len(part) >= 4 and part.lower() not in {"news", "says", "with", "from", "after", "before", "this", "that"}
    ]
    return {
        "entity_terms": _dedupe(entity_terms),
        "event_terms": _dedupe(event_terms),
        "title_terms": _dedupe(title_terms[:14]),
        "tags": _dedupe(tags),
        "official_topic_ids": _dedupe(matched_topic_ids),
    }


def _term_score(blob: str, terms: list[str], weight: int) -> int:
    score = 0
    lowered = blob.lower()
    for term in terms:
        key = str(term or "").strip().lower()
        if not key:
            continue
        if key in lowered:
            score += weight
        else:
            words = [part for part in re.split(r"[^0-9a-z]+", key) if len(part) >= 3]
            if words and sum(1 for word in words if word in lowered) >= min(2, len(words)):
                score += max(2, weight - 2)
    return score


def _candidate_relevance(row: dict, item: dict, profile: dict) -> tuple[int, str]:
    blob = " ".join(
        [
            str(row.get("asset_url") or ""),
            str(row.get("source_url") or ""),
            str(row.get("source_site") or ""),
            str(row.get("page_title") or ""),
            str(row.get("page_excerpt") or ""),
            str(row.get("query") or ""),
        ]
    ).lower()
    score = 0
    score += _term_score(blob, profile.get("entity_terms") or [], 14)
    score += _term_score(blob, profile.get("event_terms") or [], 10)
    score += _term_score(blob, profile.get("title_terms") or [], 4)
    source_url = _item_url(item).strip().lower()
    row_source = str(row.get("source_url") or "").strip().lower()
    if source_url and row_source and source_url.split("?")[0] == row_source.split("?")[0]:
        score += 18
    if str(row.get("source_site") or "").strip() and str(item.get("source_name") or "").strip().lower() in blob:
        score += 4
    if any(term in blob for term in OFFTOPIC_TERMS):
        score -= 16
    reason = f"entity/event/title score={score}"
    return score, reason


def _load_batches_from_local(batch_dir: Path, *, max_batches: int) -> list[dict]:
    if not batch_dir.exists():
        return []
    paths = sorted(batch_dir.glob("batch_*.json"), key=lambda path: path.stat().st_mtime, reverse=True)[:max_batches]
    batches = []
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                batches.append(data)
        except Exception:
            pass
    return batches


def _load_batches_from_api(base_url: str, token: str, *, max_batches: int) -> list[dict]:
    if not base_url or not token:
        return []
    url = f"{base_url.rstrip('/')}/api/external/opennews/candidate-batches"
    response = requests.get(
        url,
        headers={"X-Token": token, **DEFAULT_HEADERS},
        params={"limit": max_batches, "exclude_used": "false"},
        timeout=45,
    )
    response.raise_for_status()
    payload = response.json()
    return list(payload.get("batches") or [])


def load_recent_news_items(args: argparse.Namespace) -> list[dict]:
    batches = []
    batches.extend(_load_batches_from_local(Path(args.batch_dir).expanduser(), max_batches=args.max_batches))
    if not batches and args.base_url:
        batches.extend(_load_batches_from_api(args.base_url, args.token or os.getenv("OPENNEWS_EXTERNAL_TOKEN", ""), max_batches=args.max_batches))

    rows: list[dict] = []
    seen_ids: set[str] = set()
    cutoff = time.time() - max(1, int(args.days)) * 86400
    for batch in batches:
        for item in batch.get("items") or []:
            if not isinstance(item, dict):
                continue
            item_id = str(item.get("id") or item.get("batch_item_id") or item.get("url") or _item_title(item)).strip()
            if item_id in seen_ids:
                continue
            published_ts = _parse_timestamp(item.get("published_at") or item.get("news_time") or item.get("date"))
            if published_ts and published_ts < cutoff:
                continue
            seen_ids.add(item_id)
            rows.append(item)
    rows.sort(key=lambda item: float(item.get("trend_score") or 0), reverse=True)
    return rows[: max(1, int(args.limit_news))]


def _parse_timestamp(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if re.fullmatch(r"\d+(\.\d+)?", text):
        return float(text)
    try:
        from datetime import datetime

        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except Exception:
        return 0.0


def _candidate_key(row: dict) -> str:
    return str(row.get("asset_url") or "").split("?")[0].strip().lower()


def _source_host(value: str) -> str:
    return urlparse(str(value or "")).netloc.lower().replace("www.", "")


def _host_matches(source: str, target: str) -> bool:
    source_host = _source_host(source)
    target_host = _source_host(target)
    if not source_host or not target_host:
        return False
    return source_host == target_host or source_host.endswith(f".{target_host}") or target_host.endswith(f".{source_host}")


def _official_source_urls_for_profile(profile: dict) -> list[str]:
    urls: list[str] = []
    for topic_id in profile.get("official_topic_ids") or []:
        urls.extend(TOPIC_OFFICIAL_SOURCE_URLS.get(str(topic_id), []))
    return _dedupe(urls)


def _source_tier(source: str, query: str, item: dict, profile: dict) -> str:
    original_url = _item_url(item)
    if original_url and str(source or "").split("?")[0].lower() == original_url.split("?")[0].lower():
        return "original_article"
    if original_url and _host_matches(source, original_url):
        return "same_news_domain"
    for official_url in _official_source_urls_for_profile(profile):
        if _host_matches(source, official_url):
            return "entity_official_source"
    if str(query or "") == "manual_broad_search":
        return "broad_search"
    return "untrusted"


def _collect_candidates_for_item(item: dict, *, source_limit: int, query_source_limit: int, crawl_mode: str = "strict") -> list[dict]:
    title = _item_title(item)
    english_title = _item_english_title(item)
    summary = _item_summary(item)
    category = str(item.get("category") or item.get("batch_category") or "").strip()
    source_url = _item_url(item)
    queries, tags = _news_queries(item)
    profile = _profile_terms(item)
    candidates: list[dict] = []
    source_pairs: list[tuple[str, str]] = []
    if source_url:
        source_pairs.append((source_url, "original_news_article"))
        source_host = _source_host(source_url)
        same_site_queries = [
            f'site:{source_host} "{english_title or title}"',
            f"site:{source_host} {' '.join((profile.get('entity_terms') or [])[:4])} {' '.join((profile.get('event_terms') or [])[:3])}".strip(),
        ]
        for query in same_site_queries:
            if not source_host or not query.strip():
                continue
            try:
                for discovered_url in discover_source_urls(query, summary, limit=query_source_limit, category=category):
                    if _host_matches(discovered_url, source_url):
                        source_pairs.append((discovered_url, "same_news_domain_search"))
            except Exception:
                continue
    for official_url in _official_source_urls_for_profile(profile):
        source_pairs.append((official_url, "entity_official_source"))
    if crawl_mode == "broad":
        for query in queries:
            try:
                for discovered_url in discover_source_urls(query, summary, limit=query_source_limit, category=category):
                    source_pairs.append((discovered_url, "manual_broad_search"))
            except Exception:
                continue
            if len(source_pairs) >= source_limit:
                break
    deduped_sources: list[tuple[str, str]] = []
    seen_sources: set[str] = set()
    for source, query in source_pairs:
        source_key = str(source or "").strip().split("?")[0].lower()
        if not source_key or source_key in seen_sources:
            continue
        seen_sources.add(source_key)
        deduped_sources.append((source, query))
        if len(deduped_sources) >= source_limit:
            break
    for source, query in deduped_sources:
        tier = _source_tier(source, query, item, profile)
        if crawl_mode == "strict" and tier not in {"original_article", "same_news_domain", "entity_official_source"}:
            continue
        try:
            rows = _fetch_candidate_rows(title, source, category=category, tags=tags)
        except Exception:
            continue
        for row in rows:
            row["query"] = query
            row["source_tier"] = tier
            row["news_title"] = title
            row["news_summary"] = summary
            row["news_source_url"] = source_url
            row["news_source_name"] = item.get("source_name") or item.get("trend_domain") or ""
            row["news_published_at"] = item.get("published_at") or item.get("news_time") or ""
            row["tags"] = _dedupe(list(row.get("tags") or []) + tags)
            relevance_score, relevance_reason = _candidate_relevance(row, item, profile)
            if tier == "original_article":
                relevance_score += 28
            elif tier == "same_news_domain":
                relevance_score += 18
            elif tier == "entity_official_source":
                relevance_score += 12
            elif tier == "broad_search":
                relevance_score -= 18
            row["relevance_score"] = relevance_score
            row["relevance_reason"] = f"{tier}; {relevance_reason}"
            candidates.append(row)
    seen_assets: set[str] = set()
    host_counts: dict[str, int] = {}
    deduped_rows: list[dict] = []
    candidates.sort(key=lambda row: int(row.get("relevance_score") or 0), reverse=True)
    min_score = 24 if crawl_mode == "strict" else (10 if (profile.get("entity_terms") or profile.get("event_terms")) else 0)
    for row in candidates:
        if str(row.get("kind") or "image") != "image":
            continue
        if int(row.get("relevance_score") or 0) < min_score:
            continue
        if crawl_mode == "strict" and row.get("source_tier") == "entity_official_source":
            blob = " ".join(
                str(row.get(key) or "")
                for key in ("asset_url", "source_url", "source_site", "page_title", "page_excerpt", "query")
            ).lower()
            entity_hits = _term_score(blob, profile.get("entity_terms") or [], 1)
            if entity_hits <= 0:
                continue
        key = _candidate_key(row)
        if not key or key in seen_assets:
            continue
        host = _source_host(row.get("asset_url") or "")
        if host_counts.get(host, 0) >= 7:
            continue
        seen_assets.add(key)
        host_counts[host] = host_counts.get(host, 0) + 1
        deduped_rows.append(row)
    return deduped_rows


def _system_harvest_topic(item: dict) -> str:
    title = _item_title(item)
    english_title = _item_english_title(item)
    summary = _item_summary(item)
    profile = _profile_terms(item)
    entity_bits = " ".join((profile.get("tags") or [])[:6])
    event_bits = " ".join((profile.get("event_terms") or [])[:8])
    parts = [
        english_title or title,
        entity_bits,
        event_bits,
        "news photo official source image",
    ]
    if summary:
        parts.append(summary[:240])
    return _clean_text(" ".join(part for part in parts if part))


def _system_harvest_notes(item: dict) -> str:
    title = _item_title(item)
    summary = _item_summary(item)
    source = str(item.get("source_name") or item.get("trend_domain") or "").strip()
    return _clean_text(
        " ".join(
            part
            for part in [
                "批量补库：优先抓取和这条新闻标题、主体实体、来源网站直接相关的网络图片。",
                "可以多抓候选图给人工审核，宁可多一点，但不要头像、logo、广告、无关缩略图、成人裸露、血腥图片。",
                f"新闻标题：{title}" if title else "",
                f"新闻摘要：{summary[:500]}" if summary else "",
                f"新闻来源：{source}" if source else "",
            ]
            if part
        )
    )


def _candidate_from_system_harvester(candidate: dict, item: dict, profile: dict) -> dict:
    row = dict(candidate or {})
    row["url"] = row.get("asset_url") or row.get("url") or ""
    row["asset_url"] = row.get("asset_url") or row.get("url") or ""
    row["query"] = "system_material_harvester"
    row["source_tier"] = "system_material_harvester"
    row["news_title"] = _item_title(item)
    row["news_summary"] = _item_summary(item)
    row["news_source_url"] = _item_url(item)
    row["news_source_name"] = item.get("source_name") or item.get("trend_domain") or ""
    row["news_published_at"] = item.get("published_at") or item.get("news_time") or ""
    relevance_score, relevance_reason = _candidate_relevance(row, item, profile)
    # Keep the system harvester broad, but still prefer candidates whose own
    # metadata mentions the news entities/events.
    row["relevance_score"] = relevance_score + 10
    row["relevance_reason"] = f"system_material_harvester; {relevance_reason}"
    return row


def _collect_candidates_for_item_system_harvester(item: dict, *, max_candidates: int) -> list[dict]:
    category = str(item.get("category") or item.get("batch_category") or "").strip()
    source_url = _item_url(item)
    profile = _profile_terms(item)
    job = create_harvest_job(
        topic=_system_harvest_topic(item),
        source_text=source_url,
        search_notes=_system_harvest_notes(item),
        category=category,
        created_by_username="desktop_recent_opennews",
        created_by_display_name="桌面近期新闻补库",
    )
    try:
        run_harvest_job(str(job.get("id") or ""))
    except Exception as exc:
        print(f"  ⚠️ 系统素材采集器任务失败：{exc}")
        return []
    candidates = list_harvest_candidates(job_id=str(job.get("id") or ""))
    rows = [_candidate_from_system_harvester(candidate, item, profile) for candidate in candidates]
    rows = [row for row in rows if str(row.get("kind") or "image") == "image" and str(row.get("asset_url") or "").strip()]
    rows.sort(key=lambda row: int(row.get("relevance_score") or 0), reverse=True)
    deduped = []
    seen_assets = set()
    for row in rows:
        key = _candidate_key(row)
        if not key or key in seen_assets:
            continue
        seen_assets.add(key)
        deduped.append(row)
        if len(deduped) >= max_candidates:
            break
    return deduped


def harvest_news_item(
    item: dict,
    output_root: Path,
    index: int,
    args: argparse.Namespace,
    *,
    existing_hashes: set[str],
    existing_source_urls: set[str],
    run_hashes: set[str],
) -> list[dict]:
    title = _item_title(item) or f"news_{index}"
    category = str(item.get("category") or item.get("batch_category") or "新闻").strip() or "新闻"
    news_dir = output_root / f"{index:02d}_{_slugify(category)}_{_slugify(title)}"
    news_dir.mkdir(parents=True, exist_ok=True)
    if args.engine == "system":
        rows = _collect_candidates_for_item_system_harvester(item, max_candidates=max(args.per_news * 4, 40))
    else:
        rows = _collect_candidates_for_item(
            item,
            source_limit=args.source_limit,
            query_source_limit=args.query_source_limit,
            crawl_mode=args.crawl_mode,
        )
    manifest_rows = []
    downloaded = 0
    print(f"\n== {index:02d}. {title} ==")
    print(f"候选：{len(rows)}，目标下载：{args.per_news}")
    for row_index, row in enumerate(rows, start=1):
        if downloaded >= args.per_news:
            break
        asset_url = str(row.get("asset_url") or "").strip()
        source_key = str(row.get("source_url") or "").strip().split("?")[0].lower()
        if source_key and source_key in existing_source_urls:
            status = "skipped"
            reason = "source_url_exists"
            meta = {"width": 0, "height": 0}
            filename = ""
        else:
            suffix = _safe_suffix(asset_url)
            filename = f"{downloaded + 1:03d}_{_slugify(row.get('page_title') or row.get('title') or title, 'image')}{suffix}"
            if len(filename) > 130:
                filename = f"{downloaded + 1:03d}_{_slugify(title, 'image')}{suffix}"
            path = news_dir / filename
            ok, reason, content_type = _download_image(asset_url, path)
            meta = {"width": 0, "height": 0}
            if ok:
                usable, reason, meta = _image_is_usable(path, min_width=args.min_width, min_height=args.min_height)
                ok = usable
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
                print(f"  + {downloaded:02d}/{args.per_news} {filename}")
            else:
                status = "rejected"
                path.unlink(missing_ok=True)
                filename = ""
        manifest_rows.append(
            {
                "topic_id": "recent_opennews",
                "topic_name": title,
                "category": category,
                "status": status,
                "reason": reason,
                "filename": filename,
                "asset_url": asset_url,
                "source_url": row.get("source_url", ""),
                "source_site": row.get("source_site", "") or _source_host(row.get("source_url", "")),
                "page_title": row.get("page_title", "") or row.get("title", ""),
                "page_excerpt": row.get("page_excerpt", ""),
                "query": row.get("query", ""),
                "source_tier": row.get("source_tier", ""),
                "relevance_score": row.get("relevance_score", 0),
                "relevance_reason": row.get("relevance_reason", ""),
                "width": meta.get("width", 0),
                "height": meta.get("height", 0),
                "tags": "、".join(row.get("tags") or []),
                "news_title": title,
                "news_summary": _item_summary(item),
                "news_source_url": _item_url(item),
                "news_source_name": item.get("source_name") or item.get("trend_domain") or "",
                "news_published_at": item.get("published_at") or "",
            }
        )
        if row_index % 30 == 0:
            print(f"  checked {row_index}/{len(rows)} candidates...")
    fieldnames = list(manifest_rows[0].keys()) if manifest_rows else [
        "topic_id", "topic_name", "category", "status", "reason", "filename", "asset_url",
        "source_url", "source_site", "page_title", "page_excerpt", "query", "source_tier", "relevance_score", "relevance_reason",
        "width", "height", "tags",
        "news_title", "news_summary", "news_source_url", "news_source_name", "news_published_at",
    ]
    with (news_dir / "manifest.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest_rows)
    (news_dir / "news.json").write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest recent OpenNews-related image assets to Desktop review folders.")
    parser.add_argument("--output", default="", help="Output folder. Default: Desktop/OpenNews新闻素材补库_TIMESTAMP")
    parser.add_argument("--batch-dir", default=str(ROOT_DIR / "output" / "opennews_batches" / "batches"), help="Local OpenNews batch JSON folder.")
    parser.add_argument("--base-url", default=DEFAULT_PROD_BASE_URL, help="Production base URL for pulling latest batches when local data is absent.")
    parser.add_argument("--token", default="", help="External OpenNews X-Token. Or set OPENNEWS_EXTERNAL_TOKEN.")
    parser.add_argument("--days", type=int, default=2, help="Only include news from the last N days when timestamps are present.")
    parser.add_argument("--max-batches", type=int, default=12, help="Max recent batches to inspect.")
    parser.add_argument("--limit-news", type=int, default=30, help="Max news items to harvest.")
    parser.add_argument("--per-news", type=int, default=24, help="Max images to download per news item.")
    parser.add_argument("--source-limit", type=int, default=18, help="Max source pages per news item.")
    parser.add_argument("--query-source-limit", type=int, default=5, help="Max source pages per generated query.")
    parser.add_argument(
        "--engine",
        choices=["system", "strict"],
        default="system",
        help="system=reuse the same material-library harvester as the web UI; strict=older desktop-only original/same-domain/entity-source collector.",
    )
    parser.add_argument(
        "--crawl-mode",
        choices=["strict", "broad"],
        default="strict",
        help="strict=original article/same domain/entity official sources only; broad=also use general discovered pages.",
    )
    parser.add_argument("--min-width", type=int, default=520, help="Minimum image width.")
    parser.add_argument("--min-height", type=int, default=300, help="Minimum image height.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output).expanduser() if args.output else DEFAULT_DESKTOP / f"OpenNews新闻素材补库_{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    news_items = load_recent_news_items(args)
    if not news_items:
        raise SystemExit("No recent OpenNews items found. Provide --token or make sure local output/opennews_batches/batches exists.")
    existing_hashes, existing_source_urls = _existing_material_fingerprints()
    run_hashes: set[str] = set()
    all_rows: list[dict] = []
    print(f"新闻数：{len(news_items)}，输出：{output_root}")
    for index, item in enumerate(news_items, start=1):
        all_rows.extend(
            harvest_news_item(
                item,
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
        "candidate_count": len(all_rows),
        "downloaded_count": sum(1 for row in all_rows if row.get("status") == "downloaded"),
        "output_root": str(output_root),
    }
    (output_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n完成：{output_root}")
    print(f"下载成功：{summary['downloaded_count']} / 候选记录：{summary['candidate_count']}")
    print("下一步：在桌面删除不想要的图片，然后运行 import_harvested_images_to_material_library.py 导入。")


if __name__ == "__main__":
    main()
