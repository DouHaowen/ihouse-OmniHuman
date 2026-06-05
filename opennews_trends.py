"""
OpenNews 英文热点雷达。

第一版使用 GDELT Doc API 发现英文圈热点，再包装成 OpenNews article 结构，
方便复用现有新闻稿生成和素材成片流程。
"""

from __future__ import annotations

import html
import json
import os
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from urllib.parse import quote_plus, urlparse

import requests
from xml.etree import ElementTree as ET


GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
BING_NEWS_RSS_URL = "https://www.bing.com/news/search"
NEWSDATA_LATEST_URL = "https://newsdata.io/api/1/latest"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_HEADERS = {
    "User-Agent": "iHouse-OpenNews-Trends/0.1 (+https://aiagent.office.ihousejapan.cn)",
}
GDELT_COOLDOWN_SECONDS = 15 * 60
GDELT_MIN_REQUEST_INTERVAL_SECONDS = 6.0
GDELT_CACHE_SECONDS = 10 * 60
_GDELT_DISABLED_UNTIL = 0.0
_GDELT_LAST_REQUEST_AT = 0.0
_GDELT_CACHE: dict[str, tuple[float, list[dict]]] = {}


@dataclass(frozen=True)
class TrendCategory:
    id: str
    name: str
    query: str


TREND_CATEGORIES: list[TrendCategory] = [
    TrendCategory("all", "全部", ""),
    TrendCategory("military", "军事类", "military OR defense OR missile OR drone OR navy OR air force OR Taiwan Strait OR Ukraine"),
    TrendCategory("politics", "政治类", "White House OR Congress OR election OR government OR foreign policy OR sanctions"),
    TrendCategory("technology", "科技类", "AI OR semiconductor OR chip OR Nvidia OR OpenAI OR Apple OR Tesla"),
    TrendCategory("finance", "金融类", "Federal Reserve OR interest rates OR inflation OR stocks OR market OR oil OR dollar"),
    TrendCategory("ai", "AI", "artificial intelligence OR generative AI OR OpenAI OR Anthropic OR Nvidia"),
    TrendCategory("society", "社会类", "protest OR crime OR disaster OR health OR education"),
]

BING_TREND_QUERIES = {
    "all": "breaking news latest",
    "military": "defense military latest news",
    "politics": "White House politics latest news",
    "technology": "AI technology semiconductor latest news",
    "finance": "markets finance economy latest news",
    "ai": "AI Nvidia OpenAI latest news",
    "society": "world society latest news",
}

NEWSDATA_CATEGORY_MAP = {
    "military": "politics",
    "politics": "politics",
    "technology": "technology",
    "finance": "business",
    "ai": "technology",
    "society": "world",
}

NEWSDATA_QUERY_MAP = {
    "all": "breaking news",
    "military": "military defense Ukraine Taiwan Strait",
    "politics": "White House Congress government sanctions",
    "technology": "artificial intelligence semiconductor technology",
    "finance": "Federal Reserve markets inflation economy",
    "ai": "artificial intelligence OpenAI Nvidia",
    "society": "protest disaster health education",
}

GDELT_SAFE_TERM_REPLACEMENTS = {
    "AI": "artificial intelligence",
    "US": "United States",
    "UK": "United Kingdom",
}

TIME_RANGE_OPTIONS = [
    {"id": "1h", "name": "最近1小时", "hours": 1},
    {"id": "6h", "name": "最近6小时", "hours": 6},
    {"id": "24h", "name": "最近24小时", "hours": 24},
]

AUTHORITATIVE_DOMAINS = {
    "reuters.com": 1.6,
    "apnews.com": 1.55,
    "bbc.com": 1.45,
    "bbc.co.uk": 1.45,
    "cnn.com": 1.25,
    "cnbc.com": 1.25,
    "bloomberg.com": 1.45,
    "wsj.com": 1.35,
    "ft.com": 1.35,
    "nytimes.com": 1.3,
    "washingtonpost.com": 1.25,
    "theguardian.com": 1.2,
    "whitehouse.gov": 1.7,
    "state.gov": 1.55,
    "defense.gov": 1.6,
    "dvidshub.net": 1.45,
    "pacom.mil": 1.45,
    "nasa.gov": 1.45,
    "sec.gov": 1.45,
    "federalreserve.gov": 1.45,
}


def trend_category_payloads() -> list[dict]:
    return [{"id": item.id, "name": item.name} for item in TREND_CATEGORIES]


def trend_time_range_payloads() -> list[dict]:
    return list(TIME_RANGE_OPTIONS)


def _strip_tags(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value or "", flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _category_by_id(category_id: str) -> TrendCategory:
    category_id = (category_id or "all").strip() or "all"
    return next((item for item in TREND_CATEGORIES if item.id == category_id), TREND_CATEGORIES[0])


def _hours_for_range(range_id: str) -> int:
    range_id = (range_id or "6h").strip().lower()
    option = next((item for item in TIME_RANGE_OPTIONS if item["id"] == range_id), None)
    return int(option["hours"] if option else 6)


def _parse_gdelt_timestamp(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(value[:14] if fmt.startswith("%Y%m") else value[:19], fmt.replace("%z", ""))
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
    return 0.0


def _format_ts(ts: float) -> str:
    if not ts:
        return ""
    jst = timezone(timedelta(hours=9))
    return datetime.fromtimestamp(ts, jst).strftime("%Y-%m-%d %H:%M JST")


def _domain(url: str) -> str:
    host = urlparse(url or "").netloc.lower()
    return host[4:] if host.startswith("www.") else host


def _normalize_title(title: str) -> str:
    title = _strip_tags(title or "").lower()
    title = re.sub(r"\s*[-|｜_]\s*[^-|｜_]{2,24}$", "", title)
    title = re.sub(r"[^\w]+", "", title)
    return title[:100]


def _title_tokens(title: str) -> set[str]:
    raw = _strip_tags(title or "").lower()
    generic = {
        "says", "said", "news", "latest", "update", "video", "photo", "after", "over",
        "from", "with", "that", "this", "will", "could", "about", "more", "report",
    }
    return {token for token in re.findall(r"[a-z0-9]{3,}", raw) if token not in generic}


def _titles_similar(left: str, right: str) -> bool:
    lk = _normalize_title(left)
    rk = _normalize_title(right)
    if not lk or not rk:
        return False
    if lk in rk or rk in lk:
        return min(len(lk), len(rk)) >= 16
    if SequenceMatcher(None, lk, rk).ratio() >= 0.62:
        return True
    lt = _title_tokens(left)
    rt = _title_tokens(right)
    if len(lt) < 3 or len(rt) < 3:
        return False
    overlap = len(lt & rt)
    return overlap >= 3 and overlap / max(1, min(len(lt), len(rt))) >= 0.6


def _article_from_gdelt(raw: dict, category: TrendCategory) -> dict:
    title = _strip_tags(str(raw.get("title") or ""))[:240]
    url = str(raw.get("url") or "").strip()
    domain = _domain(url)
    seendate = str(raw.get("seendate") or raw.get("seenDate") or raw.get("date") or "")
    published_ts = _parse_gdelt_timestamp(seendate)
    source_name = _strip_tags(str(raw.get("sourceCommonName") or raw.get("domain") or domain or "GDELT"))[:90]
    image_url = str(raw.get("socialimage") or raw.get("image") or "").strip()
    return {
        "id": f"gdelt_{uuid.uuid5(uuid.NAMESPACE_URL, url or title).hex[:12]}",
        "source_id": "gdelt",
        "source_name": source_name,
        "category": category.id if category.id != "all" else "politics",
        "category_name": category.name if category.id != "all" else "英文热点",
        "title": title,
        "url": url,
        "summary": _strip_tags(str(raw.get("snippet") or raw.get("description") or ""))[:420],
        "published_at": _format_ts(published_ts) or seendate,
        "published_ts": published_ts,
        "license": "来源网站原文，请审核后使用",
        "content_type": "GDELT 英文新闻热点",
        "is_latest": bool(published_ts and time.time() - published_ts <= 24 * 3600),
        "trend_source": "GDELT",
        "trend_domain": domain,
        "image": image_url,
        "related_articles": [],
    }


def _article_from_newsdata(raw: dict, category: TrendCategory) -> dict:
    title = _strip_tags(str(raw.get("title") or ""))[:240]
    url = str(raw.get("link") or raw.get("url") or "").strip()
    domain = _domain(url)
    published_at = _strip_tags(str(raw.get("pubDate") or raw.get("pubdate") or raw.get("published_at") or ""))
    published_ts = _parse_newsdata_timestamp(published_at)
    source_name = _strip_tags(str(raw.get("source_name") or raw.get("source_id") or domain or "NewsData.io"))[:90]
    summary = _strip_tags(str(raw.get("description") or raw.get("content") or ""))[:520]
    image_url = str(raw.get("image_url") or "").strip()
    video_url = str(raw.get("video_url") or "").strip()
    return {
        "id": f"newsdata_{str(raw.get('article_id') or uuid.uuid5(uuid.NAMESPACE_URL, url or title).hex)[:16]}",
        "source_id": "newsdata",
        "source_name": source_name,
        "category": category.id if category.id != "all" else "politics",
        "category_name": category.name if category.id != "all" else "英文热点",
        "title": title,
        "url": url,
        "summary": summary,
        "published_at": _format_ts(published_ts) or published_at,
        "published_ts": published_ts,
        "license": "NewsData.io 来源聚合，请审核原站授权",
        "content_type": "NewsData.io 英文热点",
        "is_latest": bool(published_ts and time.time() - published_ts <= 24 * 3600),
        "trend_source": "NewsData.io",
        "trend_domain": domain,
        "image": image_url,
        "video_url": video_url,
        "related_articles": [],
    }


def _parse_newsdata_timestamp(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            if "%z" in fmt:
                return datetime.strptime(value, fmt).timestamp()
            dt = datetime.strptime(value[:19] if "%H" in fmt else value[:10], fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
    return 0.0


def _build_query(category: TrendCategory, keyword: str) -> str:
    keyword = _strip_tags(keyword or "")
    parts = []
    if category.query:
        parts.append(f"({category.query})")
    if keyword:
        # GDELT 只允许括号包 OR 语句，普通关键词不要额外加括号。
        parts.append(keyword)
    query = " ".join(parts).strip() or "breaking news"
    if "sourcelang:" not in query.lower():
        query = f"{query} sourcelang:English"
    return query


def _gdelt_safe_query(query: str) -> str:
    """GDELT rejects very short terms like AI, so expand common short forms."""
    query = query or ""
    for short, replacement in GDELT_SAFE_TERM_REPLACEMENTS.items():
        query = re.sub(rf"\b{re.escape(short)}\b", replacement, query, flags=re.I)
    return query


def _fetch_gdelt_articles(*, category: TrendCategory, keyword: str = "", hours: int = 6, max_records: int = 180) -> list[dict]:
    global _GDELT_DISABLED_UNTIL, _GDELT_LAST_REQUEST_AT
    if time.time() < _GDELT_DISABLED_UNTIL:
        remaining = int(_GDELT_DISABLED_UNTIL - time.time())
        raise RuntimeError(f"GDELT 正在冷却中，约 {remaining} 秒后重试")
    query = _gdelt_safe_query(_build_query(category, keyword))
    cache_key = f"{query}|{hours}|{max_records}"
    cached = _GDELT_CACHE.get(cache_key)
    if cached and time.time() - cached[0] <= GDELT_CACHE_SECONDS:
        return [dict(item) for item in cached[1]]

    wait_seconds = GDELT_MIN_REQUEST_INTERVAL_SECONDS - (time.time() - _GDELT_LAST_REQUEST_AT)
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "timespan": f"{max(1, min(hours, 24))}h",
        "sort": "HybridRel",
    }
    _GDELT_LAST_REQUEST_AT = time.time()
    response = requests.get(GDELT_DOC_API_URL, params=params, headers=DEFAULT_HEADERS, timeout=18)
    if response.status_code == 429:
        _GDELT_DISABLED_UNTIL = time.time() + GDELT_COOLDOWN_SECONDS
    response.raise_for_status()
    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"GDELT 返回非 JSON：{response.text[:180]}") from exc
    articles = payload.get("articles") if isinstance(payload, dict) else []
    parsed = [_article_from_gdelt(item, category) for item in articles if isinstance(item, dict)]
    _GDELT_CACHE[cache_key] = (time.time(), [dict(item) for item in parsed])
    return parsed


def _fetch_newsdata_articles(*, category: TrendCategory, keyword: str = "", hours: int = 6, max_records: int = 50) -> list[dict]:
    api_key = (os.getenv("NEWSDATA_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("未配置 NEWSDATA_API_KEY")
    query = _strip_tags(keyword or "").strip() or NEWSDATA_QUERY_MAP.get(category.id, "breaking news")
    for short, replacement in GDELT_SAFE_TERM_REPLACEMENTS.items():
        query = re.sub(rf"\b{re.escape(short)}\b", replacement, query, flags=re.I)
    query = re.sub(r"\bOR\b|\bAND\b|\bNOT\b|[()]", " ", query, flags=re.I)
    query = re.sub(r"\s+", " ", query).strip()[:90] or "breaking news"
    params = {
        "apikey": api_key,
        "language": "en",
        "q": query,
    }
    category_param = NEWSDATA_CATEGORY_MAP.get(category.id)
    if category_param:
        params["category"] = category_param
    response = requests.get(NEWSDATA_LATEST_URL, params=params, headers=DEFAULT_HEADERS, timeout=10)
    response.raise_for_status()
    payload = response.json()
    status = str(payload.get("status") or "").lower()
    if status and status not in {"success", "ok"}:
        raise RuntimeError(str(payload.get("message") or payload.get("results") or "NewsData.io 返回失败"))
    raw_results = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(raw_results, list):
        raw_results = []
    articles = [_article_from_newsdata(item, category) for item in raw_results[:max_records] if isinstance(item, dict)]
    return [item for item in articles if item.get("title") and item.get("url")]


def _fetch_bing_news_articles(*, category: TrendCategory, keyword: str = "", hours: int = 6, max_records: int = 80) -> list[dict]:
    query = _strip_tags(keyword or "").strip()
    if query:
        query = f"{query} latest news"
    else:
        query = BING_TREND_QUERIES.get(category.id, "breaking news latest")
    params = {
        "q": query,
        "format": "RSS",
        "mkt": "en-US",
        "setlang": "en-US",
    }
    response = requests.get(BING_NEWS_RSS_URL, params=params, headers=DEFAULT_HEADERS, timeout=24)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    articles: list[dict] = []
    fallback_articles: list[dict] = []
    cutoff = time.time() - max(1, min(hours, 24)) * 3600
    for item in root.findall(".//item"):
        title = _strip_tags(item.findtext("title") or "")
        url = _strip_tags(item.findtext("link") or "")
        summary = _strip_tags(item.findtext("description") or "")
        pub_date = _strip_tags(item.findtext("pubDate") or "")
        published_ts = 0.0
        if pub_date:
            try:
                from email.utils import parsedate_to_datetime

                published_ts = parsedate_to_datetime(pub_date).timestamp()
            except Exception:
                published_ts = 0.0
        domain = _domain(url)
        source_name = domain or "Bing News"
        article = {
            "id": f"bing_{uuid.uuid5(uuid.NAMESPACE_URL, url or title).hex[:12]}",
            "source_id": "bing_news",
            "source_name": source_name,
            "category": category.id if category.id != "all" else "politics",
            "category_name": category.name if category.id != "all" else "英文热点",
            "title": title[:240],
            "url": url,
            "summary": summary[:420],
            "published_at": _format_ts(published_ts) or pub_date,
            "published_ts": published_ts,
            "license": "来源网站原文，请审核后使用",
            "content_type": "Bing News 英文热点",
            "is_latest": bool(published_ts and time.time() - published_ts <= 24 * 3600),
            "trend_source": "Bing News RSS",
            "trend_domain": domain,
            "image": "",
            "related_articles": [],
        }
        fallback_articles.append(article)
        if not published_ts or published_ts >= cutoff:
            articles.append(article)
        if len(articles) >= max_records:
            break
    if not articles:
        articles = fallback_articles[:max_records]
    if not articles:
        raise RuntimeError("Bing News RSS 没有返回英文热点")
    return articles


def _cluster_articles(articles: list[dict]) -> list[dict]:
    clusters: list[dict] = []
    for article in articles:
        if not article.get("title") or not article.get("url"):
            continue
        matched = None
        for cluster in clusters:
            if _titles_similar(article.get("title", ""), cluster.get("title", "")):
                matched = cluster
                break
        if matched is None:
            item = dict(article)
            item["related_articles"] = [dict(article)]
            item["source_count"] = 1
            item["source_domains"] = [article.get("trend_domain") or _domain(article.get("url", ""))]
            clusters.append(item)
            continue
        related = matched.setdefault("related_articles", [])
        if not any(existing.get("url") == article.get("url") for existing in related):
            related.append(dict(article))
        domains = matched.setdefault("source_domains", [])
        domain = article.get("trend_domain") or _domain(article.get("url", ""))
        if domain and domain not in domains:
            domains.append(domain)
        matched["source_count"] = len(domains)
        if float(article.get("published_ts") or 0) > float(matched.get("published_ts") or 0):
            matched["published_ts"] = article.get("published_ts")
            matched["published_at"] = article.get("published_at")
            matched["is_latest"] = article.get("is_latest", False)
        if not matched.get("summary") and article.get("summary"):
            matched["summary"] = article.get("summary")
    return clusters


def _score_cluster(cluster: dict, *, now_ts: float) -> float:
    domains = cluster.get("source_domains") or []
    source_score = min(len(domains), 8) * 10
    authority_score = sum(AUTHORITATIVE_DOMAINS.get(str(domain).lower(), 1.0) for domain in domains[:8]) * 3
    age_hours = max(0.0, (now_ts - float(cluster.get("published_ts") or 0)) / 3600) if cluster.get("published_ts") else 24
    freshness_score = max(0.0, 28 - age_hours * 1.4)
    image_score = 5 if cluster.get("image") else 0
    return round(source_score + authority_score + freshness_score + image_score, 1)


def _translate_trend_candidates(candidates: list[dict]) -> None:
    """Add Chinese preview fields so users can read hot-topic cards before drafting."""
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key or not candidates:
        return
    items = []
    for item in candidates[:40]:
        items.append({
            "id": item.get("id"),
            "title": item.get("title") or "",
            "summary": item.get("summary") or "",
            "sources": item.get("source_domains") or [],
        })
    if not items:
        return
    model = (os.getenv("OPENAI_TEXT_MODEL") or os.getenv("OPENAI_VISION_MODEL") or "gpt-4o-mini").strip()
    prompt = f"""
请把下面英文热点新闻候选转译成简体中文，方便中文用户在页面上快速判断是否值得制作视频。

要求：
1. 不要添加原文没有的事实。
2. 标题要像新闻标题，简洁准确。
3. 摘要用 1-2 句中文说明事件核心。
4. 保留专有名词的常见中文译名；不确定的人名/机构名可保留英文。
5. 只输出 JSON，格式：
{{
  "items": [
    {{"id": "...", "title_zh": "...", "summary_zh": "..."}}
  ]
}}

候选：
{json.dumps(items, ensure_ascii=False)}
""".strip()
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "你只输出可解析 JSON。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 4096,
            "response_format": {"type": "json_object"},
        },
        timeout=80,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 热点转译失败：{response.status_code} {response.text[:300]}")
    raw = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    payload = json.loads(raw)
    translations = payload.get("items") if isinstance(payload, dict) else []
    by_id = {str(item.get("id") or ""): item for item in translations if isinstance(item, dict)}
    for candidate in candidates:
        translated = by_id.get(str(candidate.get("id") or ""))
        if not translated:
            continue
        title_zh = _strip_tags(str(translated.get("title_zh") or ""))[:260]
        summary_zh = _strip_tags(str(translated.get("summary_zh") or ""))[:520]
        if title_zh:
            candidate["title_zh"] = title_zh
        if summary_zh:
            candidate["summary_zh"] = summary_zh


def search_english_trends(*, category: str = "all", time_range: str = "6h", keyword: str = "", limit: int = 40) -> dict:
    trend_category = _category_by_id(category)
    hours = _hours_for_range(time_range)
    source_errors: list[str] = []
    raw_articles: list[dict] = []
    try:
        raw_articles = _fetch_newsdata_articles(category=trend_category, keyword=keyword, hours=hours)
    except Exception as exc:
        source_errors.append(f"NewsData.io: {exc}")
    try:
        gdelt_articles = _fetch_gdelt_articles(category=trend_category, keyword=keyword, hours=hours)
        existing_urls = {item.get("url") for item in raw_articles}
        raw_articles.extend(item for item in gdelt_articles if item.get("url") not in existing_urls)
    except Exception as exc:
        source_errors.append(f"GDELT: {exc}")
    if len(raw_articles) < 8:
        try:
            bing_articles = _fetch_bing_news_articles(category=trend_category, keyword=keyword, hours=hours)
            existing_urls = {item.get("url") for item in raw_articles}
            raw_articles.extend(item for item in bing_articles if item.get("url") not in existing_urls)
        except Exception as exc:
            source_errors.append(f"Bing News RSS: {exc}")
    if not raw_articles and source_errors:
        raise RuntimeError("；".join(source_errors[:2]))
    clusters = _cluster_articles(raw_articles)
    now_ts = time.time()
    for cluster in clusters:
        cluster["trend_score"] = _score_cluster(cluster, now_ts=now_ts)
        related = cluster.get("related_articles") or []
        cluster["related_articles"] = sorted(related, key=lambda item: float(item.get("published_ts") or 0), reverse=True)[:8]
        cluster["summary"] = cluster.get("summary") or "该热点由 GDELT 根据英文新闻报道聚合发现，请打开来源核对事实后制作视频。"
    clusters = sorted(clusters, key=lambda item: (float(item.get("trend_score") or 0), float(item.get("published_ts") or 0)), reverse=True)
    limited = clusters[: max(1, min(limit, 80))]
    try:
        _translate_trend_candidates(limited)
    except Exception as exc:
        source_errors.append(f"中文转译: {exc}")
    source_counter: dict[str, int] = defaultdict(int)
    for item in raw_articles:
        domain = item.get("trend_domain") or _domain(item.get("url", ""))
        if domain:
            source_counter[domain] += 1
    stats = [
        {"source_id": domain, "source_name": domain, "raw_count": count, "deduped_count": 0, "crawled_pages": 1}
        for domain, count in sorted(source_counter.items(), key=lambda pair: pair[1], reverse=True)[:12]
    ]
    return {
        "candidates": limited,
        "stats": stats,
        "raw_count": len(raw_articles),
        "deduped_count": len(clusters),
        "recent_count": len(limited),
        "recent_window": f"最近 {hours} 小时英文新闻热点",
        "time_range": f"{hours}h",
        "category": trend_category.id,
        "source_errors": source_errors,
    }
