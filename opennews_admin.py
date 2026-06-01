"""
OpenNews 管理员测试功能。

第一版只做新闻候选抓取和中文新闻口播稿草稿，不接入正式生产链路。
"""

from __future__ import annotations

import html
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

import requests


OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_HEADERS = {
    "User-Agent": "iHouse-OpenNews-Test/0.1 (+https://aiagent.office.ihousejapan.cn)",
}


@dataclass(frozen=True)
class OpenNewsSource:
    id: str
    name: str
    country: str
    url: str
    license: str
    content_type: str
    search_url: str = ""
    rss_url: str = ""


OPENNEWS_SOURCES: list[OpenNewsSource] = [
    OpenNewsSource(
        id="dvids",
        name="DVIDS 美军媒体素材库",
        country="美国",
        url="https://www.dvidshub.net/",
        license="Public Domain",
        content_type="军事公开视频/图片/新闻",
        search_url="https://www.dvidshub.net/search?q={query}",
    ),
    OpenNewsSource(
        id="dod",
        name="美国国防部",
        country="美国",
        url="https://www.defense.gov/",
        license="Public Domain",
        content_type="国防声明/新闻/图片",
        search_url="https://www.defense.gov/Search-Results/?query={query}",
    ),
    OpenNewsSource(
        id="indopacom",
        name="美国印太司令部",
        country="美国",
        url="https://www.pacom.mil/",
        license="Public Domain",
        content_type="印太/台海/南海军事动态",
        search_url="https://www.pacom.mil/Search/?query={query}",
    ),
    OpenNewsSource(
        id="mod_jp",
        name="日本防卫省",
        country="日本",
        url="https://www.mod.go.jp/",
        license="PDL 1.0 / CC BY 4.0 equivalent",
        content_type="防卫省/自卫队动态",
        search_url="https://www.mod.go.jp/j/search/?q={query}",
    ),
    OpenNewsSource(
        id="mofa_jp",
        name="日本外务省",
        country="日本",
        url="https://www.mofa.go.jp/",
        license="PDL 1.0 / CC BY 4.0 equivalent",
        content_type="外交声明/政策新闻",
        search_url="https://www.mofa.go.jp/search.html?q={query}",
    ),
    OpenNewsSource(
        id="mnd_tw",
        name="台湾国防部",
        country="台湾",
        url="https://www.mnd.gov.tw/",
        license="OGDL-Taiwan / CC BY 4.0 equivalent",
        content_type="国防新闻/共机绕台数据",
        search_url="https://www.mnd.gov.tw/Search.aspx?query={query}",
    ),
    OpenNewsSource(
        id="voa_zh",
        name="VOA 中文",
        country="美国",
        url="https://www.voachinese.com/",
        license="Public Domain",
        content_type="中文新闻参考",
        rss_url="https://www.voachinese.com/api/",
        search_url="https://www.voachinese.com/s?k={query}",
    ),
]


def source_payloads() -> list[dict]:
    return [source.__dict__ for source in OPENNEWS_SOURCES]


def _strip_tags(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value or "", flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_html_candidates(page_html: str, source: OpenNewsSource, limit: int = 8) -> list[dict]:
    candidates: list[dict] = []
    seen: set[str] = set()
    for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([\s\S]{8,260}?)</a>', page_html or "", flags=re.I):
        href, label_html = match.groups()
        label = _strip_tags(label_html)
        if len(label) < 8:
            continue
        if href.startswith("/"):
            href = source.url.rstrip("/") + href
        if not href.startswith("http") or href in seen:
            continue
        lowered = href.lower()
        if not any(token in lowered for token in ("news", "article", "releases", "press", "video", "image", "story", "search")):
            continue
        seen.add(href)
        candidates.append(
            {
                "id": uuid.uuid4().hex[:12],
                "source_id": source.id,
                "source_name": source.name,
                "title": label[:160],
                "url": href,
                "summary": "",
                "published_at": "",
                "license": source.license,
                "content_type": source.content_type,
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def _fetch_rss_candidates(source: OpenNewsSource, limit: int = 8) -> list[dict]:
    if not source.rss_url:
        return []
    try:
        response = requests.get(source.rss_url, headers=DEFAULT_HEADERS, timeout=15)
    except Exception:
        return []
    if response.status_code >= 400:
        return []
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return []
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    results = []
    for item in items[:limit]:
        title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or ""
        link = item.findtext("link") or ""
        if not link:
            link_node = item.find("{http://www.w3.org/2005/Atom}link")
            link = link_node.attrib.get("href", "") if link_node is not None else ""
        description = item.findtext("description") or item.findtext("summary") or ""
        published = item.findtext("pubDate") or item.findtext("published") or ""
        if not title or not link:
            continue
        results.append(
            {
                "id": uuid.uuid4().hex[:12],
                "source_id": source.id,
                "source_name": source.name,
                "title": _strip_tags(title)[:180],
                "url": link,
                "summary": _strip_tags(description)[:360],
                "published_at": _strip_tags(published)[:80],
                "license": source.license,
                "content_type": source.content_type,
            }
        )
    return results


def search_opennews_candidates(query: str, source_ids: Iterable[str] | None = None, limit_per_source: int = 6) -> list[dict]:
    query = (query or "").strip()
    selected = set(source_ids or [])
    sources = [source for source in OPENNEWS_SOURCES if not selected or source.id in selected]
    all_candidates: list[dict] = []
    for source in sources:
        if source.rss_url and not query:
            all_candidates.extend(_fetch_rss_candidates(source, limit=limit_per_source))
            continue
        if not source.search_url:
            continue
        url = source.search_url.format(query=quote_plus(query or "news"))
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=18)
        except Exception as exc:
            all_candidates.append(
                {
                    "id": uuid.uuid4().hex[:12],
                    "source_id": source.id,
                    "source_name": source.name,
                    "title": f"{source.name} 抓取失败",
                    "url": source.url,
                    "summary": str(exc),
                    "published_at": "",
                    "license": source.license,
                    "content_type": source.content_type,
                    "error": str(exc),
                }
            )
            continue
        if response.status_code >= 400:
            continue
        all_candidates.extend(_extract_html_candidates(response.text, source, limit=limit_per_source))
    return all_candidates[:60]


def fetch_article_text(url: str) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
    response.raise_for_status()
    text = _strip_tags(response.text)
    return text[:12000]


def generate_opennews_draft(*, article: dict, target_market: str = "cn", notes: str = "") -> dict:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("未配置 OPENAI_API_KEY，无法生成新闻稿")
    url = str(article.get("url") or "")
    article_text = fetch_article_text(url) if url else ""
    language = "繁體中文" if target_market == "tw" else "简体中文"
    model = (os.getenv("OPENAI_TEXT_MODEL") or os.getenv("OPENAI_VISION_MODEL") or "gpt-4o-mini").strip()
    prompt = f"""
你是 iHouse 的 OpenNews 新闻视频编辑。请根据公开新闻源生成中文短视频新闻口播稿。

输出语言：{language}
目标长度：1-3 分钟口播。
来源名称：{article.get("source_name")}
授权：{article.get("license")}
标题：{article.get("title")}
链接：{url}
管理员补充要求：{notes or "无"}

原始网页正文节选：
{article_text or article.get("summary") or "无正文。"}

要求：
1. 只根据来源正文和管理员补充写，不要编造未出现的事实。
2. 涉及军事、外交、台海、战争议题时，语气保持新闻说明，不煽动，不下定论。
3. 必须输出：标题、摘要、口播稿、素材关键词、事实核验提醒、来源标注。
4. 口播稿适合直接配音，结构为：事件一句话、背景、影响、结尾提醒。
5. 输出 JSON：
{{
  "video_title": "...",
  "summary": "...",
  "script": "...",
  "material_keywords": ["舰艇", "军演"],
  "fact_check_notes": ["..."],
  "source_credit": "..."
}}
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
            "temperature": 0.35,
            "max_tokens": 4096,
            "response_format": {"type": "json_object"},
        },
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 新闻稿生成失败：{response.status_code} {response.text[:500]}")
    raw = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        draft = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", raw)
        if not match:
            raise
        draft = json.loads(match.group(0))
    draft["_meta"] = {
        "model": model,
        "source_url": url,
        "source_name": article.get("source_name"),
        "license": article.get("license"),
        "created_at": time.time(),
    }
    return draft


def _split_script_sentences(script: str) -> list[str]:
    parts = re.split(r"(?<=[。！？!?；;])\s*", script or "")
    sentences = [part.strip() for part in parts if part and part.strip()]
    if sentences:
        return sentences
    fallback = re.split(r"[\n\r]+", script or "")
    return [part.strip() for part in fallback if part and part.strip()]


def _join_sentences(sentences: list[str], start: int, end: int) -> str:
    text = "".join(sentences[start:end]).strip()
    return text or "请关注这条新闻的最新公开信息与后续进展。"


def _estimate_duration_seconds(text: str, *, minimum: int = 6, maximum: int = 38) -> int:
    # 中文新闻口播大约 4-5 字/秒，给数字人和素材段留一点呼吸空间。
    visible_chars = len(re.sub(r"\s+", "", text or ""))
    return max(minimum, min(maximum, int(round(visible_chars / 4.2)) or minimum))


def build_opennews_script_data(*, draft: dict, article: dict | None = None, target_market: str = "cn") -> dict:
    """把 OpenNews 新闻稿转换成主生产流水线能消费的标准脚本结构。"""
    article = article or {}
    script = str(draft.get("script") or "").strip()
    if not script:
        raise ValueError("新闻稿草稿缺少口播稿，无法生成视频")

    sentences = _split_script_sentences(script)
    if len(sentences) < 5:
        sentences = sentences + ["请继续关注官方公开信息和后续进展。"]

    total = len(sentences)
    opening_end = max(1, min(total, round(total * 0.16)))
    background_end = max(opening_end + 1, min(total, round(total * 0.48)))
    transition_end = max(background_end + 1, min(total, background_end + 1))
    detail_end = max(transition_end + 1, min(total, round(total * 0.84)))
    if detail_end >= total:
        detail_end = max(transition_end + 1, total - 1)

    chunks = {
        "opening": _join_sentences(sentences, 0, opening_end),
        "background": _join_sentences(sentences, opening_end, background_end),
        "transition": _join_sentences(sentences, background_end, transition_end),
        "detail": _join_sentences(sentences, transition_end, detail_end),
        "closing": _join_sentences(sentences, detail_end, total),
    }

    keywords = draft.get("material_keywords") or []
    if not isinstance(keywords, list):
        keywords = [str(keywords)]
    keyword_text = "、".join(str(item).strip() for item in keywords if str(item).strip()) or str(draft.get("video_title") or article.get("title") or "news")
    search_keyword = " ".join(str(item).strip() for item in keywords[:4] if str(item).strip()) or "military news official footage"
    source_name = article.get("source_name") or draft.get("_meta", {}).get("source_name") or "OpenNews"

    segments = [
        {
            "type": "digital_human",
            "start": 0,
            "duration": _estimate_duration_seconds(chunks["opening"], minimum=7, maximum=16),
            "script": chunks["opening"],
            "action": "新闻主播正对镜头，语气冷静，开场交代事件核心。",
        },
        {
            "type": "material",
            "start": 0,
            "duration": _estimate_duration_seconds(chunks["background"], minimum=14, maximum=36),
            "script": chunks["background"],
            "material_keyword": keyword_text,
            "material_search_keyword": search_keyword,
            "material_desc": f"与 {source_name} 新闻相关的公开新闻画面、官方发布画面、地图、舰艇、飞机、军演或外交会晤素材。",
        },
        {
            "type": "digital_human",
            "start": 0,
            "duration": _estimate_duration_seconds(chunks["transition"], minimum=5, maximum=9),
            "script": chunks["transition"],
            "action": "新闻主播短暂出镜，用一句话承上启下，提示接下来关注影响。",
        },
        {
            "type": "material",
            "start": 0,
            "duration": _estimate_duration_seconds(chunks["detail"], minimum=16, maximum=38),
            "script": chunks["detail"],
            "material_keyword": keyword_text,
            "material_search_keyword": search_keyword,
            "material_desc": "新闻细节说明所需的公开视频、资料画面、地区背景、军事装备、现场或机构发布素材。",
        },
        {
            "type": "digital_human",
            "start": 0,
            "duration": _estimate_duration_seconds(chunks["closing"], minimum=7, maximum=16),
            "script": chunks["closing"],
            "action": "新闻主播收尾，总结事实并提醒关注后续官方信息。",
        },
    ]

    cursor = 0
    for seg in segments:
        seg["start"] = cursor
        cursor += int(seg.get("duration") or 0)
        seg["end"] = cursor

    title = str(draft.get("video_title") or article.get("title") or "OpenNews 新闻视频").strip()
    summary = str(draft.get("summary") or "").strip()
    source_credit = str(draft.get("source_credit") or article.get("url") or "").strip()
    social_post = "\n".join(part for part in [title, summary, f"来源：{source_credit}" if source_credit else ""] if part)
    return {
        "title": title,
        "cover_title": title[:28],
        "total_duration": cursor,
        "segments": segments,
        "social_post": social_post,
        "opennews": {
            "article": article,
            "draft_meta": draft.get("_meta") or {},
            "source_credit": source_credit,
            "fact_check_notes": draft.get("fact_check_notes") or [],
            "material_keywords": keywords,
        },
    }


def save_opennews_payload(root: Path, name: str, payload: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{name}_{int(time.time())}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
