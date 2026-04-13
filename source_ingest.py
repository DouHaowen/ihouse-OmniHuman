import html
import json
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urlparse

import requests

URL_RE = re.compile(r"(https?://[^\s<>'\"）)]+)", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")

YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
}


@dataclass
class SourceAnalysis:
    kind: str
    url: str = ""
    title: str = ""
    source_name: str = ""
    published_at: str = ""
    summary: str = ""
    excerpt: str = ""
    user_note: str = ""
    normalized_topic: str = ""
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "detected": self.kind != "text" or bool(self.url),
            "kind": self.kind,
            "url": self.url,
            "title": self.title,
            "source_name": self.source_name,
            "published_at": self.published_at,
            "summary": self.summary,
            "excerpt": self.excerpt,
            "user_note": self.user_note,
            "normalized_topic": self.normalized_topic,
            "error": self.error,
        }


def _clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = text.replace("\u00a0", " ")
    text = SPACE_RE.sub(" ", text)
    return text.strip()


def _strip_tags(text: str) -> str:
    return _clean_text(TAG_RE.sub(" ", text or ""))


def _extract_meta(html_text: str, names: list[str]) -> str:
    for name in names:
        patterns = [
            rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
            rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                return _clean_text(match.group(1))
    return ""


def _extract_title(html_text: str) -> str:
    meta_title = _extract_meta(html_text, ["og:title", "twitter:title"])
    if meta_title:
        return meta_title
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return _strip_tags(match.group(1))
    return ""


def _extract_paragraphs(html_text: str, limit: int = 6) -> list[str]:
    paragraphs = []
    for match in re.finditer(r"<p[^>]*>(.*?)</p>", html_text, flags=re.IGNORECASE | re.DOTALL):
        text = _strip_tags(match.group(1))
        if len(text) < 24:
            continue
        if re.fullmatch(r"[\W_]+", text):
            continue
        paragraphs.append(text)
        if len(paragraphs) >= limit:
            break
    return paragraphs


def _split_input_text(raw_text: str) -> tuple[str, str]:
    text = _clean_text(raw_text)
    if not text:
        return "", ""
    match = URL_RE.search(text)
    if not match:
        return "", text
    url = match.group(1).rstrip(".,;，。；、】）)]")
    note = _clean_text((text[: match.start()] + " " + text[match.end():]).strip())
    return url, note


def _fetch_html(url: str) -> str:
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        },
        timeout=20,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def _summarize_youtube(url: str) -> SourceAnalysis:
    title = ""
    source_name = ""
    summary = ""
    published_at = ""
    excerpt = ""
    error = ""
    html_text = ""

    try:
        oembed = requests.get(
            "https://www.youtube.com/oembed",
            params={"url": url, "format": "json"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        if oembed.ok:
            payload = oembed.json()
            title = _clean_text(payload.get("title", ""))
            source_name = _clean_text(payload.get("author_name", ""))
    except Exception as exc:
        error = str(exc)

    try:
        html_text = _fetch_html(url)
        if not title:
            title = _extract_title(html_text)
        if not source_name:
            source_name = _extract_meta(html_text, ["og:site_name"]) or "YouTube"
        summary = _extract_meta(html_text, ["og:description", "description"])
        if not summary:
            m = re.search(r'"shortDescription"\s*:\s*"(.*?)"', html_text, flags=re.IGNORECASE | re.DOTALL)
            if m:
                try:
                    summary = json.loads(f'"{m.group(1)}"')
                except Exception:
                    summary = m.group(1)
        summary = _clean_text(summary)
        if not summary:
            summary = "YouTube 页面公开摘要较少，建议结合标题与频道信息提炼脚本角度。"
        excerpt = " ".join(_extract_paragraphs(html_text, limit=3))
    except Exception as exc:
        if not error:
            error = str(exc)

    return SourceAnalysis(
        kind="youtube",
        url=url,
        title=title or "YouTube 视频",
        source_name=source_name or "YouTube",
        published_at=published_at,
        summary=summary,
        excerpt=excerpt,
        normalized_topic=_build_generation_topic(
            kind="youtube",
            url=url,
            title=title or "YouTube 视频",
            source_name=source_name or "YouTube",
            summary=summary,
            excerpt=excerpt,
            user_note="",
        ),
        error=error,
    )


def _summarize_web_page(url: str) -> SourceAnalysis:
    title = ""
    source_name = ""
    published_at = ""
    summary = ""
    excerpt = ""
    error = ""
    try:
        html_text = _fetch_html(url)
        title = _extract_title(html_text)
        source_name = _extract_meta(html_text, ["og:site_name"]) or urlparse(url).netloc
        published_at = _extract_meta(html_text, ["article:published_time", "og:updated_time", "datePublished"])
        summary = _extract_meta(html_text, ["og:description", "description"])
        paragraphs = _extract_paragraphs(html_text, limit=6)
        if not summary and paragraphs:
            summary = " ".join(paragraphs[:2])
        if not summary:
            summary = "页面公开可见摘要较少，建议结合标题和前几段正文理解。"
        excerpt = " ".join(paragraphs[:4])
    except Exception as exc:
        error = str(exc)

    return SourceAnalysis(
        kind="news",
        url=url,
        title=title or url,
        source_name=source_name or urlparse(url).netloc or "网页",
        published_at=published_at,
        summary=_clean_text(summary),
        excerpt=_clean_text(excerpt),
        normalized_topic="",
        error=error,
    )


def _build_generation_topic(*, kind: str, url: str, title: str, source_name: str, summary: str, excerpt: str, user_note: str) -> str:
    lines = []
    if kind == "youtube":
        lines.append("【YouTube来源】")
    elif kind == "news":
        lines.append("【新闻来源】")
    else:
        lines.append("【来源链接】")
    lines.append(f"链接：{url}")
    if title:
        lines.append(f"标题：{title}")
    if source_name:
        lines.append(f"来源：{source_name}")
    if summary:
        lines.append(f"摘要：{summary}")
    if excerpt:
        lines.append(f"正文要点：{excerpt}")
    if user_note:
        lines.append(f"用户备注：{user_note}")
    lines.append("请基于以上来源内容，提炼最适合短视频表达的选题角度，并在不捏造事实的前提下输出脚本。")
    return "\n".join(lines)


def _analyze_source_url(raw_source_url: str, user_note: str = "") -> SourceAnalysis:
    source_url, extra_note = _split_input_text(raw_source_url)
    if not source_url:
        source_url = _clean_text(raw_source_url)
    combined_note = _clean_text(" ".join(part for part in [user_note, extra_note] if part))
    host = urlparse(source_url).netloc.lower()
    if host in YOUTUBE_HOSTS:
        source = _summarize_youtube(source_url)
        source.user_note = combined_note
        source.normalized_topic = _build_generation_topic(
            kind=source.kind,
            url=source.url,
            title=source.title,
            source_name=source.source_name,
            summary=source.summary,
            excerpt=source.excerpt,
            user_note=combined_note,
        )
        return source

    source = _summarize_web_page(source_url)
    source.user_note = combined_note
    source.normalized_topic = _build_generation_topic(
        kind=source.kind,
        url=source.url,
        title=source.title,
        source_name=source.source_name,
        summary=source.summary,
        excerpt=source.excerpt,
        user_note=combined_note,
    )
    return source


def analyze_topic_input(raw_input: str) -> dict:
    text = _clean_text(raw_input)
    if not text:
        return SourceAnalysis(kind="text", normalized_topic="").to_dict()

    url, note = _split_input_text(text)
    if not url:
        return SourceAnalysis(kind="text", normalized_topic=text).to_dict()

    return _analyze_source_url(url, note).to_dict()


def analyze_topic_fields(topic_text: str = "", source_url: str = "", fallback_topic: str = "") -> dict:
    topic_text = _clean_text(topic_text)
    source_url = _clean_text(source_url)
    fallback_topic = _clean_text(fallback_topic)

    if source_url:
        return _analyze_source_url(source_url, topic_text).to_dict()
    if topic_text:
        return analyze_topic_input(topic_text)
    if fallback_topic:
        return analyze_topic_input(fallback_topic)
    return SourceAnalysis(kind="text", normalized_topic="").to_dict()
