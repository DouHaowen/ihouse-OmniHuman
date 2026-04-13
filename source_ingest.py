import html
import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests
try:
    from faster_whisper import WhisperModel
except Exception:
    WhisperModel = None

try:
    import yt_dlp
except Exception:
    yt_dlp = None

URL_RE = re.compile(r"(https?://[^\s<>'\"）)]+)", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
YT_INITIAL_PLAYER_RE = re.compile(r"ytInitialPlayerResponse\s*=\s*(\{.+?\})\s*;", re.DOTALL)

YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
}

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

YOUTUBE_COOKIES = {
    "CONSENT": "YES+cb.20210328-17-p0.en+FX+470",
}

_whisper_model_cache: dict[str, object] = {}


@dataclass
class SourceAnalysis:
    kind: str
    url: str = ""
    title: str = ""
    source_name: str = ""
    source_language: str = ""
    extraction_method: str = ""
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
            "source_language": self.source_language,
            "extraction_method": self.extraction_method,
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
        headers=DEFAULT_HEADERS,
        cookies=YOUTUBE_COOKIES if "youtube.com" in urlparse(url).netloc.lower() or "youtu.be" in urlparse(url).netloc.lower() else None,
        timeout=20,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def _extract_json_blob(pattern: re.Pattern[str], html_text: str) -> dict:
    match = pattern.search(html_text or "")
    if not match:
        return {}
    raw = match.group(1)
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _youtube_video_id(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host == "youtu.be":
        return parsed.path.lstrip("/").split("/")[0]
    if "youtube.com" in host:
        if parsed.path == "/watch":
            return parse_qs(parsed.query).get("v", [""])[0]
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"}:
            return parts[1]
    return ""


def _replace_query_params(url: str, **updates: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in updates.items():
        if value is None:
            query.pop(key, None)
        else:
            query[key] = value
    return urlunparse(parsed._replace(query=urlencode(query)))


def _get_whisper_model(size: str = "base"):
    if WhisperModel is None:
        return None
    if size not in _whisper_model_cache:
        _whisper_model_cache[size] = WhisperModel(size, device="cpu", compute_type="int8")
    return _whisper_model_cache[size]


def _pick_caption_track(caption_tracks: list[dict]) -> dict:
    if not caption_tracks:
        return {}
    preferred_langs = ("zh-Hant", "zh-TW", "zh-HK", "ja", "zh", "en")
    for lang in preferred_langs:
        for track in caption_tracks:
            if (track.get("languageCode") or "").lower() == lang.lower() and not track.get("kind"):
                return track
    for track in caption_tracks:
        if not track.get("kind"):
            return track
    return caption_tracks[0]


def _fetch_youtube_transcript(caption_url: str) -> str:
    if not caption_url:
        return ""
    caption_url = _replace_query_params(caption_url, fmt="json3")
    response = requests.get(
        caption_url,
        headers=DEFAULT_HEADERS,
        cookies=YOUTUBE_COOKIES,
        timeout=15,
        allow_redirects=True,
    )
    response.raise_for_status()
    raw_text = response.text or ""
    if not raw_text.strip():
        return ""
    content_type = (response.headers.get("content-type") or "").lower()
    if "json" in content_type or raw_text.lstrip().startswith("{"):
        try:
            payload = response.json()
        except Exception:
            payload = {}
        chunks = []
        for event in payload.get("events") or []:
            for seg in event.get("segs") or []:
                piece = _clean_text(seg.get("utf8", ""))
                if piece:
                    chunks.append(piece)
        return _clean_text(" ".join(chunks))

    try:
        root = ET.fromstring(raw_text)
    except Exception:
        return ""

    chunks = []
    for node in root.findall(".//text"):
        piece = _clean_text("".join(node.itertext()))
        if piece:
            chunks.append(piece)
    transcript = " ".join(chunks)
    return _clean_text(transcript)


def _fetch_best_youtube_transcript(caption_tracks: list[dict]) -> tuple[str, str]:
    if not caption_tracks:
        return "", ""
    ordered_tracks = []
    preferred = _pick_caption_track(caption_tracks)
    if preferred:
        ordered_tracks.append(preferred)
    for track in caption_tracks:
        if track not in ordered_tracks:
            ordered_tracks.append(track)
    for track in ordered_tracks:
        transcript = _fetch_youtube_transcript(track.get("baseUrl", ""))
        if transcript:
            return transcript, track.get("languageCode", "")
    return "", ""


def _yt_dlp_extract_info(url: str) -> dict:
    if yt_dlp is None:
        return {}
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True}) as ydl:
            return ydl.extract_info(url, download=False) or {}
    except Exception:
        return {}


def _pick_yt_dlp_caption(info: dict) -> tuple[str, str]:
    if not info:
        return "", ""
    pools = [info.get("subtitles") or {}, info.get("automatic_captions") or {}]
    preferred_langs = ("zh-Hant", "zh-TW", "zh-HK", "ja", "zh", "en")
    for pool in pools:
        for lang in preferred_langs:
            for entry in pool.get(lang) or []:
                url = entry.get("url") or ""
                if url:
                    return url, lang
    for pool in pools:
        for lang, entries in pool.items():
            for entry in entries or []:
                url = entry.get("url") or ""
                if url:
                    return url, lang
    return "", ""


def _download_youtube_audio(url: str) -> Path | None:
    if yt_dlp is None:
        return None
    work_dir = Path(tempfile.mkdtemp(prefix="yt-audio-"))
    outtmpl = str(work_dir / "audio.%(ext)s")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "format": "bestaudio/best",
        "outtmpl": outtmpl,
        "noplaylist": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            path = Path(ydl.prepare_filename(info))
            if path.exists():
                return path
            for candidate in work_dir.glob("audio.*"):
                if candidate.is_file():
                    return candidate
    except Exception:
        return None
    return None


def _transcribe_audio(audio_path: Path) -> tuple[str, str]:
    model = _get_whisper_model("base")
    if model is None or not audio_path or not audio_path.exists():
        return "", ""
    try:
        segments_iter, info = model.transcribe(str(audio_path), vad_filter=True, beam_size=1)
        pieces = []
        for seg in segments_iter:
            text = _clean_text(getattr(seg, "text", "") or "")
            if text:
                pieces.append(text)
        return _clean_text(" ".join(pieces)), _clean_text(getattr(info, "language", "") or "")
    except Exception:
        return "", ""


def _summarize_transcript(transcript: str, limit: int = 360) -> tuple[str, str]:
    cleaned = _clean_text(transcript)
    if not cleaned:
        return "", ""
    summary = cleaned[:limit].rsplit(" ", 1)[0] if len(cleaned) > limit else cleaned
    excerpt = cleaned[: min(len(cleaned), 1200)]
    return _clean_text(summary), _clean_text(excerpt)


def _summarize_youtube(url: str) -> SourceAnalysis:
    title = ""
    source_name = ""
    summary = ""
    published_at = ""
    excerpt = ""
    error = ""
    html_text = ""
    transcript = ""
    transcript_language = ""
    ytdlp_info = {}
    extraction_method = ""

    try:
        oembed = requests.get(
            "https://www.youtube.com/oembed",
            params={"url": url, "format": "json"},
            headers=DEFAULT_HEADERS,
            cookies=YOUTUBE_COOKIES,
            timeout=15,
        )
        if oembed.ok:
            payload = oembed.json()
            title = _clean_text(payload.get("title", ""))
            source_name = _clean_text(payload.get("author_name", ""))
    except Exception as exc:
        error = str(exc)

    try:
        ytdlp_info = _yt_dlp_extract_info(url)
        if ytdlp_info:
            title = title or _clean_text(ytdlp_info.get("title", ""))
            source_name = source_name or _clean_text(ytdlp_info.get("uploader", "") or ytdlp_info.get("channel", ""))
            summary = _clean_text(ytdlp_info.get("description", ""))
            subtitle_url, subtitle_lang = _pick_yt_dlp_caption(ytdlp_info)
            if subtitle_url:
                try:
                    transcript = _fetch_youtube_transcript(subtitle_url)
                    transcript_language = subtitle_lang
                    if transcript:
                        extraction_method = "youtube_subtitle"
                except Exception as exc:
                    error = str(exc)
        html_text = _fetch_html(url)
        if not title:
            title = _extract_title(html_text)
        if not source_name:
            source_name = _extract_meta(html_text, ["og:site_name"]) or "YouTube"
        if not transcript:
            player_response = _extract_json_blob(YT_INITIAL_PLAYER_RE, html_text)
            caption_tracks = (((player_response.get("captions") or {}).get("playerCaptionsTracklistRenderer") or {}).get("captionTracks") or [])
            try:
                transcript, transcript_language = _fetch_best_youtube_transcript(caption_tracks)
                if transcript:
                    extraction_method = "youtube_subtitle"
            except Exception as exc:
                if not error:
                    error = str(exc)
        if not summary:
            summary = _extract_meta(html_text, ["og:description", "description"])
        if not summary:
            m = re.search(r'"shortDescription"\s*:\s*"(.*?)"', html_text, flags=re.IGNORECASE | re.DOTALL)
            if m:
                try:
                    summary = json.loads(f'"{m.group(1)}"')
                except Exception:
                    summary = m.group(1)
        if not transcript:
            audio_path = _download_youtube_audio(url)
            if audio_path:
                transcript, transcript_language = _transcribe_audio(audio_path)
                if transcript:
                    extraction_method = "whisper_audio"
                try:
                    audio_path.unlink(missing_ok=True)
                    if audio_path.parent.name.startswith("yt-audio-"):
                        audio_path.parent.rmdir()
                except Exception:
                    pass
        transcript_summary, transcript_excerpt = _summarize_transcript(transcript)
        if transcript_summary:
            summary = transcript_summary
        summary = _clean_text(summary)
        if not summary:
            summary = "YouTube 页面公开摘要较少，建议结合标题与频道信息提炼脚本角度。"
        excerpt = transcript_excerpt or " ".join(_extract_paragraphs(html_text, limit=3)) or summary
        if transcript_language and transcript_language not in {source_name, title}:
            source_name = f"{source_name} · 字幕 {transcript_language}" if source_name else f"字幕 {transcript_language}"
    except Exception as exc:
        if not error:
            error = str(exc)

    if (summary or excerpt) and error and transcript:
        error = ""

    return SourceAnalysis(
        kind="youtube",
        url=url,
        title=title or "YouTube 视频",
        source_name=source_name or "YouTube",
        source_language=transcript_language,
        extraction_method=extraction_method or ("page_summary" if summary else ""),
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
