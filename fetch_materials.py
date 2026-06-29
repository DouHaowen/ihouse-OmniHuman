"""
素材搜索模块
根据关键词自动搜索并下载 Pexels 图片/视频素材
"""

import os
import re
import json
import hashlib
import time
import shutil
import tempfile
import threading
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus, urljoin, urlparse
import requests
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont, ImageStat
from material_library import (
    MATERIAL_LIBRARY_DIR,
    copy_material_to_output,
    list_material_library_items,
    register_material_file,
    search_material_library,
    update_material_library_item,
)

load_dotenv(override=False)

PRODUCTION_MATERIAL_LIBRARY_ENABLED = (
    os.getenv("PRODUCTION_MATERIAL_LIBRARY_ENABLED", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED = (
    os.getenv("OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_MATERIAL_LIBRARY_FIRST = (
    os.getenv("OPENNEWS_MATERIAL_LIBRARY_FIRST", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_LIBRARY_FALLBACK_MIN_SOURCE_IMAGES = max(
    0,
    min(10, int(os.getenv("OPENNEWS_LIBRARY_FALLBACK_MIN_SOURCE_IMAGES", "1") or "1")),
)
OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES = max(
    1,
    min(10, int(os.getenv("OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES", "4") or "4")),
)
OPENNEWS_MATERIAL_LIBRARY_DAILY_IMAGE_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_MATERIAL_LIBRARY_DAILY_IMAGE_LIMIT", "1") or "1"),
)
OPENNEWS_MATERIAL_LIBRARY_MONTHLY_IMAGE_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_MATERIAL_LIBRARY_MONTHLY_IMAGE_LIMIT", "5") or "5"),
)
OPENNEWS_MATERIAL_LIBRARY_USAGE_PATH = os.getenv(
    "OPENNEWS_MATERIAL_LIBRARY_USAGE_PATH",
    str(MATERIAL_LIBRARY_DIR.parent / "output" / "opennews_material_usage.json"),
).strip()
OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK = threading.Lock()
OPENNEWS_PEXELS_IMAGE_DAILY_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_PEXELS_IMAGE_DAILY_LIMIT", "3") or "3"),
)
OPENNEWS_PEXELS_STRICT_MATCH_ENABLED = (
    os.getenv("OPENNEWS_PEXELS_STRICT_MATCH_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_PEXELS_MIN_RELEVANCE_SCORE = max(
    8,
    min(80, int(os.getenv("OPENNEWS_PEXELS_MIN_RELEVANCE_SCORE", "28") or "28")),
)
OPENNEWS_PEXELS_EXACT_ENTITY_REQUIRED = (
    os.getenv("OPENNEWS_PEXELS_EXACT_ENTITY_REQUIRED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_NEWS_CARD_FALLBACK_ENABLED = (
    os.getenv("OPENNEWS_NEWS_CARD_FALLBACK_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_PEXELS_BATCH_REGISTRY_DIR = os.getenv(
    "OPENNEWS_PEXELS_BATCH_REGISTRY_DIR",
    str(MATERIAL_LIBRARY_DIR.parent / "output" / "opennews_batches" / "material_strategy"),
).strip()
OPENNEWS_SOURCE_IMAGE_DAILY_HASH_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_SOURCE_IMAGE_DAILY_HASH_LIMIT", "1") or "1"),
)
OPENNEWS_SOURCE_IMAGE_MONTHLY_HASH_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_SOURCE_IMAGE_MONTHLY_HASH_LIMIT", "5") or "5"),
)
OPENNEWS_SOURCE_IMAGES_PER_PAGE_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_SOURCE_IMAGES_PER_PAGE_LIMIT", "1") or "1"),
)
OPENNEWS_SOURCE_IMAGES_PER_DOMAIN_LIMIT = max(
    1,
    int(os.getenv("OPENNEWS_SOURCE_IMAGES_PER_DOMAIN_LIMIT", "2") or "2"),
)
PEXELS_API_KEY = os.getenv("PEXELS_API_KEY")
PEXELS_API_URL = "https://api.pexels.com/v1/search"
PEXELS_VIDEO_URL = "https://api.pexels.com/videos/search"
OPENNEWS_MAX_MATERIALS = 10
OPENNEWS_MAX_SOURCE_VIDEOS = 1
OPENNEWS_MAX_SOURCE_IMAGES = 10
OPENNEWS_AI_IMAGE_ENABLED = os.getenv("OPENNEWS_AI_IMAGE_ENABLED", "0").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_AI_IMAGE_REPLACE_SOURCE = os.getenv("OPENNEWS_AI_IMAGE_REPLACE_SOURCE", "1").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_AI_IMAGE_ONLY = os.getenv("OPENNEWS_AI_IMAGE_ONLY", "0").strip().lower() not in {"0", "false", "no", "off"}
OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL = (
    os.getenv("OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_MATERIAL_LIBRARY_ONLY = (
    os.getenv("OPENNEWS_MATERIAL_LIBRARY_ONLY", "0").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_REALTIME_SOURCE_REVIEW_ENABLED = (
    os.getenv("OPENNEWS_REALTIME_SOURCE_REVIEW_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_REALTIME_SOURCE_REVIEW_REQUIRED = (
    os.getenv("OPENNEWS_REALTIME_SOURCE_REVIEW_REQUIRED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_REALTIME_SOURCE_MAX_IMAGES = max(
    1,
    min(8, int(os.getenv("OPENNEWS_REALTIME_SOURCE_MAX_IMAGES", "4") or "4")),
)
OPENNEWS_REALTIME_SOURCE_CANDIDATE_LIMIT = max(
    6,
    min(120, int(os.getenv("OPENNEWS_REALTIME_SOURCE_CANDIDATE_LIMIT", "60") or "60")),
)
OPENNEWS_REALTIME_SOURCE_AUTO_IMPORT = (
    os.getenv("OPENNEWS_REALTIME_SOURCE_AUTO_IMPORT", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_QWEN_REVIEW_TIMEOUT_SECONDS = max(
    12,
    min(90, int(os.getenv("OPENNEWS_QWEN_REVIEW_TIMEOUT_SECONDS", "45") or "45")),
)
OPENNEWS_QWEN_REVIEW_MAX_SIDE = max(
    720,
    min(1800, int(os.getenv("OPENNEWS_QWEN_REVIEW_MAX_SIDE", "1280") or "1280")),
)
OPENNEWS_MATERIAL_VECTOR_ENABLED = (
    os.getenv("OPENNEWS_MATERIAL_VECTOR_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_MATERIAL_VECTOR_REQUIRED = (
    os.getenv("OPENNEWS_MATERIAL_VECTOR_REQUIRED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_MATERIAL_VECTOR_URL = os.getenv("OPENNEWS_MATERIAL_VECTOR_URL", "http://192.168.0.34:8897").strip().rstrip("/")
OPENNEWS_MATERIAL_VECTOR_TIMEOUT_SECONDS = max(
    2,
    min(30, int(os.getenv("OPENNEWS_MATERIAL_VECTOR_TIMEOUT_SECONDS", "8") or "8")),
)
OPENNEWS_IMAGE_SERVICE_URL = os.getenv("OPENNEWS_IMAGE_SERVICE_URL", "http://192.168.0.34:8894").strip().rstrip("/")
OPENNEWS_IMAGE_SERVICE_TOKEN = os.getenv("OPENNEWS_IMAGE_SERVICE_TOKEN", "local-image-5090").strip()
OPENNEWS_IMAGE_MAX_IMAGES = max(1, min(10, int(os.getenv("OPENNEWS_IMAGE_MAX_IMAGES", "8") or "8")))
OPENNEWS_IMAGE_MIN_IMAGES = max(1, min(OPENNEWS_IMAGE_MAX_IMAGES, int(os.getenv("OPENNEWS_IMAGE_MIN_IMAGES", "6") or "6")))
OPENNEWS_IMAGE_ASPECT_RATIO = os.getenv("OPENNEWS_IMAGE_ASPECT_RATIO", "square").strip().lower() or "square"
OPENNEWS_IMAGE_TIMEOUT_SECONDS = max(45, int(os.getenv("OPENNEWS_IMAGE_TIMEOUT_SECONDS", "360") or "360"))
OPENNEWS_IMAGE_MODEL = os.getenv("OPENNEWS_IMAGE_MODEL", "RealVisXL_V4.0_BakedVAE.safetensors").strip()
OPENNEWS_IMAGE_STEPS = max(8, min(50, int(os.getenv("OPENNEWS_IMAGE_STEPS", "40") or "40")))
OPENNEWS_IMAGE_CFG = float(os.getenv("OPENNEWS_IMAGE_CFG", "6.8") or "6.8")
OPENNEWS_SOURCE_IMAGE_SKIN_SAFETY_ENABLED = (
    os.getenv("OPENNEWS_SOURCE_IMAGE_SKIN_SAFETY_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_SOURCE_IMAGE_MAX_SKIN_RATIO = max(0.05, min(0.8, float(os.getenv("OPENNEWS_SOURCE_IMAGE_MAX_SKIN_RATIO", "0.28") or "0.28")))
OPENNEWS_BLANK_IMAGE_CHECK_ENABLED = (
    os.getenv("OPENNEWS_BLANK_IMAGE_CHECK_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
OPENNEWS_IMAGE_NEGATIVE_PROMPT = os.getenv(
    "OPENNEWS_IMAGE_NEGATIVE_PROMPT",
    (
        "low quality, worst quality, blurry, soft focus, motion blur, plastic skin, waxy texture, cartoon, anime, illustration, "
        "3d render, CGI, fake UI, readable text, random letters, watermark, logo, brand mark, subtitles, "
        "poster, infographic, collage, split screen, distorted hands, deformed people, bad anatomy, oversaturated, noisy, jpeg artifacts, "
        "nudity, nude, naked, explicit, sexual, erotic, pornographic, lingerie, underwear, bikini, swimsuit, "
        "cleavage, bare chest, exposed skin, shirtless, see-through clothing, intimate pose, fetish, "
        "patient body, medical nudity, surgery close-up, wound, blood, gore, anatomy close-up, body scan of torso"
    ),
).strip()

OPENNEWS_IMAGE_SAFE_SUFFIX = (
    "safe for YouTube news use, brand-safe editorial image, family-safe, fully clothed adults only, "
    "business attire when people are present, no exposed skin, no nudity, no sexual content, "
    "no underwear, no swimwear, no glamour model, no patient body, no surgery, no blood, "
    "no graphic medical content"
)

OPENNEWS_IMAGE_CAMERA_STYLES = [
    "wide establishing shot with a clear single subject",
    "medium documentary shot with realistic foreground depth",
    "close-up detail shot with shallow depth of field",
    "over-the-shoulder newsroom b-roll with a clean professional setup",
    "cinematic telephoto compression, realistic editorial press photo",
    "low angle corporate exterior shot with natural light",
    "clean macro detail shot, crisp textures, premium lens",
    "evening city documentary shot, realistic ambient light",
]

OPENNEWS_IMAGE_DOMAIN_STYLES = {
    "ai": (
        "AI data center, server racks, GPU infrastructure, semiconductor research lab, "
        "enterprise AI pricing dashboard, engineers monitoring computing systems, high-tech investment atmosphere"
    ),
    "technology": (
        "AI data center, server racks, GPU infrastructure, semiconductor research lab, "
        "enterprise AI software office, engineers monitoring computing systems, high-tech investment atmosphere"
    ),
    "cybersecurity": (
        "cybersecurity operations center, network threat monitoring dashboard, secure server racks, "
        "multi-factor authentication, phishing detection, encrypted digital lock, professional cyber defense atmosphere"
    ),
    "finance": (
        "global finance district, institutional investors, stock market screens, modern trading floor, "
        "sovereign wealth fund office atmosphere"
    ),
    "real_estate": (
        "modern residential district, apartment construction, real estate market, city skyline, "
        "property investment office"
    ),
    "military": (
        "defense briefing room, military equipment silhouettes, naval and air defense context, "
        "official strategic analysis atmosphere"
    ),
    "politics": (
        "government building exterior, diplomatic meeting room, policy briefing atmosphere, "
        "official documents and flags without readable text"
    ),
}


def _asset_kind_for_suffix(path: str) -> str:
    suffix = os.path.splitext(str(path))[1].lower()
    if suffix in {".mp4", ".mov", ".m4v", ".webm"}:
        return "video"
    return "image"


def _material_entry(path: str, *, kind: str | None = None, source: str = "pexels") -> dict:
    return {
        "path": path,
        "kind": kind or _asset_kind_for_suffix(path),
        "source": source,
        "name": os.path.basename(path),
    }


def search_photos(keyword: str, count: int = 3) -> list:
    """
    搜索图片素材，优先竖图，其次方图，最后横图。
    返回图片URL列表
    """
    headers = {"Authorization": PEXELS_API_KEY}
    collected = []
    seen = set()

    for orientation in ["portrait", "square", "landscape"]:
        params = {
            "query": keyword,
            "per_page": count,
            "orientation": orientation,
        }

        response = requests.get(PEXELS_API_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        photos = data.get("photos", [])

        for photo in photos:
            url = photo["src"]["large"]
            if url in seen:
                continue
            seen.add(url)
            collected.append(
                {
                    "url": url,
                    "photographer": photo["photographer"],
                    "alt": photo.get("alt", keyword),
                    "width": photo.get("width"),
                    "height": photo.get("height"),
                    "orientation": orientation,
                }
            )
            if len(collected) >= count:
                return collected

    return collected


def search_videos(keyword: str, count: int = 2) -> list:
    """
    搜索视频素材，优先竖屏，其次方屏，最后横屏。
    """
    headers = {"Authorization": PEXELS_API_KEY}
    results = []
    seen = set()

    for orientation in ["portrait", "square", "landscape"]:
        params = {
            "query": keyword,
            "per_page": count,
            "orientation": orientation,
        }

        response = requests.get(PEXELS_VIDEO_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        videos = data.get("videos", [])

        for v in videos:
            files = sorted(v.get("video_files", []), key=lambda x: x.get("height", 0))
            best_file = next(
                (f for f in files if f.get("height", 0) >= 720),
                files[-1] if files else None
            )
            if not best_file:
                continue
            url = best_file["link"]
            if url in seen:
                continue
            seen.add(url)
            results.append({
                "url": url,
                "width": best_file.get("width"),
                "height": best_file.get("height"),
                "orientation": orientation,
            })
            if len(results) >= count:
                return results

    return results


def download_file(url: str, output_path: str) -> str:
    """下载文件到本地"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return output_path


def _clean_ai_prompt_piece(value: str, *, max_chars: int = 220) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars].strip()


def _opennews_safe_ai_subject(subject: str) -> str:
    text = _clean_ai_prompt_piece(subject, max_chars=260)
    lower = text.lower()
    ai_model_markers = [
        "ai model", "model cost", "model price", "model fee", "foundation model",
        "llm", "large language model", "artificial intelligence market",
        "模型费用", "模型价格", "模型成本", "大模型", "人工智能市场", "生成式ai",
    ]
    if any(marker in lower or marker in text for marker in ai_model_markers):
        return (
            "enterprise artificial intelligence pricing dashboard on a laptop, GPU server racks in the background, "
            "business analysts reviewing AI infrastructure costs in a modern office, no fashion model, "
            "no human body emphasis, no exposed skin"
        )
    medical_markers = [
        "medical", "healthcare", "clinical", "doctor", "hospital", "imaging", "diagnosis",
        "医学", "医疗", "临床", "医生", "医院", "影像", "诊断", "药物研发",
    ]
    if any(marker in lower or marker in text for marker in medical_markers):
        return (
            "healthcare AI software dashboard in a modern hospital office, fully clothed doctor "
            "reviewing abstract analytics on a computer monitor, laboratory equipment and data screens, "
            "no patients visible, no human body, no anatomy images, no surgery"
        )
    return text


def _opennews_visual_domain_from_text(*parts: str) -> str:
    text = " ".join(str(part or "") for part in parts).lower()
    domain_markers = {
        "cybersecurity": ("cybersecurity", "cyber crime", "cybercrime", "hacker", "phishing", "scam", "network security", "网络安全", "网络犯罪", "诈骗", "黑客", "网络攻击"),
        "ai": ("ai", "artificial intelligence", "人工智能", "data center", "gpu", "chip", "semiconductor", "openai", "anthropic", "nvidia"),
        "real_estate": ("real estate", "housing", "residential", "property", "房产", "住宅", "不动产", "地产"),
        "finance": ("stock", "market", "fund", "investment", "investor", "sovereign wealth", "finance", "economy", "股市", "基金", "投资", "金融"),
        "military": ("military", "defense", "missile", "drone", "war", "army", "navy", "军事", "国防", "导弹", "无人机"),
        "politics": ("white house", "congress", "government", "policy", "minister", "diplomacy", "政治", "政府", "政策", "外交"),
    }
    for domain, markers in domain_markers.items():
        if any(marker in text for marker in markers):
            return domain
    return "general"


def _opennews_domain_style(domain: str) -> str:
    return OPENNEWS_IMAGE_DOMAIN_STYLES.get(domain, "modern global news documentary scene, real-world editorial b-roll atmosphere")


def _opennews_english_visual_subject(*parts: str) -> str:
    text = " ".join(str(part or "") for part in parts).lower()
    cn_text = " ".join(str(part or "") for part in parts)
    rules = [
        (
            ("芯片", "半导体", "gpu", "nvidia", "semiconductor", "chip", "晶圆", "算力"),
            "semiconductor wafers and GPU chips on a clean laboratory table, engineers in the background inspecting AI hardware",
        ),
        (
            ("数据中心", "服务器", "云计算", "算力", "data center", "server", "cloud computing"),
            "large AI data center server racks with cool blue lighting, network cables, engineers monitoring infrastructure",
        ),
        (
            ("模型费用", "模型价格", "模型成本", "ai model", "model cost", "model price", "subscription"),
            "enterprise AI subscription cost dashboard on a laptop, business analysts reviewing cloud computing expenses",
        ),
        (
            ("人工智能", "生成式ai", "大模型", "openai", "anthropic", "artificial intelligence", "llm"),
            "modern enterprise AI operations room, large screens showing abstract machine learning workflows without readable text",
        ),
        (
            ("房产", "住宅", "不动产", "房地产", "租金", "housing", "property", "real estate", "mortgage"),
            "modern residential apartment buildings and real estate contract documents, bright clean city property market scene",
        ),
        (
            ("股市", "股票", "投资", "金融", "通胀", "利率", "ipo", "stock", "market", "investor", "inflation", "fed"),
            "professional financial newsroom, trading screens with abstract market graphics, analysts reviewing investment data",
        ),
        (
            ("移民", "签证", "入境", "visa", "immigration", "border", "passport"),
            "passport and visa documents on an airport immigration desk, official travel process atmosphere, no readable personal data",
        ),
        (
            ("军事", "国防", "导弹", "无人机", "军舰", "战机", "military", "defense", "missile", "drone", "warship", "fighter"),
            "defense briefing room with maps and military equipment silhouettes, official analysis atmosphere, no gore, no combat casualties",
        ),
        (
            ("白宫", "国会", "政府", "政策", "外交", "总统", "white house", "congress", "government", "policy", "diplomacy"),
            "government building exterior and formal press briefing room, official policy news atmosphere, no unrelated politicians close-up",
        ),
        (
            ("石油", "油价", "霍尔木兹", "oil", "crude", "hormuz", "tanker"),
            "oil tanker at sea near a strategic shipping route, energy market news atmosphere, realistic maritime documentary photo",
        ),
        (
            ("航空", "飞机", "航展", "air show", "aircraft", "aviation", "jet"),
            "modern aircraft on an airshow runway, aerospace industry exhibition atmosphere, realistic telephoto documentary shot",
        ),
    ]
    haystack = f"{text} {cn_text}"
    for markers, subject in rules:
        if any(marker in haystack for marker in markers):
            return subject
    return ""


def _opennews_compose_ai_prompt(
    *,
    subject: str,
    visual_need: str,
    script_context: str,
    queries: list[str],
    theme_title: str,
    index: int,
) -> str:
    domain = _opennews_visual_domain_from_text(subject, visual_need, script_context, " ".join(queries), theme_title)
    camera_style = OPENNEWS_IMAGE_CAMERA_STYLES[index % len(OPENNEWS_IMAGE_CAMERA_STYLES)]
    domain_style = _opennews_domain_style(domain)
    mapped_subject = _opennews_english_visual_subject(subject, visual_need, script_context, " ".join(queries), theme_title)
    focused_subject = mapped_subject or _opennews_safe_ai_subject(
        ", ".join(part for part in [subject, visual_need, theme_title] if part) or script_context
    )
    context = _clean_ai_prompt_piece(script_context, max_chars=180)
    query_text = ", ".join(queries[:4])
    prompt_parts = [
        focused_subject,
        domain_style,
        f"{camera_style}, high-end realistic editorial news photography, premium documentary b-roll still",
        "award-winning photorealistic RAW photo, professional full-frame camera, 35mm or 50mm lens look, natural available light, realistic lens perspective, sharp focus, crisp textures, realistic materials, balanced contrast, rich but natural color grading",
        "clean professional composition, credible business news visual, single clear subject, strong foreground-background separation, no collage, no symbolic generic illustration",
        "match the current narration beat literally; the visible scene must correspond to the narration topic; do not show politicians, government meetings, hospitals, fashion models, or unrelated people unless the beat explicitly asks for them",
    ]
    if query_text:
        prompt_parts.append(f"visual entities to imply: {query_text}")
    if context:
        prompt_parts.append(f"news context to match: {context}")
    prompt_parts.append("no readable text, no fake letters, no charts with text, no logos, no watermark")
    prompt_parts.append(OPENNEWS_IMAGE_SAFE_SUFFIX)
    return ", ".join(part for part in prompt_parts if part)


def _opennews_script_visual_beats(script: str, *, limit: int = 8) -> list[str]:
    text = _clean_ai_prompt_piece(script, max_chars=1600)
    if not text:
        return []
    pieces = [piece.strip(" ，。！？；,.!?;:\n\t") for piece in re.split(r"[。！？；!?;\n]+", text)]
    if len([piece for piece in pieces if piece]) < 4:
        pieces = [piece.strip(" ，。！？；,.!?;:\n\t") for piece in re.split(r"[，、,：:]+", text)]
    pieces = [piece for piece in pieces if piece]
    beats: list[str] = []
    current = ""
    for piece in pieces:
        if len(current) < 90 and len(f"{current} {piece}".strip()) <= 180:
            current = f"{current} {piece}".strip()
            continue
        beats.append(current[:220])
        current = piece
        if len(beats) >= limit:
            break
    if current and len(beats) < limit:
        beats.append(current[:220])
    return beats[:limit]


def _append_opennews_ai_prompt(
    prompts: list[dict],
    seen: set[str],
    *,
    subject: str,
    visual_need: str,
    script_context: str,
    queries: list[str],
    theme_title: str,
    index: int,
    limit: int,
) -> bool:
    visual_subject = _opennews_english_visual_subject(subject, visual_need, script_context, " ".join(queries), theme_title)
    prompt = _opennews_compose_ai_prompt(
        subject=subject,
        visual_need=visual_need,
        script_context=script_context,
        queries=queries,
        theme_title=theme_title,
        index=index,
    )
    key_source = "|".join([prompt, script_context, theme_title, str(index)])
    key = hashlib.sha1(key_source.encode("utf-8", errors="ignore")).hexdigest()
    if not key or key in seen:
        return False
    seen.add(key)
    prompts.append({
        "prompt": prompt,
        "news_hint": script_context or visual_need,
        "theme_index": index,
        "theme_title": theme_title,
        "queries": queries,
        "visual_subject": visual_subject,
    })
    return len(prompts) >= limit


def _opennews_ai_image_prompts(seg: dict, *, limit: int = 10) -> list[dict]:
    """Build stable image prompts from the OpenNews visual plan."""
    prompts: list[dict] = []
    seen: set[str] = set()
    script_text = str(seg.get("script") or "")
    themes = seg.get("material_theme_plan") or []
    if isinstance(themes, list):
        for index, theme in enumerate(themes):
            if not isinstance(theme, dict):
                continue
            queries = [
                _clean_ai_prompt_piece(query, max_chars=80)
                for query in (theme.get("queries") or [])
                if _clean_ai_prompt_piece(query, max_chars=80)
            ]
            visual_need = _clean_ai_prompt_piece(theme.get("visual_need") or theme.get("title") or "", max_chars=180)
            script_context = _clean_ai_prompt_piece(theme.get("script") or "", max_chars=220)
            subject = ", ".join(queries[:3]) or visual_need or script_context
            if not subject:
                continue
            theme_title = _clean_ai_prompt_piece(theme.get("title") or visual_need or subject, max_chars=120)
            if _append_opennews_ai_prompt(
                prompts,
                seen,
                subject=subject,
                visual_need=visual_need,
                script_context=script_context,
                queries=queries,
                theme_title=theme_title,
                index=index,
                limit=limit,
            ):
                return prompts

    # 主题计划不足时，用口播文案本身拆成画面节拍补齐，避免整条新闻只靠一张泛图。
    if len(prompts) < OPENNEWS_IMAGE_MIN_IMAGES:
        base_queries = [
            _clean_ai_prompt_piece(value, max_chars=80)
            for value in [
                str(seg.get("material_keyword") or ""),
                str(seg.get("material_search_keyword") or ""),
            ]
            if _clean_ai_prompt_piece(value, max_chars=80)
        ][:3]
        for beat_index, beat in enumerate(_opennews_script_visual_beats(script_text, limit=limit)):
            if len(prompts) >= limit:
                break
            title = f"新闻画面 {beat_index + 1}"
            if _append_opennews_ai_prompt(
                prompts,
                seen,
                subject=beat,
                visual_need=f"visualize this narration beat as safe editorial b-roll: {beat}",
                script_context=beat,
                queries=base_queries,
                theme_title=title,
                index=len(prompts),
                limit=limit,
            ):
                break

    if len(prompts) < OPENNEWS_IMAGE_MIN_IMAGES:
        script_context = _clean_ai_prompt_piece(script_text, max_chars=260)
        keyword = _clean_ai_prompt_piece(
            " ".join([
                str(seg.get("material_keyword") or ""),
                str(seg.get("material_search_keyword") or ""),
            ]),
            max_chars=180,
        )
        domain = _opennews_visual_domain_from_text(keyword, script_context)
        supplemental_by_domain = {
            "ai": [
                "wide shot of AI data center server racks",
                "close-up of GPU chips and semiconductor wafers",
                "enterprise AI software dashboard in a modern office",
                "engineers monitoring cloud computing infrastructure",
                "business analysts reviewing AI investment costs",
                "high-tech research lab with abstract machine learning screens",
            ],
            "technology": [
                "semiconductor laboratory with engineers",
                "modern technology company office and software dashboard",
                "close-up of hardware components and circuit boards",
                "data center operations room",
                "product development meeting with laptops and prototypes",
                "clean macro shot of advanced electronics",
            ],
            "finance": [
                "professional trading floor with abstract market screens",
                "financial district exterior with morning light",
                "analysts reviewing investment documents",
                "central bank and interest rate policy atmosphere",
                "close-up of financial charts without readable text",
                "business newsroom discussing market movement",
            ],
            "real_estate": [
                "modern residential apartment exterior",
                "real estate contract documents and house keys",
                "city housing construction site",
                "property agent reviewing housing market data",
                "bright residential neighborhood street",
                "apartment building lobby and property market atmosphere",
            ],
            "military": [
                "defense briefing room with maps and equipment silhouettes",
                "naval ship at sea in a documentary telephoto shot",
                "military drone silhouette in a controlled test environment",
                "air defense radar and command center",
                "fighter aircraft on runway at a defense exhibition",
                "official strategic analysis room, no casualties",
            ],
            "politics": [
                "government building exterior",
                "formal press briefing room without identifiable faces",
                "policy documents on a desk without readable text",
                "diplomatic meeting room, empty chairs and flags",
                "city government district establishing shot",
                "official newsroom policy analysis atmosphere",
            ],
            "general": [
                "modern global news documentary establishing shot",
                "professional newsroom b-roll scene",
                "city exterior related to the news topic",
                "documents and laptop on a clean editorial desk",
                "wide urban documentary photo matching the topic",
                "neutral business news visual without unrelated people",
            ],
        }
        supplemental_angles = supplemental_by_domain.get(domain, supplemental_by_domain["general"])
        for angle in supplemental_angles:
            if len(prompts) >= min(limit, OPENNEWS_IMAGE_MIN_IMAGES):
                break
            _append_opennews_ai_prompt(
                prompts,
                seen,
                subject=f"{keyword}, {angle}",
                visual_need=angle,
                script_context=script_context,
                queries=[query for query in [keyword] if query],
                theme_title=f"补充安全画面 {len(prompts) + 1}",
                index=len(prompts),
                limit=limit,
            )

    if not prompts:
        fallback = _clean_ai_prompt_piece(
            " ".join([
                str(seg.get("material_search_keyword") or ""),
                str(seg.get("material_keyword") or ""),
                str(seg.get("script") or "")[:500],
            ]),
            max_chars=260,
        )
        if fallback:
            prompts.append({
                "prompt": _opennews_compose_ai_prompt(
                    subject=fallback,
                    visual_need=str(seg.get("material_desc") or ""),
                    script_context=_clean_ai_prompt_piece(seg.get("script") or "", max_chars=220),
                    queries=[],
                    theme_title=str(seg.get("material_keyword") or "OpenNews AI素材"),
                    index=0,
                ),
                "news_hint": _clean_ai_prompt_piece(seg.get("script") or "", max_chars=220),
                "theme_index": 0,
                "theme_title": str(seg.get("material_keyword") or "OpenNews AI素材"),
                "queries": [],
            })
    return prompts[:limit]


def _generate_opennews_ai_image_materials(seg: dict, output_dir: str, segment_index: int, existing_count: int) -> list[dict]:
    if not OPENNEWS_AI_IMAGE_ENABLED or not OPENNEWS_IMAGE_SERVICE_URL:
        return []

    prompts = _opennews_ai_image_prompts(seg, limit=OPENNEWS_IMAGE_MAX_IMAGES)
    if not prompts:
        return []

    materials_dir = os.path.join(output_dir, "materials")
    os.makedirs(materials_dir, exist_ok=True)
    generated: list[dict] = []
    headers = {"Content-Type": "application/json"}
    if OPENNEWS_IMAGE_SERVICE_TOKEN:
        headers["X-Token"] = OPENNEWS_IMAGE_SERVICE_TOKEN

    for prompt_index, prompt_item in enumerate(prompts):
        material_index = existing_count + len(generated)
        job_seed = hashlib.sha1(
            f"{segment_index}:{prompt_index}:{prompt_item.get('prompt')}:{time.time_ns()}".encode("utf-8")
        ).hexdigest()[:12]
        payload = {
            "job_id": f"opennews_seg{segment_index:02d}_{job_seed}",
            "prompt": prompt_item.get("prompt") or "",
            "news_hint": prompt_item.get("news_hint") or "",
            "aspect_ratio": OPENNEWS_IMAGE_ASPECT_RATIO,
            "timeout_seconds": OPENNEWS_IMAGE_TIMEOUT_SECONDS,
            "model": OPENNEWS_IMAGE_MODEL,
            "steps": OPENNEWS_IMAGE_STEPS,
            "cfg": OPENNEWS_IMAGE_CFG,
            "negative_prompt": OPENNEWS_IMAGE_NEGATIVE_PROMPT,
        }
        try:
            response = requests.post(
                f"{OPENNEWS_IMAGE_SERVICE_URL}/generate",
                json=payload,
                headers=headers,
                timeout=(10, OPENNEWS_IMAGE_TIMEOUT_SECONDS + 30),
            )
            response.raise_for_status()
            data = response.json()
            images = data.get("images") or []
            if not data.get("ok") or not images:
                raise RuntimeError(data.get("error") or "图片服务未返回图片")
            image_url = str(images[0].get("url") or "").strip()
            if not image_url:
                raise RuntimeError("图片服务返回缺少下载地址")
            download_url = urljoin(f"{OPENNEWS_IMAGE_SERVICE_URL}/", image_url.lstrip("/"))
            image_response = requests.get(download_url, headers=headers, stream=True, timeout=45)
            image_response.raise_for_status()
            content_type = image_response.headers.get("Content-Type", "")
            ext = _extension_from_url_or_content_type(download_url, content_type, ".png")
            output_path = os.path.join(materials_dir, f"material_{segment_index:02d}_ai_{material_index}{ext}")
            with open(output_path, "wb") as f:
                for chunk in image_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            if os.path.getsize(output_path) < 25 * 1024:
                os.remove(output_path)
                raise RuntimeError("生成图片文件过小，已跳过")
            entry = _material_entry(output_path, kind="image", source="opennews_ai_image")
            entry["title"] = prompt_item.get("theme_title") or "OpenNews AI生成素材"
            entry["prompt"] = prompt_item.get("prompt") or ""
            if prompt_item.get("visual_subject"):
                entry["visual_subject"] = prompt_item.get("visual_subject")
            entry["image_service_url"] = OPENNEWS_IMAGE_SERVICE_URL
            entry["image_job_id"] = data.get("job_id") or payload["job_id"]
            if prompt_item.get("theme_index") is not None:
                entry["theme_index"] = prompt_item.get("theme_index")
            if prompt_item.get("queries"):
                entry["related_query"] = " | ".join(prompt_item.get("queries") or [])
            generated.append(entry)
            print(f"  ✅ 已生成5090 AI新闻素材：{os.path.basename(output_path)}")
        except Exception as exc:
            print(f"  ⚠️ 5090 AI新闻素材生成失败：{prompt_item.get('theme_title') or prompt_item.get('prompt')}｜{exc}")
            continue
    return generated


def _extension_from_url_or_content_type(url: str, content_type: str = "", fallback: str = ".jpg") -> str:
    suffix = os.path.splitext(urlparse(str(url or "")).path)[1].lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v", ".webm"}:
        return suffix
    content_type = (content_type or "").lower()
    if "mp4" in content_type:
        return ".mp4"
    if "quicktime" in content_type:
        return ".mov"
    if "webm" in content_type:
        return ".webm"
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    if "jpeg" in content_type or "jpg" in content_type:
        return ".jpg"
    return fallback


def _source_material_url_variants(url: str) -> list[str]:
    """Try larger variants for news thumbnail URLs before giving up."""
    url = str(url or "").strip()
    variants: list[str] = []

    def add(candidate: str) -> None:
        if candidate and candidate not in variants:
            variants.append(candidate)

    # VOA/RFA-style GDB images often expose tiny thumbnails as `_w100_`.
    # The same asset usually has larger `_w650_`, `_w1023_`, `_w1200_` variants.
    if "gdb.voanews.com" in url.lower() or "gdb.rferl.org" in url.lower():
        for width in (1200, 1023, 800, 650, 480):
            add(re.sub(r"_w\d+_", f"_w{width}_", url))
        for width in (1200, 1023, 800, 650, 480):
            add(re.sub(r"_w\d+(_r\d+)", f"_w{width}\\1", url))
    add(url)
    return variants


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_source_material(url: str, output_dir: str, segment_index: int, material_index: int, kind: str = "") -> str:
    os.makedirs(os.path.join(output_dir, "materials"), exist_ok=True)
    last_error = ""
    for download_url in _source_material_url_variants(url):
        if _looks_like_unsafe_source_material_url(download_url):
            last_error = "素材 URL 命中成人/裸露站点黑名单，已跳过"
            continue
        try:
            response = requests.get(download_url, stream=True, timeout=25, headers={"User-Agent": "iHouse-OpenNews-Media/0.1"})
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            guessed_kind = kind or ("video" if "video" in content_type.lower() or re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", download_url, flags=re.I) else "image")
            lowered_content_type = content_type.lower()
            if "text/html" in lowered_content_type:
                raise RuntimeError("来源链接返回 HTML 页面，不是可下载素材")
            if guessed_kind == "image" and lowered_content_type and not any(token in lowered_content_type for token in ("image", "octet-stream")):
                raise RuntimeError(f"来源链接不是图片素材：{content_type}")
            if guessed_kind == "video" and lowered_content_type and not any(token in lowered_content_type for token in ("video", "octet-stream", "binary")):
                raise RuntimeError(f"来源链接不是视频素材：{content_type}")
            ext = _extension_from_url_or_content_type(download_url, content_type, ".mp4" if guessed_kind == "video" else ".jpg")
            output_path = os.path.join(output_dir, "materials", f"material_{segment_index:02d}_source_{material_index}{ext}")
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            file_size = os.path.getsize(output_path)
            if guessed_kind == "video" and file_size < 200 * 1024:
                os.remove(output_path)
                raise RuntimeError("视频素材文件过小，已跳过")
            if guessed_kind != "video" and file_size < 25 * 1024:
                os.remove(output_path)
                raise RuntimeError("图片素材文件过小，已跳过")
            if guessed_kind != "video" and _downloaded_image_has_unsafe_skin_ratio(output_path):
                os.remove(output_path)
                raise RuntimeError("图片疑似包含大面积裸露/皮肤区域，已安全跳过")
            if download_url != url:
                print(f"  ↗️ 已将新闻缩略图升级为高清素材：{os.path.basename(urlparse(download_url).path)}")
            return output_path
        except Exception as exc:
            last_error = str(exc)
            continue
    raise RuntimeError(last_error or "新闻来源素材下载失败")


def _opennews_is_blank_or_white_image(path: str) -> tuple[bool, str]:
    if not OPENNEWS_BLANK_IMAGE_CHECK_ENABLED:
        return False, ""
    suffix = os.path.splitext(str(path or ""))[1].lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        return False, ""
    try:
        with Image.open(path) as image:
            image = image.convert("RGB").resize((96, 96))
            stat = ImageStat.Stat(image)
            mean = sum(stat.mean) / 3
            std = sum(stat.stddev) / 3
            extrema = image.getextrema()
            bright_pixels = 0
            dark_pixels = 0
            total = 0
            for red, green, blue in image.getdata():
                total += 1
                brightness = (red + green + blue) / 3
                if brightness >= 245:
                    bright_pixels += 1
                if brightness <= 18:
                    dark_pixels += 1
            bright_ratio = bright_pixels / max(total, 1)
            dark_ratio = dark_pixels / max(total, 1)
            channel_ranges = [high - low for low, high in extrema]
            if bright_ratio >= 0.92 and std <= 28:
                return True, f"疑似白底/空白图：bright={bright_ratio:.2f}, std={std:.1f}"
            if mean >= 248 and max(channel_ranges or [0]) <= 18:
                return True, f"疑似纯白图：mean={mean:.1f}, range={max(channel_ranges or [0])}"
            if std <= 4 and (bright_ratio >= 0.65 or dark_ratio >= 0.65):
                return True, f"疑似纯色占位图：std={std:.1f}"
    except Exception as exc:
        return True, f"图片无法解析，已跳过：{exc}"
    return False, ""


def _opennews_material_path_is_usable(path: str, kind: str = "") -> tuple[bool, str]:
    if not path or not os.path.exists(path):
        return False, "素材文件不存在"
    try:
        if os.path.getsize(path) <= 0:
            return False, "素材文件为空"
    except Exception:
        return False, "素材文件无法读取"
    resolved_kind = (kind or _asset_kind_for_suffix(path) or "").lower()
    if resolved_kind != "video":
        is_blank, reason = _opennews_is_blank_or_white_image(path)
        if is_blank:
            return False, reason
    return True, ""


def _opennews_filter_usable_materials(material_items: list[dict], material_paths: list[str]) -> tuple[list[dict], list[str], list[dict]]:
    kept_items: list[dict] = []
    kept_paths: list[str] = []
    rejected: list[dict] = []
    seen_paths: set[str] = set()
    for item in material_items:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        if not path or path in seen_paths:
            continue
        ok, reason = _opennews_material_path_is_usable(path, str(item.get("kind") or ""))
        if not ok:
            rejected.append({"path": path, "source": item.get("source") or "", "reason": reason})
            try:
                if str(item.get("source") or "") != "library":
                    os.remove(path)
            except Exception:
                pass
            print(f"  ⚠️ OpenNews 素材安全过滤：{reason}｜{os.path.basename(path)}")
            continue
        seen_paths.add(path)
        kept_items.append(item)
        kept_paths.append(path)
    for path in material_paths:
        text_path = str(path or "").strip()
        if not text_path or text_path in seen_paths:
            continue
        ok, reason = _opennews_material_path_is_usable(text_path)
        if not ok:
            rejected.append({"path": text_path, "source": "material_path", "reason": reason})
            print(f"  ⚠️ OpenNews 素材路径过滤：{reason}｜{os.path.basename(text_path)}")
            continue
        seen_paths.add(text_path)
        kept_paths.append(text_path)
    return kept_items, kept_paths, rejected


SOURCE_MATERIAL_BAD_TOKENS = (
    "favicon",
    "apple-touch-icon",
    "sprite",
    "/icons/",
    "/icon/",
    "logo",
    "avatar",
    "author",
    "profile",
    "social",
    "share",
    "tracking",
    "pixel",
    "spacer",
    "blank",
    "placeholder",
    "advert",
    "/ads/",
    "banner-ad",
)

SOURCE_MATERIAL_ADULT_TOKENS = (
    "porn", "porno", "xxx", "sex", "sexy", "adult", "erotic", "hentai", "jav",
    "avdebut", "avdebyu", "nude", "naked", "nsfw", "boobs", "breast", "pussy",
    "lingerie", "underwear", "bikini", "swimsuit", "cleavage", "shirtless",
    "topless", "see-through", "fetish", "escort", "only fans", "onlyfans",
    "patient body", "anatomy", "surgery", "wound", "gore",
    "eporner", "xvideos", "xnxx", "pornhub", "redtube", "youporn", "xhamster",
    "spankbang", "tube8", "youjizz", "brazzers", "onlyfans", "chaturbate",
    "camgirl", "stripchat", "bongacams", "javhd", "javdb", "missav",
    "fc2ppv", "tokyomotion", "mgstage",
)

OPENNEWS_VISUAL_DOMAIN_TOKENS = {
    "cybersecurity": {
        "cyber", "cybersecurity", "cyber security", "cyber crime", "cybercrime",
        "network security", "hacker", "hackers", "hacking", "phishing", "scam",
        "ransomware", "malware", "data breach", "password", "authentication",
        "two-factor", "mfa", "firewall", "encryption", "encrypted", "lock",
        "threat", "vulnerability", "computer security", "网络安全", "网络犯罪",
        "网络攻击", "黑客", "诈骗", "钓鱼", "勒索软件", "恶意软件", "数据泄露",
        "密码", "认证", "防火墙", "加密", "漏洞", "威胁",
    },
    "technology": {
        "ai", "artificial intelligence", "technology", "tech", "software", "app",
        "chip", "semiconductor", "nvidia", "openai", "anthropic", "meta",
        "facebook", "spacex", "tesla", "apple", "microsoft", "google", "alphabet",
        "amazon", "siri", "wwdc", "iphone", "data center", "robot", "startup",
        "smart glasses", "glasses", "eyewear", "virtual reality", "augmented reality",
        "mixed reality", "vr", "ar", "headset", "wearable", "wearables",
    },
    "finance": {
        "stock", "stocks", "market", "nasdaq", "nyse", "wall street", "shares",
        "ipo", "earnings", "investor", "investors", "inflation", "fed",
        "interest rate", "bank", "finance", "economy", "trading", "tariff",
        "currency", "forex", "exchange rate", "dollar", "pound", "pound sterling",
        "british pound", "yen", "japanese yen", "banknote", "cash", "calculator",
        "financial analysis", "chart", "graph",
    },
    "real_estate": {
        "real estate", "housing", "home", "homes", "house", "houses", "property",
        "rental", "rent", "landlord", "mortgage", "dscr", "non-qm", "single-family",
        "residential", "apartment", "investor-owned", "房产", "房地产", "住宅",
        "房贷", "租金", "房东", "单户住宅", "不动产",
    },
    "military": {
        "military", "defense", "war", "army", "navy", "air force", "fighter",
        "jet", "ship", "warship", "destroyer", "carrier", "missile", "drone",
        "uav", "troops", "ukraine", "russia", "iran", "israel", "gaza",
    },
    "politics": {
        "white house", "congress", "parliament", "minister", "president",
        "government", "policy", "diplomacy", "spokesperson", "press briefing",
        "election", "sanction", "foreign ministry", "cabinet",
    },
    "society": {
        "school", "student", "students", "exam", "education", "university",
        "hospital", "police", "festival", "shooting", "fire", "earthquake",
        "tsunami", "city", "people", "community",
    },
}

OPENNEWS_WRONG_DOMAIN_BLOCKS = {
    "cybersecurity": {
        "shopping", "online shopping", "cart", "product recall", "recall", "airshow",
        "sports", "selfie", "fashion", "beauty", "warehouse", "bunnings",
        "vacuum", "pacific airshow", "shoe", "shoes", "购物", "网购", "召回",
        "航展", "自拍", "时尚", "仓库", "鞋",
    },
    "technology": {"white house", "parliament", "congress", "press briefing", "government meeting", "cabinet meeting", "foreign ministry", "diplomacy"},
    "finance": {
        "missile", "drone", "fighter jet", "warship", "military exercise", "troops",
        "nvidia", "openai", "anthropic", "semiconductor", "chip", "robot", "white house",
        "trump", "election", "congress", "parliament", "press briefing",
        "virtual reality", "augmented reality", "headset", "eyewear", "smart glasses",
    },
    "real_estate": {
        "stock market", "trading floor", "wall street", "nasdaq", "nyse", "bearish",
        "bull market", "crypto", "semiconductor", "chip", "nvidia", "openai",
        "military", "warship", "missile", "white house", "parliament",
    },
    "military": {"stock market", "ipo", "wall street", "earnings", "investors"},
}

OPENNEWS_GENERIC_MEDIA_TOKENS = {
    "news", "photo", "image", "video", "official", "media", "press", "latest",
    "footage", "b-roll", "article", "source", "public domain", "archive",
}

OPENNEWS_STRICT_FALLBACK_SOURCES = {"article", "related_article", "opengraph", "news_source"}

OPENNEWS_NAMED_ENTITY_PATTERNS = {
    "openai": (r"\bopenai\b",),
    "anthropic": (r"\banthropic\b|\bclaude\b",),
    "google": (r"\bgoogle\b|\balphabet\b|\bgemini\b",),
    "microsoft": (r"\bmicrosoft\b|\bazure\b",),
    "meta": (r"\bmeta\b|\bfacebook\b",),
    "deepseek": (r"\bdeepseek\b",),
    "copilot": (r"\bcopilot\b",),
    "nvidia_huang": (r"\bnvidia\b|\bjensen\s+huang\b|\bjensenhuang\b|英伟达|黃仁勳|黄仁勋",),
    "xai_grok": (r"\bxai\b|\bx\.ai\b|\bgrok\b",),
    "amazon_aws": (r"\bamazon\b|\baws\b|\bamazon\s+web\s+services\b|\bbedrock\b|亚马逊|亞馬遜",),
    "apple": (r"\bapple\b|\biphone\b|\bsiri\b|\bwwdc\b|苹果",),
    "smartphone": (r"\bsmartphone\b|\bmobile\s+phone\b|\bphone\b|手机|智能手机",),
    "tesla_spacex": (r"\btesla\b|\bspacex\b|\belon\s+musk\b|\belonmusk\b|马斯克|馬斯克",),
    "qualcomm": (r"\bqualcomm\b",),
    "broadcom": (r"\bbroadcom\b",),
    "marvell": (r"\bmarvell\b",),
    "oracle": (r"\boracle\b",),
    "trump": (r"\btrump\b|特朗普",),
    "white_house": (r"\bwhite\s+house\b|白宫|白宮",),
    "fed_powell": (r"\bfed\b|\bfederal\s+reserve\b|\bpowell\b|美联储|聯準會",),
    "iran_israel": (r"\biran\b|\bisrael\b|\bhormuz\b|伊朗|以色列|霍尔木兹|霍爾木茲",),
}

OPENNEWS_AI_COMPANY_ENTITY_GROUPS = {
    "openai", "anthropic", "google", "microsoft", "meta", "nvidia_huang",
    "xai_grok", "amazon_aws", "apple", "tesla_spacex", "deepseek", "copilot", "qualcomm",
    "broadcom", "marvell", "oracle", "smartphone",
}

OPENNEWS_ENTITY_DISPLAY_TERMS = {
    "openai": ["OpenAI", "ChatGPT"],
    "anthropic": ["Anthropic", "Claude"],
    "google": ["Google", "Alphabet", "Gemini"],
    "microsoft": ["Microsoft", "Azure", "Copilot"],
    "meta": ["Meta", "Facebook", "Llama"],
    "deepseek": ["DeepSeek"],
    "copilot": ["Copilot"],
    "nvidia_huang": ["Nvidia", "Jensen Huang", "英伟达", "黄仁勋"],
    "xai_grok": ["xAI", "Grok", "Elon Musk"],
    "amazon_aws": ["Amazon", "AWS", "Bedrock"],
    "apple": ["Apple", "iPhone", "Siri", "WWDC"],
    "smartphone": ["smartphone", "mobile phone"],
    "tesla_spacex": ["Tesla", "SpaceX", "Elon Musk"],
    "qualcomm": ["Qualcomm"],
    "broadcom": ["Broadcom"],
    "marvell": ["Marvell"],
    "oracle": ["Oracle"],
    "trump": ["Trump", "特朗普"],
    "white_house": ["White House", "白宫"],
    "fed_powell": ["Federal Reserve", "Jerome Powell", "Alan Greenspan", "美联储"],
    "iran_israel": ["Iran", "Israel", "Hormuz", "伊朗", "以色列"],
}

OPENNEWS_PEXELS_HARD_ENTITY_GROUPS = {
    "openai", "anthropic", "google", "microsoft", "meta", "deepseek", "copilot",
    "nvidia_huang", "xai_grok", "amazon_aws", "apple", "tesla_spacex",
    "qualcomm", "broadcom", "marvell", "oracle", "smartphone",
    "trump", "white_house", "fed_powell", "iran_israel",
}


def _normalized_media_basename(path: str) -> str:
    name = os.path.basename((path or "").lower())
    if not name:
        return ""
    stem, ext = os.path.splitext(name)
    stem = re.sub(r"[-_@](?:\d{2,5}x\d{2,5}|\d{2,5}w|large|medium|small|thumb|thumbnail|preview|orig|original)$", "", stem)
    stem = re.sub(r"(?:[-_](?:copy|scaled|resize|crop|web|mobile))+$", "", stem)
    return f"{stem}{ext}" if stem and ext else name


def _source_url_key(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path.lower()}"


def _source_identity_keys(url: str) -> set[str]:
    parsed = urlparse(str(url or ""))
    base = _source_url_key(url)
    basename = _normalized_media_basename(parsed.path)
    keys = {base} if base else set()
    if parsed.netloc and basename:
        keys.add(f"basename:{parsed.netloc.lower()}:{basename}")
    return keys


def _looks_like_bad_source_material(item: dict) -> bool:
    url = str(item.get("url") or "")
    title = str(item.get("title") or "")
    source_url = str(item.get("source_url") or "")
    related_query = str(item.get("related_query") or "")
    text = f"{url} {source_url} {title} {related_query}".lower()
    return any(token in text for token in SOURCE_MATERIAL_BAD_TOKENS)


def _looks_like_unsafe_source_material_url(value: str) -> bool:
    text = str(value or "").lower()
    if not text:
        return False
    parsed = urlparse(text)
    host = parsed.netloc.lower()
    haystack = f"{host} {parsed.path.lower()} {parsed.query.lower()}"
    for token in SOURCE_MATERIAL_ADULT_TOKENS:
        escaped = re.escape(token)
        if re.search(rf"(^|[^a-z0-9]){escaped}([^a-z0-9]|$)", haystack):
            return True
    return False


def _looks_like_unsafe_source_material(item: dict) -> bool:
    values = [
        item.get("url"),
        item.get("source_url"),
        item.get("title"),
        item.get("related_query"),
    ]
    return any(_looks_like_unsafe_source_material_url(str(value or "")) for value in values)


def _downloaded_image_has_unsafe_skin_ratio(path: str) -> bool:
    if not OPENNEWS_SOURCE_IMAGE_SKIN_SAFETY_ENABLED:
        return False
    try:
        from PIL import Image

        image = Image.open(path).convert("RGB")
        image.thumbnail((180, 180))
        pixels = list(image.getdata())
        if not pixels:
            return False
        skin_pixels = 0
        bright_pixels = 0
        for r, g, b in pixels:
            max_channel = max(r, g, b)
            min_channel = min(r, g, b)
            if max_channel > 60:
                bright_pixels += 1
            rgb_skin = (
                r > 95
                and g > 40
                and b > 20
                and r > g
                and r > b
                and (max_channel - min_channel) > 15
                and abs(r - g) > 12
            )
            # Catch pale skin tones that are common in unsafe editorial images.
            pale_skin = r > 170 and 105 < g < 215 and 75 < b < 190 and r >= g >= b and (r - b) > 35
            if rgb_skin or pale_skin:
                skin_pixels += 1
        denominator = max(1, bright_pixels or len(pixels))
        skin_ratio = skin_pixels / denominator
        return skin_ratio >= OPENNEWS_SOURCE_IMAGE_MAX_SKIN_RATIO
    except Exception:
        return False


def _rank_source_material(item: dict) -> int:
    kind = str(item.get("kind") or "").lower()
    title = str(item.get("title") or "").lower()
    url = str(item.get("url") or "").lower()
    source = str(item.get("source") or "").lower()
    score = 15 if kind == "video" else 0
    if "opengraph" in title:
        score += 30
    if "article" in title:
        score += 25
    if "hero" in title or "featured" in title or "lead" in title:
        score += 18
    if any(token in url for token in ("wp-content", "media", "image", "photo", "newsroom", "uploads")):
        score += 10
    if "linked video" in title or re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", url):
        score += 20
    if source in {"article", "related_article", "opengraph", "news_source"}:
        score += 18
    if source in {"general_web", "general_web_search_media"}:
        score -= 8
    return score


def _theme_balanced_source_materials(items: list[dict], relevance_tokens: set[str] | None = None) -> list[dict]:
    """Keep OpenNews visuals aligned with script themes instead of one global pool."""
    groups: dict[int, list[dict]] = {}
    unthemed_index = 9999
    for item in items:
        try:
            theme_index = int(item.get("theme_index"))
        except Exception:
            theme_index = unthemed_index
            unthemed_index += 1
        groups.setdefault(theme_index, []).append(item)

    for theme_index, group in list(groups.items()):
        groups[theme_index] = sorted(
            group,
            key=lambda item: (
                _source_material_relevance_score(item, relevance_tokens or set()),
                _rank_source_material(item),
            ),
            reverse=True,
        )

    ordered: list[dict] = []
    theme_indexes = sorted(groups)
    cursors = {theme_index: 0 for theme_index in theme_indexes}
    while True:
        changed = False
        for theme_index in theme_indexes:
            group = groups[theme_index]
            cursor = cursors[theme_index]
            if cursor >= len(group):
                continue
            ordered.append(group[cursor])
            cursors[theme_index] = cursor + 1
            changed = True
        if not changed:
            break
    return ordered


def _opennews_theme_queries(seg: dict) -> list[str]:
    queries: list[str] = []
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        for query in theme.get("queries") or []:
            query = re.sub(r"\s+", " ", str(query or "")).strip()
            if query and query.lower() not in {item.lower() for item in queries}:
                queries.append(query)
    return queries


def _opennews_clean_query(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _opennews_anchor_queries(seg: dict, relevance_tokens: set[str], visual_domain: str) -> list[dict]:
    """Build concrete visual anchors so web image search does not collapse into one broad keyword."""
    blob = " ".join(
        str(seg.get(key) or "")
        for key in ("title", "title_zh", "material_keyword", "material_search_keyword", "material_desc", "script")
    )
    blob_lower = blob.lower()
    entities = _opennews_query_named_entities(seg, relevance_tokens)
    anchors: list[dict] = []

    def add(anchor: str, *queries: str) -> None:
        clean_queries = []
        for query in queries:
            query = _opennews_clean_query(query)
            if query and query.lower() not in {item.lower() for item in clean_queries}:
                clean_queries.append(query)
        if not clean_queries:
            return
        if anchor in {item.get("anchor") for item in anchors}:
            return
        anchors.append({"anchor": anchor, "queries": clean_queries})

    for entity in sorted(entities):
        display_terms = OPENNEWS_ENTITY_DISPLAY_TERMS.get(entity) or [entity.replace("_", " ")]
        main_term = display_terms[0]
        add(f"entity:{entity}", f"{main_term} news photo", f"{main_term} official newsroom image")

    if re.search(r"\bjapan\b|日本|东京|東京|japanese", blob_lower) and not re.search(r"日元|yen|jpy|currency|forex|exchange rate|汇率|外汇", blob_lower):
        add("scene:japan_government", "Japan government AI chip space investment news", "Japan government press conference technology investment")
    finance_anchor_needed = bool(re.search(r"日元|yen|stock|stocks|share price|nasdaq|nyse|wall street|fed|federal reserve|interest rate|oil price|股市|股票|股价|美联储|利率|油价", blob_lower))
    if finance_anchor_needed or (
        visual_domain == "finance"
        and not re.search(r"real estate|housing|property|mortgage|landlord|rental|dscr|single-family|房产|房地产|住宅|房贷|房东|租金|单户住宅", blob_lower)
    ):
        if re.search(r"英镑|pound|sterling|gbp", blob_lower):
            add("scene:finance_currency", "British pound currency exchange rate forex market", "pound sterling financial market chart")
        elif re.search(r"日元|yen|jpy", blob_lower):
            add("scene:finance_currency", "Japanese yen currency exchange rate forex market", "yen financial market chart")
        elif re.search(r"外汇|forex|exchange rate|currency", blob_lower):
            add("scene:finance_currency", "forex currency exchange rate market chart", "currency trading financial analysis")
        else:
            add("scene:finance", "stock market trading screen financial news", "financial market investment analysis chart")
    if re.search(r"smart glasses|ai glasses|eyewear|virtual reality|augmented reality|mixed reality|headset|智能眼镜|眼镜|头显|vr|ar", blob_lower):
        add("scene:smart_glasses", "smart glasses wearable technology product photo", "augmented reality glasses headset technology")
    if re.search(r"real estate|housing|property|mortgage|landlord|rental|dscr|single-family|房产|房地产|住宅|房贷|房东|租金|单户住宅", blob_lower):
        add(
            "scene:real_estate",
            "real estate investors single family homes mortgage news photo",
            "housing market rental property landlord mortgage document",
            "DSCR loan real estate investor house purchase",
        )
    if re.search(r"chip|semiconductor|芯片|半导体|半導體", blob_lower):
        add("scene:semiconductor", "semiconductor chip factory news photo", "AI chip semiconductor wafer fab")
    if re.search(r"space|太空|rocket|satellite|航天|宇宙", blob_lower):
        add("scene:space", "space industry rocket satellite news photo", "Japan space agency rocket satellite")

    if visual_domain in {"ai", "technology"}:
        add("scene:ai_infrastructure", "AI data center GPU server racks news photo", "artificial intelligence chip data center")
    if visual_domain == "cybersecurity":
        add("scene:cybersecurity", "cybersecurity operations center network threat news photo", "cyber crime hacker security dashboard")
    if visual_domain == "finance":
        add("scene:finance_market", "stock market trading floor financial news photo", "central bank interest rate market news")
    if visual_domain == "real_estate":
        add("scene:housing_market", "housing market homes for sale real estate investors", "suburban single family rental homes mortgage application")
    if visual_domain == "military":
        add("scene:military", "military news warship fighter jet drone missile", "defense exercise press briefing news photo")
    if visual_domain == "politics":
        add("scene:politics", "government press briefing parliament news photo", "official government building diplomacy news")
    if visual_domain == "real_estate":
        add("scene:real_estate", "housing market homes real estate news photo", "residential property mortgage news")
    if visual_domain == "immigration":
        add("scene:immigration", "airport immigration visa passport news photo", "border immigration policy news")

    for index, query in enumerate(_opennews_theme_queries(seg)[:4]):
        add(f"theme:{index}", f"{query} news photo", f"{query} official image")

    fallback_query = _opennews_clean_query(
        str(seg.get("material_search_keyword") or seg.get("material_keyword") or seg.get("title_zh") or "")
    )
    if fallback_query:
        add("scene:main_news", f"{fallback_query} news photo", f"{fallback_query} related article image")
    return anchors[:8]


def _opennews_extract_images_from_html(
    html: str,
    base_url: str,
    *,
    limit: int = 8,
    context_tokens: set[str] | None = None,
) -> list[dict]:
    images: list[dict] = []
    seen: set[str] = set()
    context_tokens = {
        token.lower()
        for token in (context_tokens or set())
        if len(str(token or "")) >= 4 and token.lower() not in {"news", "photo", "image", "article"}
    }

    def add(url: str, title: str = "", *, require_context: bool = False) -> None:
        if len(images) >= limit:
            return
        url = str(url or "").strip()
        if not url or url.startswith("data:"):
            return
        absolute = urljoin(base_url, url)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return
        if absolute in seen:
            return
        if re.search(r"(logo|icon|avatar|sprite|placeholder|tracking|pixel)", absolute, flags=re.I):
            return
        if require_context and context_tokens:
            haystack = f"{absolute} {title}".lower()
            if not any(token in haystack for token in context_tokens):
                return
        seen.add(absolute)
        images.append({"url": absolute, "title": title})

    for pattern in (
        r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image(?::secure_url)?["\']',
        r'<meta[^>]+name=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image(?::src)?["\']',
    ):
        for match in re.finditer(pattern, html, flags=re.I):
            add(match.group(1), "article social image")
    for match in re.finditer(r'<img[^>]+(?:src|data-src|data-original)=["\']([^"\']+)["\'][^>]*>', html, flags=re.I):
        tag = match.group(0)
        alt_match = re.search(r'alt=["\']([^"\']{0,160})["\']', tag, flags=re.I)
        title = alt_match.group(1) if alt_match else "article inline image"
        add(match.group(1), title, require_context=True)
    return images


def _opennews_fetch_related_article_images(query: str, *, anchor: str, limit: int = 6) -> list[dict]:
    query = _opennews_clean_query(query)
    if not query:
        return []
    headers = {"User-Agent": "Mozilla/5.0 iHouse OpenNews media matcher/1.0"}
    candidates: list[dict] = []
    seen_urls: set[str] = set()
    context_tokens = _tokenize_opennews_relevance(query)
    try:
        rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
        response = requests.get(rss_url, timeout=12, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        for item in root.findall(".//item")[:8]:
            title = (item.findtext("title") or query).strip()
            link = (item.findtext("link") or "").strip()
            for media in list(item):
                tag = str(media.tag or "").lower()
                if tag.endswith("content") or tag.endswith("thumbnail"):
                    url = media.attrib.get("url") or ""
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        candidates.append({
                            "url": url,
                            "kind": "image",
                            "source": "related_article",
                            "title": f"{title}｜{anchor}",
                            "source_url": link,
                            "visual_anchor": anchor,
                            "_anchor_query": query,
                        })
            if len(candidates) >= limit:
                break
            if not link:
                continue
            try:
                article_response = requests.get(link, timeout=10, headers=headers)
                article_response.raise_for_status()
                for image in _opennews_extract_images_from_html(
                    article_response.text[:700000],
                    link,
                    limit=4,
                    context_tokens=context_tokens,
                ):
                    url = image["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    candidates.append({
                        "url": url,
                        "kind": "image",
                        "source": "related_article",
                        "title": f"{title}｜{image.get('title') or anchor}",
                        "source_url": link,
                        "visual_anchor": anchor,
                        "_anchor_query": query,
                    })
                    if len(candidates) >= limit:
                        break
            except Exception:
                continue
            if len(candidates) >= limit:
                break
    except Exception as exc:
        print(f"  ⚠️ 相关报道图片扩展失败：{query}｜{exc}")
    return candidates[:limit]


def _opennews_expand_source_material_candidates(seg: dict, relevance_tokens: set[str], visual_domain: str) -> list[dict]:
    anchors = _opennews_anchor_queries(seg, relevance_tokens, visual_domain)
    expanded: list[dict] = []
    seen: set[str] = set()
    for anchor in anchors:
        anchor_name = str(anchor.get("anchor") or "scene")
        for query in anchor.get("queries") or []:
            for item in _opennews_fetch_related_article_images(query, anchor=anchor_name, limit=4):
                keys = _source_identity_keys(str(item.get("url") or ""))
                if not keys or keys & seen:
                    continue
                seen.update(keys)
                expanded.append(item)
            if len([item for item in expanded if item.get("visual_anchor") == anchor_name]) >= 4:
                break
    if expanded:
        print(f"  🧭 OpenNews 已按视觉锚点扩展相关报道图片候选：{len(expanded)} 条")
    return expanded[: min(80, max(12, OPENNEWS_REALTIME_SOURCE_CANDIDATE_LIMIT))]


def _tokenize_opennews_relevance(text: str) -> set[str]:
    text = (text or "").lower()
    aliases = {
        "马斯克": "elon musk",
        "万亿富翁": "trillionaire",
        "上市": "ipo",
        "人工智能": "artificial intelligence",
        "半导体": "semiconductor",
        "芯片": "chip",
        "英伟达": "nvidia",
        "黄仁勋": "jensen huang",
        "黃仁勳": "jensen huang",
        "特朗普": "trump",
        "微软": "microsoft",
        "谷歌": "google",
        "苹果": "apple",
        "苹果公司": "apple",
        "开发者大会": "wwdc",
        "人工智能版": "artificial intelligence",
        "地震": "earthquake",
        "海啸": "tsunami",
        "菲律宾": "philippines",
        "亚马逊": "amazon",
        "谷歌母公司": "alphabet",
        "格林斯潘": "greenspan",
        "鲍威尔": "powell",
        "联邦储备": "federal reserve",
        "关税": "tariff",
        "房贷": "mortgage",
        "首套房": "starter home",
        "百万美元": "million dollar",
        "油价": "oil price",
        "霍尔木兹": "hormuz",
        "伊朗": "iran",
        "以色列": "israel",
        "手机": "smartphone",
        "智能手机": "smartphone",
        "智能眼镜": "smart glasses",
        "眼镜": "glasses eyewear",
        "虚拟现实": "virtual reality",
        "增强现实": "augmented reality",
        "混合现实": "mixed reality",
        "头显": "headset",
        "英镑": "british pound currency forex",
        "日元": "japanese yen currency forex",
        "汇率": "exchange rate forex",
        "外汇": "forex currency market",
    }
    for source, replacement in aliases.items():
        text = text.replace(source, f" {replacement} ")
    tokens = set(re.findall(r"[a-z0-9][a-z0-9.+-]{2,}", text))
    phrases = {
        "white house", "press briefing", "elon musk", "spacex", "meta", "meta ai",
        "facebook", "ipo", "trillionaire", "nvidia", "openai", "microsoft",
        "google", "alphabet", "semiconductor", "artificial intelligence",
        "biotechnology", "stock market", "investors", "ukraine", "russia",
        "taiwan strait", "drone", "missile", "wwdc", "siri", "iphone",
        "apple intelligence", "earthquake", "tsunami", "philippines",
        "federal reserve", "jerome powell", "alan greenspan", "wall street",
        "mortgage", "starter home", "oil price", "crude oil",
        "strait of hormuz", "smartphone", "smart glasses", "virtual reality",
        "augmented reality", "mixed reality", "headset", "eyewear", "forex",
        "currency market", "exchange rate", "british pound", "pound sterling",
        "japanese yen",
    }
    for phrase in phrases:
        if phrase in text:
            tokens.add(phrase)
    generic = {
        "news", "latest", "image", "photo", "video", "official", "press", "media",
        "article", "source", "related", "public", "content", "government", "meeting",
        "briefing", "company", "companies", "market", "tools", "tool",
        "america", "american", "united", "states", "state", "us", "usa", "u.s",
    }
    return {token for token in tokens if token not in generic and len(token) >= 3}


def _opennews_relevance_tokens(seg: dict) -> set[str]:
    parts = [
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("script") or "")[:900],
    ]
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        parts.append(str(theme.get("title") or ""))
        parts.append(str(theme.get("visual_need") or ""))
        parts.append(str(theme.get("script") or ""))
        parts.extend(str(query or "") for query in theme.get("queries") or [])
    return _tokenize_opennews_relevance(" ".join(parts))


def _opennews_visual_domain(seg: dict, relevance_tokens: set[str]) -> str:
    explicit = str(seg.get("opennews_category") or seg.get("category") or "").strip().lower()
    text = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("material_desc") or ""),
        str(seg.get("script") or "")[:1200],
        " ".join(sorted(relevance_tokens)),
    ]).lower()
    finance_markers = {
        "stock", "stocks", "stock market", "shares", "share price", "market",
        "wall street", "nasdaq", "nyse", "dow", "s&p", "investor", "investors",
        "earnings", "fed", "federal reserve", "interest rate", "inflation",
        "oil price", "oil prices", "crude", "yield", "bond", "tariff",
        "股市", "美股", "股票", "股价", "上涨", "下跌", "油价", "利率",
        "美联储", "通胀", "投资者", "华尔街", "市场走势",
    }
    technology_markers = {
        "product launch", "new chip", "chip design", "semiconductor manufacturing",
        "ai model", "large language model", "robotics", "software platform",
        "芯片技术", "产品发布", "大模型", "机器人", "半导体制造",
    }
    cybersecurity_markers = {
        "cyber", "cybersecurity", "cyber security", "cyber crime", "cybercrime",
        "network security", "hacker", "phishing", "scam", "ransomware", "malware",
        "data breach", "authentication", "two-factor", "mfa", "网络安全", "网络犯罪",
        "网络攻击", "黑客", "诈骗", "钓鱼", "勒索软件", "恶意软件", "数据泄露",
    }
    real_estate_markers = {
        "real estate", "housing", "home", "homes", "house", "houses", "property",
        "rental", "rent", "landlord", "mortgage", "dscr", "non-qm", "single-family",
        "residential", "apartment", "房产", "房地产", "住宅", "房贷", "租金",
        "房东", "单户住宅", "不动产", "购房",
    }
    finance_hits = sum(1 for token in finance_markers if token in text)
    technology_hits = sum(1 for token in technology_markers if token in text)
    cybersecurity_hits = sum(1 for token in cybersecurity_markers if token in text)
    real_estate_hits = sum(1 for token in real_estate_markers if token in text)
    if real_estate_hits >= 2:
        return "real_estate"
    # 股票、油价、利率新闻里经常会提到英伟达/AI/特朗普，但画面应使用金融市场素材。
    if finance_hits >= 2 and finance_hits >= technology_hits:
        return "finance"
    if cybersecurity_hits >= 2:
        return "cybersecurity"
    if explicit == "ai":
        return "technology"
    if explicit in OPENNEWS_VISUAL_DOMAIN_TOKENS:
        return explicit
    best_domain = ""
    best_hits = 0
    for domain, tokens in OPENNEWS_VISUAL_DOMAIN_TOKENS.items():
        hits = sum(1 for token in tokens if token in text)
        if hits > best_hits:
            best_hits = hits
            best_domain = domain
    return best_domain or "general"


def _opennews_item_haystack(item: dict) -> str:
    return " ".join(
        str(item.get(field) or "")
        for field in ("title", "url", "source_url", "related_query", "theme_title")
    ).lower()


def _opennews_domain_hits(text: str, domain: str) -> set[str]:
    return {token for token in OPENNEWS_VISUAL_DOMAIN_TOKENS.get(domain, set()) if token in text}


def _opennews_wrong_domain_hits(text: str, domain: str) -> set[str]:
    hits = {token for token in OPENNEWS_WRONG_DOMAIN_BLOCKS.get(domain, set()) if token in text}
    if domain != "politics" and not _opennews_domain_hits(text, domain):
        politics_hits = _opennews_domain_hits(text, "politics")
        if len(politics_hits) >= 2:
            hits.update(politics_hits)
    return hits


def _opennews_core_relevance_tokens(relevance_tokens: set[str]) -> set[str]:
    core = set()
    for token in relevance_tokens:
        lowered = token.lower().strip()
        if not lowered or lowered in OPENNEWS_GENERIC_MEDIA_TOKENS:
            continue
        if lowered in {"government", "meeting", "briefing", "company", "market", "tools", "tool"}:
            continue
        core.add(lowered)
    return core


def _opennews_quality_decision(item: dict, relevance_tokens: set[str], domain: str) -> tuple[bool, str, int]:
    source = str(item.get("source") or "").lower()
    title = str(item.get("title") or "").lower()
    haystack = _opennews_item_haystack(item)
    item_tokens = _tokenize_opennews_relevance(haystack)
    core_tokens = _opennews_core_relevance_tokens(relevance_tokens)
    overlap = core_tokens & item_tokens
    phrase_hits = {token for token in core_tokens if " " in token and token in haystack}
    domain_hits = _opennews_domain_hits(haystack, domain)
    wrong_hits = _opennews_wrong_domain_hits(haystack, domain)
    relevance_score = _source_material_relevance_score(item, relevance_tokens)
    rank_score = _rank_source_material(item)
    score = relevance_score + min(rank_score, 35)
    if overlap:
        score += len(overlap) * 10
    if phrase_hits:
        score += len(phrase_hits) * 16
    if domain_hits:
        score += min(len(domain_hits) * 8, 24)
    if wrong_hits:
        score -= 35 + len(wrong_hits) * 8

    # 原文/相关报道的主图允许稍宽，但仍不能明显跑到错误领域。
    trusted_article_source = source in {"article", "related_article", "opengraph", "news_source"}
    if wrong_hits and not (overlap or phrase_hits):
        return False, f"疑似错误领域素材：{', '.join(sorted(wrong_hits)[:4])}", score
    if source in {"general_web", "general_web_search_media"}:
        if not (overlap or phrase_hits or domain_hits):
            return False, "公开网页素材未命中新闻核心实体或视觉主题", score
        if score < 42:
            return False, f"公开网页素材相关性分数过低：{score}", score
    elif not trusted_article_source:
        if core_tokens and not (overlap or phrase_hits or domain_hits) and score < 34:
            return False, f"素材相关性分数过低：{score}", score
    else:
        if wrong_hits and score < 28:
            return False, f"原文素材但疑似跑题：{score}", score
    if "search media:" in title and not (overlap or phrase_hits or domain_hits):
        return False, "搜索素材只命中泛化检索词", score
    return True, "通过相关性检查", score


def _source_material_relevance_score(item: dict, relevance_tokens: set[str]) -> int:
    if not relevance_tokens:
        return 1
    haystack = " ".join(
        str(item.get(field) or "")
        for field in ("title", "url", "source_url", "related_query", "theme_title")
    )
    item_tokens = _tokenize_opennews_relevance(haystack)
    if not item_tokens:
        return 0
    overlap = relevance_tokens & item_tokens
    score = len(overlap) * 10
    haystack_lower = haystack.lower()
    for token in relevance_tokens:
        if " " in token and token in haystack_lower:
            score += 18
    source = str(item.get("source") or "").lower()
    title = str(item.get("title") or "").lower()
    if source in {"article", "related_article", "opengraph", "news_source"}:
        score += 8
    if "opengraph" in title or "article" in title:
        score += 6
    return score


def _opennews_min_relevance_score(item: dict) -> int:
    title = str(item.get("title") or "").lower()
    source = str(item.get("source") or "").lower()
    if "opengraph" in title or "article" in title:
        return 12
    if source in {"general_web", "general_web_search_media"}:
        return 28
    if str(item.get("kind") or "").lower() == "video":
        return 16
    return 14


def _opennews_library_category_hints(seg: dict, visual_domain: str) -> set[str]:
    blob = " ".join(
        str(seg.get(key) or "")
        for key in (
            "opennews_category", "category", "material_keyword", "material_search_keyword",
            "material_desc", "script", "theme_title",
        )
    ).lower()
    hints: set[str] = {"新闻", "通用新闻", "通用氛围"}
    category = str(seg.get("opennews_category") or seg.get("category") or "").strip().lower()
    if visual_domain == "cybersecurity":
        hints.update({"科技", "AI", "通用新闻"})
    if category == "ai" or visual_domain == "technology" and re.search(r"\b(ai|artificial intelligence|nvidia|openai|anthropic)\b|人工智能|英伟达", blob):
        hints.update({"AI", "科技"})
    if category == "technology" or visual_domain == "technology":
        hints.update({"科技", "AI"})
    if category == "finance" or visual_domain == "finance":
        hints.update({"金融"})
    if category == "real_estate":
        hints.update({"房地产", "房产", "城市街景"})
    if category == "immigration":
        hints.update({"移民", "城市街景"})
    if category == "military" or visual_domain == "military":
        hints.update({"军事"})
    if category == "politics" or visual_domain == "politics":
        hints.update({"政治"})
    return hints


def _opennews_library_primary_categories(visual_domain: str, seg: dict | None = None) -> set[str]:
    category = str((seg or {}).get("opennews_category") or (seg or {}).get("category") or "").strip().lower()
    domain = (visual_domain or category or "general").strip().lower()
    blob = " ".join(
        str((seg or {}).get(key) or "")
        for key in ("material_keyword", "material_search_keyword", "material_desc", "script")
    ).lower()
    policy_cross_topic = bool(re.search(r"\btrump\b|特朗普|白宫|白宮|white house", blob))
    if domain == "cybersecurity":
        return {"AI", "科技", "通用新闻"}
    if category == "ai":
        return {"AI", "科技", "政治"} if policy_cross_topic else {"AI", "科技"}
    if category in {"real_estate", "property", "housing"}:
        return {"房地产", "房产", "城市街景"}
    if category in {"immigration", "visa"}:
        return {"移民", "城市街景"}
    if domain == "technology":
        return {"AI", "科技", "政治"} if policy_cross_topic else {"AI", "科技"}
    if domain == "finance":
        return {"金融"}
    if domain == "military":
        return {"军事"}
    if domain == "politics":
        return {"政治"}
    if domain == "society":
        return {"新闻", "通用新闻", "通用氛围", "城市街景"}
    return set()


def _opennews_library_blocked_categories(visual_domain: str, seg: dict | None = None) -> set[str]:
    allowed = _opennews_library_primary_categories(visual_domain, seg)
    all_domain_categories = {"AI", "科技", "金融", "军事", "政治", "房地产", "房产", "移民", "城市街景"}
    if not allowed:
        return set()
    return all_domain_categories - allowed


def _opennews_library_item_searchable(item: dict) -> str:
    return " ".join([
        str(item.get("category") or ""),
        str(item.get("title") or ""),
        " ".join(item.get("tags") or []),
        " ".join(item.get("ai_tags") or []),
        " ".join(item.get("news_topics") or []),
        str(item.get("notes") or ""),
        str(item.get("source_url") or ""),
        str(item.get("source_site") or ""),
        str(item.get("original_filename") or ""),
    ]).lower()


def _opennews_library_item_fingerprint(item: dict) -> str:
    source_url = str(item.get("source_url") or "").strip().lower()
    if source_url:
        return f"url:{source_url}"
    title = re.sub(r"\s+", " ", str(item.get("title") or "").strip().lower())
    if title:
        return f"title:{title[:120]}"
    return f"file:{str(item.get('filename') or item.get('id') or '').strip().lower()}"


def _opennews_material_usage_day(ts: float | None = None) -> str:
    # Use JST for the user's “一天” rule, independent of the server timezone.
    return time.strftime("%Y-%m-%d", time.gmtime((ts or time.time()) + 9 * 3600))


def _opennews_material_usage_keys(item: dict) -> set[str]:
    keys: set[str] = set()
    for prefix, field in (
        ("id", "id"),
        ("filename", "filename"),
        ("source", "source_url"),
    ):
        value = str(item.get(field) or "").strip().lower()
        if value:
            keys.add(f"{prefix}:{value}")
    fingerprint = _opennews_library_item_fingerprint(item)
    if fingerprint:
        keys.add(f"fingerprint:{fingerprint.strip().lower()}")
    return keys


def _opennews_read_material_usage() -> dict:
    path = OPENNEWS_MATERIAL_LIBRARY_USAGE_PATH
    if not path:
        return {"entries": []}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and isinstance(payload.get("entries"), list):
            return payload
    except FileNotFoundError:
        return {"entries": []}
    except Exception as exc:
        print(f"  ⚠️ OpenNews 素材库使用记录读取失败，将临时按空记录处理：{exc}")
    return {"entries": []}


def _opennews_write_material_usage(payload: dict) -> None:
    path = OPENNEWS_MATERIAL_LIBRARY_USAGE_PATH
    if not path:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _opennews_prune_material_usage(entries: list[dict], now: float) -> list[dict]:
    cutoff = now - 40 * 86400
    pruned: list[dict] = []
    for entry in entries:
        try:
            ts = float(entry.get("used_at") or 0)
        except Exception:
            ts = 0
        if ts >= cutoff:
            pruned.append(entry)
    return pruned


def _opennews_library_image_usage_status(item: dict, *, now: float | None = None) -> tuple[bool, str]:
    if str(item.get("kind") or "").lower() != "image":
        return True, ""
    keys = _opennews_material_usage_keys(item)
    if not keys:
        return True, ""
    current = now or time.time()
    today = _opennews_material_usage_day(current)
    monthly_cutoff = current - 30 * 86400
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], current)
    day_count = 0
    month_count = 0
    for entry in entries:
        entry_keys = {str(key or "").strip().lower() for key in (entry.get("keys") or []) if str(key or "").strip()}
        if not entry_keys or not (entry_keys & keys):
            continue
        try:
            used_at = float(entry.get("used_at") or 0)
        except Exception:
            used_at = 0
        if str(entry.get("day") or "") == today:
            day_count += 1
        if used_at >= monthly_cutoff:
            month_count += 1
    if day_count >= OPENNEWS_MATERIAL_LIBRARY_DAILY_IMAGE_LIMIT:
        return False, f"当天已使用 {day_count} 次，达到上限 {OPENNEWS_MATERIAL_LIBRARY_DAILY_IMAGE_LIMIT}"
    if month_count >= OPENNEWS_MATERIAL_LIBRARY_MONTHLY_IMAGE_LIMIT:
        return False, f"近30天已使用 {month_count} 次，达到上限 {OPENNEWS_MATERIAL_LIBRARY_MONTHLY_IMAGE_LIMIT}"
    return True, ""


def _opennews_record_library_image_usage(item: dict) -> None:
    if str(item.get("kind") or "").lower() != "image":
        return
    keys = sorted(_opennews_material_usage_keys(item))
    if not keys:
        return
    now = time.time()
    entry = {
        "used_at": now,
        "day": _opennews_material_usage_day(now),
        "keys": keys,
        "id": str(item.get("id") or ""),
        "filename": str(item.get("filename") or ""),
        "title": str(item.get("title") or ""),
        "source_url": str(item.get("source_url") or ""),
    }
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], now)
        entries.append(entry)
        payload["entries"] = entries
        payload["updated_at"] = now
        _opennews_write_material_usage(payload)
    item_id = str(item.get("id") or "").strip()
    if item_id:
        try:
            update_material_library_item(
                item_id,
                {
                    "usage_count": int(item.get("usage_count") or 0) + 1,
                    "last_used_at": now,
                },
            )
        except Exception as exc:
            print(f"  ⚠️ OpenNews 素材库使用次数写回失败：{item_id}｜{exc}")


def _opennews_source_page_key(item: dict) -> str:
    source_url = str(item.get("source_url") or "").strip() or str(item.get("page_url") or "").strip()
    if not source_url:
        source_url = str(item.get("url") or "").strip()
    return _source_url_key(source_url)


def _opennews_source_domain_key(item: dict) -> str:
    for value in (item.get("source_url"), item.get("page_url"), item.get("url")):
        parsed = urlparse(str(value or ""))
        if parsed.netloc:
            return parsed.netloc.lower()
    return ""


def _opennews_source_image_hash_usage_status(content_hash: str, *, now: float | None = None) -> tuple[bool, str]:
    normalized_hash = str(content_hash or "").strip().lower()
    if not normalized_hash:
        return True, ""
    current = now or time.time()
    today = _opennews_material_usage_day(current)
    monthly_cutoff = current - 30 * 86400
    key = f"hash:{normalized_hash}"
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], current)
    day_count = 0
    month_count = 0
    for entry in entries:
        if str(entry.get("type") or "") != "source_image":
            continue
        entry_keys = {str(item or "").strip().lower() for item in (entry.get("keys") or []) if str(item or "").strip()}
        if key not in entry_keys:
            continue
        try:
            used_at = float(entry.get("used_at") or 0)
        except Exception:
            used_at = 0
        if str(entry.get("day") or "") == today:
            day_count += 1
        if used_at >= monthly_cutoff:
            month_count += 1
    if day_count >= OPENNEWS_SOURCE_IMAGE_DAILY_HASH_LIMIT:
        return False, f"同一网络图片当天已使用 {day_count} 次，达到上限 {OPENNEWS_SOURCE_IMAGE_DAILY_HASH_LIMIT}"
    if month_count >= OPENNEWS_SOURCE_IMAGE_MONTHLY_HASH_LIMIT:
        return False, f"同一网络图片近30天已使用 {month_count} 次，达到上限 {OPENNEWS_SOURCE_IMAGE_MONTHLY_HASH_LIMIT}"
    return True, ""


def _opennews_record_source_image_usage(item: dict, *, content_hash: str, copied_path: str) -> None:
    normalized_hash = str(content_hash or "").strip().lower()
    if not normalized_hash:
        return
    now = time.time()
    source_url = str(item.get("source_url") or "").strip()
    image_url = str(item.get("url") or "").strip()
    page_key = _opennews_source_page_key(item)
    domain_key = _opennews_source_domain_key(item)
    keys = {
        f"hash:{normalized_hash}",
        *(f"image:{key}" for key in _source_identity_keys(image_url)),
    }
    if page_key:
        keys.add(f"page:{page_key}")
    if domain_key:
        keys.add(f"domain:{domain_key}")
    entry = {
        "type": "source_image",
        "used_at": now,
        "day": _opennews_material_usage_day(now),
        "keys": sorted(keys),
        "hash": normalized_hash,
        "image_url": image_url,
        "source_url": source_url,
        "source_domain": domain_key,
        "title": str(item.get("title") or ""),
        "path": os.path.basename(copied_path),
    }
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], now)
        entries.append(entry)
        payload["entries"] = entries
        payload["updated_at"] = now
        _opennews_write_material_usage(payload)


def _opennews_batch_registry_path(batch_job_id: str) -> str:
    safe_id = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(batch_job_id or "").strip())
    if not safe_id:
        return ""
    return os.path.join(OPENNEWS_PEXELS_BATCH_REGISTRY_DIR, f"{safe_id}.json")


def _opennews_read_batch_registry(batch_job_id: str) -> dict:
    path = _opennews_batch_registry_path(batch_job_id)
    if not path:
        return {"entries": []}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and isinstance(payload.get("entries"), list):
            return payload
    except FileNotFoundError:
        return {"entries": []}
    except Exception as exc:
        print(f"  ⚠️ OpenNews 批次素材去重记录读取失败，将按空记录处理：{exc}")
    return {"entries": []}


def _opennews_write_batch_registry(batch_job_id: str, payload: dict) -> None:
    path = _opennews_batch_registry_path(batch_job_id)
    if not path:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _opennews_batch_registry_entries(batch_job_id: str, *, now: float | None = None) -> list[dict]:
    current = now or time.time()
    payload = _opennews_read_batch_registry(batch_job_id)
    entries = payload.get("entries") or []
    cutoff = current - 7 * 86400
    kept: list[dict] = []
    for entry in entries:
        try:
            used_at = float(entry.get("used_at") or 0)
        except Exception:
            used_at = 0
        if used_at >= cutoff:
            kept.append(entry)
    return kept


def _pexels_image_usage_keys(item: dict, *, content_hash: str = "") -> set[str]:
    keys: set[str] = set()
    for key in _source_identity_keys(str(item.get("url") or "")):
        keys.add(f"image:{key}")
    if content_hash:
        keys.add(f"hash:{str(content_hash).strip().lower()}")
    return keys


def _opennews_batch_pexels_image_allowed(batch_job_id: str, item: dict, *, content_hash: str = "") -> tuple[bool, str]:
    batch_id = str(batch_job_id or "").strip()
    if not batch_id:
        return True, ""
    keys = _pexels_image_usage_keys(item, content_hash=content_hash)
    if not keys:
        return True, ""
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        entries = _opennews_batch_registry_entries(batch_id)
    for entry in entries:
        entry_keys = {str(key or "").strip().lower() for key in (entry.get("keys") or []) if str(key or "").strip()}
        if entry_keys & keys:
            return False, "同一批次内已使用过这张免费素材图"
    return True, ""


def _opennews_record_batch_pexels_image_usage(batch_job_id: str, item: dict, *, copied_path: str, content_hash: str = "") -> None:
    batch_id = str(batch_job_id or "").strip()
    if not batch_id:
        return
    now = time.time()
    entry = {
        "type": "pexels_image",
        "used_at": now,
        "day": _opennews_material_usage_day(now),
        "keys": sorted(_pexels_image_usage_keys(item, content_hash=content_hash)),
        "url": str(item.get("url") or ""),
        "title": str(item.get("title") or item.get("alt") or ""),
        "path": os.path.basename(copied_path),
    }
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_batch_registry(batch_id)
        entries = _opennews_batch_registry_entries(batch_id, now=now)
        entries.append(entry)
        payload["entries"] = entries
        payload["updated_at"] = now
        _opennews_write_batch_registry(batch_id, payload)


def _opennews_pexels_image_usage_status(item: dict, *, content_hash: str = "", now: float | None = None) -> tuple[bool, str]:
    keys = _pexels_image_usage_keys(item, content_hash=content_hash)
    if not keys:
        return True, ""
    current = now or time.time()
    cutoff = current - 86400
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], current)
    recent_count = 0
    for entry in entries:
        if str(entry.get("type") or "") != "pexels_image":
            continue
        try:
            used_at = float(entry.get("used_at") or 0)
        except Exception:
            used_at = 0.0
        if used_at < cutoff:
            continue
        entry_keys = {str(key or "").strip().lower() for key in (entry.get("keys") or []) if str(key or "").strip()}
        if not entry_keys or not (entry_keys & keys):
            continue
        recent_count += 1
    if recent_count >= OPENNEWS_PEXELS_IMAGE_DAILY_LIMIT:
        return False, f"同一免费素材图 24 小时内已使用 {recent_count} 次，达到上限 {OPENNEWS_PEXELS_IMAGE_DAILY_LIMIT}"
    return True, ""


def _opennews_record_pexels_image_usage(item: dict, *, copied_path: str, content_hash: str = "") -> None:
    now = time.time()
    entry = {
        "type": "pexels_image",
        "used_at": now,
        "day": _opennews_material_usage_day(now),
        "keys": sorted(_pexels_image_usage_keys(item, content_hash=content_hash)),
        "url": str(item.get("url") or ""),
        "title": str(item.get("title") or item.get("alt") or ""),
        "path": os.path.basename(copied_path),
    }
    with OPENNEWS_MATERIAL_LIBRARY_USAGE_LOCK:
        payload = _opennews_read_material_usage()
        entries = _opennews_prune_material_usage(payload.get("entries") or [], now)
        entries.append(entry)
        payload["entries"] = entries
        payload["updated_at"] = now
        _opennews_write_material_usage(payload)


def _opennews_named_entities_from_text(text: str) -> set[str]:
    haystack = str(text or "").lower()
    entities: set[str] = set()
    for entity, patterns in OPENNEWS_NAMED_ENTITY_PATTERNS.items():
        if any(re.search(pattern, haystack, flags=re.I) for pattern in patterns):
            entities.add(entity)
    return entities


def _opennews_strip_negative_entity_phrases(text: str) -> str:
    """Prevent "不要使用手机/特朗普图" from becoming required entity constraints."""
    cleaned = str(text or "")
    cleaned = re.sub(r"(?:不要|避免|禁止|排除|不要使用|不要出现|不能使用|不使用)[^。；;\n]{0,120}", " ", cleaned)
    cleaned = re.sub(
        r"(?:do\s+not|don't|avoid|exclude|without|no)\s+[^.；;,\n]{0,80}",
        " ",
        cleaned,
        flags=re.I,
    )
    return cleaned


def _opennews_query_named_entities(seg: dict, relevance_tokens: set[str]) -> set[str]:
    blob = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        _opennews_strip_negative_entity_phrases(seg.get("material_desc") or ""),
        str(seg.get("script") or "")[:1400],
        " ".join(sorted(relevance_tokens)),
    ])
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        blob += " " + " ".join([
            str(theme.get("title") or ""),
            str(theme.get("visual_need") or ""),
            str(theme.get("script") or ""),
            " ".join(str(query or "") for query in theme.get("queries") or []),
        ])
    return _opennews_named_entities_from_text(blob)


def _opennews_primary_named_entities(seg: dict) -> set[str]:
    """Entities in headline/search/visual need are harder intent than script background."""
    blob = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        _opennews_strip_negative_entity_phrases(seg.get("material_desc") or ""),
        str(seg.get("theme_title") or ""),
    ])
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        blob += " " + " ".join([
            str(theme.get("title") or ""),
            _opennews_strip_negative_entity_phrases(theme.get("visual_need") or ""),
            " ".join(str(query or "") for query in theme.get("queries") or []),
        ])
    return _opennews_named_entities_from_text(blob)


def _opennews_topic_conflict_reason(searchable: str, relevance_tokens: set[str], visual_domain: str) -> str:
    text = str(searchable or "").lower()
    tokens_text = " ".join(sorted(relevance_tokens)).lower()

    def query_has(*terms: str) -> bool:
        return any(term in tokens_text for term in terms)

    technology_conflicts = [
        (("robot", "robotics", "humanoid", "机器人", "人形机器人"), ("robot", "robotics", "humanoid", "机器人"), "机器人素材与当前科技新闻主题不符"),
        (("iphone", "siri", "wwdc", "apple intelligence", "苹果"), ("iphone", "siri", "wwdc", "apple", "苹果"), "苹果/手机素材与当前科技新闻主题不符"),
        (("tesla", "spacex", "elon musk", "马斯克", "馬斯克"), ("tesla", "spacex", "elon", "musk", "马斯克"), "马斯克/Tesla/SpaceX素材与当前科技新闻主题不符"),
        (("nvidia", "jensen", "huang", "gpu", "英伟达", "黄仁勋", "黃仁勳"), ("nvidia", "jensen", "huang", "gpu", "英伟达", "黄仁勋"), "英伟达/黄仁勋素材与当前科技新闻主题不符"),
        (("openai", "anthropic", "chatgpt", "claude"), ("openai", "anthropic", "chatgpt", "claude"), "OpenAI/Anthropic素材与当前科技新闻主题不符"),
    ]
    if visual_domain == "technology":
        for item_terms, query_terms, reason in technology_conflicts:
            if any(term in text for term in item_terms) and not query_has(*query_terms):
                return reason
    if visual_domain == "cybersecurity":
        cyber_hits = _opennews_domain_hits(text, "cybersecurity")
        wrong_hits = _opennews_wrong_domain_hits(text, "cybersecurity")
        if wrong_hits:
            return f"网络安全新闻禁止混入无关消费/广告/活动素材：{', '.join(sorted(wrong_hits)[:4])}"
        if not cyber_hits:
            return "网络安全新闻素材缺少 cyber/security/hacker/phishing/network 等核心语义"
    if visual_domain == "finance":
        finance_conflicts = ("robot", "humanoid", "nvidia", "openai", "anthropic", "iphone", "siri", "wwdc", "机器人", "英伟达")
        if any(term in text for term in finance_conflicts):
            return "金融新闻禁止混入科技公司/机器人素材"
    return ""


def _opennews_library_entity_locks(seg: dict, relevance_tokens: set[str]) -> dict[str, set[str]]:
    blob = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("material_desc") or ""),
        str(seg.get("script") or "")[:1200],
        " ".join(sorted(relevance_tokens)),
    ]).lower()
    locks: dict[str, set[str]] = {}
    if re.search(r"\b(nvidia|jensen|huang)\b|英伟达|黃仁勳|黄仁勋", blob):
        locks["nvidia_huang"] = {"nvidia", "jensen", "huang", "英伟达", "黃仁勳", "黄仁勋"}
    if re.search(r"\btrump\b|特朗普", blob):
        locks["trump"] = {"trump", "特朗普"}
    if re.search(r"\bwhite house\b|白宫|白宮", blob):
        locks["white_house"] = {"white house", "白宫", "白宮"}
    if re.search(r"\b(openai|anthropic|google|microsoft|meta)\b", blob):
        company_terms = set()
        for term in ("openai", "anthropic", "google", "microsoft", "meta"):
            if term in blob:
                company_terms.add(term)
        if company_terms:
            locks["ai_company"] = company_terms
    if re.search(r"\b(xai|x\.ai|grok)\b", blob):
        locks["xai_grok"] = {"xai", "x.ai", "grok"}
    if re.search(r"\b(amazon|aws|bedrock)\b|亚马逊|亞馬遜", blob):
        locks["amazon_aws"] = {"amazon", "aws", "bedrock", "亚马逊", "亞馬遜"}
    if re.search(r"\b(apple|iphone|siri|wwdc)\b|苹果", blob):
        locks["apple"] = {"apple", "iphone", "siri", "wwdc", "苹果"}
    if re.search(r"\b(qualcomm|broadcom|marvell|oracle)\b", blob):
        chip_terms = set()
        for term in ("qualcomm", "broadcom", "marvell", "oracle"):
            if term in blob:
                chip_terms.add(term)
        if chip_terms:
            locks["tech_company"] = chip_terms
    return locks


def _opennews_library_entity_hit_score(searchable: str, entity_locks: dict[str, set[str]]) -> tuple[int, set[str], set[str]]:
    hit_groups: set[str] = set()
    missed_groups: set[str] = set()
    score = 0
    for group, terms in entity_locks.items():
        hit = any(term in searchable for term in terms)
        if hit:
            hit_groups.add(group)
            if group == "nvidia_huang":
                score += 110
            elif group in {"trump", "white_house"}:
                score += 55
            elif group in {"xai_grok", "amazon_aws"}:
                score += 80
            else:
                score += 35
        else:
            missed_groups.add(group)
    return score, hit_groups, missed_groups


def _opennews_library_domain_score(
    item: dict,
    *,
    seg: dict,
    visual_domain: str,
    relevance_tokens: set[str],
    allow_generic: bool = False,
) -> tuple[bool, int, str]:
    category = str(item.get("category") or "").strip()
    primary_categories = _opennews_library_primary_categories(visual_domain, seg)
    blocked_categories = _opennews_library_blocked_categories(visual_domain, seg)
    generic_categories = {"新闻", "通用新闻", "通用氛围"}
    searchable = _opennews_library_item_searchable(item)
    item_tokens = _tokenize_opennews_relevance(searchable)
    core_tokens = _opennews_core_relevance_tokens(relevance_tokens)
    overlap = core_tokens & item_tokens
    phrase_hits = {token for token in core_tokens if " " in token and token in searchable}
    domain_hits = _opennews_domain_hits(searchable, visual_domain)
    entity_locks = _opennews_library_entity_locks(seg, relevance_tokens)
    entity_score, entity_hit_groups, entity_missed_groups = _opennews_library_entity_hit_score(searchable, entity_locks)
    query_entities = _opennews_query_named_entities(seg, relevance_tokens)
    item_entities = _opennews_named_entities_from_text(searchable)
    entity_overlap = query_entities & item_entities
    entity_conflicts = item_entities - query_entities
    topic_conflict_reason = _opennews_topic_conflict_reason(searchable, relevance_tokens, visual_domain)
    if topic_conflict_reason and not entity_overlap:
        return False, -780, topic_conflict_reason

    if category in blocked_categories:
        return False, -1000, f"素材分类 {category} 与新闻领域 {visual_domain} 冲突"
    if primary_categories:
        if category in primary_categories:
            score = 80
        elif allow_generic and category in generic_categories:
            score = 20
        else:
            return False, -500, f"素材分类 {category or '未分类'} 不属于 {', '.join(sorted(primary_categories))}"
    else:
        score = 15 if category in generic_categories else 8

    if overlap:
        score += len(overlap) * 12
    if phrase_hits:
        score += len(phrase_hits) * 18
    if domain_hits:
        score += min(len(domain_hits) * 10, 35)
    if entity_overlap:
        score += len(entity_overlap) * 55
    if topic_conflict_reason and entity_overlap:
        score -= 25
    # 同一大类不等于同一新闻。AI/科技素材里如果出现了明确公司、人物或产品，
    # 但新闻本身没有这些实体，就不要拿来当“泛 AI 图”。
    if (
        visual_domain == "technology"
        and item_entities
        and not entity_overlap
        and (item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS)
    ):
        if query_entities:
            return False, -850, (
                "同类但实体不匹配：新闻实体 "
                f"{', '.join(sorted(query_entities))}，素材实体 {', '.join(sorted(item_entities))}"
            )
        if item_entities & {"nvidia_huang", "xai_grok", "amazon_aws", "apple", "tesla_spacex"}:
            return False, -760, f"泛AI新闻禁止混入强命名实体素材：{', '.join(sorted(item_entities))}"
    # 组合事件必须同时尊重主角。比如 Grok + Amazon Bedrock 新闻，
    # 不能只因为图片里有 Amazon 就接受 OpenAI/Nvidia/手机图。
    if "xai_grok" in query_entities and "xai_grok" not in item_entities:
        if item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS:
            return False, -880, (
                "Grok/xAI 新闻素材未命中 Grok/xAI，且混入其他AI公司实体："
                f"{', '.join(sorted(item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS))}"
            )
        if not (domain_hits or phrase_hits or overlap):
            return False, -700, "Grok/xAI 新闻素材未命中主角且缺少足够上下文"
    if "amazon_aws" in query_entities and "xai_grok" in query_entities:
        if item_entities and not {"amazon_aws", "xai_grok"}.issubset(item_entities):
            return False, -890, (
                "Amazon Bedrock + Grok 组合新闻必须同时命中 Amazon/AWS/Bedrock 与 Grok/xAI，"
                f"当前素材实体：{', '.join(sorted(item_entities))}"
            )
        ai_conflicts = (item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS) - {"amazon_aws", "xai_grok"}
        if ai_conflicts and "xai_grok" not in item_entities:
            return False, -870, (
                "Amazon Bedrock + Grok 新闻禁止使用其他AI公司画面："
                f"{', '.join(sorted(ai_conflicts))}"
            )
    if (
        query_entities
        and item_entities
        and not entity_overlap
        and (item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS)
        and (query_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS)
    ):
        return False, -820, (
            "AI/科技公司实体冲突：新闻 "
            f"{', '.join(sorted(query_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS))}，素材 "
            f"{', '.join(sorted(item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS))}"
        )
    if entity_score:
        score += entity_score
    if category == "政治" and entity_locks and not (entity_hit_groups & {"trump", "white_house"}):
        return False, -900, "AI政策交叉新闻只允许命中特朗普/白宫的政治素材"
    if "nvidia_huang" in entity_locks and "nvidia_huang" not in entity_hit_groups:
        if not allow_generic and category != "政治":
            return False, -900, "黄仁勋/英伟达新闻第一轮只接受黄仁勋或英伟达相关素材"
        score -= 90
    if "nvidia_huang" in entity_locks and re.search(r"\b(apple|iphone|siri|wwdc|robot|robotics|humanoid|spacex|tesla)\b|机器人|人形机器人|苹果", searchable):
        if "nvidia_huang" not in entity_hit_groups:
            return False, -900, "黄仁勋/英伟达新闻禁止混入苹果、手机、机器人等泛科技素材"
        score -= 35
    if "trump" in entity_locks and "white_house" in entity_locks and entity_hit_groups & {"trump", "white_house"}:
        score += 20
    if ("xai_grok" in entity_locks or "amazon_aws" in entity_locks) and not (
        entity_hit_groups & {"xai_grok", "amazon_aws"}
    ):
        neutral_infra = (
            not item_entities
            and bool(domain_hits)
            and bool(re.search(r"\b(data center|server|cloud|infrastructure|compute|storage|network)\b|数据中心|服务器|云计算|算力|基础设施", searchable))
        )
        if not allow_generic and not neutral_infra:
            return False, -900, "Grok/Amazon Bedrock 新闻第一轮只接受 xAI/Grok/AWS/Amazon/Bedrock 相关素材"
        score -= 35 if neutral_infra else 95
    if entity_locks and not entity_hit_groups and allow_generic:
        score -= 45
    if str(item.get("kind") or "").lower() == "video":
        score += 8
    score -= int(item.get("usage_count") or 0)
    score += int(float(item.get("created_at") or 0) // 86400) % 7

    # 专题素材库里金融新闻必须看起来像金融新闻，不能被公司名带到科技/政治素材。
    if visual_domain == "finance" and category != "金融":
        return False, score, "金融新闻只允许金融分类素材，避免 AI/政治图混入"
    if primary_categories and category not in primary_categories and not allow_generic:
        return False, score, "第一轮只接受同领域素材"
    return score > 0, score, "通过素材库领域过滤"


def _opennews_focus_entity_terms(seg: dict, relevance_tokens: set[str]) -> list[str]:
    entities = _opennews_query_named_entities(seg, relevance_tokens)
    ordered: list[str] = []
    priority = [
        "nvidia_huang", "xai_grok", "amazon_aws", "openai", "anthropic",
        "google", "microsoft", "meta", "deepseek", "apple", "tesla_spacex",
        "trump", "white_house", "fed_powell", "iran_israel",
    ]
    for entity in priority:
        if entity not in entities:
            continue
        ordered.extend(OPENNEWS_ENTITY_DISPLAY_TERMS.get(entity, [entity]))
    for entity in sorted(entities):
        if entity in priority:
            continue
        ordered.extend(OPENNEWS_ENTITY_DISPLAY_TERMS.get(entity, [entity]))
    deduped: list[str] = []
    seen: set[str] = set()
    for term in ordered:
        key = str(term or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(str(term).strip())
    return deduped[:10]


def _opennews_focus_event_terms(relevance_tokens: set[str], visual_domain: str) -> list[str]:
    token_text = " ".join(sorted(relevance_tokens)).lower()
    candidates = {
        "finance": [
            ("stock market trading screen", ("stock", "shares", "wall street", "nasdaq", "nyse", "股市", "股票", "股价")),
            ("Federal Reserve central bank", ("fed", "federal reserve", "interest rate", "inflation", "美联储", "利率", "通胀")),
            ("oil price energy market", ("oil", "crude", "opec", "油价", "石油")),
            ("housing mortgage market", ("housing", "mortgage", "real estate", "房贷", "房地产", "首套房")),
        ],
        "technology": [
            ("AI data center server room", ("ai", "artificial intelligence", "data center", "server", "人工智能", "数据中心")),
            ("semiconductor chip laboratory", ("chip", "semiconductor", "gpu", "芯片", "半导体")),
            ("smart glasses wearable headset", ("smart glasses", "eyewear", "glasses", "virtual reality", "augmented reality", "headset", "眼镜", "头显")),
            ("software product launch", ("software", "model", "launch", "大模型", "发布")),
            ("robotics laboratory", ("robot", "robotics", "机器人")),
        ],
        "cybersecurity": [
            ("cybersecurity operations center", ("cyber", "cybersecurity", "network security", "网络安全", "网络攻击")),
            ("hacker phishing scam malware", ("hacker", "phishing", "scam", "malware", "黑客", "诈骗", "钓鱼", "恶意软件")),
            ("digital lock authentication dashboard", ("lock", "authentication", "password", "mfa", "加密", "认证", "密码")),
        ],
        "politics": [
            ("government press briefing", ("white house", "congress", "policy", "government", "白宫", "国会", "政策")),
            ("diplomacy official meeting", ("diplomacy", "minister", "foreign", "外交")),
        ],
        "military": [
            ("warship fighter jet missile drone", ("warship", "fighter", "missile", "drone", "军舰", "战机", "导弹", "无人机")),
            ("military conflict map diplomacy", ("iran", "israel", "ukraine", "russia", "伊朗", "以色列", "乌克兰")),
        ],
    }
    terms: list[str] = []
    for label, needles in candidates.get(visual_domain, []):
        if any(needle in token_text for needle in needles):
            terms.append(label)
    if not terms and visual_domain in candidates:
        terms.append(candidates[visual_domain][0][0])
    return terms[:4]


def _opennews_pexels_requires_exact_entity(seg: dict, relevance_tokens: set[str], visual_domain: str) -> bool:
    if not OPENNEWS_PEXELS_EXACT_ENTITY_REQUIRED:
        return False
    query_entities = _opennews_primary_named_entities(seg) | _opennews_query_named_entities(seg, relevance_tokens)
    hard_entities = query_entities & OPENNEWS_PEXELS_HARD_ENTITY_GROUPS
    if not hard_entities:
        return False
    if visual_domain in {"finance", "politics", "military"}:
        return True
    if hard_entities & (
        OPENNEWS_AI_COMPANY_ENTITY_GROUPS | {"trump", "white_house", "fed_powell", "iran_israel"}
    ):
        return True
    return False


def _opennews_pexels_query_candidates(seg: dict, relevance_tokens: set[str], visual_domain: str) -> list[dict]:
    candidates: list[dict] = []
    seen: set[str] = set()
    query_entities = _opennews_primary_named_entities(seg) | _opennews_query_named_entities(seg, relevance_tokens)
    focus_entities = [entity for entity in query_entities if entity in OPENNEWS_ENTITY_DISPLAY_TERMS]
    event_terms = _opennews_focus_event_terms(relevance_tokens, visual_domain)
    raw_keyword = str(seg.get("material_search_keyword") or seg.get("material_keyword") or "").strip()

    def add(query: str, *, tier: str, exact_entity: bool = False, exact_scene: bool = False) -> None:
        clean = re.sub(r"\s+", " ", str(query or "")).strip()
        key = clean.lower()
        if not clean or key in seen:
            return
        seen.add(key)
        candidates.append({
            "query": clean,
            "tier": tier,
            "exact_entity": bool(exact_entity),
            "exact_scene": bool(exact_scene),
        })

    for entity in focus_entities:
        display_terms = OPENNEWS_ENTITY_DISPLAY_TERMS.get(entity) or [entity.replace("_", " ")]
        for term in display_terms[:3]:
            base = re.sub(r"\s+", " ", str(term or "")).strip()
            if not base:
                continue
            add(base, tier="entity", exact_entity=True)
            if visual_domain == "finance":
                add(f"{base} stock market", tier="entity_context", exact_entity=True, exact_scene=True)
                add(f"{base} earnings trading screen", tier="entity_context", exact_entity=True, exact_scene=True)
            elif visual_domain == "technology":
                add(f"{base} data center", tier="entity_context", exact_entity=True, exact_scene=True)
                add(f"{base} semiconductor chip", tier="entity_context", exact_entity=True, exact_scene=True)
                add(f"{base} software product", tier="entity_context", exact_entity=True, exact_scene=True)
            elif visual_domain == "politics":
                add(f"{base} press briefing", tier="entity_context", exact_entity=True, exact_scene=True)
                add(f"{base} government building", tier="entity_context", exact_entity=True, exact_scene=True)
            elif visual_domain == "military":
                add(f"{base} military conflict", tier="entity_context", exact_entity=True, exact_scene=True)
            elif visual_domain == "cybersecurity":
                add(f"{base} cybersecurity", tier="entity_context", exact_entity=True, exact_scene=True)

    for term in event_terms:
        add(term, tier="scene", exact_scene=True)

    for query in _opennews_theme_queries(seg):
        query_lower = query.lower()
        if any(noisy in query_lower for noisy in ("news photo", "official image", "related article image")):
            query = re.sub(r"\b(news photo|official image|related article image)\b", " ", query, flags=re.I)
        add(query, tier="theme", exact_scene=True)

    if raw_keyword:
        add(raw_keyword, tier="keyword", exact_scene=not bool(focus_entities))

    return candidates[:12]


def _opennews_card_font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc" if bold else "/System/Library/Fonts/STHeiti Light.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _opennews_card_wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int, max_lines: int) -> list[str]:
    source = re.sub(r"\s+", " ", str(text or "").strip())
    if not source:
        return []
    lines: list[str] = []
    current = ""
    for char in source:
        trial = current + char
        bbox = draw.textbbox((0, 0), trial, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = trial
            continue
        if current:
            lines.append(current)
        current = char
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and "".join(lines) != source:
        lines[-1] = lines[-1].rstrip("，。,. ") + "..."
    return lines


def _opennews_generate_verified_news_card(seg: dict, output_dir: str, segment_index: int, item_index: int, *, visual_domain: str) -> str:
    title = str(seg.get("material_keyword") or seg.get("title_zh") or seg.get("title") or "OpenNews 新闻").strip() or "OpenNews 新闻"
    summary = re.sub(r"\s+", " ", str(seg.get("script") or seg.get("material_desc") or "").strip())
    if len(summary) > 140:
        summary = summary[:140].rstrip("，。,. ") + "..."
    category = _opennews_category_from_domain(visual_domain)
    size = (1600, 900)
    bg_map = {
        "科技": ("#0B1020", "#2563EB", "#BFDBFE"),
        "AI": ("#071827", "#0E7490", "#A5F3FC"),
        "金融": ("#111827", "#B45309", "#FCD34D"),
        "政治": ("#111827", "#1D4ED8", "#BFDBFE"),
        "军事": ("#101513", "#166534", "#86EFAC"),
        "房产": ("#102018", "#047857", "#6EE7B7"),
        "移民": ("#1E1B4B", "#7C3AED", "#C4B5FD"),
    }
    bg, primary, accent = bg_map.get(category, ("#111827", "#0F766E", "#99F6E4"))
    card = Image.new("RGB", size, bg)
    draw = ImageDraw.Draw(card)
    width, height = size
    for i in range(7):
        x0 = int(width * (0.6 + i * 0.05))
        draw.line((x0, -50, x0 - int(width * 0.23), height + 50), fill=primary, width=2)
    panel = (90, 90, width - 90, height - 90)
    draw.rounded_rectangle(panel, radius=36, fill="#FFFFFF", outline=accent, width=3)
    tag_font = _opennews_card_font(32, bold=True)
    title_font = _opennews_card_font(62, bold=True)
    body_font = _opennews_card_font(32, bold=False)
    foot_font = _opennews_card_font(26, bold=False)
    x = 150
    y = 150
    pill = f"{category} | OpenNews"
    pill_box = draw.textbbox((0, 0), pill, font=tag_font)
    draw.rounded_rectangle((x, y, x + pill_box[2] + 44, y + 56), radius=26, fill=primary)
    draw.text((x + 22, y + 10), pill, font=tag_font, fill="#FFFFFF")
    y += 100
    for line in _opennews_card_wrap_text(draw, title, title_font, width - 300, 3):
        draw.text((x, y), line, font=title_font, fill="#111827")
        y += 82
    if summary:
        y += 18
        for line in _opennews_card_wrap_text(draw, summary, body_font, width - 300, 4):
            draw.text((x, y), line, font=body_font, fill="#334155")
            y += 48
    footer = "准确新闻图卡 | 按新闻标题与文案生成，避免跑题素材"
    draw.line((x, height - 180, width - 150, height - 180), fill="#CBD5E1", width=2)
    draw.text((x, height - 145), footer, font=foot_font, fill="#64748B")
    output_path = os.path.join(output_dir, "materials", f"material_{segment_index:02d}_news_card_{item_index:02d}.jpg")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    card.save(output_path, "JPEG", quality=94, optimize=True)
    return output_path


def _opennews_vector_entity_constraints(seg: dict, relevance_tokens: set[str], visual_domain: str) -> tuple[list[str], list[str]]:
    query_entities = _opennews_query_named_entities(seg, relevance_tokens)
    primary_entities = _opennews_primary_named_entities(seg)
    intent_blob = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("theme_title") or ""),
        " ".join(str((theme or {}).get("title") or "") for theme in (seg.get("material_theme_plan") or []) if isinstance(theme, dict)),
    ]).lower()
    if "xai_grok" in primary_entities and "tesla" not in intent_blob and "spacex" not in intent_blob:
        primary_entities.discard("tesla_spacex")
        query_entities.discard("tesla_spacex")
    required: list[str] = []
    forbidden: list[str] = []

    def add_terms(target: list[str], entity: str) -> None:
        for term in OPENNEWS_ENTITY_DISPLAY_TERMS.get(entity, [entity]):
            text = str(term or "").strip().lower()
            if text and text not in target:
                target.append(text)

    # 强主角新闻优先把主角推到向量库检索层。服务端会让有实体标签的
    # 图片必须命中 required；无实体背景图仍会回来，再由本地二审决定。
    for entity in sorted(primary_entities):
        if entity in OPENNEWS_AI_COMPANY_ENTITY_GROUPS or entity in {"trump", "white_house", "fed_powell", "iran_israel"}:
            add_terms(required, entity)

    # 如果新闻明确讲某个 AI/科技公司，先禁止其他强公司实体，避免
    # Grok 新闻拿 OpenAI/Apple 图、黄仁勋新闻拿手机/机器人图。
    if query_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS:
        for entity in sorted(OPENNEWS_AI_COMPANY_ENTITY_GROUPS - query_entities):
            add_terms(forbidden, entity)

    # 金融新闻需要更干净：除非新闻本身明确讲某公司股票，否则不要让
    # AI公司/手机/机器人主视觉挤进来。
    if visual_domain == "finance" and not (primary_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS):
        for entity in sorted(OPENNEWS_AI_COMPANY_ENTITY_GROUPS | {"trump", "white_house"}):
            add_terms(forbidden, entity)

    # 只传前若干个，避免服务端过滤过严导致全空。
    return required[:18], forbidden[:32]


def _opennews_vector_query_text(seg: dict, visual_domain: str) -> str:
    relevance_tokens = _opennews_relevance_tokens(seg)
    entity_terms = _opennews_focus_entity_terms(seg, relevance_tokens)
    event_terms = _opennews_focus_event_terms(relevance_tokens, visual_domain)
    parts: list[str] = []
    for label, key in (
        ("category", "opennews_category"),
        ("category", "category"),
        ("headline", "material_keyword"),
        ("search_keyword", "material_search_keyword"),
        ("visual_need", "material_desc"),
        ("theme", "theme_title"),
    ):
        value = str(seg.get(key) or "").strip()
        if value:
            parts.append(f"{label}: {value}")
    if entity_terms:
        parts.append(f"main_entities: {', '.join(entity_terms)}")
    if event_terms:
        parts.append(f"event_visuals: {', '.join(event_terms)}")
    script = str(seg.get("script") or "").strip()
    if script:
        parts.append(f"script_summary: {script[:520]}")
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        for key in ("title", "visual_need", "query", "description"):
            value = str(theme.get(key) or "").strip()
            if value:
                parts.append(f"theme_{key}: {value[:220]}")
        for query in theme.get("queries") or []:
            value = str(query or "").strip()
            if value:
                parts.append(f"theme_query: {value[:180]}")
    if visual_domain:
        parts.append(f"required_visual_domain: {visual_domain}")
    parts.append("match requirement: choose approved local news image with the same main entity or the same concrete scene, not just the same broad category")
    return "\n".join(parts)


def _opennews_vector_analysis_terms(analysis: dict) -> list[str]:
    terms: list[str] = []
    for key in ("category", "description", "entities", "scenes", "concepts", "visible_text"):
        value = analysis.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    terms.extend(str(v) for v in item.values() if v)
                else:
                    terms.append(str(item or ""))
        elif value:
                terms.append(str(value))
    return [term.strip() for term in terms if term and term.strip()]


def _opennews_qwen_review_title(seg: dict, item: dict) -> str:
    # Keep this neutral: the 5090 vision model should describe what is visible,
    # not infer relevance from our target headline/search query.
    parts = [
        "OpenNews source candidate image",
        f"source_title: {str(item.get('title') or '')[:180]}",
        f"source_url: {str(item.get('source_url') or item.get('url') or '')[:220]}",
        "instruction: describe only visible image content; do not infer from headline or URL",
    ]
    return "\n".join(part for part in parts if part.strip())


def _opennews_qwen_analyze_uploaded_image(path: str, *, title: str, material_id: str, timeout_seconds: int | None = None) -> dict:
    if not OPENNEWS_MATERIAL_VECTOR_URL:
        raise RuntimeError("未配置 5090 Qwen3-VL 素材审核服务")
    request_timeout = timeout_seconds or OPENNEWS_QWEN_REVIEW_TIMEOUT_SECONDS
    with open(path, "rb") as handle:
        response = requests.post(
            f"{OPENNEWS_MATERIAL_VECTOR_URL}/analyze-upload",
            data={"material_id": material_id, "title": title},
            files={"file": (os.path.basename(path), handle, "application/octet-stream")},
            timeout=request_timeout,
        )
    response.raise_for_status()
    payload = response.json()
    analysis = payload.get("analysis") if isinstance(payload, dict) else {}
    return analysis if isinstance(analysis, dict) else {}


def _opennews_analysis_safety_reason(analysis: dict) -> str:
    safety_status = str(analysis.get("safety_status") or "safe").strip().lower()
    text = " ".join(_opennews_vector_analysis_terms(analysis)).lower()
    unsafe_tokens = (
        "nude", "nudity", "naked", "sexual", "porn", "pornographic", "erotic",
        "lingerie", "underwear", "bikini", "swimsuit", "cleavage", "shirtless",
        "blood", "gore", "graphic violence", "暴露", "裸露", "色情", "内衣", "泳装",
        "血腥", "暴力血腥",
    )
    if safety_status in {"blocked", "risky"}:
        return f"Qwen3-VL safety_status={safety_status}"
    if any(token in text for token in unsafe_tokens):
        return "Qwen3-VL 安全描述命中裸露/色情/血腥风险词"
    return ""


def _opennews_prepare_qwen_review_image(path: str) -> tuple[str, bool]:
    """Resize large crawler images before Qwen review; keep original for final video."""
    try:
        with Image.open(path) as image:
            image = image.convert("RGB")
            width, height = image.size
            max_side = max(width, height)
            if max_side <= OPENNEWS_QWEN_REVIEW_MAX_SIDE and os.path.getsize(path) <= 1_800_000:
                return path, False
            scale = OPENNEWS_QWEN_REVIEW_MAX_SIDE / float(max_side)
            new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
            image = image.resize(new_size, Image.LANCZOS)
            fd, temp_path = tempfile.mkstemp(prefix="opennews_qwen_review_", suffix=".jpg")
            os.close(fd)
            image.save(temp_path, format="JPEG", quality=86, optimize=True)
            return temp_path, True
    except Exception:
        return path, False


def _opennews_analysis_searchable_item(analysis: dict, *, visual_domain: str) -> dict:
    analysis_terms = _opennews_vector_analysis_terms(analysis)
    return {
        "kind": "image",
        "category": _opennews_category_from_domain(visual_domain),
        "title": "",
        "tags": analysis_terms,
        "ai_tags": analysis_terms,
        "news_topics": analysis_terms,
        "notes": analysis.get("description") or "",
        "source_url": "",
        "source_site": "",
        "original_filename": "",
    }


def _opennews_concrete_scene_hit(searchable: str, visual_domain: str) -> bool:
    text = str(searchable or "").lower()
    patterns = {
        "cybersecurity": (
            r"\b(cybersecurity|cyber security|hacker|hacking|phishing|malware|ransomware|"
            r"network security|security operations center|server rack|firewall|password|"
            r"authentication|encrypted|digital lock|data breach)\b|网络安全|黑客|钓鱼|恶意软件|"
            r"勒索软件|防火墙|密码|认证|加密|数据泄露"
        ),
        "technology": (
            r"\b(data center|server room|server rack|semiconductor|chip|gpu|robotics|robot|"
            r"software dashboard|ai model|computer lab|engineer|technology company|"
            r"smart glasses|eyewear|virtual reality|augmented reality|mixed reality|headset|wearable)\b|"
            r"数据中心|服务器|芯片|半导体|机器人|大模型|算力|实验室"
        ),
        "finance": (
            r"\b(stock market|trading screen|wall street|nasdaq|nyse|investor|bank|"
            r"federal reserve|interest rate|inflation|oil price|crude oil|housing market|mortgage|"
            r"forex|currency|exchange rate|british pound|pound sterling|japanese yen|banknote|cash|"
            r"calculator|financial analysis|chart|graph)\b|"
            r"股市|交易屏|华尔街|美联储|利率|通胀|油价|石油|房贷|房地产市场"
        ),
        "military": (
            r"\b(warship|fighter jet|missile|drone|soldier|troops|military exercise|"
            r"defense briefing|naval|air force|combat vehicle)\b|军舰|战机|导弹|无人机|军演|国防|部队"
        ),
        "politics": (
            r"\b(white house|congress|parliament|government building|press briefing|"
            r"diplomatic meeting|minister|president|official flag)\b|白宫|国会|政府|记者会|外交|总统|部长"
        ),
        "real_estate": (
            r"\b(house|housing|apartment|residential|real estate|mortgage|property|city skyline)\b|"
            r"住宅|公寓|房地产|房贷|房产|城市天际线"
        ),
    }
    pattern = patterns.get(visual_domain)
    return bool(pattern and re.search(pattern, text, flags=re.I))


def _opennews_strict_realtime_visual_decision(
    analysis: dict,
    *,
    seg: dict,
    relevance_tokens: set[str],
    visual_domain: str,
) -> tuple[bool, str, int]:
    """Realtime web images must visually match the news, not just the broad category."""
    enriched = _opennews_analysis_searchable_item(analysis, visual_domain=visual_domain)
    searchable = _opennews_library_item_searchable(enriched)
    item_tokens = _tokenize_opennews_relevance(searchable)
    core_tokens = _opennews_core_relevance_tokens(relevance_tokens)
    overlap = core_tokens & item_tokens
    phrase_hits = {token for token in core_tokens if " " in token and token in searchable}
    domain_hits = _opennews_domain_hits(searchable, visual_domain)
    query_entities = _opennews_query_named_entities(seg, relevance_tokens)
    item_entities = _opennews_named_entities_from_text(searchable)
    entity_overlap = query_entities & item_entities
    strong_entities = query_entities & (
        OPENNEWS_AI_COMPANY_ENTITY_GROUPS | {"trump", "white_house", "fed_powell", "iran_israel"}
    )
    conflict_reason = _opennews_topic_conflict_reason(searchable, relevance_tokens, visual_domain)
    if conflict_reason and not entity_overlap:
        return False, f"视觉内容主题冲突：{conflict_reason}", -900

    has_scene = _opennews_concrete_scene_hit(searchable, visual_domain)
    event_terms = _opennews_focus_event_terms(relevance_tokens, visual_domain)
    event_hits = {term for term in event_terms if term.lower() in searchable}
    concrete_hit = bool(entity_overlap or phrase_hits or event_hits or has_scene)

    if strong_entities:
        if item_entities and not entity_overlap:
            return False, (
                "视觉图出现了其他明确主体，未命中新闻主角："
                f"news={', '.join(sorted(strong_entities))}; image={', '.join(sorted(item_entities))}"
            ), -860
        if not entity_overlap and not (has_scene or event_hits):
            return False, "视觉图未命中新闻主角，也缺少可接受的同场景背景", -760
    elif query_entities and item_entities and not entity_overlap:
        return False, (
            "视觉图实体与新闻实体不一致："
            f"news={', '.join(sorted(query_entities))}; image={', '.join(sorted(item_entities))}"
        ), -780

    if visual_domain == "cybersecurity" and not (domain_hits or has_scene):
        return False, "网络安全新闻图片缺少安全运营、黑客、恶意软件、加密等可见语义", -820
    if visual_domain == "finance" and not (domain_hits or has_scene or entity_overlap):
        return False, "金融新闻图片缺少股市、交易、银行、美联储、油价或房地产市场语义", -800
    if visual_domain == "technology" and not (domain_hits or has_scene or entity_overlap):
        return False, "科技/AI新闻图片缺少AI、芯片、数据中心、软件或机器人等可见语义", -790
    if not concrete_hit and len(overlap) < 2:
        return False, "视觉图只达到泛相关，未命中新闻核心对象或具体场景", -650

    score = 0
    score += len(overlap) * 18
    score += len(phrase_hits) * 30
    score += len(domain_hits) * 20
    score += len(event_hits) * 24
    if has_scene:
        score += 45
    if entity_overlap:
        score += len(entity_overlap) * 90
    if strong_entities and not entity_overlap:
        score -= 35
    if score < 58:
        return False, f"视觉相关性不足：{score}", score
    return True, f"视觉审核通过：score={score}", score


def _opennews_review_source_image_with_qwen(
    path: str,
    item: dict,
    *,
    seg: dict,
    relevance_tokens: set[str],
    visual_domain: str,
) -> tuple[bool, str, dict, int]:
    title = _opennews_qwen_review_title(seg, item)
    review_id = "review_" + hashlib.sha1(
        f"{path}|{title}|{time.time()}".encode("utf-8", errors="ignore")
    ).hexdigest()[:18]
    review_path, remove_review_path = _opennews_prepare_qwen_review_image(path)
    try:
        analysis = _opennews_qwen_analyze_uploaded_image(review_path, title=title, material_id=review_id)
    finally:
        if remove_review_path:
            try:
                os.remove(review_path)
            except Exception:
                pass
    safety_reason = _opennews_analysis_safety_reason(analysis)
    if safety_reason:
        return False, safety_reason, analysis, -1000

    strict_keep, strict_reason, strict_score = _opennews_strict_realtime_visual_decision(
        analysis,
        seg=seg,
        relevance_tokens=relevance_tokens,
        visual_domain=visual_domain,
    )
    if not strict_keep:
        return False, f"Qwen3-VL 严格视觉审核未通过：{strict_reason}", analysis, strict_score

    # For semantic review, never score with the crawler query/title/source URL.
    # Those fields often contain the target news text and can falsely prove relevance.
    enriched = _opennews_analysis_searchable_item(analysis, visual_domain=visual_domain)
    keep, domain_score, reason = _opennews_library_domain_score(
        enriched,
        seg=seg,
        visual_domain=visual_domain,
        relevance_tokens=relevance_tokens,
        allow_generic=False,
    )
    if not keep:
        keep, domain_score, reason = _opennews_library_domain_score(
            enriched,
            seg=seg,
            visual_domain=visual_domain,
            relevance_tokens=relevance_tokens,
            allow_generic=True,
        )
    if not keep:
        return False, f"Qwen3-VL 语义审核未通过：{reason}", analysis, domain_score
    focus_keep, focus_reason, focus_score = _opennews_vector_focus_decision(
        enriched,
        seg=seg,
        relevance_tokens=relevance_tokens,
        visual_domain=visual_domain,
        vector_score=0.72,
    )
    if not focus_keep:
        return False, f"Qwen3-VL 主题二审未通过：{focus_reason}", analysis, focus_score
    final_score = int(domain_score + focus_score + strict_score + 80)
    return True, f"{strict_reason}; {reason}; {focus_reason}", analysis, final_score


def _opennews_category_from_domain(visual_domain: str) -> str:
    return {
        "ai": "AI",
        "technology": "科技",
        "cybersecurity": "科技",
        "finance": "金融",
        "real_estate": "房产",
        "military": "军事",
        "politics": "政治",
        "immigration": "移民",
        "energy": "能源",
    }.get(str(visual_domain or "").lower(), "通用新闻")


def _opennews_sync_registered_material_to_vector(item: dict) -> None:
    if not OPENNEWS_MATERIAL_VECTOR_URL:
        return
    filename = os.path.basename(str(item.get("filename") or ""))
    if not filename:
        return
    file_path = (MATERIAL_LIBRARY_DIR / filename).resolve()
    library_root = MATERIAL_LIBRARY_DIR.resolve()
    if not str(file_path).startswith(str(library_root)) or not file_path.exists():
        return
    title = " | ".join(
        part
        for part in [
            str(item.get("title") or ""),
            str(item.get("category") or ""),
            " ".join(map(str, item.get("ai_tags") or [])),
            " ".join(map(str, item.get("news_topics") or [])),
        ]
        if part.strip()
    )
    try:
        _opennews_qwen_analyze_uploaded_image(
            str(file_path),
            title=title,
            material_id=f"prod_{item.get('id') or file_path.stem}",
        )
    except Exception as exc:
        print(f"  ⚠️ 实时素材已入库，但同步 5090 向量库失败：{item.get('id') or filename}｜{exc}")


def _opennews_import_reviewed_source_image(
    path: str,
    item: dict,
    *,
    seg: dict,
    analysis: dict,
    visual_domain: str,
    review_reason: str,
) -> str:
    if not OPENNEWS_REALTIME_SOURCE_AUTO_IMPORT:
        return ""
    suffix = os.path.splitext(path)[1].lower() or ".jpg"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        temp_copy = tmp.name
    shutil.copy2(path, temp_copy)
    title = (
        str(item.get("title") or "").strip()
        or str(seg.get("material_keyword") or "").strip()
        or f"OpenNews 实时素材 {int(time.time())}"
    )[:180]
    tags = _opennews_vector_analysis_terms(analysis)
    try:
        registered = register_material_file(
            temp_path=temp_copy,
            original_filename=f"opennews_realtime_{hashlib.sha1(path.encode()).hexdigest()[:10]}{suffix}",
            title=title,
            category=_opennews_category_from_domain(visual_domain),
            tags=tags[:18],
            ai_tags=tags[:24],
            notes=f"OpenNews 实时网络素材，Qwen3-VL 审核通过：{review_reason}",
            uploader_username="opennews_auto",
            uploader_display_name="OpenNews 自动入库",
            source="opennews_realtime_qwen_reviewed",
            source_url=item.get("source_url") or item.get("url") or "",
            source_site=urlparse(str(item.get("source_url") or item.get("url") or "")).netloc,
            license_note="自动抓取的新闻相关网络素材；用于内部新闻视频生产。",
            safety_status=str(analysis.get("safety_status") or "safe"),
            news_topics=[
                str(seg.get("material_keyword") or ""),
                str(seg.get("material_search_keyword") or ""),
                *tags[:12],
            ],
        )
        updated = update_material_library_item(
            str(registered.get("id") or ""),
            {
                "status": "approved",
                "reviewed_at": time.time(),
                "reviewed_by_username": "opennews_auto",
                "reviewed_by_display_name": "OpenNews 自动审核",
                "ai_provider": "5090-qwen3-vl-realtime-review",
                "ai_summary": str(analysis.get("description") or "").strip(),
                "ai_tags": tags[:32],
                "safety_status": str(analysis.get("safety_status") or "safe"),
            },
        )
        _opennews_sync_registered_material_to_vector(updated)
        print(f"  ✅ 实时素材已自动入正式素材库：{updated.get('id')}｜{title}")
        return str(updated.get("id") or "")
    except Exception as exc:
        try:
            if os.path.exists(temp_copy):
                os.remove(temp_copy)
        except Exception:
            pass
        print(f"  ⚠️ 实时素材自动入库失败，不影响本次视频：{exc}")
        return ""


def _opennews_vector_focus_decision(
    item: dict,
    *,
    seg: dict,
    relevance_tokens: set[str],
    visual_domain: str,
    vector_score: float,
) -> tuple[bool, str, int]:
    searchable = _opennews_library_item_searchable(item)
    query_entities = _opennews_query_named_entities(seg, relevance_tokens)
    item_entities = _opennews_named_entities_from_text(searchable)
    entity_overlap = query_entities & item_entities
    domain_hits = _opennews_domain_hits(searchable, visual_domain)
    item_relevance_score = _source_material_relevance_score(item, relevance_tokens)
    event_terms = _opennews_focus_event_terms(relevance_tokens, visual_domain)
    event_hit_terms = {term for term in event_terms if term.lower() in searchable}
    related_scene_hits = domain_hits or event_hit_terms or bool(
        re.search(
            r"\b(data center|server room|server rack|cloud|ai model|chatbot|software|dashboard|"
            r"stock market|trading screen|wall street|oil price|refinery|housing|mortgage|"
            r"government building|press briefing|warship|fighter jet|missile|drone)\b|"
            r"数据中心|服务器|云计算|大模型|聊天机器人|股市|交易屏|华尔街|油价|炼油厂|房贷|"
            r"政府|白宫|记者会|军舰|战机|导弹|无人机",
            searchable,
        )
    )
    topic_conflict_reason = _opennews_topic_conflict_reason(searchable, relevance_tokens, visual_domain)

    if topic_conflict_reason and not entity_overlap:
        return False, f"向量命中但主题冲突：{topic_conflict_reason}", -900

    # 如果新闻和素材双方都有明确实体，必须有交集。否则“AI新闻”很容易
    # 从本地库里误拿到 Apple/Nvidia/机器人等同类但不对应的图。
    if query_entities and item_entities and not entity_overlap:
        if item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS or query_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS:
            return False, (
                "向量命中但实体不匹配：新闻 "
                f"{', '.join(sorted(query_entities))}，素材 {', '.join(sorted(item_entities))}"
            ), -880

    # 新闻有强主角实体时，优先要主角相同；没有同主角时只接受“无明确实体”
    # 且领域足够明确的背景图，避免拿到另一个公司的主视觉。
    strong_entities = query_entities & (
        OPENNEWS_AI_COMPANY_ENTITY_GROUPS | {"trump", "white_house", "fed_powell", "iran_israel"}
    )
    if strong_entities and not entity_overlap:
        if item_entities:
            return False, (
                "向量命中但缺少新闻主角："
                f"{', '.join(sorted(strong_entities))}"
            ), -820
        if not related_scene_hits:
            return False, "向量命中为泛背景图，但没有足够同领域/同场景支撑", -520

    # 金融新闻尤其容易被公司名带偏；没有金融语义的图不要。
    if visual_domain == "finance":
        finance_hits = _opennews_domain_hits(searchable, "finance")
        if not finance_hits and not (query_entities & item_entities & {"fed_powell"}):
            return False, "金融新闻向量候选缺少金融语义", -760
    if visual_domain == "cybersecurity":
        cyber_hits = _opennews_domain_hits(searchable, "cybersecurity")
        if not cyber_hits:
            return False, "网络安全新闻向量候选缺少网络安全语义", -780

    bonus = 0
    if entity_overlap:
        bonus += len(entity_overlap) * 90
    elif strong_entities and related_scene_hits and not item_entities:
        bonus += 36
    if domain_hits:
        bonus += min(len(domain_hits) * 18, 54)
    if event_hit_terms:
        bonus += min(len(event_hit_terms) * 16, 48)
    bonus += min(item_relevance_score, 40)
    bonus += int(vector_score * 100)
    if strong_entities and not entity_overlap and not item_entities:
        return True, "未命中主角，使用同领域/同场景相关背景图", bonus
    return True, "通过向量实体二审", bonus


def _search_opennews_material_vector_fallback(
    seg: dict,
    *,
    visual_domain: str,
    target_market: str = "",
    department_id: str = "",
    limit_images: int = 1,
) -> list[dict]:
    if not OPENNEWS_MATERIAL_VECTOR_ENABLED or not OPENNEWS_MATERIAL_VECTOR_URL or limit_images <= 0:
        return []

    query = _opennews_vector_query_text(seg, visual_domain)
    if not query.strip():
        return []
    relevance_tokens = _opennews_relevance_tokens(seg)
    required_entities, forbidden_entities = _opennews_vector_entity_constraints(seg, relevance_tokens, visual_domain)
    if required_entities or forbidden_entities:
        print(
            "  🧭 向量素材实体约束："
            f"required={required_entities[:8] or []} "
            f"forbidden={forbidden_entities[:10] or []}"
        )

    def run_vector_search(*, mode: str) -> list[dict]:
        if mode == "strict":
            payload_required = required_entities
            payload_forbidden = forbidden_entities
        elif mode == "related":
            payload_required = []
            payload_forbidden = forbidden_entities
        else:
            payload_required = []
            payload_forbidden = []
        response = requests.post(
            f"{OPENNEWS_MATERIAL_VECTOR_URL}/search",
            json={
                "query": query,
                "required_entities": payload_required,
                "forbidden_entities": payload_forbidden,
                "top_k": max(20, min(60, limit_images * 14)),
            },
            timeout=OPENNEWS_MATERIAL_VECTOR_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        vector_rows = payload.get("items") if isinstance(payload, dict) else []
        return vector_rows if isinstance(vector_rows, list) else []

    try:
        vector_items = run_vector_search(mode="strict")
        if not vector_items and (required_entities or forbidden_entities):
            print("  ℹ️ 向量素材精准约束无结果，改用相关素材候选并继续本地二审")
            vector_items = run_vector_search(mode="related")
        if not vector_items and (required_entities or forbidden_entities):
            print("  ℹ️ 向量素材相关候选仍为空，最后尝试无硬约束候选并继续本地二审")
            vector_items = run_vector_search(mode="loose")
    except Exception as exc:
        print(f"  ⚠️ 5090 向量素材库不可用，回退普通素材库：{exc}")
        return []

    if not isinstance(vector_items, list) or not vector_items:
        return []

    approved = list_material_library_items(status="approved")
    by_id = {str(item.get("id") or ""): item for item in approved if item.get("id")}
    by_filename = {str(item.get("filename") or ""): item for item in approved if item.get("filename")}
    selected: list[dict] = []
    selected_ids: set[str] = set()
    selected_fingerprints: set[str] = set()
    scored: list[tuple[int, dict]] = []

    for vector_item in vector_items:
        if not isinstance(vector_item, dict):
            continue
        material_id = str(vector_item.get("material_id") or "").strip()
        raw_id = material_id[5:] if material_id.startswith("prod_") else material_id
        path_name = os.path.basename(str(vector_item.get("path") or ""))
        item = by_id.get(raw_id) or by_filename.get(path_name)
        if not item:
            continue
        if str(item.get("kind") or "").lower() != "image":
            continue
        usage_ok, usage_reason = _opennews_library_image_usage_status(item)
        if not usage_ok:
            print(f"  ⏳ 向量素材复用频控过滤：{usage_reason}｜{item.get('title') or item.get('filename')}")
            continue
        item_id = str(item.get("id") or "")
        if item_id and item_id in selected_ids:
            continue
        markets = {value.lower() for value in item.get("target_markets") or []}
        departments = {value.lower() for value in item.get("department_ids") or []}
        if markets and target_market and target_market.lower() not in markets:
            continue
        if departments and department_id and department_id.lower() not in departments:
            continue
        analysis = vector_item.get("analysis") if isinstance(vector_item.get("analysis"), dict) else {}
        enriched = {**item}
        vector_terms = _opennews_vector_analysis_terms(analysis)
        enriched["ai_tags"] = list(item.get("ai_tags") or []) + vector_terms
        enriched["news_topics"] = list(item.get("news_topics") or []) + vector_terms
        if analysis.get("category") and not enriched.get("category"):
            categories = analysis.get("category")
            if isinstance(categories, list) and categories:
                enriched["category"] = str(categories[0])
        keep, domain_score, reason = _opennews_library_domain_score(
            enriched,
            seg=seg,
            visual_domain=visual_domain,
            relevance_tokens=relevance_tokens,
            allow_generic=False,
        )
        if not keep:
            keep, domain_score, reason = _opennews_library_domain_score(
                enriched,
                seg=seg,
                visual_domain=visual_domain,
                relevance_tokens=relevance_tokens,
                allow_generic=True,
            )
        if not keep:
            continue
        vector_score = float(vector_item.get("score") or 0.0)
        focus_keep, focus_reason, focus_score = _opennews_vector_focus_decision(
            enriched,
            seg=seg,
            relevance_tokens=relevance_tokens,
            visual_domain=visual_domain,
            vector_score=vector_score,
        )
        if not focus_keep:
            print(f"  ⚠️ 向量素材二审过滤：{focus_reason}｜{item.get('title') or item.get('filename')}")
            continue
        final_score = int(domain_score + focus_score + vector_score * 120)
        scored.append((
            final_score,
            {
                **item,
                "score": final_score,
                "opennews_library_fallback": True,
                "opennews_vector_match": True,
                "vector_score": vector_score,
                "vector_reason": f"{reason}; {focus_reason}",
                "vector_analysis": analysis,
            },
        ))

    scored.sort(key=lambda pair: (pair[0], pair[1].get("created_at", 0)), reverse=True)
    for _score, item in scored:
        item_id = str(item.get("id") or "")
        if item_id and item_id in selected_ids:
            continue
        fingerprint = _opennews_library_item_fingerprint(item)
        if fingerprint and fingerprint in selected_fingerprints:
            continue
        selected.append(item)
        if item_id:
            selected_ids.add(item_id)
        if fingerprint:
            selected_fingerprints.add(fingerprint)
        if len(selected) >= limit_images:
            break
    return selected


def _search_opennews_material_library_fallback(
    seg: dict,
    *,
    visual_domain: str,
    target_market: str = "",
    department_id: str = "",
    limit_videos: int = 0,
    limit_images: int = 1,
) -> list[dict]:
    selected: list[dict] = []
    selected_ids: set[str] = set()
    selected_fingerprints: set[str] = set()
    video_count = 0
    image_count = 0
    relevance_tokens = _opennews_relevance_tokens(seg)

    def append_from_pool(pool: list[tuple[int, dict]]) -> None:
        nonlocal video_count, image_count
        for score, item in pool:
            item_id = str(item.get("id") or "")
            if item_id in selected_ids:
                continue
            fingerprint = _opennews_library_item_fingerprint(item)
            if fingerprint and fingerprint in selected_fingerprints:
                continue
            kind = str(item.get("kind") or "").lower()
            if kind == "video":
                if video_count >= limit_videos:
                    continue
                video_count += 1
            else:
                if image_count >= limit_images:
                    continue
                image_count += 1
            selected_ids.add(item_id)
            if fingerprint:
                selected_fingerprints.add(fingerprint)
            selected.append({**item, "score": int(score), "opennews_library_fallback": True})
            if video_count >= limit_videos and image_count >= limit_images:
                break

    for allow_generic in (False, True):
        if video_count >= limit_videos and image_count >= limit_images:
            break
        pool: list[tuple[int, dict]] = []
        for item in list_material_library_items(status="approved"):
            item_id = str(item.get("id") or "")
            if item_id in selected_ids:
                continue
            kind = str(item.get("kind") or "").lower()
            if kind not in {"image", "video"}:
                continue
            usage_ok, usage_reason = _opennews_library_image_usage_status(item)
            if not usage_ok:
                print(f"  ⏳ 本地素材库复用频控过滤：{usage_reason}｜{item.get('title') or item.get('filename')}")
                continue
            if kind == "video" and video_count >= limit_videos:
                continue
            if kind != "video" and image_count >= limit_images:
                continue
            markets = {value.lower() for value in item.get("target_markets") or []}
            departments = {value.lower() for value in item.get("department_ids") or []}
            if markets and target_market and target_market.lower() not in markets:
                continue
            if departments and department_id and department_id.lower() not in departments:
                continue
            keep, score, _reason = _opennews_library_domain_score(
                item,
                seg=seg,
                visual_domain=visual_domain,
                relevance_tokens=relevance_tokens,
                allow_generic=allow_generic,
            )
            if keep:
                pool.append((score, item))
        pool.sort(key=lambda pair: (pair[0], pair[1].get("created_at", 0)), reverse=True)
        append_from_pool(pool)
    return selected


def _search_opennews_material_library_safe_any(
    *,
    visual_domain: str,
    seg: dict | None = None,
    relevance_tokens: set[str] | None = None,
    target_market: str = "",
    department_id: str = "",
    limit_images: int = 1,
) -> list[dict]:
    """Final OpenNews safety fallback: approved local library images only, no web fetch."""
    if limit_images <= 0:
        return []
    primary_categories = _opennews_library_primary_categories(visual_domain, None)
    generic_categories = {"新闻", "通用新闻", "通用氛围", "城市街景"}
    seg = seg or {}
    relevance_tokens = relevance_tokens or set()
    query_entities = _opennews_query_named_entities(seg, relevance_tokens) if seg else set()
    pool: list[tuple[int, dict]] = []
    for item in list_material_library_items(status="approved"):
        kind = str(item.get("kind") or "").lower()
        if kind != "image":
            continue
        usage_ok, usage_reason = _opennews_library_image_usage_status(item)
        if not usage_ok:
            print(f"  ⏳ 安全兜底素材复用频控过滤：{usage_reason}｜{item.get('title') or item.get('filename')}")
            continue
        markets = {value.lower() for value in item.get("target_markets") or []}
        departments = {value.lower() for value in item.get("department_ids") or []}
        if markets and target_market and target_market.lower() not in markets:
            continue
        if departments and department_id and department_id.lower() not in departments:
            continue
        category = str(item.get("category") or "").strip()
        if primary_categories and category not in primary_categories and category not in generic_categories:
            continue
        searchable = _opennews_library_item_searchable(item)
        item_entities = _opennews_named_entities_from_text(searchable)
        entity_overlap = query_entities & item_entities
        topic_conflict_reason = _opennews_topic_conflict_reason(searchable, relevance_tokens, visual_domain)
        if topic_conflict_reason and not entity_overlap:
            continue
        item_relevance_score = _source_material_relevance_score(item, relevance_tokens) if relevance_tokens else 0
        if query_entities and not entity_overlap and not item_entities and category in generic_categories and item_relevance_score < 18:
            continue
        if query_entities and item_entities and not entity_overlap:
            continue
        if (
            visual_domain == "technology"
            and not query_entities
            and (item_entities & {"nvidia_huang", "xai_grok", "amazon_aws", "apple", "tesla_spacex"})
        ):
            continue
        score = 10
        if category in primary_categories:
            score += 60
        elif category in generic_categories:
            score += 30
        if query_entities and entity_overlap:
            score += len(entity_overlap) * 60
        elif query_entities and not item_entities:
            score += 8
        elif item_entities:
            score -= 20
        if relevance_tokens:
            score += min(item_relevance_score, 35)
        score -= int(item.get("usage_count") or 0)
        score += int(float(item.get("created_at") or 0) // 86400) % 7
        pool.append((score, item))
    pool.sort(key=lambda pair: (pair[0], pair[1].get("created_at", 0)), reverse=True)
    selected: list[dict] = []
    seen_fingerprints: set[str] = set()
    for score, item in pool:
        fingerprint = _opennews_library_item_fingerprint(item)
        if fingerprint and fingerprint in seen_fingerprints:
            continue
        if fingerprint:
            seen_fingerprints.add(fingerprint)
        selected.append({**item, "score": int(score), "opennews_library_fallback": True, "opennews_safe_any_fallback": True})
        if len(selected) >= limit_images:
            break
    return selected


def _append_library_material_items(
    *,
    library_items: list[dict],
    material_items: list[dict],
    material_paths: list[str],
    output_dir: str,
    segment_index: int,
    max_total_materials: int,
    max_source_videos: int,
    max_source_images: int,
    current_video_count: int,
    current_image_count: int,
    used_library_ids: set[str],
    is_opennews_material_only: bool,
) -> tuple[int, int]:
    video_count = current_video_count
    image_count = current_image_count
    for item in library_items:
        if len(material_items) >= max_total_materials:
            break
        item_kind = str(item.get("kind") or "").lower()
        if item_kind == "video" and video_count >= max_source_videos:
            continue
        if item_kind != "video" and image_count >= max_source_images:
            continue
        library_key = str(item.get("id") or item.get("path") or item.get("filename") or "")
        if library_key and library_key in used_library_ids:
            continue
        usage_ok, usage_reason = _opennews_library_image_usage_status(item)
        if not usage_ok:
            print(f"  ⏳ 本地素材库素材复用频控过滤：{usage_reason}｜{item.get('title') or item.get('filename')}")
            continue
        copied_path = copy_material_to_output(item, output_dir, segment_index, len(material_items))
        ok, reason = _opennews_material_path_is_usable(copied_path, str(item.get("kind") or "")) if is_opennews_material_only else (True, "")
        if not ok:
            print(f"  ⚠️ 本地素材库素材被过滤：{reason}｜{os.path.basename(copied_path)}")
            try:
                os.remove(copied_path)
            except Exception:
                pass
            continue
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=item.get("kind"), source="library")
        entry["library_id"] = item.get("id", "")
        entry["title"] = item.get("title", "")
        entry["library_score"] = item.get("score", 0)
        if item.get("opennews_library_fallback"):
            entry["opennews_library_fallback"] = True
        if item.get("opennews_vector_match"):
            entry["opennews_vector_match"] = True
            entry["vector_score"] = item.get("vector_score", 0)
            entry["vector_reason"] = item.get("vector_reason", "")
        material_items.append(entry)
        if library_key:
            used_library_ids.add(library_key)
        if item_kind == "video":
            video_count += 1
        else:
            image_count += 1
            _opennews_record_library_image_usage(item)
        print(f"  ✅ 已命中本地素材库：{os.path.basename(copied_path)}")
    return video_count, image_count


def _opennews_pexels_searchable_item(item: dict, *, visual_domain: str) -> dict:
    alt = str(item.get("alt") or "").strip()
    url = str(item.get("url") or "").strip()
    return {
        "kind": "image",
        "category": _opennews_category_from_domain(visual_domain),
        "title": alt,
        "tags": [alt] if alt else [],
        "ai_tags": [],
        "news_topics": [],
        "notes": alt,
        "source_url": url,
        "source_site": "pexels",
        "original_filename": os.path.basename(urlparse(url).path or ""),
        "source": "pexels",
    }


def _opennews_pexels_candidate_score(
    item: dict,
    relevance_tokens: set[str],
    *,
    query: str = "",
    visual_domain: str = "general",
) -> int:
    alt = str(item.get("alt") or "").strip()
    candidate = {
        "title": alt,
        "url": str(item.get("url") or ""),
        "related_query": "",
        "theme_title": "",
        "source": "pexels",
    }
    score = _source_material_relevance_score(candidate, relevance_tokens)
    alt_lower = alt.lower()
    query_tokens = _tokenize_opennews_relevance(query)
    query_overlap = relevance_tokens & query_tokens
    if query_overlap:
        score += min(len(query_overlap) * 3, 12)
    domain_hits = _opennews_domain_hits(alt_lower, visual_domain)
    if domain_hits:
        score += min(len(domain_hits) * 8, 24)
    if _opennews_concrete_scene_hit(alt_lower, visual_domain):
        score += 16
    event_hits = {
        term
        for term in _opennews_focus_event_terms(relevance_tokens, visual_domain)
        if term.lower() in alt_lower
    }
    if event_hits:
        score += min(len(event_hits) * 10, 24)
    query_entities = _opennews_query_named_entities({}, relevance_tokens)
    item_entities = _opennews_named_entities_from_text(alt_lower)
    entity_overlap = query_entities & item_entities
    if entity_overlap:
        score += len(entity_overlap) * 35
    orientation = str(item.get("orientation") or "").strip().lower()
    if orientation == "portrait":
        score += 8
    elif orientation == "square":
        score += 4
    alt_text = str(item.get("alt") or "").strip().lower()
    if alt_text and re.search(r"\b(news|press|briefing|official|government|market|stock|ai|chip|housing|military|policy)\b", alt_text):
        score += 6
    return score


def _opennews_pexels_candidate_decision(
    item: dict,
    *,
    seg: dict,
    relevance_tokens: set[str],
    visual_domain: str,
    query: str,
    score: int,
    exact_entity_required: bool = False,
    exact_scene_required: bool = False,
) -> tuple[bool, str, int]:
    if not OPENNEWS_PEXELS_STRICT_MATCH_ENABLED:
        return True, "Pexels 严格相关性二审已关闭", score
    alt = str(item.get("alt") or "").strip()
    url = str(item.get("url") or "").strip()
    searchable = " ".join(part for part in (alt, url) if part).lower()
    if not searchable:
        return False, "Pexels 图片缺少可验证描述", score

    item_tokens = _tokenize_opennews_relevance(searchable)
    core_tokens = _opennews_core_relevance_tokens(relevance_tokens)
    overlap = core_tokens & item_tokens
    phrase_hits = {token for token in core_tokens if " " in token and token in searchable}
    domain_hits = _opennews_domain_hits(searchable, visual_domain)
    has_scene = _opennews_concrete_scene_hit(searchable, visual_domain)
    event_terms = _opennews_focus_event_terms(relevance_tokens, visual_domain)
    event_hits = {term for term in event_terms if term.lower() in searchable}
    query_entities = _opennews_query_named_entities(seg, relevance_tokens)
    item_entities = _opennews_named_entities_from_text(searchable)
    entity_overlap = query_entities & item_entities
    strong_entities = query_entities & (
        OPENNEWS_AI_COMPANY_ENTITY_GROUPS | {"trump", "white_house", "fed_powell", "iran_israel"}
    )
    topic_conflict_reason = _opennews_topic_conflict_reason(searchable, relevance_tokens, visual_domain)

    if topic_conflict_reason and not entity_overlap:
        return False, f"Pexels 图片主题冲突：{topic_conflict_reason}", score - 120
    if (
        query_entities
        and item_entities
        and not entity_overlap
        and (item_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS)
        and (query_entities & OPENNEWS_AI_COMPANY_ENTITY_GROUPS)
    ):
        return False, (
            "Pexels 图片实体与新闻实体不一致："
            f"news={', '.join(sorted(query_entities))}; image={', '.join(sorted(item_entities))}"
        ), score - 100
    if exact_entity_required and query_entities:
        if not entity_overlap:
            return False, (
                "Pexels 强实体新闻必须命中主角实体："
                f"news={', '.join(sorted(query_entities))}; image={', '.join(sorted(item_entities)) or 'none'}"
            ), score - 140

    concrete_hit = bool(entity_overlap or phrase_hits or len(overlap) >= 2 or domain_hits or has_scene or event_hits)
    if strong_entities and not entity_overlap:
        if item_entities:
            return False, (
                "Pexels 图片出现其他明确主体，未命中新闻主角："
                f"news={', '.join(sorted(strong_entities))}; image={', '.join(sorted(item_entities))}"
            ), score - 100
        if not (domain_hits or has_scene or event_hits or len(overlap) >= 2):
            return False, "Pexels 图片未命中新闻主角，也缺少同领域/同场景支撑", score - 80

    if visual_domain == "finance" and not (domain_hits or has_scene or event_hits or entity_overlap):
        return False, "Pexels 金融新闻图片缺少金融、市场、货币或交易语义", score - 90
    if visual_domain == "cybersecurity" and not (domain_hits or has_scene or event_hits):
        return False, "Pexels 网络安全图片缺少安全、黑客、认证或网络语义", score - 90
    if visual_domain == "technology" and not (domain_hits or has_scene or event_hits or entity_overlap or phrase_hits):
        return False, "Pexels 科技新闻图片缺少 AI、芯片、软件、智能眼镜或同类技术语义", score - 80
    if exact_scene_required and not (has_scene or event_hits or phrase_hits or len(overlap) >= 2 or entity_overlap):
        return False, "Pexels 图片没有命中新闻要求的具体场景", score - 90
    if not concrete_hit:
        return False, "Pexels 图片只命中泛搜索词，未命中文案核心对象或具体场景", score - 70

    min_score = OPENNEWS_PEXELS_MIN_RELEVANCE_SCORE
    if strong_entities and not entity_overlap:
        min_score = max(22, min_score - 4)
    if score < min_score:
        return False, f"Pexels 图片相关性分数不足：{score}/{min_score}", score
    return True, "Pexels 相关性二审通过", score


def _append_opennews_free_material_items(
    *,
    seg: dict,
    material_items: list[dict],
    material_paths: list[str],
    output_dir: str,
    segment_index: int,
    max_total_materials: int,
    max_source_images: int,
    current_image_count: int,
    used_source_urls: set[str],
    used_source_hashes: set[str],
    batch_job_id: str = "",
) -> tuple[int, list[dict], list[dict]]:
    image_count = current_image_count
    rejection_log: list[dict] = []
    selected_debug: list[dict] = []
    relevance_tokens = _opennews_relevance_tokens(seg)
    visual_domain = _opennews_visual_domain(seg, relevance_tokens) if relevance_tokens else "general"
    ranked_candidates: list[tuple[int, dict]] = []
    seen_urls: set[str] = set()
    query_candidates = _opennews_pexels_query_candidates(seg, relevance_tokens, visual_domain)
    exact_entity_required = _opennews_pexels_requires_exact_entity(seg, relevance_tokens, visual_domain)
    if not query_candidates:
        fallback_query = str(seg.get("material_search_keyword") or seg.get("material_keyword") or "news").strip() or "news"
        query_candidates = [{"query": fallback_query, "tier": "fallback", "exact_entity": exact_entity_required, "exact_scene": True}]

    for query_row in query_candidates[:10]:
        query = str(query_row.get("query") or "").strip()
        if not query:
            continue
        try:
            photos = search_photos(query, count=6)
        except Exception as exc:
            rejection_log.append({"query": query, "reason": f"免费素材检索失败：{exc}"})
            continue
        for photo in photos:
            url = str(photo.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            candidate = dict(photo)
            candidate["related_query"] = query
            candidate["theme_title"] = query
            score = _opennews_pexels_candidate_score(
                candidate,
                relevance_tokens,
                query=query,
                visual_domain=visual_domain,
            )
            keep, reason, score = _opennews_pexels_candidate_decision(
                candidate,
                seg=seg,
                relevance_tokens=relevance_tokens,
                visual_domain=visual_domain,
                query=query,
                score=score,
                exact_entity_required=bool(query_row.get("exact_entity")) or exact_entity_required,
                exact_scene_required=bool(query_row.get("exact_scene")),
            )
            if not keep:
                rejection_log.append({
                    "url": url,
                    "query": query,
                    "alt": str(candidate.get("alt") or ""),
                    "score": int(score),
                    "reason": reason,
                    "tier": str(query_row.get("tier") or ""),
                })
                continue
            candidate["pexels_relevance_reason"] = reason
            candidate["pexels_query_tier"] = str(query_row.get("tier") or "")
            ranked_candidates.append((score, candidate))

    ranked_candidates.sort(key=lambda item: item[0], reverse=True)

    for score, item in ranked_candidates:
        if len(material_items) >= max_total_materials or image_count >= max_source_images:
            break
        url = str(item.get("url") or "").strip()
        url_keys = _source_identity_keys(url)
        if url_keys & used_source_urls:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": "URL 已在当前批次/任务中使用"})
            continue
        batch_ok, batch_reason = _opennews_batch_pexels_image_allowed(batch_job_id, item)
        if not batch_ok:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": batch_reason})
            continue
        filename = f"material_{segment_index:02d}_pexels_{len(material_items):02d}.jpg"
        output_path = os.path.join(output_dir, "materials", filename)
        try:
            download_file(url, output_path)
        except Exception as exc:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": f"下载失败：{exc}"})
            continue
        usable, usable_reason = _opennews_material_path_is_usable(output_path, "image")
        if not usable:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": usable_reason})
            try:
                os.remove(output_path)
            except Exception:
                pass
            continue
        try:
            content_hash = _file_sha256(output_path)
        except Exception:
            content_hash = ""
        if content_hash and content_hash in used_source_hashes:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": "内容 hash 已在当前批次/任务中使用"})
            try:
                os.remove(output_path)
            except Exception:
                pass
            continue
        batch_ok, batch_reason = _opennews_batch_pexels_image_allowed(batch_job_id, item, content_hash=content_hash)
        if not batch_ok:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": batch_reason})
            try:
                os.remove(output_path)
            except Exception:
                pass
            continue
        usage_ok, usage_reason = _opennews_pexels_image_usage_status(item, content_hash=content_hash)
        if not usage_ok:
            rejection_log.append({"url": url, "query": item.get("related_query") or "", "reason": usage_reason})
            try:
                os.remove(output_path)
            except Exception:
                pass
            continue
        if content_hash:
            used_source_hashes.add(content_hash)
        used_source_urls.update(url_keys)
        entry = _material_entry(output_path, kind="image", source="pexels")
        entry["title"] = str(item.get("alt") or item.get("related_query") or "")
        entry["related_query"] = str(item.get("related_query") or "")
        entry["photographer"] = str(item.get("photographer") or "")
        entry["pexels_score"] = int(score)
        entry["pexels_relevance_reason"] = str(item.get("pexels_relevance_reason") or "")
        material_items.append(entry)
        material_paths.append(output_path)
        image_count += 1
        _opennews_record_pexels_image_usage(item, copied_path=output_path, content_hash=content_hash)
        _opennews_record_batch_pexels_image_usage(batch_job_id, item, copied_path=output_path, content_hash=content_hash)
        selected_debug.append({
            "url": url,
            "query": str(item.get("related_query") or ""),
            "alt": str(item.get("alt") or ""),
            "score": int(score),
            "reason": str(item.get("pexels_relevance_reason") or ""),
            "tier": str(item.get("pexels_query_tier") or ""),
        })
        print(f"  ✅ 已命中免费素材库：{os.path.basename(output_path)}｜{item.get('related_query') or item.get('alt') or ''}")

    if image_count == current_image_count and OPENNEWS_NEWS_CARD_FALLBACK_ENABLED and image_count < max_source_images and len(material_items) < max_total_materials:
        try:
            card_path = _opennews_generate_verified_news_card(seg, output_dir, segment_index, len(material_items), visual_domain=visual_domain)
            entry = _material_entry(card_path, kind="image", source="opennews_news_card")
            entry["title"] = str(seg.get("material_keyword") or seg.get("title_zh") or seg.get("title") or "OpenNews 新闻")
            entry["pexels_relevance_reason"] = "免费图库严格匹配不足，已回退准确新闻图卡"
            material_items.append(entry)
            material_paths.append(card_path)
            image_count += 1
            selected_debug.append({
                "url": "",
                "query": "",
                "alt": entry["title"],
                "score": 999,
                "reason": "generated_verified_news_card",
                "tier": "news_card_fallback",
            })
            print(f"  ✅ 免费素材严格匹配未命中，已生成准确新闻图卡：{os.path.basename(card_path)}")
        except Exception as exc:
            rejection_log.append({"query": "", "reason": f"准确新闻图卡生成失败：{exc}"})

    return image_count, rejection_log, selected_debug


def _fetch_opennews_materials_legacy_strict_strategy(
    seg_with_materials: dict,
    *,
    seg: dict,
    output_dir: str,
    segment_index: int,
    target_market: str,
    department_id: str,
    used_source_urls: set[str],
    used_source_hashes: set[str],
    used_library_ids: set[str],
) -> dict:
    # Legacy strict-news-source strategy retained for future A/B testing.
    display_keyword = seg.get("material_keyword", "Japan")
    keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
    print(f"🔎 搜索素材：{display_keyword}｜检索词：{keyword}")

    material_items = []
    material_paths = []
    is_opennews_material_only = bool(seg.get("opennews_material_only") or seg.get("disable_free_material_fallback"))
    max_total_materials = OPENNEWS_MAX_MATERIALS if is_opennews_material_only else 3
    max_source_videos = OPENNEWS_MAX_SOURCE_VIDEOS if is_opennews_material_only else 1
    max_source_images = OPENNEWS_MAX_SOURCE_IMAGES if is_opennews_material_only else 2
    seen_source_urls: set[str] = set()
    used_source_page_counts: dict[str, int] = {}
    used_source_domain_counts: dict[str, int] = {}
    source_materials = []
    relevance_tokens = _opennews_relevance_tokens(seg) if is_opennews_material_only and seg.get("strict_news_media_only") else set()
    visual_domain = _opennews_visual_domain(seg, relevance_tokens) if relevance_tokens else "general"
    rejection_log: list[dict] = []
    realtime_source_review_enabled = bool(is_opennews_material_only and OPENNEWS_REALTIME_SOURCE_REVIEW_ENABLED)
    if realtime_source_review_enabled:
        max_source_videos = 0
        max_source_images = max(1, min(max_source_images, OPENNEWS_REALTIME_SOURCE_MAX_IMAGES))
        max_total_materials = max(1, min(max_total_materials, max_source_images))
    opennews_library_only = bool(is_opennews_material_only and OPENNEWS_MATERIAL_LIBRARY_ONLY and not realtime_source_review_enabled)
    source_fallback_available = (
        is_opennews_material_only
        and not opennews_library_only
        and OPENNEWS_AI_IMAGE_ONLY
        and OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL
    )
    if realtime_source_review_enabled:
        print(
            "  🧠 OpenNews 实时素材审核模式：先爬取网络候选图片，经 5090 Qwen3-VL 语义/安全审核后使用；"
            "不足再用本地向量素材库兜底"
        )
    elif opennews_library_only:
        print("  🛡️ OpenNews 素材安全模式：禁用外网爬图/新闻源图片/5090 AI生图，仅使用本地正式素材库")
    elif is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and not source_fallback_available:
        print("  ℹ️ OpenNews AI图片专用模式：跳过新闻网页/网络图片素材，只使用5090生成图")
    elif is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY:
        print("  ℹ️ OpenNews AI图片优先模式：先用5090生成图，若不足再启用严格新闻源图片兜底")
    elif is_opennews_material_only and not OPENNEWS_AI_IMAGE_ENABLED:
        print("  ℹ️ OpenNews 已暂停5090 AI生图：直接使用严格新闻源/公开网页爬取素材")
    else:
        source_fallback_available = False
    if not opennews_library_only and not (is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and not source_fallback_available):
        for item in (seg.get("source_materials") or []):
            if not isinstance(item, dict) or not item.get("url"):
                continue
            identity_keys = _source_identity_keys(str(item.get("url") or ""))
            if not identity_keys or identity_keys & seen_source_urls or identity_keys & used_source_urls:
                continue
            if _looks_like_unsafe_source_material(item) or (
                not realtime_source_review_enabled and _looks_like_bad_source_material(item)
            ):
                rejection_log.append({
                    "url": item.get("url") or "",
                    "title": item.get("title") or "",
                    "reason": "素材 URL 命中成人/裸露站点黑名单" if _looks_like_unsafe_source_material(item) else "素材 URL 命中低质量图标/广告黑名单",
                })
                print(
                    "  ⚠️ 新闻素材预过滤："
                    f"{'成人/裸露站点' if _looks_like_unsafe_source_material(item) else '低质量图标/广告'}｜{item.get('url')}"
                )
                continue
            if is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY:
                kind = str(item.get("kind") or "").strip().lower()
                source = str(item.get("source") or "").strip().lower()
                if kind == "video" or source not in OPENNEWS_STRICT_FALLBACK_SOURCES:
                    rejection_log.append({
                        "url": item.get("url") or "",
                        "title": item.get("title") or "",
                        "reason": "严格兜底只允许新闻原文/相关报道/OG主图图片",
                    })
                    continue
            relevance_score = _source_material_relevance_score(item, relevance_tokens) if relevance_tokens else 1
            min_relevance_score = _opennews_min_relevance_score(item)
            if realtime_source_review_enabled:
                min_relevance_score = max(8, min(min_relevance_score, 18))
            if relevance_tokens and relevance_score < min_relevance_score:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "title": item.get("title") or "",
                    "reason": f"基础相关性不足：{relevance_score}/{min_relevance_score}",
                })
                print(
                    "  ⚠️ 新闻素材相关性不足，已跳过："
                    f"{item.get('title') or item.get('url')}｜score={relevance_score}/{min_relevance_score}"
                )
                continue
            if relevance_tokens:
                keep_item, quality_reason, quality_score = _opennews_quality_decision(item, relevance_tokens, visual_domain)
                if not keep_item and not realtime_source_review_enabled:
                    rejection_log.append({
                        "url": item.get("url") or "",
                        "title": item.get("title") or "",
                        "reason": quality_reason,
                        "score": quality_score,
                        "domain": visual_domain,
                    })
                    print(f"  ⚠️ 新闻素材质量过滤：{quality_reason}｜{item.get('title') or item.get('url')}")
                    continue
            else:
                quality_reason = "非严格模式"
                quality_score = relevance_score
            item = dict(item)
            item["_relevance_score"] = relevance_score
            item["_quality_score"] = quality_score
            item["_quality_reason"] = quality_reason
            seen_source_urls.update(identity_keys)
            source_materials.append(item)
        if realtime_source_review_enabled:
            for item in _opennews_expand_source_material_candidates(seg, relevance_tokens, visual_domain):
                identity_keys = _source_identity_keys(str(item.get("url") or ""))
                if not identity_keys or identity_keys & seen_source_urls or identity_keys & used_source_urls:
                    continue
                if _looks_like_unsafe_source_material(item) or _looks_like_bad_source_material(item):
                    rejection_log.append({
                        "url": item.get("url") or "",
                        "title": item.get("title") or "",
                        "reason": "扩展候选命中成人/低质量素材黑名单",
                    })
                    continue
                seen_source_urls.update(identity_keys)
                source_materials.append(item)
        if is_opennews_material_only:
            source_materials = _theme_balanced_source_materials(source_materials, relevance_tokens)
    else:
        source_materials.sort(key=_rank_source_material, reverse=True)
    source_video_count = 0
    source_image_count = 0
    review_required_for_source = realtime_source_review_enabled
    used_visual_anchors: set[str] = set()
    if is_opennews_material_only and not opennews_library_only:
        ai_materials = _generate_opennews_ai_image_materials(
            seg,
            output_dir,
            segment_index,
            len(material_items),
        )
        for entry in ai_materials:
            if len(material_items) >= max_total_materials:
                break
            material_items.append(entry)
            material_paths.append(entry["path"])
            if entry.get("kind") == "video":
                source_video_count += 1
            else:
                source_image_count += 1
        ai_image_count = sum(1 for item in material_items if item.get("source") == "opennews_ai_image")
        allow_strict_source_fallback_now = (
            OPENNEWS_AI_IMAGE_ONLY
            and OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL
            and ai_image_count == 0
        )
        if allow_strict_source_fallback_now:
            review_required_for_source = bool(OPENNEWS_REALTIME_SOURCE_REVIEW_ENABLED)
            if review_required_for_source:
                max_source_videos = 0
                max_source_images = max(1, min(max_source_images, OPENNEWS_REALTIME_SOURCE_MAX_IMAGES))
                max_total_materials = max(1, min(max_total_materials, max_source_images))
        if ai_materials and OPENNEWS_AI_IMAGE_REPLACE_SOURCE and not allow_strict_source_fallback_now:
            source_materials = []
        if OPENNEWS_AI_IMAGE_ONLY and not allow_strict_source_fallback_now:
            source_materials = []
        elif allow_strict_source_fallback_now:
            print(
                "  ⚠️ 5090 AI图片不足，启用严格新闻源图片兜底："
                f"AI={ai_image_count}"
            )

    library_fallback_enabled = PRODUCTION_MATERIAL_LIBRARY_ENABLED or (
        is_opennews_material_only and OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED
    )
    if (
        is_opennews_material_only
        and OPENNEWS_MATERIAL_LIBRARY_FIRST
        and library_fallback_enabled
        and not material_items
        and not realtime_source_review_enabled
    ):
        library_target_images = max(0, min(max_total_materials, max_source_images))
        library_items = _search_opennews_material_vector_fallback(
            seg,
            visual_domain=visual_domain,
            target_market=target_market or str(seg.get("target_market") or ""),
            department_id=department_id or str(seg.get("department_id") or ""),
            limit_images=library_target_images,
        )
        if library_items:
            print(f"  ✅ OpenNews 优先使用 5090 向量素材库：{len(library_items)} 条候选")
        elif OPENNEWS_MATERIAL_VECTOR_REQUIRED:
            print("  ⚠️ OpenNews 未通过5090视觉向量素材库命中，启用正式素材库安全兜底")
            library_items = _search_opennews_material_library_safe_any(
                visual_domain=visual_domain,
                seg=seg,
                relevance_tokens=relevance_tokens,
                target_market=target_market or str(seg.get("target_market") or ""),
                department_id=department_id or str(seg.get("department_id") or ""),
                limit_images=max(1, min(library_target_images, OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES, 2)),
            )
        else:
            library_items = _search_opennews_material_library_fallback(
                seg,
                visual_domain=visual_domain,
                target_market=target_market or str(seg.get("target_market") or ""),
                department_id=department_id or str(seg.get("department_id") or ""),
                limit_videos=0,
                limit_images=library_target_images,
            )
        if library_items:
            if not any(item.get("opennews_vector_match") for item in library_items):
                print(f"  ✅ OpenNews 优先使用正式素材库：{len(library_items)} 条候选")
            source_video_count, source_image_count = _append_library_material_items(
                library_items=library_items,
                material_items=material_items,
                material_paths=material_paths,
                output_dir=output_dir,
                segment_index=segment_index,
                max_total_materials=max_total_materials,
                max_source_videos=max_source_videos,
                max_source_images=max_source_images,
                current_video_count=source_video_count,
                current_image_count=source_image_count,
                used_library_ids=used_library_ids,
                is_opennews_material_only=is_opennews_material_only,
            )
        else:
            if opennews_library_only and OPENNEWS_MATERIAL_VECTOR_REQUIRED:
                print("  ⚠️ OpenNews 强制优先视觉向量素材库，未命中时启用正式素材库安全兜底")
                library_items = _search_opennews_material_library_safe_any(
                    visual_domain=visual_domain,
                    seg=seg,
                    relevance_tokens=relevance_tokens,
                    target_market=target_market or str(seg.get("target_market") or ""),
                    department_id=department_id or str(seg.get("department_id") or ""),
                    limit_images=max(1, min(library_target_images, OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES, 2)),
                )
                if library_items:
                    print(f"  ✅ OpenNews 使用本地素材库安全兜底：{len(library_items)} 条")
            elif opennews_library_only:
                print("  ℹ️ OpenNews 正式素材库未精准命中，改用本地素材库安全兜底")
                library_items = _search_opennews_material_library_safe_any(
                    visual_domain=visual_domain,
                    seg=seg,
                    relevance_tokens=relevance_tokens,
                    target_market=target_market or str(seg.get("target_market") or ""),
                    department_id=department_id or str(seg.get("department_id") or ""),
                    limit_images=library_target_images,
                )
                if library_items:
                    print(f"  ✅ OpenNews 使用本地素材库安全兜底：{len(library_items)} 条")
                    source_video_count, source_image_count = _append_library_material_items(
                        library_items=library_items,
                        material_items=material_items,
                        material_paths=material_paths,
                        output_dir=output_dir,
                        segment_index=segment_index,
                        max_total_materials=max_total_materials,
                        max_source_videos=max_source_videos,
                        max_source_images=max_source_images,
                        current_video_count=source_video_count,
                        current_image_count=source_image_count,
                        used_library_ids=used_library_ids,
                        is_opennews_material_only=is_opennews_material_only,
                    )
            else:
                print("  ℹ️ OpenNews 正式素材库未命中，准备启用严格网络素材兜底")

    source_attempt_limit = (
        OPENNEWS_REALTIME_SOURCE_CANDIDATE_LIMIT
        if review_required_for_source
        else (260 if is_opennews_material_only else 24)
    )
    if opennews_library_only:
        source_materials = []
    for item in source_materials[:source_attempt_limit]:
        if len(material_items) >= max_total_materials:
            break
        if source_video_count >= max_source_videos and source_image_count >= max_source_images:
            break
        kind = str(item.get("kind") or "").strip().lower()
        if review_required_for_source and kind == "video":
            rejection_log.append({
                "url": item.get("url") or "",
                "title": item.get("title") or "",
                "reason": "Qwen3-VL 网络素材审核模式暂只接受图片素材，不使用网络视频",
            })
            continue
        if kind == "video" and source_video_count >= max_source_videos:
            continue
        if is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and kind == "video":
            continue
        if kind != "video" and source_image_count >= max_source_images:
            continue
        if kind != "video":
            visual_anchor = str(item.get("visual_anchor") or item.get("theme_title") or "").strip().lower()
            if visual_anchor and visual_anchor in used_visual_anchors:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": f"同一视觉锚点已使用：{visual_anchor}",
                })
                print(f"  ⏳ 网络素材同视觉锚点去重：{visual_anchor}｜{item.get('title') or item.get('url')}")
                continue
            page_key = _opennews_source_page_key(item)
            domain_key = _opennews_source_domain_key(item)
            if page_key and used_source_page_counts.get(page_key, 0) >= OPENNEWS_SOURCE_IMAGES_PER_PAGE_LIMIT:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": f"同一新闻源页面已使用 {used_source_page_counts.get(page_key, 0)} 张，达到上限 {OPENNEWS_SOURCE_IMAGES_PER_PAGE_LIMIT}",
                })
                print(f"  ⏳ 网络素材同页频控过滤：{item.get('title') or item.get('url')}")
                continue
            if domain_key and used_source_domain_counts.get(domain_key, 0) >= OPENNEWS_SOURCE_IMAGES_PER_DOMAIN_LIMIT:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": f"同一新闻源域名 {domain_key} 已使用 {used_source_domain_counts.get(domain_key, 0)} 张，达到上限 {OPENNEWS_SOURCE_IMAGES_PER_DOMAIN_LIMIT}",
                })
                print(f"  ⏳ 网络素材同域名频控过滤：{domain_key}｜{item.get('title') or item.get('url')}")
                continue
        try:
            copied_path = _download_source_material(str(item.get("url") or ""), output_dir, segment_index, len(material_items), kind=kind)
        except Exception as exc:
            print(f"  ⚠️ 新闻来源素材下载失败：{item.get('url')}｜{exc}")
            continue
        if review_required_for_source and _asset_kind_for_suffix(copied_path) != "video":
            try:
                keep_source, review_reason, review_analysis, review_score = _opennews_review_source_image_with_qwen(
                    copied_path,
                    item,
                    seg=seg,
                    relevance_tokens=relevance_tokens,
                    visual_domain=visual_domain,
                )
            except Exception as exc:
                keep_source = False
                review_reason = f"Qwen3-VL 审核不可用：{exc}"
                review_analysis = {}
                review_score = 0
            if not keep_source:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "title": item.get("title") or "",
                    "reason": review_reason,
                    "score": review_score,
                    "domain": visual_domain,
                })
                print(f"  ⚠️ Qwen3-VL 实时素材审核未通过：{review_reason}｜{item.get('title') or item.get('url')}")
                try:
                    os.remove(copied_path)
                except Exception:
                    pass
                continue
            item = dict(item)
            item["_qwen_review_analysis"] = review_analysis
            item["_qwen_review_reason"] = review_reason
            item["_qwen_review_score"] = review_score
            item["_quality_score"] = max(int(item.get("_quality_score") or 0), int(review_score or 0))
            item["_quality_reason"] = f"Qwen3-VL审核通过：{review_reason}"
            imported_id = _opennews_import_reviewed_source_image(
                copied_path,
                item,
                seg=seg,
                analysis=review_analysis,
                visual_domain=visual_domain,
                review_reason=review_reason,
            )
            if imported_id:
                item["_imported_material_id"] = imported_id
        try:
            content_hash = _file_sha256(copied_path)
        except Exception:
            content_hash = ""
        if content_hash and content_hash in used_source_hashes:
            try:
                os.remove(copied_path)
            except Exception:
                pass
            print(f"  ⚠️ 新闻来源素材内容重复，已跳过：{item.get('url')}")
            continue
        if content_hash and _asset_kind_for_suffix(copied_path) != "video":
            hash_ok, hash_reason = _opennews_source_image_hash_usage_status(content_hash)
            if not hash_ok:
                rejection_log.append({
                    "url": item.get("url") or "",
                    "source_url": item.get("source_url") or "",
                    "title": item.get("title") or "",
                    "reason": hash_reason,
                    "content_hash": content_hash,
                })
                try:
                    os.remove(copied_path)
                except Exception:
                    pass
                print(f"  ⏳ 网络图片全局复用频控过滤：{hash_reason}｜{item.get('title') or item.get('url')}")
                continue
        if content_hash:
            used_source_hashes.add(content_hash)
        used_source_urls.update(_source_identity_keys(str(item.get("url") or "")))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=kind or _asset_kind_for_suffix(copied_path), source="opennews_source")
        entry["source_url"] = item.get("source_url") or item.get("url")
        entry["title"] = item.get("title", "")
        entry["quality_score"] = item.get("_quality_score", 0)
        entry["quality_reason"] = item.get("_quality_reason", "")
        if item.get("_qwen_review_analysis"):
            entry["qwen_review"] = item.get("_qwen_review_analysis")
            entry["qwen_review_reason"] = item.get("_qwen_review_reason", "")
            entry["qwen_review_score"] = item.get("_qwen_review_score", 0)
        if item.get("_imported_material_id"):
            entry["imported_material_id"] = item.get("_imported_material_id")
        if is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY:
            entry["strict_fallback"] = True
            entry["fallback_reason"] = "5090 AI图片完全不可用，使用严格新闻源图片兜底"
        if item.get("theme_index") is not None:
            entry["theme_index"] = item.get("theme_index")
        if item.get("theme_title"):
            entry["theme_title"] = item.get("theme_title")
        if item.get("related_query"):
            entry["related_query"] = item.get("related_query")
        material_items.append(entry)
        if entry["kind"] == "video":
            source_video_count += 1
        else:
            source_image_count += 1
            visual_anchor = str(item.get("visual_anchor") or item.get("theme_title") or "").strip().lower()
            if visual_anchor:
                used_visual_anchors.add(visual_anchor)
            page_key = _opennews_source_page_key(item)
            domain_key = _opennews_source_domain_key(item)
            if page_key:
                used_source_page_counts[page_key] = used_source_page_counts.get(page_key, 0) + 1
            if domain_key:
                used_source_domain_counts[domain_key] = used_source_domain_counts.get(domain_key, 0) + 1
            if content_hash:
                _opennews_record_source_image_usage(item, content_hash=content_hash, copied_path=copied_path)
        print(f"  ✅ 已下载新闻来源素材：{os.path.basename(copied_path)}")

    blank_rejections: list[dict] = []
    if is_opennews_material_only:
        material_items, material_paths, blank_rejections = _opennews_filter_usable_materials(material_items, material_paths)
        source_video_count = sum(1 for item in material_items if item.get("kind") == "video")
        source_image_count = sum(1 for item in material_items if item.get("kind") != "video")

    remaining_slots = max(0, max_total_materials - len(material_items))
    library_items = []
    opennews_needs_library_fallback = (
        is_opennews_material_only
        and (not OPENNEWS_MATERIAL_LIBRARY_FIRST or realtime_source_review_enabled)
        and source_image_count < OPENNEWS_LIBRARY_FALLBACK_MIN_SOURCE_IMAGES
        and not any(item.get("source") == "opennews_ai_image" for item in material_items)
    )
    if remaining_slots and library_fallback_enabled:
        if is_opennews_material_only:
            if opennews_library_only:
                library_items = _search_opennews_material_vector_fallback(
                    visual_domain=visual_domain,
                    seg=seg,
                    target_market=target_market or str(seg.get("target_market") or ""),
                    department_id=department_id or str(seg.get("department_id") or ""),
                    limit_images=max(0, min(remaining_slots, max_source_images - source_image_count)),
                )
                if library_items:
                    print(f"  ✅ OpenNews 通过5090向量素材库补足素材：{len(library_items)} 条")
                elif OPENNEWS_MATERIAL_VECTOR_REQUIRED:
                    print("  ⚠️ OpenNews 强制优先视觉向量素材库，未命中时启用正式素材库安全补足")
                    library_items = _search_opennews_material_library_safe_any(
                        visual_domain=visual_domain,
                        seg=seg,
                        relevance_tokens=relevance_tokens,
                        target_market=target_market or str(seg.get("target_market") or ""),
                        department_id=department_id or str(seg.get("department_id") or ""),
                        limit_images=max(
                            1,
                            min(
                                remaining_slots,
                                OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES,
                                max_source_images - source_image_count,
                                2,
                            ),
                        ),
                    )
                else:
                    library_items = _search_opennews_material_library_safe_any(
                        visual_domain=visual_domain,
                        seg=seg,
                        relevance_tokens=relevance_tokens,
                        target_market=target_market or str(seg.get("target_market") or ""),
                        department_id=department_id or str(seg.get("department_id") or ""),
                        limit_images=max(0, min(remaining_slots, max_source_images - source_image_count)),
                    )
                    if library_items:
                        print(f"  ✅ OpenNews 本地素材库补足素材：{len(library_items)} 条")
            elif OPENNEWS_MATERIAL_LIBRARY_FIRST and not realtime_source_review_enabled:
                print(
                    "  ℹ️ OpenNews 已优先检查正式素材库，剩余素材槽位只允许严格网络兜底："
                    f"library_or_source_images={source_image_count}"
                )
                library_items = []
            elif not opennews_needs_library_fallback:
                print(
                    "  ℹ️ OpenNews 已取得网络/AI素材，跳过正式素材库兜底："
                    f"source_images={source_image_count}"
                )
                library_items = []
            else:
                library_limits = {
                    "limit_videos": 0,
                    "limit_images": max(
                        0,
                        min(
                            remaining_slots,
                            OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES,
                            max_source_images - source_image_count,
                        ),
                    ),
                }
                library_items = _search_opennews_material_vector_fallback(
                    seg,
                    visual_domain=visual_domain,
                    target_market=target_market or str(seg.get("target_market") or ""),
                    department_id=department_id or str(seg.get("department_id") or ""),
                    limit_images=library_limits["limit_images"],
                )
                if library_items:
                    print(
                        "  ✅ OpenNews 网络素材为0，启用5090向量素材库兜底："
                        f"{len(library_items)} 条"
                    )
                elif OPENNEWS_MATERIAL_VECTOR_REQUIRED:
                    print("  ⚠️ OpenNews 未通过5090视觉向量素材库命中，启用正式素材库安全兜底")
                    library_items = _search_opennews_material_library_safe_any(
                        visual_domain=visual_domain,
                        seg=seg,
                        relevance_tokens=relevance_tokens,
                        target_market=target_market or str(seg.get("target_market") or ""),
                        department_id=department_id or str(seg.get("department_id") or ""),
                        limit_images=max(
                            1,
                            min(
                                remaining_slots,
                                OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES,
                                max_source_images - source_image_count,
                                2,
                            ),
                        ),
                    )
                else:
                    library_items = _search_opennews_material_library_fallback(
                        seg,
                        visual_domain=visual_domain,
                        target_market=target_market or str(seg.get("target_market") or ""),
                        department_id=department_id or str(seg.get("department_id") or ""),
                        **library_limits,
                    )
                if library_items:
                    if not any(item.get("opennews_vector_match") for item in library_items):
                        print(
                            "  ✅ OpenNews 网络素材为0，启用正式素材库兜底："
                            f"{len(library_items)} 条"
                        )
        else:
            library_limits = {
                "limit_videos": max(0, min(remaining_slots, max_source_videos - source_video_count)),
                "limit_images": max(0, min(remaining_slots, max_source_images - source_image_count)),
            }
            library_items = search_material_library(
                seg,
                target_market=target_market or str(seg.get("target_market") or ""),
                department_id=department_id or str(seg.get("department_id") or ""),
                **library_limits,
            )
    elif remaining_slots and not library_fallback_enabled:
        print("  ℹ️ 生产素材库匹配已关闭：跳过自建素材库，继续使用原有素材来源")
    library_video_count = source_video_count
    library_image_count = source_image_count
    library_video_count, library_image_count = _append_library_material_items(
        library_items=library_items,
        material_items=material_items,
        material_paths=material_paths,
        output_dir=output_dir,
        segment_index=segment_index,
        max_total_materials=max_total_materials,
        max_source_videos=max_source_videos,
        max_source_images=max_source_images,
        current_video_count=library_video_count,
        current_image_count=library_image_count,
        used_library_ids=used_library_ids,
        is_opennews_material_only=is_opennews_material_only,
    )

    disable_free_fallback = True if is_opennews_material_only else bool(seg.get("disable_free_material_fallback"))
    allow_opennews_quality_fallback = False
    if is_opennews_material_only:
        if opennews_library_only:
            print("  🛡️ OpenNews 已禁用所有网络图片/免费图库/新闻源素材兜底，仅允许本地正式素材库")
        elif OPENNEWS_AI_IMAGE_ONLY:
            if OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL:
                if OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED:
                    print("  ℹ️ OpenNews 已禁用免费素材库兜底；5090/新闻源不足时允许正式素材库兜底")
                else:
                    print("  ℹ️ OpenNews 已禁用本地素材库/免费素材库兜底；5090不足时仅允许严格新闻源图片兜底")
            else:
                print("  ℹ️ OpenNews 已禁用新闻源/本地素材库/免费素材库兜底，仅使用5090 AI生成图片")
        else:
            if PRODUCTION_MATERIAL_LIBRARY_ENABLED:
                print("  ℹ️ OpenNews 已禁用免费素材库兜底，仅使用新闻源、公开网页爬取和本地素材库")
            else:
                print("  ℹ️ OpenNews 已禁用本地素材库/免费素材库兜底，仅使用新闻源和公开网页爬取")

    fallback_queries = _opennews_theme_queries(seg) if is_opennews_material_only else []
    if keyword and keyword not in fallback_queries:
        fallback_queries.append(keyword)

    if library_video_count < max_source_videos and (not disable_free_fallback or allow_opennews_quality_fallback):
        try:
            video_download_index = 0
            for query in fallback_queries or [keyword]:
                if library_video_count >= max_source_videos or len(material_items) >= max_total_materials:
                    break
                videos = search_videos(query, count=max(1, max_source_videos - library_video_count))
                for video in videos:
                    if library_video_count >= max_source_videos or len(material_items) >= max_total_materials:
                        break
                    video_key = _source_url_key(str(video.get("url") or ""))
                    if video_key and video_key in used_source_urls:
                        continue
                    filename = f"material_{segment_index:02d}_video_{video_download_index}.mp4"
                    video_download_index += 1
                    output_path = os.path.join(output_dir, "materials", filename)
                    download_file(video["url"], output_path)
                    if video_key:
                        used_source_urls.add(video_key)
                    material_paths.append(output_path)
                    entry = _material_entry(output_path, kind="video", source="pexels")
                    entry["title"] = query
                    material_items.append(entry)
                    library_video_count += 1
                    print(f"  ✅ 视频已下载：{filename}｜{query}")
        except Exception as e:
            print(f"  ⚠️ 视频素材搜索失败：{e}")

    if library_image_count < max_source_images and (not disable_free_fallback or allow_opennews_quality_fallback):
        try:
            photo_download_index = 0
            for query in fallback_queries or [keyword]:
                if library_image_count >= max_source_images or len(material_items) >= max_total_materials:
                    break
                photos = search_photos(query, count=max(1, max_source_images - library_image_count))
                for photo in photos:
                    if library_image_count >= max_source_images or len(material_items) >= max_total_materials:
                        break
                    photo_key = _source_url_key(str(photo.get("url") or ""))
                    if photo_key and photo_key in used_source_urls:
                        continue
                    filename = f"material_{segment_index:02d}_photo_{photo_download_index}.jpg"
                    photo_download_index += 1
                    output_path = os.path.join(output_dir, "materials", filename)
                    download_file(photo["url"], output_path)
                    if photo_key:
                        used_source_urls.add(photo_key)
                    material_paths.append(output_path)
                    entry = _material_entry(output_path, kind="image", source="pexels")
                    entry["title"] = query
                    material_items.append(entry)
                    library_image_count += 1
                    print(f"  ✅ 图片已下载：{filename}｜{query}")
        except Exception as e:
            print(f"  ⚠️ 图片素材搜索失败：{e}")

    if is_opennews_material_only:
        material_items, material_paths, final_blank_rejections = _opennews_filter_usable_materials(material_items, material_paths)
        blank_rejections.extend(final_blank_rejections)
        if not material_items:
            print("  ❌ OpenNews 段落没有任何可用素材：已阻止白板占位，等待上层任务失败处理")
    seg_with_materials["material_paths"] = material_paths
    seg_with_materials["material_items"] = material_items
    if is_opennews_material_only:
        seg_with_materials["material_quality"] = {
            "domain": visual_domain,
            "ai_image_only": OPENNEWS_AI_IMAGE_ONLY,
            "ai_image_target_min": OPENNEWS_IMAGE_MIN_IMAGES,
            "ai_image_target_max": OPENNEWS_IMAGE_MAX_IMAGES,
            "ai_image_count": sum(1 for item in material_items if item.get("source") == "opennews_ai_image"),
            "local_library_only": opennews_library_only,
            "strict_source_fallback_enabled": OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL,
            "strict_source_fallback_used": any(item.get("source") == "opennews_source" for item in material_items),
            "source_qwen_review_required": review_required_for_source,
            "source_qwen_reviewed_count": sum(
                1 for item in material_items
                if item.get("source") == "opennews_source" and item.get("qwen_review_score") is not None
            ),
            "source_unreviewed_count": sum(
                1 for item in material_items
                if item.get("source") == "opennews_source" and item.get("qwen_review_score") is None
            ),
            "requires_human_review": False,
            "review_reason": "",
            "auto_publish_allowed": not any(
                item.get("source") == "opennews_source" and item.get("qwen_review_score") is None
                for item in material_items
            ),
            "source_counts": {
                source: sum(1 for item in material_items if item.get("source") == source)
                for source in sorted({str(item.get("source") or "unknown") for item in material_items})
            },
            "library_fallback_enabled": OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED,
            "library_fallback_used": any(item.get("source") == "library" for item in material_items),
            "blank_or_invalid_rejected_count": len(blank_rejections),
            "blank_or_invalid_rejections": blank_rejections[:40],
            "relevance_tokens": sorted(relevance_tokens)[:80],
            "accepted_count": len(material_items),
            "rejected_count": len(rejection_log),
            "rejections": rejection_log[:80],
        }
    if is_opennews_material_only and len(material_items) > OPENNEWS_MAX_MATERIALS:
        seg_with_materials["material_paths"] = material_paths[:OPENNEWS_MAX_MATERIALS]
        seg_with_materials["material_items"] = material_items[:OPENNEWS_MAX_MATERIALS]
    return seg_with_materials


def _fetch_opennews_materials_free_library_strategy(
    seg_with_materials: dict,
    *,
    seg: dict,
    output_dir: str,
    segment_index: int,
    target_market: str,
    department_id: str,
    used_source_urls: set[str],
    used_source_hashes: set[str],
    used_library_ids: set[str],
    batch_job_id: str,
) -> dict:
    display_keyword = seg.get("material_keyword", "Japan")
    keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
    print(f"🔎 OpenNews 免费素材库匹配：{display_keyword}｜检索词：{keyword}")
    material_items: list[dict] = []
    material_paths: list[str] = []
    relevance_tokens = _opennews_relevance_tokens(seg)
    visual_domain = _opennews_visual_domain(seg, relevance_tokens) if relevance_tokens else "general"
    max_total_materials = OPENNEWS_MAX_MATERIALS
    max_source_images = OPENNEWS_MAX_SOURCE_IMAGES
    image_count, pexels_rejections, selected_debug = _append_opennews_free_material_items(
        seg=seg,
        material_items=material_items,
        material_paths=material_paths,
        output_dir=output_dir,
        segment_index=segment_index,
        max_total_materials=max_total_materials,
        max_source_images=max_source_images,
        current_image_count=0,
        used_source_urls=used_source_urls,
        used_source_hashes=used_source_hashes,
        batch_job_id=batch_job_id,
    )

    if image_count < OPENNEWS_LIBRARY_FALLBACK_MIN_SOURCE_IMAGES:
        fallback_limit = max(
            1,
            min(
                OPENNEWS_LIBRARY_FALLBACK_MAX_IMAGES,
                max_total_materials - len(material_items),
                max_source_images - image_count,
            ),
        )
        if fallback_limit > 0:
            library_items = _search_opennews_material_vector_fallback(
                seg,
                visual_domain=visual_domain,
                target_market=target_market or str(seg.get("target_market") or ""),
                department_id=department_id or str(seg.get("department_id") or ""),
                limit_images=fallback_limit,
            )
            if library_items:
                print(f"  ✅ 免费素材不足，启用本地向量素材库兜底：{len(library_items)} 条")
                _, image_count = _append_library_material_items(
                    library_items=library_items,
                    material_items=material_items,
                    material_paths=material_paths,
                    output_dir=output_dir,
                    segment_index=segment_index,
                    max_total_materials=max_total_materials,
                    max_source_videos=0,
                    max_source_images=max_source_images,
                    current_video_count=0,
                    current_image_count=image_count,
                    used_library_ids=used_library_ids,
                    is_opennews_material_only=True,
                )

    material_items, material_paths, blank_rejections = _opennews_filter_usable_materials(material_items, material_paths)
    seg_with_materials["material_paths"] = material_paths
    seg_with_materials["material_items"] = material_items
    seg_with_materials["material_quality"] = {
        "domain": visual_domain,
        "strategy": "free_library_script_match",
        "auto_publish_allowed": True,
        "requires_human_review": False,
        "review_reason": "",
        "source_counts": {
            source: sum(1 for item in material_items if item.get("source") == source)
            for source in sorted({str(item.get("source") or "unknown") for item in material_items})
        },
        "library_fallback_enabled": OPENNEWS_MATERIAL_LIBRARY_FALLBACK_ENABLED,
        "library_fallback_used": any(item.get("source") == "library" for item in material_items),
        "blank_or_invalid_rejected_count": len(blank_rejections),
        "blank_or_invalid_rejections": blank_rejections[:40],
        "relevance_tokens": sorted(relevance_tokens)[:80],
        "accepted_count": len(material_items),
        "rejected_count": len(pexels_rejections),
        "rejections": pexels_rejections[:80],
        "selected_free_candidates": selected_debug[:40],
    }
    return seg_with_materials


def fetch_materials_for_segment(
    seg: dict,
    output_dir: str,
    segment_index: int,
    *,
    target_market: str = "",
    department_id: str = "",
    used_source_urls: set[str] | None = None,
    used_source_hashes: set[str] | None = None,
    used_library_ids: set[str] | None = None,
) -> dict:
    seg_with_materials = seg.copy()
    is_opennews_material_only = bool(seg.get("opennews_material_only") or seg.get("disable_free_material_fallback"))
    used_source_urls = used_source_urls if used_source_urls is not None else set()
    used_source_hashes = used_source_hashes if used_source_hashes is not None else set()
    used_library_ids = used_library_ids if used_library_ids is not None else set()
    strategy = str(seg.get("material_strategy") or "").strip().lower()
    batch_job_id = str(seg.get("batch_job_id") or "").strip()
    if is_opennews_material_only and strategy == "free_library_script_match":
        return _fetch_opennews_materials_free_library_strategy(
            seg_with_materials,
            seg=seg,
            output_dir=output_dir,
            segment_index=segment_index,
            target_market=target_market,
            department_id=department_id,
            used_source_urls=used_source_urls,
            used_source_hashes=used_source_hashes,
            used_library_ids=used_library_ids,
            batch_job_id=batch_job_id,
        )
    return _fetch_opennews_materials_legacy_strict_strategy(
        seg_with_materials,
        seg=seg,
        output_dir=output_dir,
        segment_index=segment_index,
        target_market=target_market,
        department_id=department_id,
        used_source_urls=used_source_urls,
        used_source_hashes=used_source_hashes,
        used_library_ids=used_library_ids,
    )


def fetch_all_materials(segments: list, output_dir: str) -> list:
    """
    批量搜索并下载所有素材段落的图片/视频
    """
    results = []
    used_source_urls: set[str] = set()
    used_source_hashes: set[str] = set()
    used_library_ids: set[str] = set()

    for i, seg in enumerate(segments):
        if seg.get("type") != "material":
            results.append(seg)
            continue
        results.append(
            fetch_materials_for_segment(
                seg,
                output_dir,
                i,
                target_market=str(seg.get("target_market") or ""),
                department_id=str(seg.get("department_id") or ""),
                used_source_urls=used_source_urls,
                used_source_hashes=used_source_hashes,
                used_library_ids=used_library_ids,
            )
        )

    return results
