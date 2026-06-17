"""
素材搜索模块
根据关键词自动搜索并下载 Pexels 图片/视频素材
"""

import os
import re
import hashlib
import time
from urllib.parse import urljoin, urlparse
import requests
from dotenv import load_dotenv
from material_library import copy_material_to_output, search_material_library

load_dotenv(override=False)

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
    "technology": {
        "ai", "artificial intelligence", "technology", "tech", "software", "app",
        "chip", "semiconductor", "nvidia", "openai", "anthropic", "meta",
        "facebook", "spacex", "tesla", "apple", "microsoft", "google", "alphabet",
        "amazon", "siri", "wwdc", "iphone", "data center", "robot", "startup",
    },
    "finance": {
        "stock", "stocks", "market", "nasdaq", "nyse", "wall street", "shares",
        "ipo", "earnings", "investor", "investors", "inflation", "fed",
        "interest rate", "bank", "finance", "economy", "trading", "tariff",
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
    "technology": {"white house", "parliament", "congress", "press briefing", "government meeting", "cabinet meeting", "foreign ministry", "diplomacy"},
    "finance": {"missile", "drone", "fighter jet", "warship", "military exercise", "troops"},
    "military": {"stock market", "ipo", "wall street", "earnings", "investors"},
}

OPENNEWS_GENERIC_MEDIA_TOKENS = {
    "news", "photo", "image", "video", "official", "media", "press", "latest",
    "footage", "b-roll", "article", "source", "public domain", "archive",
}

OPENNEWS_STRICT_FALLBACK_SOURCES = {"article", "related_article", "opengraph", "news_source"}


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
    }
    for phrase in phrases:
        if phrase in text:
            tokens.add(phrase)
    generic = {
        "news", "latest", "image", "photo", "video", "official", "press", "media",
        "article", "source", "related", "public", "content", "government", "meeting",
        "briefing", "company", "market", "tools", "tool",
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
    if explicit in OPENNEWS_VISUAL_DOMAIN_TOKENS:
        return explicit
    text = " ".join([
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("material_desc") or ""),
        str(seg.get("script") or "")[:1200],
        " ".join(sorted(relevance_tokens)),
    ]).lower()
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
    display_keyword = seg.get("material_keyword", "Japan")
    keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
    print(f"🔎 搜索素材：{display_keyword}｜检索词：{keyword}")

    material_items = []
    material_paths = []
    is_opennews_material_only = bool(seg.get("opennews_material_only") or seg.get("disable_free_material_fallback"))
    max_total_materials = OPENNEWS_MAX_MATERIALS if is_opennews_material_only else 3
    max_source_videos = OPENNEWS_MAX_SOURCE_VIDEOS if is_opennews_material_only else 1
    max_source_images = OPENNEWS_MAX_SOURCE_IMAGES if is_opennews_material_only else 2
    used_source_urls = used_source_urls if used_source_urls is not None else set()
    used_source_hashes = used_source_hashes if used_source_hashes is not None else set()
    used_library_ids = used_library_ids if used_library_ids is not None else set()
    seen_source_urls: set[str] = set()
    source_materials = []
    relevance_tokens = _opennews_relevance_tokens(seg) if is_opennews_material_only and seg.get("strict_news_media_only") else set()
    visual_domain = _opennews_visual_domain(seg, relevance_tokens) if relevance_tokens else "general"
    rejection_log: list[dict] = []
    source_fallback_available = (
        is_opennews_material_only
        and OPENNEWS_AI_IMAGE_ONLY
        and OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL
    )
    if is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and not source_fallback_available:
        print("  ℹ️ OpenNews AI图片专用模式：跳过新闻网页/网络图片素材，只使用5090生成图")
    elif is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY:
        print("  ℹ️ OpenNews AI图片优先模式：先用5090生成图，若不足再启用严格新闻源图片兜底")
    else:
        source_fallback_available = False
    if not (is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and not source_fallback_available):
        for item in (seg.get("source_materials") or []):
            if not isinstance(item, dict) or not item.get("url"):
                continue
            identity_keys = _source_identity_keys(str(item.get("url") or ""))
            if not identity_keys or identity_keys & seen_source_urls or identity_keys & used_source_urls:
                continue
            if _looks_like_bad_source_material(item) or _looks_like_unsafe_source_material(item):
                rejection_log.append({
                    "url": item.get("url") or "",
                    "title": item.get("title") or "",
                    "reason": "素材 URL 命中成人/裸露站点黑名单",
                })
                print(f"  ⚠️ 新闻素材安全过滤：成人/裸露站点｜{item.get('url')}")
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
                if not keep_item:
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
        if is_opennews_material_only:
            source_materials = _theme_balanced_source_materials(source_materials, relevance_tokens)
        else:
            source_materials.sort(key=_rank_source_material, reverse=True)
    source_video_count = 0
    source_image_count = 0
    if is_opennews_material_only:
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
        if ai_materials and OPENNEWS_AI_IMAGE_REPLACE_SOURCE and not allow_strict_source_fallback_now:
            source_materials = []
        if OPENNEWS_AI_IMAGE_ONLY and not allow_strict_source_fallback_now:
            source_materials = []
        elif allow_strict_source_fallback_now:
            print(
                "  ⚠️ 5090 AI图片不足，启用严格新闻源图片兜底："
                f"AI={ai_image_count}"
            )

    source_attempt_limit = 260 if is_opennews_material_only else 24
    for item in source_materials[:source_attempt_limit]:
        if len(material_items) >= max_total_materials:
            break
        if source_video_count >= max_source_videos and source_image_count >= max_source_images:
            break
        kind = str(item.get("kind") or "").strip().lower()
        if kind == "video" and source_video_count >= max_source_videos:
            continue
        if is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY and kind == "video":
            continue
        if kind != "video" and source_image_count >= max_source_images:
            continue
        try:
            copied_path = _download_source_material(str(item.get("url") or ""), output_dir, segment_index, len(material_items), kind=kind)
        except Exception as exc:
            print(f"  ⚠️ 新闻来源素材下载失败：{item.get('url')}｜{exc}")
            continue
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
        if content_hash:
            used_source_hashes.add(content_hash)
        used_source_urls.update(_source_identity_keys(str(item.get("url") or "")))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=kind or _asset_kind_for_suffix(copied_path), source="opennews_source")
        entry["source_url"] = item.get("source_url") or item.get("url")
        entry["title"] = item.get("title", "")
        entry["quality_score"] = item.get("_quality_score", 0)
        entry["quality_reason"] = item.get("_quality_reason", "")
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
        print(f"  ✅ 已下载新闻来源素材：{os.path.basename(copied_path)}")

    remaining_slots = max(0, max_total_materials - len(material_items))
    library_items = []
    if remaining_slots and not (is_opennews_material_only and OPENNEWS_AI_IMAGE_ONLY):
        library_items = search_material_library(
            seg,
            target_market=target_market or str(seg.get("target_market") or ""),
            department_id=department_id or str(seg.get("department_id") or ""),
            limit_videos=max(0, min(remaining_slots, max_source_videos - source_video_count)),
            limit_images=max(0, min(remaining_slots, max_source_images - source_image_count)),
        )
    library_video_count = source_video_count
    library_image_count = source_image_count
    for item in library_items:
        if len(material_items) >= max_total_materials:
            break
        item_kind = str(item.get("kind") or "").lower()
        if item_kind == "video" and library_video_count >= max_source_videos:
            continue
        if item_kind != "video" and library_image_count >= max_source_images:
            continue
        library_key = str(item.get("id") or item.get("path") or item.get("filename") or "")
        if library_key and library_key in used_library_ids:
            continue
        copied_path = copy_material_to_output(item, output_dir, segment_index, len(material_items))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=item.get("kind"), source="library")
        entry["library_id"] = item.get("id", "")
        entry["title"] = item.get("title", "")
        material_items.append(entry)
        if library_key:
            used_library_ids.add(library_key)
        if item_kind == "video":
            library_video_count += 1
        else:
            library_image_count += 1
        print(f"  ✅ 已命中本地素材库：{os.path.basename(copied_path)}")

    disable_free_fallback = True if is_opennews_material_only else bool(seg.get("disable_free_material_fallback"))
    allow_opennews_quality_fallback = False
    if is_opennews_material_only:
        if OPENNEWS_AI_IMAGE_ONLY:
            if OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL:
                print("  ℹ️ OpenNews 已禁用本地素材库/免费素材库兜底；5090不足时仅允许严格新闻源图片兜底")
            else:
                print("  ℹ️ OpenNews 已禁用新闻源/本地素材库/免费素材库兜底，仅使用5090 AI生成图片")
        else:
            print("  ℹ️ OpenNews 已禁用免费素材库兜底，仅使用新闻源、公开网页爬取和本地素材库")

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

    seg_with_materials["material_paths"] = material_paths
    seg_with_materials["material_items"] = material_items
    if is_opennews_material_only:
        seg_with_materials["material_quality"] = {
            "domain": visual_domain,
            "ai_image_only": OPENNEWS_AI_IMAGE_ONLY,
            "ai_image_target_min": OPENNEWS_IMAGE_MIN_IMAGES,
            "ai_image_target_max": OPENNEWS_IMAGE_MAX_IMAGES,
            "ai_image_count": sum(1 for item in material_items if item.get("source") == "opennews_ai_image"),
            "strict_source_fallback_enabled": OPENNEWS_STRICT_SOURCE_FALLBACK_WHEN_AI_FAIL,
            "strict_source_fallback_used": any(item.get("source") == "opennews_source" for item in material_items),
            "requires_human_review": False,
            "review_reason": "",
            "auto_publish_allowed": True,
            "source_counts": {
                source: sum(1 for item in material_items if item.get("source") == source)
                for source in sorted({str(item.get("source") or "unknown") for item in material_items})
            },
            "relevance_tokens": sorted(relevance_tokens)[:80],
            "accepted_count": len(material_items),
            "rejected_count": len(rejection_log),
            "rejections": rejection_log[:80],
        }
    if is_opennews_material_only and len(material_items) > OPENNEWS_MAX_MATERIALS:
        seg_with_materials["material_paths"] = material_paths[:OPENNEWS_MAX_MATERIALS]
        seg_with_materials["material_items"] = material_items[:OPENNEWS_MAX_MATERIALS]
    return seg_with_materials


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
