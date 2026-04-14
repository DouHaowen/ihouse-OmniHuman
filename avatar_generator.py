"""
管理员主播图生成器

使用火山方舟 Seedream 5.0 Lite 把参考人脸图生成成适合系统主播库的竖版头像。
"""

from __future__ import annotations

import base64
import io
import os
import time
from pathlib import Path
from typing import Iterable

import requests
from PIL import Image


ARK_IMAGE_GEN_URL = os.getenv("ARK_IMAGE_GEN_URL", "https://ark.cn-beijing.volces.com/api/v3/images/generations")
ARK_IMAGE_MODEL = os.getenv("ARK_IMAGE_MODEL", "doubao-seedream-5-0-lite-260128")
DEFAULT_BRAND_LOGO_PATH = Path(os.getenv("IHOUSE_LOGO_PATH", "/app/assets/ihouse-logo.webp"))


class AvatarGenerationError(RuntimeError):
    pass


def _get_api_key() -> str:
    api_key = (os.getenv("ARK_API_KEY") or os.getenv("VOLC_ARK_API_KEY") or os.getenv("API_KEY") or "").strip()
    if not api_key:
        raise AvatarGenerationError("缺少 ARK_API_KEY，无法调用 Seedream 5.0 Lite")
    return api_key


def _resolve_brand_logo_path() -> Path | None:
    candidates = []
    env_logo = os.getenv("IHOUSE_LOGO_PATH", "").strip()
    if env_logo:
        candidates.append(Path(env_logo))
    candidates.extend(
        [
            DEFAULT_BRAND_LOGO_PATH,
            Path(__file__).resolve().parent / "assets" / "ihouse-logo.webp",
        ]
    )
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    return None


def _image_file_to_data_url(path: Path, max_side: int = 1024, quality: int = 88, force_png: bool = False) -> str:
    if not path.exists():
        raise AvatarGenerationError(f"图片不存在：{path}")
    try:
        image = Image.open(path)
    except Exception as exc:
        raise AvatarGenerationError(f"无法打开图片：{exc}") from exc
    if force_png:
        image = image.convert("RGBA")
    else:
        image = image.convert("RGB")
    image.thumbnail((max_side, max_side))
    buffer = io.BytesIO()
    if force_png:
        image.save(buffer, format="PNG", optimize=True)
        return _to_data_url(buffer.getvalue(), "png")
    image.save(buffer, format="JPEG", quality=quality, optimize=True)
    return _to_data_url(buffer.getvalue(), "jpeg")


def _compose_reference_with_logo(reference_path: str, logo_path: Path) -> tuple[bytes, str]:
    try:
        reference_image = Image.open(reference_path).convert("RGB")
    except Exception as exc:
        raise AvatarGenerationError(f"无法打开参考图片：{exc}") from exc
    try:
        logo_image = Image.open(logo_path).convert("RGBA")
    except Exception as exc:
        raise AvatarGenerationError(f"无法打开品牌 logo 图片：{exc}") from exc

    reference_image.thumbnail((1200, 1200))
    canvas_width = max(reference_image.width, 1024)
    canvas = Image.new("RGB", (canvas_width, canvas_width + 360), "white")

    ref_x = (canvas_width - reference_image.width) // 2
    ref_y = 0
    canvas.paste(reference_image, (ref_x, ref_y))

    logo_target_width = min(340, max(220, canvas_width // 3))
    logo_ratio = logo_target_width / max(1, logo_image.width)
    logo_target_height = max(1, int(logo_image.height * logo_ratio))
    logo_resized = logo_image.resize((logo_target_width, logo_target_height))
    logo_x = (canvas_width - logo_resized.width) // 2
    logo_y = canvas_width + max(48, (360 - logo_resized.height) // 2)
    canvas.paste(logo_resized, (logo_x, logo_y), logo_resized)

    buffer = io.BytesIO()
    canvas.save(buffer, format="JPEG", quality=92, optimize=True)
    return buffer.getvalue(), "jpeg"


def _to_data_url(raw_bytes: bytes, image_format: str) -> str:
    encoded = base64.b64encode(raw_bytes).decode("ascii")
    return f"data:image/{image_format.lower()};base64,{encoded}"


def _compose_reference_input(reference_path: str) -> str:
    logo_path = _resolve_brand_logo_path()
    if logo_path:
        raw_bytes, image_format = _compose_reference_with_logo(reference_path, logo_path)
        return _to_data_url(raw_bytes, image_format)
    return _image_file_to_data_url(Path(reference_path), max_side=1200, quality=92)


def build_avatar_prompt(
    avatar_name: str,
    gender: str,
    style_note: str = "",
    target_markets: Iterable[str] | None = None,
) -> str:
    market_names = {
        "cn": "中国市场",
        "tw": "台湾市场",
        "jp": "日本市场",
    }
    markets_text = "、".join(market_names.get(item, item) for item in (target_markets or [])) or "中国市场、台湾市场、日本市场"
    gender_text = "女性" if gender == "female" else "男性"
    gender_specific_line = (
        "人物呈现为年轻或成熟但克制自然的女性主播气质，发型整洁、妆容自然、表情亲和，适合资讯类工作台。"
        if gender == "female"
        else "人物呈现为干净利落、稳重自然的男性主播气质，发型整洁、表情沉稳，适合资讯类工作台。"
    )
    note_text = style_note.strip()
    prompt_parts = [
        "请基于输入的人脸参考图，生成一张适合短视频新闻工作台使用的主播照片。",
        f"人物性别为{gender_text}，主播名称参考为“{avatar_name or '未命名主播'}”。",
        "必须是9:16竖版构图，头肩部或半身，正面或接近正面，高清真实摄影风格。",
        "请尽量保留参考人脸的识别度、发型特征、年龄感和整体气质，不要把人物变成卡通、二次元或过度磨皮。",
        "画面需要干净、专业、自然、亲和，适合直接作为系统内主播图或数字人形象图。",
        "背景简洁明亮，新闻播报感或资讯主播感，避免杂乱场景、多人同框、夸张姿势和强烈遮挡。",
        gender_specific_line,
        "请同时参考品牌 logo 图样，桌面或画面中的 iHouse 品牌 logo 需要保持屋顶线条、字标比例与颜色识别度一致。",
        f"目标市场偏好：{markets_text}。",
    ]
    if note_text:
        prompt_parts.append(f"额外风格要求：{note_text}")
    prompt_parts.append("整体保持商业可用、清晰、稳重、适合工作台展示。")
    return "\n".join(prompt_parts)


def _call_seedream(reference_data_url: str, prompt: str, size: str, watermark: bool = False) -> dict:
    headers = {
        "Authorization": f"Bearer {_get_api_key()}",
        "Content-Type": "application/json",
    }
    last_error: str | None = None
    for attempt in range(3):
        payload = {
            "model": ARK_IMAGE_MODEL,
            "prompt": prompt,
            "size": size,
            "sequential_image_generation": "disabled",
            "response_format": "url",
            "watermark": watermark,
        }
        if reference_data_url:
            payload["image"] = reference_data_url
        response = requests.post(ARK_IMAGE_GEN_URL, headers=headers, json=payload, timeout=1200)
        if response.status_code >= 400:
            text = response.text[:500]
            if "InputImageSensitiveContentDetected" in text:
                raise AvatarGenerationError("参考图被模型判定为敏感或不适合直接用于主播图生成，请更换为更普通、清晰、非夸张服装的人脸照片。")
            if "Error when parsing request" in text and attempt < 2:
                last_error = f"Seedream 请求解析失败：{text}"
                time.sleep(2 * (attempt + 1))
                continue
            if response.status_code == 429 or "ServerOverloaded" in text:
                last_error = f"Seedream 当前繁忙：HTTP {response.status_code} {text}"
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
            raise AvatarGenerationError(f"Seedream 调用失败：HTTP {response.status_code} {text}")
        try:
            data = response.json()
        except Exception as exc:
            raise AvatarGenerationError(f"Seedream 返回非 JSON 内容：{exc}") from exc
        if data.get("code") not in (None, 0, 10000):
            message = str(data.get("message") or "")
            if "InputImageSensitiveContentDetected" in message:
                raise AvatarGenerationError("参考图被模型判定为敏感或不适合直接用于主播图生成，请更换为更普通、清晰、非夸张服装的人脸照片。")
            if "Error when parsing request" in message and attempt < 2:
                last_error = f"Seedream 请求解析失败：{message}"
                time.sleep(2 * (attempt + 1))
                continue
            if "ServerOverloaded" in message or "429" in message:
                last_error = f"Seedream 当前繁忙：{message}"
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
            raise AvatarGenerationError(f"Seedream 生成失败：{data}")
    return data
    raise AvatarGenerationError(last_error or "Seedream 当前繁忙，请稍后重试")


def _candidate_prompts(base_prompt: str, count: int) -> list[str]:
    variants = [
        "风格更标准一些，像正式新闻主播证件照。",
        "风格更亲和自然一些，像短视频资讯主播。",
        "风格更稳重专业一些，像栏目主讲人。",
    ]
    prompts = []
    for idx in range(max(1, count)):
        suffix = variants[idx % len(variants)]
        prompts.append(f"{base_prompt}\n{suffix}")
    return prompts


def _download_url(url: str, output_path: str) -> None:
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(resp.content)


def generate_avatar_candidates(
    reference_path: str,
    output_dir: str,
    avatar_name: str,
    gender: str,
    style_note: str = "",
    target_markets: Iterable[str] | None = None,
    count: int = 3,
    size: str = "1440x2560",
) -> list[dict]:
    """
    生成主播图候选并落盘。

    返回值：
    [
      {
        "filename": "...png",
        "path": "...",
        "url": "...",
        "prompt": "...",
      }
    ]
    """
    reference_data_url = _compose_reference_input(reference_path)
    base_prompt = build_avatar_prompt(avatar_name, gender, style_note=style_note, target_markets=target_markets)
    prompts = _candidate_prompts(base_prompt, count)

    output = []
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for index, prompt in enumerate(prompts, start=1):
        response = _call_seedream(reference_data_url, prompt, size=size, watermark=False)
        urls = response.get("data") or []
        if not urls:
            raise AvatarGenerationError(f"Seedream 未返回图片地址：{response}")
        first_item = urls[0]
        if isinstance(first_item, str):
            image_url = first_item
        elif isinstance(first_item, dict):
            image_url = first_item.get("url") or first_item.get("image_url") or first_item.get("image") or ""
        else:
            image_url = ""
        if not image_url:
            raise AvatarGenerationError(f"Seedream 返回的图片地址格式无法识别：{response}")
        ext = "jpeg"
        if "?format=png" in str(image_url).lower():
            ext = "png"
        filename = f"candidate_{index:02d}.{ext}"
        file_path = output_path / filename
        _download_url(image_url, str(file_path))
        output.append(
            {
                "filename": filename,
                "path": str(file_path),
                "url": image_url,
                "prompt": prompt,
            }
        )

    return output
