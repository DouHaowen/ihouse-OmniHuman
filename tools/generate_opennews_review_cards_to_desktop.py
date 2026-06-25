#!/usr/bin/env python3
"""
Generate accurate OpenNews review-card images for recent news items.

This is the fallback path when web image crawling is too inaccurate. Instead of
guessing a matching photo, it creates clean news-card images from the actual
news title, source, time and summary. Admins can review/import these cards into
the material library just like normal images.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT_DIR = Path(__file__).resolve().parents[1]
TOOLS_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from harvest_recent_opennews_assets_to_desktop import (  # noqa: E402
    DEFAULT_DESKTOP,
    DEFAULT_PROD_BASE_URL,
    _item_summary,
    _item_title,
    _slugify,
    load_recent_news_items,
)


CARD_TAGS = {
    "AI": ["domain:ai", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "科技": ["domain:technology", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "金融": ["domain:finance", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "政治": ["domain:politics", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "军事": ["domain:military", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "房产": ["domain:real_estate", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
    "移民": ["domain:immigration", "scene:news_card", "usage:accurate_fallback", "safety:youtube_safe"],
}

PALETTES = {
    "AI": ("#071827", "#0E7490", "#67E8F9", "#E0F2FE"),
    "科技": ("#0B1020", "#2563EB", "#93C5FD", "#EFF6FF"),
    "金融": ("#111827", "#B45309", "#FCD34D", "#FFFBEB"),
    "政治": ("#111827", "#1D4ED8", "#BFDBFE", "#EFF6FF"),
    "军事": ("#101513", "#166534", "#86EFAC", "#F0FDF4"),
    "房产": ("#102018", "#047857", "#6EE7B7", "#ECFDF5"),
    "移民": ("#1E1B4B", "#7C3AED", "#C4B5FD", "#F5F3FF"),
    "新闻": ("#111827", "#0F766E", "#99F6E4", "#F0FDFA"),
}


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc" if bold else "/System/Library/Fonts/STHeiti Light.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _published_text(item: dict) -> str:
    value = item.get("published_at") or item.get("news_time") or item.get("date") or ""
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("T", " ").replace("Z", " UTC")[:22]


def _category(item: dict) -> str:
    return _clean_text(item.get("category") or item.get("batch_category") or "新闻") or "新闻"


def _source_name(item: dict) -> str:
    return _clean_text(item.get("source_name") or item.get("trend_domain") or item.get("source") or "OpenNews")


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int, max_lines: int) -> list[str]:
    chars = list(_clean_text(text))
    lines: list[str] = []
    current = ""
    for char in chars:
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
    if len(lines) == max_lines and len("".join(lines)) < len(_clean_text(text)):
        lines[-1] = lines[-1].rstrip("，。,. ") + "..."
    return lines


def _draw_round_rect(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], radius: int, fill: str, outline: str | None = None, width: int = 1) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _accent_lines(draw: ImageDraw.ImageDraw, width: int, height: int, accent: str) -> None:
    for i in range(9):
        x0 = int(width * (0.58 + i * 0.055))
        draw.line((x0, -80, x0 - int(width * 0.22), height + 80), fill=accent, width=2)
    for i in range(5):
        y = int(height * (0.18 + i * 0.14))
        draw.line((int(width * 0.66), y, width - 60, y + 38), fill=accent, width=1)


def _draw_card(item: dict, output_path: Path, *, size: tuple[int, int], variant: str) -> None:
    width, height = size
    category = _category(item)
    bg, primary, accent, panel = PALETTES.get(category, PALETTES["新闻"])
    title = _item_title(item) or "OpenNews 新闻"
    summary = _item_summary(item)
    source = _source_name(item)
    published = _published_text(item)

    image = Image.new("RGB", size, bg)
    draw = ImageDraw.Draw(image)
    _accent_lines(draw, width, height, primary)

    margin = int(width * 0.07)
    top = int(height * 0.08)
    card_box = (margin, top, width - margin, height - top)
    _draw_round_rect(draw, card_box, int(width * 0.035), "#FFFFFF", outline=accent, width=3)

    eyebrow_font = _font(max(24, int(width * 0.025)), bold=True)
    title_font = _font(max(42, int(width * (0.055 if width > height else 0.075))), bold=True)
    body_font = _font(max(26, int(width * (0.027 if width > height else 0.04))))
    meta_font = _font(max(22, int(width * 0.022)))
    small_font = _font(max(20, int(width * 0.02)))

    x = margin + int(width * 0.045)
    y = top + int(height * 0.06)
    pill_text = f"{category} | OpenNews"
    pill_w = draw.textbbox((0, 0), pill_text, font=eyebrow_font)[2] + 54
    _draw_round_rect(draw, (x, y, x + pill_w, y + int(height * 0.055)), 28, primary)
    draw.text((x + 26, y + 11), pill_text, fill="white", font=eyebrow_font)

    y += int(height * 0.095)
    title_max_lines = 3 if width > height else 4
    title_lines = _wrap_text(draw, title, title_font, width - margin * 2 - int(width * 0.09), title_max_lines)
    for line in title_lines:
        draw.text((x, y), line, fill="#111827", font=title_font)
        y += int(title_font.size * 1.25)

    y += int(height * 0.025)
    source_line = " | ".join(part for part in [source, published] if part)
    if source_line:
        draw.text((x, y), source_line, fill="#475569", font=meta_font)
        y += int(meta_font.size * 1.8)

    if variant != "headline":
        summary_lines = _wrap_text(draw, summary, body_font, width - margin * 2 - int(width * 0.09), 4 if width > height else 6)
        for line in summary_lines:
            draw.text((x, y), line, fill="#1F2937", font=body_font)
            y += int(body_font.size * 1.45)

    footer_y = height - top - int(height * 0.09)
    draw.line((x, footer_y - 24, width - margin - int(width * 0.045), footer_y - 24), fill="#CBD5E1", width=2)
    draw.text((x, footer_y), "准确新闻图卡 | 已绑定标题、来源与发布时间", fill="#64748B", font=small_font)
    draw.text((width - margin - int(width * 0.18), footer_y), "OpenNews", fill=primary, font=small_font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, "JPEG", quality=94, optimize=True)


def generate_cards_for_item(item: dict, output_root: Path, index: int, args: argparse.Namespace) -> list[dict]:
    title = _item_title(item) or f"news_{index}"
    category = _category(item)
    news_dir = output_root / f"{index:02d}_{_slugify(category)}_{_slugify(title)}"
    news_dir.mkdir(parents=True, exist_ok=True)

    variants = [("horizontal", (1920, 1080), "summary")]
    if args.vertical:
        variants.append(("vertical", (1080, 1920), "summary"))
    if args.headline:
        variants.append(("headline_horizontal", (1920, 1080), "headline"))

    rows: list[dict] = []
    for variant_name, size, variant in variants:
        filename = f"{variant_name}_{_slugify(title, 'opennews_card')}.jpg"
        path = news_dir / filename
        _draw_card(item, path, size=size, variant=variant)
        tags = CARD_TAGS.get(category, CARD_TAGS["新闻"])
        rows.append(
            {
                "topic_id": "opennews_verified_card",
                "topic_name": title,
                "category": category,
                "status": "downloaded",
                "reason": "generated_verified_news_card",
                "filename": filename,
                "asset_url": "",
                "source_url": _clean_text(item.get("url") or item.get("source_url") or ""),
                "source_site": _source_name(item),
                "source_type": "generated_news_card",
                "page_title": title,
                "page_excerpt": _item_summary(item),
                "query": "generated_from_news_metadata",
                "source_tier": "verified_news_card",
                "relevance_score": 999,
                "relevance_reason": "directly generated from the news title, source, published time and summary",
                "width": size[0],
                "height": size[1],
                "tags": "、".join(tags),
                "news_title": title,
                "news_summary": _item_summary(item),
                "news_source_url": _clean_text(item.get("url") or item.get("source_url") or ""),
                "news_source_name": _source_name(item),
                "news_published_at": item.get("published_at") or item.get("news_time") or "",
            }
        )
    with (news_dir / "manifest.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    (news_dir / "news.json").write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"+ {index:02d}. {title} cards={len(rows)}")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate accurate OpenNews review-card images to Desktop.")
    parser.add_argument("--output", default="", help="Output folder. Default: Desktop/OpenNews准确新闻图卡_TIMESTAMP")
    parser.add_argument("--batch-dir", default=str(ROOT_DIR / "output" / "opennews_batches" / "batches"), help="Local OpenNews batch JSON folder.")
    parser.add_argument("--base-url", default=DEFAULT_PROD_BASE_URL, help="Production base URL for pulling latest batches when local data is absent.")
    parser.add_argument("--token", default="", help="External OpenNews X-Token. Or set OPENNEWS_EXTERNAL_TOKEN.")
    parser.add_argument("--days", type=int, default=2, help="Only include news from the last N days when timestamps are present.")
    parser.add_argument("--max-batches", type=int, default=12, help="Max recent batches to inspect.")
    parser.add_argument("--limit-news", type=int, default=30, help="Max news items to generate cards for.")
    parser.add_argument("--vertical", action="store_true", help="Also generate vertical 9:16 cards.")
    parser.add_argument("--headline", action="store_true", help="Also generate a headline-only horizontal card.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output).expanduser() if args.output else DEFAULT_DESKTOP / f"OpenNews准确新闻图卡_{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    news_items = load_recent_news_items(args)
    if not news_items:
        raise SystemExit("No recent OpenNews items found. Provide --token or make sure local output/opennews_batches/batches exists.")

    all_rows: list[dict] = []
    print(f"新闻数：{len(news_items)}，输出：{output_root}")
    for index, item in enumerate(news_items, start=1):
        all_rows.extend(generate_cards_for_item(item, output_root, index, args))

    if all_rows:
        with (output_root / "manifest_all.csv").open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
    summary = {
        "created_at": time.time(),
        "news_count": len(news_items),
        "card_count": len(all_rows),
        "output_root": str(output_root),
    }
    (output_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n完成：{output_root}")
    print(f"生成图卡：{len(all_rows)}")
    print("下一步：审核文件夹里的图卡，然后运行 import_harvested_images_to_material_library.py 导入。")


if __name__ == "__main__":
    main()
