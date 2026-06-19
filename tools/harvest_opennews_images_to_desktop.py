#!/usr/bin/env python3
"""
Download OpenNews news-topic image candidates to the Desktop for manual review.

This script does not import anything into the production material library. It only
creates topic folders with images plus manifest files so admins can quickly review,
delete bad files, and upload the good ones through the existing material library UI.
"""

from __future__ import annotations

import argparse
import csv
import json
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
    _fetch_candidate_rows,
    discover_source_urls,
)
from source_ingest import DEFAULT_HEADERS  # noqa: E402


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
DEFAULT_DESKTOP = Path.home() / "Desktop"


def _slugify(value: str, fallback: str = "topic") -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", str(value or "").strip()).strip("_")
    return slug[:80] or fallback


def _safe_suffix(url: str, content_type: str = "") -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in IMAGE_SUFFIXES:
        return suffix
    content_type = content_type.lower()
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    if "jpeg" in content_type or "jpg" in content_type:
        return ".jpg"
    return ".jpg"


def _image_is_usable(path: Path, *, min_width: int = 600, min_height: int = 360) -> tuple[bool, str, dict]:
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
            return True, "ok", {"width": width, "height": height}
    except Exception as exc:
        return False, f"invalid_image:{exc}", {"width": 0, "height": 0}


def _download_image(url: str, output_path: Path) -> tuple[bool, str]:
    try:
        with requests.get(url, headers=DEFAULT_HEADERS, timeout=45, stream=True, allow_redirects=True) as response:
            response.raise_for_status()
            content_type = str(response.headers.get("content-type") or "").lower()
            if content_type and "image" not in content_type and "octet-stream" not in content_type:
                return False, f"not_image:{content_type}"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("wb") as handle:
                size = 0
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > 12 * 1024 * 1024:
                        return False, "too_large"
                    handle.write(chunk)
        return True, "downloaded"
    except Exception as exc:
        return False, str(exc)


def _preset_by_id() -> dict[str, dict]:
    return {str(item.get("id") or ""): item for item in NEWS_TOPIC_HARVEST_PRESETS}


def _select_presets(topic_ids: list[str]) -> list[dict]:
    presets = _preset_by_id()
    if not topic_ids:
        return list(NEWS_TOPIC_HARVEST_PRESETS)
    selected = []
    for topic_id in topic_ids:
        if topic_id not in presets:
            available = ", ".join(presets)
            raise SystemExit(f"Unknown topic preset: {topic_id}\nAvailable: {available}")
        selected.append(presets[topic_id])
    return selected


def _candidate_key(row: dict) -> str:
    return str(row.get("asset_url") or "").split("?")[0].strip().lower()


def harvest_topic(preset: dict, output_root: Path, *, per_topic: int, sources_limit: int) -> list[dict]:
    name = str(preset.get("name") or preset.get("id") or "topic")
    category = str(preset.get("category") or "")
    topic = str(preset.get("topic") or name)
    notes = str(preset.get("notes") or "")
    topic_dir = output_root / f"{_slugify(category, 'news')}_{_slugify(name)}"
    topic_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n== {name} ==")
    print(f"Discovering sources: {topic}")
    source_urls = discover_source_urls(topic, notes, limit=sources_limit, category=category)
    print(f"Sources: {len(source_urls)}")

    rows: list[dict] = []
    errors: list[str] = []
    for source_url in source_urls:
        try:
            rows.extend(_fetch_candidate_rows(topic, source_url, category=category, tags=list(preset.get("tags") or [])))
        except Exception as exc:
            errors.append(f"{source_url}: {exc}")

    deduped: list[dict] = []
    seen_assets: set[str] = set()
    seen_hosts: dict[str, int] = {}
    for row in rows:
        if str(row.get("kind") or "image") != "image":
            continue
        asset = str(row.get("asset_url") or "")
        key = _candidate_key(row)
        if not asset or key in seen_assets:
            continue
        host = urlparse(asset).netloc.lower()
        if seen_hosts.get(host, 0) >= 8:
            continue
        seen_assets.add(key)
        seen_hosts[host] = seen_hosts.get(host, 0) + 1
        deduped.append(row)

    print(f"Image candidates: {len(deduped)}")
    manifest_rows: list[dict] = []
    downloaded = 0
    for index, row in enumerate(deduped, start=1):
        if downloaded >= per_topic:
            break
        asset_url = str(row.get("asset_url") or "")
        suffix = _safe_suffix(asset_url)
        filename = f"{downloaded + 1:03d}_{_slugify(row.get('page_title') or row.get('title') or name, 'image')}{suffix}"
        if len(filename) > 120:
            filename = f"{downloaded + 1:03d}_{_slugify(name)}{suffix}"
        path = topic_dir / filename
        ok, reason = _download_image(asset_url, path)
        if ok:
            usable, reason, meta = _image_is_usable(path)
            if not usable:
                path.unlink(missing_ok=True)
                ok = False
        else:
            meta = {"width": 0, "height": 0}
        manifest_rows.append(
            {
                "topic_id": preset.get("id", ""),
                "topic_name": name,
                "category": category,
                "status": "downloaded" if ok else "rejected",
                "reason": reason,
                "filename": path.name if ok else "",
                "asset_url": asset_url,
                "source_url": row.get("source_url", ""),
                "source_site": row.get("source_site", ""),
                "page_title": row.get("page_title", ""),
                "page_excerpt": row.get("page_excerpt", ""),
                "width": meta.get("width", 0),
                "height": meta.get("height", 0),
                "tags": "、".join(preset.get("tags") or []),
            }
        )
        if ok:
            downloaded += 1
            print(f"  + {downloaded:02d}/{per_topic} {path.name}")
        elif index % 20 == 0:
            print(f"  checked {index}/{len(deduped)} candidates...")

    topic_manifest = topic_dir / "manifest.csv"
    with topic_manifest.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(manifest_rows[0].keys()) if manifest_rows else [
            "topic_id", "topic_name", "category", "status", "reason", "filename", "asset_url",
            "source_url", "source_site", "page_title", "page_excerpt", "width", "height", "tags",
        ])
        writer.writeheader()
        writer.writerows(manifest_rows)
    (topic_dir / "sources.json").write_text(json.dumps({"sources": source_urls, "errors": errors}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Downloaded: {downloaded}, folder: {topic_dir}")
    return manifest_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Download OpenNews topic images to Desktop folders.")
    parser.add_argument("--topic", action="append", default=[], help="Topic preset id. Repeatable. Omit to run all presets.")
    parser.add_argument("--per-topic", type=int, default=30, help="Max downloaded images per topic.")
    parser.add_argument("--sources-limit", type=int, default=14, help="Max discovered source pages per topic.")
    parser.add_argument("--output", default="", help="Output directory. Default: Desktop/OpenNews素材采集_TIMESTAMP")
    parser.add_argument("--list-topics", action="store_true", help="List available topic preset ids.")
    args = parser.parse_args()

    if args.list_topics:
        for item in NEWS_TOPIC_HARVEST_PRESETS:
            print(f"{item.get('id')}\t{item.get('name')}\t{item.get('category')}")
        return

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output).expanduser() if args.output else DEFAULT_DESKTOP / f"OpenNews素材采集_{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    presets = _select_presets(args.topic)
    all_rows: list[dict] = []
    for preset in presets:
        all_rows.extend(harvest_topic(preset, output_root, per_topic=max(1, args.per_topic), sources_limit=max(1, args.sources_limit)))

    summary_path = output_root / "manifest_all.csv"
    if all_rows:
        with summary_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_rows)
    print(f"\nDone: {output_root}")
    print("Review the folders, delete bad images, then upload the remaining images through the material library UI.")


if __name__ == "__main__":
    main()
