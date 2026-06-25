#!/usr/bin/env python3
"""
Populate OpenNews local material-library review folders with candidate images.

This fills the folders created by tools/prepare_opennews_material_library_folders.py.
It is intentionally conservative by default: source-mode=curated uses structured
or official-ish providers from the topic-bank harvester instead of broad web
scraping. Admins should still delete bad images before importing into the
production material library.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
TOOLS_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from ai_material_harvester import NEWS_TOPIC_HARVEST_PRESETS  # noqa: E402
from harvest_opennews_topic_bank_to_desktop import (  # noqa: E402
    _candidate_host_allowed_for_topic,
    _collect_topic_candidates,
)
from harvest_recent_opennews_assets_to_desktop import (  # noqa: E402
    _download_image,
    _hash_file,
    _image_is_usable,
    _safe_suffix,
    _slugify,
)


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row or {}) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _topic_by_id() -> dict[str, dict]:
    return {str(topic.get("id") or ""): dict(topic) for topic in NEWS_TOPIC_HARVEST_PRESETS}


def _existing_hashes(folder: Path) -> set[str]:
    hashes: set[str] = set()
    for path in folder.iterdir() if folder.exists() else []:
        if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        try:
            hashes.add(_hash_file(path))
        except Exception:
            continue
    return hashes


def _existing_asset_urls(folder: Path) -> set[str]:
    urls: set[str] = set()
    for manifest_name in ("manifest.csv", "populate_manifest.csv"):
        for row in _read_csv(folder / manifest_name):
            asset_url = str(row.get("asset_url") or "").split("?")[0].strip().lower()
            if asset_url:
                urls.add(asset_url)
    return urls


def _topic_folder(root: Path, template_row: dict) -> Path:
    folder_value = str(template_row.get("folder") or "").strip()
    folder = Path(folder_value).expanduser() if folder_value else root
    if folder.exists():
        return folder
    fallback = root / Path(folder_value).name
    if fallback.exists():
        return fallback
    return folder


def _safe_filename(index: int, row: dict, topic: dict) -> str:
    suffix = _safe_suffix(str(row.get("asset_url") or "")) or ".jpg"
    title = row.get("image_title") or row.get("page_title") or row.get("title") or topic.get("name") or "image"
    filename = f"{index:03d}_{_slugify(title, 'image')}{suffix}"
    if len(filename) > 130:
        filename = f"{index:03d}_{_slugify(topic.get('name') or topic.get('id'), 'image')}{suffix}"
    return filename


def populate_folder(root: Path, *, args: argparse.Namespace) -> dict:
    template_path = root / "tag_template.csv"
    template_rows = _read_csv(template_path)
    if not template_rows:
        raise SystemExit(f"tag_template.csv not found or empty: {template_path}")

    topics = _topic_by_id()
    summary = {
        "root": str(root),
        "source_mode": args.source_mode,
        "topic_count": 0,
        "downloaded_count": 0,
        "skipped_count": 0,
        "topics": [],
    }
    for template_index, template_row in enumerate(template_rows, start=1):
        if args.limit_topics and template_index > args.limit_topics:
            break
        topic_id = str(template_row.get("topic_id") or "").strip()
        topic = topics.get(topic_id)
        if not topic:
            summary["topics"].append({"topic_id": topic_id, "status": "skipped", "reason": "topic_not_found"})
            continue
        folder = _topic_folder(root, template_row)
        folder.mkdir(parents=True, exist_ok=True)
        existing_images = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES]
        if existing_images and not args.fill_existing:
            summary["topics"].append(
                {
                    "topic_id": topic_id,
                    "topic_name": template_row.get("topic_name") or topic.get("name") or "",
                    "status": "skipped",
                    "reason": "folder_has_images",
                    "folder": str(folder),
                    "existing": len(existing_images),
                }
            )
            continue

        print(f"\n== {template_index:02d}. {topic.get('name') or topic_id} ==")
        rows = _collect_topic_candidates(
            topic,
            source_limit=args.source_limit,
            query_source_limit=args.query_source_limit,
            source_mode=args.source_mode,
        )
        print(f"候选：{len(rows)}，目标下载：{args.per_topic}")

        manifest_rows: list[dict] = []
        existing_hashes = _existing_hashes(folder)
        existing_urls = _existing_asset_urls(folder)
        downloaded = 0
        checked = 0
        for row in rows:
            checked += 1
            if downloaded >= args.per_topic:
                break
            asset_url = str(row.get("asset_url") or "").strip()
            asset_key = asset_url.split("?")[0].lower()
            if not asset_url or asset_key in existing_urls:
                summary["skipped_count"] += 1
                continue
            filename = _safe_filename(downloaded + 1, row, topic)
            target = folder / filename
            ok, reason, content_type = _download_image(asset_url, target, timeout=args.timeout)
            meta = {"width": 0, "height": 0}
            if ok:
                ok, reason, meta = _image_is_usable(target, min_width=args.min_width, min_height=args.min_height)
            if ok:
                try:
                    file_hash = _hash_file(target)
                except Exception as exc:
                    ok = False
                    reason = f"hash_failed:{exc}"
                    file_hash = ""
                if ok and file_hash in existing_hashes:
                    ok = False
                    reason = "duplicate_hash_in_folder"
                if ok:
                    existing_hashes.add(file_hash)
                    existing_urls.add(asset_key)
                    downloaded += 1
                    status = "downloaded"
                    print(f"  + {downloaded:02d}/{args.per_topic} {filename}")
                else:
                    target.unlink(missing_ok=True)
                    status = "rejected"
            else:
                target.unlink(missing_ok=True)
                status = "rejected"

            manifest_rows.append(
                {
                    "topic_id": topic_id,
                    "topic_name": template_row.get("topic_name") or topic.get("name") or "",
                    "category": template_row.get("category") or topic.get("category") or "",
                    "status": status,
                    "reason": reason,
                    "filename": filename if status == "downloaded" else "",
                    "asset_url": asset_url,
                    "source_url": row.get("source_url", ""),
                    "source_site": row.get("source_site", ""),
                    "source_type": row.get("source_type", ""),
                    "page_title": row.get("page_title", ""),
                    "image_title": row.get("image_title", ""),
                    "landing_url": row.get("landing_url", ""),
                    "page_excerpt": row.get("page_excerpt", ""),
                    "query": row.get("query", ""),
                    "relevance_score": row.get("relevance_score", 0),
                    "score_reason": row.get("score_reason", ""),
                    "allowed_host": _candidate_host_allowed_for_topic(row, topic),
                    "width": meta.get("width", 0),
                    "height": meta.get("height", 0),
                    "tags": template_row.get("recommended_tags") or "、".join(topic.get("tags") or []),
                    "content_type": content_type,
                }
            )

        if manifest_rows:
            _write_csv(folder / "manifest.csv", manifest_rows)
        (folder / "populate_summary.json").write_text(
            json.dumps(
                {
                    "topic_id": topic_id,
                    "topic_name": template_row.get("topic_name") or topic.get("name") or "",
                    "downloaded": downloaded,
                    "checked": checked,
                    "candidate_count": len(rows),
                    "source_mode": args.source_mode,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        summary["topic_count"] += 1
        summary["downloaded_count"] += downloaded
        summary["topics"].append(
            {
                "topic_id": topic_id,
                "topic_name": template_row.get("topic_name") or topic.get("name") or "",
                "folder": str(folder),
                "candidate_count": len(rows),
                "downloaded": downloaded,
            }
        )

    (root / "populate_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate prepared OpenNews material-library folders with curated candidate images.")
    parser.add_argument("folder", help="Folder created by prepare_opennews_material_library_folders.py")
    parser.add_argument("--source-mode", choices=["curated", "mixed", "bing"], default="curated", help="Default: curated. Use mixed only if curated is too sparse.")
    parser.add_argument("--per-topic", type=int, default=12, help="Max images to download per topic.")
    parser.add_argument("--limit-topics", type=int, default=0, help="Only populate the first N topics.")
    parser.add_argument("--source-limit", type=int, default=10, help="Max official/source pages per topic.")
    parser.add_argument("--query-source-limit", type=int, default=4, help="Max source pages per generated query.")
    parser.add_argument("--min-width", type=int, default=520, help="Minimum image width.")
    parser.add_argument("--min-height", type=int, default=300, help="Minimum image height.")
    parser.add_argument("--timeout", type=int, default=35, help="Image download timeout seconds.")
    parser.add_argument("--fill-existing", action="store_true", help="Also add images to folders that already contain images.")
    parser.add_argument("--json", action="store_true", help="Print summary JSON.")
    args = parser.parse_args()

    root = Path(args.folder).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Folder not found: {root}")
    summary = populate_folder(root, args=args)
    print(f"\n完成：{root}")
    print(f"已处理专题：{summary['topic_count']}，下载图片：{summary['downloaded_count']}")
    print("下一步：打开桌面文件夹人工删除不满意图片，再运行导入脚本。")
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
