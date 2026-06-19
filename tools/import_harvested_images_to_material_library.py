#!/usr/bin/env python3
"""
Import reviewed Desktop harvest folders into the local material library.

Expected workflow:
1. Run tools/harvest_opennews_images_to_desktop.py.
2. Manually delete bad/unsafe/unrelated images from the generated folders.
3. Run this importer against the harvest root folder.

The importer preserves category/tags/news_topics from manifest.csv and marks the
remaining images as approved so the material library matcher can find them later.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
import tempfile
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from material_library import register_material_file, update_material_library_item  # noqa: E402


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}


def _split_tags(value: str) -> list[str]:
    tags = []
    seen = set()
    for part in str(value or "").replace(",", "、").split("、"):
        text = part.strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        tags.append(text)
    return tags


def _read_manifest(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(dict(row or {}))
    return rows


def _find_manifest_rows(root: Path) -> list[tuple[Path, dict]]:
    all_manifest = root / "manifest_all.csv"
    if all_manifest.exists():
        return [(root, row) for row in _read_manifest(all_manifest)]
    results: list[tuple[Path, dict]] = []
    for manifest in sorted(root.glob("*/manifest.csv")):
        for row in _read_manifest(manifest):
            results.append((manifest.parent, row))
    return results


def _resolve_image_path(root: Path, folder: Path, row: dict) -> Path | None:
    filename = str(row.get("filename") or "").strip()
    if not filename:
        return None
    candidates = [folder / filename]
    if folder == root:
        candidates.extend(root.glob(f"*/{filename}"))
    for path in candidates:
        if path.exists() and path.suffix.lower() in IMAGE_SUFFIXES:
            return path
    return None


def import_harvested_folder(root: Path, *, dry_run: bool = False, limit: int = 0) -> dict:
    rows = _find_manifest_rows(root)
    imported = []
    skipped = []
    seen_files = set()
    for folder, row in rows:
        if str(row.get("status") or "").strip().lower() != "downloaded":
            continue
        image_path = _resolve_image_path(root, folder, row)
        if not image_path:
            skipped.append({"filename": row.get("filename", ""), "reason": "file_missing"})
            continue
        resolved = str(image_path.resolve())
        if resolved in seen_files:
            skipped.append({"filename": image_path.name, "reason": "duplicate_file"})
            continue
        seen_files.add(resolved)
        if limit and len(imported) >= limit:
            skipped.append({"filename": image_path.name, "reason": "limit_reached"})
            continue

        category = str(row.get("category") or "").strip()
        topic_name = str(row.get("topic_name") or "").strip()
        page_title = str(row.get("page_title") or "").strip()
        tags = _split_tags(row.get("tags") or "")
        news_topics = [value for value in [topic_name, category, *tags] if value]
        title = page_title or topic_name or image_path.stem
        notes = "｜".join(
            part
            for part in [
                f"专题：{topic_name}" if topic_name else "",
                str(row.get("page_excerpt") or "").strip(),
                f"来源：{row.get('source_site')}" if row.get("source_site") else "",
            ]
            if part
        )
        if dry_run:
            imported.append({"filename": image_path.name, "category": category, "tags": tags, "dry_run": True})
            continue

        with tempfile.TemporaryDirectory(prefix="ihouse_material_import_") as tmpdir:
            temp_path = Path(tmpdir) / image_path.name
            shutil.copy2(image_path, temp_path)
            item = register_material_file(
                temp_path=str(temp_path),
                original_filename=image_path.name,
                title=title,
                category=category,
                tags=tags,
                ai_tags=tags,
                notes=notes,
                uploader_username="desktop_harvest",
                uploader_display_name="桌面专题采集",
                source="desktop_topic_harvest",
                source_url=str(row.get("source_url") or ""),
                source_site=str(row.get("source_site") or ""),
                license_note="桌面专题采集导入；已由管理员本地筛选后入库。",
                safety_status="approved",
                news_topics=news_topics,
            )
        item = update_material_library_item(
            item["id"],
            {
                "status": "approved",
                "reviewed_by_username": "desktop_harvest",
                "reviewed_by_display_name": "桌面专题采集",
                "reviewed_at": __import__("time").time(),
                "safety_status": "approved",
            },
        )
        imported.append({"id": item.get("id"), "filename": item.get("filename"), "category": category, "tags": tags})
    return {"imported_count": len(imported), "skipped_count": len(skipped), "imported": imported, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser(description="Import reviewed harvested images into iHouse material library.")
    parser.add_argument("folder", help="Harvest root folder, e.g. /Users/saita/Desktop/OpenNews素材采集_20260618_120000")
    parser.add_argument("--dry-run", action="store_true", help="Preview import without changing material_library.")
    parser.add_argument("--limit", type=int, default=0, help="Max files to import.")
    args = parser.parse_args()
    root = Path(args.folder).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Folder not found: {root}")
    result = import_harvested_folder(root, dry_run=args.dry_run, limit=args.limit)
    print(f"imported={result['imported_count']} skipped={result['skipped_count']}")
    for item in result["imported"][:20]:
        print(f"+ {item.get('category')}: {item.get('filename')} tags={','.join(item.get('tags') or [])}")
    if result["imported_count"] > 20:
        print(f"... {result['imported_count'] - 20} more")


if __name__ == "__main__":
    main()
