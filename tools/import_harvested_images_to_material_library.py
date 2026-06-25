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
import hashlib
import json
import os
import requests
import shutil
import sys
import tempfile
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from material_library import (  # noqa: E402
    MATERIAL_LIBRARY_DIR,
    list_material_library_items,
    register_material_file,
    update_material_library_item,
)


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
DEFAULT_VECTOR_URL = os.getenv("MATERIAL_VECTOR_SERVICE_URL", "http://192.168.0.34:8897").strip()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _existing_image_hashes() -> set[str]:
    hashes: set[str] = set()
    for item in list_material_library_items():
        if str(item.get("kind") or "") != "image":
            continue
        filename = Path(str(item.get("filename") or "")).name
        path = MATERIAL_LIBRARY_DIR / filename
        if not path.exists() or path.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        try:
            hashes.add(_hash_file(path))
        except Exception:
            pass
    return hashes


def _existing_source_urls() -> set[str]:
    urls: set[str] = set()
    for item in list_material_library_items():
        source_url = str(item.get("source_url") or "").strip().split("?")[0].lower()
        if source_url:
            urls.add(source_url)
    return urls


def _sync_item_to_vector_library(item: dict, *, vector_url: str = DEFAULT_VECTOR_URL) -> dict:
    if not vector_url:
        return {"ok": False, "reason": "vector_url_empty"}
    if str(item.get("kind") or "").lower() != "image":
        return {"ok": False, "reason": "not_image"}
    filename = Path(str(item.get("filename") or "")).name
    file_path = (MATERIAL_LIBRARY_DIR / filename).resolve()
    library_root = MATERIAL_LIBRARY_DIR.resolve()
    if not str(file_path).startswith(str(library_root)) or not file_path.exists():
        return {"ok": False, "reason": "file_missing"}
    material_id = f"prod_{item.get('id') or file_path.stem}"
    title = " | ".join(
        part
        for part in [
            str(item.get("title") or file_path.stem),
            str(item.get("category") or ""),
            " ".join(map(str, item.get("tags") or [])),
            " ".join(map(str, item.get("news_topics") or [])),
        ]
        if str(part or "").strip()
    )
    with file_path.open("rb") as handle:
        response = requests.post(
            f"{vector_url.rstrip('/')}/analyze-upload",
            data={"material_id": material_id, "title": title},
            files={"file": (filename, handle, "application/octet-stream")},
            timeout=360,
        )
    response.raise_for_status()
    payload = response.json()
    analysis = payload.get("analysis") if isinstance(payload, dict) else {}
    if isinstance(analysis, dict):
        ai_tags = []
        for key in ("category", "entities", "scenes", "concepts", "visible_text"):
            value = analysis.get(key)
            if isinstance(value, list):
                ai_tags.extend(str(entry or "") for entry in value if str(entry or "").strip())
            elif value:
                ai_tags.append(str(value))
        update_material_library_item(
            str(item.get("id") or ""),
            {
                "ai_provider": "5090-qwen3-vl-bge-m3",
                "ai_summary": str(analysis.get("description") or "").strip(),
                "ai_tags": ai_tags,
                "safety_status": str(analysis.get("safety_status") or item.get("safety_status") or "safe"),
            },
        )
    return {"ok": True, "material_id": material_id, "analysis": analysis}


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
    if results:
        return results
    tag_template = root / "tag_template.csv"
    if tag_template.exists():
        for row in _read_manifest(tag_template):
            folder_value = str(row.get("folder") or "").strip()
            folder = Path(folder_value).expanduser() if folder_value else root
            if not folder.exists():
                # Template folders may be moved together with the root. Fall
                # back to matching the folder basename under the current root.
                folder = root / Path(folder_value).name
            if not folder.exists():
                continue
            tags = row.get("recommended_tags") or row.get("tags") or ""
            for image_path in sorted(folder.iterdir()):
                if not image_path.is_file() or image_path.suffix.lower() not in IMAGE_SUFFIXES:
                    continue
                results.append(
                    (
                        folder,
                        {
                            "status": "downloaded",
                            "filename": image_path.name,
                            "category": row.get("category") or "",
                            "topic_id": row.get("topic_id") or "",
                            "topic_name": row.get("topic_name") or "",
                            "tags": tags,
                            "page_title": image_path.stem,
                            "page_excerpt": row.get("notes") or "",
                            "source_url": "",
                            "source_site": "local_review_folder",
                            "source_type": "local_review_folder",
                        },
                    )
                )
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


def import_harvested_folder(
    root: Path,
    *,
    dry_run: bool = False,
    limit: int = 0,
    sync_vector: bool = False,
    vector_url: str = DEFAULT_VECTOR_URL,
) -> dict:
    rows = _find_manifest_rows(root)
    imported = []
    skipped = []
    seen_files = set()
    existing_hashes = _existing_image_hashes()
    existing_source_urls = _existing_source_urls()
    run_hashes: set[str] = set()
    run_source_urls: set[str] = set()
    vector_synced = []
    vector_failed = []
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
        try:
            file_hash = _hash_file(image_path)
        except Exception as exc:
            skipped.append({"filename": image_path.name, "reason": f"hash_failed:{exc}"})
            continue
        if file_hash in existing_hashes:
            skipped.append({"filename": image_path.name, "reason": "already_in_material_library_hash"})
            continue
        if file_hash in run_hashes:
            skipped.append({"filename": image_path.name, "reason": "duplicate_in_import_folder"})
            continue
        source_url_key = str(row.get("source_url") or "").strip().split("?")[0].lower()
        if source_url_key and source_url_key in existing_source_urls:
            skipped.append({"filename": image_path.name, "reason": "already_in_material_library_source_url"})
            continue
        if source_url_key and source_url_key in run_source_urls:
            skipped.append({"filename": image_path.name, "reason": "duplicate_source_url_in_import_folder"})
            continue
        run_hashes.add(file_hash)
        if source_url_key:
            run_source_urls.add(source_url_key)
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
                f"新闻：{row.get('news_title')}" if row.get("news_title") else "",
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
                news_topics=[value for value in [*news_topics, row.get("news_title"), row.get("news_source_name")] if value],
            )
        item = update_material_library_item(
            item["id"],
            {
                "status": "approved",
                "reviewed_by_username": "desktop_harvest",
                "reviewed_by_display_name": "桌面专题采集",
                "reviewed_at": time.time(),
                "safety_status": "approved",
            },
        )
        imported.append({"id": item.get("id"), "filename": item.get("filename"), "category": category, "tags": tags})
        existing_hashes.add(file_hash)
        if source_url_key:
            existing_source_urls.add(source_url_key)
        if sync_vector:
            try:
                vector_result = _sync_item_to_vector_library(item, vector_url=vector_url)
                vector_synced.append({"id": item.get("id"), "filename": item.get("filename"), **vector_result})
                print(f"  vector synced: {item.get('filename')}")
            except Exception as exc:
                vector_failed.append({"id": item.get("id"), "filename": item.get("filename"), "error": str(exc)})
                print(f"  vector failed: {item.get('filename')}: {exc}")
    return {
        "imported_count": len(imported),
        "skipped_count": len(skipped),
        "vector_synced_count": len(vector_synced),
        "vector_failed_count": len(vector_failed),
        "imported": imported,
        "skipped": skipped,
        "vector_synced": vector_synced,
        "vector_failed": vector_failed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Import reviewed harvested images into iHouse material library.")
    parser.add_argument("folder", help="Harvest root folder, e.g. /Users/saita/Desktop/OpenNews素材采集_20260618_120000")
    parser.add_argument("--dry-run", action="store_true", help="Preview import without changing material_library.")
    parser.add_argument("--limit", type=int, default=0, help="Max files to import.")
    parser.add_argument("--sync-vector", action="store_true", help="Analyze and insert newly imported images into the 5090 vector material index.")
    parser.add_argument("--vector-url", default=DEFAULT_VECTOR_URL, help="5090 vector service URL. Default: MATERIAL_VECTOR_SERVICE_URL or http://192.168.0.34:8897")
    parser.add_argument("--json", action="store_true", help="Print full JSON result.")
    args = parser.parse_args()
    root = Path(args.folder).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Folder not found: {root}")
    result = import_harvested_folder(
        root,
        dry_run=args.dry_run,
        limit=args.limit,
        sync_vector=args.sync_vector,
        vector_url=args.vector_url,
    )
    print(
        f"imported={result['imported_count']} skipped={result['skipped_count']} "
        f"vector_synced={result['vector_synced_count']} vector_failed={result['vector_failed_count']}"
    )
    for item in result["imported"][:20]:
        print(f"+ {item.get('category')}: {item.get('filename')} tags={','.join(item.get('tags') or [])}")
    if result["imported_count"] > 20:
        print(f"... {result['imported_count'] - 20} more")
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
