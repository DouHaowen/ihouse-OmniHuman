import json
import os
import re
import shutil
import threading
import time
import uuid
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
MATERIAL_LIBRARY_DIR = BASE_DIR / "material_library"
MATERIAL_LIBRARY_DIR.mkdir(exist_ok=True)
MATERIAL_LIBRARY_MANIFEST_PATH = MATERIAL_LIBRARY_DIR / "manifest.json"
MATERIAL_LIBRARY_LOCK = threading.Lock()

VIDEO_SUFFIXES = {".mp4", ".mov", ".m4v", ".webm"}
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
ALLOWED_SUFFIXES = VIDEO_SUFFIXES | IMAGE_SUFFIXES


def _now() -> float:
    return time.time()


def _normalize_list(value) -> list[str]:
    if isinstance(value, str):
        items = re.split(r"[,，、\n]+", value)
    else:
        items = list(value or [])
    normalized = []
    seen = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def _asset_kind_for_suffix(path: str) -> str:
    suffix = Path(str(path)).suffix.lower()
    if suffix in VIDEO_SUFFIXES:
        return "video"
    return "image"


def _slugify(value: str, fallback: str = "material") -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", str(value or "").strip()).strip("_")
    return slug or fallback


def _safe_suffix(filename: str) -> str:
    suffix = Path(filename or "").suffix.lower()
    return suffix if suffix in ALLOWED_SUFFIXES else ""


def _load_manifest() -> dict:
    if not MATERIAL_LIBRARY_MANIFEST_PATH.exists():
        return {"items": []}
    try:
        with open(MATERIAL_LIBRARY_MANIFEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return data
    except Exception:
        pass
    return {"items": []}


def _save_manifest(manifest: dict) -> None:
    MATERIAL_LIBRARY_DIR.mkdir(parents=True, exist_ok=True)
    with open(MATERIAL_LIBRARY_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)


def _normalize_item(item: dict) -> dict:
    filename = Path(str(item.get("filename") or "")).name
    file_path = (MATERIAL_LIBRARY_DIR / filename).resolve()
    library_root = MATERIAL_LIBRARY_DIR.resolve()
    if not filename or not str(file_path).startswith(str(library_root)):
        raise ValueError("素材文件路径非法")
    return {
        "id": str(item.get("id") or uuid.uuid4().hex[:12]),
        "filename": filename,
        "title": str(item.get("title") or Path(filename).stem),
        "kind": item.get("kind") or _asset_kind_for_suffix(filename),
        "status": str(item.get("status") or "pending").strip() or "pending",
        "category": str(item.get("category") or "").strip(),
        "tags": _normalize_list(item.get("tags")),
        "ai_tags": _normalize_list(item.get("ai_tags")),
        "ai_summary": str(item.get("ai_summary") or "").strip(),
        "ai_provider": str(item.get("ai_provider") or "").strip(),
        "department_ids": _normalize_list(item.get("department_ids")),
        "target_markets": _normalize_list(item.get("target_markets")),
        "notes": str(item.get("notes") or "").strip(),
        "uploader_username": str(item.get("uploader_username") or "").strip(),
        "uploader_display_name": str(item.get("uploader_display_name") or "").strip(),
        "original_filename": str(item.get("original_filename") or filename),
        "source": str(item.get("source") or "manual"),
        "created_at": float(item.get("created_at") or _now()),
        "reviewed_at": float(item.get("reviewed_at") or 0),
        "reviewed_by_username": str(item.get("reviewed_by_username") or "").strip(),
        "reviewed_by_display_name": str(item.get("reviewed_by_display_name") or "").strip(),
    }


def list_material_library_items(*, status: str | None = None) -> list[dict]:
    with MATERIAL_LIBRARY_LOCK:
        items = []
        for raw in _load_manifest().get("items", []):
            try:
                item = _normalize_item(raw)
            except Exception:
                continue
            full_path = MATERIAL_LIBRARY_DIR / item["filename"]
            if not full_path.exists():
                continue
            if status and item.get("status") != status:
                continue
            items.append(item)
    return sorted(items, key=lambda row: (row.get("created_at", 0), row.get("title", "")), reverse=True)


def register_material_file(
    *,
    temp_path: str,
    original_filename: str,
    title: str = "",
    category: str = "",
    tags=None,
    ai_tags=None,
    department_ids=None,
    target_markets=None,
    notes: str = "",
    uploader_username: str = "",
    uploader_display_name: str = "",
    source: str = "manual",
) -> dict:
    source_path = Path(temp_path).resolve()
    if not source_path.exists():
        raise FileNotFoundError("上传素材不存在")
    suffix = _safe_suffix(original_filename or source_path.name)
    if not suffix:
        raise ValueError("仅支持上传 jpg、jpeg、png、webp、mp4、mov、m4v、webm")
    material_id = uuid.uuid4().hex[:12]
    final_name = f"{_slugify(title or Path(original_filename or source_path.name).stem)}_{material_id}{suffix}"
    final_path = MATERIAL_LIBRARY_DIR / final_name
    MATERIAL_LIBRARY_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_path), str(final_path))
    item = _normalize_item(
        {
            "id": material_id,
            "filename": final_name,
            "title": title or Path(original_filename or source_path.name).stem,
            "kind": _asset_kind_for_suffix(final_name),
            "status": "pending",
            "category": category,
            "tags": tags,
            "ai_tags": ai_tags,
            "department_ids": department_ids,
            "target_markets": target_markets,
            "notes": notes,
            "uploader_username": uploader_username,
            "uploader_display_name": uploader_display_name,
            "original_filename": original_filename or source_path.name,
            "source": source,
            "created_at": _now(),
        }
    )
    with MATERIAL_LIBRARY_LOCK:
        manifest = _load_manifest()
        manifest["items"] = [row for row in manifest.get("items", []) if str(row.get("id")) != material_id]
        manifest["items"].append(item)
        _save_manifest(manifest)
    return item


def delete_material_library_item(item_id: str) -> dict:
    with MATERIAL_LIBRARY_LOCK:
        manifest = _load_manifest()
        items = manifest.get("items", [])
        match = next((row for row in items if str(row.get("id")) == str(item_id)), None)
        if not match:
            raise FileNotFoundError("素材不存在")
        manifest["items"] = [row for row in items if str(row.get("id")) != str(item_id)]
        _save_manifest(manifest)
    file_path = MATERIAL_LIBRARY_DIR / Path(str(match.get("filename") or "")).name
    file_path.unlink(missing_ok=True)
    return _normalize_item(match)


def update_material_library_item(item_id: str, updates: dict) -> dict:
    with MATERIAL_LIBRARY_LOCK:
        manifest = _load_manifest()
        items = manifest.get("items", [])
        index = next((idx for idx, row in enumerate(items) if str(row.get("id")) == str(item_id)), -1)
        if index < 0:
            raise FileNotFoundError("素材不存在")
        merged = dict(items[index] or {})
        merged.update(updates or {})
        normalized = _normalize_item(merged)
        items[index] = normalized
        manifest["items"] = items
        _save_manifest(manifest)
    return normalized


def _text_variants(value: str) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    variants = [text]
    variants.extend(part for part in re.split(r"[\s,，、/|;；:：()（）\-_.]+", text) if len(part) >= 2)
    deduped = []
    seen = set()
    for item in variants:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _material_score(item: dict, seg: dict, *, target_market: str = "", department_id: str = "") -> int:
    searchable = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("category") or ""),
            " ".join(item.get("tags") or []),
            str(item.get("notes") or ""),
            str(item.get("original_filename") or ""),
        ]
    ).lower()
    phrases = []
    phrases.extend(_text_variants(seg.get("material_keyword", "")))
    phrases.extend(_text_variants(seg.get("material_search_keyword", "")))
    phrases.extend(_text_variants(seg.get("material_desc", "")))
    phrases.extend(_text_variants(seg.get("script", "")))
    score = 0
    for phrase in phrases[:24]:
        if phrase and phrase in searchable:
            score += 6 if len(phrase) >= 4 else 3
    if target_market and target_market in {value.lower() for value in item.get("target_markets") or []}:
        score += 5
    if department_id and department_id in {value.lower() for value in item.get("department_ids") or []}:
        score += 5
    if seg.get("material_keyword") and str(item.get("category") or "").lower() in str(seg.get("material_keyword") or "").lower():
        score += 4
    return score


def search_material_library(
    seg: dict,
    *,
    target_market: str = "",
    department_id: str = "",
    limit_videos: int = 1,
    limit_images: int = 2,
) -> list[dict]:
    items = list_material_library_items(status="approved")
    filtered = []
    for item in items:
        markets = {value.lower() for value in item.get("target_markets") or []}
        departments = {value.lower() for value in item.get("department_ids") or []}
        if markets and target_market and target_market.lower() not in markets:
            continue
        if departments and department_id and department_id.lower() not in departments:
            continue
        score = _material_score(item, seg, target_market=target_market.lower(), department_id=department_id.lower())
        if score <= 0:
            continue
        filtered.append((score, item))
    filtered.sort(key=lambda pair: (pair[0], pair[1].get("created_at", 0)), reverse=True)
    selected = []
    used_ids = set()
    video_count = 0
    image_count = 0
    for score, item in filtered:
        if item["id"] in used_ids:
            continue
        if item.get("kind") == "video":
            if video_count >= limit_videos:
                continue
            video_count += 1
        else:
            if image_count >= limit_images:
                continue
            image_count += 1
        used_ids.add(item["id"])
        selected.append({**item, "score": score})
        if video_count >= limit_videos and image_count >= limit_images:
            break
    return selected


def copy_material_to_output(item: dict, output_dir: str, segment_index: int, item_index: int) -> str:
    source_path = (MATERIAL_LIBRARY_DIR / Path(str(item.get("filename") or "")).name).resolve()
    if not source_path.exists():
        raise FileNotFoundError("素材源文件不存在")
    suffix = source_path.suffix.lower()
    output_root = Path(output_dir) / "materials"
    output_root.mkdir(parents=True, exist_ok=True)
    filename = f"material_{segment_index:02d}_library_{item.get('kind', 'image')}_{item_index:02d}{suffix}"
    target_path = output_root / filename
    shutil.copy2(source_path, target_path)
    return str(target_path)
