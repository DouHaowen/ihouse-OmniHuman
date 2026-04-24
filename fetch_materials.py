"""
素材搜索模块
根据关键词自动搜索并下载 Pexels 图片/视频素材
"""

import os
import requests
from dotenv import load_dotenv
from material_library import copy_material_to_output, search_material_library

load_dotenv(override=False)

PEXELS_API_KEY = os.getenv("PEXELS_API_KEY")
PEXELS_API_URL = "https://api.pexels.com/v1/search"
PEXELS_VIDEO_URL = "https://api.pexels.com/videos/search"


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


def fetch_materials_for_segment(seg: dict, output_dir: str, segment_index: int, *, target_market: str = "", department_id: str = "") -> dict:
    seg_with_materials = seg.copy()
    display_keyword = seg.get("material_keyword", "Japan")
    keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
    print(f"🔎 搜索素材：{display_keyword}｜检索词：{keyword}")

    material_items = []
    material_paths = []

    library_items = search_material_library(
        seg,
        target_market=target_market or str(seg.get("target_market") or ""),
        department_id=department_id or str(seg.get("department_id") or ""),
        limit_videos=1,
        limit_images=2,
    )
    library_video_count = 0
    library_image_count = 0
    for item in library_items:
        copied_path = copy_material_to_output(item, output_dir, segment_index, len(material_items))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=item.get("kind"), source="library")
        entry["library_id"] = item.get("id", "")
        entry["title"] = item.get("title", "")
        material_items.append(entry)
        if item.get("kind") == "video":
            library_video_count += 1
        else:
            library_image_count += 1
        print(f"  ✅ 已命中本地素材库：{os.path.basename(copied_path)}")

    if library_video_count < 1:
        try:
            videos = search_videos(keyword, count=1 - library_video_count)
            for j, video in enumerate(videos):
                filename = f"material_{segment_index:02d}_video_{j}.mp4"
                output_path = os.path.join(output_dir, "materials", filename)
                download_file(video["url"], output_path)
                material_paths.append(output_path)
                material_items.append(_material_entry(output_path, kind="video"))
                print(f"  ✅ 视频已下载：{filename}")
        except Exception as e:
            print(f"  ⚠️ 视频素材搜索失败：{e}")

    if library_image_count < 2:
        try:
            photos = search_photos(keyword, count=2 - library_image_count)
            for j, photo in enumerate(photos):
                filename = f"material_{segment_index:02d}_photo_{j}.jpg"
                output_path = os.path.join(output_dir, "materials", filename)
                download_file(photo["url"], output_path)
                material_paths.append(output_path)
                material_items.append(_material_entry(output_path, kind="image"))
                print(f"  ✅ 图片已下载：{filename}")
        except Exception as e:
            print(f"  ⚠️ 图片素材搜索失败：{e}")

    seg_with_materials["material_paths"] = material_paths
    seg_with_materials["material_items"] = material_items
    return seg_with_materials


def fetch_all_materials(segments: list, output_dir: str) -> list:
    """
    批量搜索并下载所有素材段落的图片/视频
    """
    results = []

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
            )
        )

    return results
