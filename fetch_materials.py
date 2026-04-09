"""
素材搜索模块
根据关键词自动搜索并下载 Pexels 图片/视频素材
"""

import os
import requests
from dotenv import load_dotenv

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


def fetch_all_materials(segments: list, output_dir: str) -> list:
    """
    批量搜索并下载所有素材段落的图片/视频
    """
    results = []
    
    for i, seg in enumerate(segments):
        if seg.get("type") != "material":
            results.append(seg)
            continue
        
        display_keyword = seg.get("material_keyword", "Japan")
        keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
        print(f"🔎 搜索素材：{display_keyword}｜检索词：{keyword}")
        
        seg_with_materials = seg.copy()
        
        material_items = []
        material_paths = []

        # 优先抓一条视频素材，成片时可直接裁成对应段落长度
        try:
            videos = search_videos(keyword, count=1)
            for j, video in enumerate(videos):
                ext = "mp4"
                filename = f"material_{i:02d}_video_{j}.{ext}"
                output_path = os.path.join(output_dir, "materials", filename)
                download_file(video["url"], output_path)
                material_paths.append(output_path)
                material_items.append(_material_entry(output_path, kind="video"))
                print(f"  ✅ 视频已下载：{filename}")
        except Exception as e:
            print(f"  ⚠️ 视频素材搜索失败：{e}")

        # 再补图片素材，供人工替换或成片兜底
        try:
            photos = search_photos(keyword, count=2)
            for j, photo in enumerate(photos):
                ext = "jpg"
                filename = f"material_{i:02d}_photo_{j}.{ext}"
                output_path = os.path.join(output_dir, "materials", filename)
                download_file(photo["url"], output_path)
                material_paths.append(output_path)
                material_items.append(_material_entry(output_path, kind="image"))
                print(f"  ✅ 图片已下载：{filename}")
        except Exception as e:
            print(f"  ⚠️ 图片素材搜索失败：{e}")

        seg_with_materials["material_paths"] = material_paths
        seg_with_materials["material_items"] = material_items

        results.append(seg_with_materials)
    
    return results
