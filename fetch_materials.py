"""
素材搜索模块
根据关键词自动搜索并下载Pexels免费素材图片
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

PEXELS_API_KEY = os.getenv("PEXELS_API_KEY")
PEXELS_API_URL = "https://api.pexels.com/v1/search"
PEXELS_VIDEO_URL = "https://api.pexels.com/videos/search"


def search_photos(keyword: str, count: int = 3) -> list:
    """
    搜索图片素材
    返回图片URL列表
    """
    headers = {"Authorization": PEXELS_API_KEY}
    params = {
        "query": keyword,
        "per_page": count,
        "orientation": "landscape",
    }
    
    response = requests.get(PEXELS_API_URL, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    photos = data.get("photos", [])
    
    return [
        {
            "url": p["src"]["large"],
            "photographer": p["photographer"],
            "alt": p.get("alt", keyword),
        }
        for p in photos
    ]


def search_videos(keyword: str, count: int = 2) -> list:
    """
    搜索视频素材
    """
    headers = {"Authorization": PEXELS_API_KEY}
    params = {
        "query": keyword,
        "per_page": count,
        "orientation": "landscape",
    }
    
    response = requests.get(PEXELS_VIDEO_URL, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    videos = data.get("videos", [])
    
    results = []
    for v in videos:
        # 选择最合适的分辨率（优先720p）
        files = sorted(v.get("video_files", []), key=lambda x: x.get("height", 0))
        best_file = next(
            (f for f in files if f.get("height", 0) >= 720),
            files[-1] if files else None
        )
        
        if best_file:
            results.append({
                "url": best_file["link"],
                "width": best_file.get("width"),
                "height": best_file.get("height"),
            })
    
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
        
        # 搜索图片
        try:
            photos = search_photos(keyword, count=2)
            material_paths = []
            
            for j, photo in enumerate(photos):
                ext = "jpg"
                filename = f"material_{i:02d}_photo_{j}.{ext}"
                output_path = os.path.join(output_dir, "materials", filename)
                
                download_file(photo["url"], output_path)
                material_paths.append(output_path)
                print(f"  ✅ 图片已下载：{filename}")
            
            seg_with_materials["material_paths"] = material_paths
        
        except Exception as e:
            print(f"  ⚠️ 素材搜索失败：{e}")
            seg_with_materials["material_paths"] = []
        
        results.append(seg_with_materials)
    
    return results
