"""
素材搜索模块
根据关键词自动搜索并下载 Pexels 图片/视频素材
"""

import os
import re
import hashlib
from urllib.parse import urlparse
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


def _extension_from_url_or_content_type(url: str, content_type: str = "", fallback: str = ".jpg") -> str:
    suffix = os.path.splitext(urlparse(str(url or "")).path)[1].lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v", ".webm"}:
        return suffix
    content_type = (content_type or "").lower()
    if "mp4" in content_type:
        return ".mp4"
    if "quicktime" in content_type:
        return ".mov"
    if "webm" in content_type:
        return ".webm"
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    if "jpeg" in content_type or "jpg" in content_type:
        return ".jpg"
    return fallback


def _source_material_url_variants(url: str) -> list[str]:
    """Try larger variants for news thumbnail URLs before giving up."""
    url = str(url or "").strip()
    variants: list[str] = []

    def add(candidate: str) -> None:
        if candidate and candidate not in variants:
            variants.append(candidate)

    # VOA/RFA-style GDB images often expose tiny thumbnails as `_w100_`.
    # The same asset usually has larger `_w650_`, `_w1023_`, `_w1200_` variants.
    if "gdb.voanews.com" in url.lower() or "gdb.rferl.org" in url.lower():
        for width in (1200, 1023, 800, 650, 480):
            add(re.sub(r"_w\d+_", f"_w{width}_", url))
        for width in (1200, 1023, 800, 650, 480):
            add(re.sub(r"_w\d+(_r\d+)", f"_w{width}\\1", url))
    add(url)
    return variants


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_source_material(url: str, output_dir: str, segment_index: int, material_index: int, kind: str = "") -> str:
    os.makedirs(os.path.join(output_dir, "materials"), exist_ok=True)
    last_error = ""
    for download_url in _source_material_url_variants(url):
        try:
            response = requests.get(download_url, stream=True, timeout=25, headers={"User-Agent": "iHouse-OpenNews-Media/0.1"})
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            guessed_kind = kind or ("video" if "video" in content_type.lower() or re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", download_url, flags=re.I) else "image")
            lowered_content_type = content_type.lower()
            if "text/html" in lowered_content_type:
                raise RuntimeError("来源链接返回 HTML 页面，不是可下载素材")
            if guessed_kind == "image" and lowered_content_type and not any(token in lowered_content_type for token in ("image", "octet-stream")):
                raise RuntimeError(f"来源链接不是图片素材：{content_type}")
            if guessed_kind == "video" and lowered_content_type and not any(token in lowered_content_type for token in ("video", "octet-stream", "binary")):
                raise RuntimeError(f"来源链接不是视频素材：{content_type}")
            ext = _extension_from_url_or_content_type(download_url, content_type, ".mp4" if guessed_kind == "video" else ".jpg")
            output_path = os.path.join(output_dir, "materials", f"material_{segment_index:02d}_source_{material_index}{ext}")
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            file_size = os.path.getsize(output_path)
            if guessed_kind == "video" and file_size < 200 * 1024:
                os.remove(output_path)
                raise RuntimeError("视频素材文件过小，已跳过")
            if guessed_kind != "video" and file_size < 25 * 1024:
                os.remove(output_path)
                raise RuntimeError("图片素材文件过小，已跳过")
            if download_url != url:
                print(f"  ↗️ 已将新闻缩略图升级为高清素材：{os.path.basename(urlparse(download_url).path)}")
            return output_path
        except Exception as exc:
            last_error = str(exc)
            continue
    raise RuntimeError(last_error or "新闻来源素材下载失败")


SOURCE_MATERIAL_BAD_TOKENS = (
    "favicon",
    "apple-touch-icon",
    "sprite",
    "/icons/",
    "/icon/",
    "logo",
    "avatar",
    "author",
    "profile",
    "social",
    "share",
    "tracking",
    "pixel",
    "spacer",
    "blank",
    "placeholder",
    "advert",
    "/ads/",
    "banner-ad",
)


def _normalized_media_basename(path: str) -> str:
    name = os.path.basename((path or "").lower())
    if not name:
        return ""
    stem, ext = os.path.splitext(name)
    stem = re.sub(r"[-_@](?:\d{2,5}x\d{2,5}|\d{2,5}w|large|medium|small|thumb|thumbnail|preview|orig|original)$", "", stem)
    stem = re.sub(r"(?:[-_](?:copy|scaled|resize|crop|web|mobile))+$", "", stem)
    return f"{stem}{ext}" if stem and ext else name


def _source_url_key(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path.lower()}"


def _source_identity_keys(url: str) -> set[str]:
    parsed = urlparse(str(url or ""))
    base = _source_url_key(url)
    basename = _normalized_media_basename(parsed.path)
    keys = {base} if base else set()
    if parsed.netloc and basename:
        keys.add(f"basename:{parsed.netloc.lower()}:{basename}")
    return keys


def _looks_like_bad_source_material(item: dict) -> bool:
    url = str(item.get("url") or "")
    title = str(item.get("title") or "")
    text = f"{url} {title}".lower()
    return any(token in text for token in SOURCE_MATERIAL_BAD_TOKENS)


def _rank_source_material(item: dict) -> int:
    kind = str(item.get("kind") or "").lower()
    title = str(item.get("title") or "").lower()
    url = str(item.get("url") or "").lower()
    score = 100 if kind == "video" else 0
    if "opengraph" in title:
        score += 20
    if "article" in title:
        score += 15
    if "linked video" in title or re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", url):
        score += 30
    return score


def _theme_balanced_source_materials(items: list[dict]) -> list[dict]:
    """Keep OpenNews visuals aligned with script themes instead of one global pool."""
    groups: dict[int, list[dict]] = {}
    unthemed_index = 9999
    for item in items:
        try:
            theme_index = int(item.get("theme_index"))
        except Exception:
            theme_index = unthemed_index
            unthemed_index += 1
        groups.setdefault(theme_index, []).append(item)

    for theme_index, group in list(groups.items()):
        groups[theme_index] = sorted(group, key=_rank_source_material, reverse=True)

    ordered: list[dict] = []
    theme_indexes = sorted(groups)
    cursors = {theme_index: 0 for theme_index in theme_indexes}
    while True:
        changed = False
        for theme_index in theme_indexes:
            group = groups[theme_index]
            cursor = cursors[theme_index]
            if cursor >= len(group):
                continue
            ordered.append(group[cursor])
            cursors[theme_index] = cursor + 1
            changed = True
        if not changed:
            break
    return ordered


def _opennews_theme_queries(seg: dict) -> list[str]:
    queries: list[str] = []
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        for query in theme.get("queries") or []:
            query = re.sub(r"\s+", " ", str(query or "")).strip()
            if query and query.lower() not in {item.lower() for item in queries}:
                queries.append(query)
    return queries


def _tokenize_opennews_relevance(text: str) -> set[str]:
    text = (text or "").lower()
    aliases = {
        "马斯克": "elon musk",
        "万亿富翁": "trillionaire",
        "上市": "ipo",
        "人工智能": "artificial intelligence",
        "半导体": "semiconductor",
        "芯片": "chip",
        "英伟达": "nvidia",
        "微软": "microsoft",
        "谷歌": "google",
        "苹果": "apple",
        "亚马逊": "amazon",
    }
    for source, replacement in aliases.items():
        text = text.replace(source, f" {replacement} ")
    tokens = set(re.findall(r"[a-z0-9][a-z0-9.+-]{2,}", text))
    phrases = {
        "white house", "press briefing", "elon musk", "spacex", "meta", "meta ai",
        "facebook", "ipo", "trillionaire", "nvidia", "openai", "microsoft",
        "google", "alphabet", "semiconductor", "artificial intelligence",
        "biotechnology", "stock market", "investors", "ukraine", "russia",
        "taiwan strait", "drone", "missile",
    }
    for phrase in phrases:
        if phrase in text:
            tokens.add(phrase)
    generic = {
        "news", "latest", "image", "photo", "video", "official", "press", "media",
        "article", "source", "related", "public", "content", "government", "meeting",
        "briefing", "company", "market", "tools", "tool",
    }
    return {token for token in tokens if token not in generic and len(token) >= 3}


def _opennews_relevance_tokens(seg: dict) -> set[str]:
    parts = [
        str(seg.get("material_keyword") or ""),
        str(seg.get("material_search_keyword") or ""),
        str(seg.get("script") or "")[:900],
    ]
    for theme in seg.get("material_theme_plan") or []:
        if not isinstance(theme, dict):
            continue
        parts.append(str(theme.get("title") or ""))
        parts.append(str(theme.get("visual_need") or ""))
        parts.append(str(theme.get("script") or ""))
        parts.extend(str(query or "") for query in theme.get("queries") or [])
    return _tokenize_opennews_relevance(" ".join(parts))


def _source_material_relevance_score(item: dict, relevance_tokens: set[str]) -> int:
    if not relevance_tokens:
        return 1
    haystack = " ".join(
        str(item.get(field) or "")
        for field in ("title", "url", "source_url", "related_query", "theme_title")
    )
    item_tokens = _tokenize_opennews_relevance(haystack)
    if not item_tokens:
        return 0
    overlap = relevance_tokens & item_tokens
    score = len(overlap) * 10
    haystack_lower = haystack.lower()
    for token in relevance_tokens:
        if " " in token and token in haystack_lower:
            score += 18
    return score


def fetch_materials_for_segment(
    seg: dict,
    output_dir: str,
    segment_index: int,
    *,
    target_market: str = "",
    department_id: str = "",
    used_source_urls: set[str] | None = None,
    used_source_hashes: set[str] | None = None,
    used_library_ids: set[str] | None = None,
) -> dict:
    seg_with_materials = seg.copy()
    display_keyword = seg.get("material_keyword", "Japan")
    keyword = seg.get("material_search_keyword") or display_keyword or "Japan"
    print(f"🔎 搜索素材：{display_keyword}｜检索词：{keyword}")

    material_items = []
    material_paths = []
    is_opennews_material_only = bool(seg.get("opennews_material_only") or seg.get("disable_free_material_fallback"))
    max_source_videos = 14 if is_opennews_material_only else 1
    max_source_images = 28 if is_opennews_material_only else 2
    used_source_urls = used_source_urls if used_source_urls is not None else set()
    used_source_hashes = used_source_hashes if used_source_hashes is not None else set()
    used_library_ids = used_library_ids if used_library_ids is not None else set()
    seen_source_urls: set[str] = set()
    source_materials = []
    relevance_tokens = _opennews_relevance_tokens(seg) if is_opennews_material_only and seg.get("strict_news_media_only") else set()
    for item in (seg.get("source_materials") or []):
        if not isinstance(item, dict) or not item.get("url"):
            continue
        identity_keys = _source_identity_keys(str(item.get("url") or ""))
        if not identity_keys or identity_keys & seen_source_urls or identity_keys & used_source_urls:
            continue
        if _looks_like_bad_source_material(item):
            continue
        if relevance_tokens and _source_material_relevance_score(item, relevance_tokens) <= 0:
            print(f"  ⚠️ 新闻素材相关性不足，已跳过：{item.get('title') or item.get('url')}")
            continue
        seen_source_urls.update(identity_keys)
        source_materials.append(item)
    if is_opennews_material_only:
        source_materials = _theme_balanced_source_materials(source_materials)
    else:
        source_materials.sort(key=_rank_source_material, reverse=True)
    source_video_count = 0
    source_image_count = 0
    source_attempt_limit = 260 if is_opennews_material_only else 24
    for item in source_materials[:source_attempt_limit]:
        if source_video_count >= max_source_videos and source_image_count >= max_source_images:
            break
        kind = str(item.get("kind") or "").strip().lower()
        if kind == "video" and source_video_count >= max_source_videos:
            continue
        if kind != "video" and source_image_count >= max_source_images:
            continue
        try:
            copied_path = _download_source_material(str(item.get("url") or ""), output_dir, segment_index, len(material_items), kind=kind)
        except Exception as exc:
            print(f"  ⚠️ 新闻来源素材下载失败：{item.get('url')}｜{exc}")
            continue
        try:
            content_hash = _file_sha256(copied_path)
        except Exception:
            content_hash = ""
        if content_hash and content_hash in used_source_hashes:
            try:
                os.remove(copied_path)
            except Exception:
                pass
            print(f"  ⚠️ 新闻来源素材内容重复，已跳过：{item.get('url')}")
            continue
        if content_hash:
            used_source_hashes.add(content_hash)
        used_source_urls.update(_source_identity_keys(str(item.get("url") or "")))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=kind or _asset_kind_for_suffix(copied_path), source="opennews_source")
        entry["source_url"] = item.get("source_url") or item.get("url")
        entry["title"] = item.get("title", "")
        if item.get("theme_index") is not None:
            entry["theme_index"] = item.get("theme_index")
        if item.get("theme_title"):
            entry["theme_title"] = item.get("theme_title")
        if item.get("related_query"):
            entry["related_query"] = item.get("related_query")
        material_items.append(entry)
        if entry["kind"] == "video":
            source_video_count += 1
        else:
            source_image_count += 1
        print(f"  ✅ 已下载新闻来源素材：{os.path.basename(copied_path)}")

    library_items = search_material_library(
        seg,
        target_market=target_market or str(seg.get("target_market") or ""),
        department_id=department_id or str(seg.get("department_id") or ""),
        limit_videos=max(0, max_source_videos + 2 - source_video_count),
        limit_images=max(0, max_source_images + 4 - source_image_count),
    )
    library_video_count = source_video_count
    library_image_count = source_image_count
    for item in library_items:
        item_kind = str(item.get("kind") or "").lower()
        if item_kind == "video" and library_video_count >= max_source_videos:
            continue
        if item_kind != "video" and library_image_count >= max_source_images:
            continue
        library_key = str(item.get("id") or item.get("path") or item.get("filename") or "")
        if library_key and library_key in used_library_ids:
            continue
        copied_path = copy_material_to_output(item, output_dir, segment_index, len(material_items))
        material_paths.append(copied_path)
        entry = _material_entry(copied_path, kind=item.get("kind"), source="library")
        entry["library_id"] = item.get("id", "")
        entry["title"] = item.get("title", "")
        material_items.append(entry)
        if library_key:
            used_library_ids.add(library_key)
        if item_kind == "video":
            library_video_count += 1
        else:
            library_image_count += 1
        print(f"  ✅ 已命中本地素材库：{os.path.basename(copied_path)}")

    disable_free_fallback = bool(seg.get("disable_free_material_fallback"))
    allow_opennews_quality_fallback = False
    if is_opennews_material_only:
        print("  ℹ️ OpenNews 已禁用免费素材库兜底，仅使用新闻源、公开网页爬取和本地素材库")

    fallback_queries = _opennews_theme_queries(seg) if is_opennews_material_only else []
    if keyword and keyword not in fallback_queries:
        fallback_queries.append(keyword)

    if library_video_count < max_source_videos and (not disable_free_fallback or allow_opennews_quality_fallback):
        try:
            video_download_index = 0
            for query in fallback_queries or [keyword]:
                if library_video_count >= max_source_videos:
                    break
                videos = search_videos(query, count=max(1, max_source_videos - library_video_count))
                for video in videos:
                    if library_video_count >= max_source_videos:
                        break
                    video_key = _source_url_key(str(video.get("url") or ""))
                    if video_key and video_key in used_source_urls:
                        continue
                    filename = f"material_{segment_index:02d}_video_{video_download_index}.mp4"
                    video_download_index += 1
                    output_path = os.path.join(output_dir, "materials", filename)
                    download_file(video["url"], output_path)
                    if video_key:
                        used_source_urls.add(video_key)
                    material_paths.append(output_path)
                    entry = _material_entry(output_path, kind="video", source="pexels")
                    entry["title"] = query
                    material_items.append(entry)
                    library_video_count += 1
                    print(f"  ✅ 视频已下载：{filename}｜{query}")
        except Exception as e:
            print(f"  ⚠️ 视频素材搜索失败：{e}")

    if library_image_count < max_source_images and (not disable_free_fallback or allow_opennews_quality_fallback):
        try:
            photo_download_index = 0
            for query in fallback_queries or [keyword]:
                if library_image_count >= max_source_images:
                    break
                photos = search_photos(query, count=max(1, max_source_images - library_image_count))
                for photo in photos:
                    if library_image_count >= max_source_images:
                        break
                    photo_key = _source_url_key(str(photo.get("url") or ""))
                    if photo_key and photo_key in used_source_urls:
                        continue
                    filename = f"material_{segment_index:02d}_photo_{photo_download_index}.jpg"
                    photo_download_index += 1
                    output_path = os.path.join(output_dir, "materials", filename)
                    download_file(photo["url"], output_path)
                    if photo_key:
                        used_source_urls.add(photo_key)
                    material_paths.append(output_path)
                    entry = _material_entry(output_path, kind="image", source="pexels")
                    entry["title"] = query
                    material_items.append(entry)
                    library_image_count += 1
                    print(f"  ✅ 图片已下载：{filename}｜{query}")
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
    used_source_urls: set[str] = set()
    used_source_hashes: set[str] = set()
    used_library_ids: set[str] = set()

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
                used_source_urls=used_source_urls,
                used_source_hashes=used_source_hashes,
                used_library_ids=used_library_ids,
            )
        )

    return results
