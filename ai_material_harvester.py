import json
import re
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse
from xml.etree import ElementTree as ET

import requests

from material_library import register_material_file
from source_ingest import DEFAULT_HEADERS


BASE_DIR = Path(__file__).resolve().parent
HARVEST_DIR = BASE_DIR / "material_harvest"
HARVEST_DIR.mkdir(exist_ok=True)
HARVEST_JOBS_PATH = HARVEST_DIR / "jobs.json"
HARVEST_CANDIDATES_PATH = HARVEST_DIR / "candidates.json"
HARVEST_LOCK = threading.Lock()

URL_RE = re.compile(r"(https?://[^\s<>'\"）)]+)", re.IGNORECASE)
IMG_RE = re.compile(r"<img[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
SOURCE_RE = re.compile(r"<source[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
VIDEO_RE = re.compile(r"<video[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
ANCHOR_RE = re.compile(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
META_CONTENT_RE_TPL = r"<meta[^>]+(?:property|name)=['\"]{name}['\"][^>]+content=['\"]([^'\"]+)['\"]"
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm"}


def _now() -> float:
    return time.time()


def _clean_text(text: str) -> str:
    return WHITESPACE_RE.sub(" ", str(text or "").strip())


def _extract_urls(text: str) -> list[str]:
    seen = set()
    urls = []
    for match in URL_RE.findall(str(text or "")):
        url = str(match or "").rstrip(".,;，。；、】）)]")
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_jobs() -> list[dict]:
    data = _load_json(HARVEST_JOBS_PATH, {"jobs": []})
    return list(data.get("jobs") or [])


def _save_jobs(rows: list[dict]) -> None:
    _save_json(HARVEST_JOBS_PATH, {"jobs": rows})


def _load_candidates() -> list[dict]:
    data = _load_json(HARVEST_CANDIDATES_PATH, {"candidates": []})
    return list(data.get("candidates") or [])


def _save_candidates(rows: list[dict]) -> None:
    _save_json(HARVEST_CANDIDATES_PATH, {"candidates": rows})


def _normalize_job(job: dict) -> dict:
    return {
        "id": str(job.get("id") or uuid.uuid4().hex[:12]),
        "topic": _clean_text(job.get("topic") or ""),
        "source_urls": _extract_urls("\n".join(job.get("source_urls") or [])),
        "discovered_source_urls": _extract_urls("\n".join(job.get("discovered_source_urls") or [])),
        "search_notes": _clean_text(job.get("search_notes") or ""),
        "status": str(job.get("status") or "queued"),
        "message": _clean_text(job.get("message") or ""),
        "error": _clean_text(job.get("error") or ""),
        "candidate_count": int(job.get("candidate_count") or 0),
        "created_at": float(job.get("created_at") or _now()),
        "updated_at": float(job.get("updated_at") or _now()),
        "created_by_username": _clean_text(job.get("created_by_username") or ""),
        "created_by_display_name": _clean_text(job.get("created_by_display_name") or ""),
    }


def _normalize_candidate(candidate: dict) -> dict:
    asset_url = str(candidate.get("asset_url") or "").strip()
    parsed = urlparse(asset_url)
    return {
        "id": str(candidate.get("id") or uuid.uuid4().hex[:12]),
        "job_id": str(candidate.get("job_id") or "").strip(),
        "topic": _clean_text(candidate.get("topic") or ""),
        "kind": str(candidate.get("kind") or "image"),
        "title": _clean_text(candidate.get("title") or ""),
        "page_title": _clean_text(candidate.get("page_title") or ""),
        "page_excerpt": _clean_text(candidate.get("page_excerpt") or ""),
        "source_url": str(candidate.get("source_url") or "").strip(),
        "asset_url": asset_url,
        "domain": parsed.netloc.lower(),
        "status": str(candidate.get("status") or "pending"),
        "notes": _clean_text(candidate.get("notes") or ""),
        "created_at": float(candidate.get("created_at") or _now()),
        "updated_at": float(candidate.get("updated_at") or _now()),
        "imported_material_id": str(candidate.get("imported_material_id") or "").strip(),
    }


def _extract_meta(html_text: str, names: list[str]) -> str:
    for name in names:
        pattern = re.compile(META_CONTENT_RE_TPL.format(name=re.escape(name)), re.IGNORECASE | re.DOTALL)
        match = pattern.search(html_text or "")
        if match:
            return _clean_text(match.group(1))
    return ""


def _extract_page_title(html_text: str) -> str:
    title = _extract_meta(html_text, ["og:title", "twitter:title"])
    if title:
        return title
    match = TITLE_RE.search(html_text or "")
    return _clean_text(match.group(1)) if match else ""


def _extract_excerpt(html_text: str) -> str:
    excerpt = _extract_meta(html_text, ["description", "og:description", "twitter:description"])
    if excerpt:
        return excerpt
    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html_text or "", flags=re.IGNORECASE | re.DOTALL)
    for paragraph in paragraphs:
        text = _clean_text(re.sub(r"<[^>]+>", " ", paragraph))
        if len(text) >= 24:
            return text[:220]
    return ""


def _looks_like_asset(url: str) -> bool:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS


def _kind_for_asset_url(url: str) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    return "image"


def _extract_asset_urls(page_url: str, html_text: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    seen = set()

    def add(url: str, kind: str):
        normalized = urljoin(page_url, str(url or "").strip())
        if not normalized.startswith(("http://", "https://")):
            return
        if normalized in seen:
            return
        if kind == "image" and not (_looks_like_asset(normalized) or "image" in normalized or "img" in normalized):
            return
        seen.add(normalized)
        found.append((normalized, kind))

    for meta_name in ["og:image", "twitter:image", "og:image:url"]:
        value = _extract_meta(html_text, [meta_name])
        if value:
            add(value, "image")
    for meta_name in ["og:video", "twitter:player:stream"]:
        value = _extract_meta(html_text, [meta_name])
        if value:
            add(value, "video")

    for value in IMG_RE.findall(html_text or "")[:20]:
        add(value, "image")
    for value in VIDEO_RE.findall(html_text or "")[:10]:
        add(value, "video")
    for value in SOURCE_RE.findall(html_text or "")[:10]:
        add(value, _kind_for_asset_url(value))
    return found[:24]


def _discover_source_urls_from_bing_news(query: str, limit: int = 8) -> list[str]:
    if not query.strip():
        return []
    rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
    response = requests.get(rss_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    root = ET.fromstring(response.text or "")
    seen = set()
    urls = []
    for item in root.findall(".//item"):
        link = _clean_text(item.findtext("link", ""))
        if not link or link in seen:
            continue
        seen.add(link)
        urls.append(link)
        if len(urls) >= limit:
            break
    return urls


def _discover_source_urls_from_duckduckgo(query: str, limit: int = 8) -> list[str]:
    if not query.strip():
        return []
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    html_text = response.text or ""
    seen = set()
    urls = []
    for href, label in ANCHOR_RE.findall(html_text):
        url = _clean_text(urljoin(search_url, href))
        if not url.startswith(("http://", "https://")):
            continue
        host = urlparse(url).netloc.lower()
        if "duckduckgo.com" in host:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def discover_source_urls(topic: str, search_notes: str = "", limit: int = 10) -> list[str]:
    query_parts = [topic.strip(), search_notes.strip()]
    query = " ".join(part for part in query_parts if part)
    if not query:
        return []
    candidates = []
    errors = []
    for fn in (_discover_source_urls_from_bing_news, _discover_source_urls_from_duckduckgo):
        try:
            candidates.extend(fn(query, limit=limit))
        except Exception as exc:
            errors.append(str(exc))
    deduped = []
    seen = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
        if len(deduped) >= limit:
            break
    return deduped


def _fetch_candidate_rows(topic: str, source_url: str) -> list[dict]:
    response = requests.get(source_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    html_text = response.text or ""
    page_title = _extract_page_title(html_text)
    page_excerpt = _extract_excerpt(html_text)
    rows = []
    for index, (asset_url, kind) in enumerate(_extract_asset_urls(source_url, html_text), start=1):
        rows.append(
            _normalize_candidate(
                {
                    "topic": topic,
                    "kind": kind,
                    "title": page_title or f"{topic or '候选素材'} {index}",
                    "page_title": page_title,
                    "page_excerpt": page_excerpt,
                    "source_url": source_url,
                    "asset_url": asset_url,
                    "status": "pending",
                }
            )
        )
    return rows


def create_harvest_job(
    *,
    topic: str,
    source_text: str,
    search_notes: str,
    created_by_username: str,
    created_by_display_name: str,
) -> dict:
    job = _normalize_job(
        {
            "topic": topic,
            "source_urls": _extract_urls(source_text),
            "search_notes": search_notes,
            "status": "queued",
            "message": "等待开始采集",
            "created_by_username": created_by_username,
            "created_by_display_name": created_by_display_name,
        }
    )
    with HARVEST_LOCK:
        jobs = _load_jobs()
        jobs = [row for row in jobs if str(row.get("id")) != job["id"]]
        jobs.append(job)
        _save_jobs(jobs)
    return job


def _update_job(job_id: str, updates: dict) -> dict:
    with HARVEST_LOCK:
        jobs = _load_jobs()
        index = next((idx for idx, row in enumerate(jobs) if str(row.get("id")) == str(job_id)), -1)
        if index < 0:
            raise FileNotFoundError("采集任务不存在")
        merged = dict(jobs[index] or {})
        merged.update(updates or {})
        merged["updated_at"] = _now()
        normalized = _normalize_job(merged)
        jobs[index] = normalized
        _save_jobs(jobs)
    return normalized


def list_harvest_jobs() -> list[dict]:
    with HARVEST_LOCK:
        jobs = [_normalize_job(row) for row in _load_jobs()]
    return sorted(jobs, key=lambda row: row.get("created_at", 0), reverse=True)


def list_harvest_candidates(*, status: str = "", job_id: str = "") -> list[dict]:
    with HARVEST_LOCK:
        rows = [_normalize_candidate(row) for row in _load_candidates()]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    if job_id:
        rows = [row for row in rows if row.get("job_id") == job_id]
    return sorted(rows, key=lambda row: row.get("created_at", 0), reverse=True)


def _append_candidates(job_id: str, rows: list[dict]) -> list[dict]:
    normalized_rows = []
    with HARVEST_LOCK:
        candidates = _load_candidates()
        existing_urls = {str(row.get("asset_url") or "").strip() for row in candidates if str(row.get("job_id") or "") == str(job_id)}
        for row in rows:
            candidate = _normalize_candidate({**row, "job_id": job_id})
            if candidate["asset_url"] in existing_urls:
                continue
            existing_urls.add(candidate["asset_url"])
            candidates.append(candidate)
            normalized_rows.append(candidate)
        _save_candidates(candidates)
    return normalized_rows


def run_harvest_job(job_id: str) -> dict:
    job = _update_job(job_id, {"status": "running", "message": "正在抓取网页素材候选"})
    source_urls = list(job.get("source_urls") or [])
    discovered_source_urls = []
    if not source_urls:
        discovered_source_urls = discover_source_urls(job.get("topic", ""), job.get("search_notes", ""), limit=10)
        if discovered_source_urls:
            job = _update_job(
                job_id,
                {
                    "message": f"已自动发现 {len(discovered_source_urls)} 条来源，正在抓取候选素材",
                    "discovered_source_urls": discovered_source_urls,
                },
            )
            source_urls = list(discovered_source_urls)
    if not source_urls:
        return _update_job(job_id, {"status": "failed", "error": "请至少提供一条来源链接，或填写可搜索的采集主题", "message": "没有可采集的来源链接"})

    collected: list[dict] = []
    errors: list[str] = []
    for source_url in source_urls:
        try:
            collected.extend(_fetch_candidate_rows(job.get("topic", ""), source_url))
        except Exception as exc:
            errors.append(f"{source_url}: {exc}")

    added = _append_candidates(job_id, collected)
    if not added and errors:
        return _update_job(job_id, {"status": "failed", "candidate_count": 0, "error": "；".join(errors[:3]), "message": "采集失败"})
    final_message = f"已抓取 {len(added)} 条候选素材"
    if errors:
        final_message += f"，另有 {len(errors)} 条来源抓取失败"
    return _update_job(
        job_id,
        {
            "status": "done",
            "message": final_message,
            "error": "；".join(errors[:3]),
            "candidate_count": len(added),
            "discovered_source_urls": discovered_source_urls or job.get("discovered_source_urls") or [],
        },
    )


def run_harvest_job_async(job_id: str) -> None:
    worker = threading.Thread(target=run_harvest_job, args=(job_id,), daemon=True, name=f"harvest-{job_id}")
    worker.start()


def update_harvest_candidate(candidate_id: str, updates: dict) -> dict:
    with HARVEST_LOCK:
        rows = _load_candidates()
        index = next((idx for idx, row in enumerate(rows) if str(row.get("id")) == str(candidate_id)), -1)
        if index < 0:
            raise FileNotFoundError("候选素材不存在")
        merged = dict(rows[index] or {})
        merged.update(updates or {})
        merged["updated_at"] = _now()
        normalized = _normalize_candidate(merged)
        rows[index] = normalized
        _save_candidates(rows)
    return normalized


def import_harvest_candidate_to_material_library(
    candidate_id: str,
    *,
    uploader_username: str,
    uploader_display_name: str,
    category: str = "",
    notes: str = "",
) -> dict:
    candidates = list_harvest_candidates()
    candidate = next((row for row in candidates if str(row.get("id")) == str(candidate_id)), None)
    if not candidate:
        raise FileNotFoundError("候选素材不存在")
    asset_url = str(candidate.get("asset_url") or "").strip()
    if not asset_url:
        raise ValueError("候选素材没有可导入的资源链接")
    response = requests.get(asset_url, headers=DEFAULT_HEADERS, timeout=60, stream=True)
    response.raise_for_status()
    suffix = Path(urlparse(asset_url).path).suffix.lower()
    if suffix not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS:
        content_type = str(response.headers.get("content-type") or "").lower()
        if "png" in content_type:
            suffix = ".png"
        elif "webp" in content_type:
            suffix = ".webp"
        elif "jpeg" in content_type or "jpg" in content_type:
            suffix = ".jpg"
        elif "webm" in content_type:
            suffix = ".webm"
        elif "quicktime" in content_type:
            suffix = ".mov"
        elif "mp4" in content_type:
            suffix = ".mp4"
        else:
            suffix = ".jpg" if candidate.get("kind") == "image" else ".mp4"
    temp_root = HARVEST_DIR / "downloads"
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_path = temp_root / f"{uuid.uuid4().hex[:12]}{suffix}"
    with temp_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 64):
            if chunk:
                handle.write(chunk)
    # External asset URLs are often redirect links or dynamic paths without a usable suffix.
    # For crawler imports, always persist with a normalized filename derived from the
    # downloaded asset so material_library can safely accept it.
    original_name = f"harvest_{candidate_id}{suffix}"
    item = register_material_file(
        temp_path=str(temp_path),
        original_filename=original_name or f"harvest_{candidate_id}{suffix}",
        title=candidate.get("page_title") or candidate.get("title") or f"候选素材 {candidate_id}",
        category=category,
        notes=notes or candidate.get("page_excerpt") or candidate.get("notes") or "",
        uploader_username=uploader_username,
        uploader_display_name=uploader_display_name,
        source="ai_harvest_import",
    )
    update_harvest_candidate(candidate_id, {"status": "imported", "imported_material_id": item.get("id", "")})
    return item
