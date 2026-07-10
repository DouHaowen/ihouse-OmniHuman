import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import fcntl

from dotenv import load_dotenv

load_dotenv(override=False)


class XBrowserPublishError(RuntimeError):
    pass


X_BROWSER_STATE_DIR = Path(os.getenv("X_BROWSER_STATE_DIR", "output/x_browser")).resolve()
X_BROWSER_USER_DATA_DIR = Path(os.getenv("X_BROWSER_USER_DATA_DIR", str(X_BROWSER_STATE_DIR / "profile"))).resolve()
X_BROWSER_SCREENSHOT_DIR = Path(os.getenv("X_BROWSER_SCREENSHOT_DIR", str(X_BROWSER_STATE_DIR / "screenshots"))).resolve()
# X 会把无头(headless)浏览器卡在启动 logo 画面，SPA 不加载 → 发帖框永远找不到、发布失败。
# 因此默认改为「有头 + 虚拟显示(Xvfb)」运行，行为与真实浏览器一致；可用 X_BROWSER_HEADLESS=1 强制无头。
X_BROWSER_HEADLESS = (os.getenv("X_BROWSER_HEADLESS", "0") or "0").strip().lower() not in {"0", "false", "no", "off"}
X_BROWSER_DISPLAY = (os.getenv("X_BROWSER_DISPLAY", ":98") or ":98").strip()
X_BROWSER_SCREEN = (os.getenv("X_BROWSER_SCREEN", "1365x900x24") or "1365x900x24").strip()
X_BROWSER_SLOW_MO_MS = max(0, int(os.getenv("X_BROWSER_SLOW_MO_MS", "0") or "0"))
X_BROWSER_UPLOAD_TIMEOUT_SECONDS = max(60, int(os.getenv("X_BROWSER_UPLOAD_TIMEOUT_SECONDS", "900") or "900"))
X_BROWSER_POST_TIMEOUT_SECONDS = max(20, int(os.getenv("X_BROWSER_POST_TIMEOUT_SECONDS", "120") or "120"))
X_BROWSER_POST_SETTLE_SECONDS = max(5, int(os.getenv("X_BROWSER_POST_SETTLE_SECONDS", "15") or "15"))
X_BROWSER_COMPOSE_URL = os.getenv("X_BROWSER_COMPOSE_URL", "https://x.com/compose/post").strip() or "https://x.com/compose/post"
X_BROWSER_EXECUTABLE_PATH = os.getenv("X_BROWSER_EXECUTABLE_PATH", "").strip()
X_BROWSER_DEBUG_DIR = Path(os.getenv("X_BROWSER_DEBUG_DIR", str(X_BROWSER_STATE_DIR / "debug"))).resolve()
X_BROWSER_PREPARED_VIDEO_DIR = Path(
    os.getenv("X_BROWSER_PREPARED_VIDEO_DIR", str(X_BROWSER_STATE_DIR / "prepared_videos"))
).resolve()
X_BROWSER_PUBLISH_STATE_PATH = Path(
    os.getenv("X_BROWSER_PUBLISH_STATE_PATH", str(X_BROWSER_STATE_DIR / "publish_state.json"))
).resolve()
X_BROWSER_TRANSCODE_UPLOAD = (os.getenv("X_BROWSER_TRANSCODE_UPLOAD", "1") or "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
X_BROWSER_TRANSCODE_TIMEOUT_SECONDS = max(
    120,
    int(os.getenv("X_BROWSER_TRANSCODE_TIMEOUT_SECONDS", "900") or "900"),
)
X_BROWSER_MIN_POST_INTERVAL_SECONDS = max(
    0,
    int(os.getenv("X_BROWSER_MIN_POST_INTERVAL_SECONDS", "90") or "90"),
)
X_BROWSER_MEDIA_UI_RETRIES = max(0, int(os.getenv("X_BROWSER_MEDIA_UI_RETRIES", "2") or "2"))
X_BROWSER_MEDIA_FULL_RETRIES = max(0, int(os.getenv("X_BROWSER_MEDIA_FULL_RETRIES", "1") or "1"))
X_BROWSER_MEDIA_RETRY_DELAY_SECONDS = max(
    5,
    int(os.getenv("X_BROWSER_MEDIA_RETRY_DELAY_SECONDS", "30") or "30"),
)
X_BROWSER_MEDIA_READY_SETTLE_SECONDS = max(
    5,
    int(os.getenv("X_BROWSER_MEDIA_READY_SETTLE_SECONDS", "12") or "12"),
)
X_BROWSER_PUBLISH_STATE_LOCK = threading.RLock()


def _xvfb_running(display: str) -> bool:
    """扫描 /proc 判断是否真有 Xvfb 进程在服务该 display（不依赖可能残留的 socket 文件）。"""
    want = display.encode()
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as fh:
                    parts = fh.read().split(b"\x00")
            except Exception:
                continue
            if parts and parts[0].split(b"/")[-1] == b"Xvfb" and want in parts:
                return True
    except Exception:
        pass
    return False


def _ensure_publish_display() -> str:
    """确保有一个真正在运行的虚拟显示(Xvfb)供有头发布使用。返回 DISPLAY，失败返回空串。
    以“是否存在 Xvfb 进程”为准——重启后可能残留僵尸 socket 文件，只看文件会误判。"""
    display = X_BROWSER_DISPLAY
    if not display:
        return ""
    if _xvfb_running(display):
        return display
    xvfb = shutil.which("Xvfb")
    if not xvfb:
        return ""
    num = display.lstrip(":").split(".")[0]
    # 清理僵尸 lock / socket，否则 Xvfb 会因“display 被占用”启动即退出。
    for stale in (Path(f"/tmp/.X{num}-lock"), Path(f"/tmp/.X11-unix/X{num}")):
        try:
            stale.unlink()
        except Exception:
            pass
    try:
        proc = subprocess.Popen(
            [xvfb, display, "-screen", "0", X_BROWSER_SCREEN, "-ac", "+extension", "RANDR"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        return ""
    socket_path = Path(f"/tmp/.X11-unix/X{num}")
    for _ in range(40):  # 最多等约 8 秒
        if socket_path.exists() and _xvfb_running(display):
            return display
        if proc.poll() is not None:  # Xvfb 启动即退出
            return ""
        time.sleep(0.2)
    return ""


def _resolve_browser_executable() -> str:
    # Debian 12 的系统 Chromium 带 H.264 支持；Playwright 自带的 ARM64 Chromium
    # 缺少 H.264，X 会在选中 MP4 后直接报媒体加载失败。
    for candidate in (X_BROWSER_EXECUTABLE_PATH, "/usr/bin/chromium"):
        if not candidate:
            continue
        try:
            path = Path(candidate)
            if path.is_file():
                return str(path)
        except Exception:
            continue
    return ""


def x_browser_env_config() -> dict[str, Any]:
    return {
        "state_dir": str(X_BROWSER_STATE_DIR),
        "user_data_dir": str(X_BROWSER_USER_DATA_DIR),
        "screenshot_dir": str(X_BROWSER_SCREENSHOT_DIR),
        "headless": X_BROWSER_HEADLESS,
        "executable_path": _resolve_browser_executable(),
        "compose_url": X_BROWSER_COMPOSE_URL,
        "upload_timeout_seconds": X_BROWSER_UPLOAD_TIMEOUT_SECONDS,
        "post_timeout_seconds": X_BROWSER_POST_TIMEOUT_SECONDS,
        "post_settle_seconds": X_BROWSER_POST_SETTLE_SECONDS,
        "prepared_video_dir": str(X_BROWSER_PREPARED_VIDEO_DIR),
        "transcode_upload": X_BROWSER_TRANSCODE_UPLOAD,
        "min_post_interval_seconds": X_BROWSER_MIN_POST_INTERVAL_SECONDS,
        "media_ui_retries": X_BROWSER_MEDIA_UI_RETRIES,
        "media_full_retries": X_BROWSER_MEDIA_FULL_RETRIES,
        "media_ready_settle_seconds": X_BROWSER_MEDIA_READY_SETTLE_SECONDS,
    }


def x_browser_auth_ready() -> bool:
    if not X_BROWSER_USER_DATA_DIR.exists() or not X_BROWSER_USER_DATA_DIR.is_dir():
        return False
    try:
        for child in X_BROWSER_USER_DATA_DIR.iterdir():
            if child.name.startswith("."):
                continue
            return True
    except Exception:
        return False
    return False


def x_browser_profile_dir() -> Path:
    X_BROWSER_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return X_BROWSER_USER_DATA_DIR


def _import_playwright():
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise XBrowserPublishError(
            "服务器未安装 Playwright/Chromium，不能使用 X 浏览器自动发布；请先更新镜像或安装依赖"
        ) from exc
    return sync_playwright, PlaywrightTimeoutError


def _ensure_video_path(video_path: Path) -> Path:
    path = Path(video_path)
    if not path.is_file():
        raise XBrowserPublishError(f"X 浏览器发布失败：视频文件不存在：{path}")
    return path.resolve()


def _prepared_video_cache_path(video_path: Path) -> Path:
    stat = video_path.stat()
    raw = f"h264-mp4-v2:{video_path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return X_BROWSER_PREPARED_VIDEO_DIR / f"x_{digest}.mp4"


def _cleanup_prepared_video_cache(*, keep: int = 30) -> None:
    try:
        files = sorted(
            X_BROWSER_PREPARED_VIDEO_DIR.glob("x_*.mp4"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        return
    for path in files[max(1, int(keep or 30)) :]:
        try:
            path.unlink()
        except Exception:
            pass


def prepare_x_video_for_upload(video_path: Path) -> Path:
    """Normalize media for X web upload and cache it by source file identity."""
    source = _ensure_video_path(video_path)
    if not X_BROWSER_TRANSCODE_UPLOAD:
        return source
    X_BROWSER_PREPARED_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    output = _prepared_video_cache_path(source)
    if output.exists() and output.stat().st_size > 0:
        return output
    tmp_path = output.with_suffix(f".{threading.get_ident()}.part.mp4")
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(source),
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-vf",
        "scale=trunc(iw/2)*2:trunc(ih/2)*2:in_range=auto:out_range=tv,format=yuv420p,fps=30",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "22",
        "-profile:v",
        "high",
        "-level:v",
        "4.1",
        "-pix_fmt",
        "yuv420p",
        "-color_range",
        "tv",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        "-max_muxing_queue_size",
        "1024",
        str(tmp_path),
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=X_BROWSER_TRANSCODE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        raise XBrowserPublishError(f"X 上传兼容转码失败：{exc}") from exc
    if completed.returncode != 0 or not tmp_path.exists() or tmp_path.stat().st_size <= 0:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        error = (completed.stderr or completed.stdout or "ffmpeg failed").strip()[-1000:]
        raise XBrowserPublishError(f"X 上传兼容转码失败：{error}")
    tmp_path.replace(output)
    _cleanup_prepared_video_cache()
    return output


def _profile_publish_key(profile_dir: Path) -> str:
    return hashlib.sha256(str(profile_dir.resolve()).encode("utf-8")).hexdigest()[:24]


def _profile_publish_lock_path(profile_dir: Path) -> Path:
    lock_dir = X_BROWSER_STATE_DIR / "publish_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / f"{_profile_publish_key(profile_dir)}.lock"


@contextmanager
def _exclusive_profile_publish_lock(profile_dir: Path):
    lock_path = _profile_publish_lock_path(profile_dir)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps({"pid": os.getpid(), "acquired_at": time.time()}))
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _load_x_browser_publish_state() -> dict:
    with X_BROWSER_PUBLISH_STATE_LOCK:
        try:
            payload = json.loads(X_BROWSER_PUBLISH_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {"profiles": {}}
        if not isinstance(payload, dict):
            return {"profiles": {}}
        if not isinstance(payload.get("profiles"), dict):
            payload["profiles"] = {}
        return payload


def _save_x_browser_publish_state(state: dict) -> None:
    with X_BROWSER_PUBLISH_STATE_LOCK:
        X_BROWSER_PUBLISH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = X_BROWSER_PUBLISH_STATE_PATH.with_name(
            f".{X_BROWSER_PUBLISH_STATE_PATH.name}.{threading.get_ident()}.tmp"
        )
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(X_BROWSER_PUBLISH_STATE_PATH)


def _wait_for_x_browser_publish_slot(profile_dir: Path) -> None:
    if X_BROWSER_MIN_POST_INTERVAL_SECONDS <= 0:
        return
    state = _load_x_browser_publish_state()
    profile_state = (state.get("profiles") or {}).get(_profile_publish_key(profile_dir)) or {}
    try:
        last_success_at = float(profile_state.get("last_success_at") or 0)
    except Exception:
        last_success_at = 0
    wait_seconds = X_BROWSER_MIN_POST_INTERVAL_SECONDS - (time.time() - last_success_at)
    if wait_seconds > 0:
        print(f"[X browser] waiting {int(wait_seconds)}s for the account publish interval", flush=True)
        time.sleep(wait_seconds)


def _record_x_browser_publish_success(profile_dir: Path, result: dict) -> None:
    state = _load_x_browser_publish_state()
    profiles = state.setdefault("profiles", {})
    profiles[_profile_publish_key(profile_dir)] = {
        "profile_dir": str(profile_dir),
        "last_success_at": time.time(),
        "post_id": str(result.get("post_id") or ""),
        "x_url": str(result.get("x_url") or ""),
    }
    state["updated_at"] = time.time()
    _save_x_browser_publish_state(state)


def _is_retryable_media_failure(error: Exception | str) -> bool:
    text = str(error or "").lower()
    return any(
        marker in text
        for marker in (
            "media failed",
            "upload failed",
            "failed to load",
            "媒体上传失败",
            "视频上传失败",
        )
    )


def _tweet_id_from_url(url: str) -> str:
    match = re.search(r"/status/(\d+)", str(url or ""))
    return match.group(1) if match else ""


def _handle_from_status_href(href: str) -> str:
    match = re.search(r"^/([A-Za-z0-9_]{1,20})/status/\d+", str(href or ""))
    return match.group(1) if match else ""


def _screenshot(page: Any, prefix: str) -> str:
    try:
        X_BROWSER_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = X_BROWSER_SCREENSHOT_DIR / f"{prefix}_{int(time.time())}.png"
        page.screenshot(path=str(path), full_page=True)
        return str(path)
    except Exception:
        return ""


def _safe_page_text(page: Any, limit: int = 1200) -> str:
    try:
        text = page.locator("body").inner_text(timeout=3000) or ""
    except Exception:
        return ""
    return re.sub(r"\s+", " ", text).strip()[:limit]


def _debug_log_path() -> Path:
    X_BROWSER_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    return X_BROWSER_DEBUG_DIR / f"x_publish_{int(time.time())}.json"


def _append_debug_event(events: list[dict[str, Any]], stage: str, page: Any | None = None, **extra: Any) -> None:
    event = {
        "ts": time.time(),
        "stage": stage,
    }
    if page is not None:
        try:
            event["url"] = str(page.url or "")
        except Exception:
            event["url"] = ""
        event["body_excerpt"] = _safe_page_text(page)
    event.update(extra)
    events.append(event)


def _write_debug_log(path: Path, events: list[dict[str, Any]]) -> str:
    X_BROWSER_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(events, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(path)


def _looks_logged_out(page: Any) -> bool:
    url = str(getattr(page, "url", "") or "")
    if (
        "/login" in url
        or "/i/flow/login" in url
        or "/onboarding" in url
        or "redirect_after_login" in url
        or "/i/flow/signup" in url
    ):
        return True
    selectors = [
        'input[name="text"]',
        'a[href="/login"]',
        'a[data-testid="loginButton"]',
        'div[data-testid="login"]',
    ]
    for selector in selectors:
        try:
            if page.locator(selector).first.is_visible(timeout=1000):
                return True
        except Exception:
            pass
    return False


def _first_visible(locator: Any, timeout_ms: int = 5000) -> Any:
    count = locator.count()
    for index in range(count):
        item = locator.nth(index)
        try:
            if item.is_visible(timeout=timeout_ms):
                return item
        except Exception:
            continue
    return locator.first


def _fill_post_text(page: Any, text: str, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    text = str(text or "").strip()[:280]
    if not text:
        return
    candidates = [
        'div[data-testid="tweetTextarea_0"]',
        'div[role="textbox"][data-testid^="tweetTextarea"]',
        'div[role="textbox"]',
    ]
    last_error: Exception | None = None
    for selector in candidates:
        try:
            box = _first_visible(page.locator(selector), timeout_ms=8000)
            box.click(timeout=8000)
            box.fill(text, timeout=8000)
            if debug_events is not None:
                _append_debug_event(debug_events, "text_filled", page, selector=selector, text_length=len(text))
            return
        except Exception as exc:
            last_error = exc
    raise XBrowserPublishError(f"找不到 X 发帖文本框：{last_error}")


def _get_profile_href(page: Any) -> str:
    selectors = [
        'a[data-testid="AppTabBar_Profile_Link"]',
        'a[aria-label*="Profile"]',
        'a[aria-label*="プロフィール"]',
        'a[href^="/"][role="link"]',
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = locator.count()
            for index in range(min(count, 20)):
                item = locator.nth(index)
                href = str(item.get_attribute("href") or "")
                if re.fullmatch(r"/[A-Za-z0-9_]{1,20}", href):
                    return href
        except Exception:
            continue
    return ""


def _collect_status_hrefs(page: Any, *, limit: int = 50) -> set[str]:
    hrefs: set[str] = set()
    try:
        links = page.locator('a[href*="/status/"]')
        for index in range(min(links.count(), limit)):
            href = str(links.nth(index).get_attribute("href") or "").strip()
            if href:
                hrefs.add(href)
    except Exception:
        pass
    return hrefs


def _status_links_for_handle(page: Any, handle: str, *, limit: int = 20) -> list[str]:
    hrefs: list[str] = []
    handle = str(handle or "").strip()
    if not handle:
        return hrefs
    selectors = [
        f'a[href="/{handle}/status/"]',
        f'a[href^="/{handle}/status/"]',
    ]
    seen: set[str] = set()
    for selector in selectors:
        try:
            links = page.locator(selector)
            for index in range(min(links.count(), limit)):
                href = str(links.nth(index).get_attribute("href") or "").strip()
                if href and href not in seen:
                    hrefs.append(href)
                    seen.add(href)
        except Exception:
            continue
    return hrefs


def _normalize_text(value: str, *, limit: int = 120) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _post_text_match_candidates(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    first_line = next((line.strip() for line in raw.splitlines() if line.strip()), "")
    without_urls = re.sub(r"https?://\S+", "", raw, flags=re.I)
    candidates = [
        _normalize_text(first_line, limit=80),
        _normalize_text(without_urls, limit=80),
        _normalize_text(raw, limit=48),
    ]
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _find_matching_post_on_profile(page: Any, handle: str, text: str) -> dict[str, str]:
    targets = _post_text_match_candidates(text)
    if not handle or not targets:
        return {}
    articles = page.locator('article[data-testid="tweet"]')
    count = min(articles.count(), 8)
    for index in range(count):
        article = articles.nth(index)
        try:
            article_text = _normalize_text(article.inner_text(timeout=3000), limit=400)
            if not any(target in article_text for target in targets):
                continue
            links = article.locator(f'a[href^="/{handle}/status/"]')
            for link_index in range(min(links.count(), 5)):
                href = str(links.nth(link_index).get_attribute("href") or "").strip()
                post_id = _tweet_id_from_url(href)
                if post_id:
                    has_media = False
                    try:
                        has_media = article.locator(
                            'video, div[data-testid="videoPlayer"], div[data-testid="tweetPhoto"], '
                            'a[href*="/video/"], a[href$="/photo/1"]'
                        ).count() > 0
                    except Exception:
                        has_media = False
                    return {
                        "post_id": post_id,
                        "x_url": f"https://x.com/i/web/status/{post_id}",
                        "has_media": "1" if has_media else "0",
                    }
        except Exception:
            continue
    return {}


def _upload_video_file(page: Any, video_path: Path, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    candidates = [
        'input[data-testid="fileInput"]',
        'input[type="file"][accept*="video"]',
        'input[type="file"]',
    ]
    last_error: Exception | None = None
    for selector in candidates:
        try:
            file_input = page.locator(selector).first
            file_input.set_input_files(str(video_path), timeout=15000)
            if debug_events is not None:
                _append_debug_event(debug_events, "video_selected", page, selector=selector, video_path=str(video_path))
            return
        except Exception as exc:
            last_error = exc
    raise XBrowserPublishError(f"找不到 X 视频上传控件：{last_error}")


def _click_media_retry(page: Any, *, debug_events: list[dict[str, Any]] | None = None) -> bool:
    patterns = [
        re.compile(r"^\s*retry\s*$", re.I),
        re.compile(r"^\s*重试\s*$"),
        re.compile(r"^\s*再試行\s*$"),
        re.compile(r"^\s*やり直す\s*$"),
    ]
    for pattern in patterns:
        try:
            button = page.get_by_role("button", name=pattern).first
            if not button.is_visible(timeout=1500):
                continue
            button.click(timeout=5000)
            if debug_events is not None:
                _append_debug_event(debug_events, "media_retry_clicked", page, pattern=pattern.pattern)
            return True
        except Exception:
            continue
    return False


def _wait_video_ready(page: Any, video_name: str, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    deadline = time.time() + X_BROWSER_UPLOAD_TIMEOUT_SECONDS
    last_state = ""
    ui_retry_count = 0
    ready_since = 0.0
    while time.time() < deadline:
        pending_text = ""
        try:
            pending_text = page.locator("body").inner_text(timeout=3000)
        except Exception:
            pending_text = ""
        lowered = pending_text.lower()
        failed = any(mark in lowered for mark in ("media failed", "upload failed"))
        # 只信可靠信号：附件预览元素 / “移除媒体”按钮 / 上传进度条。
        # 不再用 "edit"、"upload caption file" 等页面通用词兜底——那些词在 X 界面到处都是，
        # 视频没附上时也会命中，导致误判“已就绪”从而只发出文案、不带视频。
        has_video_chip = False
        uploading_bar = False
        try:
            # 强信号：本地预览缩略图(blob:)/video 元素/附件预览/移除媒体按钮。
            chip = page.locator(
                'div[data-testid="attachments"] video, div[data-testid="attachments"] img, '
                'img[src^="blob:"], video[src^="blob:"], video[poster], '
                '[data-testid="removeMedia"], button[aria-label*="Remove"], '
                'div[aria-label*="移除"], div[aria-label*="削除"]'
            )
            has_video_chip = chip.count() > 0
        except Exception:
            has_video_chip = False
        try:
            # 只看附件区域内的进度条，避免命中 X 顶部页面加载条导致永远“上传中”
            uploading_bar = page.locator('div[data-testid="attachments"] div[role="progressbar"]').count() > 0
        except Exception:
            uploading_bar = False
        uploading = uploading_bar or any(mark in lowered for mark in ("processing", "uploading", "正在上传", "处理中"))
        state = "ready_check"
        if uploading:
            state = "pending"
        elif not has_video_chip:
            state = "waiting_preview"
        if state != last_state and debug_events is not None:
            _append_debug_event(debug_events, "upload_state", page, state=state)
            last_state = state
        if failed:
            if ui_retry_count < X_BROWSER_MEDIA_UI_RETRIES and _click_media_retry(page, debug_events=debug_events):
                ui_retry_count += 1
                if debug_events is not None:
                    _append_debug_event(debug_events, "media_retry_wait", page, retry=ui_retry_count)
                time.sleep(5)
                continue
            raise XBrowserPublishError("X 视频上传失败，页面提示 media/upload failed")
        if uploading:
            ready_since = 0.0
            time.sleep(5)
            continue
        if not has_video_chip:
            ready_since = 0.0
            time.sleep(2)
            continue
        try:
            button = page.locator('button[data-testid="tweetButton"], div[data-testid="tweetButton"]').first
            if button.is_enabled(timeout=2000):
                if ready_since <= 0:
                    ready_since = time.time()
                    if debug_events is not None:
                        _append_debug_event(debug_events, "upload_ready_settling", page)
                if time.time() - ready_since < X_BROWSER_MEDIA_READY_SETTLE_SECONDS:
                    time.sleep(2)
                    continue
                if debug_events is not None:
                    _append_debug_event(debug_events, "upload_ready", page)
                return
        except Exception:
            ready_since = 0.0
        time.sleep(3)
    raise XBrowserPublishError(f"等待 X 视频上传处理超时（{X_BROWSER_UPLOAD_TIMEOUT_SECONDS} 秒）")


def _click_post_button(page: Any, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    selectors = [
        'button[data-testid="tweetButton"]',
        'div[data-testid="tweetButton"]',
        'button[data-testid="tweetButtonInline"]',
        'div[data-testid="tweetButtonInline"]',
    ]
    last_error: Exception | None = None
    scoped_roots: list[tuple[str, Any]] = []
    try:
        dialogs = page.locator('div[role="dialog"]:visible, div[data-testid="sheetDialog"]:visible')
        for index in reversed(range(min(dialogs.count(), 4))):
            scoped_roots.append((f"dialog:{index}", dialogs.nth(index)))
    except Exception:
        pass
    scoped_roots.append(("page", page))
    for scope_name, root in scoped_roots:
        for selector in selectors:
            try:
                candidates = root.locator(selector)
                for index in reversed(range(min(candidates.count(), 6))):
                    button = candidates.nth(index)
                    if not button.is_visible(timeout=300) or not button.is_enabled(timeout=300):
                        continue
                    button.click(timeout=5000)
                    if debug_events is not None:
                        _append_debug_event(debug_events, "post_clicked", page, selector=f"{scope_name}:{selector}:{index}")
                    return
            except Exception as exc:
                last_error = exc
    try:
        for scope_name, root in scoped_roots:
            candidates = root.locator("button, [role='button']")
            matched: list[tuple[float, float, int, str]] = []
            for index in range(min(candidates.count(), 100)):
                item = candidates.nth(index)
                try:
                    if not item.is_visible(timeout=200) or not item.is_enabled(timeout=200):
                        continue
                    text = re.sub(r"\s+", " ", (item.inner_text(timeout=200) or "")).strip()
                    if not text:
                        continue
                    lowered = text.lower()
                    if lowered not in {"post", "post all", "ポストする", "投稿", "发布", "發佈", "發布"}:
                        continue
                    box = item.bounding_box() or {}
                    x = float(box.get("x", 0.0))
                    y = float(box.get("y", 0.0))
                    matched.append((x, y, index, text))
                except Exception:
                    continue
            matched.sort(key=lambda row: (-row[0], -row[1]))
            if debug_events is not None:
                _append_debug_event(
                    debug_events,
                    "post_button_candidates",
                    page,
                    scope=scope_name,
                    candidates=[{"index": idx, "text": text, "x": x, "y": y} for x, y, idx, text in matched[:10]],
                )
            for _, _, index, text in matched:
                try:
                    item = candidates.nth(index)
                    item.click(timeout=5000)
                    if debug_events is not None:
                        _append_debug_event(debug_events, "post_clicked", page, selector=f"{scope_name}:generic_button:{text}")
                    return
                except Exception as exc:
                    last_error = exc
    except Exception as exc:
        last_error = exc
    raise XBrowserPublishError(f"找不到 X 发布按钮：{last_error}")


def _x_post_submission_signal(page: Any, state: dict[str, Any], *, timeout_seconds: float = 12.0) -> str:
    deadline = time.time() + max(1.0, timeout_seconds)
    hidden_polls = 0
    while time.time() < deadline:
        if state.get("request_seen"):
            return "create_tweet_request"
        try:
            dialog = page.locator('div[role="dialog"]:visible').first
            editor = page.locator('div[data-testid="tweetTextarea_0"]:visible').first
            visible = dialog.is_visible(timeout=200) and editor.is_visible(timeout=200)
        except Exception:
            visible = False
        hidden_polls = 0 if visible else hidden_polls + 1
        if hidden_polls >= 3:
            return "composer_closed"
        page.wait_for_timeout(250)
    return ""


def _submit_post(page: Any, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    submission: dict[str, Any] = {"request_seen": False, "request_url": "", "response_status": 0}

    def on_request(request: Any) -> None:
        url = str(getattr(request, "url", "") or "")
        if "createtweet" not in url.lower() and "createpost" not in url.lower():
            return
        submission["request_seen"] = True
        submission["request_url"] = url

    def on_response(response: Any) -> None:
        url = str(getattr(response, "url", "") or "")
        if "createtweet" not in url.lower() and "createpost" not in url.lower():
            return
        submission["response_status"] = int(getattr(response, "status", 0) or 0)

    page.on("request", on_request)
    page.on("response", on_response)
    focus_selectors = [
        'div[role="dialog"] div[data-testid="tweetTextarea_0"]',
        'div[role="dialog"] div[role="textbox"]',
        'div[data-testid="tweetTextarea_0"]',
        'div[role="textbox"]',
    ]
    for selector in focus_selectors:
        try:
            box = page.locator(selector).first
            box.click(timeout=3000)
            if debug_events is not None:
                _append_debug_event(debug_events, "post_submit_focus", page, selector=selector)
            break
        except Exception:
            continue
    last_error: Exception | None = None
    try:
        _click_post_button(page, debug_events=debug_events)
        signal = _x_post_submission_signal(page, submission)
        if signal:
            if debug_events is not None:
                _append_debug_event(debug_events, "post_submit_confirmed", page, signal=signal, **submission)
            return
        last_error = XBrowserPublishError("点击 Post 后没有触发 CreateTweet 请求，编辑器也没有关闭")
    except Exception as exc:
        last_error = exc
    for shortcut in ("Control+Enter", "Meta+Enter"):
        try:
            for selector in focus_selectors:
                try:
                    page.locator(selector).first.click(timeout=1000)
                    break
                except Exception:
                    continue
            page.keyboard.press(shortcut)
            if debug_events is not None:
                _append_debug_event(debug_events, "post_submit_shortcut", page, shortcut=shortcut)
            signal = _x_post_submission_signal(page, submission)
            if signal:
                if debug_events is not None:
                    _append_debug_event(debug_events, "post_submit_confirmed", page, signal=signal, **submission)
                return
            last_error = XBrowserPublishError(f"{shortcut} 没有触发 CreateTweet 请求，编辑器也没有关闭")
        except Exception as exc:
            last_error = exc
    raise XBrowserPublishError(f"提交 X Post 失败：{last_error}")


def _wait_post_result(
    page: Any,
    *,
    expected_handle: str = "",
    existing_status_hrefs: set[str] | None = None,
    existing_profile_status_hrefs: set[str] | None = None,
    text: str = "",
    debug_events: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    existing_status_hrefs = set(existing_status_hrefs or set())
    existing_profile_status_hrefs = set(existing_profile_status_hrefs or set())
    deadline = time.time() + X_BROWSER_POST_TIMEOUT_SECONDS
    normalized_text = _normalize_text(text, limit=80)
    settle_deadline = time.time() + min(X_BROWSER_POST_SETTLE_SECONDS, X_BROWSER_POST_TIMEOUT_SECONDS)
    best_candidate: dict[str, str] = {}
    while time.time() < deadline:
        post_id = _tweet_id_from_url(page.url)
        if post_id and (not expected_handle or f"/{expected_handle}/status/{post_id}" not in existing_profile_status_hrefs):
            if debug_events is not None:
                _append_debug_event(debug_events, "post_result_url", page, post_id=post_id)
            best_candidate = {"post_id": post_id, "x_url": f"https://x.com/i/web/status/{post_id}"}
            if not expected_handle or not normalized_text:
                return best_candidate
        try:
            links = page.locator('a[href*="/status/"]')
            for index in range(min(links.count(), 20)):
                href = str(links.nth(index).get_attribute("href") or "")
                if not href or href in existing_status_hrefs:
                    continue
                if expected_handle:
                    href_handle = _handle_from_status_href(href)
                    if href_handle.lower() != expected_handle.lower():
                        continue
                    if href in existing_profile_status_hrefs:
                        continue
                post_id = _tweet_id_from_url(href)
                if post_id:
                    candidate = {"post_id": post_id, "x_url": f"https://x.com/i/web/status/{post_id}"}
                    best_candidate = candidate
                    if debug_events is not None:
                        _append_debug_event(debug_events, "post_result_link_candidate", page, post_id=post_id, href=href)
        except Exception:
            pass
        if time.time() < settle_deadline:
            if debug_events is not None:
                _append_debug_event(debug_events, "post_settling", page)
            time.sleep(2)
            continue
        if expected_handle and normalized_text:
            try:
                page.goto(f"https://x.com/{expected_handle}", wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(4000)
                current_profile_hrefs = set(_status_links_for_handle(page, expected_handle, limit=20))
                matching = _find_matching_post_on_profile(page, expected_handle, text)
                if debug_events is not None:
                    _append_debug_event(
                        debug_events,
                        "profile_poll",
                        page,
                        current_profile_status_hrefs=list(sorted(current_profile_hrefs))[:20],
                        matching=matching,
                    )
                if matching and matching.get("has_media") == "1":
                    post_id = matching.get("post_id", "")
                    href = f"/{expected_handle}/status/{post_id}" if post_id else ""
                    if href and href not in existing_profile_status_hrefs:
                        if debug_events is not None:
                            _append_debug_event(debug_events, "post_result_profile", page, post_id=post_id, href=href)
                        return {"post_id": post_id, "x_url": matching.get("x_url", "")}
            except Exception:
                pass
        if best_candidate:
            if debug_events is not None:
                _append_debug_event(debug_events, "post_result_candidate_fallback", page, **best_candidate)
            if not expected_handle or not normalized_text:
                return best_candidate
        time.sleep(2)
    return {"post_id": "", "x_url": ""}


def _publish_video_to_x_browser_locked(
    video_path: Path,
    *,
    text: str,
    made_with_ai: bool = True,
    user_data_dir: str | Path | None = None,
) -> dict[str, Any]:
    source_video_path = _ensure_video_path(video_path)
    video_path = prepare_x_video_for_upload(source_video_path)
    profile_dir = Path(user_data_dir).expanduser().resolve() if user_data_dir else X_BROWSER_USER_DATA_DIR
    profile_dir.mkdir(parents=True, exist_ok=True)
    # 清理上次异常退出残留的 Singleton 锁,否则新 chromium 会以为 profile 被占用而启动失败。
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (profile_dir / lock_name).unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass
    X_BROWSER_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    X_BROWSER_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    debug_events: list[dict[str, Any]] = []
    debug_log_path = _debug_log_path()
    sync_playwright, _ = _import_playwright()
    # 有头模式需要一个显示；无 DISPLAY 时启动虚拟显示 Xvfb。启动失败才退回无头。
    headless = X_BROWSER_HEADLESS
    launch_env: dict[str, str] | None = None
    if not headless:
        display = os.environ.get("DISPLAY") or _ensure_publish_display()
        if display:
            launch_env = {**os.environ, "DISPLAY": display}
        else:
            headless = True
    with sync_playwright() as playwright:
        browser_type = playwright.chromium
        executable_path = _resolve_browser_executable() or None
        _append_debug_event(
            debug_events,
            "launch_start",
            executable_path=executable_path or "",
            headless=headless,
            display=(launch_env or {}).get("DISPLAY", ""),
            compose_url=X_BROWSER_COMPOSE_URL,
            video_path=str(video_path),
            source_video_path=str(source_video_path),
            user_data_dir=str(profile_dir),
        )
        context = browser_type.launch_persistent_context(
            str(profile_dir),
            executable_path=executable_path,
            headless=headless,
            env=launch_env,
            slow_mo=X_BROWSER_SLOW_MO_MS,
            viewport={"width": 1365, "height": 900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(X_BROWSER_COMPOSE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            _append_debug_event(debug_events, "page_opened", page)
            if _looks_logged_out(page):
                screenshot = _screenshot(page, "x_login_required")
                debug_path = _write_debug_log(debug_log_path, debug_events)
                raise XBrowserPublishError(
                    "X 浏览器发布失败：当前服务器浏览器未登录 X。"
                    "需要先在持久化浏览器 profile 中完成一次登录。"
                    + (f" 截图：{screenshot}" if screenshot else "")
                    + f" 调试日志：{debug_path}"
                )
            profile_href = _get_profile_href(page)
            expected_handle = profile_href.lstrip("/").strip()
            existing_status_hrefs = _collect_status_hrefs(page)
            existing_profile_status_hrefs: set[str] = set()
            if expected_handle:
                try:
                    page.goto(f"https://x.com/{expected_handle}", wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(3000)
                    existing_profile_status_hrefs = set(_status_links_for_handle(page, expected_handle, limit=20))
                    page.goto(X_BROWSER_COMPOSE_URL, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(2000)
                except Exception:
                    existing_profile_status_hrefs = set()
            _append_debug_event(
                debug_events,
                "account_context",
                page,
                expected_handle=expected_handle,
                existing_status_hrefs=list(sorted(existing_status_hrefs))[:20],
                existing_profile_status_hrefs=list(sorted(existing_profile_status_hrefs))[:20],
            )
            _fill_post_text(page, text, debug_events=debug_events)
            _upload_video_file(page, video_path, debug_events=debug_events)
            _wait_video_ready(page, video_path.name, debug_events=debug_events)
            ready_screenshot = _screenshot(page, "x_ready_to_post")
            _append_debug_event(debug_events, "ready_to_post", page, screenshot=ready_screenshot)
            _submit_post(page, debug_events=debug_events)
            result = _wait_post_result(
                page,
                expected_handle=expected_handle,
                existing_status_hrefs=existing_status_hrefs,
                existing_profile_status_hrefs=existing_profile_status_hrefs,
                text=text,
                debug_events=debug_events,
            )
            if result.get("x_url"):
                result["verified_media"] = True
            else:
                screenshot = _screenshot(page, "x_post_result_unknown")
                _append_debug_event(debug_events, "post_result_unknown", page, screenshot=screenshot)
                result["verified_media"] = False
                result["warning"] = "X 页面未返回可验证的视频帖子，请到主页人工复核"
                if screenshot:
                    result["screenshot"] = screenshot
            debug_path = _write_debug_log(debug_log_path, debug_events)
            if not result.get("post_id") or not result.get("x_url") or not result.get("verified_media"):
                raise XBrowserPublishError(
                    "X 未返回可验证的新视频帖子，不能记为发布成功。"
                    + (f" 截图：{result.get('screenshot')}" if result.get("screenshot") else "")
                    + f" 调试日志：{debug_path}"
                )
            return {
                "post_id": result.get("post_id", ""),
                "x_url": result.get("x_url", ""),
                "text": str(text or "").strip()[:280],
                "media_id": "",
                "media": {
                    "provider": "browser",
                    "video_path": str(video_path),
                    "source_video_path": str(source_video_path),
                    "transcoded": video_path != source_video_path,
                },
                "made_with_ai": made_with_ai,
                "publisher": "browser",
                "debug_log": debug_path,
                "raw": result,
            }
        except Exception as exc:
            screenshot = _screenshot(page, "x_publish_failed")
            _append_debug_event(debug_events, "publish_failed", page, screenshot=screenshot, error=str(exc))
            debug_path = _write_debug_log(debug_log_path, debug_events)
            if isinstance(exc, XBrowserPublishError):
                message = str(exc)
            else:
                message = f"X 浏览器发布失败：{exc}"
            if screenshot and "截图：" not in message:
                message = f"{message} 截图：{screenshot}"
            if debug_path and "调试日志：" not in message:
                message = f"{message} 调试日志：{debug_path}"
            raise XBrowserPublishError(message) from exc
        finally:
            context.close()


def publish_video_to_x_browser(
    video_path: Path,
    *,
    text: str,
    made_with_ai: bool = True,
    user_data_dir: str | Path | None = None,
) -> dict[str, Any]:
    source = _ensure_video_path(video_path)
    profile_dir = Path(user_data_dir).expanduser().resolve() if user_data_dir else X_BROWSER_USER_DATA_DIR
    profile_dir.mkdir(parents=True, exist_ok=True)
    with _exclusive_profile_publish_lock(profile_dir):
        _wait_for_x_browser_publish_slot(profile_dir)
        last_error: Exception | None = None
        attempts = X_BROWSER_MEDIA_FULL_RETRIES + 1
        for attempt in range(1, attempts + 1):
            try:
                result = _publish_video_to_x_browser_locked(
                    source,
                    text=text,
                    made_with_ai=made_with_ai,
                    user_data_dir=profile_dir,
                )
                raw_result = result.get("raw") if isinstance(result.get("raw"), dict) else {}
                if (
                    not str(result.get("post_id") or "").strip()
                    or not str(result.get("x_url") or "").strip()
                    or raw_result.get("verified_media") is False
                ):
                    raise XBrowserPublishError("X 未返回可验证的新视频帖子，不能记为发布成功")
                _record_x_browser_publish_success(profile_dir, result)
                return result
            except XBrowserPublishError as exc:
                last_error = exc
                if attempt >= attempts or not _is_retryable_media_failure(exc):
                    raise
                print(
                    f"[X browser] media upload failed, retrying full publish "
                    f"in {X_BROWSER_MEDIA_RETRY_DELAY_SECONDS}s ({attempt}/{attempts})",
                    flush=True,
                )
                time.sleep(X_BROWSER_MEDIA_RETRY_DELAY_SECONDS)
        raise XBrowserPublishError(str(last_error or "X 浏览器自动发布失败"))
