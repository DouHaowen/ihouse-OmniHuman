import json
import os
import re
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(override=False)


class XBrowserPublishError(RuntimeError):
    pass


X_BROWSER_STATE_DIR = Path(os.getenv("X_BROWSER_STATE_DIR", "output/x_browser")).resolve()
X_BROWSER_USER_DATA_DIR = Path(os.getenv("X_BROWSER_USER_DATA_DIR", str(X_BROWSER_STATE_DIR / "profile"))).resolve()
X_BROWSER_SCREENSHOT_DIR = Path(os.getenv("X_BROWSER_SCREENSHOT_DIR", str(X_BROWSER_STATE_DIR / "screenshots"))).resolve()
X_BROWSER_HEADLESS = (os.getenv("X_BROWSER_HEADLESS", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
X_BROWSER_SLOW_MO_MS = max(0, int(os.getenv("X_BROWSER_SLOW_MO_MS", "0") or "0"))
X_BROWSER_UPLOAD_TIMEOUT_SECONDS = max(60, int(os.getenv("X_BROWSER_UPLOAD_TIMEOUT_SECONDS", "900") or "900"))
X_BROWSER_POST_TIMEOUT_SECONDS = max(20, int(os.getenv("X_BROWSER_POST_TIMEOUT_SECONDS", "120") or "120"))
X_BROWSER_POST_SETTLE_SECONDS = max(5, int(os.getenv("X_BROWSER_POST_SETTLE_SECONDS", "15") or "15"))
X_BROWSER_COMPOSE_URL = os.getenv("X_BROWSER_COMPOSE_URL", "https://x.com/compose/post").strip() or "https://x.com/compose/post"
X_BROWSER_EXECUTABLE_PATH = os.getenv("X_BROWSER_EXECUTABLE_PATH", "").strip()
X_BROWSER_DEBUG_DIR = Path(os.getenv("X_BROWSER_DEBUG_DIR", str(X_BROWSER_STATE_DIR / "debug"))).resolve()


def _resolve_browser_executable() -> str:
    candidates = []
    if X_BROWSER_EXECUTABLE_PATH:
        candidates.append(X_BROWSER_EXECUTABLE_PATH)
    candidates.extend(
        [
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/snap/bin/chromium",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]
    )
    for candidate in candidates:
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
    if "/login" in url or "/i/flow/login" in url:
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


def _find_matching_post_on_profile(page: Any, handle: str, text: str) -> dict[str, str]:
    target = _normalize_text(text, limit=80)
    if not handle or not target:
        return {}
    articles = page.locator('article[data-testid="tweet"]')
    count = min(articles.count(), 8)
    for index in range(count):
        article = articles.nth(index)
        try:
            article_text = _normalize_text(article.inner_text(timeout=3000), limit=400)
            if target not in article_text:
                continue
            links = article.locator(f'a[href^="/{handle}/status/"]')
            for link_index in range(min(links.count(), 5)):
                href = str(links.nth(link_index).get_attribute("href") or "").strip()
                post_id = _tweet_id_from_url(href)
                if post_id:
                    has_media = False
                    try:
                        has_media = article.locator('video, img[src*="twimg"], div[data-testid="tweetPhoto"]').count() > 0
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


def _wait_video_ready(page: Any, video_name: str, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    deadline = time.time() + X_BROWSER_UPLOAD_TIMEOUT_SECONDS
    last_state = ""
    normalized_video_name = str(video_name or "").strip().lower()
    body_markers = ("edit", "upload caption file", normalized_video_name)
    while time.time() < deadline:
        pending_text = ""
        try:
            pending_text = page.locator("body").inner_text(timeout=3000)
        except Exception:
            pending_text = ""
        lowered = pending_text.lower()
        uploading = any(mark in lowered for mark in ("processing", "uploading", "正在上传", "处理中"))
        failed = any(mark in lowered for mark in ("media failed", "upload failed"))
        has_video_chip = False
        state = "ready_check"
        try:
            previews = page.locator('div[data-testid="attachments"] img, div[data-testid="attachments"] video')
            has_video_chip = previews.count() > 0
        except Exception:
            has_video_chip = False
        if not has_video_chip:
            has_video_chip = any(marker and marker in lowered for marker in body_markers)
        if uploading:
            state = "pending"
        elif not has_video_chip:
            state = "waiting_preview"
        if state != last_state and debug_events is not None:
            _append_debug_event(debug_events, "upload_state", page, state=state)
            last_state = state
        if uploading or failed:
            if failed:
                raise XBrowserPublishError("X 视频上传失败，页面提示 media/upload failed")
            time.sleep(5)
            continue
        if not has_video_chip:
            time.sleep(2)
            continue
        try:
            button = page.locator('button[data-testid="tweetButton"], div[data-testid="tweetButton"]').first
            if button.is_enabled(timeout=2000):
                if debug_events is not None:
                    _append_debug_event(debug_events, "upload_ready", page)
                return
        except Exception:
            pass
        time.sleep(3)
    raise XBrowserPublishError(f"等待 X 视频上传处理超时（{X_BROWSER_UPLOAD_TIMEOUT_SECONDS} 秒）")


def _click_post_button(page: Any, *, debug_events: list[dict[str, Any]] | None = None) -> None:
    dialog_selectors = [
        'div[role="dialog"]',
        'div[data-testid="sheetDialog"]',
    ]
    selectors = [
        'button[data-testid="tweetButton"]',
        'div[data-testid="tweetButton"]',
        'button[data-testid="tweetButtonInline"]',
        'div[data-testid="tweetButtonInline"]',
    ]
    last_error: Exception | None = None
    scoped_roots = []
    for dialog_selector in dialog_selectors:
        try:
            dialog = page.locator(dialog_selector).first
            if dialog.count():
                scoped_roots.append((f"dialog:{dialog_selector}", dialog))
        except Exception:
            continue
    scoped_roots.append(("page", page))
    for scope_name, root in scoped_roots:
        for selector in selectors:
            try:
                button = _first_visible(root.locator(selector), timeout_ms=5000)
                button.click(timeout=10000)
                if debug_events is not None:
                    _append_debug_event(debug_events, "post_clicked", page, selector=f"{scope_name}:{selector}")
                return
            except Exception as exc:
                last_error = exc
    text_patterns = [
        re.compile(r"^\s*post\s*$", re.I),
        re.compile(r"^\s*post all\s*$", re.I),
        re.compile(r"^\s*ポストする\s*$"),
        re.compile(r"^\s*投稿\s*$"),
        re.compile(r"^\s*发布\s*$"),
        re.compile(r"^\s*發佈\s*$"),
        re.compile(r"^\s*發布\s*$"),
    ]
    for scope_name, root in scoped_roots:
        for pattern in text_patterns:
            try:
                button = root.get_by_role("button", name=pattern).first
                button.wait_for(timeout=5000)
                button.click(timeout=10000)
                if debug_events is not None:
                    _append_debug_event(debug_events, "post_clicked", page, selector=f"{scope_name}:role_button:{pattern.pattern}")
                return
            except Exception as exc:
                last_error = exc
    try:
        for scope_name, root in scoped_roots:
            candidates = root.locator("button, [role='button']")
            matched: list[tuple[float, float, int, str]] = []
            for index in range(candidates.count()):
                item = candidates.nth(index)
                try:
                    if not item.is_visible(timeout=1000):
                        continue
                    text = re.sub(r"\s+", " ", (item.inner_text(timeout=1000) or "")).strip()
                    if not text:
                        continue
                    lowered = text.lower()
                    if not any(token in lowered for token in ("post", "ポスト", "投稿", "发布", "發佈", "發布")):
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
                    item.click(timeout=10000, force=True)
                    if debug_events is not None:
                        _append_debug_event(debug_events, "post_clicked", page, selector=f"{scope_name}:generic_button:{text}")
                    return
                except Exception as exc:
                    last_error = exc
    except Exception as exc:
        last_error = exc
    raise XBrowserPublishError(f"找不到 X 发布按钮：{last_error}")


def _submit_post(page: Any, *, debug_events: list[dict[str, Any]] | None = None) -> None:
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
    shortcuts = ["Control+Enter", "Meta+Enter"]
    last_error: Exception | None = None
    for shortcut in shortcuts:
        try:
            page.keyboard.press(shortcut)
            if debug_events is not None:
                _append_debug_event(debug_events, "post_submit_shortcut", page, shortcut=shortcut)
            return
        except Exception as exc:
            last_error = exc
    try:
        _click_post_button(page, debug_events=debug_events)
        return
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
    while time.time() < deadline:
        post_id = _tweet_id_from_url(page.url)
        if post_id and (not expected_handle or f"/{expected_handle}/status/{post_id}" not in existing_profile_status_hrefs):
            if debug_events is not None:
                _append_debug_event(debug_events, "post_result_url", page, post_id=post_id)
            return {"post_id": post_id, "x_url": f"https://x.com/i/web/status/{post_id}"}
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
                matching = _find_matching_post_on_profile(page, expected_handle, normalized_text)
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
        time.sleep(2)
    return {"post_id": "", "x_url": ""}


def publish_video_to_x_browser(
    video_path: Path,
    *,
    text: str,
    made_with_ai: bool = True,
    user_data_dir: str | Path | None = None,
) -> dict[str, Any]:
    video_path = _ensure_video_path(video_path)
    profile_dir = Path(user_data_dir).expanduser().resolve() if user_data_dir else X_BROWSER_USER_DATA_DIR
    profile_dir.mkdir(parents=True, exist_ok=True)
    X_BROWSER_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    X_BROWSER_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    debug_events: list[dict[str, Any]] = []
    debug_log_path = _debug_log_path()
    sync_playwright, _ = _import_playwright()
    with sync_playwright() as playwright:
        browser_type = playwright.chromium
        executable_path = _resolve_browser_executable() or None
        _append_debug_event(
            debug_events,
            "launch_start",
            executable_path=executable_path or "",
            headless=X_BROWSER_HEADLESS,
            compose_url=X_BROWSER_COMPOSE_URL,
            video_path=str(video_path),
            user_data_dir=str(profile_dir),
        )
        context = browser_type.launch_persistent_context(
            str(profile_dir),
            executable_path=executable_path,
            headless=X_BROWSER_HEADLESS,
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
            if not result.get("x_url"):
                screenshot = _screenshot(page, "x_post_result_unknown")
                _append_debug_event(debug_events, "post_result_unknown", page, screenshot=screenshot)
                result["warning"] = "X 页面未返回明确帖子链接，请到主页确认是否已发布"
                if screenshot:
                    result["screenshot"] = screenshot
            debug_path = _write_debug_log(debug_log_path, debug_events)
            return {
                "post_id": result.get("post_id", ""),
                "x_url": result.get("x_url", ""),
                "text": str(text or "").strip()[:280],
                "media_id": "",
                "media": {"provider": "browser", "video_path": str(video_path)},
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
