import json
import re
import sys
import time
from pathlib import Path

from x_browser_publisher import (
    X_BROWSER_HEADLESS,
    X_BROWSER_SLOW_MO_MS,
    X_BROWSER_USER_DATA_DIR,
    XBrowserPublishError,
    _click_post_button,
    _first_visible,
    _import_playwright,
    _looks_logged_out,
    _resolve_browser_executable,
    _screenshot,
    _wait_post_result,
)


def _get_profile_href(page):
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
            for i in range(min(count, 20)):
                item = locator.nth(i)
                href = str(item.get_attribute("href") or "")
                if re.fullmatch(r"/[A-Za-z0-9_]{1,20}", href):
                    return href
        except Exception:
            continue
    return ""


def _find_post_on_profile(page, marker: str) -> bool:
    try:
        body = page.locator("body").inner_text(timeout=5000) or ""
        return marker in body
    except Exception:
        return False


def main():
    marker = f"IHOUSE_X_SMOKE_{int(time.time())}"
    sync_playwright, _ = _import_playwright()
    X_BROWSER_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            str(X_BROWSER_USER_DATA_DIR),
            executable_path=_resolve_browser_executable() or None,
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
        result = {
            "ok": False,
            "marker": marker,
            "current_url": "",
            "profile_href": "",
            "profile_url": "",
            "post_found_on_profile": False,
            "post_url": "",
            "screenshot": "",
        }
        try:
            page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            result["current_url"] = page.url
            if _looks_logged_out(page):
                raise XBrowserPublishError("当前浏览器未登录 X")

            box = _first_visible(page.locator('div[data-testid="tweetTextarea_0"], div[role="textbox"]'), timeout_ms=8000)
            box.click(timeout=8000)
            box.fill(marker, timeout=8000)
            _click_post_button(page)
            post_result = _wait_post_result(page)
            result["post_url"] = post_result.get("x_url", "")
            result["current_url"] = page.url

            page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            href = _get_profile_href(page)
            result["profile_href"] = href
            if href:
                result["profile_url"] = f"https://x.com{href}"
                page.goto(result["profile_url"], wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
                result["post_found_on_profile"] = _find_post_on_profile(page, marker)
            result["screenshot"] = _screenshot(page, "x_smoke_profile_check")
            result["ok"] = True
        except Exception as exc:
            result["error"] = str(exc)
            result["current_url"] = getattr(page, "url", "")
            result["screenshot"] = _screenshot(page, "x_smoke_failed")
        finally:
            context.close()
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
