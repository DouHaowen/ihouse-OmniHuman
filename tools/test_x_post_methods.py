from pathlib import Path
import json
import sys
import time

from playwright.sync_api import sync_playwright

from x_browser_publisher import (
    _fill_post_text,
    _resolve_browser_executable,
    _upload_video_file,
    _wait_video_ready,
    x_browser_profile_dir,
)


VIDEO_PATH = Path("/app/output/1782899978_full_OpenNewsCPEC20开启巴/final_edit/final_video_vertical.mp4")
TEXT = "CPEC 2.0开启巴基斯坦数字未来\n来源：提交方式测试\n#OpenNews #iHouse"


def recent_events(events: list[dict]) -> list[dict]:
    keep = []
    for event in events:
        url = event.get("url", "")
        if any(key in url for key in ["CreateTweet", "CreatePost", "media/upload", "graphql"]):
            keep.append(event)
    return keep[-30:]


def run(method: str) -> None:
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(x_browser_profile_dir()),
            executable_path=_resolve_browser_executable() or None,
            headless=True,
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
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        events: list[dict] = []

        def on_response(resp):
            url = resp.url
            if any(k in url for k in ["CreateTweet", "CreatePost", "tweet", "media", "graphql"]):
                try:
                    body = resp.text()[:1200]
                except Exception as exc:
                    body = f"<resp text error {exc}>"
                events.append({"type": "response", "url": url, "status": resp.status, "body": body})

        page.on("response", on_response)
        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        _fill_post_text(page, TEXT)
        _upload_video_file(page, VIDEO_PATH)
        _wait_video_ready(page, VIDEO_PATH.name)
        button = page.locator('div[role="dialog"] button[data-testid="tweetButton"]').first
        box = button.bounding_box() or {}
        if method == "click":
            button.click(timeout=10000)
        elif method == "force_click":
            button.click(timeout=10000, force=True)
        elif method == "mouse":
            page.mouse.click(box.get("x", 0) + box.get("width", 0) / 2, box.get("y", 0) + box.get("height", 0) / 2)
        elif method == "dom_click":
            button.evaluate("(el) => el.click()")
        elif method == "enter_shortcut":
            page.keyboard.press("Meta+Enter")
        elif method == "ctrl_enter":
            page.keyboard.press("Control+Enter")
        else:
            raise SystemExit(f"unknown method: {method}")
        page.wait_for_timeout(15000)
        result = {
            "method": method,
            "url": page.url,
            "body": (page.locator("body").inner_text(timeout=5000) or "")[:3000],
            "events": recent_events(events),
        }
        print(json.dumps(result, ensure_ascii=False))
        ctx.close()


if __name__ == "__main__":
    run(sys.argv[1])
