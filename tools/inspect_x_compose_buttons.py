from pathlib import Path
import json

from playwright.sync_api import sync_playwright

from x_browser_publisher import (
    _fill_post_text,
    _resolve_browser_executable,
    _upload_video_file,
    _wait_video_ready,
    x_browser_profile_dir,
)


VIDEO_PATH = Path("/app/output/1782899978_full_OpenNewsCPEC20开启巴/final_edit/final_video_vertical.mp4")
TEXT = "CPEC 2.0开启巴基斯坦数字未来\n来源：按钮检查\n#OpenNews #iHouse"


def main() -> None:
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
        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        _fill_post_text(page, TEXT)
        _upload_video_file(page, VIDEO_PATH)
        _wait_video_ready(page, VIDEO_PATH.name)
        dialog = page.locator('div[role="dialog"]').first
        buttons = dialog.locator("button, [role='button']") if dialog.count() else page.locator("button, [role='button']")
        rows = []
        for i in range(min(buttons.count(), 40)):
            b = buttons.nth(i)
            try:
                txt = (b.inner_text(timeout=1000) or "").strip()
                testid = b.get_attribute("data-testid")
                if not txt and not testid:
                    continue
                box = b.bounding_box() or {}
                rows.append(
                    {
                        "i": i,
                        "text": txt,
                        "testid": testid,
                        "disabled": b.get_attribute("disabled"),
                        "aria_disabled": b.get_attribute("aria-disabled"),
                        "tag": b.evaluate("(el) => el.tagName"),
                        "class": b.get_attribute("class"),
                        "x": box.get("x"),
                        "y": box.get("y"),
                        "w": box.get("width"),
                        "h": box.get("height"),
                        "html": b.evaluate("(el) => el.outerHTML.slice(0, 600)"),
                    }
                )
            except Exception:
                pass
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        ctx.close()


if __name__ == "__main__":
    main()
