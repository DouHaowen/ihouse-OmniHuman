import json

from playwright.sync_api import sync_playwright

from x_browser_publisher import _resolve_browser_executable, x_browser_profile_dir


def main() -> None:
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(x_browser_profile_dir()),
            executable_path=_resolve_browser_executable() or None,
            headless=True,
            viewport={"width": 1400, "height": 1400},
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
        page.goto("https://x.com/opennewsagent", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        top = page.locator('article[data-testid="tweet"]').nth(0)
        text = top.inner_text(timeout=5000)
        links = top.locator('a[href^="/opennewsagent/status/"]')
        href = links.nth(0).get_attribute("href") if links.count() else ""
        has_video = top.locator("video").count() > 0
        has_media = top.locator('video, div[data-testid="tweetPhoto"], img[src*="twimg"]').count() > 0
        media_texts = []
        for i in range(min(top.locator("span").count(), 20)):
            try:
                value = (top.locator("span").nth(i).inner_text(timeout=500) or "").strip()
                if value:
                    media_texts.append(value)
            except Exception:
                pass
        print(json.dumps({
            "href": href,
            "has_video": has_video,
            "has_media": has_media,
            "text": text[:1000],
            "span_texts": media_texts,
        }, ensure_ascii=False, indent=2))
        ctx.close()


if __name__ == "__main__":
    main()
