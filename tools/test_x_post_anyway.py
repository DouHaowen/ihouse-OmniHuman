"""附视频→等上传→直接点 Post(不等本地预览),看发出的推文带不带视频。"""
import glob, json, os, time
from pathlib import Path
import app
import x_browser_publisher as xb

ds = sorted([d for d in glob.glob("output/*full*OpenNews*") if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
video = None
for d in ds:
    r = json.load(open(os.path.join(d, "result.json")))
    if r.get("final_video_path"):
        try:
            video = app._resolve_youtube_publish_video(Path(d).resolve(), r, aspect_ratio="vertical")
            break
        except Exception:
            continue
print("video:", str(video), flush=True)
sync_playwright, _ = xb._import_playwright()
display = os.environ.get("DISPLAY") or xb._ensure_publish_display()
env = {**os.environ, "DISPLAY": display} if display else None
with sync_playwright() as pw:
    ctx = pw.chromium.launch_persistent_context(
        str(xb.x_browser_profile_dir()), executable_path=xb._resolve_browser_executable() or None,
        headless=False, env=env, viewport={"width": 1365, "height": 900}, locale="ja-JP", timezone_id="Asia/Tokyo",
        args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    try:
        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        existing = xb._collect_status_hrefs(page)
        handle = xb._get_profile_href(page).lstrip("/").strip()
        xb._fill_post_text(page, "iHouse OpenNews 测试 %s" % time.strftime("%H:%M"))
        xb._upload_video_file(page, Path(video))
        print("已附视频，等 28s 让字节上传...", flush=True)
        page.wait_for_timeout(28000)
        # dump 媒体容器元素
        info = page.evaluate("""() => {
            const box = document.querySelector('[data-testid="attachments"], [aria-label*="Media"], [role="progressbar"]');
            const inputs = [...document.querySelectorAll('input[type=file]')].map(i=>({accept:i.accept, testid:i.getAttribute('data-testid')}));
            const prog = document.querySelectorAll('[role="progressbar"]').length;
            return {hasBox: !!box, inputs, progressbars: prog};
        }""")
        print("DOM:", json.dumps(info, ensure_ascii=False), flush=True)
        print("直接点 Post...", flush=True)
        xb._submit_post(page)
        res = xb._wait_post_result(page, expected_handle=handle, existing_status_hrefs=existing, existing_profile_status_hrefs=set(), text="")
        print("结果:", json.dumps(res, ensure_ascii=False)[:400], flush=True)
        page.wait_for_timeout(3000)
        page.screenshot(path="output/x_browser/screenshots/post_anyway.png")
    except Exception as e:
        import traceback
        print("失败:", repr(e)[:200])
        traceback.print_exc()
        try: page.screenshot(path="output/x_browser/screenshots/post_anyway_fail.png")
        except Exception: pass
    finally:
        ctx.close()
