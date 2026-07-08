"""细看 X 合成器上传视频后的真实状态，定位 media failed 根因。"""
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
print("video:", str(video), os.path.getsize(video)//1024, "KB", flush=True)

sync_playwright, _ = xb._import_playwright()
display = os.environ.get("DISPLAY") or xb._ensure_publish_display()
env = {**os.environ, "DISPLAY": display} if display else None
print("display:", display, flush=True)
with sync_playwright() as pw:
    ctx = pw.chromium.launch_persistent_context(
        str(xb.x_browser_profile_dir()),
        executable_path=xb._resolve_browser_executable() or None,
        headless=False, env=env,
        viewport={"width": 1365, "height": 900}, locale="ja-JP", timezone_id="Asia/Tokyo",
        args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    try:
        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        # 填文本
        try:
            xb._fill_post_text(page, "iHouse probe %s" % time.strftime("%H:%M:%S"))
            print("文本已填", flush=True)
        except Exception as e:
            print("填文本失败:", repr(e)[:120], flush=True)
        # 传视频
        xb._upload_video_file(page, Path(video))
        print("已调用 set_input_files", flush=True)
        for i in range(12):
            page.wait_for_timeout(5000)
            info = {}
            for name, sel in [
                ("attachments", 'div[data-testid="attachments"]'),
                ("blobimg", 'img[src^="blob:"]'),
                ("video", 'video'),
                ("removeMedia", '[data-testid="removeMedia"]'),
                ("progressbar", 'div[role="progressbar"]'),
                ("tweetButton", 'button[data-testid="tweetButton"], div[data-testid="tweetButton"]'),
            ]:
                try:
                    info[name] = page.locator(sel).count()
                except Exception:
                    info[name] = "err"
            # 发帖按钮是否可用
            try:
                btn = page.locator('button[data-testid="tweetButton"], div[data-testid="tweetButton"]').first
                info["btn_enabled"] = btn.is_enabled(timeout=1500)
            except Exception:
                info["btn_enabled"] = "err"
            # 错误/提示文本
            body = ""
            try:
                body = page.locator("body").inner_text(timeout=3000).lower()
            except Exception:
                pass
            marks = [m for m in ("media failed", "upload failed", "処理中", "processing", "uploading", "not valid", "できません", "サポート", "unsupported", "too long", "file size") if m in body]
            print("t=%02ds %s marks=%s" % (i*5, info, marks), flush=True)
            if info.get("removeMedia") and info.get("btn_enabled") is True and not info.get("progressbar"):
                print(">>> 看起来就绪了", flush=True)
                break
        page.screenshot(path="output/x_browser/screenshots/probe_final.png")
        print("截图: output/x_browser/screenshots/probe_final.png", flush=True)
    finally:
        ctx.close()
