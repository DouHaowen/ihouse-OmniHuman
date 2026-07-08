"""用系统 chromium(150,带 H.264)开远程调试，Playwright 经 CDP 连上，
看视频预览能否渲染(编解码是否解决)。若能→用这条路发 X。"""
import glob, json, os, subprocess, time, socket
from pathlib import Path
import app
import x_browser_publisher as xb

# 1) 视频
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

# 2) 虚拟显示
display = os.environ.get("DISPLAY") or xb._ensure_publish_display()
print("display:", display, flush=True)

# 3) 系统 chromium + 远程调试
syschrome = "/usr/lib/chromium/chromium"
profile = "/app/output/x_browser/cdp_profile"
# 复制全局登录 profile 的 cookies 到 cdp_profile? 先直接用全局 profile
profile = str(xb.x_browser_profile_dir())
subprocess.run("pkill -f chrome-linux/chrome; pkill -f /usr/lib/chromium/chromium; sleep 1", shell=True)
Path(profile + "/SingletonLock").unlink(missing_ok=True) if hasattr(Path, "unlink") else None
env = {**os.environ, "DISPLAY": display}
proc = subprocess.Popen(
    [syschrome, "--remote-debugging-port=9222", "--user-data-dir=" + profile,
     "--no-first-run", "--no-default-browser-check", "--disable-dev-shm-usage",
     "--no-sandbox", "--disable-setuid-sandbox", "--window-size=1365,900"],
    env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
)
# 等调试端口
ok = False
for _ in range(30):
    try:
        s = socket.create_connection(("127.0.0.1", 9222), timeout=1); s.close(); ok = True; break
    except Exception:
        time.sleep(0.5)
print("chromium 调试端口就绪:", ok, flush=True)
if not ok:
    print("系统 chromium 起不来"); raise SystemExit

sync_playwright, _ = xb._import_playwright()
with sync_playwright() as pw:
    try:
        browser = pw.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=15000)
        print("✅ CDP 连接成功!", flush=True)
    except Exception as e:
        print("❌ CDP 连接失败:", repr(e)[:150]); raise SystemExit
    ctx = browser.contexts[0] if browser.contexts else browser.new_context()
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    try:
        page.goto("https://x.com/compose/post", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        xb._fill_post_text(page, "iHouse CDP 测试 %s" % time.strftime("%H:%M"))
        xb._upload_video_file(page, Path(video))
        print("已附视频,观察预览是否渲染(编解码测试)...", flush=True)
        for i in range(9):
            page.wait_for_timeout(4000)
            try:
                nvideo = page.locator('video').count()
                nblob = page.locator('img[src^="blob:"]').count()
                nremove = page.locator('[data-testid="removeMedia"], button[aria-label*="Remove"]').count()
                natt = page.locator('div[data-testid="attachments"]').count()
            except Exception:
                nvideo = nblob = nremove = natt = -1
            print("t=%02ds video=%s blob=%s removeMedia=%s attachments=%s" % (i*4, nvideo, nblob, nremove, natt), flush=True)
            if nremove > 0 or natt > 0:
                print(">>> ✅ 视频预览出来了! 编解码 OK,这条路可行", flush=True)
                break
        page.screenshot(path="output/x_browser/screenshots/cdp_test.png")
    finally:
        try: browser.close()
        except Exception: pass
    subprocess.run("pkill -f /usr/lib/chromium/chromium", shell=True)
