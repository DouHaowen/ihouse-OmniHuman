"""在 Xvfb :99 上用 Playwright 自带 chromium(headed)打开 X 登录页,保持一小时供人工登录。
用法(容器内): DISPLAY=:99 cd /app && PYTHONPATH=/app nohup python3 tools/x_login_headed.py &
"""
import os
import time

os.environ["DISPLAY"] = ":99"
from playwright.sync_api import sync_playwright

PROFILE = "/app/output/x_browser/profile"
for lock in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
    try:
        os.unlink(os.path.join(PROFILE, lock))
    except Exception:
        pass

p = sync_playwright().start()
ctx = p.chromium.launch_persistent_context(
    PROFILE,
    headless=False,
    args=["--no-sandbox", "--disable-dev-shm-usage", "--start-maximized"],
    viewport={"width": 1360, "height": 880},
)
page = ctx.pages[0] if ctx.pages else ctx.new_page()
page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=60000)
print("login window ready, url:", page.url, flush=True)
# 保持窗口一小时,供人工在 noVNC 里登录
time.sleep(3600)
