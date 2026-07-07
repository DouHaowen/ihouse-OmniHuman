"""把已登录子档(含 auth_token)的 Cookies 复制到 Default,让发布器读到登录态。
用法: docker exec OmniHuman python3 /app/tools/x_copy_login_to_default.py
"""
import glob
import os
import shutil
import sqlite3

P = "/app/output/x_browser/profile"


def has_auth(cookies_path):
    try:
        con = sqlite3.connect(cookies_path)
        rows = con.execute(
            "SELECT name FROM cookies WHERE host_key LIKE '%x.com' AND name IN ('auth_token','ct0','twid')"
        ).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


# 找到含 auth_token 的源 Cookies
src = None
for f in glob.glob(P + "/**/Cookies", recursive=True):
    if "auth_token" in has_auth(f):
        src = f
        break
if not src:
    print("未找到含 auth_token 的子档"); raise SystemExit(1)
print("源(已登录):", src)

dst = os.path.join(P, "Default", "Cookies")
os.makedirs(os.path.dirname(dst), exist_ok=True)
# 备份并清理 Default 的旧 cookies + WAL
try:
    shutil.copy(dst, dst + ".bak")
except Exception:
    pass
for ext in ("", "-wal", "-journal"):
    try:
        os.unlink(dst + ext)
    except Exception:
        pass
# 复制源(含可能的 WAL)
shutil.copy(src, dst)
for ext in ("-wal", "-journal"):
    if os.path.exists(src + ext):
        shutil.copy(src + ext, dst + ext)
print("已复制到 Default")
print("Default 现有关键 cookie:", sorted(has_auth(dst)))
