"""实测某频道某语言三平台的真实绑定与可发布性。
用法(容器内): PYTHONPATH=/app python3 tools/diag_channel_publish.py technology cn
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

import app

channel_id = sys.argv[1] if len(sys.argv) > 1 else "technology"
language = sys.argv[2] if len(sys.argv) > 2 else "cn"


def line(*a):
    print(*a, flush=True)


cfg = app._load_opennews_channels_config(include_secrets=True)
ch = None
for c in cfg.get("channels", []):
    if str(c.get("id")) == channel_id:
        ch = c
        break
if not ch:
    line("找不到频道", channel_id)
    raise SystemExit

acc = (ch.get("accounts") or {}).get(language) or {}
line("=== 频道 %s / 语言 %s ===" % (channel_id, language))
line("频道 enabled:", ch.get("enabled"), "| platforms:", ch.get("platforms"), "| languages:", ch.get("languages"))

# ---------- YouTube ----------
line("\n--- YouTube ---")
yt = acc.get("youtube") or {}
line("slot:", {k: yt.get(k) for k in ("enabled", "channel_name", "token_store_path")})
tok = str(yt.get("token_store_path") or "").strip()
if not yt.get("enabled"):
    line("YouTube: 该语言未启用")
elif not tok:
    line("YouTube: 用全局 token", str(app.YOUTUBE_TOKEN_STORE_PATH))
    tok = str(app.YOUTUBE_TOKEN_STORE_PATH)
if tok:
    p = Path(tok)
    line("token 文件存在:", p.exists())
    if p.exists():
        try:
            from youtube_publisher import get_youtube_channel
            info = get_youtube_channel(p)
            line("✅ YouTube API 可用 → 频道:", info.get("title"), "| id:", info.get("channel_id"))
        except Exception as e:
            line("❌ YouTube API 调用失败:", repr(e)[:200])

# ---------- X ----------
line("\n--- X ---")
x = acc.get("x") or {}
line("slot:", {k: x.get(k) for k in ("enabled", "binding_mode", "account_label", "handle", "profile_dir")})
pdir = str(x.get("profile_dir") or "").strip()
if pdir:
    prof = Path(pdir)
    line("profile 目录存在:", prof.exists())
    # 查 Cookies 里的 auth_token(登录凭据)
    found = False
    for ck in list(prof.rglob("Cookies"))[:6]:
        try:
            con = sqlite3.connect("file:%s?mode=ro&immutable=1" % ck, uri=True)
            rows = con.execute(
                "SELECT host_key,name FROM cookies WHERE name IN ('auth_token','ct0') AND (host_key LIKE '%x.com' OR host_key LIKE '%twitter.com')"
            ).fetchall()
            con.close()
            names = sorted({r[1] for r in rows})
            hosts = sorted({r[0] for r in rows})
            if names:
                found = True
                line("✅ 找到登录 Cookie:", names, "| host:", hosts, "| 文件:", str(ck))
        except Exception as e:
            line("读取 Cookies 失败:", str(ck), repr(e)[:100])
    if not found:
        line("❌ 未找到 x.com/twitter.com 的 auth_token,可能没真正登录成功")

# ---------- Facebook ----------
line("\n--- Facebook ---")
fb = acc.get("fb") if isinstance(acc.get("fb"), dict) else acc.get("facebook") or {}
line("slot:", {k: (("***" if k == "page_access_token" and fb.get(k) else fb.get(k))) for k in ("enabled", "binding_mode", "page_name", "page_id", "page_access_token")})
fpath = app._opennews_channel_account_token_path(channel_id, language, "facebook")
line("per-channel FB token 文件:", str(fpath), "存在:", fpath.exists())
if not (fb.get("page_id") and fb.get("page_access_token")):
    line("❌ Facebook 未绑定:缺 page_id / page_access_token → 需要点「授权 Facebook」完成 OAuth 并选择 Page")
else:
    line("✅ Facebook 槽内有 page_id + token,尝试校验...")
    try:
        from facebook_publisher import get_facebook_page
        info = get_facebook_page(fpath) if fpath.exists() else None
        line("Facebook Page:", info)
    except Exception as e:
        line("Facebook 校验失败:", repr(e)[:200])
line("\n=== done ===")
