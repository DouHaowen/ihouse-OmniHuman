import glob, json, os, sqlite3
from pathlib import Path
import app

print("========== ① Facebook 400 完整错误 ==========")
errs = []
for d in sorted(glob.glob("output/*full*OpenNews*"), key=os.path.getmtime, reverse=True):
    rj = os.path.join(d, "result.json")
    if not os.path.exists(rj):
        continue
    try:
        r = json.load(open(rj))
    except Exception:
        continue
    e = r.get("facebook_auto_publish_error")
    if e and "400" in str(e):
        print("dir:", os.path.basename(d))
        print(str(e)[:900])
        break

print("\n========== ② 最近 technology 自动批次是否盖 channel_id ==========")
bd = "output/opennews_batches"
jobs = sorted(glob.glob(bd + "/jobs/*.json") + glob.glob(bd + "/*job*.json"), key=os.path.getmtime, reverse=True)[:6]
if not jobs:
    # 兜底：全局找 job 文件
    jobs = sorted(glob.glob(bd + "/**/*.json", recursive=True), key=os.path.getmtime, reverse=True)[:12]
shown = 0
for j in jobs:
    try:
        data = json.load(open(j))
    except Exception:
        continue
    opts = data.get("options") if isinstance(data, dict) else None
    if isinstance(opts, dict) and ("opennews_channel_id" in opts or "youtube_auto_publish" in opts):
        print("job:", os.path.basename(j),
              "| channel_id=", repr(opts.get("opennews_channel_id")),
              "| youtube_auto=", opts.get("youtube_auto_publish"),
              "| x_auto=", opts.get("x_auto_publish"),
              "| fb_auto=", opts.get("facebook_auto_publish"))
        shown += 1
    if shown >= 5:
        break
if not shown:
    print("(未找到带 options 的批次 job 文件)")

print("\n========== ③ X technology_cn 登录状态 ==========")
prof = Path("output/x_browser/profiles/technology_cn")
print("profile 存在:", prof.exists())
found = []
for ck in list(prof.rglob("Cookies"))[:8]:
    try:
        con = sqlite3.connect("file:%s?mode=ro&immutable=1" % ck, uri=True)
        rows = con.execute("SELECT name,host_key FROM cookies WHERE name IN ('auth_token','ct0') AND (host_key LIKE '%x.com' OR host_key LIKE '%twitter.com')").fetchall()
        con.close()
        for name, host in rows:
            found.append((name, host, str(ck)))
    except Exception as ex:
        print("  读 Cookies 失败:", ck, repr(ex)[:80])
if found:
    for name, host, ck in found:
        print("  ✅", name, host, "|", ck)
else:
    print("  ❌ 未找到 auth_token（technology_cn 仍未登录）")
# 也看全局 profile
print("  --- 全局 profile ---")
gprof = Path("output/x_browser/profile")
gfound = []
for ck in list(gprof.rglob("Cookies"))[:8]:
    try:
        con = sqlite3.connect("file:%s?mode=ro&immutable=1" % ck, uri=True)
        rows = con.execute("SELECT name,host_key FROM cookies WHERE name='auth_token' AND (host_key LIKE '%x.com' OR host_key LIKE '%twitter.com')").fetchall()
        con.close()
        for name, host in rows:
            gfound.append((host, str(ck)))
    except Exception:
        pass
print("  全局 auth_token:", gfound if gfound else "无")
