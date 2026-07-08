import glob, json, os
from pathlib import Path
import app

print("=== per-channel YouTube 路径 helper ===")
p = app._opennews_channel_account_token_path("technology", "cn", "youtube")
print("technology/cn ->", p)

print("\n=== 当前 technology/cn YouTube 账号槽 ===")
acc = app._opennews_publish_account_for("technology", "cn", "youtube")
a = acc.get("account") or {}
print("enabled:", acc.get("enabled"), "| channel_name:", a.get("channel_name"), "| token_store_path:", a.get("token_store_path") or "(空=回退全局)")
print("发布将用 token:", app._opennews_youtube_token_path_for("technology", "cn"))

print("\n=== 扫描已产出结果里 opennews_channel_id 的分布 ===")
counts = {}
withch = []
for d in sorted(glob.glob("output/*full*OpenNews*"), key=os.path.getmtime, reverse=True)[:40]:
    rj = os.path.join(d, "result.json")
    if not os.path.exists(rj):
        continue
    try:
        r = json.load(open(rj))
    except Exception:
        continue
    wf = r.get("workflow_config") or {}
    cid = wf.get("opennews_channel_id")
    counts[str(cid)] = counts.get(str(cid), 0) + 1
    if cid and str(cid) != "None":
        withch.append((os.path.basename(d), cid, len(r.get("youtube_publish_records") or [])))
print("channel_id 分布(最近40条):", counts)
print("带频道ID的样例(dir, channel_id, youtube记录数):")
for x in withch[:6]:
    print("  ", x)
