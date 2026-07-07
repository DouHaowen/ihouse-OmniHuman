"""干跑验证:确认 OpenNews 自动发布路径对 YouTube 的门禁判定正确。
不实际发帖,只打印决策。用法:docker exec OmniHuman python /app/tools/verify_youtube_wiring.py
"""
import json
from pathlib import Path

import app

base = Path("/app/output")
picked = None
for d in sorted(
    (p for p in base.iterdir() if p.is_dir()),
    key=lambda p: p.stat().st_mtime,
    reverse=True,
):
    rj = d / "result.json"
    if not rj.exists():
        continue
    try:
        r = json.loads(rj.read_text(encoding="utf-8"))
    except Exception:
        continue
    wf = r.get("workflow_config") or {}
    if wf.get("opennews_channel_id") or r.get("opennews_channel_id") or wf.get("opennews_channel"):
        picked = (d.name, r)
        break

if not picked:
    print("未找到带频道信息的 OpenNews 结果")
    raise SystemExit

name, r = picked
wf = r.get("workflow_config") or {}
cid = app._opennews_result_channel_id(r)
print("样本:", name)
print("workflow target_market:", wf.get("target_market"))
print("解析 channel_id:", cid)
print("youtube default / disabled:", app._opennews_youtube_auto_publish_default(), app._opennews_youtube_auto_publish_disabled())
print("youtube 语言版本开关:", app._opennews_youtube_publish_language_versions_enabled())
print("--- YouTube 账号门禁 (enabled=会发) ---")
for mkt in ("cn", "jp", "en"):
    en = app._opennews_publish_account_for(cid, mkt, "youtube").get("enabled")
    print("  {}/{}/youtube -> enabled={}".format(cid, mkt, en))
print("--- 对照 X / Facebook (cn) ---")
for plat in ("x", "facebook"):
    en = app._opennews_publish_account_for(cid, "cn", plat).get("enabled")
    print("  {}/cn/{} -> enabled={}".format(cid, plat, en))
print("--- 对照:非目标频道 general 的 youtube 门禁(应为 False)---")
for mkt in ("cn",):
    en = app._opennews_publish_account_for("general", mkt, "youtube").get("enabled")
    print("  general/{}/youtube -> enabled={}".format(mkt, en))
