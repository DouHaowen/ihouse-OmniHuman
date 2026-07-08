"""把一条已合成视频路由成 technology 频道，强制发 YouTube，验证“路由对就能发”。"""
import glob, json, os
from pathlib import Path
import app

ds = sorted([d for d in glob.glob("output/*full*OpenNews*") if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
target = None
for d in ds:
    r = json.load(open(os.path.join(d, "result.json")))
    if r.get("final_video_path") and not (r.get("youtube_publish_records")):
        target = (Path(d).resolve(), r)
        break
if not target:
    print("无合适视频"); raise SystemExit
od, r = target
print("dir:", od.name)
# 关键：把结果路由成 technology 频道
wf = r.get("workflow_config") or {}
wf["opennews_channel_id"] = "technology"
wf["opennews_channel_name"] = "科技新闻"
r["workflow_config"] = wf
ch = app._opennews_result_channel_id(r)
acc = app._opennews_publish_account_for(ch, "cn", "youtube")
print("路由 channel =", ch, "| youtube account enabled =", acc.get("enabled"), "| token =", acc.get("account", {}).get("token_store_path") or "全局")
print("=== 强制发 YouTube (public) ===", flush=True)
try:
    recs = app._publish_opennews_result_to_youtube(od, r, aspects=["vertical"], privacy_status="public", include_language_versions=False)
    for rec in recs[:2]:
        print("✅ YouTube:", rec.get("youtube_url") or rec.get("video_id"))
    if not recs:
        print("⚠ 仍无记录(账号可能未启用)")
    app._save_result_to_output_dir(od, r)
except Exception as e:
    import traceback
    print("❌ 失败:", repr(e)[:200]); traceback.print_exc()
