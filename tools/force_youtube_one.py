"""强制对一条已合成 OpenNews 视频发布 YouTube，验证上传链路。"""
import glob, json, os, sys
from pathlib import Path
import app

pat = sys.argv[1] if len(sys.argv) > 1 else "*full*OpenNews*"
ds = sorted([d for d in glob.glob("output/" + pat) if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
target = None
for d in ds:
    r = json.load(open(os.path.join(d, "result.json")))
    if r.get("final_video_path"):
        target = (Path(d).resolve(), r)
        break
if not target:
    print("没有找到已合成的视频"); raise SystemExit
od, r = target
print("dir:", od.name)
wf = r.get("workflow_config") or {}
print("workflow youtube_auto_publish =", wf.get("youtube_auto_publish"))
print("已有 youtube_publish_records =", len(r.get("youtube_publish_records") or []))
print("=== 强制发布 YouTube (public) ===", flush=True)
try:
    recs = app._publish_opennews_result_to_youtube(
        od, r, aspects=["vertical"], privacy_status="public", include_language_versions=False,
    )
    for rec in recs[:2]:
        print("✅ YouTube:", rec.get("youtube_url") or rec.get("video_id"))
    if not recs:
        print("⚠ 无记录返回")
    app._save_result_to_output_dir(od, r)
except Exception as e:
    import traceback
    print("❌ 失败:", repr(e)[:200])
    traceback.print_exc()
