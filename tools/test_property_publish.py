import glob, os
from pathlib import Path
import app

ds = [d for d in glob.glob("/app/output/*property_video*山王*") if os.path.isdir(d)]
if not ds:
    ds = sorted([d for d in glob.glob("/app/output/*property_video*") if os.path.isdir(d) and app._property_video_final_exists(Path(d))], key=os.path.getmtime, reverse=True)
if not ds:
    print("没找到已做好的房源视频"); raise SystemExit
od = Path(ds[0])
print("房源视频:", od.name)
r = app._load_result_from_output_dir(od) or {}
r.pop("youtube_publish_records", None)  # 允许重发测试
print("=== 发布到共用 YouTube 账号(Shorts)===", flush=True)
app._maybe_publish_property_video_to_youtube(od, r, title_hint="山王民宿 房源实拍")
r2 = app._load_result_from_output_dir(od) or {}
recs = r2.get("youtube_publish_records") or []
if recs:
    print("✅ 发布成功:", recs[0].get("youtube_url"))
else:
    print("❌ 未发布，错误:", r2.get("youtube_auto_publish_error"))
