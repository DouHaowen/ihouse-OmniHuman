import glob
import json
import os
import sys
from pathlib import Path

import app

pat = sys.argv[1] if len(sys.argv) > 1 else "*中国AI芯片创业公司*"
ds = sorted([d for d in glob.glob("/app/output/" + pat) if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
d = Path(ds[0])
print("dir:", d.name)
r = json.loads((d / "result.json").read_text())
print("compose 前 mp4:", [os.path.basename(p) for p in glob.glob(str(d) + "/**/*.mp4", recursive=True)])
print("=== 强制合成 ===", flush=True)
try:
    composed = app._compose_history_result(d, r, user=None, requested_aspect_ratio="", cost_scope="manual_force")
    print("合成完成")
except Exception as e:
    import traceback
    print("合成失败:", repr(e)[:200])
    traceback.print_exc()
    composed = r
mp4 = [os.path.basename(p) for p in glob.glob(str(d) + "/**/*.mp4", recursive=True)]
print("compose 后 mp4:", mp4)
if mp4:
    print("=== 发布 YouTube + Facebook ===", flush=True)
    wf = composed.get("workflow_config") or {}
    wf.setdefault("opennews_channel_id", "technology")
    wf.setdefault("target_market", "cn")
    composed["workflow_config"] = wf
    try:
        yt = app._publish_opennews_result_to_youtube(d, composed, aspects=["vertical"], privacy_status="public", include_language_versions=False)
        for rec in yt[:1]:
            print("YouTube:", rec.get("youtube_url"))
    except Exception as e:
        print("YouTube 失败:", repr(e)[:120])
    try:
        fb = app._publish_opennews_result_to_facebook(d, composed, aspects=["vertical"], include_language_versions=False)
        for rec in fb[:1]:
            print("Facebook: post_id=%s" % (rec.get("post_id") or rec.get("video_id")))
    except Exception as e:
        print("Facebook 失败:", repr(e)[:120])
    app._save_result_to_output_dir(d, composed)
