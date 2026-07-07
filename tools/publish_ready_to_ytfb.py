"""把已产好但未发布的 OpenNews 科技成片,批量发布到 YouTube + Facebook(跳过 X)。
用法:cd /app && PYTHONPATH=/app nohup python3 tools/publish_ready_to_ytfb.py > /app/output/publish_ytfb.log 2>&1 &
"""
import json
import time
from pathlib import Path

import app

MAX_PUBLISH = 3  # 本次最多发几条


def log(m):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), m), flush=True)


log("=== 扫描可发布科技成片 ===")
base = Path("/app/output")
candidates = []
for d in sorted((p for p in base.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True):
    rj = d / "result.json"
    if not rj.exists():
        continue
    try:
        r = json.loads(rj.read_text())
    except Exception:
        continue
    cid = app._opennews_result_channel_id(r)
    if cid != "technology":
        continue
    if not app._opennews_result_has_publishable_video(d, r):
        continue
    if r.get("youtube_publish_records"):  # 已发过 YouTube 的跳过
        continue
    candidates.append((d, r))
    if len(candidates) >= MAX_PUBLISH:
        break

log("找到 %d 条待发布" % len(candidates))
for d, r in candidates:
    title = str(r.get("title") or r.get("topic") or "")[:40]
    log("--- 发布: %s (%s) ---" % (title, d.name[:30]))
    wf = r.get("workflow_config") or {}
    wf.setdefault("opennews_channel_id", "technology")
    wf.setdefault("target_market", "cn")
    r["workflow_config"] = wf
    # YouTube
    try:
        yt = app._publish_opennews_result_to_youtube(d, r, aspects=["vertical"], privacy_status="public", include_language_versions=False)
        for rec in yt[:1]:
            log("  ✅ YouTube: %s" % rec.get("youtube_url"))
        if not yt:
            log("  ⚠️ YouTube 无记录")
    except Exception as e:
        log("  ❌ YouTube 失败: %r" % e)
    # Facebook
    try:
        fb = app._publish_opennews_result_to_facebook(d, r, aspects=["vertical"], include_language_versions=False)
        for rec in fb[:1]:
            log("  ✅ Facebook: post_id=%s" % (rec.get("post_id") or rec.get("video_id")))
        if not fb:
            log("  ⚠️ Facebook 无记录")
    except Exception as e:
        log("  ❌ Facebook 失败: %r" % e)
    app._save_result_to_output_dir(d, r)
log("=== 完成 ===")
