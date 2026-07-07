"""端到端实测:对一条已生产的 technology 中文成片跑真实自动发布(X+FB+YouTube)。
选取标准:channel=technology、target_market=cn、有竖屏成片、尚未发过 YouTube。
用法:cd /app && PYTHONPATH=/app python3 tools/test_opennews_autopublish.py
"""
import json
import sys
from pathlib import Path

import app

base = Path("/app/output")
picked = None
for d in sorted((p for p in base.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True):
    rj = d / "result.json"
    if not rj.exists():
        continue
    try:
        r = json.loads(rj.read_text(encoding="utf-8"))
    except Exception:
        continue
    cid = app._opennews_result_channel_id(r)
    wf = r.get("workflow_config") or {}
    market = str(wf.get("target_market") or "cn")
    if cid != "technology" or market != "cn":
        continue
    if not app._opennews_result_has_publishable_video(d, r):
        continue
    n_x = len(r.get("x_publish_records") or [])
    n_fb = len(r.get("facebook_publish_records") or [])
    n_yt = len(r.get("youtube_publish_records") or [])
    already_yt = bool(n_yt)
    # 记录第一个可用的作为兜底
    if picked is None:
        picked = (d, r, already_yt)
    # 优先选完全没发过任何平台的,避免重复发帖
    if n_x == 0 and n_fb == 0 and n_yt == 0:
        picked = (d, r, already_yt)
        break

if not picked:
    print("未找到合适的 technology/cn 成片")
    sys.exit(1)

output_dir, r, already_yt = picked
print("样本:", output_dir.name)
print("标题:", str(r.get("title") or r.get("topic") or "")[:50])
print("发布前状态: x_records=%d fb_records=%d yt_records=%d already_yt=%s" % (
    len(r.get("x_publish_records") or []),
    len(r.get("facebook_publish_records") or []),
    len(r.get("youtube_publish_records") or []),
    already_yt,
))
print("=== 运行真实自动发布 _auto_publish_opennews_result_data ===", flush=True)
res = app._auto_publish_opennews_result_data(output_dir, r)
print("x_error:", res.get("x_error") or "(无)")
print("facebook_error:", res.get("facebook_error") or "(无)")
print("youtube_error:", res.get("youtube_error") or "(无)")
yt = res.get("youtube_records") or []
x = res.get("x_records") or []
fb = res.get("facebook_records") or []
print("--- YouTube 发布记录 ---")
for rec in yt:
    print("  format=%s market=%s id=%s url=%s" % (
        rec.get("youtube_format"), rec.get("target_market"),
        rec.get("video_id"), rec.get("youtube_url"),
    ))
print("--- X 发布记录 ---")
for rec in x:
    print("  market=%s id=%s url=%s" % (rec.get("target_market"), rec.get("post_id"), rec.get("x_url") or rec.get("url")))
print("--- Facebook 发布记录 ---")
for rec in fb:
    print("  market=%s id=%s" % (rec.get("target_market"), rec.get("post_id") or rec.get("video_id")))
