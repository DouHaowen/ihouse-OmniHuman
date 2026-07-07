"""聚焦测试:确认签名修复后 X / Facebook 发布能跨过 TypeError 真正执行。
只测 X + Facebook,不碰 YouTube。用法:cd /app && PYTHONPATH=/app python3 tools/test_xfb_publish.py
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
    if app._opennews_result_channel_id(r) != "technology":
        continue
    if str((r.get("workflow_config") or {}).get("target_market") or "cn") != "cn":
        continue
    if not app._opennews_result_has_publishable_video(d, r):
        continue
    picked = (d, r)
    break

if not picked:
    print("未找到 technology/cn 成片")
    sys.exit(1)

output_dir, r = picked
print("样本:", output_dir.name, "|", str(r.get("title") or "")[:40])

print("=== X 发布 ===", flush=True)
try:
    recs = app._publish_opennews_result_to_x(output_dir, r, aspects=["vertical"], include_language_versions=False)
    print("X 成功记录数:", len(recs))
    for rec in recs:
        print("  post_id=%s url=%s" % (rec.get("post_id"), rec.get("x_url") or rec.get("url")))
except Exception as e:
    print("X 仍失败:", repr(e))

print("=== Facebook 发布 ===", flush=True)
try:
    recs = app._publish_opennews_result_to_facebook(output_dir, r, aspects=["vertical"], include_language_versions=False)
    print("FB 成功记录数:", len(recs))
    for rec in recs:
        print("  id=%s" % (rec.get("post_id") or rec.get("video_id")))
except Exception as e:
    print("FB 仍失败:", repr(e))
