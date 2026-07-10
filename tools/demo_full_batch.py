"""受控验证:清 seen -> 强制生产 1 条科技新闻(中日英)-> 发 X/FB/YouTube,并盯 job 到完成。
用法(容器内 nohup):cd /app && PYTHONPATH=/app nohup python3 tools/demo_full_batch.py > /app/output/demo_full_batch.log 2>&1 &
"""
import json
import shutil
import time
from pathlib import Path

import app

D = "/app/output/opennews_batches"
BAK = f"{D}/seen.json.bak.20260707_demo"


def log(m):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), m), flush=True)


log("=== demo start ===")

# 触发前记录已存在 job id
def job_ids():
    return {j.get("job_id") for j in app.list_opennews_batch_jobs(app.OPENNEWS_BATCH_DIR, limit=10)}

before_jobs = job_ids()

# 1) 清 seen
json.dump({}, open(f"{D}/seen.json", "w"))
log("seen.json cleared")

# 2) 触发
ch = dict(app._find_opennews_channel("technology", include_secrets=True))
ch["time_range"] = "24h"
res = app._run_opennews_channel_fetch(ch, triggered_by="demo_verify")
log("fetch: %s" % json.dumps({k: res.get(k) for k in ("ok", "message")}, ensure_ascii=False))

# 3) 立即恢复 seen
shutil.copy(BAK, f"{D}/seen.json")
log("seen.json restored")

# 4) 找到新建的 job
new_job = None
for _ in range(20):
    diff = job_ids() - before_jobs
    if diff:
        new_job = sorted(diff)[-1]
        break
    time.sleep(3)
if not new_job:
    log("未发现新 job(可能没有可生产的新条目),退出")
    raise SystemExit
log("tracking job: %s" % new_job)

# 5) 轮询 job 到完成
deadline = time.time() + 60 * 40
last_msg = ""
while time.time() < deadline:
    j = app.load_opennews_batch_job(app.OPENNEWS_BATCH_DIR, new_job)
    st = j.get("status")
    msg = str(j.get("message") or "")
    if msg != last_msg:
        log("job status=%s | %s" % (st, msg[:70]))
        last_msg = msg
    if st in ("done", "completed", "partial", "failed", "error"):
        break
    time.sleep(20)

# 6) 报告产出 + 发布记录
j = app.load_opennews_batch_job(app.OPENNEWS_BATCH_DIR, new_job)
log("=== FINAL job status=%s ===" % j.get("status"))
for it in (j.get("items") or []):
    hid = it.get("history_id") or it.get("output_history_id") or it.get("result_history_id")
    log("item: %s | status=%s | history_id=%s" % (str(it.get("title") or "")[:36], it.get("status"), hid))
    if not hid:
        continue
    rj = Path("/app/output") / hid / "result.json"
    if not rj.exists():
        continue
    r = json.loads(rj.read_text())
    langs = [v.get("target_market") for v in (r.get("language_versions") or [])]
    x = r.get("x_publish_records") or []
    fb = r.get("facebook_publish_records") or []
    yt = r.get("youtube_publish_records") or []
    log("  langs(除主cn外)=%s | 主cn发布 X=%d FB=%d YouTube=%d" % (langs, len(x), len(fb), len(yt)))
    for rec in yt[:1]:
        log("  YouTube: %s" % rec.get("youtube_url"))
    for rec in x[:1]:
        log("  X: %s" % (rec.get("x_url") or rec.get("post_id")))
    for rec in fb[:1]:
        log("  FB: %s" % (rec.get("post_id") or rec.get("video_id")))
    if r.get("youtube_auto_publish_error"):
        log("  YT_err: %s" % str(r.get("youtube_auto_publish_error"))[:80])
    if r.get("x_auto_publish_error"):
        log("  X_err: %s" % str(r.get("x_auto_publish_error"))[:80])
log("=== demo done ===")
