"""受控演示:清空 seen.json 强制生产 1 条科技新闻(中日英)并发布 X/FB/YouTube。
安全措施:produce_limit 已临时设 1;fetch 后立即恢复 seen.json;进程保活到生产+发布完成。
用法(容器内 nohup 后台):cd /app && PYTHONPATH=/app nohup python3 tools/demo_full_batch.py > /app/output/demo_full_batch.log 2>&1 &
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

# 记录触发前已存在的产出目录,便于识别新产出
before = set(p.name for p in Path("/app/output").iterdir() if p.is_dir())

# 1) 清空 seen.json
json.dump({}, open(f"{D}/seen.json", "w"))
log("seen.json cleared")

# 2) 载入 tech 频道(produce_limit 已=1)
ch = dict(app._find_opennews_channel("technology", include_secrets=True))
ch["time_range"] = "24h"
log("tech produce_limit=%s languages=%s" % (ch.get("produce_limit"), ch.get("languages")))

# 3) 触发抓取->生产(生产在守护线程里跑)
res = app._run_opennews_channel_fetch(ch, triggered_by="manual_seen_clear_demo")
log("fetch result: %s" % json.dumps({k: res.get(k) for k in ("ok", "running", "message")}, ensure_ascii=False))

# 4) 立即恢复 seen.json,避免 scheduler 再把旧闻当新的
shutil.copy(BAK, f"{D}/seen.json")
log("seen.json restored")

# 5) 保活并监控:等待新产出目录出现并完成发布
deadline = time.time() + 60 * 50  # 最多 50 分钟
last = ""
while time.time() < deadline:
    time.sleep(45)
    now_dirs = set(p.name for p in Path("/app/output").iterdir() if p.is_dir())
    new_dirs = [n for n in (now_dirs - before) if "OpenNews" in n or "opennews" in n.lower()]
    status = "new_dirs=%s" % (new_dirs or "(尚无)")
    if status != last:
        log(status)
        last = status
    # 若有新产出且其 result.json 带发布记录,汇报并结束
    done = False
    for n in new_dirs:
        rj = Path("/app/output") / n / "result.json"
        if not rj.exists():
            continue
        try:
            r = json.loads(rj.read_text())
        except Exception:
            continue
        yt = r.get("youtube_publish_records") or []
        x = r.get("x_publish_records") or []
        fb = r.get("facebook_publish_records") or []
        langs = r.get("language_versions") or []
        if yt or x or fb:
            log("PRODUCED+PUBLISHED dir=%s langs=%d x=%d fb=%d yt=%d" % (n, len(langs) + 1, len(x), len(fb), len(yt)))
            for rec in yt[:1]:
                log("  YouTube: %s" % rec.get("youtube_url"))
            for rec in x[:1]:
                log("  X: %s" % (rec.get("x_url") or rec.get("url") or rec.get("post_id")))
            for rec in fb[:1]:
                log("  FB: %s" % (rec.get("post_id") or rec.get("video_id")))
            done = True
    if done:
        break

log("=== demo waiter exit ===")
