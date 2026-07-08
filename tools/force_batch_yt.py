"""强制跑一批 technology / 中文 / 仅 YouTube 的自动化新闻(绕过去重，用于实测端到端）。
从最新 technology 批次取 3 条，走真实生产管线 + 自动发 YouTube。"""
import glob, json, os, time
from pathlib import Path
import app


def log(m):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), m), flush=True)


# 绕过两处去重（本次实测用）
app._opennews_is_duplicate_auto_event = lambda *a, **k: False

# 直接用最新 technology 批次文件里内联的条目对象
ds = sorted(glob.glob("/app/output/opennews_batches/batches/*technology*.json"), key=os.path.getmtime, reverse=True)
items = []
seen_ids = set()
for f in ds:
    b = json.load(open(f))
    for it in (b.get("items") or []):
        iid = it.get("id") or it.get("key")
        if iid and iid not in seen_ids and isinstance(it, dict):
            seen_ids.add(iid)
            items.append(it)
    if len(items) >= 3:
        break
items = items[:3]
if not items:
    log("批次文件里没有内联条目，退出"); raise SystemExit
for it in items:
    log("  待产: %s" % str(it.get("title") or "")[:50])

ch = app._find_opennews_channel("technology", include_secrets=True)
presenter = app._next_opennews_batch_presenter_config()
user = app._external_news_user()
options = {
    "target_market": "cn",
    "department_id": user.get("department_id") or "real_estate",
    "voice_preset_id": presenter.get("voice_preset_id", ""),
    "aspect_ratio": "vertical",
    "opennews_channel_id": "technology",
    "opennews_channel_name": ch.get("name") or "科技新闻",
    "opennews_language_markets": ["cn"],
    "opennews_presenter": presenter,
    "youtube_auto_publish": True,
    "youtube_privacy_status": "public",
    "youtube_aspects": ["vertical"],
    "x_auto_publish": False,
    "facebook_auto_publish": False,
    "material_strategy": "free_library_script_match",
}
job = app.create_opennews_batch_job(app.OPENNEWS_BATCH_DIR, username="force_demo", items=items, options=options)
job_id = job.get("job_id")
log("批次任务已建: %s" % job_id)

try:
    app._run_opennews_external_produce_job(
        job_id=job_id, user=dict(user), public_base_url="https://aiagent.office.ihousejapan.cn",
    )
    log("生产流程返回")
except Exception as e:
    import traceback
    log("生产异常: %r" % e); traceback.print_exc()

# 报告
j = app.load_opennews_batch_job(app.OPENNEWS_BATCH_DIR, job_id)
log("=== 批次最终 status=%s | %s ===" % (j.get("status"), str(j.get("message") or "")[:80]))
for it in (j.get("items") or []):
    hid = it.get("history_id") or it.get("output_history_id") or it.get("result_history_id")
    log("item status=%s history_id=%s" % (it.get("status"), hid))
    if not hid:
        continue
    rj = Path("/app/output") / hid / "result.json"
    if not rj.exists():
        continue
    r = json.loads(rj.read_text())
    yt = r.get("youtube_publish_records") or []
    log("  YouTube记录=%d" % len(yt))
    for rec in yt[:1]:
        log("  ✅ YouTube: %s" % rec.get("youtube_url"))
    if r.get("youtube_auto_publish_error"):
        log("  YT错误: %s" % str(r.get("youtube_auto_publish_error"))[:120])
log("=== DONE ===")
