import json, time, app
p = app.OPENNEWS_CHANNELS_CONFIG_PATH
cfg = json.loads(p.read_text(encoding="utf-8"))
cfg["scheduler_enabled"] = True
for c in cfg.get("channels", []):
    if c.get("id") == "technology":
        c["enabled"] = True
        c["time_range"] = "24h"        # 抓取窗口放宽到 24 小时
        c["interval_minutes"] = 60      # 间隔缩短到 60 分钟
        c["limit"] = 30                 # 候选量加大
        c["produce_limit"] = 3
        c["next_run_at"] = 0            # 立即触发一次
        c.setdefault("platforms", {})["youtube"] = True
        c["platforms"]["x"] = False
        c["platforms"]["facebook"] = False
        print("technology 已调整: time_range=24h interval=60 limit=30 next_run_at=0")
p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
print("now=", int(time.time()))
