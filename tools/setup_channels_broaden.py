"""拓宽所有频道关键词(清空收窄 keyword,靠类目 OR 词库放量) + 新增"房产移民"频道。

- keyword 与类目内置 OR 词是 AND 关系,填得越多越窄 -> 一律清空,交给类目词库放量。
- 科技频道:保持启用,24h / 60min / limit 30。
- military/politics/finance:清空 keyword(拓宽),启用状态不变。
- 新增 real_estate_immigration 频道:启用,youtube 平台,24h / 60min / limit 30。
"""
import json, time
import app

p = app.OPENNEWS_CHANNELS_CONFIG_PATH
cfg = json.loads(p.read_text(encoding="utf-8"))
cfg["scheduler_enabled"] = True
channels = cfg.get("channels", [])

by_id = {c.get("id"): c for c in channels}

# 1) 现有频道:清空收窄的 keyword
for c in channels:
    old_kw = c.get("keyword")
    c["keyword"] = ""
    if old_kw:
        print(f"[清空keyword] {c.get('id')}: {old_kw!r} -> ''")

# 2) 科技频道放量参数
if "technology" in by_id:
    t = by_id["technology"]
    t["enabled"] = True
    t["time_range"] = "24h"
    t["interval_minutes"] = 60
    t["limit"] = 30
    t["produce_limit"] = 3
    t.setdefault("platforms", {})
    t["platforms"]["youtube"] = True
    t["next_run_at"] = 0
    print("[科技] enabled/24h/60min/limit30 已设置")

# 3) 新增 房产移民 频道
if "real_estate_immigration" not in by_id:
    new_raw = {
        "id": "real_estate_immigration",
        "name": "房产移民",
        "enabled": True,
        "category": "real_estate_immigration",
        "keyword": "",
        "time_range": "24h",
        "interval_minutes": 60,
        "limit": 30,
        "produce_limit": 3,
        "platforms": {"x": False, "facebook": False, "youtube": True},
    }
    normalized = app._normalize_opennews_channel(new_raw, None)
    normalized["next_run_at"] = 0
    normalized.setdefault("created_at", int(time.time()))
    normalized["updated_at"] = int(time.time())
    channels.append(normalized)
    print("[新增] 房产移民 real_estate_immigration:", normalized.get("category"), "| platforms=", normalized.get("platforms"))
else:
    r = by_id["real_estate_immigration"]
    r["enabled"] = True
    r["next_run_at"] = 0
    print("[房产移民] 已存在,置为启用")

cfg["channels"] = channels
p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
print("\n=== 写入完成,当前频道 ===")
for c in cfg["channels"]:
    print(f"  {c.get('id'):24s} enabled={c.get('enabled')} category={c.get('category')} time_range={c.get('time_range')} kw={c.get('keyword')!r} yt={(c.get('platforms') or {}).get('youtube')}")
print("now=", int(time.time()))
