import app, property_auto
# 关调度器
cfg = app._load_property_auto_config()
cfg["scheduler_enabled"] = False
app._save_property_auto_config(cfg)
print("已关闭房源调度器")
# 对账（成片存在→done）
st = property_auto.load_property_state(str(app.PROPERTY_AUTO_DIR))
changed = app._reconcile_property_auto_state(st, max_attempts=3)
if changed:
    property_auto.save_property_state(str(app.PROPERTY_AUTO_DIR), st)
for rid, v in (st.get("records") or {}).items():
    print("  ", v.get("status"), "|", v.get("name"))
