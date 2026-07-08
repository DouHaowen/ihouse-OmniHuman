import app, property_auto, glob, os
st = property_auto.load_property_state(str(app.PROPERTY_AUTO_DIR))
recs = st.get("records") or {}
print("状态记录数:", len(recs))
for rid, v in recs.items():
    d = (v.get("output_dir") or "").split("/")[-1]
    print("  status=", v.get("status"), "| name=", v.get("name"), "| task=", v.get("last_task") or "-", "| dir=", d)
cfg = app._load_property_auto_config()
print("scheduler_enabled:", cfg.get("scheduler_enabled"), "| next_run_at:", int(cfg.get("next_run_at") or 0), "| last_msg:", cfg.get("last_run_message"))
print("运行中任务:", app._find_running_property_auto_task())
ds = sorted(glob.glob("/app/output/*property_video*"), key=os.path.getmtime, reverse=True)[:3]
print("最新房源视频目录:")
for d in ds:
    fin = bool(app._property_video_final_exists(__import__("pathlib").Path(d)))
    print("  ", os.path.basename(d), "| 有成片:", fin)
