"""诊断产出结果为什么没带 opennews_channel_id（导致发布路由到 general）。"""
import glob, json, os
import app

ds = sorted([x for x in glob.glob("output/*full*OpenNews*") if os.path.isdir(x)], key=os.path.getmtime, reverse=True)[:5]
for d in ds:
    r = json.load(open(os.path.join(d, "result.json")))
    wf = r.get("workflow_config") or {}
    ch = app._opennews_result_channel_id(r)
    print("===", os.path.basename(d))
    print("  wf.opennews_channel_id =", repr(wf.get("opennews_channel_id")))
    print("  wf.opennews_channel(obj) =", repr((wf.get("opennews_channel") or {}).get("id") if isinstance(wf.get("opennews_channel"), dict) else None))
    print("  -> 路由结果 channel_id =", ch)
    print("  wf.target_market =", repr(wf.get("target_market")))
    print("  wf.youtube_auto_publish =", repr(wf.get("youtube_auto_publish")))

# 该路由频道的账号 enabled 情况
print("\n--- general 频道各平台 enabled(cn) ---")
for plat in ("youtube", "facebook", "x"):
    acc = app._opennews_publish_account_for("general", "cn", plat)
    print("  general/cn/%s enabled=%s" % (plat, acc.get("enabled")))
print("--- technology 频道各平台 enabled(cn) ---")
for plat in ("youtube", "facebook", "x"):
    acc = app._opennews_publish_account_for("technology", "cn", plat)
    print("  technology/cn/%s enabled=%s" % (plat, acc.get("enabled")))
