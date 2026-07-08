"""用登录着的全局 X profile 真发一条测试(验证 X 发布链路 + 视频是否真附上)。"""
import glob, json, os, time
from pathlib import Path
import app

ds = sorted([d for d in glob.glob("output/*full*OpenNews*") if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
video = None
for d in ds:
    r = json.load(open(os.path.join(d, "result.json")))
    fvp = r.get("final_video_path")
    if fvp:
        od = Path(d).resolve()
        try:
            video = app._resolve_youtube_publish_video(od, r, aspect_ratio="vertical")
            break
        except Exception:
            continue
if not video:
    print("找不到成片"); raise SystemExit
print("测试视频:", str(video), os.path.getsize(video)//1024, "KB", flush=True)
text = "iHouse OpenNews 测试推送 %s" % time.strftime("%H:%M")
print("发布文本:", text, flush=True)
print("=== 用全局登录 profile 发布(account_config 空=全局) ===", flush=True)
try:
    res = app._upload_video_to_opennews_x(video, text=text, made_with_ai=True, account_config={})
    print("✅ 结果:", json.dumps({k: res.get(k) for k in ("x_url", "post_id", "text", "warning")}, ensure_ascii=False))
    if res.get("x_url"):
        print("✅✅ 发布成功,链接:", res.get("x_url"))
        print("    media:", res.get("media"))
except Exception as e:
    import traceback
    print("❌ 发布失败:", repr(e)[:300])
    traceback.print_exc()
