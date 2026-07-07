import glob
import json
import os
import sys

pat = sys.argv[1] if len(sys.argv) > 1 else "*中国AI芯片*"
ds = sorted([d for d in glob.glob("/app/output/" + pat) if os.path.isdir(d)], key=os.path.getmtime, reverse=True)
if not ds:
    print("无匹配目录"); sys.exit()
d = ds[0]
print("dir:", os.path.basename(d))
r = json.load(open(os.path.join(d, "result.json")))
segs = r.get("segments") or []
print("段数:", len(segs), "| total_duration:", r.get("total_duration"))
for i, s in enumerate(segs):
    ap = s.get("audio_path") or ""
    print("  seg%d type=%s scriptlen=%d audio=%s exists=%s dur=%s" % (
        i, s.get("type"), len((s.get("script") or "").strip()),
        os.path.basename(ap) if ap else "None",
        os.path.exists(ap) if ap else False,
        s.get("duration"),
    ))
ad = os.path.join(d, "audio")
print("audio目录:", sorted(os.listdir(ad)) if os.path.isdir(ad) else "无")
# language_versions
for v in (r.get("language_versions") or []):
    vsegs = v.get("segments") or []
    print("lang %s: 段数=%d error=%s" % (v.get("target_market"), len(vsegs), v.get("error")))
