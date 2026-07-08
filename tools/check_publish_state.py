import glob, json, os, sys

pats = sys.argv[1:] or ["*full*OpenNews*"]
dirs = []
for pat in pats:
    dirs += [d for d in glob.glob("output/" + pat) if os.path.isdir(d)]
dirs = sorted(set(dirs), key=os.path.getmtime, reverse=True)[:6]
for d in dirs:
    rj = os.path.join(d, "result.json")
    if not os.path.exists(rj):
        continue
    r = json.load(open(rj))
    print("===", os.path.basename(d))
    for k in ("youtube_publish_records", "facebook_publish_records", "x_publish_records"):
        recs = r.get(k) or []
        extra = ""
        if recs:
            rec = recs[0]
            extra = " -> " + str(rec.get("youtube_url") or rec.get("x_url") or rec.get("post_id") or rec.get("video_id") or "ok")[:70]
        print("   %s = %d%s" % (k, len(recs), extra))
    for k in ("youtube_auto_publish_error", "facebook_auto_publish_error", "x_auto_publish_error"):
        if r.get(k):
            print("   WARN %s : %s" % (k, str(r.get(k))[:130]))
