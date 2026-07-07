import sqlite3, glob, os

fs = glob.glob('/app/output/x_browser/profile/**/Cookies', recursive=True)
print('找到 Cookies 文件:', len(fs))
for f in fs:
    prof = os.path.basename(os.path.dirname(f))
    try:
        con = sqlite3.connect(f)
        n = con.execute("SELECT count(*) FROM cookies WHERE host_key LIKE '%x.com'").fetchone()[0]
        keys = [r[0] for r in con.execute(
            "SELECT name FROM cookies WHERE host_key LIKE '%x.com' AND name IN ('auth_token','ct0','twid')"
        ).fetchall()]
        con.close()
        print('  子档[%s] x.com_cookie=%d 关键=%s' % (prof, n, keys))
    except Exception as e:
        print('  子档[%s] 读取失败: %r' % (prof, e))
