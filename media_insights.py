"""Read-only analytics index for videos published by this system."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator


PLATFORM_RECORD_KEYS = {
    "youtube": ("youtube_publish_records", "video_id", "youtube_url"),
    "facebook": ("facebook_publish_records", "video_id", "facebook_url"),
    "x": ("x_publish_records", "post_id", "x_url"),
}

ACCOUNT_PLACEHOLDERS = {
    "youtube": {"youtube", "youtube account", "youtube 账号", "全局 youtube"},
    "facebook": {"facebook", "facebook page", "facebook 账号"},
    "x": {"x", "twitter", "x account", "x 账号", "twitter 账号"},
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            return 0.0


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def merge_media_account_config(
    platform: str,
    configured: dict[str, Any] | None,
    discovered: dict[str, Any] | None,
) -> dict[str, Any]:
    """Keep receipt metadata, but never let legacy placeholder labels replace configured identities."""
    platform = _text(platform).lower()
    merged = dict(configured or {})
    receipt = dict(discovered or {})
    placeholders = ACCOUNT_PLACEHOLDERS.get(platform, set())
    identity_keys = {
        "youtube": ("channel_id", "channel_name", "account_label", "label"),
        "facebook": ("page_id", "page_name", "account_label", "label"),
        "x": ("user_id", "handle", "username", "account_label", "label"),
    }.get(platform, ("account_label", "label"))
    for key in identity_keys:
        value = _text(receipt.get(key))
        if not value or value.lower().lstrip("@") in placeholders:
            receipt.pop(key, None)
    merged.update(receipt)
    return merged


def _workflow_identity(result: dict[str, Any], output_dir: Path) -> tuple[str, str, str]:
    workflow_config = result.get("workflow_config") if isinstance(result.get("workflow_config"), dict) else {}
    channel_id = _text(workflow_config.get("opennews_channel_id") or result.get("opennews_channel_id"))
    channel_name = _text(workflow_config.get("opennews_channel_name") or result.get("opennews_channel_name"))
    owner = _text(result.get("owner_username"))
    source = workflow_config.get("source")
    if channel_id or channel_name or isinstance(source, dict) and source.get("article"):
        return "opennews", channel_id or "general", channel_name or "OpenNews 综合新闻"
    if owner == "property_auto" or "property" in output_dir.name.lower():
        return "property_video", "property_video", "房源实拍"
    if owner == "topic_auto":
        return "digital_human", "digital_human", "自动化数字人"
    segments = result.get("segments") if isinstance(result.get("segments"), list) else []
    if any(isinstance(segment, dict) and segment.get("type") == "digital_human" for segment in segments):
        return "digital_human", "digital_human", "数字人视频"
    return "system_video", "system_video", "系统视频"


def discover_system_publications(
    output_root: Path,
    *,
    result_loader: Callable[[Path], dict[str, Any] | None] | None = None,
) -> list[dict[str, Any]]:
    """Find platform receipts in result.json files; manual platform posts are never listed."""
    output_root = Path(output_root)
    if not output_root.exists():
        return []
    result_paths = sorted(
        output_root.glob("*/result.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    publications: dict[tuple[str, str], dict[str, Any]] = {}
    for result_path in result_paths:
        if result_loader:
            try:
                result = result_loader(result_path.parent)
            except Exception:
                continue
        else:
            try:
                result = json.loads(result_path.read_text(encoding="utf-8"))
            except Exception:
                continue
        if not isinstance(result, dict):
            continue
        output_dir = result_path.parent
        workflow, default_channel_id, default_channel_name = _workflow_identity(result, output_dir)
        workflow_config = result.get("workflow_config") if isinstance(result.get("workflow_config"), dict) else {}
        primary_market = _text(workflow_config.get("target_market") or result.get("target_market") or "cn")
        contexts: list[tuple[dict[str, Any], str, str]] = [(result, primary_market, "primary")]
        for version in result.get("language_versions") or []:
            if isinstance(version, dict):
                market = _text(version.get("target_market") or version.get("language") or primary_market)
                contexts.append((version, market, market or "translated"))
        for context, target_market, language_version in contexts:
            title = _text(context.get("title") or result.get("title") or result.get("topic") or output_dir.name)
            for platform, (records_key, id_key, url_key) in PLATFORM_RECORD_KEYS.items():
                records = context.get(records_key)
                if not isinstance(records, list):
                    continue
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    external_id = _text(record.get(id_key) or (record.get("tweet_id") if platform == "x" else ""))
                    if not external_id:
                        continue
                    key = (platform, external_id)
                    if key in publications:
                        continue
                    publish_account = record.get("publish_account") if isinstance(record.get("publish_account"), dict) else {}
                    channel_id = _text(record.get("opennews_channel_id") or default_channel_id)
                    channel_name = _text(record.get("opennews_channel_name") or default_channel_name)
                    created_at = _timestamp(record.get("created_at")) or result_path.stat().st_mtime
                    publications[key] = {
                        "platform": platform,
                        "external_id": external_id,
                        "history_id": _text(record.get("history_id") or output_dir.name),
                        "workflow": workflow,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "target_market": _text(record.get("target_market") or target_market or primary_market),
                        "language_version": _text(record.get("language_version") or language_version),
                        "account_key": "",
                        "account_label": "",
                        "account_external_id": "",
                        "title": title,
                        "url": _text(record.get(url_key)),
                        "aspect_ratio": _text(record.get("aspect_ratio")),
                        "published_at": created_at,
                        "created_at": created_at,
                        "publish_account": publish_account,
                        "record": record,
                    }
    return list(publications.values())


class MediaInsightsStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS contents (
                    platform TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    history_id TEXT NOT NULL DEFAULT '',
                    workflow TEXT NOT NULL DEFAULT '',
                    channel_id TEXT NOT NULL DEFAULT '',
                    channel_name TEXT NOT NULL DEFAULT '',
                    target_market TEXT NOT NULL DEFAULT '',
                    language_version TEXT NOT NULL DEFAULT '',
                    account_key TEXT NOT NULL DEFAULT '',
                    account_label TEXT NOT NULL DEFAULT '',
                    account_external_id TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    aspect_ratio TEXT NOT NULL DEFAULT '',
                    published_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL DEFAULT 0,
                    view_count INTEGER,
                    like_count INTEGER,
                    comment_count INTEGER,
                    repost_count INTEGER,
                    metrics_status TEXT NOT NULL DEFAULT 'pending',
                    metrics_error TEXT NOT NULL DEFAULT '',
                    comments_status TEXT NOT NULL DEFAULT 'pending',
                    comments_error TEXT NOT NULL DEFAULT '',
                    last_synced_at REAL NOT NULL DEFAULT 0,
                    last_comments_synced_at REAL NOT NULL DEFAULT 0,
                    discovered_at REAL NOT NULL DEFAULT 0,
                    record_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (platform, external_id)
                );
                CREATE INDEX IF NOT EXISTS idx_contents_published_at ON contents(published_at DESC);
                CREATE INDEX IF NOT EXISTS idx_contents_channel ON contents(channel_id, published_at DESC);
                CREATE INDEX IF NOT EXISTS idx_contents_account ON contents(account_key, published_at DESC);

                CREATE TABLE IF NOT EXISTS metric_snapshots (
                    platform TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    captured_at REAL NOT NULL,
                    view_count INTEGER,
                    like_count INTEGER,
                    comment_count INTEGER,
                    repost_count INTEGER,
                    PRIMARY KEY (platform, external_id, captured_at),
                    FOREIGN KEY (platform, external_id) REFERENCES contents(platform, external_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS comments (
                    platform TEXT NOT NULL,
                    comment_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    parent_id TEXT NOT NULL DEFAULT '',
                    author_id TEXT NOT NULL DEFAULT '',
                    author_name TEXT NOT NULL DEFAULT '',
                    author_avatar_url TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    published_at REAL NOT NULL DEFAULT 0,
                    published_at_text TEXT NOT NULL DEFAULT '',
                    like_count INTEGER,
                    reply_count INTEGER,
                    url TEXT NOT NULL DEFAULT '',
                    fetched_at REAL NOT NULL DEFAULT 0,
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (platform, comment_id),
                    FOREIGN KEY (platform, external_id) REFERENCES contents(platform, external_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_comments_content ON comments(platform, external_id, published_at DESC);

                CREATE TABLE IF NOT EXISTS sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT ''
                );
                """
            )

    def upsert_publications(self, publications: Iterable[dict[str, Any]]) -> int:
        now = time.time()
        rows = list(publications)
        if not rows:
            return 0
        with self._write_lock, self._connect() as connection:
            for item in rows:
                connection.execute(
                    """
                    INSERT INTO contents (
                        platform, external_id, history_id, workflow, channel_id, channel_name,
                        target_market, language_version, account_key, account_label,
                        account_external_id, title, url, aspect_ratio, published_at, created_at,
                        discovered_at, record_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(platform, external_id) DO UPDATE SET
                        history_id=excluded.history_id,
                        workflow=excluded.workflow,
                        channel_id=excluded.channel_id,
                        channel_name=excluded.channel_name,
                        target_market=excluded.target_market,
                        language_version=excluded.language_version,
                        account_key=CASE WHEN excluded.account_key != '' THEN excluded.account_key ELSE contents.account_key END,
                        account_label=CASE WHEN excluded.account_label != '' THEN excluded.account_label ELSE contents.account_label END,
                        account_external_id=CASE WHEN excluded.account_external_id != '' THEN excluded.account_external_id ELSE contents.account_external_id END,
                        title=CASE WHEN excluded.title != '' THEN excluded.title ELSE contents.title END,
                        url=CASE WHEN excluded.url != '' THEN excluded.url ELSE contents.url END,
                        aspect_ratio=excluded.aspect_ratio,
                        published_at=CASE WHEN excluded.published_at > 0 THEN excluded.published_at ELSE contents.published_at END,
                        record_json=excluded.record_json
                    """,
                    (
                        _text(item.get("platform")), _text(item.get("external_id")), _text(item.get("history_id")),
                        _text(item.get("workflow")), _text(item.get("channel_id")), _text(item.get("channel_name")),
                        _text(item.get("target_market")), _text(item.get("language_version")), _text(item.get("account_key")),
                        _text(item.get("account_label")), _text(item.get("account_external_id")), _text(item.get("title")),
                        _text(item.get("url")), _text(item.get("aspect_ratio")), _timestamp(item.get("published_at")),
                        _timestamp(item.get("created_at")), now, _json(item.get("record") or {}),
                    ),
                )
        return len(rows)

    def contents_for_sync(self, *, limit: int = 120, force: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        freshness = now - 1800
        where = "1=1" if force else "(last_synced_at = 0 OR last_synced_at < ?)"
        params: list[Any] = [] if force else [freshness]
        params.append(max(1, min(int(limit or 120), 500)))
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM contents WHERE {where} ORDER BY last_synced_at ASC, published_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def save_metrics(self, item: dict[str, Any], metrics: dict[str, Any]) -> None:
        now = time.time()
        values = (
            _number(metrics.get("view_count")),
            _number(metrics.get("like_count")),
            _number(metrics.get("comment_count")),
            _number(metrics.get("repost_count")),
        )
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """UPDATE contents SET view_count=?, like_count=?, comment_count=?, repost_count=?,
                   title=CASE WHEN ? != '' THEN ? ELSE title END,
                   metrics_status='ok', metrics_error='', last_synced_at=?
                   WHERE platform=? AND external_id=?""",
                (*values, _text(metrics.get("title") or metrics.get("text")), _text(metrics.get("title") or metrics.get("text")),
                 now, item["platform"], item["external_id"]),
            )
            connection.execute(
                """INSERT OR REPLACE INTO metric_snapshots
                   (platform, external_id, captured_at, view_count, like_count, comment_count, repost_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (item["platform"], item["external_id"], float(int(now)), *values),
            )

    def save_metrics_error(self, item: dict[str, Any], error: Exception | str) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """UPDATE contents SET metrics_status='error', metrics_error=?, last_synced_at=?
                   WHERE platform=? AND external_id=?""",
                (_text(error)[:1000], time.time(), item["platform"], item["external_id"]),
            )

    def save_comments(self, item: dict[str, Any], comments: Iterable[dict[str, Any]]) -> int:
        now = time.time()
        rows = [comment for comment in comments if isinstance(comment, dict) and _text(comment.get("comment_id"))]
        with self._write_lock, self._connect() as connection:
            for comment in rows:
                connection.execute(
                    """INSERT INTO comments (
                        platform, comment_id, external_id, parent_id, author_id, author_name,
                        author_avatar_url, message, published_at, published_at_text, like_count,
                        reply_count, url, fetched_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(platform, comment_id) DO UPDATE SET
                        parent_id=excluded.parent_id, author_id=excluded.author_id,
                        author_name=excluded.author_name, author_avatar_url=excluded.author_avatar_url,
                        message=excluded.message, published_at=excluded.published_at,
                        published_at_text=excluded.published_at_text, like_count=excluded.like_count,
                        reply_count=excluded.reply_count, url=excluded.url,
                        fetched_at=excluded.fetched_at, raw_json=excluded.raw_json
                    """,
                    (
                        item["platform"], _text(comment.get("comment_id")), item["external_id"],
                        _text(comment.get("parent_id")), _text(comment.get("author_id")), _text(comment.get("author_name")),
                        _text(comment.get("author_avatar_url")), _text(comment.get("message")),
                        _timestamp(comment.get("published_at") or comment.get("published_at_text")), _text(comment.get("published_at_text")),
                        _number(comment.get("like_count")), _number(comment.get("reply_count")), _text(comment.get("url")),
                        now, _json(comment.get("raw") or {}),
                    ),
                )
            connection.execute(
                """UPDATE contents SET comments_status='ok', comments_error='', last_comments_synced_at=?
                   WHERE platform=? AND external_id=?""",
                (now, item["platform"], item["external_id"]),
            )
        return len(rows)

    def save_comments_error(self, item: dict[str, Any], error: Exception | str) -> None:
        with self._write_lock, self._connect() as connection:
            connection.execute(
                """UPDATE contents SET comments_status='error', comments_error=?, last_comments_synced_at=?
                   WHERE platform=? AND external_id=?""",
                (_text(error)[:1000], time.time(), item["platform"], item["external_id"]),
            )

    def set_sync_state(self, **values: Any) -> None:
        with self._write_lock, self._connect() as connection:
            for key, value in values.items():
                connection.execute(
                    "INSERT OR REPLACE INTO sync_state(key, value) VALUES (?, ?)",
                    (_text(key), _json(value)),
                )

    def sync_status(self) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM sync_state").fetchall()
            count_row = connection.execute(
                "SELECT COUNT(*) AS content_count, SUM(CASE WHEN metrics_status='error' THEN 1 ELSE 0 END) AS error_count FROM contents"
            ).fetchone()
        state: dict[str, Any] = {}
        for row in rows:
            try:
                state[row["key"]] = json.loads(row["value"])
            except Exception:
                state[row["key"]] = row["value"]
        state["content_count"] = int(count_row["content_count"] or 0)
        state["error_count"] = int(count_row["error_count"] or 0)
        return state

    @staticmethod
    def _filters(channel_id: str, platform: str, account_key: str, days: int) -> tuple[str, list[Any]]:
        clauses = ["published_at >= ?"]
        params: list[Any] = [time.time() - max(1, min(int(days or 30), 3650)) * 86400]
        if channel_id:
            clauses.append("channel_id = ?")
            params.append(channel_id)
        if platform:
            clauses.append("platform = ?")
            params.append(platform)
        if account_key:
            clauses.append("account_key = ?")
            params.append(account_key)
        return " AND ".join(clauses), params

    def dashboard(
        self,
        *,
        days: int = 30,
        channel_id: str = "",
        platform: str = "",
        account_key: str = "",
        search: str = "",
        sort_by: str = "views",
        metric_state: str = "",
        limit: int = 50,
        offset: int = 0,
        configured_channels: Iterable[dict[str, Any]] = (),
        configured_accounts: Iterable[dict[str, Any]] = (),
    ) -> dict[str, Any]:
        where, params = self._filters(_text(channel_id), _text(platform), _text(account_key), days)
        list_where = where
        list_params = list(params)
        metric_state = _text(metric_state).lower()
        if metric_state == "viewed":
            list_where += " AND metrics_status='ok' AND view_count > 0"
        elif metric_state == "zero":
            list_where += " AND metrics_status='ok' AND view_count = 0"
        elif metric_state == "unavailable":
            list_where += " AND (metrics_status!='ok' OR view_count IS NULL)"
        elif metric_state == "ready":
            list_where += " AND metrics_status='ok'"
        if search:
            list_where += " AND (title LIKE ? OR account_label LIKE ? OR channel_name LIKE ?)"
            pattern = f"%{_text(search)}%"
            list_params.extend([pattern, pattern, pattern])
        sort_by = _text(sort_by).lower()
        sort_orders = {
            "views": "CASE WHEN metrics_status='ok' AND view_count IS NOT NULL THEN 0 ELSE 1 END, view_count DESC, published_at DESC",
            "likes": "CASE WHEN metrics_status='ok' AND like_count IS NOT NULL THEN 0 ELSE 1 END, like_count DESC, published_at DESC",
            "comments": "CASE WHEN metrics_status='ok' AND comment_count IS NOT NULL THEN 0 ELSE 1 END, comment_count DESC, published_at DESC",
            "published": "published_at DESC",
        }
        if sort_by not in sort_orders:
            sort_by = "views"
        order_by = sort_orders[sort_by]
        range_since = params[0]
        with self._connect() as connection:
            summary = connection.execute(
                f"""SELECT COUNT(*) AS content_count,
                    COALESCE(SUM(view_count), 0) AS view_count,
                    COALESCE(SUM(like_count), 0) AS like_count,
                    COALESCE(SUM(comment_count), 0) AS comment_count,
                    SUM(CASE WHEN metrics_status='ok' THEN 1 ELSE 0 END) AS metrics_ready_count,
                    SUM(CASE WHEN metrics_status='error' THEN 1 ELSE 0 END) AS metrics_error_count,
                    SUM(CASE WHEN metrics_status='ok' AND view_count IS NOT NULL THEN 1 ELSE 0 END) AS view_ready_count
                    FROM contents WHERE {list_where}""",
                list_params,
            ).fetchone()
            content_rows = connection.execute(
                f"SELECT * FROM contents WHERE {list_where} ORDER BY {order_by} LIMIT ? OFFSET ?",
                (*list_params, max(1, min(int(limit or 50), 200)), max(0, int(offset or 0))),
            ).fetchall()
            total = connection.execute(f"SELECT COUNT(*) AS count FROM contents WHERE {list_where}", list_params).fetchone()
            channels = connection.execute(
                """SELECT channel_id, MAX(channel_name) AS channel_name, COUNT(*) AS content_count
                   FROM contents WHERE published_at >= ? GROUP BY channel_id ORDER BY channel_name""",
                (range_since,),
            ).fetchall()
            account_rows = connection.execute(
                """SELECT account_key, MAX(account_label) AS account_label, platform,
                   COUNT(*) AS content_count, channel_id,
                   SUM(CASE WHEN metrics_status='ok' THEN 1 ELSE 0 END) AS metrics_ready_count,
                   SUM(CASE WHEN metrics_status='error' THEN 1 ELSE 0 END) AS metrics_error_count,
                   COALESCE(SUM(view_count), 0) AS view_count,
                   MAX(published_at) AS latest_published_at
                   FROM contents WHERE account_key != '' AND published_at >= ?
                   GROUP BY account_key, platform, channel_id ORDER BY account_label""",
                (range_since,),
            ).fetchall()
        channel_catalog: dict[str, dict[str, Any]] = {}
        for item in configured_channels:
            if not isinstance(item, dict) or not _text(item.get("channel_id")):
                continue
            channel_catalog[_text(item.get("channel_id"))] = {
                "channel_id": _text(item.get("channel_id")),
                "channel_name": _text(item.get("channel_name") or item.get("channel_id")),
                "content_count": int(item.get("content_count") or 0),
                "configured": True,
            }
        for row in channels:
            key = _text(row["channel_id"])
            existing = channel_catalog.setdefault(
                key,
                {"channel_id": key, "channel_name": _text(row["channel_name"]), "content_count": 0, "configured": False},
            )
            existing["content_count"] = int(row["content_count"] or 0)
            if not existing.get("channel_name"):
                existing["channel_name"] = _text(row["channel_name"])

        account_catalog: dict[tuple[str, str], dict[str, Any]] = {}
        for item in configured_accounts:
            if not isinstance(item, dict):
                continue
            key = (_text(item.get("platform")), _text(item.get("account_key")))
            if not all(key):
                continue
            account_catalog[key] = {
                "account_key": key[1],
                "account_label": _text(item.get("account_label")),
                "account_external_id": _text(item.get("account_external_id")),
                "platform": key[0],
                "channel_ids": sorted({_text(value) for value in (item.get("channel_ids") or []) if _text(value)}),
                "target_markets": sorted({_text(value) for value in (item.get("target_markets") or []) if _text(value)}),
                "content_count": int(item.get("content_count") or 0),
                "metrics_ready_count": 0,
                "metrics_error_count": 0,
                "view_count": 0,
                "latest_published_at": 0,
                "configured": True,
            }
        for row in account_rows:
            key = (_text(row["platform"]), _text(row["account_key"]))
            existing = account_catalog.setdefault(
                key,
                {
                    "account_key": key[1],
                    "account_label": _text(row["account_label"]),
                    "account_external_id": "",
                    "platform": key[0],
                    "channel_ids": [],
                    "target_markets": [],
                    "content_count": 0,
                    "metrics_ready_count": 0,
                    "metrics_error_count": 0,
                    "view_count": 0,
                    "latest_published_at": 0,
                    "configured": False,
                },
            )
            existing["content_count"] += int(row["content_count"] or 0)
            existing["metrics_ready_count"] += int(row["metrics_ready_count"] or 0)
            existing["metrics_error_count"] += int(row["metrics_error_count"] or 0)
            existing["view_count"] += int(row["view_count"] or 0)
            existing["latest_published_at"] = max(
                float(existing.get("latest_published_at") or 0),
                float(row["latest_published_at"] or 0),
            )
            channel_value = _text(row["channel_id"])
            if channel_value and channel_value not in existing["channel_ids"]:
                existing["channel_ids"].append(channel_value)
            if not existing.get("account_label"):
                existing["account_label"] = _text(row["account_label"])
        for item in account_catalog.values():
            item["channel_ids"] = sorted(item["channel_ids"])
            ready_count = int(item.get("metrics_ready_count") or 0)
            error_count = int(item.get("metrics_error_count") or 0)
            if ready_count and error_count:
                item["analytics_status"] = "partial"
            elif ready_count:
                item["analytics_status"] = "ready"
            elif error_count:
                item["analytics_status"] = "unavailable"
            else:
                item["analytics_status"] = "waiting"
        contents = []
        for row in content_rows:
            item = dict(row)
            item.pop("record_json", None)
            contents.append(item)
        summary_payload = dict(summary)
        view_ready_count = int(summary_payload.get("view_ready_count") or 0)
        summary_payload["average_view_count"] = (
            float(summary_payload.get("view_count") or 0) / view_ready_count if view_ready_count else None
        )
        summary_payload["interaction_count"] = int(summary_payload.get("like_count") or 0) + int(
            summary_payload.get("comment_count") or 0
        )
        return {
            "range_days": max(1, min(int(days or 30), 3650)),
            "summary": summary_payload,
            "contents": contents,
            "total": int(total["count"] or 0),
            "offset": max(0, int(offset or 0)),
            "limit": max(1, min(int(limit or 50), 200)),
            "sort_by": sort_by,
            "metric_state": metric_state,
            "channels": sorted(channel_catalog.values(), key=lambda item: item.get("channel_name") or item.get("channel_id")),
            "accounts": sorted(account_catalog.values(), key=lambda item: (item.get("platform") or "", item.get("account_label") or "")),
            "sync": self.sync_status(),
        }

    def comments(self, platform: str, external_id: str, *, limit: int = 100) -> dict[str, Any]:
        with self._connect() as connection:
            content = connection.execute(
                "SELECT * FROM contents WHERE platform=? AND external_id=?",
                (_text(platform), _text(external_id)),
            ).fetchone()
            rows = connection.execute(
                """SELECT platform, comment_id, external_id, parent_id, author_id, author_name,
                   author_avatar_url, message, published_at, published_at_text, like_count,
                   reply_count, url FROM comments WHERE platform=? AND external_id=?
                   ORDER BY published_at DESC LIMIT ?""",
                (_text(platform), _text(external_id), max(1, min(int(limit or 100), 500))),
            ).fetchall()
        if not content:
            return {"content": None, "comments": [], "count": 0}
        content_payload = dict(content)
        content_payload.pop("record_json", None)
        return {"content": content_payload, "comments": [dict(row) for row in rows], "count": len(rows)}


class MediaInsightsSynchronizer:
    def __init__(
        self,
        store: MediaInsightsStore,
        output_root: Path,
        enrich_publication: Callable[[dict[str, Any]], dict[str, Any]],
        fetch_metrics: Callable[[dict[str, Any]], dict[str, Any]],
        fetch_comments: Callable[[dict[str, Any]], list[dict[str, Any]]],
        result_loader: Callable[[Path], dict[str, Any] | None] | None = None,
    ):
        self.store = store
        self.output_root = Path(output_root)
        self.enrich_publication = enrich_publication
        self.fetch_metrics = fetch_metrics
        self.fetch_comments = fetch_comments
        self.result_loader = result_loader
        self._lock = threading.Lock()

    def discover(self) -> int:
        publications = [
            self.enrich_publication(item)
            for item in discover_system_publications(self.output_root, result_loader=self.result_loader)
        ]
        return self.store.upsert_publications(publications)

    def sync_once(self, *, force: bool = False, limit: int = 120) -> dict[str, Any]:
        if not self._lock.acquire(blocking=False):
            return {"ok": False, "running": True, "message": "媒体数据正在同步"}
        started_at = time.time()
        self.store.set_sync_state(running=True, started_at=started_at, last_error="")
        result = {"ok": True, "running": False, "discovered": 0, "synced": 0, "failed": 0, "comments": 0}
        try:
            result["discovered"] = self.discover()
            for item in self.store.contents_for_sync(limit=limit, force=force):
                try:
                    metrics = self.fetch_metrics(item)
                    self.store.save_metrics(item, metrics)
                    result["synced"] += 1
                except Exception as exc:
                    self.store.save_metrics_error(item, exc)
                    result["failed"] += 1
                try:
                    comments = self.fetch_comments(item)
                    result["comments"] += self.store.save_comments(item, comments)
                except Exception as exc:
                    self.store.save_comments_error(item, exc)
            result["finished_at"] = time.time()
            self.store.set_sync_state(
                running=False,
                finished_at=result["finished_at"],
                last_result=result,
                last_error="",
            )
            return result
        except Exception as exc:
            result.update({"ok": False, "error": _text(exc), "finished_at": time.time()})
            self.store.set_sync_state(running=False, finished_at=result["finished_at"], last_error=_text(exc), last_result=result)
            return result
        finally:
            self._lock.release()
