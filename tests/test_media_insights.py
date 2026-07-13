import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from facebook_publisher import get_facebook_video_comments
from media_insights import (
    MediaInsightsStore,
    MediaInsightsSynchronizer,
    discover_system_publications,
    merge_media_account_config,
)
from x_publisher import get_x_post_comments, get_x_post_metrics, get_x_read_access_token
from youtube_publisher import get_youtube_video_comments


class MediaInsightsDiscoveryTests(unittest.TestCase):
    def test_discovers_only_system_publish_receipts_and_deduplicates_platform_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir)
            first = output_root / "100_opennews_story"
            first.mkdir()
            (first / "result.json").write_text(
                json.dumps(
                    {
                        "title": "科技新闻",
                        "owner_username": "admin",
                        "workflow_config": {
                            "opennews_channel_id": "technology",
                            "opennews_channel_name": "科技前沿",
                            "target_market": "cn",
                        },
                        "youtube_publish_records": [
                            {"video_id": "yt-1", "youtube_url": "https://youtu.be/yt-1", "created_at": time.time()}
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            duplicate = output_root / "101_opennews_retry"
            duplicate.mkdir()
            (duplicate / "result.json").write_text(
                json.dumps(
                    {
                        "title": "重复回执",
                        "workflow_config": {"opennews_channel_id": "technology"},
                        "youtube_publish_records": [{"video_id": "yt-1", "created_at": time.time()}],
                        "language_versions": [
                            {
                                "target_market": "jp",
                                "title": "日本語版",
                                "facebook_publish_records": [
                                    {"video_id": "fb-1", "facebook_url": "https://facebook.test/fb-1", "created_at": time.time()}
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            unpublished = output_root / "102_unpublished"
            unpublished.mkdir()
            (unpublished / "result.json").write_text(json.dumps({"title": "没有发布"}), encoding="utf-8")

            publications = discover_system_publications(output_root)

            self.assertEqual({(item["platform"], item["external_id"]) for item in publications}, {("youtube", "yt-1"), ("facebook", "fb-1")})
            facebook = next(item for item in publications if item["platform"] == "facebook")
            self.assertEqual(facebook["target_market"], "jp")
            self.assertEqual(facebook["channel_id"], "technology")


class MediaInsightsStoreTests(unittest.TestCase):
    def test_placeholder_account_does_not_override_configured_identity(self):
        account = merge_media_account_config(
            "x",
            {"handle": "opennewsagent", "account_label": "OpenNews"},
            {"handle": "X 账号", "account_label": "X 账号", "profile_dir": "/tmp/profile"},
        )

        self.assertEqual(account["handle"], "opennewsagent")
        self.assertEqual(account["account_label"], "OpenNews")
        self.assertEqual(account["profile_dir"], "/tmp/profile")

        facebook = merge_media_account_config(
            "facebook",
            {"page_id": "page-123", "page_name": "OpenNews"},
            {"page_id": "", "page_name": "Facebook Page", "profile": "legacy"},
        )
        self.assertEqual(facebook["page_id"], "page-123")
        self.assertEqual(facebook["page_name"], "OpenNews")
        self.assertEqual(facebook["profile"], "legacy")

    def test_configured_accounts_are_visible_before_any_video_is_indexed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MediaInsightsStore(Path(temp_dir) / "insights.db")

            dashboard = store.dashboard(
                days=7,
                configured_channels=[{"channel_id": "technology", "channel_name": "科技前沿"}],
                configured_accounts=[
                    {
                        "platform": "youtube",
                        "account_key": "youtube:configured",
                        "account_label": "OpenNews 科技前沿",
                        "channel_ids": ["technology"],
                        "target_markets": ["cn"],
                    }
                ],
            )

            self.assertEqual(dashboard["summary"]["content_count"], 0)
            self.assertEqual(dashboard["accounts"][0]["account_label"], "OpenNews 科技前沿")
            self.assertEqual(dashboard["accounts"][0]["channel_ids"], ["technology"])
            self.assertEqual(dashboard["channels"][0]["channel_name"], "科技前沿")

    def test_sync_persists_metrics_comments_and_dashboard_filters(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "output"
            history = output / "200_topic_auto"
            history.mkdir(parents=True)
            now = time.time()
            (history / "result.json").write_text(
                json.dumps(
                    {
                        "title": "数字人内容",
                        "owner_username": "topic_auto",
                        "youtube_publish_records": [
                            {"video_id": "yt-2", "youtube_url": "https://youtu.be/yt-2", "created_at": now}
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            store = MediaInsightsStore(root / "insights.db")

            def enrich(item):
                return {**item, "account_key": "youtube:one", "account_label": "科技账号"}

            def metrics(item):
                return {"view_count": 1200, "like_count": 80, "comment_count": 2, "title": "平台标题"}

            def comments(item):
                return [
                    {
                        "comment_id": "comment-1",
                        "author_name": "测试用户",
                        "message": "内容很好",
                        "published_at_text": "2026-07-13T10:00:00Z",
                        "like_count": 3,
                    }
                ]

            synchronizer = MediaInsightsSynchronizer(store, output, enrich, metrics, comments)
            result = synchronizer.sync_once(force=True)
            dashboard = store.dashboard(days=7, channel_id="digital_human")
            comment_payload = store.comments("youtube", "yt-2")

            self.assertTrue(result["ok"])
            self.assertEqual(result["synced"], 1)
            self.assertEqual(dashboard["summary"]["content_count"], 1)
            self.assertEqual(dashboard["summary"]["view_count"], 1200)
            self.assertEqual(dashboard["contents"][0]["title"], "平台标题")
            self.assertEqual(comment_payload["comments"][0]["message"], "内容很好")
            self.assertGreater(comment_payload["comments"][0]["published_at"], 0)

    def test_metrics_failure_is_not_converted_to_zero(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "output"
            history = output / "300_property"
            history.mkdir(parents=True)
            (history / "result.json").write_text(
                json.dumps(
                    {
                        "title": "房源实拍",
                        "owner_username": "property_auto",
                        "facebook_publish_records": [{"video_id": "fb-error", "created_at": time.time()}],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            store = MediaInsightsStore(root / "insights.db")

            def fail(_item):
                raise RuntimeError("权限不足")

            synchronizer = MediaInsightsSynchronizer(store, output, lambda item: item, fail, fail)
            result = synchronizer.sync_once(force=True)
            item = store.dashboard(days=7)["contents"][0]

            self.assertEqual(result["failed"], 1)
            self.assertEqual(item["metrics_status"], "error")
            self.assertIsNone(item["view_count"])
            self.assertIn("权限不足", item["metrics_error"])

    def test_dashboard_sorts_views_before_zero_and_unavailable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MediaInsightsStore(Path(temp_dir) / "insights.db")
            now = time.time()

            def publication(external_id, published_at):
                return {
                    "platform": "youtube",
                    "external_id": external_id,
                    "channel_id": "technology",
                    "channel_name": "科技新闻",
                    "account_key": "youtube:technology",
                    "account_label": "OpenNews 科技前沿",
                    "title": external_id,
                    "published_at": published_at,
                    "created_at": published_at,
                }

            rows = [
                publication("new-zero", now),
                publication("older-high", now - 30),
                publication("newer-low", now - 10),
                publication("newest-error", now + 10),
            ]
            store.upsert_publications(rows)
            store.save_metrics(rows[0], {"view_count": 0, "like_count": 0, "comment_count": 0})
            store.save_metrics(rows[1], {"view_count": 500, "like_count": 2, "comment_count": 1})
            store.save_metrics(rows[2], {"view_count": 20, "like_count": 8, "comment_count": 3})
            store.save_metrics_error(rows[3], "未授权")

            dashboard = store.dashboard(days=7)
            self.assertEqual(
                [item["external_id"] for item in dashboard["contents"]],
                ["older-high", "newer-low", "new-zero", "newest-error"],
            )
            self.assertEqual(dashboard["summary"]["average_view_count"], 520 / 3)
            self.assertEqual(dashboard["accounts"][0]["analytics_status"], "partial")
            self.assertEqual(dashboard["accounts"][0]["channel_content_counts"], {"technology": 4})

            zero_only = store.dashboard(days=7, metric_state="zero")
            self.assertEqual([item["external_id"] for item in zero_only["contents"]], ["new-zero"])
            self.assertEqual(zero_only["summary"]["content_count"], 1)

            likes = store.dashboard(days=7, sort_by="likes")
            self.assertEqual([item["external_id"] for item in likes["contents"][:2]], ["newer-low", "older-high"])


class PlatformCommentReaderTests(unittest.TestCase):
    @patch("youtube_publisher.refresh_youtube_access_token", return_value="token")
    @patch("youtube_publisher.requests.get")
    def test_youtube_comment_threads_are_normalized(self, request_get, _refresh):
        response = Mock(status_code=200)
        response.json.return_value = {
            "items": [
                {
                    "snippet": {
                        "totalReplyCount": 1,
                        "topLevelComment": {
                            "id": "yt-comment",
                            "snippet": {"authorDisplayName": "观众", "textOriginal": "很好", "publishedAt": "2026-07-13T01:00:00Z"},
                        },
                    },
                    "replies": {"comments": [{"id": "yt-reply", "snippet": {"parentId": "yt-comment", "textOriginal": "谢谢"}}]},
                }
            ]
        }
        request_get.return_value = response

        comments = get_youtube_video_comments(Path("unused.json"), "video-1")

        self.assertEqual([item["comment_id"] for item in comments], ["yt-comment", "yt-reply"])
        self.assertEqual(comments[1]["parent_id"], "yt-comment")

    @patch("facebook_publisher._graph_get")
    def test_facebook_comments_are_normalized(self, graph_get):
        graph_get.return_value = {
            "data": [
                {
                    "id": "fb-comment",
                    "message": "有帮助",
                    "created_time": "2026-07-13T01:00:00+0000",
                    "from": {"id": "user-1", "name": "读者"},
                    "like_count": 4,
                }
            ]
        }

        comments = get_facebook_video_comments(Path("unused.json"), "video-2", page_access_token="page-token")

        self.assertEqual(comments[0]["author_name"], "读者")
        self.assertEqual(comments[0]["like_count"], 4)

    @patch("x_publisher.refresh_x_access_token", return_value="token")
    @patch("x_publisher.requests.get")
    def test_x_replies_are_normalized(self, request_get, _refresh):
        response = Mock(status_code=200)
        response.json.return_value = {
            "data": [{"id": "reply-1", "author_id": "user-2", "text": "同意", "public_metrics": {"like_count": 2}}],
            "includes": {"users": [{"id": "user-2", "name": "X User", "username": "xuser"}]},
        }
        request_get.return_value = response

        comments = get_x_post_comments(Path("unused.json"), "post-1")

        self.assertEqual(comments[0]["author_name"], "X User")
        self.assertEqual(comments[0]["url"], "https://x.com/xuser/status/reply-1")

    @patch("x_publisher.x_env_config", return_value={"bearer_token": "app-bearer"})
    @patch("x_publisher.refresh_x_access_token", side_effect=Exception("missing client"))
    def test_x_read_token_falls_back_only_for_publish_errors(self, _refresh, _config):
        with self.assertRaises(Exception):
            get_x_read_access_token(Path("unused.json"))

    @patch("x_publisher.x_env_config", return_value={"bearer_token": "app-bearer"})
    @patch("x_publisher.refresh_x_access_token")
    def test_x_read_token_uses_app_bearer_when_oauth_is_unavailable(self, refresh, _config):
        from x_publisher import XPublishError

        refresh.side_effect = XPublishError("未配置 X_CLIENT_ID")

        self.assertEqual(get_x_read_access_token(Path("unused.json")), "app-bearer")

    @patch("x_publisher.get_x_read_access_token", return_value="token")
    @patch("x_publisher.requests.get")
    def test_x_public_impression_count_is_used_as_views(self, request_get, _token):
        response = Mock(status_code=200)
        response.json.return_value = {
            "data": {
                "id": "post-2",
                "text": "test",
                "public_metrics": {"impression_count": 123, "like_count": 4, "reply_count": 2},
            }
        }
        request_get.return_value = response

        metrics = get_x_post_metrics(Path("unused.json"), "post-2")

        self.assertEqual(metrics["view_count"], 123)
        self.assertEqual(metrics["like_count"], 4)


if __name__ == "__main__":
    unittest.main()
