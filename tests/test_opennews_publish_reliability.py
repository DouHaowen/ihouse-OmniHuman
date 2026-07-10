import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app
import youtube_publisher


class ResultPersistenceTests(unittest.TestCase):
    def test_stale_checkpoint_cannot_remove_channel_or_publish_records(self):
        existing = {
            "workflow_config": {
                "opennews": True,
                "opennews_channel_id": "technology",
                "opennews_channel_name": "科技前沿",
                "batch_job_id": "batch-1",
            },
            "youtube_publish_records": [
                {"video_id": "video-1", "created_at": 20},
            ],
            "segments": [
                {
                    "type": "digital_human",
                    "script": "anchor",
                    "video_path": "/tmp/dh_00.mp4",
                    "audio_path": "/tmp/segment_00.mp3",
                }
            ],
        }
        stale_checkpoint = {
            "workflow_config": {
                "opennews": True,
                "opennews_channel_id": "general",
                "compose_aspect_ratio": "vertical",
            },
            "segments": [{"type": "digital_human", "script": "anchor"}],
        }

        merged = app._merge_result_for_persistence(existing, stale_checkpoint)

        self.assertEqual(merged["workflow_config"]["opennews_channel_id"], "technology")
        self.assertEqual(merged["workflow_config"]["batch_job_id"], "batch-1")
        self.assertEqual(merged["youtube_publish_records"][0]["video_id"], "video-1")
        self.assertEqual(merged["segments"][0]["video_path"], "/tmp/dh_00.mp4")

    def test_generated_segment_files_are_restored_from_output_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "audio").mkdir()
            (output_dir / "digital_human").mkdir()
            (output_dir / "audio" / "segment_00_digital_human.mp3").write_bytes(b"audio")
            (output_dir / "digital_human" / "dh_00.mp4").write_bytes(b"video")
            result = {
                "workflow_config": {"digital_human_engine": "infinitetalk_local"},
                "segments": [{"type": "digital_human", "script": "anchor"}],
            }

            changed = app._restore_result_generated_segment_paths(output_dir, result)

            self.assertTrue(changed)
            self.assertEqual(result["segments"][0]["video_path"], str(output_dir / "digital_human" / "dh_00.mp4"))
            self.assertEqual(result["segments"][0]["audio_path"], str(output_dir / "audio" / "segment_00_digital_human.mp3"))

    def test_history_cannot_compose_before_digital_human_video_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            audio = output_dir / "audio.mp3"
            material = output_dir / "material.jpg"
            audio.write_bytes(b"audio")
            material.write_bytes(b"image")
            result = {
                "title": "regular task",
                "segments": [
                    {"type": "digital_human", "script": "anchor", "audio_path": str(audio)},
                    {
                        "type": "material",
                        "script": "body",
                        "audio_path": str(audio),
                        "material_items": [{"path": str(material), "kind": "image"}],
                    },
                ],
            }

            lifecycle = app._build_history_lifecycle(output_dir, result)

            self.assertFalse(lifecycle["can_compose"])
            self.assertEqual(lifecycle["stage_key"], "digital_human")

    def test_persist_result_file_merges_concurrent_writer_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            first = {
                "workflow_config": {"opennews_channel_id": "real_estate_immigration"},
                "youtube_publish_records": [{"video_id": "video-2", "created_at": 30}],
            }
            app._persist_result_file(output_dir, first)
            stale = {"workflow_config": {"opennews": True}, "title": "stale"}
            app._persist_result_file(output_dir, stale)

            saved = json.loads((output_dir / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["workflow_config"]["opennews_channel_id"], "real_estate_immigration")
            self.assertEqual(saved["youtube_publish_records"][0]["video_id"], "video-2")

    def test_facebook_success_clears_stale_pending_state(self):
        existing = {
            "facebook_publish_pending_at": 100,
            "facebook_publish_pending_reason": "wait",
            "facebook_publish_throttled": "wait",
            "facebook_auto_publish_error": "old error",
        }
        incoming = {
            "facebook_publish_records": [{"video_id": "fb-1", "created_at": 200}],
        }

        merged = app._merge_result_for_persistence(existing, incoming)

        self.assertNotIn("facebook_publish_pending_at", merged)
        self.assertNotIn("facebook_publish_pending_reason", merged)
        self.assertNotIn("facebook_publish_throttled", merged)
        self.assertNotIn("facebook_auto_publish_error", merged)

    def test_batch_job_receipt_is_recovered_when_result_was_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "history-1"
            output_dir.mkdir()
            (output_dir / "result.json").write_text(
                json.dumps({"title": "news", "workflow_config": {"opennews": True}}),
                encoding="utf-8",
            )
            jobs_dir = root / "opennews_batches" / "batch_jobs"
            jobs_dir.mkdir(parents=True)
            (jobs_dir / "opennews_batch_1.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "history_id": "history-1",
                                "youtube_records": [
                                    {"video_id": "video-from-batch", "created_at": 100}
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(app, "OPENNEWS_BATCH_DIR", root / "opennews_batches"):
                loaded = app._load_result_from_output_dir(output_dir)

            self.assertEqual(loaded["youtube_publish_records"][0]["video_id"], "video-from-batch")
            self.assertEqual(
                loaded["publish_receipts_recovered_from_batch_job"],
                "opennews_batch_1",
            )


class FacebookRecoveryTests(unittest.TestCase):
    def test_frequency_blocks_use_persistent_exponential_cooldown(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "publish_state.json"
            with (
                mock.patch.object(app, "FACEBOOK_PUBLISH_STATE_PATH", state_path),
                mock.patch.dict(
                    os.environ,
                    {
                        "OPENNEWS_FACEBOOK_FREQUENCY_COOLDOWN_SECONDS": "3600",
                        "OPENNEWS_FACEBOOK_FREQUENCY_MAX_COOLDOWN_SECONDS": "14400",
                    },
                ),
                mock.patch.object(app.time, "time", return_value=1000),
            ):
                first_until = app._trigger_facebook_publish_cooldown("368", page_id="page-1")
                second_until = app._trigger_facebook_publish_cooldown("368", page_id="page-1")

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(first_until, 4600)
            self.assertEqual(second_until, 8200)
            self.assertEqual(state["pages"]["page-1"]["frequency_block_count"], 2)

    def test_legacy_368_cooldown_is_migrated_to_safe_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "publish_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "pages": {
                            "page-1": {
                                "cooldown_set_at": 1000,
                                "cooldown_until": 1200,
                                "cooldown_reason": 'OAuth error {"code":368}',
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(app, "FACEBOOK_PUBLISH_STATE_PATH", state_path),
                mock.patch.dict(
                    os.environ,
                    {
                        "OPENNEWS_FACEBOOK_FREQUENCY_COOLDOWN_SECONDS": "3600",
                        "OPENNEWS_FACEBOOK_FREQUENCY_MAX_COOLDOWN_SECONDS": "14400",
                    },
                ),
                mock.patch.object(app.time, "time", return_value=1100),
            ):
                remaining = app._facebook_publish_cooldown_remaining("page-1")

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(remaining, 3500)
            self.assertEqual(state["pages"]["page-1"]["cooldown_until"], 4600)
            self.assertEqual(state["pages"]["page-1"]["frequency_block_count"], 1)

    def test_pending_publish_becomes_due(self):
        result = {}
        with mock.patch.object(app.time, "time", return_value=1000):
            app._mark_facebook_publish_pending(result, retry_at=1100, reason="throttled")

        self.assertFalse(app._facebook_publish_pending_due(result, now_ts=1099))
        self.assertTrue(app._facebook_publish_pending_due(result, now_ts=1100))
        app._clear_facebook_publish_pending(result)
        self.assertFalse(app._facebook_publish_pending_due(result, now_ts=1200))

    def test_cooldown_marks_result_for_automatic_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = {"workflow_config": {"opennews_channel_id": "technology", "target_market": "cn"}}
            account = {"enabled": True, "account": {"page_id": "page-1"}}
            with (
                mock.patch.object(app, "_opennews_publish_account_for", return_value=account),
                mock.patch.object(app, "_facebook_publish_cooldown_remaining", return_value=100),
                mock.patch.object(app.time, "time", return_value=1000),
            ):
                records = app._publish_opennews_result_to_facebook_locked(Path(tmp), result)

            self.assertEqual(records, [])
            self.assertEqual(result["facebook_publish_pending_at"], 1100)
            self.assertIn("自动补发", result["facebook_publish_pending_reason"])

    def test_disabled_channel_does_not_create_facebook_retry(self):
        result = {"workflow_config": {"opennews_channel_id": "real_estate_immigration", "target_market": "cn"}}
        with mock.patch.object(app, "_opennews_publish_account_for", return_value={"enabled": False}):
            records = app._publish_opennews_result_to_facebook_locked(Path("/tmp/unused"), result)

        self.assertEqual(records, [])
        self.assertNotIn("facebook_publish_pending_at", result)


class ExternalProduceMarkerTests(unittest.TestCase):
    def test_active_marker_blocks_recovery_until_cleared(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            app._mark_opennews_external_produce_active(output_dir, task_id="task-1", batch_job_id="batch-1")
            self.assertTrue(app._opennews_external_produce_active(output_dir))
            app._clear_opennews_external_produce_active(output_dir)
            self.assertFalse(app._opennews_external_produce_active(output_dir))


class YouTubePublishIdempotencyTests(unittest.TestCase):
    def _result(self) -> dict:
        return {
            "title": "Meta自研AI芯片Iris九月投产",
            "workflow_config": {
                "opennews": True,
                "opennews_channel_id": "technology",
                "source": {
                    "article": {
                        "url": "https://example.com/meta-iris",
                        "title": "Meta Iris chip",
                    }
                },
            },
        }

    def _publish_once(self, output_dir: Path) -> dict:
        video_path = output_dir / "video.mp4"
        video_path.write_bytes(b"video")
        return app._publish_opennews_youtube_once(
            output_dir,
            self._result(),
            channel_id="technology",
            target_market="cn",
            language_version="primary",
            aspect_ratio="vertical",
            video_path=video_path,
            thumbnail_path=None,
            metadata={"title": "Meta Iris #Shorts", "description": "source", "tags": []},
            privacy_status="public",
            category_id="25",
        )

    def test_atomic_ledger_claim_allows_only_one_publisher(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger_dir = Path(tmp)
            identity = {
                "platform": "youtube",
                "channel_id": "technology",
                "target_market": "cn",
                "language_version": "primary",
                "aspect_ratio": "vertical",
                "content_identity": "url:https://example.com/story",
                "source_url": "https://example.com/story",
            }
            with mock.patch.object(app, "OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR", ledger_dir):
                first = app._claim_opennews_youtube_publish(identity, history_id="history-1")
                second = app._claim_opennews_youtube_publish(identity, history_id="history-2")
                app._complete_opennews_youtube_publish(
                    identity,
                    {"history_id": "history-1", "video_id": "video-1"},
                    claim_id=first["claim_id"],
                )
                third = app._claim_opennews_youtube_publish(identity, history_id="history-2")

            self.assertTrue(first["acquired"])
            self.assertFalse(second["acquired"])
            self.assertEqual(second["status"], "publishing")
            self.assertEqual(third["status"], "published")
            self.assertEqual(third["record"]["video_id"], "video-1")

    def test_recovery_waits_for_active_publish_claim_but_retries_after_ttl(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger_dir = Path(tmp)
            result = self._result()
            identity = app._opennews_youtube_publish_identity(
                result,
                channel_id="technology",
                target_market="cn",
                language_version="primary",
                aspect_ratio="vertical",
            )
            now = 1000.0
            with (
                mock.patch.object(app, "OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR", ledger_dir),
                mock.patch.object(app.time, "time", return_value=now),
                mock.patch.dict(
                    os.environ,
                    {"OPENNEWS_YOUTUBE_PUBLISH_CLAIM_TTL_SECONDS": "300"},
                ),
            ):
                app._claim_opennews_youtube_publish(identity, history_id="history-1")
                active = app._opennews_youtube_publish_claim_active(result, now_ts=1299)
                expired = app._opennews_youtube_publish_claim_active(result, now_ts=1300)

            self.assertTrue(active)
            self.assertFalse(expired)

    def test_cross_directory_dedup_reads_batch_job_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs_dir = root / "opennews_batches" / "batch_jobs"
            jobs_dir.mkdir(parents=True)
            (jobs_dir / "opennews_batch_1.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "history_id": "history-1",
                                "article": {
                                    "url": "https://example.com/meta-iris",
                                    "title": "Meta Iris chip",
                                    "opennews_channel_id": "technology",
                                },
                                "youtube_records": [{"video_id": "video-1"}],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(app, "OUTPUT_DIR", root),
                mock.patch.object(app, "OPENNEWS_BATCH_DIR", root / "opennews_batches"),
            ):
                keys = app._opennews_channel_published_event_keys("technology", platform="youtube")
                excluded = app._opennews_channel_published_event_keys(
                    "technology",
                    platform="youtube",
                    exclude_dir="history-1",
                )

            self.assertIn("url:https://example.com/meta-iris", keys)
            self.assertNotIn("url:https://example.com/meta-iris", excluded)

    def test_recent_youtube_match_restores_receipt_without_uploading(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "history-1"
            output_dir.mkdir()
            remote = {
                "video_id": "existing-video",
                "youtube_url": "https://www.youtube.com/watch?v=existing-video",
                "title": "Meta Iris #Shorts",
            }
            with (
                mock.patch.object(app, "OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR", Path(tmp) / "ledger"),
                mock.patch.object(app, "find_recent_youtube_upload", return_value=remote),
                mock.patch.object(app, "upload_video_to_youtube") as upload_mock,
            ):
                record = self._publish_once(output_dir)

            upload_mock.assert_not_called()
            self.assertEqual(record["video_id"], "existing-video")
            self.assertTrue(record["recovered_from_youtube"])

    def test_successful_upload_receipt_blocks_retry_with_stale_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "history-1"
            output_dir.mkdir()
            with (
                mock.patch.object(app, "OPENNEWS_YOUTUBE_PUBLISH_LEDGER_DIR", Path(tmp) / "ledger"),
                mock.patch.object(app, "find_recent_youtube_upload", return_value={}),
                mock.patch.object(
                    app,
                    "upload_video_to_youtube",
                    return_value={
                        "video_id": "new-video",
                        "youtube_url": "https://www.youtube.com/watch?v=new-video",
                    },
                ) as upload_mock,
            ):
                first = self._publish_once(output_dir)
                second = self._publish_once(output_dir)

            self.assertEqual(first["video_id"], "new-video")
            self.assertEqual(second["video_id"], "new-video")
            upload_mock.assert_called_once()

    def test_recent_upload_lookup_matches_exact_source_url(self):
        channel_response = mock.Mock(status_code=200, text="")
        channel_response.json.return_value = {
            "items": [
                {"contentDetails": {"relatedPlaylists": {"uploads": "uploads-1"}}}
            ]
        }
        playlist_response = mock.Mock(status_code=200, text="")
        playlist_response.json.return_value = {
            "items": [
                {
                    "snippet": {
                        "title": "Meta Iris #Shorts",
                        "description": "来源：https://example.com/meta-iris",
                        "publishedAt": "2026-07-10T00:00:00Z",
                        "resourceId": {"videoId": "existing-video"},
                    }
                }
            ]
        }
        with (
            mock.patch.object(youtube_publisher, "refresh_youtube_access_token", return_value="token"),
            mock.patch.object(
                youtube_publisher.requests,
                "get",
                side_effect=[channel_response, playlist_response],
            ),
        ):
            record = youtube_publisher.find_recent_youtube_upload(
                Path("/tmp/token.json"),
                title="different title",
                source_url="https://example.com/meta-iris",
            )

        self.assertEqual(record["video_id"], "existing-video")


if __name__ == "__main__":
    unittest.main()
