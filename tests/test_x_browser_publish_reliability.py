import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app
import x_browser_login_manager
import x_browser_publisher


class XBrowserMediaPreparationTests(unittest.TestCase):
    def test_prepare_video_transcodes_to_x_compatible_h264(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"source-video")
            prepared_dir = root / "prepared"

            def fake_run(command, **kwargs):
                Path(command[-1]).write_bytes(b"prepared-video")
                return mock.Mock(returncode=0, stdout="", stderr="")

            with (
                mock.patch.object(x_browser_publisher, "X_BROWSER_PREPARED_VIDEO_DIR", prepared_dir),
                mock.patch.object(x_browser_publisher, "X_BROWSER_TRANSCODE_UPLOAD", True),
                mock.patch.object(x_browser_publisher.subprocess, "run", side_effect=fake_run) as run_mock,
            ):
                prepared = x_browser_publisher.prepare_x_video_for_upload(source)

            command = run_mock.call_args.args[0]
            self.assertTrue(prepared.exists())
            self.assertEqual(prepared.suffix, ".mp4")
            self.assertEqual(prepared.read_bytes(), b"prepared-video")
            self.assertIn("libx264", command)
            self.assertIn("aac", command)
            self.assertIn("yuv420p", command)
            self.assertIn("+faststart", command)

    def test_system_chromium_is_the_default_browser(self):
        with (
            mock.patch.object(x_browser_publisher, "X_BROWSER_EXECUTABLE_PATH", ""),
            mock.patch.object(Path, "is_file", autospec=True, side_effect=lambda path: str(path) == "/usr/bin/chromium"),
        ):
            executable = x_browser_publisher._resolve_browser_executable()

        self.assertEqual(executable, "/usr/bin/chromium")

    def test_post_matching_uses_title_instead_of_wrapped_source_url(self):
        text = (
            "韩国5760亿美元AI计划带动Vertiv股价单日暴涨9.1%\n"
            "来源：24/7 Wall St\n"
            "https://example.com/a-very-long-source-url\n"
            "#OpenNews #iHouse"
        )
        article_text = (
            "OpenNews Agent · Now 韩国5760亿美元AI计划带动Vertiv股价单日暴涨9.1% "
            "来源：24/7 Wall St https:// example.com/a-very-long-source-url #OpenNews #iHouse 1:11"
        )

        candidates = x_browser_publisher._post_text_match_candidates(text)

        self.assertIn("韩国5760亿美元AI计划带动Vertiv股价单日暴涨9.1%", candidates)
        self.assertTrue(any(candidate in article_text for candidate in candidates))

    def test_media_failure_retries_inside_exclusive_profile_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"video")
            profile = root / "profile"
            success = {"post_id": "post-1", "x_url": "https://x.com/i/web/status/post-1"}
            with (
                mock.patch.object(x_browser_publisher, "X_BROWSER_STATE_DIR", root / "state"),
                mock.patch.object(
                    x_browser_publisher,
                    "X_BROWSER_PUBLISH_STATE_PATH",
                    root / "state" / "publish_state.json",
                ),
                mock.patch.object(x_browser_publisher, "X_BROWSER_MIN_POST_INTERVAL_SECONDS", 0),
                mock.patch.object(x_browser_publisher, "X_BROWSER_MEDIA_FULL_RETRIES", 1),
                mock.patch.object(x_browser_publisher.time, "sleep") as sleep_mock,
                mock.patch.object(
                    x_browser_publisher,
                    "_publish_video_to_x_browser_locked",
                    side_effect=[
                        x_browser_publisher.XBrowserPublishError("media failed to load"),
                        success,
                    ],
                ) as publish_mock,
            ):
                result = x_browser_publisher.publish_video_to_x_browser(
                    source,
                    text="test",
                    user_data_dir=profile,
                )

            self.assertEqual(result["post_id"], "post-1")
            self.assertEqual(publish_mock.call_count, 2)
            sleep_mock.assert_called_once()
            state = json.loads((root / "state" / "publish_state.json").read_text(encoding="utf-8"))
            self.assertEqual(next(iter(state["profiles"].values()))["post_id"], "post-1")

    def test_unverified_result_is_not_recorded_as_publish_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.mp4"
            source.write_bytes(b"video")
            profile = root / "profile"
            with (
                mock.patch.object(x_browser_publisher, "X_BROWSER_STATE_DIR", root / "state"),
                mock.patch.object(
                    x_browser_publisher,
                    "X_BROWSER_PUBLISH_STATE_PATH",
                    root / "state" / "publish_state.json",
                ),
                mock.patch.object(x_browser_publisher, "X_BROWSER_MIN_POST_INTERVAL_SECONDS", 0),
                mock.patch.object(x_browser_publisher, "X_BROWSER_MEDIA_FULL_RETRIES", 0),
                mock.patch.object(
                    x_browser_publisher,
                    "_publish_video_to_x_browser_locked",
                    return_value={"post_id": "", "x_url": "", "raw": {"verified_media": False}},
                ),
            ):
                with self.assertRaises(x_browser_publisher.XBrowserPublishError):
                    x_browser_publisher.publish_video_to_x_browser(
                        source,
                        text="test",
                        user_data_dir=profile,
                    )

            self.assertFalse((root / "state" / "publish_state.json").exists())


class XBrowserLoginStatusTests(unittest.TestCase):
    def test_orphaned_chromium_is_degraded_not_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (
                mock.patch.object(x_browser_login_manager, "X_BROWSER_LOGIN_RUNTIME_DIR", root),
                mock.patch.object(x_browser_login_manager, "_load_pid", side_effect=lambda name: 5 if name == "chromium" else 0),
                mock.patch.object(x_browser_login_manager, "_process_alive", side_effect=lambda pid: pid == 5),
                mock.patch.object(x_browser_login_manager, "x_browser_profile_dir", return_value=root / "profile"),
            ):
                status = x_browser_login_manager.x_browser_login_status()

            self.assertFalse(status["running"])
            self.assertTrue(status["degraded"])
            self.assertTrue(status["profile_in_use"])


class XBrowserAppIntegrationTests(unittest.TestCase):
    def test_degraded_login_session_is_stopped_before_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            video = Path(tmp) / "video.mp4"
            video.write_bytes(b"video")
            with (
                mock.patch.object(app, "_opennews_x_publish_mode", return_value="browser"),
                mock.patch.object(
                    app,
                    "x_browser_login_status",
                    return_value={"running": False, "degraded": True, "profile_in_use": True},
                ),
                mock.patch.object(app, "stop_x_browser_login") as stop_mock,
                mock.patch.object(
                    app,
                    "publish_video_to_x_browser",
                    return_value={"post_id": "post-1", "x_url": "https://x.com/i/web/status/post-1"},
                ),
            ):
                result = app._upload_video_to_opennews_x(video, text="test")

            stop_mock.assert_called_once()
            self.assertEqual(result["publish_mode"], "browser")


if __name__ == "__main__":
    unittest.main()
