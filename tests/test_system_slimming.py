import json
import re
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import app
import facebook_publisher


class HistoryBundleTests(unittest.TestCase):
    def test_bundle_contains_editing_deliverables(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            (output_dir / "audio").mkdir()
            (output_dir / "digital_human").mkdir()
            (output_dir / "materials").mkdir()
            (output_dir / "final").mkdir()
            (output_dir / "script.json").write_text('{"title":"test"}', encoding="utf-8")
            (output_dir / "script_readable.txt").write_text("script", encoding="utf-8")
            (output_dir / "social_posts.txt").write_text("post", encoding="utf-8")
            (output_dir / "audio" / "voice.mp3").write_bytes(b"audio")
            (output_dir / "digital_human" / "host.mp4").write_bytes(b"host")
            (output_dir / "materials" / "image.jpg").write_bytes(b"image")
            (output_dir / "final" / "video.mp4").write_bytes(b"final")

            result = {
                "topic": "Bundle test",
                "title": "Bundle title",
                "total_duration": 5,
                "final_video_path": "final/video.mp4",
                "segments": [
                    {
                        "type": "digital_human",
                        "start": 0,
                        "end": 5,
                        "duration": 5,
                        "script": "hello",
                        "audio_path": "audio/voice.mp3",
                        "video_path": "digital_human/host.mp4",
                        "material_paths": ["materials/image.jpg"],
                    }
                ],
            }

            bundle_path = app._build_history_bundle_zip(output_dir, result)
            self.addCleanup(bundle_path.unlink, missing_ok=True)
            root = app._bundle_root_name(output_dir.name, result)
            with zipfile.ZipFile(bundle_path) as archive:
                names = set(archive.namelist())
                self.assertIn(f"{root}/00_项目说明/README.txt", names)
                self.assertIn(f"{root}/01_脚本/script.json", names)
                self.assertIn(f"{root}/02_配音/01_digital_human.mp3", names)
                self.assertIn(f"{root}/03_数字人视频/01_digital_human.mp4", names)
                self.assertIn(f"{root}/04_素材/01_material_01.jpg", names)
                self.assertIn(f"{root}/06_剪辑时间轴数据/timeline.csv", names)
                self.assertIn(f"{root}/07_成片/final_video.mp4", names)
                timeline = archive.read(f"{root}/06_剪辑时间轴数据/timeline.csv").decode("utf-8-sig")
                self.assertIn("hello", timeline)


class PublishingWiringTests(unittest.TestCase):
    def test_facebook_page_listing_is_wired_into_app(self):
        self.assertIs(app.get_facebook_pages, facebook_publisher.get_facebook_pages)


class RetiredFeatureTests(unittest.IsolatedAsyncioTestCase):
    async def test_retired_manual_review_endpoint_stays_gone(self):
        with mock.patch.object(app, "_require_user", return_value=({"role": "admin"}, None)):
            response = await app.opennews_batches_prepare_review(object())
        self.assertEqual(response.status_code, 410)

    async def test_retired_avatar_lab_endpoint_stays_gone(self):
        with mock.patch.object(app, "_require_user", return_value=({"role": "admin"}, None)):
            response = await app.admin_generate_avatar(object(), None)
        self.assertEqual(response.status_code, 410)


class RepositoryHygieneTests(unittest.TestCase):
    def test_docker_context_excludes_secrets_and_development_files(self):
        root = Path(app.__file__).parent
        patterns = {
            line.strip()
            for line in (root / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        for required in {".env", "docs/", "tests/", "tools/_*.py"}:
            self.assertIn(required, patterns)

    def test_removed_heavy_dependencies_are_not_declared(self):
        root = Path(app.__file__).parent
        requirement_names = {
            re.split(r"[<>=!~]", line.strip(), maxsplit=1)[0].lower()
            for line in (root / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        self.assertNotIn("whisperx", requirement_names)
        self.assertNotIn("volcengine-python-sdk", requirement_names)
        self.assertIn("faster-whisper", requirement_names)
        self.assertIn("volcengine", requirement_names)

    def test_avatar_manifest_only_references_existing_files(self):
        root = Path(app.__file__).parent
        manifest = json.loads((root / "assets" / "avatar_library_manifest.json").read_text(encoding="utf-8"))
        missing = [filename for filename in manifest if not (root / "assets" / filename).is_file()]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
