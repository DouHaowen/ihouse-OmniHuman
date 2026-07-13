import json
import os
import unittest
from unittest.mock import Mock, patch

import app


class MediaInsightsAgentAuthTests(unittest.TestCase):
    @staticmethod
    def request(*, authorization: str = "", api_key: str = "") -> Mock:
        request = Mock()
        request.headers = {}
        if authorization:
            request.headers["Authorization"] = authorization
        if api_key:
            request.headers["X-Media-Insights-Key"] = api_key
        return request

    @patch.object(app, "_verify_jclaw_lab_token", return_value=None)
    def test_accepts_server_to_server_api_key(self, _verify):
        with patch.dict(os.environ, {"MEDIA_INSIGHTS_AGENT_API_KEY": "test-secret"}, clear=False):
            self.assertIsNone(
                app._require_media_insights_agent_access(self.request(api_key="test-secret"))
            )
            self.assertIsNone(
                app._require_media_insights_agent_access(
                    self.request(authorization="Bearer test-secret")
                )
            )

    @patch.object(app, "_verify_jclaw_lab_token", return_value=None)
    def test_rejects_invalid_api_key(self, _verify):
        with patch.dict(os.environ, {"MEDIA_INSIGHTS_AGENT_API_KEY": "test-secret"}, clear=False):
            response = app._require_media_insights_agent_access(self.request(api_key="wrong"))
        self.assertEqual(response.status_code, 401)
        self.assertEqual(json.loads(response.body)["error"], "无效的媒体数据接口凭证")

    @patch.object(app, "_verify_jclaw_lab_token", return_value={"app": "ihouse-media-insights"})
    def test_accepts_valid_jclaw_app_token(self, _verify):
        with patch.dict(
            os.environ,
            {"MEDIA_INSIGHTS_AGENT_API_KEY": "", "MEDIA_INSIGHTS_AGENT_API_KEYS": ""},
            clear=False,
        ):
            self.assertIsNone(
                app._require_media_insights_agent_access(
                    self.request(authorization="Bearer signed.jwt.token")
                )
            )


if __name__ == "__main__":
    unittest.main()
