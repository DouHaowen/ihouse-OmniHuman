import os
import unittest
from unittest import mock

import generate_script
import opennews_admin


class Gpt54RelayConfigTests(unittest.TestCase):
    def test_general_script_defaults_to_gpt54(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(generate_script._get_openai_relay_model(), "gpt-5.4")
            self.assertFalse(generate_script._openai_relay_use_chat_completions())

    def test_opennews_defaults_to_gpt54(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(opennews_admin._get_opennews_relay_model(), "gpt-5.4")
            self.assertEqual(opennews_admin._get_opennews_relay_model_attempts(), ["gpt-5.4"])
            self.assertFalse(opennews_admin._opennews_relay_use_chat_completions())

    def test_office_relay_uses_standard_chat_completions(self):
        env = {
            "OPENAI_RELAY_BASE_URL": "https://api.office.ihousejapan.cn",
            "OPENAI_RELAY_USE_CHAT_COMPLETIONS": "1",
            "OPENNEWS_RELAY_USE_CHAT_COMPLETIONS": "1",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(generate_script._openai_relay_use_chat_completions())
            self.assertTrue(opennews_admin._opennews_relay_use_chat_completions())


if __name__ == "__main__":
    unittest.main()
