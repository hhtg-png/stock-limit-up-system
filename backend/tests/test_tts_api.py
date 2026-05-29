import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.tts import router as tts_router


class TtsApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.audio_path = Path(self.temp_dir.name) / "speech.mp3"
        self.audio_path.write_bytes(b"mp3-bytes")
        app = FastAPI()
        app.include_router(tts_router, prefix="/tts")
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        self.temp_dir.cleanup()

    def test_speech_endpoint_returns_cached_mp3_file(self):
        with patch(
            "app.api.v1.tts.edge_tts_service.synthesize_to_file",
            AsyncMock(return_value=self.audio_path),
        ) as synthesize:
            response = self.client.get("/tts/speech", params={"text": "聚合快讯标题"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"mp3-bytes")
        self.assertEqual(response.headers["content-type"], "audio/mpeg")
        self.assertIn("public", response.headers["cache-control"])
        synthesize.assert_awaited_once_with("聚合快讯标题", voice=None)

    def test_speech_endpoint_rejects_empty_text(self):
        response = self.client.get("/tts/speech", params={"text": "   "})

        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
