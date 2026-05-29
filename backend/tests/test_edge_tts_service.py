import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.services.edge_tts_service import EdgeTtsService


class EdgeTtsServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_synthesize_to_file_uses_edge_tts_and_reuses_cached_audio(self):
        calls = []
        created = []

        class FakeCommunicate:
            def __init__(self, text, voice, rate, volume, pitch):
                created.append(
                    {
                        "text": text,
                        "voice": voice,
                        "rate": rate,
                        "volume": volume,
                        "pitch": pitch,
                    }
                )

            async def save(self, path):
                calls.append(path)
                Path(path).write_bytes(b"mp3-bytes")

        service = EdgeTtsService(cache_dir=self.cache_dir)

        with patch(
            "app.services.edge_tts_service.edge_tts",
            SimpleNamespace(Communicate=FakeCommunicate),
        ):
            first = asyncio.run(service.synthesize_to_file("  聚合快讯标题  "))
            second = asyncio.run(service.synthesize_to_file("聚合快讯标题"))

        self.assertEqual(first, second)
        self.assertEqual(first.read_bytes(), b"mp3-bytes")
        self.assertEqual(len(calls), 1)
        self.assertEqual(created[0]["text"], "聚合快讯标题")
        self.assertEqual(created[0]["voice"], "zh-CN-XiaoyiNeural")
        self.assertEqual(created[0]["rate"], "+18%")
        self.assertEqual(created[0]["volume"], "+0%")
        self.assertEqual(created[0]["pitch"], "+2Hz")


if __name__ == "__main__":
    unittest.main()
