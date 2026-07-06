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
        self.assertEqual(created[0]["pitch"], "+0Hz")

    def test_synthesize_to_file_wraps_edge_tts_errors_as_runtime_error(self):
        class FakeCommunicate:
            def __init__(self, text, voice, rate, volume, pitch):
                pass

            async def save(self, path):
                raise Exception("No audio was received")

        service = EdgeTtsService(cache_dir=self.cache_dir)

        with patch(
            "app.services.edge_tts_service.edge_tts",
            SimpleNamespace(Communicate=FakeCommunicate),
        ):
            with self.assertRaisesRegex(RuntimeError, "edge-tts synthesis failed"):
                asyncio.run(service.synthesize_to_file("XD联德股涨停"))

        self.assertEqual(list(self.cache_dir.glob("*.tmp")), [])

    def test_synthesize_to_file_times_out_slow_edge_tts(self):
        class FakeCommunicate:
            def __init__(self, text, voice, rate, volume, pitch):
                pass

            async def save(self, path):
                await asyncio.sleep(0.05)

        service = EdgeTtsService(cache_dir=self.cache_dir, timeout_seconds=0.01)

        with patch(
            "app.services.edge_tts_service.edge_tts",
            SimpleNamespace(Communicate=FakeCommunicate),
        ):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                asyncio.run(service.synthesize_to_file("慢速神经语音"))

        self.assertEqual(list(self.cache_dir.glob("*.tmp")), [])

    def test_synthesize_to_file_times_out_while_waiting_for_synthesis_slot(self):
        class FakeCommunicate:
            def __init__(self, text, voice, rate, volume, pitch):
                pass

            async def save(self, path):
                Path(path).write_bytes(b"mp3-bytes")

        async def synthesize_while_slot_busy():
            service = EdgeTtsService(
                cache_dir=self.cache_dir,
                timeout_seconds=0.01,
                max_concurrent_synthesis=1,
            )
            await service._synthesis_semaphore.acquire()
            try:
                with patch(
                    "app.services.edge_tts_service.edge_tts",
                    SimpleNamespace(Communicate=FakeCommunicate),
                ):
                    with self.assertRaisesRegex(RuntimeError, "timed out"):
                        await asyncio.wait_for(
                            service.synthesize_to_file("排队中的播报"),
                            timeout=0.05,
                        )
            finally:
                service._synthesis_semaphore.release()

        asyncio.run(synthesize_while_slot_busy())

        self.assertEqual(list(self.cache_dir.glob("*.tmp")), [])

    def test_synthesize_to_file_limits_concurrent_edge_tts_calls(self):
        active = 0
        max_active = 0

        class FakeCommunicate:
            def __init__(self, text, voice, rate, volume, pitch):
                pass

            async def save(self, path):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)
                Path(path).write_bytes(b"mp3-bytes")
                active -= 1

        async def synthesize_pair():
            service = EdgeTtsService(
                cache_dir=self.cache_dir,
                timeout_seconds=1,
                max_concurrent_synthesis=1,
            )
            with patch(
                "app.services.edge_tts_service.edge_tts",
                SimpleNamespace(Communicate=FakeCommunicate),
            ):
                await asyncio.gather(
                    service.synthesize_to_file("第一条播报"),
                    service.synthesize_to_file("第二条播报"),
                )

        asyncio.run(synthesize_pair())

        self.assertEqual(max_active, 1)


if __name__ == "__main__":
    unittest.main()
