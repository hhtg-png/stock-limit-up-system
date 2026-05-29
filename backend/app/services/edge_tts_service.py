"""Neural TTS audio cache backed by the edge-tts package."""
import asyncio
import hashlib
import re
from pathlib import Path
from typing import Dict, Optional

try:
    import edge_tts
except ImportError:  # pragma: no cover - exercised only when dependency is missing in runtime
    edge_tts = None


class EdgeTtsService:
    def __init__(
        self,
        *,
        cache_dir: Optional[Path] = None,
        voice: str = "zh-CN-XiaoyiNeural",
        rate: str = "+18%",
        volume: str = "+0%",
        pitch: str = "+0Hz",
        max_text_length: int = 180,
    ):
        self.cache_dir = Path(cache_dir or "data/tts-cache")
        self.voice = voice
        self.rate = rate
        self.volume = volume
        self.pitch = pitch
        self.max_text_length = max_text_length
        self._locks: Dict[str, asyncio.Lock] = {}

    async def synthesize_to_file(self, text: str, *, voice: Optional[str] = None) -> Path:
        clean_text = self._normalize_text(text)
        if not clean_text:
            raise ValueError("text is required")

        voice_name = (voice or self.voice).strip() or self.voice
        cache_key = self._cache_key(clean_text, voice_name)
        target_path = self.cache_dir / f"{cache_key}.mp3"
        if self._is_valid_audio_file(target_path):
            return target_path

        lock = self._locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            if self._is_valid_audio_file(target_path):
                return target_path

            if edge_tts is None:
                raise RuntimeError("edge-tts is not installed")

            self.cache_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = target_path.with_suffix(".tmp")
            try:
                communicate = edge_tts.Communicate(
                    clean_text,
                    voice=voice_name,
                    rate=self.rate,
                    volume=self.volume,
                    pitch=self.pitch,
                )
                await communicate.save(str(tmp_path))
                if not self._is_valid_audio_file(tmp_path):
                    raise RuntimeError("edge-tts returned empty audio")
                tmp_path.replace(target_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)

        return target_path

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()[: self.max_text_length]

    def _cache_key(self, text: str, voice: str) -> str:
        raw = f"{voice}|{self.rate}|{self.volume}|{self.pitch}|{text}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _is_valid_audio_file(path: Path) -> bool:
        try:
            return path.exists() and path.stat().st_size > 0
        except OSError:
            return False


edge_tts_service = EdgeTtsService()
