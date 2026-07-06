"""Text-to-speech endpoints for realtime broadcast playback."""
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger
from pydantic import BaseModel, Field

from app.services.edge_tts_service import edge_tts_service

router = APIRouter()


class SpeechPlaybackLogRequest(BaseModel):
    stage: str = Field(..., min_length=1, max_length=64)
    mode: str = Field(..., min_length=1, max_length=40)
    text: str = Field("", max_length=160)
    elapsed_ms: Optional[int] = Field(None, ge=0, le=60000)
    detail: dict[str, Any] = Field(default_factory=dict)


@router.get("/speech", summary="神经语音播报音频")
async def get_speech_audio(
    text: str = Query(..., min_length=1, max_length=240, description="待播报文本"),
    voice: Optional[str] = Query(None, max_length=80, description="edge-tts voice name"),
):
    if not text.strip():
        raise HTTPException(status_code=422, detail="text is required")

    try:
        audio_path = await edge_tts_service.synthesize_to_file(text, voice=voice)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.warning(f"TTS synthesis failed unexpectedly: {exc}")
        raise HTTPException(status_code=503, detail="TTS synthesis unavailable") from exc

    return FileResponse(
        audio_path,
        media_type="audio/mpeg",
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )


@router.post("/playback-log", summary="客户端语音播放状态日志")
async def record_speech_playback_log(payload: SpeechPlaybackLogRequest):
    safe_detail = {
        str(key)[:40]: value
        for key, value in list((payload.detail or {}).items())[:8]
        if isinstance(value, (str, int, float, bool)) or value is None
    }
    safe_text = payload.text.strip().replace("\n", " ")[:120]
    logger.info(
        f"TTS_PLAYBACK stage={payload.stage} mode={payload.mode} "
        f"elapsed_ms={payload.elapsed_ms} text={safe_text} detail={safe_detail}"
    )
    return {"ok": True}
