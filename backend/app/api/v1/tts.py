"""Text-to-speech endpoints for realtime broadcast playback."""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.services.edge_tts_service import edge_tts_service

router = APIRouter()


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

    return FileResponse(
        audio_path,
        media_type="audio/mpeg",
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )
