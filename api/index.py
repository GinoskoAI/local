import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from spitch import AsyncSpitch
import io

app = FastAPI()

client = AsyncSpitch(api_key=os.getenv("SPITCH_API_KEY"))

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        payload = await request.json()
        text = payload.get("text")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        voice = request.query_params.get("voice", "sade")
        lang = request.query_params.get("lang", "yo")

        # 🔴 IMPORTANT: Fully generate audio FIRST
        audio_bytes = b""

        response = await client.speech.generate(
            text=text,
            voice=voice,
            language=lang,
            format="mp3"
        )

        audio_bytes = response.audio  # ← full binary blob

        if not audio_bytes:
            raise RuntimeError("Spitch returned empty audio")

        # ✅ Now stream from memory (Ultravox-safe)
        def audio_stream():
            yield audio_bytes

        return StreamingResponse(
            audio_stream(),
            media_type="audio/mpeg"
        )

    except Exception as e:
        print("TTS ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
