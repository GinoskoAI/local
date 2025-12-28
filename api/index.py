import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from spitch import AsyncSpitch

app = FastAPI()

# Initialize Spitch client
spitch = AsyncSpitch(
    api_key=os.getenv("SPITCH_API_KEY")
)

@app.post("/v1/tts")
async def tts(request: Request):
    try:
        payload = await request.json()
        text = payload.get("text")

        if not text or not isinstance(text, str):
            raise HTTPException(status_code=400, detail="Field 'text' is required")

        # Optional query params
        voice = request.query_params.get("voice", "sade")
        language = request.query_params.get("lang", "yo")

        # 🔴 IMPORTANT: generate WAV natively
        response = await spitch.speech.generate(
            text=text,
            voice=voice,
            language=language,
            format="wav"
        )

        audio_bytes = response.audio

        if not audio_bytes:
            raise RuntimeError("Spitch returned empty audio")

        # Ultravox-compatible response
        return StreamingResponse(
            iter([audio_bytes]),
            media_type="audio/wav"
        )

    except Exception as e:
        print("TTS ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
