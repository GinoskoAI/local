import os
import httpx
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# A single frame of MP3 silence (approx 26ms at 44.1kHz)
# This trick keeps the connection alive
SILENT_MP3_FRAME = (
    b'\xff\xfb\x90\x64\x00\x0f\xf0\x00\x00\x69\x00\x00\x00\x08\x00\x00'
    b'\x0d\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
)

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text
        }

        async def combined_generator():
            # PHASE 1: Send Silence immediately (Buys ~2 seconds)
            # We send 100 frames of silence (~2.6 seconds) to prevent timeout
            for _ in range(100):
                yield SILENT_MP3_FRAME
                # Small sleep to simulate real-time streaming
                await asyncio.sleep(0.026)

            # PHASE 2: Fetch and Stream Spitch Audio
            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                    if resp.status_code != 200:
                        print(f"Spitch Error: {resp.status_code}")
                        return 
                    
                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            combined_generator(), 
            media_type="audio/mpeg"
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
