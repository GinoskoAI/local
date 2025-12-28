import os
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        # 1. Parse Request
        data = await request.json()
        text = data.get("text")
        
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        # 2. Define the Upstream Request (Manual Mode)
        url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text
        }

        # 3. Generator with Manual Client Control
        async def upstream_generator():
            # We open the client manually so WE control when it closes
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream("POST", url, json=payload, headers=headers) as resp:
                    # If Spitch returns an error (e.g. 401/500), print it
                    if resp.status_code != 200:
                        error_msg = await resp.read()
                        print(f"Spitch API Error {resp.status_code}: {error_msg}")
                        yield b"" 
                        return

                    # Stream bytes one by one
                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            upstream_generator(), 
            media_type="audio/mpeg"
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
