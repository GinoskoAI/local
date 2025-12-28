import os
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        # Use the exact URL from your successful logs
        spitch_url = "https://api.spi-tch.com/v1/speech"
        
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "stream": True  # <--- TRYING TO FORCE STREAMING
        }

        async def upstream_generator():
            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                    # LOGGING: Print what Spitch is actually sending back
                    print(f"Spitch Response Status: {resp.status_code}")
                    print(f"Spitch Content-Type: {resp.headers.get('content-type')}")

                    if resp.status_code != 200:
                        error_text = await resp.aread()
                        print(f"ERROR BODY: {error_text}")
                        return 

                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            upstream_generator(), 
            media_type="audio/mpeg"
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
