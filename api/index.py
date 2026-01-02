import os
import httpx
import time
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        # 1. Parse Request
        try:
            body_bytes = await request.body()
            data = json.loads(body_bytes)
            text = data.get("text")
            log(f"📥 REQUEST: {text[:50]}...")
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        if not text:
            raise HTTPException(status_code=400, detail="No text")

        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        # 2. Spitch Config
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        # We request WAV directly. 
        # Since Spitch updated their API, we assume this now streams faster 
        # or sends the header immediately.
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav" 
        }

        # 3. Direct Proxy Generator
        async def stream_generator():
            start_time = time.perf_counter()
            log("🚀 Contacting Spitch...")
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Open the stream and keep it open
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                    
                    if resp.status_code != 200:
                        error_text = await resp.aread()
                        log(f"❌ Spitch Error: {resp.status_code} - {error_text}")
                        return

                    log(f"✅ Spitch Response Received in {time.perf_counter() - start_time:.2f}s")
                    log(f"🔍 Content-Type: {resp.headers.get('content-type')}")

                    # Stream bytes exactly as received (Passthrough)
                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            stream_generator(), 
            media_type="audio/wav",
            headers={
                "Cache-Control": "no-cache",
                "X-Content-Type-Options": "nosniff"
            }
        )

    except Exception as e:
        log(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
