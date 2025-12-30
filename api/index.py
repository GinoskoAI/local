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
            log(f"📥 REQUEST: {text[:40]}...")
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        # 2. Spitch Config - ENABLE TRUE STREAMING
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        # CRITICAL CHANGE: We remove 'format': 'wav'. 
        # We let Spitch use its default (usually MP3) which streams instantly.
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "stream": True # Explicitly request streaming
        }

        # 3. Direct Pipe Generator (No Buffering)
        async def stream_generator():
            start_time = time.perf_counter()
            log("🚀 Contacting Spitch...")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                    
                    if resp.status_code != 200:
                        err = await resp.aread()
                        log(f"❌ Error: {err}")
                        return

                    first_byte_time = time.perf_counter() - start_time
                    log(f"⚡ LATENCY CHECK: First byte received in {first_byte_time:.2f}s")
                    
                    # Log the content type to confirm it is MP3
                    ct = resp.headers.get('content-type')
                    log(f"🔍 Format received: {ct}")

                    # Stream bytes immediately to Ultravox
                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            stream_generator(), 
            media_type="audio/mpeg", # We tell Ultravox it's MP3
            headers={
                "Cache-Control": "no-cache",
                "X-Content-Type-Options": "nosniff"
            }
        )

    except Exception as e:
        log(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
