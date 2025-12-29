import os
import httpx
import time
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from typing import AsyncIterator

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# Helper for consistent log format
def log(msg: str):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "spitch-ultravox-bridge"}

@app.post("/v1/tts")
async def generate_speech(request: Request):
    """
    Enhanced TTS endpoint with real-time logging for debugging Ultravox interactions.
    """
    request_start = time.perf_counter()
    try:
        # 1. Log Incoming Request (Raw Body)
        # This shows exactly what text Ultravox is sending
        try:
            body_bytes = await request.body()
            body_str = body_bytes.decode("utf-8")
            data = json.loads(body_str)
            log(f"📥 ULTRAVOX REQUEST BODY: {body_str}")
        except Exception as e:
            log(f"⚠️ Could not parse body: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON")

        text = data.get("text")
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            log("❌ Error: No text provided in request")
            raise HTTPException(status_code=400, detail="Text is required")

        log(f"📝 PROCESSING: Voice='{voice_id}' | Lang='{lang_code}' | Text Length={len(text)}")

        # 2. Configure Spitch
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        }
        
        # We request WAV format to make Ultravox happy
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav" 
        }

        async def audio_stream() -> AsyncIterator[bytes]:
            log("🚀 Connecting to Spitch API...")
            spitch_start = time.perf_counter()
            
            async with httpx.AsyncClient(timeout=90.0) as client:
                # We use stream() instead of post() to get headers immediately
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as response:
                    
                    # 3. Log Spitch Response Headers
                    duration = time.perf_counter() - spitch_start
                    log(f"✅ SPITCH CONNECTED in {duration:.2f}s")
                    log(f"🔍 SPITCH HEADERS: Status={response.status_code} | Type={response.headers.get('content-type')}")

                    if response.status_code != 200:
                        error_msg = await response.aread()
                        log(f"❌ SPITCH ERROR: {error_msg.decode('utf-8')}")
                        raise HTTPException(status_code=response.status_code, detail="Spitch API Error")

                    # 4. Stream and Log Chunks
                    chunk_count = 0
                    total_bytes = 0
                    
                    async for chunk in response.aiter_bytes():
                        chunk_size = len(chunk)
                        total_bytes += chunk_size
                        chunk_count += 1
                        
                        # Log every 10th chunk to avoid spamming logs, but show activity
                        if chunk_count % 10 == 1: 
                            log(f"📤 Streaming Chunk #{chunk_count} ({chunk_size} bytes) to Ultravox...")
                        
                        yield chunk

                    total_duration = time.perf_counter() - request_start
                    log(f"🏁 COMPLETE: Streamed {total_bytes} bytes in {total_duration:.2f}s")

        return StreamingResponse(
            audio_stream(),
            media_type="audio/wav", # Set to WAV as requested
            headers={
                "Cache-Control": "no-cache",
                "X-Content-Type-Options": "nosniff"
            }
        )

    except Exception as e:
        log(f"🔥 CRITICAL SYSTEM ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
