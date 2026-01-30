import os
import httpx
import time
import json
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# --- PERFECT 24kHz WAV HEADER ---
# This forces Ultravox to accept the stream immediately.
# Hex: 5DC0 = 24000Hz
WAV_HEADER = (
    b'\x52\x49\x46\x46\xff\xff\xff\xff\x57\x41\x56\x45\x66\x6d\x74\x20'
    b'\x10\x00\x00\x00\x01\x00\x01\x00\xc0\x5d\x00\x00\x80\xbb\x00\x00'
    b'\x02\x00\x10\x00\x64\x61\x74\x61\xff\xff\xff\xff'
)

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        # 1. Parse Incoming Request
        try:
            body_bytes = await request.body()
            data = json.loads(body_bytes)
            text = data.get("text")
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        # 2. Get Params (or defaults)
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            # Send silence if no text, to prevent crashes
            return StreamingResponse(iter([WAV_HEADER + (b'\x00'*1000)]), media_type="audio/wav")

        log(f"📥 REQUEST ({lang_code}/{voice_id}): {text[:40]}...")

        # 3. Spitch Config
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav"
        }

        # 4. Smart Stream Generator
        async def stream_generator():
            # Always send OUR valid header first. 
            # This prevents "Invalid WAV Header" errors in Ultravox.
            yield WAV_HEADER
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                    
                    # ERROR CHECK: If Spitch fails, log it and stop. 
                    # Do NOT send the error text to Ultravox.
                    if resp.status_code != 200:
                        err_text = await resp.aread()
                        log(f"❌ SPITCH ERROR ({resp.status_code}): {err_text.decode('utf-8')}")
                        # We simply return, leaving the stream as just a header (silent failure is better than crash)
                        return

                    log(f"✅ Streaming Audio ({lang_code})...")

                    # Stream the audio, but SKIP Spitch's header (first 44 bytes)
                    # to avoid static pop/noise since we sent our own header.
                    is_first_chunk = True
                    async for chunk in resp.aiter_bytes():
                        if is_first_chunk:
                            if len(chunk) > 44:
                                yield chunk[44:]
                            is_first_chunk = False
                        else:
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
        log(f"🔥 System Error: {str(e)}")
        # Return a silent WAV file on crash so Ultravox doesn't disconnect
        return StreamingResponse(iter([WAV_HEADER + (b'\x00'*1000)]), media_type="audio/wav")
