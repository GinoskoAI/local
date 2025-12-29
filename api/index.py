import os
import httpx
import time
import asyncio
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# --- WAV CONFIGURATION ---
# 1. WAV Header (RIFF/WAVE, 16-bit PCM, Mono, 24kHz)
# This MUST be sent first so Ultravox knows it's a valid audio stream.
WAV_HEADER = (
    b'\x52\x49\x46\x46\x24\x00\x00\x00\x57\x41\x56\x45\x66\x6d\x74\x20'
    b'\x10\x00\x00\x00\x01\x00\x01\x00\x80\x5e\x00\x00\x00\xbd\x00\x00'
    b'\x02\x00\x10\x00\x64\x61\x74\x61\x00\x00\x00\x00'
)

# 2. PCM Silence (10ms of 0-byte audio)
# We feed this to Ultravox while waiting for Spitch.
PCM_SILENCE = b'\x00\x00' * 480 

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        # --- PARSE REQUEST ---
        try:
            body_bytes = await request.body()
            data = json.loads(body_bytes)
            text = data.get("text")
            log(f"📥 ULTRAVOX REQUEST: {text}")
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        if not text:
            raise HTTPException(status_code=400, detail="No text")

        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        # --- SETUP SPITCH ---
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        # Request WAV explicitly
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav" 
        }

        # --- BACKGROUND WORKER ---
        # This fetches Spitch audio in the background while main thread sends silence
        queue = asyncio.Queue()

        async def fetch_spitch_background():
            start_time = time.perf_counter()
            log("🚀 [Background] Requesting Spitch...")
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                        duration = time.perf_counter() - start_time
                        
                        if resp.status_code != 200:
                            log(f"❌ [Background] Spitch Failed: {resp.status_code}")
                            await queue.put(None) # Signal End
                            return

                        log(f"✅ [Background] Spitch Ready in {duration:.2f}s! Buffering...")
                        
                        # Strip Spitch's WAV Header (first 44 bytes) to avoid noise
                        # We already sent our own header.
                        first_chunk = True
                        async for chunk in resp.aiter_bytes():
                            if first_chunk and len(chunk) > 44:
                                chunk = chunk[44:] # Skip header
                                first_chunk = False
                            elif first_chunk:
                                continue # Skip small header-only chunks
                            
                            await queue.put(chunk)

            except Exception as e:
                log(f"🔥 [Background] Error: {e}")
            finally:
                await queue.put(None) # Always signal end

        # --- MAIN STREAM GENERATOR ---
        async def stream_manager():
            # 1. Send Header IMMEDIATELY (Time = 0.0s)
            yield WAV_HEADER
            
            # 2. Start Spitch in Background
            asyncio.create_task(fetch_spitch_background())

            log("⏳ Sending Silence Loop to Ultravox...")
            
            # 3. Queue Loop
            silence_count = 0
            while True:
                try:
                    # Check Queue (Don't wait long, so we can send silence)
                    chunk = queue.get_nowait()
                    
                    if chunk is None: # End of stream
                        log("🏁 Audio Finished.")
                        break
                    
                    # We have real audio! Send it.
                    if silence_count > 0:
                        log(f"🔊 Real Audio Playing! (Stopped silence after {silence_count} frames)")
                        silence_count = 0 # Reset
                    yield chunk

                except asyncio.QueueEmpty:
                    # Queue is empty = Spitch is still thinking.
                    # Send Silence to keep Ultravox alive.
                    yield PCM_SILENCE
                    silence_count += 1
                    await asyncio.sleep(0.01) # Sleep 10ms (matches PCM_SILENCE duration)

        return StreamingResponse(
            stream_manager(), 
            media_type="audio/wav",
            headers={
                "Cache-Control": "no-cache",
                "X-Content-Type-Options": "nosniff"
            }
        )

    except Exception as e:
        log(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
