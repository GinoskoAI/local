import os
import httpx
import time
import asyncio
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# --- 1. CORRECTED WAV HEADER (24kHz, 16-bit, Mono) ---
# Hex for 24000Hz is 5DC0 -> \xC0\x5D
# Hex for 48000 ByteRate is BB80 -> \x80\xBB
WAV_HEADER = (
    b'\x52\x49\x46\x46\xff\xff\xff\xff\x57\x41\x56\x45\x66\x6d\x74\x20'
    b'\x10\x00\x00\x00\x01\x00\x01\x00\xc0\x5d\x00\x00\x80\xbb\x00\x00'
    b'\x02\x00\x10\x00\x64\x61\x74\x61\xff\xff\xff\xff'
)

# --- 2. PCM SILENCE (24kHz) ---
# 10ms of silence = 24000 * 2 bytes * 0.01s = 480 bytes
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
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        if not text:
            raise HTTPException(status_code=400, detail="No text")

        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        log(f"📥 ULTRAVOX REQUEST: {text[:50]}...")

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

        queue = asyncio.Queue()

        # --- BACKGROUND WORKER ---
        async def fetch_spitch_background():
            start_time = time.perf_counter()
            log("🚀 [Background] Requesting Spitch...")
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                        if resp.status_code != 200:
                            log(f"❌ Spitch Error: {resp.status_code}")
                            await queue.put(None)
                            return

                        log(f"✅ Spitch Ready in {time.perf_counter() - start_time:.2f}s")
                        
                        # SKIP SPITCH HEADER (44 bytes) to avoid double-header noise
                        first_chunk = True
                        async for chunk in resp.aiter_bytes():
                            if first_chunk:
                                if len(chunk) > 44:
                                    await queue.put(chunk[44:])
                                first_chunk = False
                            else:
                                await queue.put(chunk)

            except Exception as e:
                log(f"🔥 Error: {e}")
            finally:
                await queue.put(None)

        # --- MAIN STREAM GENERATOR ---
        async def stream_manager():
            # 1. Send our Clean Header immediately
            yield WAV_HEADER
            
            # 2. Start Background Fetch
            asyncio.create_task(fetch_spitch_background())
            
            # 3. Stream Loop
            spitch_has_started = False
            
            while True:
                if not spitch_has_started:
                    # MODE A: Waiting for Spitch (Send Silence)
                    try:
                        # Check if data arrived (non-blocking)
                        chunk = queue.get_nowait()
                        
                        if chunk is None: break # End of stream
                        
                        # Data arrived! Switch modes.
                        log("🔊 Spitch Audio Started! Switching to direct stream.")
                        spitch_has_started = True
                        yield chunk
                        
                    except asyncio.QueueEmpty:
                        # No data yet? Send silence and keep waiting.
                        yield PCM_SILENCE
                        await asyncio.sleep(0.01) 
                else:
                    # MODE B: Spitch is streaming (Block and wait for data)
                    # We DO NOT send silence here, or it will stutter.
                    chunk = await queue.get() # Waits forever until data arrives
                    
                    if chunk is None:
                        log("🏁 Stream Finished.")
                        break
                    yield chunk

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
