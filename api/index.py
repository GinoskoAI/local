import os
import httpx
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# WAV HEADER + SILENCE (16-bit PCM, Mono, 24kHz)
# This mimics a valid WAV stream so Ultravox doesn't disconnect while waiting.
WAV_HEADER = (
    b'\x52\x49\x46\x46\x24\x00\x00\x00\x57\x41\x56\x45\x66\x6d\x74\x20'
    b'\x10\x00\x00\x00\x01\x00\x01\x00\x80\x5e\x00\x00\x00\xbd\x00\x00'
    b'\x02\x00\x10\x00\x64\x61\x74\x61\x00\x00\x00\x00'
)
# 10ms of silence in PCM format
PCM_SILENCE = b'\x00\x00' * 240 

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

        # 2. Spitch Configuration (WAV Mode)
        spitch_url = "https://api.spi-tch.com/v1/speech"
        
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav"  # <--- FORCE WAV FORMAT
        }

        # 3. Background Queue Logic (To prevent Timeout)
        queue = asyncio.Queue()
        
        async def fetch_spitch():
            try:
                # 60s timeout because Spitch is slow
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                        if resp.status_code != 200:
                            print(f"Spitch Error {resp.status_code}")
                            await queue.put(None) 
                            return

                        # Chunk the response into the queue
                        async for chunk in resp.aiter_bytes():
                            await queue.put(chunk)
                            
            except Exception as e:
                print(f"Fetch Error: {e}")
            finally:
                await queue.put(None) # Signal end of stream

        # 4. The Streaming Response Generator
        async def wav_stream_manager():
            # Send the WAV Header first (Critical for Ultravox to accept the stream)
            yield WAV_HEADER
            
            # Start fetching Spitch in background
            asyncio.create_task(fetch_spitch())

            while True:
                try:
                    # Check for real audio
                    chunk = await asyncio.wait_for(queue.get(), timeout=0.01)
                    if chunk is None:
                        break
                    yield chunk

                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    # If waiting for Spitch, send Silence
                    yield PCM_SILENCE
                    await asyncio.sleep(0.01)

        return StreamingResponse(
            wav_stream_manager(), 
            media_type="audio/wav" # <--- Correct MIME Type
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
