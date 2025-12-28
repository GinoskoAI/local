import os
import httpx
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# A small chunk of WAV silence to keep the connection alive
# This is a 44.1kHz Mono WAV silence header/frame
WAV_SILENCE_CHUNK = b'\x52\x49\x46\x46\x24\x00\x00\x00\x57\x41\x56\x45\x66\x6d\x74\x20\x10\x00\x00\x00\x01\x00\x01\x00\x44\xac\x00\x00\x88\x58\x01\x00\x02\x00\x10\x00\x64\x61\x74\x61\x00\x00\x00\x00'

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
        
        # NATIVE WAV REQUEST
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text,
            "format": "wav"  # Force Spitch to return WAV
        }

        queue = asyncio.Queue()
        
        async def fetch_spitch():
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                        if resp.status_code != 200:
                            print(f"Spitch Error {resp.status_code}")
                            await queue.put(None)
                            return

                        async for chunk in resp.aiter_bytes():
                            await queue.put(chunk)
            except Exception as e:
                print(f"Fetch Error: {e}")
            finally:
                await queue.put(None)

        async def stream_manager():
            task = asyncio.create_task(fetch_spitch())
            
            # Initial Silence to wake up Ultravox
            yield WAV_SILENCE_CHUNK

            while True:
                try:
                    # Check for real audio
                    chunk = await asyncio.wait_for(queue.get(), timeout=0.1)
                    if chunk is None:
                        break
                    yield chunk
                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    # Keep the line hot with tiny empty bytes if needed
                    yield b'' 
                    await asyncio.sleep(0.01)

        return StreamingResponse(
            stream_manager(), 
            media_type="audio/wav" # Correct MIME type for Ultravox
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
