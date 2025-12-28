import os
import httpx
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

# Standard MP3 Silence Frame (approx 26ms)
SILENT_FRAME = (
    b'\xff\xfb\x90\x64\x00\x0f\xf0\x00\x00\x69\x00\x00\x00\x08\x00\x00'
    b'\x0d\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
)

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

        # 2. Setup Spitch Request
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text
        }

        # 3. Define the Background Worker
        # This will run Spitch in the background and push audio to a Queue
        queue = asyncio.Queue()
        
        async def fetch_spitch():
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", spitch_url, json=payload, headers=headers) as resp:
                        if resp.status_code != 200:
                            print(f"Spitch Error {resp.status_code}")
                            await queue.put(None) # Signal error/end
                            return

                        async for chunk in resp.aiter_bytes():
                            await queue.put(chunk)
            except Exception as e:
                print(f"Fetch Error: {e}")
            finally:
                await queue.put(None) # Signal completion

        # 4. The Main Stream Generator
        async def stream_manager():
            # Start fetching Spitch in the background
            task = asyncio.create_task(fetch_spitch())

            # While the background task is running...
            while True:
                try:
                    # Check if we have real audio ready (don't wait long)
                    # If the queue is empty, this raises TimeoutError immediately
                    chunk = await asyncio.wait_for(queue.get(), timeout=0.01)
                    
                    if chunk is None: # End of stream signal
                        break
                    
                    yield chunk

                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    # NO AUDIO YET? Send Silence to keep Ultravox alive
                    yield SILENT_FRAME
                    # Sleep briefly to match MP3 frame duration (~26ms)
                    await asyncio.sleep(0.020)

        return StreamingResponse(
            stream_manager(), 
            media_type="audio/mpeg"
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
