import os
import httpx
import asyncio
import wave
import io
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

SPITCH_URL = "https://api.spi-tch.com/v1/speech"


def generate_silence_wav_frame(duration_sec=0.1, sample_rate=24000, channels=1, sampwidth=2):
    """
    Generate a short WAV frame of silence (100ms by default).
    Returns raw WAV bytes (header + data).
    """
    num_frames = int(duration_sec * sample_rate)
    silent_data = b'\x00' * num_frames * channels * sampwidth

    buffer = io.BytesIO()
    with wave.open(buffer, 'wb') as wav_file:
        wav_file.setnchannels(channels)
        wav_file.setsampwidth(sampwidth)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(silent_data)
    return buffer.getvalue()


@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text
        }

        # Queue to hold audio chunks from Spitch
        audio_queue = asyncio.Queue()
        done = asyncio.Event()

        async def fetch_spitch_audio():
            """Fetch full audio from Spitch and put it into the queue."""
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    async with client.stream("POST", SPITCH_URL, json=payload, headers=headers) as resp:
                        print(f"Spitch Response Status: {resp.status_code}")
                        content_type = resp.headers.get("content-type", "")
                        print(f"Spitch Content-Type: {content_type}")

                        if resp.status_code != 200:
                            error_body = await resp.aread()
                            print(f"Spitch Error: {error_body.decode()}")
                            return

                        # Read full audio into memory (since Spitch isn't streaming anyway)
                        full_audio = b""
                        async for chunk in resp.aiter_bytes():
                            full_audio += chunk

                        await audio_queue.put(full_audio)

            except Exception as e:
                print(f"Fetch Error: {e}")
            finally:
                done.set()

        # Start Spitch fetch in background
        fetch_task = asyncio.create_task(fetch_spitch_audio())

        # Stream: send silence until real audio arrives
        async def audio_stream():
            silence_frame = generate_silence_wav_frame(duration_sec=0.2, sample_rate=24000)
            buffer_sent = False

            while not done.is_set():
                # Send short silence burst to keep connection alive
                yield silence_frame
                await asyncio.sleep(0.18)  # ~5 Hz keepalive

            # Now send real audio
            if not audio_queue.empty():
                real_audio = await audio_queue.get()
                yield real_audio
            else:
                # Fallback: if no audio, send one final silence
                yield silence_frame

        return StreamingResponse(
            audio_stream(),
            media_type="audio/wav"
        )

    except Exception as e:
        print(f"System Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
