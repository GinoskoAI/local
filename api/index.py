import os
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from io import BytesIO

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

@app.get("/health")
async def health_check():
    """Health check endpoint to verify the server is running"""
    return {"status": "ok", "service": "spitch-ultravox-bridge"}

@app.post("/v1/tts")
async def generate_speech(request: Request):
    """
    Generate speech using Spitch API and return complete audio to Ultravox.
    This version pre-buffers the entire audio to avoid streaming issues.
    """
    try:
        # 1. Parse the incoming request from Ultravox
        data = await request.json()
        text = data.get("text")
        
        # Get voice and language from URL parameters (allows flexibility)
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        print(f"Generating {lang_code} audio for: '{text[:50]}...' using voice '{voice_id}'")

        # 2. Prepare the request to Spitch API
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

        # 3. Fetch the COMPLETE audio file from Spitch (blocking approach)
        # This waits for Spitch to finish generating before sending anything
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(spitch_url, json=payload, headers=headers)
            
            if response.status_code != 200:
                error_text = response.text
                print(f"Spitch API Error {response.status_code}: {error_text}")
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"Spitch API failed: {error_text}"
                )
            
            # Get the complete audio data
            audio_data = response.content
            
            if len(audio_data) < 100:
                print(f"Warning: Audio data suspiciously small ({len(audio_data)} bytes)")
            else:
                print(f"Successfully generated {len(audio_data)} bytes of audio")

        # 4. Return the complete audio to Ultravox
        # Using Response instead of StreamingResponse for complete data
        return Response(
            content=audio_data,
            media_type="audio/mpeg",
            headers={
                "Content-Length": str(len(audio_data)),
                "Cache-Control": "no-cache"
            }
        )

    except httpx.TimeoutException:
        print("Timeout waiting for Spitch API")
        raise HTTPException(status_code=504, detail="Spitch API timeout")
    
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
