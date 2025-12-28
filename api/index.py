import os
import httpx
import asyncio
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from typing import AsyncIterator

app = FastAPI()
API_KEY = os.getenv("SPITCH_API_KEY")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "service": "spitch-ultravox-bridge"}

@app.post("/v1/tts")
async def generate_speech(request: Request):
    """
    Generate speech with proper streaming to prevent Ultravox timeout.
    Uses chunk-based streaming with immediate response headers.
    """
    try:
        # Parse request
        data = await request.json()
        text = data.get("text")
        
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        print(f"[REQUEST] Lang: {lang_code}, Voice: {voice_id}, Text: '{text[:50]}...'")

        # Spitch API configuration
        spitch_url = "https://api.spi-tch.com/v1/speech"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "Accept": "audio/mpeg"
        }
        payload = {
            "language": lang_code,
            "voice": voice_id,
            "text": text
        }

        async def audio_stream() -> AsyncIterator[bytes]:
            """Stream audio with proper error handling"""
            try:
                # Create client with longer timeout
                async with httpx.AsyncClient(timeout=90.0) as client:
                    print("[SPITCH] Sending request to Spitch API...")
                    
                    # Make request and get response
                    response = await client.post(
                        spitch_url, 
                        json=payload, 
                        headers=headers
                    )
                    
                    if response.status_code != 200:
                        error_msg = response.text[:200]
                        print(f"[ERROR] Spitch returned {response.status_code}: {error_msg}")
                        raise HTTPException(
                            status_code=response.status_code,
                            detail=f"Spitch API error: {error_msg}"
                        )
                    
                    # Get complete audio content
                    audio_content = response.content
                    content_length = len(audio_content)
                    
                    print(f"[SUCCESS] Received {content_length} bytes from Spitch")
                    
                    if content_length < 100:
                        print(f"[WARNING] Audio suspiciously small: {content_length} bytes")
                        raise HTTPException(
                            status_code=500,
                            detail="Generated audio file is too small"
                        )
                    
                    # Stream the audio in chunks (important for Ultravox compatibility)
                    chunk_size = 8192  # 8KB chunks
                    for i in range(0, content_length, chunk_size):
                        chunk = audio_content[i:i + chunk_size]
                        yield chunk
                        # Small delay to simulate real-time streaming
                        await asyncio.sleep(0.01)
                    
                    print("[COMPLETE] Audio streaming finished")
                    
            except httpx.TimeoutException:
                print("[ERROR] Timeout waiting for Spitch")
                raise HTTPException(status_code=504, detail="Spitch API timeout")
            except httpx.RequestError as e:
                print(f"[ERROR] Network error: {str(e)}")
                raise HTTPException(status_code=502, detail=f"Network error: {str(e)}")
            except Exception as e:
                print(f"[ERROR] Unexpected error: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        # Return streaming response with proper headers
        return StreamingResponse(
            audio_stream(),
            media_type="audio/mpeg",
            headers={
                "Cache-Control": "no-cache",
                "X-Content-Type-Options": "nosniff",
                "Accept-Ranges": "bytes"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"[FATAL] System error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
