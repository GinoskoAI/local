import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from spitch import AsyncSpitch

app = FastAPI()

# Get the key from Vercel environment variables
API_KEY = os.getenv("SPITCH_API_KEY")

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        
        # Pull voice and language from URL parameters
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        # 1. Define a generator to keep the connection open
        async def audio_generator():
            client = AsyncSpitch(api_key=API_KEY)
            # The 'async with' must stay open WHILE we yield chunks
            async with client.speech.with_streaming_response.generate(
                language=lang_code, 
                voice=voice_id, 
                text=text
            ) as response:
                async for chunk in response.iter_bytes():
                    yield chunk

        # 2. Return the generator to Ultravox
        return StreamingResponse(
            audio_generator(), 
            media_type="audio/mpeg" # Spitch default is mp3
        )

    except Exception as e:
        print(f"Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
