import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from spitch import AsyncSpitch

app = FastAPI()

# Move the client initialization here or inside the function
API_KEY = os.getenv("SPITCH_API_KEY")

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        voice_id = request.query_params.get("voice", "sade")
        lang_code = request.query_params.get("lang", "yo")

        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        # FIX: Define a generator that keeps the connection alive
        async def audio_generator():
            client = AsyncSpitch(api_key=API_KEY)
            # Opening the stream INSIDE the generator ensures it stays open
            async with client.speech.with_streaming_response.generate(
                language=lang_code, 
                voice=voice_id, 
                text=text
            ) as response:
                async for chunk in response.iter_bytes():
                    yield chunk

        return StreamingResponse(audio_generator(), media_type="audio/mpeg")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
