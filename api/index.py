import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from spitch import AsyncSpitch

app = FastAPI()

# Initialize Spitch Client
# We use os.getenv to keep your key safe in Vercel settings
client = AsyncSpitch(api_key=os.getenv("SPITCH_API_KEY"))

@app.post("/v1/tts")
async def generate_speech(request: Request):
    try:
        data = await request.json()
        text = data.get("text")
        
        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        # ---------------------------------------------------------
        # SETTINGS FOR YORUBA
        # Voice options: "sade" (Female) or "femi" (Male)
        # ---------------------------------------------------------
        async with client.speech.with_streaming_response.generate(
            language="yo", 
            voice="sade", 
            text=text
        ) as response:
            return StreamingResponse(
                response.iter_bytes(), 
                media_type="audio/mpeg"
            )

    except Exception as e:
        print(f"Error: {str(e)}")
        # Ultravox needs to know if something broke
        raise HTTPException(status_code=500, detail=str(e))
