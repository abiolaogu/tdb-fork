"""
LumaDB AI Service

Provides AI-powered features for the LumaDB database:
- Vector similarity search (semantic search)
- Natural language to query translation
- Embeddings generation
- AI-powered query optimization
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from .vector import VectorIndex
from .nlp import NLPProcessor
from .inference import ModelManager


# Global instances
vector_index: Optional[VectorIndex] = None
nlp_processor: Optional[NLPProcessor] = None
model_manager: Optional[ModelManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources."""
    global vector_index, nlp_processor, model_manager

    print("Initializing LumaDB AI Service...")

    # Initialize components
    model_manager = ModelManager()
    await model_manager.load_models()

    vector_index = VectorIndex(model_manager)
    nlp_processor = NLPProcessor(model_manager)

    print("LumaDB AI Service ready!")

    yield

    # Cleanup
    print("Shutting down LumaDB AI Service...")


app = FastAPI(
    title="LumaDB AI Service",
    description="AI-powered features for LumaDB database",
    version="1.0.0",
    lifespan=lifespan,
)

# ... (middleware setup)

# ... (models)

# ... (endpoints)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "luma-ai",
        "version": "1.0.0",
        "models_loaded": model_manager.is_ready() if model_manager else False,
    }

class GenerateRequest(BaseModel):
    prompt: str
    context: Optional[str] = None
    max_tokens: int = 100

@app.post("/generate")
async def generate_text(request: GenerateRequest):
    """Generate text using the loaded LLM."""
    if not model_manager or not model_manager.is_ready():
        raise HTTPException(status_code=503, detail="Models not loaded")
    
    # Construct prompt with context if provided
    final_prompt = request.prompt
    if request.context:
        final_prompt = f"Context:\n{request.context}\n\nQuestion: {request.prompt}\nAnswer:"
        
    try:
        # Check if LLM is available in model_manager (added in previous step)
        if not hasattr(model_manager, 'llm') or not model_manager.llm:
             raise HTTPException(status_code=500, detail="LLM not configured")

        response = await model_manager.llm.generate(final_prompt, max_tokens=request.max_tokens)
        return {"response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class EmbeddingRequest(BaseModel):
    text: str

@app.post("/embedding")
async def get_embedding(request: EmbeddingRequest):
    """Get vector embedding for text."""
    if not model_manager:
        raise HTTPException(status_code=503, detail="Models not loaded")
    
    try:
        embedding = await model_manager.get_embedding(request.text)
        return {"embedding": embedding}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Run the AI service."""
    host = os.environ.get("LUMA_AI_HOST", "0.0.0.0")
    port = int(os.environ.get("LUMA_AI_PORT", "8000"))

    print(f"Starting LumaDB AI Service on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
