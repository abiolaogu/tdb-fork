"""
TDB+ AI Service

Provides AI-powered features for the TDB+ database:
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

    print("Initializing TDB+ AI Service...")

    # Initialize components
    model_manager = ModelManager()
    await model_manager.load_models()

    vector_index = VectorIndex(model_manager)
    nlp_processor = NLPProcessor(model_manager)

    print("TDB+ AI Service ready!")

    yield

    # Cleanup
    print("Shutting down TDB+ AI Service...")


app = FastAPI(
    title="TDB+ AI Service",
    description="AI-powered features for TDB+ database",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Request/Response Models
# ============================================================================

class EmbeddingRequest(BaseModel):
    texts: list[str]
    model: str = "all-MiniLM-L6-v2"


class EmbeddingResponse(BaseModel):
    embeddings: list[list[float]]
    model: str
    dimensions: int


class VectorSearchRequest(BaseModel):
    collection: str
    query: str
    top_k: int = 10
    filter: Optional[dict] = None


class VectorSearchResponse(BaseModel):
    results: list[dict]
    query_embedding: list[float]
    search_time_ms: float


class IndexDocumentRequest(BaseModel):
    collection: str
    document_id: str
    text: str
    metadata: Optional[dict] = None


class NLQueryRequest(BaseModel):
    query: str
    context: Optional[str] = None
    target_language: str = "tql"


class NLQueryResponse(BaseModel):
    original_query: str
    translated_query: str
    target_language: str
    confidence: float
    explanation: Optional[str] = None


class SemanticAnalysisRequest(BaseModel):
    text: str
    analyze_entities: bool = True
    analyze_sentiment: bool = True
    extract_keywords: bool = True


class SemanticAnalysisResponse(BaseModel):
    entities: Optional[list[dict]] = None
    sentiment: Optional[dict] = None
    keywords: Optional[list[str]] = None


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "tdb-ai",
        "version": "1.0.0",
        "models_loaded": model_manager.is_ready() if model_manager else False,
    }


@app.post("/embeddings", response_model=EmbeddingResponse)
async def generate_embeddings(request: EmbeddingRequest):
    """Generate embeddings for text inputs."""
    if not model_manager:
        raise HTTPException(status_code=503, detail="Model manager not initialized")

    embeddings = await model_manager.get_embeddings(request.texts, request.model)

    return EmbeddingResponse(
        embeddings=embeddings,
        model=request.model,
        dimensions=len(embeddings[0]) if embeddings else 0,
    )


@app.post("/vector/index")
async def index_document(request: IndexDocumentRequest):
    """Index a document for vector search."""
    if not vector_index:
        raise HTTPException(status_code=503, detail="Vector index not initialized")

    await vector_index.index_document(
        collection=request.collection,
        document_id=request.document_id,
        text=request.text,
        metadata=request.metadata,
    )

    return {"status": "indexed", "document_id": request.document_id}


@app.post("/vector/search", response_model=VectorSearchResponse)
async def vector_search(request: VectorSearchRequest):
    """Perform vector similarity search."""
    if not vector_index:
        raise HTTPException(status_code=503, detail="Vector index not initialized")

    import time
    start = time.time()

    results, query_embedding = await vector_index.search(
        collection=request.collection,
        query=request.query,
        top_k=request.top_k,
        filter=request.filter,
    )

    search_time = (time.time() - start) * 1000

    return VectorSearchResponse(
        results=results,
        query_embedding=query_embedding,
        search_time_ms=search_time,
    )


@app.delete("/vector/index/{collection}")
async def delete_collection_index(collection: str):
    """Delete a collection's vector index."""
    if not vector_index:
        raise HTTPException(status_code=503, detail="Vector index not initialized")

    await vector_index.delete_collection(collection)
    return {"status": "deleted", "collection": collection}


@app.post("/nlp/translate", response_model=NLQueryResponse)
async def translate_natural_query(request: NLQueryRequest):
    """Translate natural language to database query."""
    if not nlp_processor:
        raise HTTPException(status_code=503, detail="NLP processor not initialized")

    result = await nlp_processor.translate_query(
        query=request.query,
        context=request.context,
        target_language=request.target_language,
    )

    return NLQueryResponse(
        original_query=request.query,
        translated_query=result["query"],
        target_language=request.target_language,
        confidence=result["confidence"],
        explanation=result.get("explanation"),
    )


@app.post("/nlp/analyze", response_model=SemanticAnalysisResponse)
async def analyze_text(request: SemanticAnalysisRequest):
    """Perform semantic analysis on text."""
    if not nlp_processor:
        raise HTTPException(status_code=503, detail="NLP processor not initialized")

    result = await nlp_processor.analyze(
        text=request.text,
        analyze_entities=request.analyze_entities,
        analyze_sentiment=request.analyze_sentiment,
        extract_keywords=request.extract_keywords,
    )

    return SemanticAnalysisResponse(**result)


@app.post("/nlp/suggest")
async def suggest_query_completion(partial_query: str, collection: Optional[str] = None):
    """Suggest query completions."""
    if not nlp_processor:
        raise HTTPException(status_code=503, detail="NLP processor not initialized")

    suggestions = await nlp_processor.suggest_completions(
        partial_query=partial_query,
        collection=collection,
    )

    return {"suggestions": suggestions}


@app.get("/models")
async def list_models():
    """List available AI models."""
    if not model_manager:
        raise HTTPException(status_code=503, detail="Model manager not initialized")

    return {"models": model_manager.list_models()}


@app.get("/metrics")
async def get_metrics():
    """Get service metrics."""
    return {
        "embeddings_generated": 0,
        "vector_searches": 0,
        "nl_translations": 0,
        "cache_hit_rate": 0.0,
    }


def main():
    """Run the AI service."""
    host = os.environ.get("TDB_AI_HOST", "0.0.0.0")
    port = int(os.environ.get("TDB_AI_PORT", "8000"))

    print(f"Starting TDB+ AI Service on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
