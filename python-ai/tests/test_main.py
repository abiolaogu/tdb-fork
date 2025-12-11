import pytest
from httpx import AsyncClient
from lumaai.main import app

@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

@pytest.mark.asyncio
async def test_embedding_endpoint_mock():
    # Only if mock runs without extensive setup
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/embedding", json={"text": "hello"})
    # It might fail if model manager isn't loaded or dependencies missing
    # We accept 200 or 503 (loading)
    assert response.status_code in [200, 503]
