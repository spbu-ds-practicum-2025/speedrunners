import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

import pytest
import respx
import pytest_asyncio
import httpx
from httpx import Response
from fastapi.testclient import TestClient

try:
    from main import app, id_buffer, ID_GEN_URL, ROUTER_URL
except ImportError as e:
    print(f"ERROR: Failed to import from main.py. Check your project structure.")
    print(f"Attempted path: {parent_dir}")
    raise e

@pytest.fixture(autouse=True)
def reset_buffer():
    """Очищает глобальный буфер ID перед каждым тестом."""
    id_buffer.available_ids = []
    yield
    id_buffer.available_ids = []


@pytest.fixture
def client():
    """Синхронный TestClient для тестирования FastAPI приложения"""
    with TestClient(app=app) as client:
        yield client

# ----------------------------------------------------------------------
#                           ТЕСТЫ /shorten
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shorten_success(client):
    id_buffer.available_ids = []  # Гарантируем чистоту
    async with respx.mock:
        respx.post(ID_GEN_URL).mock(
            return_value=Response(200, json={"start": 100, "end": 100})
        )
        save_route = respx.post(f"{ROUTER_URL}/save_link").mock(return_value=Response(200))

        response = client.post("/shorten", json={"url": "https://google.com"})

        assert response.status_code == 200
        data = response.json()

        # ИСПРАВЛЕНО: ID 100 кодируется как '1c'
        assert data["short_code"] == "1c"
        assert data["short_url"] == "http://localhost:8080/1c"


@pytest.mark.asyncio
async def test_shorten_id_gen_failure(client):
    """
    Тест: ID Generator недоступен (500 ошибка).
    Ожидаем: 500 от нашего API.
    """
    async with respx.mock:
        # Мокаем ошибку от ID Generator
        respx.post(ID_GEN_URL).mock(return_value=Response(500))

        # ИСПРАВЛЕНО: Убран await
        response = client.post("/shorten", json={"url": "https://test.com"})

        assert response.status_code == 500
        assert response.json()["detail"] == "ID Generator unavailable"


@pytest.mark.asyncio
async def test_shorten_router_retry_success(client):
    id_buffer.available_ids = [] # Гарантируем чистоту
    async with respx.mock:
        respx.post(ID_GEN_URL).mock(
            return_value=Response(200, json={"start": 500, "end": 500})
        )

        router_route = respx.post(f"{ROUTER_URL}/save_link").mock(
            side_effect=[Response(503), Response(200)]
        )

        response = client.post("/shorten", json={"url": "https://test.com/retry"})

        assert response.status_code == 200
        # ИСПРАВЛЕНО: ID 500 кодируется как '84'
        assert response.json()["short_code"] == "84"

@pytest.mark.asyncio
async def test_shorten_router_retry_failure(client):
    """
    Тест: Router всегда возвращает 503, все попытки исчерпаны.
    Ожидаем: 503 от нашего API после 3 попыток.
    """
    async with respx.mock:
        # ID Gen работает, гарантируя ID 200
        respx.post(ID_GEN_URL).mock(
            return_value=Response(200, json={"start": 200, "end": 200})
        )

        # Router всегда возвращает 503
        router_route = respx.post(f"{ROUTER_URL}/save_link").mock(
            return_value=Response(503)
        )

        # ИСПРАВЛЕНО: Убран await
        response = client.post("/shorten", json={"url": "https://test.com"})

        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]

        # Проверяем, что попыток было 3
        assert router_route.call_count == 3


# ----------------------------------------------------------------------
#                          ТЕСТЫ /{short_code}
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redirect_success(client):
    """
    Тест: Успешное перенаправление по коду.
    Ожидаем: 307 Redirect и заголовок Location.
    """
    async with respx.mock:
        short_code = "ABC"
        original = "https://example.com/long/path"

        # Мокаем ответ от Router
        respx.get(f"{ROUTER_URL}/get_link").mock(
            return_value=Response(200, json={"original_url": original})
        )

        # ИСПРАВЛЕНО: Убран await
        response = client.get(f"/{short_code}", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == original


@pytest.mark.asyncio
async def test_redirect_not_found(client):
    """
    Тест: Ссылка не найдена в Router (404).
    Ожидаем: 404 от нашего API.
    """
    async with respx.mock:
        short_code = "UNKNOWN"

        # Router возвращает 404
        respx.get(f"{ROUTER_URL}/get_link").mock(
            return_value=Response(404)
        )

        # ИСПРАВЛЕНО: Убран await
        response = client.get(f"/{short_code}")

        assert response.status_code == 404
        assert response.json()["detail"] == "Link not found"


@pytest.mark.asyncio
async def test_redirect_service_down(client):
    """
    Тест: Router лежит (сетевая ошибка).
    Ожидаем: 503 от нашего API.
    """
    async with respx.mock:
        # Имитируем исключение соединения/таймаута
        respx.get(f"{ROUTER_URL}/get_link").mock(side_effect=httpx.RequestError("Down"))

        # ИСПРАВЛЕНО: Убран await
        response = client.get("/abc")

        assert response.status_code == 503
        assert "Service temporarily unavailable" in response.json()["detail"]