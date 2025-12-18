import sys
import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock, mock_open
import services.api_service.main as api_main
import services.router_service.sharding as router_sharding
from services.router_service.storage import AsyncStorage

# --- НАСТРОЙКА ПУТЕЙ ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(os.path.join(root_dir, "services", "api_service"))
sys.path.append(os.path.join(root_dir, "services", "router_service"))
sys.path.append(os.path.join(root_dir, "services", "id_service"))

with patch("services.router_service.storage.AsyncStorage"):
    import services.router_service.main as router_main

# Патчим открытие файлов для ID Service, чтобы не создавать мусор на диске
with patch("builtins.open", new_callable=mock_open, read_data="100"):
    import services.id_service.main as id_main
    import services.id_service.state_manager as id_state


# --- ИМПОРТЫ ---


# Патчим сторедж для роутера


# ==========================================
# 1. ТЕСТЫ API SERVICE
# ==========================================


@pytest.mark.asyncio
async def test_api_shorten_endpoint():
    api_main.id_buffer.available_ids = [123]
    mock_response = MagicMock()
    mock_response.status_code = 200

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response
        req = api_main.ShortenRequest(url="http://google.com")
        result = await api_main.shorten(req)
        assert result["short_code"] == api_main.encode_base62(123)


@pytest.mark.asyncio
async def test_api_redirect_endpoint():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"original_url": "http://found.com"}

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        response = await api_main.redirect_to_url("abc")
        assert response.status_code == 307


@pytest.mark.asyncio
async def test_id_buffer():
    buffer = api_main.IDBuffer()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"start": 10, "end": 12}

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        new_id = await buffer.get_one_id()
        assert new_id == 10


def test_base62():
    assert api_main.encode_base62(62) == "10"


# ==========================================
# 2. ТЕСТЫ ROUTER SERVICE
# ==========================================


def test_sharding_logic():
    router_sharding.SHARD_LIMIT = 1000
    assert router_sharding.get_target_shard(1) == "shard_0.db"
    assert router_sharding.should_preallocate(900) == 1


@pytest.mark.asyncio
async def test_router_save_link():
    router_main.storage.insert_link = AsyncMock()
    router_main.storage.create_new_shard = AsyncMock()

    req = router_main.LinkSchema(id=100, short_code="1C", original_url="http://ya.ru")
    bg_tasks = MagicMock()
    resp = await router_main.save_link(req, bg_tasks)
    assert resp["status"] == "ok"


@pytest.mark.asyncio
async def test_router_get_link_found():
    router_main.storage.get_original_url = AsyncMock(return_value="http://ya.ru")
    resp = await router_main.get_link("1C")
    assert resp["original_url"] == "http://ya.ru"


# ==========================================
# 3. ТЕСТЫ ID SERVICE (НОВОЕ!)
# ==========================================


def test_state_manager_read():
    """Проверяем чтение состояния из файла"""
    # Мокаем open, чтобы он вернул "500"
    with patch("builtins.open", mock_open(read_data="500")):
        manager = id_state.StateManager()
        assert manager.get_current_max() == 500


def test_state_manager_write():
    """Проверяем запись состояния"""
    m = mock_open()
    with patch("builtins.open", m):
        manager = id_state.StateManager()
        manager.update_max(600)
        # Проверяем, что в файл записалось "600"
        m().write.assert_called_with("600")


@pytest.mark.asyncio
async def test_id_generator_allocate():
    """Проверяем эндпоинт /allocate"""
    # Мокаем методы стейт менеджера внутри id_main
    id_main.state.get_current_max = MagicMock(return_value=1000)
    id_main.state.update_max = MagicMock()

    req = id_main.AllocateRequest(size=100)

    # Вызываем ручку напрямую
    resp = await id_main.allocate_range(req)

    assert resp["start"] == 1001
    assert resp["end"] == 1100
    # Проверяем, что новое состояние (1100) сохранилось
    id_main.state.update_max.assert_called_with(1100)


# ==========================================
# 4. ТЕСТЫ STORAGE (САМОЕ СЛОЖНОЕ)
# ==========================================


# Создаем универсальный Мок, который работает и как await, и как async with
class MockCursor:
    def __init__(self, fetch_result=None):
        self.fetch_result = fetch_result
        # Метод fetchone - асинхронный
        self.fetchone = AsyncMock(return_value=fetch_result)

    # Позволяет делать await db.execute(...)
    def __await__(self):
        async def _ret():
            return self

        return _ret().__await__()

    # Позволяет делать async with db.execute(...)
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


@pytest.mark.asyncio
async def test_storage_get_db_path():
    """Простой тест путей"""
    storage = AsyncStorage("test_data")
    assert storage._get_db_path("shard_0.db") == "test_data/shard_0.db"


@pytest.mark.asyncio
async def test_storage_insert_retry_logic():
    """Проверяем, что insert пытается писать и вызывает aiosqlite"""
    storage = AsyncStorage("test_data")

    # Используем наш умный курсор
    mock_cursor = MockCursor()
    mock_db = AsyncMock()
    # execute возвращает наш курсор
    mock_db.execute = MagicMock(return_value=mock_cursor)

    mock_conn = AsyncMock()
    mock_conn.__aenter__.return_value = mock_db

    with patch("aiosqlite.connect", return_value=mock_conn) as mock_connect:
        await storage.insert_link("shard_0.db", 1, "abc", "http://url")

        mock_connect.assert_called_with("test_data/shard_0.db")
        assert mock_db.execute.call_count >= 1


@pytest.mark.asyncio
async def test_storage_get_url_found():
    """Проверяем чтение, если запись есть"""
    storage = AsyncStorage("test_data")

    # Курсор вернет данные (имитация aiosqlite.Row через dict)
    mock_cursor = MockCursor(fetch_result={"original_url": "http://found.com"})

    mock_db = AsyncMock()
    mock_db.execute = MagicMock(return_value=mock_cursor)

    mock_conn = AsyncMock()
    mock_conn.__aenter__.return_value = mock_db

    with patch("os.path.exists", return_value=True):
        with patch("aiosqlite.connect", return_value=mock_conn):
            url = await storage.get_original_url("shard_0.db", "abc")
            # Теперь ошибки не будет, и вернется URL
            assert url == "http://found.com"


@pytest.mark.asyncio
async def test_storage_get_url_not_found():
    """Проверяем чтение, если записи нет"""
    storage = AsyncStorage("test_data")

    # Курсор вернет None
    mock_cursor = MockCursor(fetch_result=None)

    mock_db = AsyncMock()
    mock_db.execute = MagicMock(return_value=mock_cursor)

    mock_conn = AsyncMock()
    mock_conn.__aenter__.return_value = mock_db

    with patch("os.path.exists", return_value=True):
        with patch("aiosqlite.connect", return_value=mock_conn):
            url = await storage.get_original_url("shard_0.db", "abc")
            assert url is None


@pytest.mark.asyncio
async def test_storage_create_new_shard():
    """Проверяем создание новой таблицы"""
    storage = AsyncStorage("test_data")

    mock_cursor = MockCursor()
    mock_db = AsyncMock()
    mock_db.execute = MagicMock(return_value=mock_cursor)

    mock_conn = AsyncMock()
    mock_conn.__aenter__.return_value = mock_db

    with patch("os.path.exists", return_value=False):
        with patch("aiosqlite.connect", return_value=mock_conn):
            await storage.create_new_shard("shard_1.db")
            assert mock_db.execute.call_count >= 1
