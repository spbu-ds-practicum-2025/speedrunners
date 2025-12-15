import asyncio
import logging
import aiosqlite
import os
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

class AsyncStorage:
    def __init__(self, root_dir: str):
        """
        Инициализация менеджера хранилища.
        :param root_dir: Путь к ПАПКЕ, где лежат все шарды (например, '/app/data')
        """
        self.root_dir = root_dir
        # Создаем директорию для данных, если её нет
        os.makedirs(self.root_dir, exist_ok=True)

    def _get_db_path(self, shard_name: str) -> str:
        """Собирает полный путь к файлу шарда."""
        return os.path.join(self.root_dir, shard_name)

    async def _init_pragmas(self, db: aiosqlite.Connection):
        """Оптимизации SQLite для работы с уже открытым соединением."""
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA cache_size=-64000;")
        await db.execute("PRAGMA busy_timeout=5000;")

    async def create_new_shard(self, shard_name: str):
        """
        Физически создает новый файл шарда, включает WAL и создает таблицу.
        """
        db_path = self._get_db_path(shard_name)
        
        # Логируем (полезно для отладки преаллокации)
        if os.path.exists(db_path):
            logger.info(f"[STORAGE] Shard {shard_name} already exists. Checking schema...")
        else:
            logger.info(f"[STORAGE] Creating NEW shard: {shard_name}")

        async with aiosqlite.connect(db_path) as db:
            # 1. Сразу включаем WAL (как в требовании)
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            
            # 2. Накатываем CREATE TABLE
            await db.execute("""
                CREATE TABLE IF NOT EXISTS links (
                    id INTEGER PRIMARY KEY,
                    short_code TEXT UNIQUE NOT NULL,
                    original_url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            await db.commit()

    async def execute_write(self, shard_name: str, query: str, params: tuple = ()) -> None:
        """Запись данных в конкретный шард с ретраями."""
        db_path = self._get_db_path(shard_name)
        retries = 5
        delay = 0.05

        for attempt in range(1, retries + 1):
            try:
                async with aiosqlite.connect(db_path) as db:
                    await self._init_pragmas(db)
                    await db.execute(query, params)
                    await db.commit()
                    return
            except aiosqlite.OperationalError as e:
                if "database is locked" in str(e) or "busy" in str(e).lower():
                    if attempt < retries:
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Failed to write to {shard_name} after {retries} attempts.")
                        raise e
                else:
                    raise e

    async def fetch_one(self, shard_name: str, query: str, params: tuple = ()) -> Optional[Any]:
        """Чтение одной строки из конкретного шарда."""
        db_path = self._get_db_path(shard_name)
        
        if not os.path.exists(db_path):
            return None

        async with aiosqlite.connect(db_path) as db:
            await self._init_pragmas(db)
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def insert_link(self, shard_name: str, link_id: int, short_code: str, original_url: str) -> None:
        """Вставка ссылки в указанный файл."""
        query = "INSERT INTO links (id, short_code, original_url) VALUES (?, ?, ?)"
        await self.execute_write(shard_name, query, (link_id, short_code, original_url))

    async def get_original_url(self, shard_name: str, short_code: str) -> Optional[str]:
        """Поиск ссылки в конкретном шарде."""
        query = "SELECT original_url FROM links WHERE short_code = ?"
        row = await self.fetch_one(shard_name, query, (short_code,))
        if row:
            return row['original_url']
        return None