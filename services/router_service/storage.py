import asyncio
import logging
import aiosqlite
import os
import random  # <--- Добавили для случайных пауз
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class AsyncStorage:
    def __init__(self, root_dir: str):
        """
        Инициализация менеджера хранилища.
        """
        self.root_dir = root_dir
        os.makedirs(self.root_dir, exist_ok=True)

    def _get_db_path(self, shard_name: str) -> str:
        return os.path.join(self.root_dir, shard_name)

    async def _init_pragmas(self, db: aiosqlite.Connection):
        """Оптимизации SQLite."""
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA cache_size=-64000;")
        # Увеличиваем таймаут драйвера до 10 секунд
        await db.execute("PRAGMA busy_timeout=10000;") 

    async def create_new_shard(self, shard_name: str):
        """Создание нового файла."""
        db_path = self._get_db_path(shard_name)
        
        if os.path.exists(db_path):
            logger.info(f"[STORAGE] Shard {shard_name} exists.")
        else:
            logger.info(f"[STORAGE] Creating NEW shard: {shard_name}")

        async with aiosqlite.connect(db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
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
        """
        Запись с усиленной защитой от блокировок (High Concurrency).
        """
        db_path = self._get_db_path(shard_name)
        
        # НАСТРОЙКИ ДЛЯ СТРЕСС-ТЕСТА (50 потоков)
        retries = 30           # Пытаемся 30 раз (раньше было 5)
        base_delay = 0.1       # Минимальная пауза

        for attempt in range(1, retries + 1):
            try:
                async with aiosqlite.connect(db_path) as db:
                    await self._init_pragmas(db)
                    await db.execute(query, params)
                    await db.commit()
                    return  # Успех!

            except aiosqlite.OperationalError as e:
                is_locked = "database is locked" in str(e) or "busy" in str(e).lower()
                
                if is_locked:
                    if attempt < retries:
                        # JITTER: Случайная пауза, чтобы потоки не бились синхронно
                        sleep_time = base_delay + random.uniform(0.05, 0.25)
                        
                        logger.warning(
                            f"LOCKED {shard_name}. Try {attempt}/{retries}. Sleep {sleep_time:.2f}s"
                        )
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.error(f"GAVE UP writing to {shard_name} after {retries} attempts.")
                        raise e # Всё, сдаемся
                else:
                    raise e # Ошибка SQL (синтаксис и т.д.)

    async def fetch_one(self, shard_name: str, query: str, params: tuple = ()) -> Optional[Any]:
        """Чтение одной строки."""
        db_path = self._get_db_path(shard_name)
        if not os.path.exists(db_path):
            return None

        async with aiosqlite.connect(db_path) as db:
            await self._init_pragmas(db)
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def insert_link(self, shard_name: str, link_id: int, short_code: str, original_url: str) -> None:
        query = "INSERT INTO links (id, short_code, original_url) VALUES (?, ?, ?)"
        await self.execute_write(shard_name, query, (link_id, short_code, original_url))

    async def get_original_url(self, shard_name: str, short_code: str) -> Optional[str]:
        query = "SELECT original_url FROM links WHERE short_code = ?"
        row = await self.fetch_one(shard_name, query, (short_code,))
        if row:
            return row['original_url']
        return None