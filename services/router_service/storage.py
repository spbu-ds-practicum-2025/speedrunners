import asyncio
import logging
import aiosqlite
import os
import random
from typing import Optional

logger = logging.getLogger(__name__)


class AsyncStorage:
    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        os.makedirs(self.root_dir, exist_ok=True)
        # Внутренний замок: не даем пытаться писать в файл двум потокам одного роутера одновременно
        self._write_lock = asyncio.Lock()

    def _get_db_path(self, shard_name: str) -> str:
        return os.path.join(self.root_dir, shard_name)

    async def _init_pragmas(self, db: aiosqlite.Connection):
        """Включаем WAL и настраиваем таймауты."""
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        # Важно: если база занята, драйвер сам ждет до 10 сек
        await db.execute("PRAGMA busy_timeout=10000;")
        await db.execute("PRAGMA wal_autocheckpoint=100")

    async def create_table_if_missing(self, db: aiosqlite.Connection):
        """Создает таблицу, если её нет (Self-healing)."""
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY,
                short_code TEXT UNIQUE NOT NULL,
                original_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        )
        await db.commit()

    async def create_new_shard(self, shard_name: str):
        db_path = self._get_db_path(shard_name)
        if os.path.exists(db_path):
            return

        logger.info(f"[STORAGE] Initializing file: {shard_name}")
        async with self._write_lock:
            async with aiosqlite.connect(db_path) as db:
                await self._init_pragmas(db)
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS links (
                        id INTEGER PRIMARY KEY,
                        short_code TEXT UNIQUE NOT NULL,
                        original_url TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                )
                await db.commit()

    async def insert_link(
        self, shard_name: str, link_id: int, short_code: str, original_url: str
    ) -> None:
        db_path = self._get_db_path(shard_name)
        query = "INSERT INTO links (id, short_code, original_url) VALUES (?, ?, ?)"

        # 1. Захватываем внутренний замок (очередь внутри питона)
        async with self._write_lock:
            # 2. Пытаемся записать в файл с механизмом Retry
            retries = 100
            for attempt in range(1, retries + 1):
                try:
                    # Открываем и СРАЗУ закрываем после записи
                    async with aiosqlite.connect(db_path) as db:
                        await self._init_pragmas(db)
                        await db.execute(query, (link_id, short_code, original_url))
                        await db.commit()
                        return  # Успех

                except Exception as e:
                    # === ВОТ ОНО: ЛЕЧЕНИЕ ===
                    # if "no such table" in str(e):
                    #     logger.warning(f"[STORAGE] Table missing in {shard_name}. Fixing...")
                    #     async with aiosqlite.connect(db_path) as db:
                    #         await self.create_table_if_missing(db)
                    #     continue # Пробуем записать снова в следующем цикле

                    is_locked = "locked" in str(e) or "busy" in str(e).lower()
                    if is_locked:
                        if attempt < retries:
                            # Умная пауза
                            sleep_time = 0.1 + random.uniform(0.05, 0.5)
                            await asyncio.sleep(sleep_time)
                        else:
                            logger.error(
                                f"Failed to write to {shard_name} after retries."
                            )
                            raise e
                    else:
                        raise e

    async def get_original_url(self, shard_name: str, short_code: str) -> Optional[str]:
        db_path = self._get_db_path(shard_name)
        if not os.path.exists(db_path):
            return None

        # Для чтения замок не нужен (WAL позволяет читать параллельно)
        try:
            async with aiosqlite.connect(db_path) as db:
                await self._init_pragmas(db)
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT original_url FROM links WHERE short_code = ?", (short_code,)
                ) as cursor:
                    row = await cursor.fetchone()
                    return row["original_url"] if row else None
        except Exception:
            return None

    async def force_checkpoint(self, shard_name: str):
        """Принудительно слить WAL в БД и очистить временные файлы."""
        db_path = self._get_db_path(shard_name)
        if not os.path.exists(db_path):
            return

        try:
            # Открываем, делаем TRUNCATE checkpoint и закрываем
            async with aiosqlite.connect(db_path) as db:
                await db.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                logger.info(f"[STORAGE] Checkpoint done for {shard_name}")
        except Exception as e:
            logger.error(f"[STORAGE] Checkpoint failed: {e}")
