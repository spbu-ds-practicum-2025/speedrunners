import asyncio
import logging
import aiosqlite
from typing import Any, List, Optional

# Настраиваем логгер
logger = logging.getLogger(__name__)

class AsyncStorage:
    def __init__(self, db_path: str):
        """
        Инициализация подключения к конкретному файлу шарда.
        :param db_path: Путь к файлу базы данных (например, 'data/shard_0.db')
        """
        self.db_path = db_path

    async def _init_pragmas(self, db: aiosqlite.Connection):
        """
        Применение оптимизаций SQLite для высокой производительности и конкурентности.
        """
        # WAL режим
        await db.execute("PRAGMA journal_mode=WAL;")
        # Баланс между скоростью и надежностью
        await db.execute("PRAGMA synchronous=NORMAL;")
        # Увеличенный кэш
        await db.execute("PRAGMA cache_size=-64000;")
        # Таймаут на уровне драйвера
        await db.execute("PRAGMA busy_timeout=5000;")

    async def execute_write(self, query: str, params: tuple = ()) -> None:
        """
        Выполнение записи с механизмом повторных попыток при блокировке.
        """
        retries = 5
        delay = 0.05  # 50 мс

        for attempt in range(1, retries + 1):
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    await self._init_pragmas(db)
                    await db.execute(query, params)
                    await db.commit()
                    return  # Успех

            except aiosqlite.OperationalError as e:
                # Если база занята (LOCKED), ждем и пробуем снова
                if "database is locked" in str(e) or "busy" in str(e).lower():
                    if attempt < retries:
                        logger.warning(
                            f"Database {self.db_path} locked. Retry {attempt}/{retries}..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Failed to write to {self.db_path} after retries.")
                        raise e
                else:
                    raise e

    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[Any]:
        """Чтение одной строки."""
        async with aiosqlite.connect(self.db_path) as db:
            await self._init_pragmas(db)
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def fetch_all(self, query: str, params: tuple = ()) -> List[Any]:
        """Чтение всех строк."""
        async with aiosqlite.connect(self.db_path) as db:
            await self._init_pragmas(db)
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def initialize_table(self):
        """
        Создает таблицу links.
        """
        query = """
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY,           -- Тот самый ID от генератора
            short_code TEXT UNIQUE NOT NULL,  -- Уникальный код (abc1)
            original_url TEXT NOT NULL,       -- Длинная ссылка
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Дата создания
        );
        """
        await self.execute_write(query)

    async def insert_link(self, link_id: int, short_code: str, original_url: str) -> None:
        """
        Вставка новой записи с указанием ID.
        """
        query = "INSERT INTO links (id, short_code, original_url) VALUES (?, ?, ?)"
        # Передаем 3 параметра, created_at заполнится сам
        await self.execute_write(query, (link_id, short_code, original_url))

    async def get_original_url(self, short_code: str) -> Optional[str]:
        """
        Возвращает оригинальный URL по короткому коду или None.
        """
        query = "SELECT original_url FROM links WHERE short_code = ?"
        row = await self.fetch_one(query, (short_code,))
        
        if row:
            # row['original_url'] работает благодаря db.row_factory = aiosqlite.Row
            return row['original_url']
        return None