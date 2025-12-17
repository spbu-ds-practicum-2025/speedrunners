import os
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from contextlib import asynccontextmanager

from storage import AsyncStorage
from sharding import get_target_shard, should_preallocate
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
# ТЕПЕРЬ мы работаем с ПАПКОЙ, а не одним файлом
# Если запускаем локально, данные будут в папке 'data'
DATA_DIR = os.getenv("DATA_DIR", "data")

# Инициализируем менеджер хранилища (указываем папку)
storage = AsyncStorage(DATA_DIR)

class LinkSchema(BaseModel):
    id: int
    short_code: str
    original_url: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте создаем первый шард (shard_0.db) физически
    await storage.create_new_shard("shard_0.db")
    print(f"Storage initialized. Root dir: {DATA_DIR}")
    yield

app = FastAPI(lifespan=lifespan, title="Router Service")

@app.post("/save_link")
async def save_link(link: LinkSchema, background_tasks: BackgroundTasks):
    try:
        # 1. Вычисляем имя целевого файла (например, "shard_0.db")
        target_shard_name = get_target_shard(link.id)
        
        # 2. Проверяем преаллокацию (нужен ли следующий?)
        next_shard_idx = should_preallocate(link.id)
        if next_shard_idx != -1:
            next_shard_name = f"shard_{next_shard_idx}.db"
            # Задача в фон: создать новый файл через storage
            background_tasks.add_task(storage.create_new_shard, next_shard_name)
        
        # 3. Пишем в целевой шард!
        # ТЕПЕРЬ мы передаем имя файла первым аргументом
        await storage.insert_link(target_shard_name, link.id, link.short_code, link.original_url)
        
        return {"status": "ok", "shard": target_shard_name}
    
    except Exception as e:
        print(f"[ROUTER] Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/get_link")
async def get_link(short_code: str):
    """
    Поиск ссылки.
    MVP: Ищем только в shard_0.db (так как пока нет Scatter/Gather).
    """
    # Ищем в нулевом шарде
    url = await storage.get_original_url("shard_0.db", short_code)
    
    if url is None:
        raise HTTPException(status_code=404, detail="Link not found in shard_0")
    
    return {"original_url": url}