from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from contextlib import asynccontextmanager
from storage import AsyncStorage
from sharding import get_target_shard, should_preallocate

# Подключаем файл storage.py, который ты прислал (он отличный)
import os
DATA_PATH = os.getenv("DATA_PATH", "/app/data/shard_0.db")

storage = AsyncStorage(DATA_PATH) # Указываем полный путь к Volume

class LinkSchema(BaseModel):
    id: int           
    short_code: str
    original_url: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализируем БД при старте
    await storage.initialize_table()
    print("Storage initialized: shard_0.db ready.")
    yield

app = FastAPI(lifespan=lifespan, title="Router")

@app.post("/save_link")
async def save_link(link: LinkSchema, background_tasks: BackgroundTasks):
    try:
        # 1. Вычисляем целевой шард
        target_shard = get_target_shard(link.id)
        
        # 2. Проверяем преаллокацию (нужен ли следующий?)
        next_shard_idx = should_preallocate(link.id)
        if next_shard_idx != -1:
            next_shard_name = f"shard_{next_shard_idx}.db"
            # Добавляем задачу в фон: создать файл, если его нет
            background_tasks.add_task(storage.create_shard_if_not_exists, next_shard_name)
        # Пишем в базу
        await storage.insert_link(link.id, link.short_code, link.original_url)
        
        return {"status": "ok", "shard": target_shard}
        print(f"[ROUTER] Saved {link.short_code}")
        return {"status": "ok", "message": "Link saved"}
    
    except Exception as e:
        print(f"[ROUTER] Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_link")
async def get_link(short_code: str):
    # Читаем из базы
    url = await storage.get_original_url(short_code)
    if url is None:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"original_url": url}