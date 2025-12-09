from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from storage import AsyncStorage

# Подключаем файл storage.py, который ты прислал (он отличный)
storage = AsyncStorage("/app/data/shard_0.db") # Указываем полный путь к Volume

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
async def save_link(link: LinkSchema):
    try:
        # Пишем в базу
        await storage.insert_link(link.id, link.short_code, link.original_url)
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