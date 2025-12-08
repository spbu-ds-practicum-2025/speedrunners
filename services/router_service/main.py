from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from storage import AsyncStorage

storage = AsyncStorage("shard_0.db")

class LinkSchema(BaseModel):
    id: int               # <--- Добавили поле ID (пока вручную)
    short_code: str
    original_url: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    await storage.initialize_table()
    print("Storage initialized: shard_0.db ready.")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/save_link")
async def save_link(link: LinkSchema):
    try:
        # Передаем link.id в метод
        await storage.insert_link(link.id, link.short_code, link.original_url)
        return {"status": "ok", "message": "Link saved"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_link")
async def get_link(short_code: str):
    url = await storage.get_original_url(short_code)
    if url is None:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"original_url": url}