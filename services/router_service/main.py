from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Импортируем наш класс хранилища
from storage import AsyncStorage

# 1. Глобальный экземпляр хранилища (MVP: только один шард)
storage = AsyncStorage("shard_0.db")

# Pydantic-модель для валидации входящих данных
class LinkSchema(BaseModel):
    short_code: str
    original_url: str

# Lifespan (события запуска и остановки) - новый стандарт FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте создаем таблицу, если её нет
    await storage.initialize_table()
    print("Storage initialized: shard_0.db ready.")
    yield
    # При выключении можно закрыть соединения, если потребуется

app = FastAPI(lifespan=lifespan)

@app.post("/save_link")
async def save_link(link: LinkSchema):
    """
    Сохраняет ссылку в БД.
    """
    try:
        # ЗАДАЧА ВЫПОЛНЕНА: Заменили print на реальную запись
        await storage.insert_link(link.short_code, link.original_url)
        return {"status": "ok", "message": "Link saved"}
    except Exception as e:
        # Например, если такой short_code уже есть (Unique constraint failed)
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_link")
async def get_link(short_code: str):
    """
    Возвращает original_url по short_code.
    Пример запроса: GET /get_link?short_code=Abc12
    """
    # ЗАДАЧА ВЫПОЛНЕНА: Вызываем метод поиска
    url = await storage.get_original_url(short_code)
    
    if url is None:
        raise HTTPException(status_code=404, detail="Link not found")
        
    return {"original_url": url}