from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
import httpx
import asyncio

app = FastAPI(title="Public API Service")

# Ссылки на соседние контейнеры
ID_GEN_URL = "http://id_generator:8000/allocate"
ROUTER_URL = "http://router_service:8001"

# === ЛОГИКА БУФЕРА (Клиент для ID Gen) ===
class IDBuffer:
    def __init__(self):
        self.available_ids = []
    
    async def get_one_id(self) -> int:
        if not self.available_ids:
            print("[API] Буфер пуст, запрашиваю новые ID...")
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.post(ID_GEN_URL, json={"size": 100})
                    resp.raise_for_status()
                    data = resp.json()
                    self.available_ids = list(range(data["start"], data["end"] + 1))
                except Exception as e:
                    print(f"[API] Ошибка ID Generator: {e}")
                    raise HTTPException(status_code=500, detail="ID Generator unavailable")
        
        return self.available_ids.pop(0)

id_buffer = IDBuffer()

# === BASE62 ===
BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
def encode_base62(num: int) -> str:
    if num == 0: return "0"
    arr = []
    base = 62
    while num:
        num, rem = divmod(num, base)
        arr.append(BASE62[rem])
    arr.reverse()
    return "".join(arr)

# === МОДЕЛИ ===
class ShortenRequest(BaseModel):
    url: str

# === ЭНДПОИНТЫ ===
@app.get("/")
def index():
    return FileResponse("interface.html")

@app.post("/shorten")
async def shorten(req: ShortenRequest):
    # 1. Получаем ID по сети (или из буфера)
    link_id = await id_buffer.get_one_id()

    # 2. Кодируем
    short_code = encode_base62(link_id)

    # 3. Отправляем в Роутер (Соблюдаем схему Роутера!)
    payload = {
        "id": link_id,
        "short_code": short_code,
        "original_url": req.url # Важно: Роутер ждет original_url
    }

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{ROUTER_URL}/save_link", json=payload)
            if resp.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Router error: {resp.text}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Router unavailable: {e}")

    return {
        "short_url": f"http://localhost:8080/{short_code}",
        "short_code": short_code
    }

@app.get("/{short_code}")
async def redirect_to_url(short_code: str):
    async with httpx.AsyncClient() as client:
        # Запрашиваем оригинальную ссылку у Роутера
        resp = await client.get(f"{ROUTER_URL}/get_link", params={"short_code": short_code})
        
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Link not found")
        
        if resp.status_code == 200:
            long_url = resp.json()["original_url"]
            return RedirectResponse(long_url)
            
    raise HTTPException(status_code=500, detail="Unknown error")