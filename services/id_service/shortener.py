from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
from state_manager import StateManager
from fastapi.responses import FileResponse

app = FastAPI()
state = StateManager()

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def encode_base62(num: int) -> str:
    if num == 0:
        return "0"
    result = []
    base = 62
    while num > 0:
        num, rem = divmod(num, base)
        result.append(BASE62_ALPHABET[rem])
    return "".join(reversed(result))


class ShortenRequest(BaseModel):
    url: str


ROUTER_URL = "http://router_service:8001/save_link"


@app.post("/shorten")
async def shorten(req: ShortenRequest):
    #Получаем новый ID
    link_id = state.get_next_id()

    #Кодируем в Base62
    short_code = encode_base62(link_id)

    #Формируем JSON для Router Service
    payload = {
        "id": link_id,
        "short_code": short_code,
        "original_url": req.url
    }

    #Отправляем POST в Router Service
    async with httpx.AsyncClient() as client:
        resp = await client.post(ROUTER_URL, json=payload)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"Router service error: {resp.text}"
        )

    # Возвращаем клиенту короткую ссылку
    return {
        "short_url": f"http://shortener/{short_code}",
        "id": link_id,
        "short_code": short_code
    }
@app.get("/")
def index():
    return FileResponse("interface.html")
