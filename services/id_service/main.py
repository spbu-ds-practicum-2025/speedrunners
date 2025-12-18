from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from state_manager import StateManager
import asyncio  # <--- 1. Импорт

app = FastAPI(title="ID Generator Service")
state = StateManager()

# 2. Создаем глобальный замок
lock = asyncio.Lock()


class AllocateRequest(BaseModel):
    size: int


@app.post("/allocate")
async def allocate_range(request: AllocateRequest):  # <--- 3. Добавь async!
    if request.size <= 0:
        raise HTTPException(status_code=400, detail="Size must be positive")

    # 4. Входим в критическую секцию.
    # Пока один запрос не выйдет отсюда, второй будет ждать.
    async with lock:
        current_max = state.get_current_max()
        new_start = current_max + 1
        new_end = current_max + request.size
        state.update_max(new_end)

    print(f"Allocated range: {new_start} - {new_end}")

    return {"start": new_start, "end": new_end}
