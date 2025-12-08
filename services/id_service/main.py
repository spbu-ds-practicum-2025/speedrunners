from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from state_manager import StateManager

# 1. Создаем объект приложения
app = FastAPI(title="ID Generator Service")

# 2. Инициализируем менеджер состояния
state = StateManager()

# 3. Определяем схему данных (Pydantic Model)
# Это описание того, какой JSON мы ждем от Апи Сервиса
class AllocateRequest(BaseModel):
    size: int  # Например: 1000

# 4. Пишем эндпоинт (ручку)
@app.post("/allocate")
def allocate_range(request: AllocateRequest):
    """
    Выдает диапазон ID и сохраняет новое состояние на диск.
    """
    # Валидация бизнес-логики (нельзя просить 0 или меньше)
    if request.size <= 0:
        raise HTTPException(status_code=400, detail="Size must be positive")

    # Читаем текущий максимум (например, 0)
    current_max = state.get_current_max()

    # Вычисляем новый диапазон
    # start = 1
    # end = 1000
    new_start = current_max + 1
    new_end = current_max + request.size

    # Сохраняем новый максимум (1000) на диск
    state.update_max(new_end)

    print(f"Allocated range: {new_start} - {new_end}") # Лог в консоль

    # Возвращаем JSON
    return {
        "start": new_start,
        "end": new_end
    }