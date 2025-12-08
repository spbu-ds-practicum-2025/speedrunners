from fastapi import FastAPI
from pydantic import BaseModel
from sharding import get_target_shard

app = FastAPI(title="Router Service")


class SaveRequest(BaseModel):
    id: int
    url: str
    short_code: str

@app.post("/save_link")
def save_link(data: SaveRequest):
    """
    1. Вычисляет шард по ID.
    2. (В будущем) Передает данные в Storage для записи в БД.
    """

    target_file = get_target_shard(data.id)
    
    # Логирование (вместо реальной записи в БД пока что)
    print(f"[ROUTER] Request to save ID={data.id} in {target_file}")
    print(f"[ROUTER] Data: {data.url} -> {data.short_code}")
    
    # await storage.insert(target_file, data.id, data.url)
    
    return {
        "status": "ok",
        "shard": target_file,
        "message": "Link saved (simulated)"
    }