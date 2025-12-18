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
    handlers=[logging.StreamHandler()],
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


BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def encode_base62(num: int) -> str:
    if num == 0:
        return "0"
    arr = []
    base = 62
    while num:
        num, rem = divmod(num, base)
        arr.append(BASE62[rem])
    arr.reverse()
    return "".join(arr)


def decode_base62(string: str) -> int:
    base = len(BASE62)
    strlen = len(string)
    num = 0
    idx = 0
    for char in string:
        power = strlen - (idx + 1)
        try:
            num += BASE62.index(char) * (base**power)
        except ValueError:
            raise ValueError(f"Invalid character '{char}' in short code")
        idx += 1
    return num


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    await storage.create_new_shard("shard_0.db")
    print(f"Storage initialized. Root dir: {DATA_DIR}")

    yield  # <-- Тут работает приложение

    # SHUTDOWN (Добавлено!)
    print("Shutting down router...")

    # Пытаемся слить данные для первых 5 шардов (или сколько их там)
    for i in range(5):
        try:
            await storage.force_checkpoint(f"shard_{i}.db")
        except Exception:
            pass


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # При старте создаем первый шард (shard_0.db) физически
#     await storage.create_new_shard("shard_0.db")
#     print(f"Storage initialized. Root dir: {DATA_DIR}")
#     yield

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
        await storage.insert_link(
            target_shard_name, link.id, link.short_code, link.original_url
        )

        return {"status": "ok", "shard": target_shard_name}

    except Exception as e:
        print(f"[ROUTER] Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/get_link")
async def get_link(short_code: str):
    try:
        link_id = decode_base62(short_code)
        target_shard = get_target_shard(link_id)

        # Пытаемся найти
        url = await storage.get_original_url(target_shard, short_code)

        if url is None:
            # Выбрасываем 404
            raise HTTPException(
                status_code=404, detail=f"Link not found in {target_shard}"
            )

        return {"original_url": url}

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid short code format")
    except HTTPException as http_exc:
        # Пробрасываем HTTP исключения (404) дальше, не ловя их в общий except
        raise http_exc
    except Exception as e:
        print(f"Read Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
