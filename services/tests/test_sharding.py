import httpx
import asyncio
import os

# Стучимся в публичный API
API_URL = "http://localhost/shorten"
DATA_DIR = "services/data"  # Путь к папке данных на твоем компьютере


async def run_test():
    print("🚀 Начинаем тест шардирования через Public API...")

    # Чтобы тест был честным, лучше запускать его на чистой базе.
    # Но даже на грязной мы увидим создание новых файлов, если лимит низкий.

    print("Генерируем нагрузку (1200 ссылок)...")

    for i in range(1, 1201):
        payload = {"url": f"http://site.com/resource_{i}"}

        try:
            async with httpx.AsyncClient() as client:
                await client.post(API_URL, json=payload)

            if i % 50 == 0:
                print(f"Saved {i} links...", end="\r")

        except Exception as e:
            print(f"Ошибка запроса: {e}")
            break

    print("\n\nПроверка файловой системы...")

    files = os.listdir(DATA_DIR)
    shards = [f for f in files if f.startswith("shard_") and f.endswith(".db")]
    shards.sort()

    print(f"Найдены шарды: {shards}")

    if len(shards) >= 2:
        print(
            "✅ PASS: Система создала несколько шардов (Динамическое шардирование работает)."
        )
    else:
        print(
            "⚠️ WARNING: Найден только один шард. Возможно, лимит слишком большой или база не сброшена."
        )
        print("Для проверки шардинга установите SHARD_LIMIT=100 в docker-compose.")


if __name__ == "__main__":
    asyncio.run(run_test())
