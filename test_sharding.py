import httpx
import asyncio
import time

# Адрес Роутера (мы будем стучаться к нему снаружи, через localhost)
ROUTER_URL = "http://localhost:8001/save_link"

async def run_test():
    print("🚀 Начинаем тест шардирования...")
    
    # Генерируем 250 ссылок
    # Представим, что лимит шарда = 100 (мы настроим это в docker-compose)
    # Ожидание: 
    # ID 0-99 -> shard_0
    # ID 100-199 -> shard_1
    # ID 200-249 -> shard_2
    
    for i in range(1, 251):
        payload = {
            "id": i,
            "short_code": f"test_{i}",
            "original_url": f"http://site.com/{i}"
        }
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(ROUTER_URL, json=payload)
                
            if i % 10 == 0:
                print(f"Saved ID {i}...", end="\r")
                
            # Проверяем триггер преаллокации (90% от 100 = 90)
            if i == 90:
                print(f"\n⚠️ ID {i}: Достигнут порог 90%! Должен создаться shard_1.db")
                await asyncio.sleep(0.5) # Даем фору фоновой задаче
            if i == 190:
                print(f"\n⚠️ ID {i}: Достигнут порог 90%! Должен создаться shard_2.db")
                await asyncio.sleep(0.5)

        except Exception as e:
            print(f"Ошибка на ID {i}: {e}")
            break

    print("\n✅ Тест завершен. Проверь папку data!")

if __name__ == "__main__":
    asyncio.run(run_test())