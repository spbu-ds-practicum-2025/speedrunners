import requests
import concurrent.futures
import time
import random

# Конфигурация
API_URL = "http://127.0.0.1:8000/save_link"
NUM_THREADS = 50  # Количество потоков (как просил Тимлид)
REQUESTS_PER_THREAD = 20  # Сколько запросов шлет каждый поток (итого 1000 запросов)

# Для теста пишем всё в Shard 0 (IDs < 1 000 000)
# Генерируем ID от 1 до 50000, чтобы точно попасть в первый файл
START_ID = 1


def send_request(thread_index):
    """
    Функция, которую выполняет каждый поток.
    """
    success_count = 0
    error_count = 0

    for i in range(REQUESTS_PER_THREAD):
        # Вычисляем уникальный ID, чтобы не было конфликтов (Unique Constraint)
        # Пример: Поток 0 шлет ID 0, 50, 100... Поток 1 шлет 1, 51, 101...
        current_id = START_ID + (thread_index + (i * NUM_THREADS))

        payload = {
            "id": current_id,
            "short_code": f"stress_{current_id}",  # Уникальный код
            "original_url": f"http://example.com/{current_id}"
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            if response.status_code == 200:
                success_count += 1
            else:
                print(f"[Thread {thread_index}] Error {response.status_code}: {response.text}")
                error_count += 1
        except Exception as e:
            print(f"[Thread {thread_index}] Exception: {e}")
            error_count += 1

    return success_count, error_count


def main():
    print(f"--- STARTING STRESS TEST ---")
    print(f"Threads: {NUM_THREADS}")
    print(f"Total Requests: {NUM_THREADS * REQUESTS_PER_THREAD}")
    print(f"Target: {API_URL} (Assuming Shard 0)")

    start_time = time.time()

    # Запускаем пул из 50 потоков
    total_success = 0
    total_errors = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Запускаем задачи
        futures = [executor.submit(send_request, i) for i in range(NUM_THREADS)]

        # Ждем выполнения
        for future in concurrent.futures.as_completed(futures):
            s, e = future.result()
            total_success += s
            total_errors += e

    duration = time.time() - start_time
    rps = (NUM_THREADS * REQUESTS_PER_THREAD) / duration

    print(f"\n--- TEST FINISHED ---")
    print(f"Time: {duration:.2f} seconds")
    print(f"RPS (Requests per sec): {rps:.2f}")
    print(f"Success: {total_success}")
    print(f"Errors: {total_errors}")


if __name__ == "__main__":
    main()