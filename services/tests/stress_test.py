import requests
import concurrent.futures
import time
import random

# Конфигурация
API_URL = "http://127.0.0.1:80/shorten"
NUM_THREADS = 50  # Количество пользователей
REQUESTS_PER_THREAD = 50  # Запросов от каждого


def send_request(thread_index):
    success_count = 0
    error_count = 0

    # Имитация Ramp-up (пользователи приходят не одновременно)
    time.sleep(random.uniform(0.1, 1.0))

    for i in range(REQUESTS_PER_THREAD):
        long_url = f"http://example.com/thread_{thread_index}_req_{i}"
        payload = {"url": long_url}

        try:
            # Timeout 30 сек, чтобы ждать очередь в SQLite
            response = requests.post(API_URL, json=payload, timeout=30)
            if response.status_code == 200:
                success_count += 1
            else:
                # Если 503 - это тоже "условно штатное" поведение при перегрузке
                print(
                    f"[Thread {thread_index}] Error {response.status_code}: {response.text}"
                )
                error_count += 1
        except Exception as e:
            print(f"[Thread {thread_index}] Exception: {e}")
            error_count += 1

    return success_count, error_count


def main():
    print("--- STARTING CONCURRENCY TEST ---")
    print(f"Simulating {NUM_THREADS} concurrent users...")

    start_time = time.time()
    total_success = 0
    total_errors = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(send_request, i) for i in range(NUM_THREADS)]

        for future in concurrent.futures.as_completed(futures):
            s, e = future.result()
            total_success += s
            total_errors += e

    duration = time.time() - start_time
    rps = (total_success + total_errors) / duration

    print("\n--- TEST FINISHED ---")
    print(f"Time: {duration:.2f} seconds")
    print(f"Avg RPS: {rps:.2f}")
    print(f"Success: {total_success}")
    print(f"Errors: {total_errors}")


if __name__ == "__main__":
    main()
