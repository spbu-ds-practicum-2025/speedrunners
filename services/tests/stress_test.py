import requests
import concurrent.futures
import time

# Конфигурация
API_URL = "http://127.0.0.1:80/shorten" # <-- Убедись, что адрес правильный
NUM_THREADS = 20
REQUESTS_PER_THREAD = 100

def send_request(thread_index):
    success_count = 0
    error_count = 0
    
    for i in range(REQUESTS_PER_THREAD):
        # Просто шлем уникальные ссылки, ID сгенерирует сервер
        long_url = f"http://example.com/thread_{thread_index}_req_{i}"
        
        payload = {
            "url": long_url  # <-- ИСПРАВЛЕНО: поле называется url, и только оно нужно
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=30)
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
    rps = (NUM_THREADS * REQUESTS_PER_THREAD) / duration

    print(f"\n--- TEST FINISHED ---")
    print(f"Time: {duration:.2f} seconds")
    print(f"RPS: {rps:.2f}")
    print(f"Success: {total_success}")
    print(f"Errors: {total_errors}")

if __name__ == "__main__":
    main()