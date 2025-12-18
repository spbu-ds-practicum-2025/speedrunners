import httpx
import sys

# Стучимся в Nginx (порт 80)
BASE_URL = "http://localhost"

def run_e2e():
    print("🚀 Запуск функциональных E2E тестов...\n")
    
    # 1. Тест на 404
    # ИСПОЛЬЗУЕМ ТОЛЬКО БУКВЫ/ЦИФРЫ, чтобы пройти валидацию формата
    bad_code = "NonExistentCode123" 
    print(f"TEST 1: Запрос несуществующей ссылки (/{bad_code})...", end=" ")
    try:
        r = httpx.get(f"{BASE_URL}/{bad_code}")
        if r.status_code == 404:
            print("✅ PASS (Вернул 404)")
        else:
            print(f"❌ FAIL (Ожидался 404, получен {r.status_code})")
            # Для отладки выведем ответ
            if r.status_code == 500:
                print(f"   Details: {r.text}")
    except Exception as e:
        print(f"❌ ERROR: {e}")

    # 2. Тест на валидацию (Пустой URL)
    print("TEST 2: Отправка пустого URL...", end=" ")
    try:
        # API должен вернуть 422 (Pydantic)
        r = httpx.post(f"{BASE_URL}/shorten", json={"url": ""}) 
        if r.status_code == 422:
             print(f"✅ PASS (Вернул {r.status_code} Validation Error)")
        else:
             print(f"❌ FAIL (Ожидалась ошибка 422, получен {r.status_code})")
    except Exception as e:
        print(f"❌ ERROR: {e}")

    # 3. Тест полного цикла
    print("TEST 3: Полный цикл (Shorten -> Redirect)...", end=" ")
    original = "https://www.wikipedia.org"
    try:
        # А. Сокращаем
        r = httpx.post(f"{BASE_URL}/shorten", json={"url": original})
        if r.status_code != 200:
            print(f"❌ FAIL (Shorten failed: {r.text})")
            return
        
        short_code = r.json()["short_code"]
        
        # Б. Проверяем редирект (без перехода)
        r_redir = httpx.get(f"{BASE_URL}/{short_code}", follow_redirects=False)
        
        if r_redir.status_code == 307 and r_redir.headers["location"] == original:
            print("✅ PASS")
        else:
            print(f"❌ FAIL (Ожидался 307 на {original}, получен {r_redir.status_code})")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    run_e2e()