import httpx
import time
import sys

# Теперь стучимся на 80 порт (по умолчанию http://localhost)
BASE_URL = "http://localhost"


def check_system():
    print("🚬 Запуск дымового теста (Smoke Test)...")

    try:
        # 1. Проверка главной страницы
        print("1. Запрос главной страницы...", end=" ")
        r = httpx.get(f"{BASE_URL}/")
        if r.status_code == 200:
            print("✅ OK")
        else:
            print(f"❌ FAIL ({r.status_code})")
            sys.exit(1)

        # 2. Создание ссылки
        print("2. Создание короткой ссылки...", end=" ")
        payload = {"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}
        r = httpx.post(f"{BASE_URL}/shorten", json=payload)

        if r.status_code == 200:
            data = r.json()
            short_code = data["short_code"]
            print(f"✅ OK (Code: {short_code})")
        else:
            print(f"❌ FAIL ({r.text})")
            sys.exit(1)

        # 3. Проверка редиректа
        print(f"3. Проверка редиректа /{short_code}...", end=" ")
        # allow_redirects=False, чтобы увидеть 307, а не улететь на ютуб
        r = httpx.get(f"{BASE_URL}/{short_code}", follow_redirects=False)

        if r.status_code == 307:
            print(f"✅ OK (Location: {r.headers['location']})")
        else:
            print(f"❌ FAIL (Status: {r.status_code})")
            sys.exit(1)

        print("\n🎉 СИСТЕМА ПОЛНОСТЬЮ РАБОТОСПОСОБНА! 🎉")

    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Даем системе пару секунд на старт после docker-compose up
    time.sleep(2)
    check_system()
