# services/api_service/Dockerfile

FROM python:3.14-slim

# Отключаем буферизацию (чтобы логи летели в консоль сразу)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Сначала копируем зависимости (для кэширования слоев)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код (main.py) и интерфейс (interface.html)
COPY . .

# Запускаем на порту 8080 (чтобы не путать с другими)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]