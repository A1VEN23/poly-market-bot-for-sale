FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render использует PORT из env (по умолчанию 10000)
EXPOSE 10000

# Запускаем uvicorn — он поднимет FastAPI + бот через lifespan
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-10000} --log-level warning