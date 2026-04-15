FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000

# PYTHONMALLOC=malloc + limit GC threshold to reduce peak RSS
# uvicorn with 1 worker (saves ~60MB vs default 2)
CMD uvicorn app:app \
    --host 0.0.0.0 \
    --port ${PORT:-10000} \
    --workers 1 \
    --log-level warning \
    --no-access-log
