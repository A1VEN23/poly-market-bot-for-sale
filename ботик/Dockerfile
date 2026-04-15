FROM python:3.11-slim

# Reduce image size — no cache, no extras
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render injects PORT; default 10000 for local runs
EXPOSE 10000

# Single worker — keeps RAM predictable on 512 MB free tier.
# --timeout-keep-alive 75 avoids Render's 75 s idle-connection close.
CMD uvicorn app:app \
    --host 0.0.0.0 \
    --port ${PORT:-10000} \
    --workers 1 \
    --log-level warning \
    --timeout-keep-alive 75
