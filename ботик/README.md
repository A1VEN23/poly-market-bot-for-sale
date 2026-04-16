---
title: Polik Bott
emoji: 🤖
colorFrom: green
colorTo: blue
sdk: docker
app_port: 7860
---

# Polik Bott

Telegram-бот для мониторинга Polymarket. Отправляет торговые сигналы на основе трёх стратегий:

- **UMA Oracle** — мониторинг рынков возле истечения
- **News Analysis** — AI-анализ новостей через Ollama
- **Metaculus Consensus** — расхождение прогнозов Metaculus и Polymarket

## Деплой на Hugging Face Spaces

Docker SDK, FastAPI на порту 7860, Telegram-бот в отдельном потоке.

### Переменные окружения (Settings → Variables and secrets)

| Ключ | Описание |
|------|----------|
| `TELEGRAM_TOKEN` | Токен Telegram-бота |
| `METACULUS_API_KEY` | API-ключ Metaculus |
| `CRYPTO_BOT_TOKEN` | Токен CryptoBot (для оплаты VIP) |
| `ADMIN_IDS` | ID администраторов (через запятую) |
