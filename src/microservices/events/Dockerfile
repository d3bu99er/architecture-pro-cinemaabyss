# Сборочный этап
FROM python:3.11-slim AS builder

WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Копируем исходный код
COPY main.py .

# Финальный образ
FROM python:3.11-slim

WORKDIR /app

# Устанавливаем только необходимые системные пакеты (ca-certificates для HTTPS)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Копируем установленные зависимости и приложение из builder-этапа
COPY --from=builder /root/.local /root/.local
COPY --from=builder /app/main.py .

# Добавляем /root/.local/bin в PATH, чтобы uvicorn был доступен
ENV PATH=/root/.local/bin:$PATH

EXPOSE 8082

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8082"]