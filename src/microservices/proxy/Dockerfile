FROM python:3.11-slim

WORKDIR /app

# Системные зависимости (если нужны колёса)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Устанавливаем зависимости в системный site-packages!
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY main.py .

# Необязательно, но лучше под некорневым пользователем
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["python", "main.py"]