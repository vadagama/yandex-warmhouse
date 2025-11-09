FROM python:3.11-slim

WORKDIR /app

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY app.py .

# Открываем порт
EXPOSE 8081

# Запускаем приложение
CMD ["python", "app.py"]

