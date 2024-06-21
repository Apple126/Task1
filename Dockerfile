# Используем базовый образ Python
FROM python:3.10-slim

# Установим зависимости для работы с PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev

# Установим рабочую директорию
WORKDIR /app

# Скопируем requirements.txt в контейнер
COPY requirements.txt .

# Установим зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем все файлы вашего проекта в контейнер
COPY . .

# Определим команду для запуска скрипта
CMD ["python", "SQL connect.py"]