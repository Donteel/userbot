FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py login.py server_login.py ignore_list.txt ./
ENV DB_FILE=/app/data/users.db WORKDIR=/app/data PYTHONUNBUFFERED=1
CMD ["python", "bot.py"]
