# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Default command: Celery worker (beat uses a different command in stack file)
CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "--concurrency=4", "-Q", "default,high_priority"]
