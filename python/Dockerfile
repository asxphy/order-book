FROM python:3.12-slim-trixie

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# system deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Default envs (override in compose)
ENV KAFKA_BOOTSTRAP=kafka:9092 \
    ENGINE_GROUP=matching-engine \
    ORDERS_IN=orders.in \
    EVENTS_OUT=events.out

CMD ["python", "-m", "app"]
