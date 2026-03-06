FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY demo_producer/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY demo_producer/ demo_producer/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
# Default: send all sample logs once then exit
# Override with: docker compose run demo-producer python scripts/demo_producer.py --loop
CMD ["python", "demo_producer/demo_producer.py"]