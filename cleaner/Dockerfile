FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY cleaner/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/   shared/
COPY cleaner/  cleaner/

RUN touch shared/__init__.py cleaner/__init__.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "cleaner/consumer"]