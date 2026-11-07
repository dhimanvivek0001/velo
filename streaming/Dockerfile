# Image for all Velo streaming apps (producer, sink, lag monitor)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install libraries first: this layer is cached and only rebuilt when requirements change
COPY requirements.txt /app/streaming/requirements.txt
RUN pip install --no-cache-dir -r /app/streaming/requirements.txt

# Then copy the code (changes often, rebuilds in seconds)
COPY . /app/streaming
