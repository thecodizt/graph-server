FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories with proper permissions
RUN mkdir -p /app/data/livestate \
    /app/data/statearchive \
    /app/data/schemaarchive \
    /app/data/liveschema \
    /app/data/nativeformat \
    && chown -R nobody:nogroup /app/data \
    && chmod -R 777 /app/data

COPY . /app

RUN python setup.py
