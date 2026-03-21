FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget curl gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# Install Playwright Chromium + its dependencies
RUN playwright install chromium --with-deps

COPY . .

CMD uvicorn api.main:app --host 0.0.0.0 --port $PORT
