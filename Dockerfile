FROM python:3.11-slim-buster

WORKDIR /app

# Install system dependencies for Python packages (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .
CMD ["sleep", "infinity"]