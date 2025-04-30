# syntax = docker/dockerfile:1.4

###########################
# Builder stage (deps)
###########################
FROM python:3.11-slim-buster AS deps
WORKDIR /deps

# Install OS dependencies for building wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and build Python deps, with a cache mount whose ID is hard-coded
COPY backend/requirements.txt .
RUN --mount=type=cache,id=s/d26617b4-84b9-44ca-92fd-1c7259296ecc-/root/cache/pip,target=/root/.cache/pip \
    PIP_NO_BINARY=scikit-learn \
    pip install --no-cache-dir -r requirements.txt

###########################
# Runtime stage (slim)
###########################
FROM python:3.11-slim-buster AS runtime

COPY --from=deps /usr/local /usr/local
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        curl && \
    rm -rf /var/lib/apt/lists/*

ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libgomp.so.1 \
    OMP_NUM_THREADS=1 \
    PYTHONUNBUFFERED=1 \
    PREFECT_HOME=/root/.prefect

WORKDIR /app
COPY backend/ .

CMD ["bash"]