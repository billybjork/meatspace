# syntax = docker/dockerfile:1.4

###########################
# Builder stage (deps)
###########################
FROM python:3.11-slim-buster AS deps

# Grab the Railway service ID at build-time for cache-prefixing
ARG RAILWAY_SERVICE_ID
WORKDIR /deps

# Debug: print out the service ID (remove after verifying)
RUN echo "Building on service $RAILWAY_SERVICE_ID"

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

# Copy requirements and build Python deps, with a cache mount
COPY backend/requirements.txt .

RUN --mount=type=cache,\
id=s/d26617b4-84b9-44ca-92fd-1c7259296ecc-/root/cache/pip,\
target=/root/.cache/pip \
    PIP_NO_BINARY=scikit-learn \
    pip install --no-cache-dir -r requirements.txt

###########################
# Runtime stage (slim)
###########################
FROM python:3.11-slim-buster AS runtime

# Pull in the built Python packages
COPY --from=deps /usr/local /usr/local

# Install only runtime OS deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Preload and env defaults
ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libgomp.so.1 \
    OMP_NUM_THREADS=1 \
    PYTHONUNBUFFERED=1 \
    PREFECT_HOME=/root/.prefect

WORKDIR /app
COPY backend/ .

CMD ["bash"]