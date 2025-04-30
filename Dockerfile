# syntax=docker/dockerfile:1

######################################################################
# 1) "deps" stage – build once-in-a-while when requirements change    #
######################################################################
FROM python:3.11-slim-buster AS deps

# Minimal system libraries needed by your Python stack
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential gcc \
        libgl1-mesa-glx libglib2.0-0 \
        curl ca-certificates ffmpeg \
    && \
    # Optional: fast, static ARM‑64 ffmpeg (strips pattern matching inside tar)
    curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz \
      | tar -xJ --wildcards --strip-components=2 -C /usr/local/bin '*/ffmpeg' \
    && chmod +x /usr/local/bin/ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /deps
COPY backend/requirements.txt .

# Cache pip wheels between rebuilds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

############################################################
# 2) runtime stage – copied fast for every incremental build #
############################################################
FROM python:3.11-slim-buster

# Pull in the fully-installed site-packages and binaries
COPY --from=deps /usr/local /usr/local

# Runtime environment tweaks
ENV PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

WORKDIR /app
COPY backend/ .

# Default command; overridden in docker-compose
CMD ["prefect", "server", "start", "--host", "0.0.0.0"]