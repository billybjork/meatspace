# syntax=docker/dockerfile:1

# -----------------------
# Builder stage
# -----------------------
FROM python:3.11-slim-buster AS deps
# Consider updating to a specific patch version or newer OS (e.g., bookworm) later for security

# Core OS dependencies – keep this lean but include everything we need to
# build numpy / scipy / scikit‑learn from source and to run FFmpeg + OpenCV
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libgomp1 && \
    # Clean up apt cache
    rm -rf /var/lib/apt/lists/*

# Workdir just for installing Python deps so layers cache nicely
WORKDIR /deps

# Copy the requirements file *only* – this lets us leverage Docker cache
COPY backend/requirements.txt .

# Build wheels **from source** for scikit‑learn to avoid pre‑built wheels that
# pull in a conflicting libgomp binary.
# The combo of PIP_NO_BINARY + --no-cache-dir guarantees a clean, fresh build.
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_NO_BINARY=scikit-learn \
    pip install --no-cache-dir -r requirements.txt

# -----------------------
# Runtime stage (slimmer image)
# -----------------------
FROM python:3.11-slim-buster AS runtime
# Consider updating to a specific patch version or newer OS (e.g., bookworm) later for security

# Copy the built Python site‑packages and binaries from the builder layer
COPY --from=deps /usr/local /usr/local

# Install runtime OS dependencies including curl
# libgomp needs to be available *and* pre‑loaded...
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libgomp1 \
        curl && \
    # Clean up apt cache - Note: VSCode linter might flag the 'rm' below incorrectly
    rm -rf /var/lib/apt/lists/*

# IMPORTANT ➜ preload libgomp so the dynamic loader reserves TLS space early
# Adjust path if building for x86_64 (/usr/lib/x86_64-linux-gnu/libgomp.so.1)
ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libgomp.so.1 \
    OMP_NUM_THREADS=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
# Copy your backend code into the image
COPY backend/ .

# Prefect home inside the container
ENV PREFECT_HOME=/root/.prefect

# Default entry (override in docker‑compose)
CMD ["bash"]