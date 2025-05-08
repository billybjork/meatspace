# syntax=docker/dockerfile:1

# ─────────────────────────────────────────
# 1. Builder stage – compile wheels & deps
# ─────────────────────────────────────────
FROM python:3.11-slim-buster AS deps

# Install only what’s needed to build packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential git \
        ffmpeg \
        libgl1 libglib2.0-0 libgomp1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /deps

# Copy only requirements so layer is cached until deps.txt changes
COPY requirements.txt .

# Force scikit-learn to build from source (for OpenMP support)
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_NO_BINARY=scikit-learn \
    pip install --no-cache-dir -r requirements.txt


# ─────────────────────────────────────────
# 2. Runtime stage – lean, production image
# ─────────────────────────────────────────
FROM python:3.11-slim-buster AS runtime

# Bring in the Python interpreter + wheels from deps stage
COPY --from=deps /usr/local /usr/local

# Install only the runtime OS libraries we actually need
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libgl1 libglib2.0-0 libgomp1 curl && \
    rm -rf /var/lib/apt/lists/*

# ─────────────────────────
# OpenMP preload fix
# ─────────────────────────
# Some C-extensions (sklearn, transformers) require libgomp to be
# loaded before Python starts, or they hit “static TLS block” errors.
#
# We auto-detect the correct libgomp.so.1 path and instruct the
# dynamic loader to pre-load it for every executable via ld.so.preload.
RUN set -e; \
    GOMP="$(ldconfig -p | grep -Eo '/.*libgomp\.so\.1$' | head -n1)"; \
    echo "Preloading ${GOMP}"; \
    echo "${GOMP}" > /etc/ld.so.preload

# Set threads and unbuffered output for Python
ENV OMP_NUM_THREADS=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy the application code after deps are in place
COPY . .

# Prefect stores state under this directory by default
ENV PREFECT_HOME=/root/.prefect

# Default shell entrypoint – your docker-compose overrides this with `prefect worker start`
CMD ["bash"]