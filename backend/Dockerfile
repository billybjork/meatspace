# syntax=docker/dockerfile:1

# -----------------------
# Builder stage
# -----------------------
    FROM python:3.11-slim-buster AS deps

    # Core OS dependencies
    RUN apt-get update && \
        apt-get install -y --no-install-recommends \
            build-essential \
            git \
            ffmpeg \
            libgl1 \
            libglib2.0-0 \
            libgomp1 && \
        rm -rf /var/lib/apt/lists/*
    
    WORKDIR /deps
    
    # Copy only requirements (now relative to ./backend)
    COPY requirements.txt .
    
    # Build wheels from source for scikit-learn
    RUN --mount=type=cache,target=/root/.cache/pip \
        PIP_NO_BINARY=scikit-learn \
        pip install --no-cache-dir -r requirements.txt
    
    # -----------------------
    # Runtime stage
    # -----------------------
    FROM python:3.11-slim-buster AS runtime
    
    # Copy built packages
    COPY --from=deps /usr/local /usr/local
    
    # Runtime OS deps
    RUN apt-get update && \
        apt-get install -y --no-install-recommends \
            ffmpeg \
            libgl1 \
            libglib2.0-0 \
            libgomp1 \
            curl && \
        rm -rf /var/lib/apt/lists/*
    
    # Preload libgomp for compatibility
    ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libgomp.so.1 \
        OMP_NUM_THREADS=1 \
        PYTHONUNBUFFERED=1
    
    WORKDIR /app
    
    # Copy your backend code (everything under ./backend)
    COPY . .
    
    # Set Prefect home
    ENV PREFECT_HOME=/root/.prefect
    
    # Default shell entry
    CMD ["bash"]    