FROM python:3.12-slim

# 系统库：opencv-headless 需要 libgl，pypdfium2 自带二进制无需编译
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HF_HOME=/opt/models \
    HF_HUB_OFFLINE=1 \
    TMPDIR=/tmp/indra \
    OCR_THREADS=1 \
    REDIS_URL=redis://redis:6379/0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 同一镜像同时承载 API 与 Worker，靠启动命令区分
COPY ocr_engine.py api.py worker.py ./
COPY scripts/worker_healthcheck.py ./worker_healthcheck.py

EXPOSE 8000

# 默认启动 API；Worker 由 docker-compose 用 command 覆盖
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port 8000 --workers ${API_WORKERS:-1}"]
