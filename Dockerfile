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
    OCR_THREADS=8 \
    OCR_WORKERS=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8000

# 单 worker 足以应对低负载（200 文档/天）；多 worker 需配合 Redis 共享任务状态
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port 8000 --workers ${OCR_WORKERS:-1}"]
