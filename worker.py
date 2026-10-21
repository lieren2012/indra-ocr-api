"""
Indra OCR Worker（v2.0 —— 队列解耦版）

职责（重，加载模型、吃 OCR 算力）：
  - 心跳：每轮往 Redis 写 `ocr:worker:{id}`（TTL 30s），供 API 健康探测
  - 从 Redis 队列 BRPOP 取任务 → 读共享 tmp 里的文件 → 跑 OCR → 结果写回任务哈希
  - 完成后删本地文件；异常标记 error

横向扩容 = 多跑几个本进程的副本（docker compose `worker` 服务 replicas），
每个副本独立加载模型、占用 OCR_THREADS 个线程。API 不需动。

环境变量：
  REDIS_URL       同 api.py
  HF_HOME         模型缓存目录（离线部署指向预置目录）
  HF_HUB_OFFLINE  1 强制离线
  TMPDIR          与 api.py 共享的落盘目录
  OCR_THREADS     onnxruntime 线程数（默认 8）
  WORKER_HEARTBEAT_TTL  心跳 TTL（默认 30s）
"""

from __future__ import annotations

import json
import os
import signal
import time
import uuid

import redis

import ocr_engine

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
TMPDIR = os.getenv("TMPDIR", "/tmp/indra")
OCR_THREADS = int(os.getenv("OCR_THREADS", "8"))
QUEUE = "ocr:queue"
HEARTBEAT_TTL = int(os.getenv("WORKER_HEARTBEAT_TTL", "30"))
JOB_TTL = int(os.getenv("JOB_TTL", "86400"))

WORKER_ID = uuid.uuid4().hex
HEARTBEAT_KEY = f"ocr:worker:{WORKER_ID}"

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_stop = {"flag": False}


def _handle_signal(signum, frame):  # noqa: ANN001
    _stop["flag"] = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def process(job_id: str) -> None:
    try:
        job = r.hgetall(f"ocr:job:{job_id}")
        if not job:
            return
        suffix = job.get("suffix", ".png")
        max_pages = int(job.get("max_pages", "0") or "0")
        path = os.path.join(TMPDIR, job_id + suffix)
        if not os.path.exists(path):
            r.hset(f"ocr:job:{job_id}", "status", "error")
            r.hset(f"ocr:job:{job_id}", "error", "file missing (worker restarted before pickup?)")
            r.expire(f"ocr:job:{job_id}", JOB_TTL)
            return

        r.hset(f"ocr:job:{job_id}", "status", "processing")
        t0 = time.time()
        result = ocr_engine.run_ocr(path, max_pages)
        elapse = round(time.time() - t0, 3)
        r.hset(
            f"ocr:job:{job_id}",
            mapping={
                "status": "done",
                "result": json.dumps(result, ensure_ascii=False),
                "elapse": str(elapse),
            },
        )
        r.expire(f"ocr:job:{job_id}", JOB_TTL)  # 完成后保留 24h，避免 Redis 无限增长
        try:
            os.remove(path)
        except OSError:
            pass
    except Exception as exc:  # noqa: BLE001
        try:
            r.hset(
                f"ocr:job:{job_id}",
                mapping={"status": "error", "error": str(exc)},
            )
            r.expire(f"ocr:job:{job_id}", JOB_TTL)
        except Exception:
            pass
        suffix = (job or {}).get("suffix", ".png")
        try:
            leftover = os.path.join(TMPDIR, job_id + suffix)
            if os.path.exists(leftover):
                os.remove(leftover)
        except OSError:
            pass


def main() -> None:
    ocr_engine.load_model(OCR_THREADS)
    print(f"[worker {WORKER_ID}] model loaded, threads={OCR_THREADS}", flush=True)
    while not _stop["flag"]:
        # 心跳：声明自己活着，并顺手清掉过期心跳（由 TTL 自动处理）
        r.set(HEARTBEAT_KEY, str(time.time()), ex=HEARTBEAT_TTL)
        item = r.brpop(QUEUE, timeout=max(5, HEARTBEAT_TTL - 5))
        if item is None:
            continue
        _, job_id = item
        process(job_id)
    print(f"[worker {WORKER_ID}] shutting down", flush=True)


if __name__ == "__main__":
    main()
