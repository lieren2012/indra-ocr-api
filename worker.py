"""
Indra OCR Worker（v2.1 —— 可靠队列 + 独立心跳）

职责（重，加载模型、吃 OCR 算力）：
  - 注册：把自己的 id 写入 Redis 集合 `ocr:workers`（供 API 看门狗识别死活）
  - 心跳：独立后台线程每 15s 刷 `ocr:worker:{id}`（TTL 60s）——与 OCR 主线程解耦，
    因此**长文档处理期间心跳仍持续**，不会被误判离线、也不会被看门狗误回收。
  - 取任务：BLMOVE 原子地把 job 从 `ocr:queue` 挪到本 worker 的 `ocr:processing:{id}`，
    处理成功后才 LREM 移除。若进程中途崩溃，job 仍留在 processing 列表里，
    由 API 的看门狗在心跳过期后重新入队——**不丢任务**。
  - 优雅退出：收到 SIGTERM 后处理完当前任务，剩余未处理项重新入队。

横向扩容 = 多跑几个本进程副本（compose `--scale worker=N`），
每个副本独立加载模型、占用 OCR_THREADS 个线程。API 不需动。

环境变量：
  REDIS_URL       同 api.py
  HF_HOME         模型缓存目录（离线部署指向预置目录）
  HF_HUB_OFFLINE  1 强制离线
  TMPDIR          与 api.py 共享的落盘目录
  OCR_THREADS     onnxruntime 线程数（默认 8）
  WORKER_HEARTBEAT_TTL  心跳 TTL 秒（默认 60）
  WORKER_HEARTBEAT_REFRESH 心跳刷新间隔秒（默认 15）
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
import uuid

import redis

import ocr_engine

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
TMPDIR = os.getenv("TMPDIR", "/tmp/indra")
OCR_THREADS = int(os.getenv("OCR_THREADS", "8"))
JOB_TTL = int(os.getenv("JOB_TTL", "86400"))
HB_TTL = int(os.getenv("WORKER_HEARTBEAT_TTL", "60"))
HB_REFRESH = int(os.getenv("WORKER_HEARTBEAT_REFRESH", "15"))

QUEUE = "ocr:queue"
WORKERS_SET = "ocr:workers"

WORKER_ID = uuid.uuid4().hex
HEARTBEAT_KEY = f"ocr:worker:{WORKER_ID}"
PROC_LIST = f"ocr:processing:{WORKER_ID}"

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
_stop = threading.Event()


def _handle_signal(signum, frame):  # noqa: ANN001
    _stop.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _heartbeat_loop() -> None:
    # 独立线程：无论主线程在跑多久的 OCR，心跳都持续刷新
    while not _stop.is_set():
        try:
            r.set(HEARTBEAT_KEY, str(time.time()), ex=HB_TTL)
        except Exception:
            pass
        _stop.wait(HB_REFRESH)


def process(job_id: str) -> None:
    job_key = f"ocr:job:{job_id}"
    job = r.hgetall(job_key)
    if not job:
        r.lrem(PROC_LIST, 1, job_id)
        return

    suffix = job.get("suffix", ".png")
    max_pages = int(job.get("max_pages", "0") or "0")
    path = os.path.join(TMPDIR, job_id + suffix)

    if not os.path.exists(path):
        r.hset(job_key, mapping={"status": "error", "error": "file missing (worker restarted?)"})
        r.expire(job_key, JOB_TTL)
        r.lrem(PROC_LIST, 1, job_id)
        return

    try:
        r.hset(job_key, mapping={"status": "processing", "processing_since": str(time.time())})
        t0 = time.time()
        result = ocr_engine.run_ocr(path, max_pages)
        elapse = round(time.time() - t0, 3)
        r.hset(
            job_key,
            mapping={
                "status": "done",
                "result": json.dumps(result, ensure_ascii=False),
                "elapse": str(elapse),
            },
        )
        r.expire(job_key, JOB_TTL)
        try:
            os.remove(path)
        except OSError:
            pass
    except Exception as exc:  # noqa: BLE001
        try:
            r.hset(job_key, mapping={"status": "error", "error": str(exc)})
            r.expire(job_key, JOB_TTL)
        except Exception:
            pass
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
    finally:
        # 只有成功/失败都已落库后才从 processing 列表移除；崩溃则留给看门狗回收
        r.lrem(PROC_LIST, 1, job_id)


def _cleanup() -> None:
    """优雅退出：把本 worker 未处理完的任务重新入队，并注销自己。"""
    try:
        jobs = r.lrange(PROC_LIST, 0, -1)
        if jobs:
            r.rpush(QUEUE, *jobs)
            for jid in jobs:
                r.hset(f"ocr:job:{jid}", "status", "pending")
                r.hdel(f"ocr:job:{jid}", "processing_since")
        r.delete(PROC_LIST)
        r.srem(WORKERS_SET, WORKER_ID)
        r.delete(HEARTBEAT_KEY)
    except Exception:
        pass


def main() -> None:
    # 先注册 + 首次心跳，再启动心跳线程（避免"在集合里但没心跳"的窗口）
    try:
        r.set(HEARTBEAT_KEY, str(time.time()), ex=HB_TTL)
        r.sadd(WORKERS_SET, WORKER_ID)
    except Exception:
        pass
    threading.Thread(target=_heartbeat_loop, daemon=True).start()

    ocr_engine.load_model(OCR_THREADS)
    print(f"[worker {WORKER_ID}] model loaded, threads={OCR_THREADS}", flush=True)

    while not _stop.is_set():
        try:
            # BLMOVE：原子地把任务从队列挪到本 worker 的 processing 列表
            job_id = r.blmove(QUEUE, PROC_LIST, 10, "LEFT", "RIGHT")
        except Exception as exc:  # noqa: BLE001
            print(f"[worker {WORKER_ID}] blmove error: {exc}", flush=True)
            _stop.wait(2)
            continue
        if job_id:
            process(job_id)

    _cleanup()
    print(f"[worker {WORKER_ID}] stopped", flush=True)


if __name__ == "__main__":
    main()
