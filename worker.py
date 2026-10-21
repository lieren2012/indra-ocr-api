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
# 容器内专属路径（/tmp 非共享卷），供 healthcheck 校验本 worker 心跳
WORKER_ID_FILE = "/tmp/india_worker_id"

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


def _finalize(job_key: str, jid: str, path: "str | None", mapping: dict) -> bool:
    """把终态写入 Redis；**只有落库成功**才移除 processing 记录并删文件。

    顺序严格为：落库 → 移出 processing → 删文件。
    - 落库失败：直接返回 False，不删文件、不移出 processing（交给看门狗稍后重投），绝不丢任务。
    - 落库成功但删文件失败：只是留下垃圾文件（cron 兜底清理），任务结果已安全。
    """
    try:
        r.hset(job_key, mapping=mapping)
        r.expire(job_key, JOB_TTL)
    except Exception:
        return False  # 落库失败 —— 保留一切，等重投
    try:
        r.lrem(PROC_LIST, 1, jid)
    except Exception:
        pass
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
    return True


def process(job_id: str) -> None:
    job_key = f"ocr:job:{job_id}"
    try:
        job = r.hgetall(job_key)
    except Exception:
        return  # 读不到 → 保留在 processing，看门狗会重投

    if not job:
        try:
            r.lrem(PROC_LIST, 1, job_id)
        except Exception:
            pass
        return

    suffix = job.get("suffix", ".png")
    max_pages = int(job.get("max_pages", "0") or "0")
    path = os.path.join(TMPDIR, job_id + suffix)

    if not os.path.exists(path):
        _finalize(job_key, job_id, None, {"status": "error", "error": "file missing (worker restarted?)"})
        return

    try:
        r.hset(job_key, mapping={"status": "processing", "processing_since": str(time.time())})
        t0 = time.time()
        result = ocr_engine.run_ocr(path, max_pages)
        elapse = round(time.time() - t0, 3)
    except Exception as exc:  # noqa: BLE001
        _finalize(job_key, job_id, path, {"status": "error", "error": str(exc)})
        return

    # OCR 成功：落库失败则留在 processing（不删文件），由看门狗稍后重投，绝不丢
    _finalize(
        job_key,
        job_id,
        path,
        {
            "status": "done",
            "result": json.dumps(result, ensure_ascii=False),
            "elapse": str(elapse),
        },
    )


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
    # ★ 先加载模型：模型真正就绪之后才注册 + 发心跳。
    #   否则 /readyz 会在模型还在加载时就误报"有 worker 就绪"。
    #   若加载失败，本进程直接退出（未注册、无心跳），/readyz 会如实返回 503。
    ocr_engine.load_model(OCR_THREADS)
    print(f"[worker {WORKER_ID}] model loaded, threads={OCR_THREADS}", flush=True)

    # 写容器专属 worker_id（/tmp 非共享卷，供 healthcheck 校验自身心跳）
    try:
        with open(WORKER_ID_FILE, "w", encoding="utf-8") as f:
            f.write(WORKER_ID)
    except OSError:
        pass

    # 模型就绪后再注册 + 首次心跳，然后启动独立心跳线程
    try:
        r.set(HEARTBEAT_KEY, str(time.time()), ex=HB_TTL)
        r.sadd(WORKERS_SET, WORKER_ID)
    except Exception:
        pass
    threading.Thread(target=_heartbeat_loop, daemon=True).start()

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
