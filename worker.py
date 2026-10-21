"""
Indra OCR Worker（v2.2 —— 可靠队列 + 独立心跳 + 失败自动重投）

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


# 结果写不进 Redis 时，把任务从 processing **原子** 搬回队列（重投）。
# RPUSH 在前、LREM 在后：若 RPUSH 失败则整个脚本报错、LREM 不执行，
# 任务仍安全留在 processing，绝不会出现"既不在队列也不在 processing"的丢失。
_REQUEUE_LUA = """
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('LREM', KEYS[1], 1, ARGV[1])
return 1
"""


def _try_lrem(jid: str, delete_path: "str | None" = None) -> None:
    """尝试把任务移出 processing；**仅在确实移除成功时**才删文件。"""
    try:
        removed = r.lrem(PROC_LIST, 1, jid)
    except Exception:
        return
    if removed and delete_path:
        try:
            if os.path.exists(delete_path):
                os.remove(delete_path)
        except OSError:
            pass


def _requeue(job_id: str) -> None:
    """结果写不进 Redis 时主动重投，避免任务卡在本 worker 手里永远不重试。
    重投也失败 → 结束本进程，让心跳过期、由看门狗兜底回收。"""
    for _ in range(3):
        try:
            r.eval(_REQUEUE_LUA, 2, PROC_LIST, QUEUE, job_id)
        except Exception:
            time.sleep(0.5)
            continue
        try:
            r.hset(f"ocr:job:{job_id}", "status", "pending")
            r.hdel(f"ocr:job:{job_id}", "processing_since")
        except Exception:
            pass
        return
    print(
        f"[worker {WORKER_ID}] FATAL: cannot persist nor requeue job {job_id}; "
        f"exiting so reaper recovers it after heartbeat TTL",
        flush=True,
    )
    os._exit(2)


def _finalize(job_key: str, jid: str, path: "str | None", mapping: dict) -> bool:
    """把终态写入 Redis；返回是否落库成功。

    顺序：落库(重试3次) → LREM → **仅当 LREM 确实移除成功才**删文件。
    - 落库失败：返回 False（保留文件与 processing 记录），由调用方 _requeue。
    - LREM 失败/未移除：**保留文件** —— 否则任务日后被重投时会因"文件不存在"报错，
      并覆盖掉原本成功的结果。
    """
    for _ in range(3):
        try:
            r.hset(job_key, mapping=mapping)
            r.expire(job_key, JOB_TTL)
            break
        except Exception:
            time.sleep(0.5)
    else:
        return False  # 落库失败

    try:
        removed = r.lrem(PROC_LIST, 1, jid)
    except Exception:
        return True  # 落库成功但无法确认移除：保留文件，绝不删
    if removed:  # 确认已移出 processing，才安全删文件
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
        _try_lrem(job_id)
        return

    # 已经成功过（上次落库后没来得及移出就被重投）：只清理，不重算、绝不改成 error
    if job.get("status") == "done":
        _try_lrem(job_id, os.path.join(TMPDIR, job_id + job.get("suffix", ".png")))
        return

    suffix = job.get("suffix", ".png")
    max_pages = int(job.get("max_pages", "0") or "0")
    path = os.path.join(TMPDIR, job_id + suffix)

    if not os.path.exists(path):
        if not _finalize(job_key, job_id, None, {"status": "error", "error": "file missing (worker restarted?)"}):
            _requeue(job_id)
        return

    try:
        r.hset(job_key, mapping={"status": "processing", "processing_since": str(time.time())})
        t0 = time.time()
        result = ocr_engine.run_ocr(path, max_pages)
        elapse = round(time.time() - t0, 3)
    except Exception as exc:  # noqa: BLE001
        if not _finalize(job_key, job_id, path, {"status": "error", "error": str(exc)}):
            _requeue(job_id)  # 连 error 都写不进 Redis → 重投，别让任务卡死
        return

    # OCR 成功：落库失败 → 重投（而不是留在 processing 里卡死）
    ok = _finalize(
        job_key,
        job_id,
        path,
        {
            "status": "done",
            "result": json.dumps(result, ensure_ascii=False),
            "elapse": str(elapse),
        },
    )
    if not ok:
        _requeue(job_id)


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
