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


# 重投：一个 Lua 里完成「看状态 → 置 pending → 入队 → 移出 processing」。
# 必须原子，否则会出现这两类覆盖竞争：
#   - 先入队再改状态：别的 worker 抢先算完写了 done，本 worker 又把状态改回 pending；
#   - 改状态和入队分两次网络往返：任意一步失败就留下"状态 pending 但不在队列"的僵尸任务。
# 因此：
#   - 任务已是 done（别人已经算完）→ 只清理，绝不回退状态；
#   - 任务哈希已过期 → 直接丢弃；
#   - 其余 → **先置 pending 再入队**，取到它的 worker 看到的已经是 pending。
_REQUEUE_LUA = """
local plist = KEYS[1]
local queue = KEYS[2]
local key   = KEYS[3]
local jid   = ARGV[1]
if redis.call('EXISTS', key) == 0 then
  redis.call('LREM', plist, 1, jid)
  return 0
end
if redis.call('HGET', key, 'status') == 'done' then
  redis.call('LREM', plist, 1, jid)
  return 0
end
redis.call('HSET', key, 'status', 'pending')
redis.call('HDEL', key, 'processing_since')
redis.call('RPUSH', queue, jid)
redis.call('LREM', plist, 1, jid)
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


def _requeue(job_id: str, strict: bool = True) -> bool:
    """安全重投。返回是否成功。

    strict=True（正常处理路径）：重试 3 次仍失败 → 结束进程，让心跳过期、
      看门狗按 processing 列表兜底回收。绝不"什么都不做就继续跑"。
    strict=False（优雅退出时）：失败就返回 False，由调用方保留注册信息，
      交看门狗处理（此时不能自杀，否则列表会没人管）。
    """
    for _ in range(3):
        try:
            r.eval(_REQUEUE_LUA, 3, PROC_LIST, QUEUE, f"ocr:job:{job_id}", job_id)
            return True
        except Exception:
            time.sleep(0.5)
    if strict:
        print(
            f"[worker {WORKER_ID}] FATAL: cannot requeue job {job_id}; "
            f"exiting so reaper recovers it after heartbeat TTL",
            flush=True,
        )
        os._exit(2)
    return False


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

    # 读任务状态。读失败**不能**直接 return：心跳还在，看门狗不会回收，
    # 任务就永远卡在这个 processing 列表里（这正是最容易漏的一类死锁）。
    # 改为重投——重投脚本自己在 Redis 里判断状态，done 的只清理、不会重算。
    job = None
    for _ in range(2):
        try:
            job = r.hgetall(job_key)
            break
        except Exception:
            time.sleep(0.3)
    if job is None:
        _requeue(job_id)
        return

    if not job:
        _try_lrem(job_id)   # 任务已过期/不存在，清掉 processing 记录即可
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

    # 先标记 processing。这一步写不进去说明 Redis 有问题 —— 重投，而不是
    # 继续跑并在 OCR 之后把本可能成功的任务标成 error。
    try:
        r.hset(job_key, mapping={"status": "processing", "processing_since": str(time.time())})
    except Exception:
        _requeue(job_id)
        return

    try:
        t0 = time.time()
        result = ocr_engine.run_ocr(path, max_pages)
        elapse = round(time.time() - t0, 3)
    except Exception as exc:  # noqa: BLE001
        if not _finalize(job_key, job_id, path, {"status": "error", "error": str(exc)}):
            _requeue(job_id)
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
    """优雅退出：把本 worker 未处理完的任务逐个原子重投，再注销自己。

    若有任务没能安全重投，**不注销、不删心跳**：心跳过期后看门狗会按
    processing 列表兜底。否则列表会变成没人认领的孤儿。
    """
    try:
        jobs = r.lrange(PROC_LIST, 0, -1)
    except Exception:
        return  # Redis 都读不到，什么都别动，交看门狗

    all_ok = True
    for jid in jobs:
        if not _requeue(jid, strict=False):
            all_ok = False
    if not all_ok:
        print(
            f"[worker {WORKER_ID}] shutdown: some jobs could not be requeued; "
            f"leaving them for reaper",
            flush=True,
        )
        return

    try:
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
