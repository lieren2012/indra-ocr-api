"""
Indra OCR API 服务（v2.1 —— 队列解耦 + 可靠队列 + 看门狗）

职责（极轻量，不加载 OCR 模型）：
  - 接收上传 → 落盘到共享 tmp → 写 Redis 任务哈希 + 入队 → 立即返回 job_id
  - 提供任务状态/结果查询
  - 鉴权（按 API Key）+ 背压（队列过长直接 429，保护系统）
  - 后台看门狗：把"已取出但 Worker 已死"的任务重新入队，避免丢任务

接口：
  GET  /healthz             存活检查（Redis 连通 + 在线 Worker 数，ok 字段如实反映）
  GET  /readyz              就绪检查（Redis 连通且至少 1 个 Worker，否则 503）
  POST /v1/jobs             提交任务（multipart: file, max_pages=0 不限）
  GET  /v1/jobs/{job_id}    查询状态/结果（pending/processing/done/error）

环境变量：
  REDIS_URL        Redis 连接（默认 redis://redis:6379/0）
  INDRA_API_KEY    非空则启用鉴权（请求头 X-API-Key 或 ?api_key=）
  TMPDIR           上传文件落盘目录（默认 /tmp/indra，须为 API 与 Worker 共享卷）
  MAX_QUEUE        队列上限，超过返回 429（默认 1000）
  JOB_TTL          任务完成后结果保留秒数（默认 86400）
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

import redis
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, UploadFile

# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
INDRA_API_KEY = os.getenv("INDRA_API_KEY", "")        # 空字符串 = 不鉴权
TMPDIR = os.getenv("TMPDIR", "/tmp/indra")
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "1000"))
JOB_TTL = int(os.getenv("JOB_TTL", "86400"))

QUEUE = "ocr:queue"
WORKERS_SET = "ocr:workers"          # 所有注册过的 worker id（持久集合）
REAPER_LOCK = "ocr:reaper:lock"      # 看门狗互斥锁（多 API 副本时只让一个干活）
REAPER_INTERVAL = int(os.getenv("REAPER_INTERVAL", "30"))

Path(TMPDIR).mkdir(parents=True, exist_ok=True)

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="Indra OCR API", version="2.1.0")


# --------------------------------------------------------------------------- #
# 健康 / 就绪
# --------------------------------------------------------------------------- #
def _snapshot() -> dict:
    redis_ok = True
    try:
        r.ping()
    except Exception:
        redis_ok = False

    workers = -1
    queued = -1
    if redis_ok:
        try:
            workers = len(r.keys("ocr:worker:*"))   # 只统计未过期的在线心跳
            queued = r.llen(QUEUE)
        except Exception:
            pass

    # ok 必须同时满足：Redis 通 + 至少 1 个在线 Worker
    ok = redis_ok and workers > 0
    return {
        "ok": ok,
        "redis": redis_ok,
        "active_workers": workers,
        "queued": queued,
        "max_queue": MAX_QUEUE,
    }


@app.get("/healthz")
def healthz() -> dict:
    # 存活探针：始终 200，但 ok 字段如实反映能否真正处理任务（worker=0 → false）
    return _snapshot()


@app.get("/readyz")
def readyz() -> dict:
    snap = _snapshot()
    if not snap["ok"]:
        raise HTTPException(status_code=503, detail=snap)
    return snap


# --------------------------------------------------------------------------- #
# 看门狗：把死掉 Worker 的 processing 列表重新入队，避免丢任务
# --------------------------------------------------------------------------- #
# 原子地把某个死 worker 的 processing 列表整体挪回队列：
# LMOVE 逐个搬本身是原子的，整个 Lua 脚本又在 Redis 内单线程执行，
# 因此不存在"搬了一半"的中间可见状态，也就不会产生重复任务。
_REAP_LUA = """
local plist = KEYS[1]
local queue = KEYS[2]
local ids = {}
while true do
  local jid = redis.call('LMOVE', plist, queue, 'RIGHT', 'LEFT')
  if not jid then break end
  ids[#ids+1] = jid
end
return ids
"""


def _reap_once() -> None:
    # 多 API 副本时用 Redis 锁保证同一时刻只有一个在看门
    if not r.set(REAPER_LOCK, "1", nx=True, ex=30):
        return
    try:
        for wid in list(r.smembers(WORKERS_SET)):
            if r.exists(f"ocr:worker:{wid}"):
                continue  # 心跳还在 → 活着，跳过
            plist = f"ocr:processing:{wid}"
            try:
                moved = r.eval(_REAP_LUA, 2, plist, QUEUE) or []
            except Exception:
                continue  # 这段失败就下次再试，绝不留下"半搬"的重复任务
            for jid in moved:
                try:
                    # 状态重置失败无妨：任务已在队列，Worker 处理时会覆盖为 processing
                    r.hset(f"ocr:job:{jid}", "status", "pending")
                    r.hdel(f"ocr:job:{jid}", "processing_since")
                except Exception:
                    pass
            try:
                r.srem(WORKERS_SET, wid)
            except Exception:
                pass
    finally:
        try:
            r.delete(REAPER_LOCK)
        except Exception:
            pass


def _reaper_loop() -> None:
    while True:
        time.sleep(REAPER_INTERVAL)
        try:
            _reap_once()
        except Exception as exc:  # noqa: BLE001
            print(f"[reaper] error: {exc}", flush=True)


@app.on_event("startup")
def _startup() -> None:
    threading.Thread(target=_reaper_loop, daemon=True).start()


# --------------------------------------------------------------------------- #
# 鉴权（请求头 X-API-Key 或查询参数 api_key，POST/GET 都可用）
# --------------------------------------------------------------------------- #
def require_auth(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    api_key: str | None = Query(None),
) -> None:
    if not INDRA_API_KEY:
        return
    if (x_api_key or api_key) != INDRA_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


# --------------------------------------------------------------------------- #
# 提交任务（立即返回 job_id，不阻塞 OCR）
# --------------------------------------------------------------------------- #
@app.post("/v1/jobs")
async def submit(
    file: UploadFile = File(...),
    max_pages: int = Query(0, description="0 = 不限制页数"),
    _: None = Depends(require_auth),
) -> dict:
    # 背压：队列过长直接拒绝，保护系统不被冲垮
    try:
        qlen = r.llen(QUEUE)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}") from exc
    if qlen >= MAX_QUEUE:
        raise HTTPException(
            status_code=429,
            detail=f"queue full (length={qlen}, limit={MAX_QUEUE}); retry later",
        )

    # 文件名强制 ASCII（uuid4().hex），规避 indra 中文路径崩溃
    ext = os.path.splitext(file.filename or "")[1].lower() or ".png"
    job_id = uuid.uuid4().hex
    tmp_path = os.path.join(TMPDIR, job_id + ext)
    data = await file.read()
    with open(tmp_path, "wb") as f:
        f.write(data)

    try:
        r.hset(
            f"ocr:job:{job_id}",
            mapping={
                "status": "pending",
                "filename": file.filename or "",
                "suffix": ext,
                "max_pages": str(max_pages),
                "created_at": str(time.time()),
            },
        )
        r.rpush(QUEUE, job_id)
    except redis.RedisError as exc:
        # 入队失败（如 Redis 内存满 noeviction）：删掉刚落盘的文件，返回 503 让调用方重试
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise HTTPException(status_code=503, detail=f"queue write failed: {exc}") from exc

    return {"job_id": job_id, "status": "pending", "queue_position": qlen + 1}


# --------------------------------------------------------------------------- #
# 查询任务
# --------------------------------------------------------------------------- #
@app.get("/v1/jobs/{job_id}")
def get_job(
    job_id: str,
    _: None = Depends(require_auth),
) -> dict:
    try:
        data = r.hgetall(f"ocr:job:{job_id}")
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}") from exc
    if not data:
        raise HTTPException(status_code=404, detail="job not found")
    status = data.get("status")
    resp: dict = {"job_id": job_id, "status": status}
    if status == "done":
        resp["result"] = json.loads(data.get("result", "{}"))
        if data.get("elapse"):
            resp["elapse"] = float(data["elapse"])
    elif status == "error":
        resp["error"] = data.get("error")
    return resp
