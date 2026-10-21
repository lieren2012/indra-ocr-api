"""
Indra OCR API 服务（v2.0 —— 队列解耦版）

职责（极轻量，不加载 OCR 模型）：
  - 接收上传 → 落盘到共享 tmp → 写 Redis 任务哈希 + 入队 → 立即返回 job_id
  - 提供任务状态/结果查询
  - 鉴权（按 API Key）+ 背压（队列过长直接 429，保护系统）

OCR 计算完全在 Worker 里跑，本服务不受 OCR 阻塞，可自由横向扩副本。

接口：
  GET  /healthz             健康检查（Redis 连通性、在线 Worker 数、排队数）
  POST /v1/jobs             提交任务（multipart: file, max_pages=0 不限）
  GET  /v1/jobs/{job_id}    查询状态/结果（pending/processing/done/error）

环境变量：
  REDIS_URL        Redis 连接（默认 redis://redis:6379/0）
  INDRA_API_KEY    非空则启用鉴权（请求头 X-API-Key 或 ?api_key=）
  TMPDIR           上传文件落盘目录（默认 /tmp/indra，须为 API 与 Worker 共享卷）
  MAX_QUEUE        队列上限，超过返回 429（默认 1000）
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import redis
from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi import Header

# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
INDRA_API_KEY = os.getenv("INDRA_API_KEY", "")        # 空字符串 = 不鉴权
TMPDIR = os.getenv("TMPDIR", "/tmp/indra")
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "1000"))
QUEUE = "ocr:queue"
JOB_TTL = int(os.getenv("JOB_TTL", "86400"))          # 任务完成后再保留 24h

Path(TMPDIR).mkdir(parents=True, exist_ok=True)

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="Indra OCR API", version="2.0.0")


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
# 健康检查
# --------------------------------------------------------------------------- #
@app.get("/healthz")
def healthz() -> dict:
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    workers = 0
    queued = -1
    if redis_ok:
        try:
            workers = len(r.keys("ocr:worker:*"))
            queued = r.llen(QUEUE)
        except Exception:
            pass
    return {
        "ok": redis_ok,
        "redis": redis_ok,
        "active_workers": workers,
        "queued": queued,
        "max_queue": MAX_QUEUE,
    }


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
    qlen = r.llen(QUEUE)
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
    return {"job_id": job_id, "status": "pending", "queue_position": qlen + 1}


# --------------------------------------------------------------------------- #
# 查询任务
# --------------------------------------------------------------------------- #
@app.get("/v1/jobs/{job_id}")
def get_job(
    job_id: str,
    _: None = Depends(require_auth),
) -> dict:
    data = r.hgetall(f"ocr:job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="job not found")
    status = data.get("status")
    resp: dict = {"job_id": job_id, "status": status}
    if status == "done":
        import json

        resp["result"] = json.loads(data.get("result", "{}"))
        if data.get("elapse"):
            resp["elapse"] = float(data["elapse"])
    elif status == "error":
        resp["error"] = data.get("error")
    elif status == "processing":
        resp["queue_position"] = None
    return resp
