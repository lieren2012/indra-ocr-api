"""
Indra OCR HTTP API
==================

把 indra-ocr 1.1.2 包装成 HTTP 服务，供知识库系统调用。

设计要点（均为实测踩坑后的结论）：
1. 临时文件必须落 ASCII 路径 —— indra 内部 cv2.imread 读不了中文路径，会抛 InputError。
2. conv.convert 是 CPU 密集且只接受文件路径 —— 用 asyncio.to_thread 包住，避免阻塞事件循环。
3. 临时文件必须清理 —— finally 删除，另配 cron 兜底。
4. 单 Uvicorn worker + OCR_THREADS 内部并行即可满足低负载；多 worker 需配 Redis 共享任务状态。

接口：
  GET  /healthz                 健康检查（模型是否就绪、线程数、临时目录）
  POST /v1/ocr                  同步：图片或 <=max_pages 的 PDF（multipart 上传）
  POST /v1/ocr/async            异步：大文件，返回 task_id
  GET  /v1/tasks/{task_id}      查询异步任务结果/进度

环境变量：
  INDRA_API_KEY   非空则启用 API Key 鉴权（请求体或 header 带 api_key=）
  OCR_THREADS     onnxruntime 线程数（默认 8）
  TMPDIR          临时文件与上传落盘目录（默认 /tmp/indra）
  HF_HOME         huggingface 模型缓存目录（离线部署指向预置目录）
  HF_HUB_OFFLINE  设为 1 强制离线，禁止联网下载
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
import uuid
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile

import indra_ocr

# --------------------------------------------------------------------------- #
# 配置
# --------------------------------------------------------------------------- #
API_KEY = os.getenv("INDRA_API_KEY", "")           # 空字符串 = 不鉴权
OCR_THREADS = int(os.getenv("OCR_THREADS", "8"))
TMPDIR = os.getenv("TMPDIR", "/tmp/indra")
Path(TMPDIR).mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Indra OCR API", version="1.0.0")

# 全局模型句柄（每个 worker 进程启动时加载一次，常驻内存）
_conv: "indra_ocr.Converter | None" = None


# --------------------------------------------------------------------------- #
# 启动 / 健康检查
# --------------------------------------------------------------------------- #
@app.on_event("startup")
def _boot() -> None:
    global _conv
    # mode="ocr" 永不读取 PDF 文本层，保证知识库扫描件一律走真正的 OCR。
    # 若希望带文本层的 PDF 优先用文本层（更快），把 mode 改为 "auto"。
    _conv = indra_ocr.load(threads=OCR_THREADS, mode="ocr")


def _model_ready() -> bool:
    return _conv is not None


@app.get("/healthz")
def healthz() -> dict:
    return {
        "ok": _model_ready(),
        "model_ready": _model_ready(),
        "threads": OCR_THREADS,
        "tmpdir": TMPDIR,
        "api_auth": bool(API_KEY),
    }


# --------------------------------------------------------------------------- #
# 鉴权
# --------------------------------------------------------------------------- #
def _check_auth(api_key: str | None = Form(None)) -> None:
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


# --------------------------------------------------------------------------- #
# 文件落盘（强制 ASCII 路径，规避中文路径 cv2 崩溃）
# --------------------------------------------------------------------------- #
def _save_ascii(data: bytes, suffix: str) -> str:
    # uuid4().hex 是纯 ASCII，彻底绕开中文/空格/特殊字符导致的路径问题
    path = os.path.join(TMPDIR, uuid.uuid4().hex + suffix)
    with open(path, "wb") as f:
        f.write(data)
    return path


# --------------------------------------------------------------------------- #
# 核心 OCR（在独立线程中执行）
# --------------------------------------------------------------------------- #
def _run_ocr(path: str, max_pages: int) -> dict:
    pages = []
    for idx, page in _conv.convert(path, max_pages=max_pages):
        lines = [
            {"text": ln.text, "box": ln.box, "conf": float(ln.conf)}
            for ln in page.lines
        ]
        pages.append(
            {
                "index": idx,
                "text": page.text,
                "markdown": page.markdown,
                "lines": lines,
            }
        )
    return {"pages": pages}


# --------------------------------------------------------------------------- #
# 同步接口
# --------------------------------------------------------------------------- #
@app.post("/v1/ocr")
async def ocr(
    file: UploadFile = File(...),
    max_pages: int = Form(10),
    api_key: str | None = Depends(_check_auth),
) -> dict:
    if not _model_ready():
        raise HTTPException(status_code=503, detail="model not ready")

    ext = os.path.splitext(file.filename or "")[1].lower() or ".png"
    tmp_path = _save_ascii(await file.read(), ext)
    t0 = time.perf_counter()
    try:
        result = await asyncio.to_thread(_run_ocr, tmp_path, max_pages)
        result["elapse"] = round(time.perf_counter() - t0, 3)
        return result
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# --------------------------------------------------------------------------- #
# 异步接口（大 PDF / 批量，避免同步超时）
# 说明：单 worker 下用进程内字典存任务状态即可；多 worker 部署时
#       应改用 Redis / 共享存储，否则任务状态只存在于发起请求的那个 worker。
# --------------------------------------------------------------------------- #
_tasks: dict[str, dict] = {}
_tasks_lock = threading.Lock()


def _cleanup_old_tasks() -> None:
    # 防止字典无限增长（单机低负载，任务量极小，这里只是兜底）
    with _tasks_lock:
        if len(_tasks) > 1000:
            for k in list(_tasks)[:500]:
                _tasks.pop(k, None)


@app.post("/v1/ocr/async")
async def ocr_async(
    file: UploadFile = File(...),
    max_pages: int = Form(0),
    api_key: str | None = Depends(_check_auth),
) -> dict:
    if not _model_ready():
        raise HTTPException(status_code=503, detail="model not ready")

    ext = os.path.splitext(file.filename or "")[1].lower() or ".png"
    tmp_path = _save_ascii(await file.read(), ext)
    task_id = uuid.uuid4().hex

    with _tasks_lock:
        _tasks[task_id] = {"status": "pending", "result": None, "error": None}

    def _worker() -> None:
        try:
            result = _run_ocr(tmp_path, max_pages)
            with _tasks_lock:
                _tasks[task_id] = {"status": "done", "result": result, "error": None}
        except Exception as exc:  # noqa: BLE001
            with _tasks_lock:
                _tasks[task_id] = {"status": "error", "error": str(exc), "result": None}
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    threading.Thread(target=_worker, daemon=True).start()
    _cleanup_old_tasks()
    return {"task_id": task_id, "status": "pending"}


@app.get("/v1/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    with _tasks_lock:
        task = _tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task
