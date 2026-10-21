"""
Indra OCR 引擎封装（被 Worker 进程加载）

只负责"加载模型 + 跑一次 OCR"，不碰网络、不碰队列。
API 服务不需要加载模型——模型只在 Worker 里常驻，这是解耦的关键：
API 可以随便横向扩，模型算力集中在 Worker 池。
"""

from __future__ import annotations

import indra_ocr

# 每个 Worker 进程一个全局模型句柄（启动时加载一次，常驻内存）
_conv: "indra_ocr.Converter | None" = None


def load_model(threads: int) -> None:
    global _conv
    # mode="ocr" 永不读取 PDF 文本层，保证扫描件一律走真正的 OCR。
    # 若希望带文本层的 PDF 优先用文本层（更快），把 mode 改为 "auto"。
    _conv = indra_ocr.load(threads=threads, mode="ocr")


def run_ocr(path: str, max_pages: int) -> dict:
    """对单个文件跑 OCR，返回 {pages:[...]}。max_pages<=0 表示不限制。"""
    if _conv is None:
        raise RuntimeError("model not loaded")
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
