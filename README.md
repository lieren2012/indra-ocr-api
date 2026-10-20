# Indra OCR API 服务（Docker 部署）

> [Indra OCR](https://github.com/datalab-to/indra)（datalab-to 出品，纯 CPU 轻量 OCR）的 HTTP API 封装。FastAPI + Docker 一键部署，供知识库等系统调用。
> **许可证**：本项目代码 Apache 2.0（见 `LICENSE`）；Indra 模型权重为修改版 OpenRAIL-M，商用前请阅 [条款](https://huggingface.co/datalab-to/indra-ocr)。
> **实测吞吐**：单页 0.5–2.0s（纯 CPU，无需 GPU）。快速部署见下方 §3 / §4；离线模型预置见 `scripts/prepare_models.sh`。

---

# Indra OCR 服务部署说明

把 `indra-ocr 1.1.2` 包装成 HTTP API，供知识库系统调用。
实测每页 0.5–2.0s（纯 CPU，无需 GPU）。

---

## 0. 你的负载与配置结论

**实际负载（已确认）**：每天 ≤ 200 个文档（平均 ~20 页）→ 峰值 ~4000 页/天 ≈ **0.14 页/秒平均**。
按此负载，连 1 个 CPU 核心都用不满。因此：

- **不需要队列**（Redis/RQ）—— 同步接口 + Nginx 限流即可；异步接口仅作大文件兜底。
- **不需要负载均衡** —— 单机 32 核，多 worker 由 OS 调度，无需额外 LB。
- **推荐配置**：8 核 / 12GB 限制 + 单 Uvicorn worker + `OCR_THREADS=8`。
  你那台服务器有富余，分配 8 核给 OCR 对现有业务无感。

---

## 1. 目录结构

```
indra-ocr-service/
├── app.py                  # FastAPI 应用
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── nginx.conf
├── scripts/prepare_models.sh   # 联网机预置模型（只需跑一次）
├── models/                 # 离线模型（预置后由 compose 挂载，体积 ~72MB）
└── tmp/                    # 临时上传文件（务必放数据盘）
```

---

## 2. 离线预置模型（内网服务器必须）

在有外网的机器上：

```bash
cd indra-ocr-service
bash scripts/prepare_models.sh ./models
# 模型缓存到 ./models，约 72MB
```

把整个 `models/` 目录拷到内网服务器项目目录下，compose 会只读挂载为 `/opt/models`。

> ⚠️ 若跳过此步，`HF_HUB_OFFLINE=1` 会让服务启动即因无法下载模型而失败。
> 离线环境务必确认 `models/hub/models--datalab-to--indra-ocr` 存在。

---

## 3. 部署（Docker，推荐）

```bash
cd indra-ocr-service
# 可选：设置 API Key 鉴权（空则不鉴权）
export INDRA_API_KEY="你的密钥"

# 构建并启动
docker compose up -d --build

# 查看健康
curl http://localhost:8080/healthz
# 期望: {"ok":true,"model_ready":true,...}
```

**磁盘注意**：Docker 默认 `data-root` 在 `/var/lib/docker`（根分区）。
若根分区紧张（如 / 只剩 40GB），把 `tmp/` 与 compose 的镜像数据迁到数据盘
（如 `/www/indra/`），并配置 Docker `data-root` 到数据盘。

---

## 4. 裸机部署（无 Docker 时，Ubuntu）

```bash
cd indra-ocr-service
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# 模型目录（已预置）
export HF_HOME=/opt/indra-models
export HF_HUB_OFFLINE=1
export OCR_THREADS=8
export TMPDIR=/www/indra/tmp   # 放数据盘

# 后台启动（systemd / nohup 自行封装）
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1
```

systemd 单元示例（`/etc/systemd/system/indra-ocr.service`）：

```ini
[Unit]
Description=Indra OCR API
After=network.target

[Service]
WorkingDirectory=/opt/indra-ocr-service
Environment=HF_HOME=/opt/indra-models
Environment=HF_HUB_OFFLINE=1
Environment=OCR_THREADS=8
Environment=TMPDIR=/www/indra/tmp
ExecStart=/opt/indra-ocr-service/.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000 --workers 1
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

> Windows 部署注意：`usable_cores()` 在 Windows 退化为 `os.cpu_count()`，
> 不感知 CPU 限制，需手动设 `OCR_THREADS`；用 NSSM 或计划任务做进程守护。

---

## 5. 调用示例

**同步（图片 / ≤10 页 PDF）**

```bash
curl -F "file=@扫描件.png" http://localhost:8080/v1/ocr
# 带鉴权：
curl -F "file=@doc.pdf" -F "api_key=你的密钥" http://localhost:8080/v1/ocr
```

返回：
```json
{
  "pages": [
    {
      "index": 0,
      "text": "识别文本",
      "markdown": "# 标题\n\n正文…",
      "lines": [{"text": "…", "box": [x0,y0,x1,y1], "conf": 0.93}]
    }
  ],
  "elapse": 1.42
}
```

**异步（大 PDF / 批量）**

```bash
TID=$(curl -s -F "file=@book.pdf" http://localhost:8080/v1/ocr/async | python -c "import sys,json;print(json.load(sys.stdin)['task_id'])")
curl http://localhost:8080/v1/tasks/$TID
```

> **批量导入建议（如半小时内传完 100 文档）**
> - 调用方**走异步接口** `/v1/ocr/async` 而非同步等待，避免前端 hang 30 分钟；
>   传完拿到 `task_id` 后轮询 `/v1/tasks/{id}` 即可。
> - 客户端**并发上传控制在 8–16 个**以内（与服务端 `limit_conn 16` 对齐），
>   不要一次性开 100 个并发连接。
> - 单 worker + 进程内队列即可扛住（~4–8 页/秒），**无需 Redis 等外部队列**；
>   除非服务在处理途中重启（进行中任务丢失），由调用方按 `task_id` 重试补偿。

---

## 6. 接口一览

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 健康检查 |
| POST | `/v1/ocr` | 同步 OCR（multipart: file, max_pages=10） |
| POST | `/v1/ocr/async` | 异步 OCR（返回 task_id） |
| GET | `/v1/tasks/{id}` | 查询异步任务结果 |

---

## 7. 运维要点

1. **临时文件清理**：`app.py` 每次请求 `finally` 删除；另加 cron 兜底删除 1 小时前的文件：
   `0 * * * * find /www/indra/tmp -type f -mmin +60 -delete`
2. **限流**：Nginx `limit_req rate=10r/s burst=20`，防止突发打爆内存。
3. **监控**：`/healthz` 探活 + P95 延迟（实际远低于瓶颈）。
4. **合规**：Indra 权重为修改版 OpenRAIL-M，营收 >2000 万美元或与 Datalab API 竞争不可用，部署前确认。

---

## 8. 已知坑

| 坑 | 规避 |
|---|---|
| 中文路径崩溃 | 临时文件用 `uuid4().hex` 纯 ASCII 文件名；**且 `TMPDIR` 目录路径本身也不能含中文**（实测：cv2.imread 连中文目录路径也读不了，会 500） |
| 内网无法下载模型 | 预置 `HF_HOME` + `HF_HUB_OFFLINE=1` |
| 大 PDF 同步超时 | 超过 10 页走异步接口 |
| 多 worker 任务状态丢失 | 单 worker 即可；多 worker 需改 Redis |
| 根分区被 Docker 写满 | data-root / 临时目录迁到数据盘 |

---

## 9. 官方资源与免责提醒

- **GitHub**：https://github.com/datalab-to/indra
- **官网博客（发布说明）**：https://datalab.to/blog/indra-ocr
- **HuggingFace 模型**：https://huggingface.co/datalab-to/indra-ocr
- 出品方 **Datalab**（datalab.to，做 Marker/Surya/pdftext 的文档智能公司，可信）

**能力边界（来自官方说明，部署前务必核对）**：
- ✅ 简繁中文扫描件 OCR 支持；❌ 阿拉伯文/日文假名/韩文/泰文等扫描件不支持（仅能从数字 PDF 文本层读取）
- ❌ 手写体、竖排文字、复杂表格（多级表头/合并单元格/无框线）会丢结构
- 许可：代码 Apache 2.0，**模型权重为修改版 OpenRAIL-M**（营收/融资 < $20M 免费，不可与 Datalab API 直接竞争）

> 注：官方自报 "32 核 49 页/秒" 为自建 benchmark（OmniParseBench）且排除模型加载/warmup。
> 实际部署按单页 0.5–2s 规划更稳妥（本机实测单页 threads=4 约 0.5s）。

---

## 10. 同类 OCR 产品（参考）

| 项目 | GitHub | 定位 |
|---|---|---|
| **Indra**（本仓库封装） | https://github.com/datalab-to/indra | 纯 onnxruntime 轻量 CPU OCR，中文扫描件友好，无 PyTorch/PaddlePaddle 依赖 |
| RapidOCR | https://github.com/RapidAI/RapidOCR | 轻量端侧推理（PP-OCR 系列），快，但无版面块/表格结构 |
| PaddleOCR | https://github.com/PaddlePaddle/PaddleOCR | 完整版含 PP-Structure，表格/竖排还原强；代价是需 PaddlePaddle 框架（~500MB+） |

> 三者共同短板：手写体、竖排文字、复杂表格（合并单元格/多级表头/无框线）会丢结构，需上 VLM（如 PaddleOCR-VL / Qwen2.5-VL）补刀。

