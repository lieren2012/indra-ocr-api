# Indra OCR API 服务（Docker 部署 · v2.0 队列解耦版）

> [Indra OCR](https://github.com/datalab-to/indra)（datalab-to 出品，纯 CPU 轻量 OCR）的 HTTP API 封装。FastAPI + Redis + Worker 队列，供知识库等系统调用。
> **许可证**：本项目代码 Apache 2.0（见 `LICENSE`）；Indra 模型权重为修改版 OpenRAIL-M，商用前请阅 [条款](https://huggingface.co/datalab-to/indra-ocr)。
> **实测吞吐**：单页 0.5–2.0s（纯 CPU，无需 GPU）。快速部署见 §3；离线模型预置见 `scripts/prepare_models.sh`。

---

## 0. 这版为什么改架构

v1 是单进程内字典存任务——在你的原始约束（≤200 文档/天、单一内部调用、内网）下够用。但一旦**公开暴露**或面临**突发/批量高吞吐**，就必须解耦：

- **公开** = 不可信外部调用 → 需要鉴权 + 限流 + 背压，否则谁都能打爆你。
- **突发/批量**（如半小时内涌入几万文档）→ ingestion 和 processing 必须分开，否则客户端 hang 或内存雪崩。

v2 把服务拆成三层，**API 只收任务立即返回 `job_id`，OCR 计算全在 Worker 池跑**：

```
Nginx (限流+鉴权+背压)
   │
   ▼
API   (极轻量，不加载模型)：收上传 → 落共享盘 → 写 Redis 任务+入队 → 立即返回 job_id
   │                                  ▲
   │ 队列 ocr:queue                   │ 结果写回任务哈希
   ▼                                  │
Worker 池 (加载模型，吃算力)：BRPOP 取任务 → 跑 OCR → 存结果 → 删文件
```

**这套单机也能跑（比原来稳得多），要更高吞吐就加 Worker 副本 / 机器。**

> ⚠️ **吞吐实话**：队列管理负载，但不创造算力。单机 32 核满打满算约 **~1 万文档/小时**到顶；要 3 万+/小时需多机跑 Worker 横向扩容（见 §4 扩容）。

---

## 1. 目录结构

```
indra-ocr-service/
├── api.py                  # API 服务（收任务、查状态、鉴权、背压）
├── worker.py               # Worker（从队列取任务跑 OCR）
├── ocr_engine.py           # OCR 引擎封装（被 Worker 加载，模型常驻）
├── requirements.txt
├── Dockerfile              # 同一镜像，靠启动命令区分 api / worker
├── docker-compose.yml      # redis + api + worker(replicas) + nginx
├── nginx.conf
├── scripts/prepare_models.sh   # 联网机预置模型（只需跑一次）
├── models/                 # 离线模型（预置后挂载，~72MB）
└── tmp/                    # 上传文件共享盘（api 写、worker 读，务必放数据盘）
```

---

## 2. 离线预置模型（内网服务器必须）

在有外网的机器上：

```bash
cd indra-ocr-service
bash scripts/prepare_models.sh ./models
# 模型缓存到 ./models，约 72MB
```

把整个 `models/` 目录拷到内网服务器项目目录下，Worker 会只读挂载为 `/opt/models`。

> ⚠️ 若跳过此步，`HF_HUB_OFFLINE=1` 会让 Worker 启动即因无法下载模型而失败。

---

## 3. 部署（Docker，推荐）

```bash
cd indra-ocr-service
# 公开部署务必设置 API Key
export INDRA_API_KEY="你的密钥"

# 构建并启动（redis + api + 4 个 worker + nginx）
docker compose up -d --build

# 健康检查
curl http://localhost:8080/healthz
# 期望: {"ok":true,"redis":true,"active_workers":4,"queued":0,...}
```

**磁盘注意**：Docker 默认 `data-root` 在 `/var/lib/docker`（根分区）。
若根分区紧张，把 `tmp/`、`redis-data/` 与镜像数据迁到数据盘（如 `/www/indra/`），并配置 Docker `data-root` 到数据盘。

---

## 4. 横向扩容（应对高吞吐）

吞吐瓶颈在 **Worker 算力**，不在 API/Redis。扩容只看 Worker：

- **同机多副本**：`docker compose up -d --scale worker=N`（N = floor(机器核数 / OCR_THREADS)）。
  例：32 核、`OCR_THREADS=8` → 同时跑 4 个 Worker 副本。
- **跨机集群**：在更多机器上跑 Worker 容器，指向**同一个 Redis**（Redis 部署在内网可达处；文件共享盘换成对象存储 / NFS）。
  API 是无状态的，也可多机部署在 Redis 前。
- **队列做天然背压**：突发洪峰时任务在 Redis 里堆积，API 照常秒回 `job_id`；队列超过 `MAX_QUEUE`（默认 1000）才对客户端返回 `429`，保护系统不被冲垮。

> Worker 副本数 × `OCR_THREADS` 不应超过机器总核数，否则上下文切换反而掉速。

---

## 5. 调用示例

**提交任务（立即返回 job_id）**

```bash
JID=$(curl -s -F "file=@扫描件.png" -F "api_key=你的密钥" \
  http://localhost:8080/v1/jobs | python -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
echo "job: $JID"
```

**轮询结果**

```bash
curl "http://localhost:8080/v1/jobs/$JID?api_key=你的密钥"
# pending   → {"job_id":...,"status":"pending","queue_position":3}
# processing→ {"job_id":...,"status":"processing"}
# done      → {"job_id":...,"status":"done","result":{...},"elapse":1.42}
# error     → {"job_id":...,"status":"error","error":"..."}
```

> **批量导入建议**（如半小时内传完几万文档）：全部走 `/v1/jobs` 并发提交即可——
> API 秒回、任务在 Redis 排队、Worker 池匀速消化；客户端按 `job_id` 轮询或拿到 `429` 后退避重试。
> 不需要你在客户端自己维护队列。

---

## 6. 接口一览

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 健康检查（Redis 连通、在线 Worker 数、排队数） |
| POST | `/v1/jobs` | 提交任务（multipart: `file`, `max_pages=0` 不限）→ 返回 `job_id` |
| GET | `/v1/jobs/{id}` | 查询状态/结果（`pending`/`processing`/`done`/`error`） |

鉴权：请求头 `X-API-Key` 或查询参数 `api_key`（设了 `INDRA_API_KEY` 才启用；空则不鉴权）。
背压：队列 ≥ `MAX_QUEUE` 时 `POST /v1/jobs` 返回 **429**。

---

## 7. 运维要点

1. **临时文件清理**：Worker 处理完即删；另加 cron 兜底删 1 小时前的文件：
   `0 * * * * find /www/indra/tmp -type f -mmin +60 -delete`
2. **Redis 容量**：任务完成保留 24h（`JOB_TTL`）后自动过期；Redis 配了 `maxmemory 1gb + allkeys-lru` 兜底，不会撑爆。
3. **监控**：`/healthz` 探活 + `active_workers` / `queued`；Worker 心跳 TTL 30s，进程挂了会被探活发现。
4. **扩容/缩容**：调 `worker` 副本数即可，API 与 Redis 不动。
5. **合规**：Indra 权重为修改版 OpenRAIL-M，营收 >2000 万美元或与 Datalab API 竞争不可用，部署前确认。

---

## 8. 已知坑

| 坑 | 规避 |
|---|---|
| 中文路径崩溃 | 任务文件用 `uuid4().hex` 纯 ASCII 文件名；**且 `TMPDIR` 目录路径本身也不能含中文**（cv2.imread 连中文目录路径也读不了，会 500） |
| 内网无法下载模型 | 预置 `HF_HOME` + `HF_HUB_OFFLINE=1` |
| Worker 重启丢进行中任务 | 任务文件落在共享盘，重启后文件仍在但任务状态可能停在 pending；Worker 取到后会重新处理（幂等），结果以最终为准 |
| 根分区被 Docker 写满 | data-root / 临时目录迁到数据盘 |
| Redis 是关键依赖 | 没有 Redis 整个服务不可用；生产建议 Redis 持久化 + 主从 |

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
