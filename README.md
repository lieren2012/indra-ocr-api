# Indra OCR API 服务（Docker 部署 · v2.2 队列解耦 + 可靠队列 + 自动伸缩）

> [Indra OCR](https://github.com/datalab-to/indra)（datalab-to 出品，纯 CPU 轻量 OCR）的 HTTP API 封装。FastAPI + Redis 队列 + Worker 池：**API 收任务秒回、Worker 池异步 OCR**，**可靠队列不丢任务**、**按队列积压自动伸缩 1→4**，供知识库等系统调用，也适合任何人自部署。
> **开源 · 可自部署**：任何人 `git clone` 本仓库即可跑自己的 OCR API，不依赖作者的服务；欢迎自建、二次开发与 PR。
> **许可证**：本项目代码 Apache 2.0（见 `LICENSE`）；Indra 模型权重为修改版 OpenRAIL-M，商用前请阅 [条款](https://huggingface.co/datalab-to/indra-ocr)。
> **实测吞吐**：单页 0.5–2.0s（纯 CPU，无需 GPU）。快速部署见 §5；离线模型预置见 `scripts/prepare_models.sh`。

---

## 1. 架构

```
Nginx (限流 / 背压 / 上传体积)
   │
   ▼
API（极轻量，不加载模型）：收上传 → 落共享盘 → 写任务哈希 → 入队 → 立即返回 job_id
   │                                  ▲
   │  队列 ocr:queue                   │ 结果写回任务哈希
   ▼                                  │
Worker 池（加载模型，吃算力）：BLMOVE 取任务 → 跑 OCR → 结果落库 → 删文件
                              （+ 独立心跳线程）
```

三层各管一件事：

- **API**：只做收件与查询，毫秒级返回 `job_id`；带背压（队列满返 `429`）和看门狗（回收死 Worker 的任务）。
- **Redis**：`ocr:queue` 待处理队列 + 每个 Worker 的 `ocr:processing:{id}` 在处理列表 + `ocr:worker:{id}` 心跳 + `ocr:job:{id}` 结果哈希。
- **Worker**：`BLMOVE` 原子取任务，OCR 跑完、结果成功写进 Redis 之后才移除记录并删文件。

单机一套 compose 即可跑；要更高吞吐就加 Worker 副本或加机器，API 不用动。

> ⚠️ **吞吐实话**：队列管理负载，但不创造算力。线程数只加速单页内部的推理，**总吞吐靠 Worker 副本数**（换算见 §2）。

---

## 2. 配置一览（默认值都在这里）

| 项 | 默认 | 说明 / 怎么改 |
|---|---|---|
| **Worker 副本数** | **1** | compose 不写死 `replicas`；用 `--scale worker=N` 或 `scripts/autoscale.sh` 增减，**只动 worker** |
| **每 Worker 线程数** `OCR_THREADS` | **8** | onnxruntime 线程数，≈ 该副本占用的核数；改 `.env` 或 compose 环境变量 |
| 单机总线程 | 副本数 × `OCR_THREADS` ≤ 物理核数 | 32 核 → 顶格 4 副本 × 8 线程；超了上下文切换反而掉速 |
| 单副本 CPU 上限 | `cpus: 8.0` | `deploy.resources.limits`，与 `OCR_THREADS` 对齐 |
| 单副本内存上限 | `2G` | 模型 72MB + onnxruntime 运行时；实际 RSS 以 `docker stats` 为准 |
| 队列上限 `MAX_QUEUE` | `1000` | 超过则 `POST /v1/jobs` 返 `429`，形成背压 |
| 结果保留 `JOB_TTL` | `86400`（24h） | 过期自动删除任务哈希 |
| Worker 心跳 TTL / 刷新 | `60s` / `15s` | 进程真挂约 60s 内被发现 |
| 看门狗间隔 | `30s` | 跑在 API 进程内，扫死 Worker 的处理列表并原子重投 |
| Redis 内存策略 | `maxmemory 1gb` + `noeviction` | 内存满时**写失败**（API 返 503）而不是淘汰队列；结果体大就调到 `2gb`/`4gb` |
| Redis 持久化 | AOF `appendfsync everysec` | 掉电最多丢 ~1s 写入 |
| 伸缩上下限 | `MIN_WORKERS=1` `MAX_WORKERS=4` | 见 §6 |
| 伸缩采样 / 空闲 / 冷却 | `INTERVAL=5s` `IDLE_SECS=120s` `SCALE_COOLDOWN=30s` | 见 §6 |

**容量换算**（按单页 0.5–2s 估）：

| 配置 | 小时处理量 |
|---|---|
| 1 Worker × 8 线程 | 约 1 800–3 600 页/h |
| 2 Worker | 约 3 600–7 200 页/h |
| 4 Worker（32 核机器） | 约 7 000–14 000 页/h |

> 要 3 万+/小时得**多机跑 Worker**（§6 方式 C），单机核数不够。

---

## 3. 目录结构

```
indra-ocr-service/
├── api.py                  # API 服务（收任务、查状态、鉴权、背压、看门狗）
├── worker.py               # Worker（可靠取任务、独立心跳、跑 OCR）
├── ocr_engine.py           # OCR 引擎封装（被 Worker 加载，模型常驻）
├── requirements.txt
├── Dockerfile              # 同一镜像，靠启动命令区分 api / worker
├── docker-compose.yml      # redis + api + worker(默认1，可 --scale) + nginx
├── nginx.conf
├── .env.example            # 复制成 .env 改配置
├── scripts/
│   ├── prepare_models.sh   # 联网机预置模型（只需跑一次）
│   ├── autoscale.sh        # 宿主机自动伸缩（按队列积压 1→4，可常驻）
│   └── worker_healthcheck.py
├── tests/
│   └── test_worker_recovery.py   # 不丢任务/幂等自检（无需 Redis）
├── models/                 # 离线模型（预置后挂载，~72MB）
├── tmp/                    # 上传文件共享盘（api 写、worker 读，务必放数据盘）
└── redis-data/             # Redis AOF 落盘（务必放数据盘）
```

---

## 4. 离线预置模型（内网服务器必须）

在有外网的机器上：

```bash
cd indra-ocr-service
bash scripts/prepare_models.sh ./models
# 模型缓存到 ./models，约 72MB
```

把整个 `models/` 目录拷到内网服务器项目目录下，Worker 会只读挂载为 `/opt/models`。

> ⚠️ 若跳过此步，`HF_HUB_OFFLINE=1` 会让 Worker 启动即因无法下载模型而失败。

---

## 5. 部署（Docker，推荐）

```bash
cd indra-ocr-service
cp .env.example .env        # 编辑：至少填 INDRA_API_KEY
# 公开部署务必设置 API Key
export INDRA_API_KEY="你的密钥"

# 构建并启动（redis + api + 1 个 worker + nginx）
docker compose up -d --build

# 就绪检查（注意：/healthz 的 ok 字段只有在有在线 worker 时才为 true）
curl http://localhost:8080/readyz
# 期望: {"ok":true,"redis":true,"active_workers":1,"queued":0,...}
#       worker 数为 0 时返回 HTTP 503 —— 部署验收请看 /readyz
```

**磁盘注意**：Docker 默认 `data-root` 在 `/var/lib/docker`（根分区）。
若根分区紧张，把 `tmp/`、`redis-data/` 与镜像数据迁到数据盘（如 `/www/indra/`），并配置 Docker `data-root` 到数据盘。

---

## 6. 自动伸缩（1 → 2 → 3 → 4，怎么加、怎么减）

### 6.1 规则

| 触发 | 判定条件 | 动作 |
|---|---|---|
| **扩容** | 每 `INTERVAL=5s` 采样 `LLEN ocr:queue`，**> 0**（即有任务在排队等 Worker）且距上次调整 ≥ `SCALE_COOLDOWN=30s` | 副本 **+1**，上限 `MAX_WORKERS=4` |
| **缩容** | 队列连续为 0 累计 ≥ `IDLE_SECS=120s`，且当前 > `MIN_WORKERS`，且不在冷却期 | 副本 **−1**，下限 `MIN_WORKERS=1` |
| **钳制** | 任何情况下结果都夹在 `[MIN_WORKERS, MAX_WORKERS]` | — |
| **读不到队列** | `redis-cli` 报错 / `exec` 失败（返回非纯数字） | **本轮跳过**，既不扩也不缩，避免误判抖动 |
| **失败重试** | `docker compose up -d --scale` 报错则保持现状，`SCALE_COOLDOWN` 后重试 | 不中断服务 |

即：**默认 1 个 → 一条任务排队就说明 1 个跑不动 → 加第 2 个 → 还积压 → 第 3 个 → 第 4 个为止；队列空满 2 分钟再逐个减回 1 个。**
积压时 1→4 大约 **1 分钟**（每次加完留 30s 给新 Worker 加载模型、开始消费）。

### 6.2 手动

```bash
docker compose up -d --scale worker=4     # N = floor(核数 / OCR_THREADS)
```

### 6.3 自动（推荐）

```bash
# 前台试跑，观察日志
bash scripts/autoscale.sh

# 后台常驻
nohup bash scripts/autoscale.sh >>/var/log/indra-autoscale.log 2>&1 &
```

可调参数：`MIN_WORKERS=1 MAX_WORKERS=4 INTERVAL=5 IDLE_SECS=120 SCALE_COOLDOWN=30`。
写在 `.env` 里也行（脚本启动时会加载同目录 `.env`），或用 systemd 的 `Environment=` 覆盖。

```bash
# .env
MIN_WORKERS=1
MAX_WORKERS=4        # 一般 = floor(物理核数 / OCR_THREADS)
OCR_THREADS=8
```

### 6.4 保证控制器本身不倒（systemd）

自动伸缩的可靠性取决于**控制器活着**，用 systemd 托管：

```ini
# /etc/systemd/system/indra-autoscale.service
[Unit]
Description=Indra OCR autoscaler
After=docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=/opt/indra-ocr-service
Environment=MIN_WORKERS=1
Environment=MAX_WORKERS=4
Environment=INTERVAL=5
Environment=IDLE_SECS=120
Environment=SCALE_COOLDOWN=30
ExecStart=/bin/bash /opt/indra-ocr-service/scripts/autoscale.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now indra-autoscale
journalctl -u indra-autoscale -f          # 看扩缩容流水
```

**怎么验证它在工作**：

```bash
docker compose ps -q worker | grep -c .    # 当前副本数
docker compose exec -T redis redis-cli LLEN ocr:queue   # 当前积压
tail -f /var/log/indra-autoscale.log       # 应出现 "workers 1 -> 2"
```

**控制器挂了会怎样**：副本数**停在最后一次的值**，服务照常跑，只是不再自动增减；重启后按当前队列继续判断。`docker compose up -d` 也能手动兜底。

**缩容不会丢任务**：Compose 发 `SIGTERM` → Worker 处理完当前任务、把剩余未处理项重新入队、注销心跳后才退出（`stop_grace_period: 300s`，长任务也能跑完）。

### 6.5 跨机集群

在更多机器上跑 Worker 容器，指向**同一个 Redis**（Redis 部署在内网可达处；文件共享盘换成对象存储 / NFS）。
API 是无状态的，也可多机部署在 Redis 前。真要按需弹性到几十副本，用 K8s + HPA/KEDA 按 Redis 队列长度扩副本。

> 队列是天然背压：洪峰时任务在 Redis 堆积、API 照常秒回 `job_id`；超过 `MAX_QUEUE`（默认 1000）才返回 `429`。

---

## 7. 调用示例

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

## 8. 接口一览

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 存活检查（始终 200；`ok` 字段 = Redis 通 且 在线 Worker > 0） |
| GET | `/readyz` | 就绪检查（无在线 Worker 时返回 **503**；部署验收看这个） |
| POST | `/v1/jobs` | 提交任务（multipart: `file`, `max_pages=0` 不限）→ 返回 `job_id` |
| GET | `/v1/jobs/{id}` | 查询状态/结果（`pending`/`processing`/`done`/`error`） |

鉴权：请求头 `X-API-Key` 或查询参数 `api_key`（设了 `INDRA_API_KEY` 才启用；空则不鉴权）。
背压：队列 ≥ `MAX_QUEUE` 时 `POST /v1/jobs` 返回 **429**。

---

## 9. 运维要点

1. **临时文件清理**：Worker 处理完即删；另加 cron 兜底删 1 小时前的文件：
   `0 * * * * find /www/indra/tmp -type f -mmin +60 -delete`
2. **Redis 持久化 + 不淘汰**：开启 **AOF**（`appendfsync everysec`，掉电最多丢 ~1s 写入）；**`noeviction`**（内存满时**写失败而非淘汰队列/任务/心跳**，API 返回 503 让调用方重试，绝不静默丢数据）；结果保留 24h（`JOB_TTL`）后自动过期。
3. **监控**：`/readyz` 做验收（无在线 Worker 直接 503），`/healthz` 看 `active_workers` / `queued`。Worker 心跳由**独立线程**每 15s 刷新、TTL 60s，**长文档处理期间也不会误判离线**；进程真挂了约 60s 内被发现。Compose 健康检查：**api 用 `/readyz`、worker 用自身心跳**，`docker compose ps` 能看到真实健康态。
4. **不丢任务**：任务先 `BLMOVE` 原子挪到 `ocr:processing:{worker}`；**结果成功落库才** `LREM` + 删文件（落库失败 → 原子重投回队列，绝不删文件）。看门狗每 30s 用**原子 Lua 脚本**回收心跳过期 Worker 的处理列表。
   - 落库失败：重投 3 次；仍失败则 Worker 主动退出（心跳过期 → 看门狗兜底），**不会卡在某个 Worker 手里无人重试**。
   - `LREM` 失败：**保留文件**，避免任务日后被重投时因"文件不存在"报错并覆盖原本成功的结果。
   - 已 `done` 的任务被重投：只清理记录，**不重算、绝不改成 error**（幂等）。
5. **扩容/缩容**：见 §6；`docker compose ps -q worker | grep -c .` 确认实际副本数。
6. **合规**：Indra 权重为修改版 OpenRAIL-M，营收 >2000 万美元或与 Datalab API 竞争不可用，部署前确认。
7. **自检**：改完代码跑一遍 `python tests/test_worker_recovery.py`（内存假 Redis，不需要跑容器），覆盖"写结果失败要重投""LREM 失败不删文件""已 done 不重算"等不丢任务的关键路径。

---

## 10. 已知坑

| 坑 | 规避 |
|---|---|
| 中文路径崩溃 | 任务文件用 `uuid4().hex` 纯 ASCII 文件名；**且 `TMPDIR` 目录路径本身也不能含中文**（cv2.imread 连中文目录路径也读不了，会 500） |
| 内网无法下载模型 | 预置 `HF_HOME` + `HF_HUB_OFFLINE=1` |
| Worker 崩溃/重启丢任务 | 已用可靠队列兜住：任务在处理列表中，看门狗约 60s 后自动重新入队；极端情况（OCR 完成但没来得及落库就崩溃）会**重复处理一次**，结果以最终为准（幂等） |
| 根分区被 Docker 写满 | data-root / 临时目录迁到数据盘 |
| Redis 掉电 / 内存满 | 已开 AOF（`everysec`，最多丢 ~1s）+ `noeviction`（写满返 503，**不淘汰任务**）；生产建议再加 Redis 主从/备份 |
| Redis 是关键依赖 | 没有 Redis 整个服务不可用；务必开持久化 |
| 自动伸缩不生效 | 确认 `autoscale.sh` 在**宿主机**、有 docker 权限；`journalctl -u indra-autoscale -f` 看是否有 `LLEN` 失败 |

---

## 11. 官方资源与免责提醒

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

## 12. 同类 OCR 产品（参考）

| 项目 | GitHub | 定位 |
|---|---|---|
| **Indra**（本仓库封装） | https://github.com/datalab-to/indra | 纯 onnxruntime 轻量 CPU OCR，中文扫描件友好，无 PyTorch/PaddlePaddle 依赖 |
| RapidOCR | https://github.com/RapidAI/RapidOCR | 轻量端侧推理（PP-OCR 系列），快，但无版面块/表格结构 |
| PaddleOCR | https://github.com/PaddlePaddle/PaddleOCR | 完整版含 PP-Structure，表格/竖排还原强；代价是需 PaddlePaddle 框架（~500MB+） |

> 三者共同短板：手写体、竖排文字、复杂表格（合并单元格/多级表头/无框线）会丢结构，需上 VLM（如 PaddleOCR-VL / Qwen2.5-VL）补刀。
