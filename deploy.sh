#!/usr/bin/env bash
# =============================================================================
# Indra OCR API 一键部署（零配置：不填任何东西也能跑）
# -----------------------------------------------------------------------------
#   bash deploy.sh                     # 推荐：全部默认 + 询问是否开伸缩
#   bash deploy.sh -y                  # 全部默认，不询问
#   bash deploy.sh -y --autoscale      # 部署完直接开自动伸缩
#   bash deploy.sh -y --no-autoscale   # 部署完不开自动伸缩
#   bash deploy.sh --port=9000         # 指定端口（默认 8080，占用会自动顺延）
#
# 做的事：检查环境 → 生成 .env（随机密钥 + 端口）→ 构建启动 → 等就绪 →
#        打印访问地址/密钥/调用示例 → 询问是否开启自动伸缩。
# =============================================================================
set -uo pipefail

cd "$(dirname "$0")" || exit 1

PORT=8080
PORT_ARG=""                 # --port= 显式指定，优先级高于 .env
ASSUME_YES=0
AUTOSCALE="ask"
WAIT_SECS="${WAIT_SECS:-300}"

usage() {
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
}

for arg in "$@"; do
    case "$arg" in
        -y | --yes) ASSUME_YES=1 ;;
        --autoscale) AUTOSCALE="yes" ;;
        --no-autoscale) AUTOSCALE="no" ;;
        --port=*) PORT_ARG="${arg#*=}" ;;
        -h | --help) usage; exit 0 ;;
        *) echo "未知参数: $arg（--help 看用法）"; exit 1 ;;
    esac
done

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
say() { printf '%s\n' "$*"; }
warn() { printf '\033[1;33m[!] %s\033[0m\n' "$*"; }
die() { printf '\033[1;31m[x] %s\033[0m\n' "$*"; exit 1; }

ask() {
    local q="$1" ans
    [ "$ASSUME_YES" = "1" ] && return 0
    [ -t 0 ] || return 0                  # 非交互：按"是"处理
    printf '%s [Y/n] ' "$q"
    read -r ans || ans=""
    case "$ans" in n | N | no | NO) return 1 ;; *) return 0 ;; esac
}

set_env() {                               # set_env KEY VALUE
    if grep -qE "^$1=" .env 2>/dev/null; then
        sed -i "s|^$1=.*|$1=$2|" .env
    else
        printf '%s=%s\n' "$1" "$2" >>.env
    fi
}

gen_key() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
    elif [ -r /dev/urandom ]; then
        od -An -tx1 -N24 /dev/urandom | tr -d ' \n'
    else
        printf 'indra-%s-%s' "$(date +%s)" "$RANDOM$RANDOM"
    fi
}

port_in_use() {                           # 端口是否已被占用
    local p="$1"
    if command -v ss >/dev/null 2>&1; then
        ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${p}$" && return 0
    elif command -v netstat >/dev/null 2>&1; then
        netstat -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${p}$" && return 0
    fi
    if command -v timeout >/dev/null 2>&1; then
        timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/${p}" 2>/dev/null && return 0
    fi
    return 1
}

is_ours() {                               # 该端口上跑的是不是我们自己的服务
    curl -fsS --max-time 2 "http://127.0.0.1:$1/readyz" 2>/dev/null |
        grep -q '"active_workers"'
}

# 端口取值顺序：--port= > .env 的 INDRA_PORT > 默认 8080。
# 空值 / 非数字 / 越界一律回到 8080；8080 被别的程序占了就自动顺延，
# 不让"端口没填"或"端口被占"阻断部署。
resolve_port() {
    local p="$1" i=0
    case "$p" in
        '') : ;;
        *[!0-9]*) warn "端口 \"$p\" 不是数字，改用默认 8080" >&2; p=8080 ;;
    esac
    p="${p:-8080}"
    if [ "$p" -lt 1 ] || [ "$p" -gt 65535 ]; then
        warn "端口 $p 超出 1–65535，改用默认 8080" >&2; p=8080
    fi
    while [ "$i" -lt 20 ]; do
        if ! port_in_use "$p" || is_ours "$p"; then
            printf '%s' "$p"; return 0
        fi
        warn "端口 $p 已被其他程序占用，自动顺延到 $((p + 1))" >&2
        p=$((p + 1)); i=$((i + 1))
    done
    printf '%s' 8080
}

detect_ip() {                             # 取本机对外 IP（用于打印访问地址）
    local ip=""
    command -v hostname >/dev/null 2>&1 && ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    [ -z "$ip" ] && command -v ip >/dev/null 2>&1 &&
        ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')
    [ -z "$ip" ] && command -v ifconfig >/dev/null 2>&1 &&
        ip=$(ifconfig 2>/dev/null | awk '/inet /{print $2}' | grep -v '^127\.' | head -1 | sed 's/addr://')
    echo "${ip:-127.0.0.1}"
}

# --------------------------------------------------------------------------- #
# 1. 环境检查
# --------------------------------------------------------------------------- #
step "检查环境"
command -v docker >/dev/null 2>&1 || die "没找到 docker。请先装 Docker Engine：https://docs.docker.com/engine/install/"
docker info >/dev/null 2>&1 || die "docker 没运行，或当前用户没权限。用 sudo bash deploy.sh，或把用户加入 docker 组后重新登录。"
docker compose version >/dev/null 2>&1 || die "需要 Docker Compose v2（命令是 docker compose，不是 docker-compose）。"
command -v curl >/dev/null 2>&1 || die "没找到 curl（用来做就绪检查）。"

NPROC=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
# 每 Worker 线程数 ≈ 核数/4，夹在 [2,8]：既能让多份文档并行，又不至于 1 核 1 线程太慢。
THREADS=$((NPROC / 4))
[ "$THREADS" -lt 2 ] && THREADS=2
[ "$THREADS" -gt 8 ] && THREADS=8
MAX_WORKERS=$((NPROC / THREADS))
[ "$MAX_WORKERS" -lt 1 ] && MAX_WORKERS=1
[ "$MAX_WORKERS" -gt 4 ] && MAX_WORKERS=4

say "CPU 核数: $NPROC → 每个 Worker 用 $THREADS 线程，最多可扩到 $MAX_WORKERS 个 Worker"
say "        （想改：编辑 .env 的 OCR_THREADS / MAX_WORKERS，再 docker compose up -d）"

# --------------------------------------------------------------------------- #
# 2. 生成 / 复用 .env（密钥只在这里生成一次）
# --------------------------------------------------------------------------- #
step "准备配置 .env"
FRESH_ENV=0
if [ -f .env ]; then
    say "检测到已有 .env，直接沿用（不会覆盖你的密钥）"
else
    KEY=$(gen_key)
    cat >.env <<EOF
# 由 deploy.sh 生成于 $(date '+%F %T')
INDRA_PORT=$PORT
INDRA_API_KEY=$KEY
OCR_THREADS=$THREADS
MIN_WORKERS=1
MAX_WORKERS=$MAX_WORKERS
MAX_QUEUE=1000
JOB_TTL=86400
EOF
    chmod 600 .env 2>/dev/null
    FRESH_ENV=1
fi

# 读回配置：按行取值，不 source .env（避免密钥里的 $ # 空格被 shell 解释；
# 同时 tr -d '\r' 去掉 Windows 记事本留下的回车，否则密钥末尾多个 \r 会鉴权失败）
env_get() {
    sed -n "s/^$1=//p" .env 2>/dev/null | tail -1 | tr -d '\r'
}
KEY="$(env_get INDRA_API_KEY)"
# 伸缩上限以 .env 为准（autoscale.sh 读的就是它），没有才用按核数推荐的值
ENV_MAX="$(env_get MAX_WORKERS)"
ENV_MAX="${ENV_MAX:-$MAX_WORKERS}"

# 端口优先级：--port= > .env 的 INDRA_PORT > 默认 8080（.env 里留空也算 8080）
if [ -n "$PORT_ARG" ]; then
    PORT="$PORT_ARG"
else
    PORT="$(env_get INDRA_PORT)"
    PORT="${PORT:-8080}"
fi

# 不填就用 8080；最终值写回 .env，日后看配置就知道走哪个口
PORT=$(resolve_port "$PORT")
set_env INDRA_PORT "$PORT"

if [ "$FRESH_ENV" = "1" ]; then
    say "已生成 .env（端口 $PORT，随机 API Key）"
else
    say "沿用已有 .env 的密钥，端口 $PORT"
fi
say "        （想换端口：改 .env 的 INDRA_PORT 后重跑本脚本，或 --port=9000）"

mkdir -p tmp redis-data

# --------------------------------------------------------------------------- #
# 3. 模型：有离线模型就离线跑，没有就允许首次联网下载
# --------------------------------------------------------------------------- #
step "检查 OCR 模型"
if [ -n "$(ls -A models 2>/dev/null)" ]; then
    set_env HF_HUB_OFFLINE 1
    say "models/ 已有离线模型（$(du -sh models 2>/dev/null | cut -f1)），按离线模式启动"
else
    warn "models/ 为空，Worker 首次启动需要从 HuggingFace 下载模型（约 72MB）"
    if ask "现在允许联网下载模型？"; then
        set_env HF_HUB_OFFLINE 0
    else
        die "没有模型没法跑 OCR。请先在有外网的机器执行 bash scripts/prepare_models.sh ./models 并把 models/ 拷过来，再重跑本脚本。"
    fi
fi

# --------------------------------------------------------------------------- #
# 4. 构建 + 启动
# --------------------------------------------------------------------------- #
step "构建镜像并启动（首次构建约 3–10 分钟）"
docker compose up -d --build || die "启动失败，请看上面的报错，或执行 docker compose logs --tail=100"

# --------------------------------------------------------------------------- #
# 5. 等就绪（/readyz 有在线 Worker 才算真就绪）
# --------------------------------------------------------------------------- #
step "等待服务就绪（Worker 要加载模型，首次可能 1–3 分钟）"
READY=0
elapsed=0
if [ "$WAIT_SECS" -gt 0 ]; then
    while [ "$elapsed" -lt "$WAIT_SECS" ]; do
        if curl -fsS "http://127.0.0.1:${PORT}/readyz" >/dev/null 2>&1; then
            READY=1
            break
        fi
        printf '.'
        sleep 3
        elapsed=$((elapsed + 3))
    done
    printf '\n'
else
    warn "已跳过等待（WAIT_SECS=0）"
    READY=1
fi

if [ "$READY" != "1" ]; then
    warn "等了 ${WAIT_SECS}s 还没就绪，最近的日志："
    docker compose ps
    docker compose logs --tail=30 worker 2>/dev/null
    die "未就绪。常见原因：模型没下来（models/ 为空且无外网）、内存不足、Docker 起不来（docker compose logs）。"
fi

# --------------------------------------------------------------------------- #
# 6. 打印访问信息
# --------------------------------------------------------------------------- #
IP=$(detect_ip)
cat >deploy-info.txt <<EOF
Indra OCR API
地址      : http://${IP}:${PORT}
本机      : http://127.0.0.1:${PORT}
API Key   : ${KEY}
健康检查  : curl http://127.0.0.1:${PORT}/readyz
提交任务  : curl -H "X-API-Key: ${KEY}" -F "file=@test.png" http://${IP}:${PORT}/v1/jobs
查结果    : curl -H "X-API-Key: ${KEY}" http://${IP}:${PORT}/v1/jobs/<job_id>
生成时间  : $(date '+%F %T')
EOF

cat <<EOF

============================================================
 ✅ Indra OCR API 已启动
------------------------------------------------------------
 局域网地址 : http://${IP}:${PORT}       ← 填到知识库/其他系统里
 本机地址   : http://127.0.0.1:${PORT}
 API Key    : ${KEY}
------------------------------------------------------------
 提交文件：
   curl -H "X-API-Key: ${KEY}" -F "file=@test.png" \\
        http://${IP}:${PORT}/v1/jobs

 查结果：
   curl -H "X-API-Key: ${KEY}" \\
        http://${IP}:${PORT}/v1/jobs/<job_id>

 健康检查（有无在线 Worker 看这里）：
   curl http://${IP}:${PORT}/readyz
============================================================
 上面这些也写进了 deploy-info.txt
EOF

[ "$KEY" = "" ] && warn "当前 .env 里 API Key 为空 = 不鉴权，公开部署请务必设置！"

# --------------------------------------------------------------------------- #
# 7. 是否开启自动伸缩
# --------------------------------------------------------------------------- #
if [ "$AUTOSCALE" = "ask" ]; then
    printf '\n'
    if ask "是否开启自动伸缩？（默认 1 个 Worker，队列积压时自动加到 ${ENV_MAX} 个，空闲 2 分钟再减回 1 个）"; then
        AUTOSCALE="yes"
    else
        AUTOSCALE="no"
    fi
fi

AUTOSCALE_ON=0
if [ "$AUTOSCALE" = "yes" ]; then
    step "开启自动伸缩"
    if [ "$(id -u)" = "0" ] && command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
        UNIT=/etc/systemd/system/indra-autoscale.service
        cat >"$UNIT" <<EOF
[Unit]
Description=Indra OCR autoscaler
After=docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=$PWD
ExecStart=/bin/bash $PWD/scripts/autoscale.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
        if systemctl enable --now indra-autoscale >/dev/null 2>&1; then
            say "已安装为系统服务：indra-autoscale.service（重启后自动运行）"
            say "  查看状态: systemctl status indra-autoscale"
            say "  看扩缩日志: journalctl -u indra-autoscale -f"
            say "  关掉: systemctl disable --now indra-autoscale"
            AUTOSCALE_ON=1
        else
            warn "systemd 启动失败，改用后台进程"
        fi
    fi
    if [ "$AUTOSCALE_ON" = "0" ]; then
        nohup bash scripts/autoscale.sh >>autoscale.log 2>&1 &
        echo $! >.autoscale.pid
        say "已在后台运行（PID $(cat .autoscale.pid)），日志: autoscale.log"
        say "  关掉: kill \$(cat .autoscale.pid)"
        say "  注意：这种方式的控制器在重启后不会自动拉起，建议用 root 跑本脚本装成 systemd 服务"
        AUTOSCALE_ON=1
    fi
else
    say ""
    say "未开启自动伸缩。随时可以开启："
    say "  sudo bash deploy.sh -y --autoscale      # 装成 systemd 服务"
    say "  bash scripts/autoscale.sh               # 或前台跑"
    say "临时手动扩容：docker compose up -d --scale worker=2"
fi

# --------------------------------------------------------------------------- #
# 8. 收尾提示
# --------------------------------------------------------------------------- #
cat <<EOF

============================================================
 常用命令
------------------------------------------------------------
 看日志      : docker compose logs -f worker
 看状态      : docker compose ps
 当前 Worker : docker compose ps -q worker | grep -c .
 队列积压    : docker compose exec -T redis redis-cli LLEN ocr:queue
 手动扩容    : docker compose up -d --scale worker=2
 改密钥      : 编辑 .env 里的 INDRA_API_KEY，再 docker compose up -d
 停止服务    : docker compose down        （数据在 redis-data/，不会丢）
 重新部署    : docker compose up -d --build
============================================================
EOF

if [ "$FRESH_ENV" = "1" ] && [ -n "$KEY" ]; then
    say ""
    say "密钥已保存到 .env（权限 600）。请把 .env 加入备份，别提交到 git。"
fi
