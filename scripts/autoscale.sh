#!/usr/bin/env bash
# =============================================================================
# Indra OCR 自动伸缩控制器（在宿主机运行，可常驻）
# -----------------------------------------------------------------------------
# Docker Compose 原生不支持按负载自动增减副本；本脚本补上这件事：
#   扩容：每 INTERVAL 秒采样 LLEN ocr:queue，>0 就 +1，直到 MAX_WORKERS
#   缩容：队列连续为 0 累计 IDLE_SECS 秒就 -1，直到 MIN_WORKERS
#   冷却：每次增减后静默 SCALE_COOLDOWN 秒，避免抖动 / 新 worker 还没起来又加
# 只动 worker，不重建 api / redis / nginx。
#
# 用法：
#   bash scripts/autoscale.sh                      # 前台（Ctrl-C 退出）
#   nohup bash scripts/autoscale.sh \
#       >>/var/log/indra-autoscale.log 2>&1 &      # 后台常驻
#   systemd 常驻见 README「自动伸缩」一节
#
# 环境变量：
#   MIN_WORKERS=1      缩容下限（默认 1）
#   MAX_WORKERS=4      扩容上限（默认 4；机器核数 / OCR_THREADS）
#   INTERVAL=5         采样间隔秒
#   IDLE_SECS=120      队列空闲多久才缩容
#   SCALE_COOLDOWN=30  两次调整之间的最小间隔
# =============================================================================
set -uo pipefail   # 故意不加 -e：单次 docker 抖动不应让控制器退出

cd "$(dirname "$0")/.." || exit 1   # 切到 compose 文件所在目录

# 若存在 .env 则加载，便于把 MIN_WORKERS/MAX_WORKERS 和其它配置写在同一处
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
fi

MIN="${MIN_WORKERS:-1}"
MAX="${MAX_WORKERS:-4}"
INTERVAL="${INTERVAL:-5}"
IDLE_SECS="${IDLE_SECS:-120}"
COOLDOWN="${SCALE_COOLDOWN:-30}"

# 读队列长度。只有纯数字才算有效；连接失败/报错串返回 NA，
# 本周期直接跳过——绝不能把 "redis:6379 Connection refused" 里的 6379 当积压。
qlen() {
    local v
    v=$(docker compose exec -T redis redis-cli LLEN ocr:queue 2>/dev/null | tr -d '\r\n ')
    case "$v" in
        '' | *[!0-9]*) echo NA ;;
        *) echo "$v" ;;
    esac
}

# 运行中的 worker 容器数
workers() {
    local v
    v=$(docker compose ps -q worker 2>/dev/null | grep -c .)
    [ -z "$v" ] && v=0
    echo "$v"
}

log() { echo "[autoscale] $(date '+%F %T') $*"; }

current=$(workers)
idle=0
err=0
last_change=$(date +%s)

log "start: current=$current min=$MIN max=$MAX interval=${INTERVAL}s idle=${IDLE_SECS}s cooldown=${COOLDOWN}s"

while true; do
    q=$(qlen)
    if [ "$q" = "NA" ]; then
        # 读不到队列（Redis 挂了 / exec 失败）→ 不扩不缩，避免误判
        err=$((err + 1))
        [ $((err % 20)) -eq 1 ] && log "cannot read queue length (redis/exec problem), skip cycle #$err"
        sleep "$INTERVAL"
        continue
    fi
    err=0
    now=$(date +%s)
    desired="$current"

    if [ "$q" -gt 0 ]; then
        idle=0
        # 有积压且不在冷却期 → 加一个（新 worker 起来后要给它时间消化）
        if [ "$current" -lt "$MAX" ] && [ $((now - last_change)) -ge "$COOLDOWN" ]; then
            desired=$((current + 1))
        fi
    else
        idle=$((idle + INTERVAL))
        if [ "$idle" -ge "$IDLE_SECS" ] && [ "$current" -gt "$MIN" ] \
           && [ $((now - last_change)) -ge "$COOLDOWN" ]; then
            desired=$((current - 1))
            idle=0
        fi
    fi

    # 夹到 [MIN, MAX]
    [ "$desired" -lt "$MIN" ] && desired="$MIN"
    [ "$desired" -gt "$MAX" ] && desired="$MAX"

    if [ "$desired" -ne "$current" ]; then
        log "queue=$q workers ${current} -> ${desired}"
        if ! docker compose up -d --scale "worker=$desired" --no-recreate >/dev/null 2>&1; then
            log "scale failed (docker compose error), retry after ${COOLDOWN}s"
        fi
        last_change=$now   # 成功或失败都进入冷却，避免每 5s 刷同一条命令
        current=$(workers)
    fi

    sleep "$INTERVAL"
done
