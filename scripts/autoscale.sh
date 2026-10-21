#!/usr/bin/env bash
# =============================================================================
# Indra OCR 自动伸缩控制器（在宿主机运行，可常驻）
# -----------------------------------------------------------------------------
# 默认形态：每个 worker 只吃 1 个线程（1 核），靠"多开副本"顶吞吐。
#   Worker 1 → 队列有积压 → 起第 2 个 → 还积压 → 第 3 个 … → 最多 8 个
#   队列连续空闲 IDLE_SECS 秒 → 逐个减回 1 个
#
# Docker Compose 原生不支持按负载增减副本；本脚本补上这件事：
#   扩容：每 INTERVAL 秒采样 LLEN ocr:queue，>0 就 +1，直到 MAX_WORKERS
#   缩容：队列连续为 0 累计 IDLE_SECS 秒就 -1，直到 MIN_WORKERS
#   冷却：每次增减后静默 SCALE_COOLDOWN 秒（新 worker 要时间加载模型）
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
#   MAX_WORKERS=8      扩容上限（默认 8；超过 CPU 核数会被自动夹到核数）
#   INTERVAL=5         采样间隔秒
#   IDLE_SECS=90       队列空闲多久才缩容
#   SCALE_COOLDOWN=15  两次调整之间的最小间隔
# =============================================================================
set -uo pipefail   # 故意不加 -e：单次 docker 抖动不应让控制器退出

cd "$(dirname "$0")/.." || exit 1   # 切到 compose 文件所在目录

log() { echo "[autoscale] $(date '+%F %T') $*"; }

# 若存在 .env 则只读其中与伸缩有关的几个数值键（不 source，避免执行 .env 内容；
# 顺手去掉 Windows 记事本留下的 \r，否则 "8\r" 会让算术判断报错）
if [ -f .env ]; then
    while IFS='=' read -r k v || [ -n "$k" ]; do
        k="${k%$'\r'}"; v="${v%$'\r'}"
        case "$k" in
            MIN_WORKERS | MAX_WORKERS | INTERVAL | IDLE_SECS | SCALE_COOLDOWN)
                export "$k=$v"
                ;;
        esac
    done <.env
fi

num() {   # num 值 默认值 → 只接受纯数字，否则用默认值
    case "${1:-}" in
        '' | *[!0-9]*) echo "$2" ;;
        *) echo "$1" ;;
    esac
}

MIN=$(num "${MIN_WORKERS:-}" 1)
MAX=$(num "${MAX_WORKERS:-}" 8)
INTERVAL=$(num "${INTERVAL:-}" 5)
IDLE_SECS=$(num "${IDLE_SECS:-}" 90)
COOLDOWN=$(num "${SCALE_COOLDOWN:-}" 15)

[ "$MIN" -lt 1 ] && MIN=1
[ "$MAX" -lt "$MIN" ] && MAX="$MIN"

# 1 线程 = 1 核：副本数超过核数只会互相抢 CPU，没有意义
CORES=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
CORES=$(num "$CORES" 4)
if [ "$MAX" -gt "$CORES" ]; then
    log "MAX_WORKERS=$MAX 超过 CPU 核数 $CORES，按 $CORES 处理（1 线程/worker）"
    MAX="$CORES"
fi
[ "$MIN" -gt "$MAX" ] && MIN="$MAX"

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

current=$(workers)
idle=0
err=0
at_max_logged=0
last_change=$(date +%s)

log "start: current=$current min=$MIN max=$MAX interval=${INTERVAL}s idle=${IDLE_SECS}s cooldown=${COOLDOWN}s cores=$CORES"

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
        if [ "$current" -lt "$MAX" ] && [ $((now - last_change)) -ge "$COOLDOWN" ]; then
            # 有积压且不在冷却期 → 加一个（新 worker 起来后要给它时间消化）
            desired=$((current + 1))
            at_max_logged=0
        elif [ "$current" -ge "$MAX" ] && [ "$at_max_logged" != "$q" ]; then
            # 已到上限还有积压：明确告警，别让人以为还能自动加
            log "backlog=$q but already at max=$MAX workers (cores=$CORES) —— 再加机器或提高 MAX_WORKERS"
            at_max_logged="$q"
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
