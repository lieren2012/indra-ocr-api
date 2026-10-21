#!/usr/bin/env bash
# =============================================================================
# Indra OCR 自动伸缩控制器（在宿主机运行，可选常驻）
# -----------------------------------------------------------------------------
# Docker Compose 原生不支持按负载自动增减副本；本脚本补上这件事：
#   - 队列积压时：每次 +1 个 worker（最多 MAX_WORKERS）
#   - 队列空闲持续 IDLE_SECS 秒：每次 -1 个 worker（最少 MIN_WORKERS）
# 通过 `docker compose up -d --scale worker=N --no-recreate` 增删副本，
# 只动 worker，不重建 api / redis / nginx。
#
# 用法：
#   bash scripts/autoscale.sh            # 前台运行（Ctrl-C 退出）
#   nohup bash scripts/autoscale.sh >/var/log/indra-autoscale.log 2>&1 &   # 后台常驻
#   或 systemd（见 README §4）
#
# 可调环境变量：MIN_WORKERS=1 MAX_WORKERS=4 INTERVAL=5 IDLE_SECS=120
# =============================================================================
set -euo pipefail

MIN="${MIN_WORKERS:-1}"
MAX="${MAX_WORKERS:-4}"
INTERVAL="${INTERVAL:-5}"
IDLE_SECS="${IDLE_SECS:-120}"

cd "$(dirname "$0")/.."   # 切到 compose 文件所在目录

qlen() {  # 队列长度（在 redis 容器内执行，无需对外暴露端口）
    docker compose exec -T redis redis-cli LLEN ocr:queue 2>/dev/null | tr -d '\r' || echo 0
}

workers() {  # 当前运行中的 worker 容器数
    docker compose ps -q worker 2>/dev/null | wc -l | tr -d ' '
}

current=$(workers); [ -z "$current" ] && current=0
idle=0
echo "[autoscale] start: current=$current min=$MIN max=$MAX interval=${INTERVAL}s idle=${IDLE_SECS}s"

while true; do
    q=$(qlen); [ -z "$q" ] && q=0
    desired=$current

    if [ "$q" -gt 0 ]; then
        idle=0
        desired=$((current + 1))            # 有积压 → 扩 1
    else
        idle=$((idle + INTERVAL))
        if [ "$idle" -ge "$IDLE_SECS" ] && [ "$current" -gt "$MIN" ]; then
            desired=$((current - 1))        # 空闲够久 → 缩 1
            idle=0
        fi
    fi

    # 夹到 [MIN, MAX]
    [ "$desired" -lt "$MIN" ] && desired=$MIN
    [ "$desired" -gt "$MAX" ] && desired=$MAX

    if [ "$desired" -ne "$current" ]; then
        echo "[autoscale] $(date +%T) queue=$q workers ${current} -> ${desired}"
        docker compose up -d --scale "worker=$desired" --no-recreate >/dev/null 2>&1 || true
        current=$(workers); [ -z "$current" ] && current=0
    fi

    sleep "$INTERVAL"
done
