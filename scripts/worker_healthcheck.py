"""Worker 容器健康检查：确认本 worker 的心跳仍在 Redis 中。

模型未加载好 / 进程卡死 → 心跳缺失 → 退出码 1（unhealthy）。
worker.py 在模型加载成功后才写 /tmp/india_worker_id 并发心跳。
"""

import os
import sys

try:
    import redis
except Exception:
    sys.exit(1)

ID_FILE = "/tmp/india_worker_id"


def main() -> int:
    if not os.path.exists(ID_FILE):
        return 1
    try:
        with open(ID_FILE, encoding="utf-8") as f:
            wid = f.read().strip()
    except OSError:
        return 1
    if not wid:
        return 1
    try:
        r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
        return 0 if r.exists("ocr:worker:" + wid) else 1
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
