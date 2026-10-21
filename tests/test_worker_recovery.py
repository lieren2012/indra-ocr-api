"""worker.py 恢复逻辑单元测试（无需真实 Redis）。

用内存假 Redis 模拟，重点验证几条不该被破坏的规则：
  1. 读/写 Redis 失败时任务必须能被重新投递，绝不能卡在某个活着的 worker 手里；
  2. 重投是原子的：已 done 的任务只清理，绝不把状态回退成 pending；
  3. 移出 processing 失败时保留文件，避免日后重投报"文件不存在"并覆盖成功结果；
  4. 已 done 的任务被重投只清理，不重算、不改回 error。

运行：python tests/test_worker_recovery.py
"""

import os
import sys
import tempfile
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


class FakeRedis:
    """只实现 worker.py 用到的那几个命令。"""

    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.lists = {}
        self.sets = {}
        self.fail_hset = False          # 任何 hset 都失败（Redis 挂）
        self.fail_hset_result = False   # 只让"写结果"那次失败（模拟内存满）
        self.fail_lrem = False
        self.fail_hgetall = False

    # --- kv ---
    def set(self, k, v, ex=None):
        self.kv[k] = v

    def delete(self, *keys):
        for k in keys:
            self.kv.pop(k, None)
            self.lists.pop(k, None)

    # --- hash ---
    def hset(self, name, key=None, value=None, mapping=None):
        if self.fail_hset:
            raise RuntimeError("redis down")
        if self.fail_hset_result and mapping and "result" in mapping:
            raise RuntimeError("OOM command not allowed (noeviction)")
        h = self.hashes.setdefault(name, {})
        if mapping:
            h.update(mapping)
        elif key is not None:
            h[key] = value

    def hgetall(self, name):
        if self.fail_hgetall:
            raise RuntimeError("redis timeout")
        return dict(self.hashes.get(name, {}))

    def hdel(self, name, *fields):
        h = self.hashes.get(name, {})
        for f in fields:
            h.pop(f, None)

    def expire(self, name, ttl):
        return True

    def exists(self, name):
        return int(name in self.hashes or name in self.kv)

    # --- list ---
    def rpush(self, name, *vals):
        self.lists.setdefault(name, []).extend(vals)

    def lrange(self, name, start, end):
        return list(self.lists.get(name, []))

    def lrem(self, name, count, value):
        if self.fail_lrem:
            raise RuntimeError("redis down")
        lst = self.lists.get(name, [])
        removed = 0
        out = []
        for v in lst:
            if v == value and (count == 0 or removed < abs(count)):
                removed += 1
                continue
            out.append(v)
        self.lists[name] = out
        return removed

    def blmove(self, src, dst, timeout, wherefrom, whereto):
        lst = self.lists.get(src) or []
        if not lst:
            return None
        jid = lst.pop(0)
        self.lists.setdefault(dst, []).append(jid)
        return jid

    # --- set ---
    def sadd(self, name, *vals):
        self.sets.setdefault(name, set()).update(vals)

    def srem(self, name, *vals):
        for v in vals:
            self.sets.get(name, set()).discard(v)

    # --- 脚本：按关键命令识别三个 Lua ---
    def eval(self, script, numkeys, *args):
        if "LINDEX" in script:                    # 看门狗：整表扫描
            return self._reap(script, args)
        if "HGET" in script and "RPUSH" in script:  # worker 重投（带状态守卫）
            plist, queue, key, jid = args[0], args[1], args[2], args[3]
            if key not in self.hashes or self.hashes[key].get("status") == "done":
                self.lists[plist] = [x for x in self.lists.get(plist, []) if x != jid]
                return 0
            self.hashes[key]["status"] = "pending"
            self.hashes[key].pop("processing_since", None)
            self.lists.setdefault(queue, []).append(jid)
            self.lists[plist] = [x for x in self.lists[plist] if x != jid]
            return 1
        raise AssertionError("unexpected script: %r" % script[:60])

    def _reap(self, script, args):
        plist, queue, prefix = args[0], args[1], args[2]
        moved, dropped = [], []
        while self.lists.get(plist):
            jid = self.lists[plist][-1]
            key = prefix + jid
            if key not in self.hashes or self.hashes[key].get("status") == "done":
                self.lists[plist].pop()
                dropped.append(jid)
            else:
                self.hashes[key]["status"] = "pending"
                self.hashes[key].pop("processing_since", None)
                self.lists.setdefault(queue, []).append(jid)
                self.lists[plist].pop()
                moved.append(jid)
        return [moved, dropped]


# 注入假 redis / ocr_engine，再导入 worker
fake = FakeRedis()
redis_mod = types.ModuleType("redis")
redis_mod.Redis = types.SimpleNamespace(from_url=lambda *a, **k: fake)
redis_mod.RedisError = RuntimeError
sys.modules["redis"] = redis_mod

calls = {"run_ocr": 0}
ocr_mod = types.ModuleType("ocr_engine")
ocr_mod.load_model = lambda threads: None


def _fake_run_ocr(path, max_pages):
    calls["run_ocr"] += 1
    return {"pages": [{"index": 0, "text": "ok", "markdown": "ok", "lines": []}]}


ocr_mod.run_ocr = _fake_run_ocr
sys.modules["ocr_engine"] = ocr_mod

import worker as w  # noqa: E402

w.r = fake
w.time.sleep = lambda s: None   # 不让重试等待拖慢测试


def new_job(jid, status="pending", create_file=True):
    path = os.path.join(w.TMPDIR, jid + ".png")
    os.makedirs(w.TMPDIR, exist_ok=True)
    if create_file:
        with open(path, "wb") as f:
            f.write(b"x")
    fake.hashes["ocr:job:" + jid] = {"status": status, "suffix": ".png", "max_pages": "0"}
    fake.lists.setdefault(w.PROC_LIST, []).append(jid)
    return path


def reset():
    fake.kv.clear()
    fake.hashes.clear()
    fake.lists.clear()
    fake.sets.clear()
    fake.fail_hset = False
    fake.fail_hset_result = False
    fake.fail_lrem = False
    fake.fail_hgetall = False
    calls["run_ocr"] = 0


def check(name, cond, detail=""):
    if not cond:
        print(f"[FAIL] {name} {detail}")
        raise SystemExit(1)
    print(f"[ok]   {name}")


# --------------------------------------------------------------------------- #
# 1. 读写 Redis 失败 → 必须重投（用户报告的 bug 1）
# --------------------------------------------------------------------------- #
def test_hgetall_failure_requeues():
    """读任务状态失败不能直接 return：心跳还在，看门狗不会回收 → 任务永久卡住。"""
    reset()
    jid = "jobH"
    new_job(jid, status="processing")
    fake.fail_hgetall = True

    w.process(jid)

    check("读状态失败 → 任务回到队列", jid in fake.lists.get(w.QUEUE, []))
    check("读状态失败 → 移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))


def test_processing_mark_failure_requeues_not_error():
    """连"标记 processing"都写不进去时，不能继续跑然后把它标成 error。"""
    reset()
    jid = "jobI"
    new_job(jid, status="pending")
    fake.fail_hset = True

    w.process(jid)

    check("标记 processing 失败 → 不跑 OCR", calls["run_ocr"] == 0)
    check("标记 processing 失败 → 状态不是 error",
          fake.hashes["ocr:job:jobI"]["status"] != "error")
    check("标记 processing 失败 → 任务回到队列", jid in fake.lists.get(w.QUEUE, []))


def test_write_result_failure_requeues():
    """OCR 成功但结果写不进 Redis（noeviction 内存满）→ 重投，不能卡死。"""
    reset()
    jid = "jobG"
    new_job(jid, status="pending")
    fake.fail_hset_result = True

    w.process(jid)

    check("写结果失败 → OCR 确实跑过", calls["run_ocr"] == 1)
    check("写结果失败 → 任务回到队列", jid in fake.lists.get(w.QUEUE, []))
    check("写结果失败 → 移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))
    check("写结果失败 → 状态回 pending", fake.hashes["ocr:job:jobG"]["status"] == "pending")


# --------------------------------------------------------------------------- #
# 2. 重投的原子性 / 覆盖竞争（用户报告的 bug 2）
# --------------------------------------------------------------------------- #
def test_requeue_does_not_clobber_done():
    """别的 worker 已经算完写了 done，旧 worker 再重投也不能把状态改回 pending。"""
    reset()
    jid = "jobJ"
    new_job(jid, status="done")

    w._requeue(jid)

    check("已 done → 不重新入队", jid not in fake.lists.get(w.QUEUE, []))
    check("已 done → 状态仍是 done", fake.hashes["ocr:job:jobJ"]["status"] == "done")
    check("已 done → 仍从 processing 清理掉", jid not in fake.lists.get(w.PROC_LIST, []))


def test_requeue_sets_pending_atomically():
    """重投脚本自己置 pending 并入队，中间没有"已入队但状态未改"的空窗。"""
    reset()
    jid = "jobK"
    new_job(jid, status="processing")

    ok = w._requeue(jid)

    check("重投成功", ok is True)
    check("重投后状态 pending", fake.hashes["ocr:job:jobK"]["status"] == "pending")
    check("重投后清掉 processing_since", "processing_since" not in fake.hashes["ocr:job:jobK"])
    check("重投后只在队列、不在 processing",
          jid in fake.lists.get(w.QUEUE, []) and jid not in fake.lists.get(w.PROC_LIST, []))


def test_requeue_missing_job_is_dropped():
    """任务哈希已过期（24h TTL）→ 直接丢弃，不要造一个只有 status 的空任务。"""
    reset()
    fake.lists.setdefault(w.PROC_LIST, []).append("jobX")

    w._requeue("jobX")

    check("任务不存在 → 不入队", "jobX" not in fake.lists.get(w.QUEUE, []))
    check("任务不存在 → 不凭空创建哈希", "ocr:job:jobX" not in fake.hashes)
    check("任务不存在 → 清出 processing", "jobX" not in fake.lists.get(w.PROC_LIST, []))


def test_reaper_does_not_reset_done():
    """看门狗同样不能把已完成的任务改回 pending（api.py 的 Lua 语义）。"""
    reset()
    fake.hashes["ocr:job:jobR1"] = {"status": "pending"}
    fake.hashes["ocr:job:jobR2"] = {"status": "done"}
    fake.lists[w.PROC_LIST] = ["jobR1", "jobR2"]

    moved, dropped = fake.eval(
        "local jid = redis.call('LINDEX', plist, -1)", 2, w.PROC_LIST, w.QUEUE, "ocr:job:"
    )

    check("看门狗重投未完成任务", moved == ["jobR1"])
    check("看门狗丢弃已完成任务", dropped == ["jobR2"])
    check("已完成任务状态保持 done", fake.hashes["ocr:job:jobR2"]["status"] == "done")


# --------------------------------------------------------------------------- #
# 3. 落库/移除顺序
# --------------------------------------------------------------------------- #
def test_persist_failure_keeps_file_and_record():
    """落库失败：返回 False、文件留着、任务仍在 processing（由 _requeue 兜）"""
    reset()
    jid = "jobA"
    path = new_job(jid)
    fake.fail_hset = True

    ok = w._finalize("ocr:job:" + jid, jid, path, {"status": "done"})

    check("落库失败 → _finalize 返回 False", ok is False)
    check("落库失败 → 文件不删", os.path.exists(path))
    check("落库失败 → 仍在 processing", jid in fake.lists.get(w.PROC_LIST, []))


def test_lrem_failure_keeps_file():
    """落库成功但移出 processing 失败：保留文件，避免日后重投覆盖成功结果"""
    reset()
    jid = "jobB"
    path = new_job(jid)
    fake.fail_lrem = True

    ok = w._finalize("ocr:job:" + jid, jid, path, {"status": "done"})

    check("LREM 失败 → 仍返回 True（已落库）", ok is True)
    check("LREM 失败 → 文件保留", os.path.exists(path))
    check("LREM 失败 → 结果已是 done", fake.hashes["ocr:job:jobB"]["status"] == "done")


# --------------------------------------------------------------------------- #
# 4. 幂等 / 正常路径 / 优雅退出
# --------------------------------------------------------------------------- #
def test_done_job_redelivered_is_idempotent():
    """已经 done 的任务被重投：不重算 OCR，也不会被改成 error"""
    reset()
    jid = "jobD"
    path = new_job(jid, status="done")

    w.process(jid)

    check("已 done 的任务不重算", calls["run_ocr"] == 0)
    check("已 done 的任务状态不变", fake.hashes["ocr:job:jobD"]["status"] == "done")
    check("已 done 的任务被清理出 processing", jid not in fake.lists.get(w.PROC_LIST, []))
    check("已 done 的任务文件被删", not os.path.exists(path))


def test_missing_file_becomes_error_not_stuck():
    """文件丢了（比如磁盘被清）：标记 error 并移出 processing，不能卡住"""
    reset()
    jid = "jobE"
    new_job(jid, status="pending", create_file=False)

    w.process(jid)

    check("文件缺失 → status=error", fake.hashes["ocr:job:jobE"]["status"] == "error")
    check("文件缺失 → 移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))


def test_happy_path_finalizes_done():
    """正常路径：跑 OCR → 写 done → 移出 processing → 删文件"""
    reset()
    jid = "jobF"
    path = new_job(jid, status="pending")

    w.process(jid)

    h = fake.hashes["ocr:job:jobF"]
    check("正常路径 → status=done", h["status"] == "done")
    check("正常路径 → 有结果", "pages" in h.get("result", ""))
    check("正常路径 → 移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))
    check("正常路径 → 文件已删", not os.path.exists(path))


def test_cleanup_requeues_and_unregisters():
    """优雅退出：剩余任务逐个原子重投，然后注销自己"""
    reset()
    j1 = new_job("jobL1", status="processing")
    j2 = new_job("jobL2", status="processing")
    fake.sadd(w.WORKERS_SET, w.WORKER_ID)

    w._cleanup()

    check("退出时两个任务都回队列",
          "jobL1" in fake.lists.get(w.QUEUE, []) and "jobL2" in fake.lists.get(w.QUEUE, []))
    check("退出时 processing 列表清空", not fake.lists.get(w.PROC_LIST))
    check("退出时已注销", w.WORKER_ID not in fake.sets.get(w.WORKERS_SET, set()))
    check("退出时已删心跳", w.HEARTBEAT_KEY not in fake.kv)


if __name__ == "__main__":
    w.TMPDIR = tempfile.mkdtemp(prefix="indra-test-")
    for fn in (
        test_hgetall_failure_requeues,
        test_processing_mark_failure_requeues_not_error,
        test_write_result_failure_requeues,
        test_requeue_does_not_clobber_done,
        test_requeue_sets_pending_atomically,
        test_requeue_missing_job_is_dropped,
        test_reaper_does_not_reset_done,
        test_persist_failure_keeps_file_and_record,
        test_lrem_failure_keeps_file,
        test_done_job_redelivered_is_idempotent,
        test_missing_file_becomes_error_not_stuck,
        test_happy_path_finalizes_done,
        test_cleanup_requeues_and_unregisters,
    ):
        print(f"\n=== {fn.__name__} ===")
        fn()
    print("\nALL PASSED")
