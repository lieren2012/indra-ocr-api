"""worker.py 恢复逻辑单元测试（无需真实 Redis）。

用内存假 Redis 模拟，重点验证三条不该被破坏的规则：
  1. 结果没写进 Redis → 绝不能删文件、不能移出 processing；
  2. 移出 processing 失败 → 保留文件，避免日后重投报"文件不存在"并覆盖成功结果；
  3. 已经 done 的任务被重投 → 只清理，不重算、不改回 error。

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
        self.fail_hset = False
        self.fail_lrem = False

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
        h = self.hashes.setdefault(name, {})
        if mapping:
            h.update(mapping)
        elif key is not None:
            h[key] = value

    def hgetall(self, name):
        return dict(self.hashes.get(name, {}))

    def hdel(self, name, *fields):
        h = self.hashes.get(name, {})
        for f in fields:
            h.pop(f, None)

    def expire(self, name, ttl):
        return True

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

    # --- 脚本：按出现的关键命令区分两个 Lua ---
    def eval(self, script, numkeys, *args):
        if "LMOVE" in script:              # 看门狗：把 processing 整体搬回队列
            plist, queue = args[0], args[1]
            moved = []
            while self.lists.get(plist):
                jid = self.lists[plist].pop(0)
                self.lists.setdefault(queue, []).append(jid)
                moved.append(jid)
            return moved
        if "RPUSH" in script:              # worker 重投：先入队再移出
            plist, queue, jid = args[0], args[1], args[2]
            if jid not in self.lists.get(plist, []):
                return 0
            self.lists.setdefault(queue, []).append(jid)
            self.lists[plist] = [x for x in self.lists[plist] if x != jid]
            return 1
        raise AssertionError("unexpected script: %r" % script[:60])


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
    fake.fail_lrem = False
    calls["run_ocr"] = 0


def check(name, cond, detail=""):
    if not cond:
        print(f"[FAIL] {name} {detail}")
        raise SystemExit(1)
    print(f"[ok]   {name}")


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
    check("LREM 失败 → 结果已是 done",
          fake.hashes["ocr:job:jobB"]["status"] == "done")


def test_requeue_puts_job_back():
    """写不进 Redis 时能被重新投递，而不是卡死在某个 worker 手里"""
    reset()
    jid = "jobC"
    new_job(jid, status="processing")

    w._requeue(jid)

    check("重投后回到队列", jid in fake.lists.get(w.QUEUE, []))
    check("重投后移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))
    check("重投后状态回到 pending", fake.hashes["ocr:job:jobC"]["status"] == "pending")
    check("重投后清掉 processing_since", "processing_since" not in fake.hashes["ocr:job:jobC"])


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


def test_write_failure_requeues_instead_of_sticking():
    """用户报告的 bug 1：写结果失败后任务卡在本 worker 手里、看门狗又不回收。
    正确行为：process() 主动重投回队列，别的 worker 能重新捡起来。"""
    reset()
    jid = "jobG"
    new_job(jid, status="pending")
    fake.fail_hset = True          # 结果写不进 Redis

    w.process(jid)

    check("写结果失败 → 任务回到队列", jid in fake.lists.get(w.QUEUE, []))
    check("写结果失败 → 移出 processing", jid not in fake.lists.get(w.PROC_LIST, []))
    check("写结果失败 → 状态回 pending", fake.hashes["ocr:job:jobG"]["status"] == "pending")


if __name__ == "__main__":
    w.TMPDIR = tempfile.mkdtemp(prefix="indra-test-")
    for fn in (
        test_persist_failure_keeps_file_and_record,
        test_lrem_failure_keeps_file,
        test_requeue_puts_job_back,
        test_write_failure_requeues_instead_of_sticking,
        test_done_job_redelivered_is_idempotent,
        test_missing_file_becomes_error_not_stuck,
        test_happy_path_finalizes_done,
    ):
        print(f"\n=== {fn.__name__} ===")
        fn()
    print("\nALL PASSED")
