#!/usr/bin/env python3
"""端到端自测：验证延迟代理的关键语义与试传取消语义。

前置（见 README）：
  * 示例接收端监听 127.0.0.1:19000
  * 代理网页/API 在 127.0.0.1:18000，代理 TCP 监听 127.0.0.1:17000

运行：python3 tests/selftest.py
可用环境变量覆盖：WEB_URL / PROXY_HOST / PROXY_PORT / TARGET
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import random
import socket
import struct
import sys
import time

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "17000"))
TARGET_HOST, TARGET_PORT = os.environ.get("TARGET", "127.0.0.1:19000").rsplit(":", 1)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client: httpx.AsyncClient, *, up: float, down: float,
                  target_host: str = TARGET_HOST, target_port: int | None = None):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": target_host, "target_port": int(target_port or TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down})
    r.raise_for_status()
    d = r.json()
    assert d["listening"], d
    return d


async def upload(client: httpx.AsyncClient, payload: bytes, name="blob.bin",
                 uid: str | None = None):
    kw = {}
    if uid is not None:
        kw["data"] = {"upload_id": uid}
    r = await client.post("/api/test-upload", files={"file": (name, payload)}, **kw)
    r.raise_for_status()
    return r.json()


async def cancel(client: httpx.AsyncClient, uid: str):
    return await client.post("/api/test-upload/cancel", json={"upload_id": uid})


async def find_conn(client: httpx.AsyncClient, up_bytes: int, retries: int = 30):
    """按上行字节数精确定位一条连接（先历史后活动）。"""
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if c["up"]["bytes"] == up_bytes:
                return c
        await asyncio.sleep(0.2)
    return None


async def find_conn_by_id(client: httpx.AsyncClient, cid: int | None,
                          retries: int = 30):
    """按连接 ID 定位一条连接（先历史后活动）。"""
    if cid is None:
        return None
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if c["id"] == cid:
                return c
        await asyncio.sleep(0.2)
    return None


async def main():
    async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(300.0, connect=10.0)) as client:
        # 0. 服务起来且在监听
        d = (await client.get("/api/config")).json()
        check("web api up, proxy listening", d.get("listening") is True, str(d))

        await set_cfg(client, up=300, down=300)

        # 1. 上传 1 MiB：回执须带正确字节数与 SHA-256（隐含验证：EOF 在全部
        #    待发字节之后才转发，否则接收端统计不到完整大小）
        payload = os.urandom(1024 * 1024)
        sha = hashlib.sha256(payload).hexdigest()
        t0 = time.monotonic()
        res = await upload(client, payload)
        dt = time.monotonic() - t0
        check("upload ok", res.get("ok") is True, str(res)[:300])
        check("welcome line received", bool(res.get("welcome")), str(res.get("welcome")))
        receipt = res.get("receipt", "")
        check("receipt byte count matches", f"bytes={len(payload)}" in receipt, receipt)
        check("receipt sha256 matches", sha in receipt, receipt)
        # 流水化：1 MiB = 64 个 16KiB 块，若按块累加等待需 >= 64*0.3s ≈ 19s
        check("delay pipelined (not per-chunk)", dt < 8.0, f"{dt:.2f}s for 1 MiB @300ms")

        # 2. 统计：双向字节数、64 KiB 缓冲上限
        conn = await find_conn(client, len(payload))
        check("connection stats recorded", conn is not None)
        if conn:
            check("up bytes counted", conn["up"]["bytes"] == len(payload), str(conn["up"]))
            check("down bytes counted", conn["down"]["bytes"] > 0, str(conn["down"]))
            check("up buffer peak <= 64 KiB", conn["up"]["peak"] <= 65536, str(conn["up"]["peak"]))
            check("up buffer peak reaches 64 KiB", conn["up"]["peak"] == 65536,
                  str(conn["up"]["peak"]))
            check("receipt captured in stats", sha in conn.get("receipt", ""),
                  conn.get("receipt", "")[-80:])

        # 3. 每条连接沿用建立时的配置：连接中途改配置不影响它
        await set_cfg(client, up=1500, down=1500)
        reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(reader.readline(), 30)  # 欢迎行
        data = b"snapshot-test-" + b"x" * 200
        writer.write(data)
        await writer.drain()
        await asyncio.sleep(0.3)  # 让数据进入延迟线
        await set_cfg(client, up=0, down=0)  # 连接中途改配置
        writer.write_eof()
        receipt_b = await asyncio.wait_for(reader.read(), 60)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        check("snapshot conn receipt ok", f"bytes={len(data)}".encode() in receipt_b,
              receipt_b.decode(errors="replace"))
        conn2 = await find_conn(client, len(data))
        check("connection kept establish-time config",
              conn2 is not None and conn2["up_delay_ms"] == 1500
              and conn2["down_delay_ms"] == 1500,
              f"up={conn2 and conn2['up_delay_ms']} down={conn2 and conn2['down_delay_ms']}")

        # 4. 并发上传互不影响
        await set_cfg(client, up=200, down=200)
        payloads = [os.urandom(256 * 1024) for _ in range(3)]
        results = await asyncio.gather(*(upload(client, p, f"c{i}.bin")
                                         for i, p in enumerate(payloads)))
        ok = all(r.get("ok") and hashlib.sha256(p).hexdigest() in r.get("receipt", "")
                 for r, p in zip(results, payloads))
        check("3 concurrent uploads, receipts correct", ok,
              str([r.get("receipt") for r in results]))

        # 5. 异常断连（RST）后释放队列与连接
        before = {c["id"] for c in (await client.get("/api/connections")).json()["history"]}

        def rst():
            s = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=5)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.sendall(b"reset-me")
            s.close()  # 发 RST（在途数据可能被丢弃，故不按字节数识别该连接）
        await asyncio.to_thread(rst)
        rst_conn = None
        for _ in range(30):
            d = (await client.get("/api/connections")).json()
            for c in d["history"]:
                if c["id"] not in before:
                    rst_conn = c
                    break
            if rst_conn is not None:
                break
            await asyncio.sleep(0.2)
        check("reset connection moved to history", rst_conn is not None)
        if rst_conn is not None:
            active_ids = {c["id"] for c in (await client.get("/api/connections")).json()["active"]}
            check("reset connection released from active set", rst_conn["id"] not in active_ids,
                  f"state={rst_conn['state']} error={rst_conn['error']}")

        # 6. 取消建连中的试传：目标指向黑洞地址使拨号挂起，取消应撤销拨号
        blackhole = "192.0.2.1"  # TEST-NET-1 (RFC 5737)，通常不可路由
        dial_hangs = False
        try:  # 探测本环境对黑洞地址是“挂起”还是“快速失败/意外连通”
            _r, _w = await asyncio.wait_for(asyncio.open_connection(blackhole, 9), 2)
            _w.close()
        except asyncio.TimeoutError:
            dial_hangs = True
        except Exception:
            pass
        if not dial_hangs:
            print("[skip] cancel-while-dialling: 本环境对黑洞地址的拨号不挂起，跳过")
        else:
            await set_cfg(client, up=100, down=100,
                          target_host=blackhole, target_port=9)
            uid = "cancel-dial"
            t0 = time.monotonic()
            task = asyncio.create_task(upload(client, b"x" * 4096, "dial.bin", uid))
            await asyncio.sleep(0.8)  # 让代理进入“目标建连中”
            r = await cancel(client, uid)
            check("cancel while dialling accepted",
                  r.status_code == 200 and r.json().get("ok") is True, r.text[:200])
            res = await asyncio.wait_for(task, 30)
            dt = time.monotonic() - t0
            check("dialling aborted fast (not 15s connect timeout)", dt < 10.0,
                  f"{dt:.2f}s")
            check("dial-cancel result is cancelled, not ok",
                  res.get("cancelled") is True and res.get("ok") is False,
                  str(res)[:240])
            check("dial-cancel has no receipt, delivery unknown",
                  not res.get("receipt") and res.get("delivery") == "unknown",
                  str(res)[:240])
            conn = await find_conn_by_id(client, res.get("proxy_conn_id"))
            check("dial-cancel conn recorded as cancelled with reason",
                  conn is not None and conn["state"] == "cancelled"
                  and bool(conn.get("error")),
                  f"{conn and conn['state']} {conn and conn.get('error')}")

        # 7. 取消已接通、传输中的试传；并发的另一试传不受影响
        await set_cfg(client, up=4000, down=4000)
        payload_a = os.urandom(512 * 1024)
        payload_b = os.urandom(256 * 1024)
        sha_b = hashlib.sha256(payload_b).hexdigest()
        ta = asyncio.create_task(upload(client, payload_a, "a.bin", "cancel-mid"))
        tb = asyncio.create_task(upload(client, payload_b, "b.bin", "keep-me"))
        flowing = False  # 等数据进入延迟线（上行已有字节）
        for _ in range(60):
            d = (await client.get("/api/connections")).json()
            if any(c["up"]["bytes"] > 0 for c in d["active"]):
                flowing = True
                break
            await asyncio.sleep(0.1)
        check("mid-transfer upload flowing", flowing)
        t0 = time.monotonic()
        r = await cancel(client, "cancel-mid")
        check("cancel mid-transfer accepted",
              r.status_code == 200 and r.json().get("ok") is True, r.text[:200])
        res_a = await asyncio.wait_for(ta, 30)
        dt = time.monotonic() - t0
        check("mid-transfer cancelled fast (normal would take >=4s)", dt < 3.5,
              f"{dt:.2f}s")
        check("mid-cancel result is cancelled, not ok",
              res_a.get("cancelled") is True and res_a.get("ok") is False,
              str(res_a)[:240])
        check("mid-cancel sent some bytes, no receipt, delivery unknown",
              res_a.get("bytes_sent", 0) > 0 and not res_a.get("receipt")
              and res_a.get("delivery") == "unknown", str(res_a)[:240])
        # 4s 上行延迟下，取消时首批数据尚未到投递时刻 -> 已转发应为 0
        check("mid-cancel nothing delivered to target yet",
              res_a.get("proxy_up", {}).get("delivered") == 0,
              str(res_a.get("proxy_up")))
        conn_a = await find_conn_by_id(client, res_a.get("proxy_conn_id"))
        check("mid-cancel conn recorded as cancelled with reason",
              conn_a is not None and conn_a["state"] == "cancelled"
              and bool(conn_a.get("error")),
              f"{conn_a and conn_a['state']} {conn_a and conn_a.get('error')}")
        if conn_a:
            check("mid-cancel delivered <= read invariant",
                  conn_a["up"]["delivered"] <= conn_a["up"]["bytes"],
                  str(conn_a["up"]))
        res_b = await asyncio.wait_for(tb, 60)
        check("concurrent upload unaffected, receipt correct",
              res_b.get("ok") is True and sha_b in res_b.get("receipt", ""),
              str(res_b)[:240])

        # 8. 取消与回执交错
        await set_cfg(client, up=400, down=400)
        # 8a. 完整回执确认后再取消：结果仍是成功，取消无效
        uid = "cancel-late"
        payload = os.urandom(64 * 1024)
        sha = hashlib.sha256(payload).hexdigest()
        res = await upload(client, payload, "late.bin", uid)
        check("late-cancel baseline upload ok", res.get("ok") is True
              and sha in res.get("receipt", ""), str(res)[:200])
        r = await cancel(client, uid)
        check("cancel after confirmed receipt rejected",
              r.status_code == 200 and r.json().get("ok") is False
              and r.json().get("already_finished") is True, r.text[:200])
        # 8b. 未知 upload_id：404
        r = await cancel(client, "no-such-upload")
        check("cancel unknown upload id -> 404", r.status_code == 404,
              r.text[:120])
        # 8c. 随机时刻取消的交错压力：每次试传只给一个最终结果，且结果自洽
        #     —— ok 必须带完整正确回执；cancelled 必须无回执且标“结果未知”
        rng = random.Random(20261117)
        trials = 8
        consistent = 0
        outcomes = []
        for i in range(trials):
            uid = f"cancel-race-{i}"
            payload = os.urandom(64 * 1024)
            sha = hashlib.sha256(payload).hexdigest()
            task = asyncio.create_task(upload(client, payload, f"r{i}.bin", uid))
            await asyncio.sleep(rng.uniform(0.3, 2.2))
            await cancel(client, uid)  # 可能生效，也可能因已完成而无效
            res = await asyncio.wait_for(task, 30)
            if res.get("ok"):
                ok_consistent = (sha in res.get("receipt", "")
                                 and not res.get("cancelled"))
                outcomes.append("ok" if ok_consistent else "BAD-ok")
            else:
                ok_consistent = (res.get("cancelled") is True
                                 and not res.get("receipt")
                                 and res.get("delivery") == "unknown")
                outcomes.append("cancelled" if ok_consistent else "BAD-cancel")
            consistent += bool(ok_consistent)
        check("race trials each give one consistent final result",
              consistent == trials, f"{consistent}/{trials} {outcomes}")

        # 9. 取消测试之后，正常链路（延迟/缓冲/半关闭取回执）仍工作
        await set_cfg(client, up=300, down=300)
        payload = os.urandom(128 * 1024)
        sha = hashlib.sha256(payload).hexdigest()
        res = await upload(client, payload, "post-cancel.bin")
        check("normal upload still works after cancels",
              res.get("ok") is True and sha in res.get("receipt", ""),
              str(res)[:200])

        await set_cfg(client, up=300, down=300)  # 恢复默认

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
