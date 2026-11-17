#!/usr/bin/env python3
"""引擎级测试：取消（abort）语义的两个边界场景。

场景 1：目标仍在建连（拨号挂起）时取消 -> 撤销拨号，不等到 15s 连接超时。
场景 2：拨号完成与 wait_for 恢复之间的间隙取消 -> 刚接通的目标连接也
        必须被终止，不能进入泵阶段泄漏。

场景 1 在端到端自测（tests/selftest.py）里依赖真实网络的黑洞地址，环境不
支持时会跳过，这里用挂起的假拨号确定性覆盖。场景 2 通过在拨号返回前用
loop.call_soon 安排 abort 来确定性命中。

运行：python3 tests/test_cancel_dial.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "proxy"))

from app.engine import DelayProxy, ProxyConfig  # noqa: E402

TARGET = ("203.0.113.9", 9)  # TEST-NET-3，仅作为标记，不会真正拨号


async def wait_history(proxy, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not proxy.history and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert proxy.history, "connection not moved to history"
    return proxy.history[0]


async def scenario_dial_hangs():
    """目标建连中取消：撤销拨号。"""
    real_open = asyncio.open_connection
    dial_started = asyncio.Event()
    dial_cancelled = asyncio.Event()

    async def fake_open_connection(host, port, *args, **kwargs):
        if (host, port) == TARGET:  # 只拦截代理到目标的拨号，其余放行
            dial_started.set()
            try:
                await asyncio.Future()  # 永不完成：模拟丢包链路上的挂起拨号
            except asyncio.CancelledError:
                dial_cancelled.set()
                raise
            raise AssertionError("unreachable: dial should never complete")
        return await real_open(host, port, *args, **kwargs)

    asyncio.open_connection = fake_open_connection
    try:
        proxy = DelayProxy(ProxyConfig(listen_host="127.0.0.1", listen_port=0,
                                       target_host=TARGET[0], target_port=TARGET[1],
                                       up_delay_ms=0, down_delay_ms=0))
        await proxy.apply_config(proxy.cfg)
        assert proxy.listening, proxy.listen_error
        port = proxy.server.sockets[0].getsockname()[1]

        reader, writer = await real_open("127.0.0.1", port)
        await asyncio.wait_for(dial_started.wait(), 5)  # 代理已进入“目标建连中”
        assert len(proxy.active) == 1, proxy.active
        cid = next(iter(proxy.active))

        t0 = time.monotonic()
        assert proxy.abort(cid, "test: cancel while dialling") is True
        data = await asyncio.wait_for(reader.read(100), 5)  # 客户端侧应被关闭
        dt = time.monotonic() - t0
        assert data == b"", f"expected EOF on client side, got {data!r}"
        assert dt < 5, f"abort took {dt:.1f}s (connect timeout is 15s)"

        info = await wait_history(proxy)
        assert info.state == "cancelled", f"state={info.state}"
        assert info.error == "test: cancel while dialling", f"error={info.error}"
        assert info.ended_at is not None
        assert dial_cancelled.is_set(), "dial task was not cancelled"
        assert not proxy.active, "connection still in active set"

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        await proxy.shutdown()
    finally:
        asyncio.open_connection = real_open
    print("[ok ] scenario 1: cancel while dialling aborts the dial")


async def scenario_abort_at_dial_completion():
    """拨号完成瞬间取消：刚接通的目标连接也必须被终止。"""
    real_open = asyncio.open_connection
    target_got_conn = asyncio.Event()
    target_eof = asyncio.Event()

    async def target_handler(treader, twriter):
        target_got_conn.set()
        try:
            if await treader.read(100) == b"":
                target_eof.set()  # 代理侧关闭了到目标的连接
        except (ConnectionError, OSError):
            target_eof.set()
        finally:
            try:
                twriter.close()
                await twriter.wait_closed()
            except Exception:
                pass

    target = await asyncio.start_server(target_handler, "127.0.0.1", 0)
    tport = target.sockets[0].getsockname()[1]

    proxy = DelayProxy(ProxyConfig(listen_host="127.0.0.1", listen_port=0,
                                   target_host="127.0.0.1", target_port=tport,
                                   up_delay_ms=0, down_delay_ms=0))
    await proxy.apply_config(proxy.cfg)
    assert proxy.listening, proxy.listen_error
    port = proxy.server.sockets[0].getsockname()[1]

    def patched_open_connection(host, p, *args, **kwargs):
        coro = real_open(host, p, *args, **kwargs)
        if (host, p) == ("127.0.0.1", tport):
            async def wrap():
                result = await coro
                # 拨号刚完成、wait_for 尚未恢复时安排 abort，命中完成间隙
                conn = next(iter(proxy.active.values()))
                asyncio.get_running_loop().call_soon(
                    conn.abort, "test: cancel at dial completion")
                return result
            return wrap()
        return coro

    asyncio.open_connection = patched_open_connection
    try:
        reader, writer = await real_open("127.0.0.1", port)
        await asyncio.wait_for(target_got_conn.wait(), 5)  # 目标侧确实被接通
        data = await asyncio.wait_for(reader.read(100), 5)
        assert data == b"", f"expected EOF on client side, got {data!r}"
        await asyncio.wait_for(target_eof.wait(), 5)  # 刚接通的目标连接被终止

        info = await wait_history(proxy)
        assert info.state == "cancelled", f"state={info.state}"
        assert info.error == "test: cancel at dial completion", f"error={info.error}"
        assert not proxy.active, "connection still in active set"

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    finally:
        asyncio.open_connection = real_open
        await proxy.shutdown()
        target.close()
        await target.wait_closed()
    print("[ok ] scenario 2: cancel at dial completion terminates both sides")


async def main():
    await scenario_dial_hangs()
    await scenario_abort_at_dial_completion()
    print("cancel-dial engine tests PASSED")


if __name__ == "__main__":
    asyncio.run(main())
