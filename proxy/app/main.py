"""FastAPI 应用：代理配置、连接统计、上传试传（可取消）、网页。"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .engine import DelayProxy, ProxyConfig

STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_CHUNK = 256 * 1024
UPLOAD_SESSION_TTL = 300.0  # 已结束试传会话的保留时间（秒）


def _env_config() -> ProxyConfig:
    return ProxyConfig(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "7000")),
        target_host=os.getenv("TARGET_HOST", "127.0.0.1"),
        target_port=int(os.getenv("TARGET_PORT", "9000")),
        up_delay_ms=float(os.getenv("UP_DELAY_MS", "300")),
        down_delay_ms=float(os.getenv("DOWN_DELAY_MS", "300")),
    )


proxy = DelayProxy(_env_config())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await proxy.apply_config(proxy.cfg)  # 启动监听器
    yield
    await proxy.shutdown()


app = FastAPI(title="TCP Delay Proxy", lifespan=lifespan)


class ConfigIn(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = Field(ge=1, le=65535)
    target_host: str = Field(min_length=1)
    target_port: int = Field(ge=1, le=65535)
    up_delay_ms: float = Field(ge=0, le=3_600_000)
    down_delay_ms: float = Field(ge=0, le=3_600_000)


class UploadSession:
    """一次网页试传：登记后可通过 /api/test-upload/cancel 取消。

    状态约定：
    * cancel_requested 一旦置位，试传协程停止读文件并中止代理 TCP 会话；
    * receipt_confirmed 一旦置位（完整回执已读到 EOF），取消不再改变结果；
    * 两个标志的先后由同一事件循环内的执行顺序决定，交错时结果唯一。
    """

    def __init__(self, upload_id: str, filename: str | None):
        self.id = upload_id
        self.filename = filename
        self.started_at = time.time()
        self.cancel_requested = False
        self.cancel_reason: str | None = None
        self.receipt_confirmed = False
        self.finalized = False
        self.local_port: int | None = None      # 试传到代理监听口的本机端口
        self.proxy_conn_id: int | None = None   # 对应的代理连接 ID
        self.result: dict | None = None


UPLOADS: dict[str, UploadSession] = {}


def _purge_sessions():
    now = time.time()
    for uid in [uid for uid, s in UPLOADS.items()
                if s.finalized and now - s.started_at > UPLOAD_SESSION_TTL]:
        UPLOADS.pop(uid, None)


class _UploadCancelled(Exception):
    """试传被取消（内部控制流）。"""


def _raise_if_cancelled(session: UploadSession):
    if session.cancel_requested:
        raise _UploadCancelled()


async def _abort_proxy_conn(session: UploadSession, reason: str,
                            timeout: float = 3.0) -> bool:
    """中止该试传对应的代理 TCP 会话（建连中撤销拨号，已接通立即终止）。

    代理侧登记连接可能略晚于本端 connect 返回，故按本机端口查找时短暂重试。
    若连接已自行结束（abort 返回 False），视为目的已达。"""
    deadline = time.monotonic() + timeout
    while True:
        if session.proxy_conn_id is not None:
            proxy.abort(session.proxy_conn_id, reason)
            return True
        if session.local_port is not None:
            conn = proxy.find_by_client_port(
                session.local_port, not_before=session.started_at - 1.0)
            if conn is not None:
                session.proxy_conn_id = conn.info.id
                conn.abort(reason)
                return True
        if session.finalized or time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.05)


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config():
    snap = proxy.snapshot()
    return {k: snap[k] for k in ("config", "listening", "listen_error")}


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    await proxy.apply_config(ProxyConfig(**cfg.model_dump()))
    snap = proxy.snapshot()
    return {"ok": snap["listening"],
            **{k: snap[k] for k in ("config", "listening", "listen_error")}}


@app.get("/api/connections")
async def connections():
    return proxy.snapshot()


@app.post("/api/history/clear")
async def clear_history():
    proxy.history.clear()
    return {"ok": True}


async def _read_to_eof(reader: asyncio.StreamReader) -> bytes:
    parts = []
    while True:
        data = await reader.read(65536)
        if not data:
            return b"".join(parts)
        parts.append(data)


def _cancel_result(session: UploadSession, result: dict):
    """构造“已取消”结果：已送出但未确认的数据标记为结果未知，
    不宣称目标未收件。"""
    result["cancelled"] = True
    result["receipt"] = None
    result["delivery"] = "unknown"
    sent = result["bytes_sent"]
    msg = (f"试传已取消（{session.cancel_reason or '用户取消'}）："
           f"已向代理送出 {sent} 字节，未确认完整回执，目标实际收件情况未知")
    if session.proxy_conn_id is not None:
        result["proxy_conn_id"] = session.proxy_conn_id
        info = proxy.conn_info(session.proxy_conn_id)
        if info is not None:
            result["proxy_up"] = {"read": info.up.bytes,
                                  "delivered": info.up.delivered}
            msg += (f"；代理侧已读 {info.up.bytes} 字节、已转发目标连接 "
                    f"{info.up.delivered} 字节，取消时缓冲中未转发的数据已丢弃")
    result["detail"] = msg


async def _run_upload(session: UploadSession, file: UploadFile) -> dict:
    """执行一次试传：经本代理转发文件到目标，半关闭后读回执到 EOF。"""
    cfg = proxy.cfg
    delay_s = (cfg.up_delay_ms + cfg.down_delay_ms) / 1000.0
    result: dict = {"upload_id": session.id, "file_name": session.filename,
                    "bytes_sent": 0, "ok": False, "cancelled": False}
    writer = None
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", cfg.listen_port), timeout=15)
        sock = writer.get_extra_info("sockname")
        session.local_port = sock[1] if sock else None
        _raise_if_cancelled(session)
        # 目标（示例接收端）先发的欢迎行；自定义目标可能不发，探测失败不阻塞上传
        try:
            welcome = await asyncio.wait_for(reader.readline(),
                                             timeout=max(5.0, delay_s + 5))
            result["welcome"] = welcome.decode("utf-8", "replace").strip()
        except Exception:
            result["welcome"] = None
        _raise_if_cancelled(session)
        while True:
            chunk = await file.read(UPLOAD_CHUNK)
            if not chunk:
                break
            _raise_if_cancelled(session)  # 取消后停止继续读文件
            writer.write(chunk)
            await writer.drain()
            result["bytes_sent"] += len(chunk)
        # 半关闭：目标读到 EOF 后才返回回执
        if writer.can_write_eof():
            writer.write_eof()
        else:
            writer.close()
        receipt = await asyncio.wait_for(_read_to_eof(reader),
                                         timeout=max(120.0, 2 * delay_s + 60))
        # 取消优先于确认：若取消已生效，上面的 EOF 是连接被中止造成的，
        # 读到的可能只是截断的回执，不能算“完整回执已确认”
        _raise_if_cancelled(session)
        # 完整回执已确认：此后的取消请求不再改变本次结果
        session.receipt_confirmed = True
        result["receipt"] = receipt.decode("utf-8", "replace").strip()
        result["ok"] = True
    except _UploadCancelled:
        await _abort_proxy_conn(session, session.cancel_reason or "cancelled",
                                timeout=1.0)
        _cancel_result(session, result)
    except Exception as exc:
        if session.cancel_requested:
            await _abort_proxy_conn(
                session, session.cancel_reason or "cancelled", timeout=1.0)
            _cancel_result(session, result)
        else:
            result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["duration_ms"] = round((time.monotonic() - t0) * 1000, 1)
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        session.result = result
        session.finalized = True
    return result


@app.post("/api/test-upload")
async def test_upload(file: UploadFile, upload_id: str = Form("")):
    """把上传的文件经本代理转发到目标：先读欢迎行，发完文件后半关闭，
    再读回执直到 EOF。整条路径都经过延迟代理，等同真实设备链路。
    带 upload_id 时可用 /api/test-upload/cancel 取消本次试传。"""
    snap = proxy.snapshot()
    if not snap["listening"]:
        raise HTTPException(503, f"proxy listener not running: {snap['listen_error']}")
    _purge_sessions()
    session = UploadSession(upload_id.strip() or uuid.uuid4().hex, file.filename)
    UPLOADS[session.id] = session
    try:
        return await _run_upload(session, file)
    finally:
        await file.close()


class CancelIn(BaseModel):
    upload_id: str = Field(min_length=1, max_length=128)


@app.post("/api/test-upload/cancel")
async def cancel_test_upload(body: CancelIn):
    """取消进行中的试传：服务端停止读文件并中止对应代理 TCP 会话
    （目标仍在建连则撤销拨号，已接通则终止连接），其他试传不受影响。
    若完整回执已确认（或试传已结束），取消不再生效。"""
    session = UPLOADS.get(body.upload_id)
    if session is None:
        raise HTTPException(404, "试传会话不存在（未知 upload_id 或已过期清理）")
    if session.receipt_confirmed or session.finalized:
        return {"ok": False, "already_finished": True,
                "detail": "试传已完成（回执已确认），取消未生效"
                if session.receipt_confirmed else "试传已结束，取消未生效"}
    session.cancel_requested = True
    session.cancel_reason = f"用户在网页取消试传 (upload_id={session.id})"
    aborted = await _abort_proxy_conn(session, session.cancel_reason)
    return {"ok": True, "proxy_conn_aborted": aborted,
            "proxy_conn_id": session.proxy_conn_id}
