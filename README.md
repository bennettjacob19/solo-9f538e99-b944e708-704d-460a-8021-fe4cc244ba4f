# TCP 延迟代理（慢链路“上传结束收不到回执”复现工具）

带网页的 TCP 延迟代理：可配置目标地址与上下行延迟，网页上直接上传文件试传，
实时显示每条连接的回执、双向字节数与缓冲峰值。用于在本地复现慢链路下
“上传结束却收不到回执”的问题。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

* 网页 / API：http://localhost:8000
* TCP 延迟代理监听：`localhost:7000`（可直接对接设备或客户端）
* 示例接收端：容器网络内 `receiver:9000`（先发欢迎行，读到 EOF 后回
  `RECEIPT bytes=<n> sha256=<hex>`）

网页上可修改监听地址/端口、目标地址、上下行延迟（毫秒），保存后对**之后建立
的连接**生效；已建立的连接沿用建立时的配置。修改监听地址/端口会重启监听器。

## 复现“慢链路收不到回执”

1. 把上行/下行延迟调大（例如各 2000 ms）；
2. 在“上传试传”里选一个文件开始试传；
3. 在“活动连接”里观察：上行缓冲被 64 KiB 上限顶住、EOF 等待队列冲刷后才
   半关闭、回执（下行尾部）在延迟后才到达——上传“结束”与回执到达之间的
   时间差即慢链路效应。

## 取消试传

网页上“开始试传”后可点“取消试传”撤销本次试传（例如选错了文件）：

* 浏览器发出取消后，服务端停止继续读文件，并中止本次试传对应的代理
  TCP 会话：目标仍在建连时撤销拨号，已接通时立即终止连接（队列中待发
  字节直接丢弃，不再冲刷）；其他试传/连接不受影响。
* 取消与回执到达交错时，页面只给一个最终结果：**只有完整回执已确认才
  显示成功**；否则显示“已取消”，已送出但未确认的数据标记为**结果未知**
  （不宣称目标未收件）。结果 JSON 里带 `proxy_up.read/delivered`（代理
  已读/已转发到目标连接的字节数）供参考。
* 连接记录中该次连接状态为 `cancelled`，错误列显示取消原因；字节列在
  已读与已转发不一致时会附“已转 N”。

## 关键语义（实现于 `proxy/app/engine.py`）

* **延迟起算**：每个数据块的投递时刻 = 代理读到它的时刻 + 延迟；队列按
  FIFO 投递，连续数据流水通过延迟线，不按读取块数累加等待。
* **64 KiB 应用缓冲**：每方向“已读未写”字节数上限 64 KiB，满则暂停该向
  读取（反向与别的连接不受影响）。
* **半关闭**：收到单向 EOF 后，先把该向待发字节全部写出，再 `write_eof`
  转发半关闭；反向继续传输直到结束。
* **断连释放**：正常结束或对端 RST 都会取消泵任务、清空队列、关闭两侧
  连接，并把连接移入历史表。
* **取消（abort）**：`Connection.abort(reason)` 建连中撤销拨号、已接通
  立即终止两侧连接，待发字节不冲刷；记录状态 `cancelled` 及原因。
  每方向统计含 `delivered`（已写出字节），与 `bytes`（已读）对比可知
  取消时有多少数据仍在缓冲。
* **配置快照**：每条连接在 accept 时快照配置，整条连接沿用。

## 本地开发（不用 Docker）

```bash
pip install -r proxy/requirements.txt
PORT=19000 python3 receiver/server.py &
cd proxy && LISTEN_PORT=17000 TARGET_HOST=127.0.0.1 TARGET_PORT=19000 \
  UP_DELAY_MS=300 DOWN_DELAY_MS=300 \
  uvicorn app.main:app --host 127.0.0.1 --port 18000
```

## 自测

```bash
python3 tests/selftest.py          # 需按上面方式起好接收端与代理（端口见脚本头注释）
python3 tests/test_cancel_dial.py  # 引擎级：建连中取消/拨号完成瞬间取消（独立运行，无需先起服务）
```

覆盖：回执字节数/SHA-256 校验、延迟流水化（非按块累加）、64 KiB 缓冲上限
与峰值、EOF 先冲刷再半关闭、连接级配置快照、并发上传、RST 断连释放、
取消（传输中终止连接且并发试传不受影响、取消与回执交错时结果唯一且自洽、
回执确认后取消无效、取消后正常链路仍可用；建连中撤销拨号与拨号完成瞬间
取消由 `tests/test_cancel_dial.py` 覆盖——端到端版本依赖真实网络里拨号
挂起的链路，环境不支持时自动跳过）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/config` | 当前配置与监听状态 |
| POST | `/api/config` | 更新配置（仅影响新连接） |
| GET | `/api/connections` | 活动+历史连接：双向字节数（含已转发 delivered）、缓冲当前/峰值、回执尾部 |
| POST | `/api/test-upload` |  multipart 上传文件（可带 `upload_id` 表单字段），经代理试传并返回回执 |
| POST | `/api/test-upload/cancel` | 按 `upload_id` 取消进行中的试传；已完成（回执已确认）则返回 `already_finished` |
| POST | `/api/history/clear` | 清空历史连接 |

## 目录结构

```
docker-compose.yml      # proxy + receiver 一键启动
proxy/
  Dockerfile
  requirements.txt
  app/
    main.py             # FastAPI：配置/统计/试传（可取消）/网页
    engine.py           # 延迟代理引擎（asyncio，无第三方依赖）
    static/index.html   # 网页
receiver/
  Dockerfile
  server.py             # 示例接收端：欢迎行 + EOF 后回执(字节数+SHA-256)
tests/selftest.py       # 端到端自测
```
