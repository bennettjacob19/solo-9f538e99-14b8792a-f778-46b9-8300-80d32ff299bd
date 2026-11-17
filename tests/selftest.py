#!/usr/bin/env python3
"""端到端自测：验证延迟代理的关键语义（含停滞时限）。

前置（见 README）：
  * 示例接收端监听 127.0.0.1:19000
  * 代理网页/API 在 127.0.0.1:18000，代理 TCP 监听 127.0.0.1:17000
  * 本脚本自起三个“卡死目标”：19011 停读、19012 不回执、19013 迟到回执

运行：python3 tests/selftest.py
可用环境变量覆盖：WEB_URL / PROXY_HOST / PROXY_PORT / TARGET
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import struct
import sys
import time

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "17000"))
TARGET_HOST, TARGET_PORT = os.environ.get("TARGET", "127.0.0.1:19000").rsplit(":", 1)
STOP_READ_PORT = 19011   # 本脚本自起：读满阈值后停读
NO_RECEIPT_PORT = 19012  # 本脚本自起：读到 EOF 也不回执
LATE_RECEIPT_PORT = 19013  # 本脚本自起：回执迟到 5 秒

BAD_WELCOME = b"WELCOME stall-test target\n"
STOP_AFTER = 128 * 1024

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client: httpx.AsyncClient, *, up: float, down: float,
                  stall: float = 10, target_port: int | None = None):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST,
        "target_port": int(target_port if target_port is not None else TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down,
        "stall_timeout_s": stall})
    r.raise_for_status()
    d = r.json()
    assert d["listening"], d
    return d


async def upload(client: httpx.AsyncClient, payload: bytes, name="blob.bin"):
    r = await client.post("/api/test-upload", files={"file": (name, payload)})
    r.raise_for_status()
    return r.json()


async def find_conn(client: httpx.AsyncClient, up_bytes: int, retries: int = 30):
    """按上行字节数精确定位一条连接（先历史后活动）。"""
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if c["up"]["bytes"] == up_bytes:
                return c
        await asyncio.sleep(0.2)
    return None


async def wait_for_conn(client: httpx.AsyncClient, pred, timeout: float = 20.0):
    """轮询直到出现满足条件的连接记录（先历史后活动）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if pred(c):
                return c
        await asyncio.sleep(0.2)
    return None


async def get_conn_by_client(client: httpx.AsyncClient, client_str: str):
    d = (await client.get("/api/connections")).json()
    for c in d["history"] + d["active"]:
        if c["client"] == client_str:
            return c
    return None


def _close_quiet(writer):
    try:
        writer.close()
    except Exception:
        pass


async def start_bad_targets():
    """本脚本自起的卡死目标：停读 / 不回执 / 迟到回执。"""
    servers = []

    async def stop_read_handle(reader, writer):
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
        try:
            writer.write(BAD_WELCOME)
            await writer.drain()
            remaining = STOP_AFTER
            while remaining > 0:
                chunk = await reader.read(min(65536, remaining))
                if not chunk:
                    return
                remaining -= len(chunk)
            await asyncio.sleep(120)  # 停读但保持连接
        except (ConnectionError, OSError):
            pass
        finally:
            _close_quiet(writer)

    async def no_receipt_handle(reader, writer):
        try:
            writer.write(BAD_WELCOME)
            await writer.drain()
            while await reader.read(65536):
                pass
            await asyncio.sleep(120)  # 读到 EOF 也不发回执，保持连接
        except (ConnectionError, OSError):
            pass
        finally:
            _close_quiet(writer)

    async def late_receipt_handle(reader, writer):
        try:
            writer.write(BAD_WELCOME)
            await writer.drain()
            total = 0
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                total += len(chunk)
            await asyncio.sleep(5)  # 回执迟到 5 秒（停滞判定之后才发出）
            writer.write(f"RECEIPT bytes={total}\n".encode())
            await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            _close_quiet(writer)

    servers.append(await asyncio.start_server(stop_read_handle, "127.0.0.1", STOP_READ_PORT))
    servers.append(await asyncio.start_server(no_receipt_handle, "127.0.0.1", NO_RECEIPT_PORT))
    servers.append(await asyncio.start_server(late_receipt_handle, "127.0.0.1", LATE_RECEIPT_PORT))
    return servers


async def main():
    bad_servers = await start_bad_targets()
    try:
        async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(300.0, connect=10.0)) as client:
            # 0. 服务起来且在监听
            d = (await client.get("/api/config")).json()
            check("web api up, proxy listening", d.get("listening") is True, str(d))
            check("config exposes stall_timeout_s",
                  isinstance(d.get("config", {}).get("stall_timeout_s"), (int, float)), str(d.get("config")))

            await set_cfg(client, up=300, down=300, stall=10)

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
                check("forwarded bytes tracked",
                      conn["up"]["forwarded"] == len(payload)
                      and conn["down"]["forwarded"] > 0,
                      f"up_fwd={conn['up']['forwarded']} down_fwd={conn['down']['forwarded']}")
                check("no stall on healthy conn", conn.get("stall") is None)

            # 3. 每条连接沿用建立时的配置（含停滞时限）：连接中途改配置不影响它
            await set_cfg(client, up=1500, down=1500, stall=7)
            reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
            await asyncio.wait_for(reader.readline(), 30)  # 欢迎行
            data = b"snapshot-test-" + b"x" * 200
            writer.write(data)
            await writer.drain()
            await asyncio.sleep(0.3)  # 让数据进入延迟线
            await set_cfg(client, up=0, down=0, stall=1)  # 连接中途改配置
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
                  and conn2["down_delay_ms"] == 1500
                  and conn2["stall_timeout_s"] == 7,
                  f"up={conn2 and conn2['up_delay_ms']} down={conn2 and conn2['down_delay_ms']}"
                  f" stall={conn2 and conn2['stall_timeout_s']}")

            # 4. 并发上传互不影响
            await set_cfg(client, up=200, down=200, stall=10)
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

            # 6. 停滞时限取值校验：仅接受 1~30 秒
            base_cfg = {"listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
                        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
                        "up_delay_ms": 100, "down_delay_ms": 100}
            for bad in (0, 0.5, 31, 100):
                r = await client.post("/api/config", json={**base_cfg, "stall_timeout_s": bad})
                check(f"stall_timeout_s={bad} rejected", r.status_code == 422,
                      f"HTTP {r.status_code}")
            for good in (1, 30):
                r = await client.post("/api/config", json={**base_cfg, "stall_timeout_s": good})
                check(f"stall_timeout_s={good} accepted",
                      r.status_code == 200 and r.json().get("listening") is True,
                      f"HTTP {r.status_code}")

            # 7. 延迟期内不误判：上行延迟 3s > 停滞时限 1s，数据在延迟线里躺 3s
            #    仍须正常送达并拿到回执（延迟期结束后无进展才最多再等一个时限）
            await set_cfg(client, up=3000, down=0, stall=1)
            reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
            c7 = f"{writer.get_extra_info('sockname')[0]}:{writer.get_extra_info('sockname')[1]}"
            await asyncio.wait_for(reader.readline(), 10)  # 欢迎行（下行无延迟）
            data7 = b"delay-ok-" + b"d" * 200
            writer.write(data7)
            await writer.drain()
            writer.write_eof()
            t0 = time.monotonic()
            receipt7 = await asyncio.wait_for(reader.read(), 15)  # 读到 EOF
            dt7 = time.monotonic() - t0
            writer.close()
            check("no misjudgement inside delay period (receipt ok)",
                  f"bytes={len(data7)}".encode() in receipt7,
                  f"{dt7:.2f}s receipt={receipt7.decode(errors='replace')!r}")
            check("delay actually elapsed (> stall timeout)", dt7 > 1.5, f"{dt7:.2f}s")
            conn7 = await wait_for_conn(client, lambda c: c["client"] == c7 and c["state"] != "active")
            check("delay-period conn closed normally (not stalled)",
                  conn7 is not None and conn7["state"] == "closed" and conn7.get("stall") is None,
                  f"state={conn7 and conn7['state']} stall={conn7 and conn7.get('stall')}")

            # 8. 目标不回执：等回执阶段停滞；网页试传显示未确认
            await set_cfg(client, up=100, down=100, stall=2, target_port=NO_RECEIPT_PORT)
            payload8 = os.urandom(100 * 1024)
            t0 = time.monotonic()
            res8 = await upload(client, payload8, "noreceipt.bin")
            dt8 = time.monotonic() - t0
            check("no-receipt upload unconfirmed",
                  res8.get("unconfirmed") is True and res8.get("ok") is not True, str(res8)[:300])
            pc8 = res8.get("proxy_conn") or {}
            check("no-receipt conn stalled", pc8.get("state") == "stalled", str(pc8)[:300])
            stall8 = pc8.get("stall") or {}
            check("stall phase = receipt (等回执)", stall8.get("phase") == "receipt", str(stall8))
            check("stall up_forwarded = full payload",
                  stall8.get("up_forwarded") == len(payload8),
                  f"{stall8.get('up_forwarded')} vs {len(payload8)}")
            check("stall down_forwarded = welcome only",
                  stall8.get("down_forwarded") == len(BAD_WELCOME),
                  str(stall8.get("down_forwarded")))
            check("stall timeout snapshot recorded", stall8.get("timeout_s") == 2, str(stall8))
            check("stall cut the wait short", dt8 < 30, f"{dt8:.2f}s")
            if pc8.get("id") is not None:
                d = (await client.get("/api/connections")).json()
                active_ids = {c["id"] for c in d["active"]}
                hist = {c["id"]: c for c in d["history"]}
                check("stalled conn released from active set", pc8["id"] not in active_ids)
                check("stalled conn in history with stall record",
                      pc8["id"] in hist and hist[pc8["id"]]["state"] == "stalled"
                      and (hist[pc8["id"]].get("stall") or {}).get("phase") == "receipt",
                      str(hist.get(pc8["id"], {}))[:200])

            # 9. 目标停读：上行阶段停滞；64 KiB 缓冲顶住；其他连接照常传输
            await set_cfg(client, up=50, down=50, stall=2, target_port=STOP_READ_PORT)
            reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
            c9 = f"{writer.get_extra_info('sockname')[0]}:{writer.get_extra_info('sockname')[1]}"
            await asyncio.wait_for(reader.readline(), 10)  # 欢迎行

            async def blast():
                try:
                    for _ in range(512):  # 8 MiB，远超 64 KiB 应用缓冲与内核缓冲
                        writer.write(b"z" * 16384)
                        await writer.drain()
                except (ConnectionError, OSError, asyncio.CancelledError):
                    pass
            bt = asyncio.create_task(blast())
            await asyncio.sleep(0.2)  # 让停读连接先卡住
            # 卡死连接占着代理时，新连接（指向正常接收端）照常传输
            await set_cfg(client, up=100, down=100, stall=2)
            payload9 = os.urandom(200 * 1024)
            sha9 = hashlib.sha256(payload9).hexdigest()
            res9 = await upload(client, payload9, "normal.bin")
            check("other conn transfers normally while one is stuck",
                  res9.get("ok") is True and sha9 in res9.get("receipt", ""),
                  str(res9)[:200])
            conn9 = await wait_for_conn(
                client, lambda c: c["client"] == c9 and c["state"] == "stalled", timeout=25)
            check("stop-read conn stalled", conn9 is not None)
            if conn9:
                stall9 = conn9.get("stall") or {}
                check("stall phase = up (上行)", stall9.get("phase") == "up", str(stall9))
                check("up_forwarded >= stop-read threshold",
                      stall9.get("up_forwarded", 0) >= STOP_AFTER,
                      str(stall9.get("up_forwarded")))
                check("up_forwarded < 8 MiB (rest stuck in proxy)",
                      stall9.get("up_forwarded", 0) < 8 * 1024 * 1024,
                      str(stall9.get("up_forwarded")))
                check("up buffer peak still 64 KiB", conn9["up"]["peak"] == 65536,
                      str(conn9["up"]["peak"]))
                check("down_forwarded = welcome only",
                      stall9.get("down_forwarded") == len(BAD_WELCOME),
                      str(stall9.get("down_forwarded")))
            bt.cancel()
            await asyncio.gather(bt, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

            # 10. 迟到回执不覆盖停滞结果
            await set_cfg(client, up=100, down=100, stall=2, target_port=LATE_RECEIPT_PORT)
            reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
            c10 = f"{writer.get_extra_info('sockname')[0]}:{writer.get_extra_info('sockname')[1]}"
            await asyncio.wait_for(reader.readline(), 10)  # 欢迎行
            writer.write(b"late-" + b"x" * 60000)
            await writer.drain()
            writer.write_eof()
            conn10 = await wait_for_conn(
                client, lambda c: c["client"] == c10 and c["state"] == "stalled", timeout=25)
            check("late-receipt conn stalled in receipt phase",
                  conn10 is not None and (conn10.get("stall") or {}).get("phase") == "receipt",
                  str(conn10 and conn10.get("stall")))
            down_fwd_at_stall = (conn10.get("stall") or {}).get("down_forwarded") if conn10 else None
            await asyncio.sleep(4.5)  # 等迟到回执（5 秒）发出并撞上已关闭的连接
            conn10b = await get_conn_by_client(client, c10)
            check("late receipt did not overwrite stall result",
                  conn10b is not None and conn10b["state"] == "stalled"
                  and (conn10b.get("stall") or {}).get("phase") == "receipt",
                  f"state={conn10b and conn10b['state']}")
            check("no late receipt bytes forwarded after stall",
                  conn10b is not None
                  and conn10b["down"]["forwarded"] == down_fwd_at_stall,
                  f"at_stall={down_fwd_at_stall} now={conn10b and conn10b['down']['forwarded']}")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

            await set_cfg(client, up=300, down=300, stall=10)  # 恢复默认
    finally:
        for s in bad_servers:
            s.close()

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
