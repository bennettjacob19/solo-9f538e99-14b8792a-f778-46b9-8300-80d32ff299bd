"""本地示例接收端（模拟设备/平台侧服务）。

正常模式（MODE=normal）：
1. 连接建立后立即发送一行欢迎语；
2. 持续读取直到对端 EOF（半关闭）；
3. 返回收到的字节数和 SHA-256 作为回执，然后关闭连接。

卡死模拟（用环境变量 MODE 选择，用于复现“目标停读 / 不回执”）：
* MODE=stop-read ：读满 STOP_READ_AFTER_BYTES 后停止读取但保持连接，
  代理的 64 KiB 缓冲与 TCP 窗口被顶满，上行再无实际转发（模拟目标停读）。
* MODE=no-receipt：照常读到 EOF，但不发回执、保持连接，
  代理上行已半关闭、下行再无实际转发（模拟不回执）。

卡死连接最多保持 HOLD_S 秒后自行关闭，避免进程内无限堆积。
"""
import asyncio
import hashlib
import os
import socket

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9000"))
MODE = os.getenv("MODE", "normal")  # normal | stop-read | no-receipt
STOP_READ_AFTER = int(os.getenv("STOP_READ_AFTER_BYTES", str(128 * 1024)))
HOLD_S = float(os.getenv("HOLD_S", "300"))
WELCOME = b"WELCOME tcp-delay-demo receiver; send data then half-close (EOF) to get the receipt\n"


async def _hold():
    """保持连接但不读不写（卡死），HOLD_S 后由 finally 关闭。"""
    await asyncio.sleep(HOLD_S)


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    print(f"[receiver] connection from {peer} (mode={MODE})", flush=True)
    try:
        if MODE == "stop-read":
            # 小接收缓冲：停读后 TCP 窗口很快关死，反压迅速传回代理
            sock = writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
        writer.write(WELCOME)
        await writer.drain()
        h = hashlib.sha256()
        total = 0
        if MODE == "stop-read":
            remaining = STOP_READ_AFTER
            while remaining > 0:
                chunk = await reader.read(min(65536, remaining))
                if not chunk:
                    return  # 对端提前 EOF
                remaining -= len(chunk)
            print(f"[receiver] {peer} read {STOP_READ_AFTER} bytes, now STOP reading "
                  f"(holding connection)", flush=True)
            await _hold()
            return
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break  # 对端 EOF
            h.update(chunk)
            total += len(chunk)
        if MODE == "no-receipt":
            print(f"[receiver] {peer} got EOF after {total} bytes, "
                  f"NOT sending receipt (holding connection)", flush=True)
            await _hold()
            return
        receipt = f"RECEIPT bytes={total} sha256={h.hexdigest()}\n"
        writer.write(receipt.encode())
        await writer.drain()
        print(f"[receiver] {peer} -> {total} bytes, receipt sent", flush=True)
    except (ConnectionError, OSError) as exc:
        print(f"[receiver] {peer} error: {exc}", flush=True)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"[receiver] listening on {addrs} (mode={MODE})", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
