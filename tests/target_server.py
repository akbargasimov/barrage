"""Local test target: a deliberately stressable HTTP server for verifying
barrage. Modes:
  --mode ok        normal fast responses (default)
  --mode slow      adds artificial latency that grows with load (squeeze point)
  --mode h2        HTTP/2 cleartext (h2c, requires the h2 package)
Run: python3 tests/target_server.py --port 8099 --mode slow
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time


class Session:
    __slots__ = ("conns",)

    def __init__(self):
        self.conns = 0


STATE = {"active": 0, "served": 0, "h2_streams": 0, "h2_resets": 0,
         "slow_mode": False, "h2_mode": False}
BASE_DELAY = 0.004       # 4ms base service time
CONTENTION = 0.00002     # extra ms-of-work per concurrent request


async def _handle_h2(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Minimal h2c server: counts stream-open and RST_STREAM events."""
    import h2.connection
    import h2.events
    import h2.config

    h2c = h2.connection.H2Connection(config=h2.config.H2Configuration(
        client_side=False, header_encoding="utf-8"))
    h2c.initiate_connection()
    writer.write(h2c.data_to_send())
    await writer.drain()
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            events = h2c.receive_data(data)
            out = h2c.data_to_send()
            if out:
                writer.write(out)
            for ev in events:
                if isinstance(ev, h2.events.RequestReceived):
                    STATE["h2_streams"] += 1
                    STATE["served"] += 1
                    body = json.dumps({"ok": True, "proto": "h2"}).encode()
                    h2c.send_headers(ev.stream_id, [
                        (":status", "200"),
                        ("content-type", "application/json"),
                        ("content-length", str(len(body))),
                    ])
                    h2c.send_data(ev.stream_id, body, end_stream=True)
                    STATE["active"] += 1
                    try:
                        await asyncio.sleep(BASE_DELAY)
                    finally:
                        STATE["active"] -= 1
                elif isinstance(ev, h2.events.StreamReset):
                    STATE["h2_resets"] += 1
            out = h2c.data_to_send()
            if out:
                writer.write(out)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            # read headers
            content_length = 0
            close_after = False
            first = await asyncio.wait_for(reader.readline(), timeout=30)
            if not first:
                break  # client closed the connection
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=30)
                if line in (b"\r\n", b"\n", b""):
                    break
                low = line.lower()
                if low.startswith(b"content-length:"):
                    content_length = int(line.split(b":")[1].strip() or b"0")
                elif low.startswith(b"connection:") and b"close" in low:
                    close_after = True
            if content_length:
                await asyncio.wait_for(reader.readexactly(content_length), timeout=30)

            # in-service accounting: parked keep-alive connections don't
            # count as load; only requests being serviced do (Little's law
            # then gives a clean concurrency->latency knee)
            STATE["active"] += 1
            try:
                STATE["served"] += 1
                delay = BASE_DELAY + CONTENTION * max(0, STATE["active"] - 1) ** 1.5
                if STATE["slow_mode"]:
                    delay *= 2.5
                await asyncio.sleep(delay)

                body = json.dumps({"ok": True, "active": STATE["active"],
                                   "served": STATE["served"]}).encode()
                head = (b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: application/json\r\n"
                        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                        + (b"Connection: close\r\n\r\n" if close_after
                           else b"Connection: keep-alive\r\n\r\n"))
                writer.write(head + body)
                await writer.drain()
            finally:
                STATE["active"] -= 1
            if close_after:
                break
    except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--mode", choices=["ok", "slow", "h2"], default="ok")
    args = ap.parse_args()
    STATE["slow_mode"] = args.mode == "slow"
    STATE["h2_mode"] = args.mode == "h2"
    handler = _handle_h2 if args.mode == "h2" else handle

    server = await asyncio.start_server(handler, "127.0.0.1", args.port,
                                        backlog=128)
    print(f"target ready on 127.0.0.1:{args.port} (mode={args.mode})",
          flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
