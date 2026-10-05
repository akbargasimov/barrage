"""UDP flood vector: scheduled packet emission with token-bucket pacing
done upstream. Each unit = one datagram (configurable payload size).
Connectionless: every send is fire-and-forget with a shared socket.
"""

from __future__ import annotations

import asyncio
import os

from .base import Vector


class _SinkProtocol(asyncio.DatagramProtocol):
    """Fire-and-forget sink: count ICMP errors, ignore incoming datagrams."""

    def __init__(self, stats):
        self.stats = stats

    def datagram_received(self, data, addr):
        self.stats.bump("bytes_recv", len(data))

    def error_received(self, exc):
        self.stats.bump("reset")


class UdpFlood(Vector):
    name = "udpflood"
    needs_udp = True

    def __init__(self, cfg, stats, worker_id):
        super().__init__(cfg, stats, worker_id)
        self._payload = os.urandom(max(16, min(cfg.udp_payload, 60_000)))
        self._transport = None

    async def background(self):
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _SinkProtocol(self.stats),
            remote_addr=(self.cfg.target.host, self.cfg.target.port),
        )

    async def run_unit(self, t: float):
        if self._transport is None:
            return
        self._transport.sendto(self._payload)
        self.stats.bump("packets")
        self.stats.bump("bytes_sent", len(self._payload))

    async def aclose(self):
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
