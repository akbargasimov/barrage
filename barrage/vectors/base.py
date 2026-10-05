"""Vector base class: one work unit = one scheduled load action.

A vector is driven by an open-loop scheduler: `run_unit()` is invoked at
times computed from the LoadModel (see engine.pacer). Vectors that hold
connections (slowloris/tcpexhaust) additionally keep a background pool of
resident connections via `background()`; their `run_unit` tops the pool up
at the configured rate.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional

from ..config import AttackConfig
from ..metrics import WorkerStats


class Vector(ABC):
    name = "base"
    needs_tls = False
    needs_udp = False

    def __init__(self, cfg: AttackConfig, stats: WorkerStats, worker_id: int):
        self.cfg = cfg
        self.stats = stats
        self.worker_id = worker_id
        self._stopped = False

    @abstractmethod
    async def run_unit(self, t: float):
        """Execute one scheduled work unit at scheduler-time t."""

    async def background(self):
        """Optional long-lived task (resident connection pools)."""

    def stop(self):
        self._stopped = True

    # -- shared helpers -------------------------------------------------
    async def _connect(self, reader_writer_factory, timeout: float):
        return await asyncio.wait_for(reader_writer_factory(), timeout=timeout)

    def _finish_unit(self, ok: bool):
        self.stats.bump("requests")
        self.stats.bump("success" if ok else "error")


def build_vector(name: str, cfg: AttackConfig, stats: WorkerStats,
                 worker_id: int) -> Vector:
    from .httpflood import HttpFlood
    from .slowloris import Slowloris, SlowPost
    from .tcpexhaust import TcpExhaust
    from .udpflood import UdpFlood
    from .http2reset import Http2Reset

    registry = {
        "httpflood": HttpFlood,
        "slowloris": Slowloris,
        "slowpost": SlowPost,
        "tcpexhaust": TcpExhaust,
        "udpflood": UdpFlood,
        "http2reset": Http2Reset,
    }
    cls = registry.get(name)
    if cls is None:
        raise ValueError(f"unknown vector: {name!r} (known: {sorted(registry)})")
    if cfg.target.use_tls and cls.needs_tls:
        # TLS is fine for http vectors; flag exists for future raw vectors.
        pass
    return cls(cfg, stats, worker_id)
