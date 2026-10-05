"""Per-worker statistics + aggregation into orchestrator-side history.

Design: workers keep plain-Python local counters (fast, no shared-memory
races) and push small delta payloads through a multiprocessing queue every
REPORT_INTERVAL seconds. The orchestrator aggregates into a global
time-series used by the dashboard and the adaptive brain.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Tuple

REPORT_INTERVAL = 0.5
LATENCY_SAMPLE_BUDGET = 800   # max latency samples per delta payload

COUNTERS = (
    "issued", "requests", "success", "error", "timeout", "connfail", "reset",
    "bytes_sent", "bytes_recv", "retries", "packets", "streams_reset",
    "connections", "dropped", "status_2xx", "status_3xx", "status_4xx",
    "status_5xx",
)
# counters where "more" is bad (used by brain)
BAD_COUNTERS = ("error", "timeout", "connfail", "reset")


@dataclass
class WorkerStats:
    worker_id: int
    counters: Dict[str, int] = field(default_factory=lambda: {k: 0 for k in COUNTERS})
    last_counters: Dict[str, int] = field(default_factory=lambda: {k: 0 for k in COUNTERS})
    latencies: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=20_000))
    conn_active: int = 0
    recording: bool = True
    started: float = field(default_factory=time.monotonic)

    def bump(self, name: str, n: int = 1):
        if not self.recording:
            return
        self.counters[name] = self.counters.get(name, 0) + n

    def observe_latency(self, seconds: float):
        if not self.recording:
            return
        self.latencies.append((time.monotonic(), seconds))

    def reset(self):
        """Zero all counters/latencies (end of warmup -> clean measurement)."""
        self.counters = {k: 0 for k in self.counters}
        self.last_counters = {k: 0 for k in self.last_counters}
        self.latencies.clear()
        self.started = time.monotonic()

    def delta(self) -> Dict:
        """Counters since last delta + a sample of recent latencies."""
        now = time.monotonic()
        d = {k: self.counters[k] - self.last_counters[k] for k in self.counters}
        self.last_counters = dict(self.counters)
        lat = list(self.latencies)
        if len(lat) > LATENCY_SAMPLE_BUDGET:
            lat = lat[-LATENCY_SAMPLE_BUDGET:]
        return {
            "worker": self.worker_id,
            "t": now,
            "counters": d,
            "active": self.conn_active,
            "latencies": lat,
        }

    def final(self) -> Dict:
        return {
            "final": True,
            "worker": self.worker_id,
            "counters": dict(self.counters),
            "active": self.conn_active,
            "elapsed": time.monotonic() - self.started,
        }


def aggregate(deltas: List[Dict], prev: Dict) -> Dict:
    """Fold worker deltas into a global snapshot. `prev` carries cumulative
    counters; returns a new snapshot dict."""
    counters = {k: prev.get("counters", {}).get(k, 0) for k in COUNTERS}
    for d in deltas:
        for k, v in d.get("counters", {}).items():
            counters[k] = counters.get(k, 0) + v
    active = sum(d.get("active", 0) for d in deltas)
    snap = {
        "t": time.time(),
        "monotonic": time.monotonic(),
        "counters": counters,
        "active": active,
        "latencies": deltas and [x for d in deltas for x in d.get("latencies", [])] or [],
    }
    return snap


def rates_and_quantiles(snap: Dict, prev: Optional[Dict], dt: float) -> Dict:
    """Derive per-second rates + latency quantiles from consecutive snapshots."""
    c = snap["counters"]
    if prev and dt > 0:
        rps = (c["requests"] - prev["counters"]["requests"]) / dt
        ok = (c["success"] - prev["counters"]["success"]) / dt
        err = (c["error"] - prev["counters"]["error"]) / dt
        to = (c["timeout"] - prev["counters"]["timeout"]) / dt
        cf = (c["connfail"] - prev["counters"]["connfail"]) / dt
        pps = (c["packets"] - prev["counters"]["packets"]) / dt
        rps_rst = (c["streams_reset"] - prev["counters"]["streams_reset"]) / dt
    else:
        rps = ok = err = to = cf = pps = rps_rst = 0.0
    lat = sorted(x[1] for x in snap.get("latencies", []))

    def q(p):
        if not lat:
            return 0.0
        i = min(len(lat) - 1, int(p * (len(lat) - 1) + 0.5))
        return lat[i]

    total_done = c["requests"] + c["packets"] + c["streams_reset"] + c["connections"]
    bad = c["error"] + c["timeout"] + c["connfail"] + c["reset"]
    return {
        "rps": rps, "ok_ps": ok, "err_ps": err, "timeout_ps": to,
        "connfail_ps": cf, "packets_ps": pps, "resets_ps": rps_rst,
        "p50_ms": q(0.50) * 1000, "p95_ms": q(0.95) * 1000, "p99_ms": q(0.99) * 1000,
        "err_rate": (bad / total_done) if total_done else 0.0,
    }
