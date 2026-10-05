"""The Brain — adaptive traffic shaping.

Three cooperating pieces:

1. CollapseFinder   — binary-searches the offered rate to find the knee of
                      the latency curve: the highest rate whose p95 latency
                      stays within a tolerance band of the baseline. This is
                      the "exact load a service degrades at" answer.
2. AnomalyDetector  — online EWMA + z-score over the live telemetry stream
                      (latency, error rate, active conns). Flags regime
                      changes so probe mode can back off before the run
                      collapses entirely.
3. VectorRecommender — given a run's telemetry, scores which vector family
                      hurt the target most (CPU-bound vs connection-bound vs
                      protocol-cost asymmetry), so the next run starts smart.

All logic is pure Python over snapshot dicts — no shared state with workers.
"""

from __future__ import annotations

import math
import time
from typing import Dict, List, Optional

from .metrics import BAD_COUNTERS


# ---------------------------------------------------------------------------
class AnomalyDetector:
    """EWMA baseline + z-score alarm per signal."""

    SIGNALS = ("p95_ms", "err_rate", "rps", "connfail_ps")

    def __init__(self, alpha: float = 0.15, z_alarm: float = 3.5,
                 warmup: int = 8):
        self.alpha = alpha
        self.z_alarm = z_alarm
        self.warmup = warmup
        self.n = 0
        self.mean: Dict[str, float] = {}
        self.var: Dict[str, float] = {}
        self.last_alarm: Optional[str] = None

    def update(self, rates: Dict[str, float]) -> Optional[str]:
        """Feed one snapshot's derived rates; returns alarm name or None."""
        self.n += 1
        alarmed = None
        for sig in self.SIGNALS:
            x = rates.get(sig, 0.0)
            if sig not in self.mean:
                self.mean[sig] = x
                self.var[sig] = max(x * x, 1e-9)
                continue
            # score FIRST against the pre-update baseline: a spike must not
            # inflate its own variance in the same step
            if self.n >= self.warmup:
                sd = math.sqrt(max(self.var[sig], 1e-12,
                                   (0.05 * abs(self.mean[sig])) ** 2))
                if sd > 0:
                    z = abs(x - self.mean[sig]) / sd
                    if z >= self.z_alarm:
                        alarmed = sig
            # then fold the observation in
            delta = x - self.mean[sig]
            self.mean[sig] += self.alpha * delta
            self.var[sig] = (1 - self.alpha) * (self.var[sig] + self.alpha * delta * delta)
        self.last_alarm = alarmed
        return alarmed


# ---------------------------------------------------------------------------
class CollapseFinder:
    """Expanding-interval binary search over offered rate.

    Runs inside `probe` pattern: orchestrator asks 'what rate should we offer
    next?', offers it for probe_interval seconds, feeds back observed p95 +
    error rate, and the finder narrows on the knee.

    Model: p95(r) is monotone-ish; knee = highest r with
        p95(r) <= baseline_p95 * latency_tolerance
        and err_rate(r) <= max_err_rate
    """

    def __init__(self, baseline_p95: float, latency_tolerance: float = 2.0,
                 max_err_rate: float = 0.05, min_rate: float = 50.0,
                 max_rate: float = 2_000_000.0):
        if baseline_p95 <= 0:
            baseline_p95 = 1.0
        self.baseline_p95 = baseline_p95
        self.lat_tol = latency_tolerance
        self.max_err = max_err_rate
        self.lo = min_rate          # known-good
        self.hi = max_rate          # known/assumed-bad (unexplored)
        self.best_ok = min_rate
        self.history: List[Dict] = []
        self.done = False
        self._tried_hi = False

    def next_rate(self) -> float:
        if self.done:
            return self.best_ok
        if not self._tried_hi:
            # first aggressive jump to bracket the collapse quickly
            self._tried_hi = True
            return min(self.hi, max(self.lo * 8, self.lo + 2000))
        if self.hi - self.lo <= max(5.0, self.lo * 0.05):
            self.done = True
            return self.best_ok
        return (self.lo + self.hi) / 2.0

    def observe(self, rate: float, p95_ms: float, err_rate: float):
        """Feed back what happened at `rate`."""
        ok = (p95_ms <= self.baseline_p95 * self.lat_tol
              and err_rate <= self.max_err)
        self.history.append({
            "rate": rate, "p95_ms": p95_ms, "err_rate": err_rate, "ok": ok,
            "t": time.time(),
        })
        if ok:
            self.lo = max(self.lo, rate)
            self.best_ok = max(self.best_ok, rate)
        else:
            self.hi = min(self.hi, rate)
            if rate <= self.lo * 1.05:
                # even low rates are failing: target is down entirely
                self.done = True
        if self.hi - self.lo <= max(5.0, self.lo * 0.05):
            self.done = True

    def result(self) -> Dict:
        return {
            "collapse_rate": self.hi,
            "safe_rate": self.best_ok,
            "baseline_p95_ms": self.baseline_p95,
            "bracket": [self.lo, self.hi],
            "probes": len(self.history),
            "history": self.history,
        }


# ---------------------------------------------------------------------------
class VectorRecommender:
    """Score vector families by observed damage-per-unit-cost from a run."""

    FAMILIES = {
        "httpflood": "request-rate",
        "http2reset": "protocol-asymmetry",
        "slowloris": "connection-hold",
        "slowpost": "connection-hold",
        "tcpexhaust": "connection-hold",
        "udpflood": "packet-rate",
    }

    @staticmethod
    def score(rates: Dict[str, float], counters: Dict[str, int]) -> Dict[str, float]:
        """Heuristic damage signature scoring. Higher = more effective."""
        bad = sum(counters.get(k, 0) for k in BAD_COUNTERS)
        total = max(1, counters.get("requests", 0) + counters.get("packets", 0)
                    + counters.get("streams_reset", 0) + counters.get("connections", 0))
        distress = bad / total
        p95 = rates.get("p95_ms", 0.0)
        connfail = rates.get("connfail_ps", 0.0)
        return {
            "request-rate": distress * 2 + (p95 / 10_000.0),
            "protocol-asymmetry": rates.get("resets_ps", 0.0) / 50_000.0 + distress * 3,
            "connection-hold": (counters.get("connections", 0) / 5_000.0)
                               + connfail * 2 + distress,
            "packet-rate": rates.get("packets_ps", 0.0) / 100_000.0 + distress,
        }

    @staticmethod
    def recommend(scores: Dict[str, float]) -> str:
        return max(scores, key=scores.get) if scores else "httpflood"
