"""Configuration model: targets, load shapes, attack run parameters."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urlparse

from . import __version__

DURATION_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(value) -> float:
    """'90s' -> 90.0, '5m' -> 300.0, '250ms' -> 0.25, 12 -> 12.0"""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h)?", s)
    if not m:
        raise ValueError(f"bad duration: {value!r}")
    return float(m.group(1)) * DURATION_UNITS[m.group(2) or "s"]


def parse_rate(value) -> float:
    """'10k' -> 10000.0, '2.5k' -> 2500.0, '500' -> 500.0"""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(k|m)?", s)
    if not m:
        raise ValueError(f"bad rate: {value!r}")
    mult = {"k": 1_000.0, "m": 1_000_000.0}.get(m.group(2) or "", 1.0)
    return float(m.group(1)) * mult


@dataclass
class Target:
    url: str
    host: str
    port: int
    scheme: str
    path: str
    use_tls: bool

    @classmethod
    def parse(cls, url: str) -> "Target":
        url = url.strip()
        if "://" not in url:
            url = "http://" + url
        p = urlparse(url)
        scheme = (p.scheme or "http").lower()
        if scheme not in ("http", "https"):
            raise ValueError(f"unsupported scheme: {scheme!r} (use http/https)")
        use_tls = scheme == "https"
        host = p.hostname
        if not host:
            raise ValueError(f"no host in url: {url!r}")
        port = p.port or (443 if use_tls else 80)
        path = p.path or "/"
        return cls(url=url, host=host, port=port, scheme=scheme, path=path,
                   use_tls=use_tls)


@dataclass
class LoadModel:
    """Open-loop arrival model: work units are SCHEDULED at computed times,
    independent of response latency. That's what makes it a load model
    instead of a closed-loop crawler: offered load stays exact even when
    the server slows down."""

    rate0: float = 100.0          # units/sec at t=0 (global across workers)
    ramp: float = 0.0             # extra units/sec per second (linear growth)
    pattern: str = "linear"       # linear | step | wave | burst | probe
    step_every: float = 10.0      # seconds per step (pattern=step)
    step_size: float = 100.0      # added units/sec each step (pattern=step)
    wave_period: float = 30.0     # seconds (pattern=wave)
    burst_period: float = 30.0    # seconds (pattern=burst)
    burst_len: float = 5.0        # seconds of high load each burst
    burst_factor: float = 3.0     # multiplier during burst

    def validate(self):
        if self.rate0 <= 0:
            raise ValueError("rate must be > 0")
        if self.pattern not in ("linear", "step", "wave", "burst", "probe"):
            raise ValueError(f"unknown pattern: {self.pattern}")
        if self.ramp < 0 or self.step_size < 0:
            raise ValueError("ramp/step_size must be >= 0")

    def global_rate(self, t: float, probe_rate: Optional[float] = None) -> float:
        """Global offered rate (units/sec) at elapsed time t."""
        if self.pattern == "step":
            r = self.rate0 + self.step_size * (int(t / self.step_every))
        elif self.pattern == "wave":
            import math
            r = self.rate0 * (1.0 + 0.5 * math.sin(2 * math.pi * t / self.wave_period)) + self.ramp * t
        elif self.pattern == "burst":
            in_burst = (t % self.burst_period) < self.burst_len
            r = self.rate0 * (self.burst_factor if in_burst else 1.0) + self.ramp * t
        elif self.pattern == "probe":
            r = probe_rate if probe_rate is not None else self.rate0
        else:  # linear
            r = self.rate0 + self.ramp * t
        return max(0.0, r)


@dataclass
class AttackConfig:
    target: Target
    vectors: List[str] = field(default_factory=lambda: ["httpflood"])
    duration: float = 60.0
    processes: int = 4
    connections: int = 256          # per process (vector-dependent meaning)
    load: LoadModel = field(default_factory=LoadModel)
    # http
    methods: List[str] = field(default_factory=lambda: ["GET"])
    paths: List[str] = field(default_factory=lambda: ["/"])
    post_data: str = '{"ping": 1}'
    headers: dict = field(default_factory=dict)
    user_agent: str = f"barrage/{__version__} (+load-test)"
    keepalive: bool = True
    connect_timeout: float = 3.0
    read_timeout: float = 10.0
    request_timeout: float = 15.0
    retries: int = 1
    max_inflight: int = 2048        # per worker
    # slowloris / slowpost
    trickle_interval: float = 15.0
    slowpost_size: int = 1_048_576
    # udp
    udp_payload: int = 1024
    # http2
    http2_streams: int = 256
    # adaptive brain
    adaptive: bool = False
    probe_start: float = 100.0
    probe_max: float = 2_000_000.0
    # measurement hygiene: pre-warm caches at half rate for N seconds,
    # then reset counters and measure clean
    warmup: float = 0.0
    # misc
    quiet: bool = False
    no_color: bool = False
    json_out: Optional[str] = None
    metrics_port: Optional[int] = None   # Prometheus text exposition
    dry_run: bool = False

    def __post_init__(self):
        self.load.validate()
        if self.processes < 1 or self.connections < 1:
            raise ValueError("processes/connections must be >= 1")
        if self.duration <= 0:
            raise ValueError("duration must be > 0")

    def worker_rate(self, t: float, probe_rate: Optional[float] = None) -> float:
        return self.load.global_rate(t, probe_rate) / self.processes
