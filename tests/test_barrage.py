"""Unit tests: config parsing, load model, metrics math, brain logic."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from barrage.config import (AttackConfig, LoadModel, Target, parse_duration,
                            parse_rate)
from barrage.metrics import WorkerStats, aggregate, rates_and_quantiles
from barrage.brain import AnomalyDetector, CollapseFinder, VectorRecommender

import pytest


# --- config ----------------------------------------------------------------
def test_parse_duration():
    assert parse_duration("90s") == 90.0
    assert parse_duration("5m") == 300.0
    assert parse_duration("250ms") == 0.25
    assert parse_duration("2h") == 7200.0
    assert parse_duration(12) == 12.0
    with pytest.raises(ValueError):
        parse_duration("bogus")


def test_parse_rate():
    assert parse_rate("10k") == 10_000.0
    assert parse_rate("2.5k") == 2500.0
    assert parse_rate("1m") == 1_000_000.0
    assert parse_rate("500") == 500.0
    with pytest.raises(ValueError):
        parse_rate("many")


def test_target_parse():
    t = Target.parse("https://example.com:8443/app")
    assert t.host == "example.com" and t.port == 8443
    assert t.use_tls and t.path == "/app"
    t2 = Target.parse("localhost:8099")
    assert t2.scheme == "http" and t2.port == 8099 and t2.path == "/"
    with pytest.raises(ValueError):
        Target.parse("ftp://x")


def test_load_model_patterns():
    lm = LoadModel(rate0=100, ramp=10)
    assert lm.global_rate(0) == 100
    assert lm.global_rate(10) == 200
    lm2 = LoadModel(rate0=100, pattern="step", step_every=10, step_size=50)
    assert lm2.global_rate(5) == 100
    assert lm2.global_rate(15) == 150
    lm3 = LoadModel(rate0=100, pattern="burst", burst_period=30, burst_len=5,
                    burst_factor=3)
    assert lm3.global_rate(1) == 300
    assert lm3.global_rate(10) == 100
    with pytest.raises(ValueError):
        LoadModel(rate0=0).validate()


def test_config_worker_rate_split():
    cfg = AttackConfig(target=Target.parse("http://x"),
                       processes=4,
                       load=LoadModel(rate0=800))
    assert cfg.worker_rate(0) == 200


# --- metrics ---------------------------------------------------------------
def test_worker_stats_delta_and_final():
    st = WorkerStats(0)
    st.bump("requests", 5)
    st.bump("success", 4)
    st.observe_latency(0.01)
    d = st.delta()
    assert d["counters"]["requests"] == 5
    d2 = st.delta()
    assert d2["counters"]["requests"] == 0  # deltas, not cumulatives
    f = st.final()
    assert f["final"] is True and f["counters"]["requests"] == 5


def test_aggregate_and_rates():
    prev = {"counters": {}}
    d1 = {"worker": 0, "t": 0, "active": 2,
          "counters": {"requests": 100, "success": 95, "error": 5},
          "latencies": [(0, 0.01), (0, 0.02)]}
    snap = aggregate([d1], prev)
    assert snap["counters"]["requests"] == 100
    rates = rates_and_quantiles(snap, None, 1.0)
    assert rates["rps"] == 0.0  # no prev snapshot -> zero rates
    prev2 = snap
    d2 = {"worker": 0, "t": 1, "active": 3,
          "counters": {"requests": 60, "success": 55, "error": 5},
          "latencies": [(1, 0.03), (1, 0.05)]}
    snap2 = aggregate([d2], prev2)
    rates2 = rates_and_quantiles(snap2, prev2, 1.0)
    assert abs(rates2["rps"] - 60.0) < 1e-9
    assert rates2["p50_ms"] == 50.0 or rates2["p50_ms"] == 30.0
    assert 0 < rates2["err_rate"] <= 1


# --- brain -----------------------------------------------------------------
def test_anomaly_detector_alarms_on_spike():
    det = AnomalyDetector(warmup=3, z_alarm=3.0)
    for _ in range(15):
        det.update({"p95_ms": 10.0, "err_rate": 0.0, "rps": 100.0,
                    "connfail_ps": 0.0})
    assert det.last_alarm is None
    det.update({"p95_ms": 500.0, "err_rate": 0.4, "rps": 5.0,
                "connfail_ps": 0.0})
    assert det.last_alarm in ("p95_ms", "err_rate", "rps")


def test_collapse_finder_bracketing():
    # simulate: knee at ~5000 rps (p95 explodes past that)
    def p95_at(r):
        return 20.0 if r < 5000 else 200.0

    cf = CollapseFinder(baseline_p95=20.0, latency_tolerance=2.0,
                        min_rate=100, max_rate=100_000)
    for _ in range(40):
        r = cf.next_rate()
        cf.observe(r, p95_at(r), 0.01 if r < 5000 else 0.5)
        if cf.done:
            break
    res = cf.result()
    assert res["safe_rate"] >= 4900
    assert res["collapse_rate"] <= 5300


def test_recommender_orders_families():
    counters = {"requests": 1000, "error": 300, "timeout": 100,
                "streams_reset": 0, "connections": 0, "packets": 0}
    rates = {"p95_ms": 900.0, "connfail_ps": 0, "resets_ps": 0,
             "packets_ps": 0}
    scores = VectorRecommender.score(rates, counters)
    top = VectorRecommender.recommend(scores)
    assert top in VectorRecommender.FAMILIES.values() or top in scores
    assert scores["request-rate"] > 0


# --- v0.2: warmup, prometheus, sparkline -----------------------------------
def test_worker_stats_warmup_reset():
    st = WorkerStats(0)
    st.bump("requests", 100)
    st.observe_latency(0.5)
    st.recording = False
    st.bump("requests", 50)          # not recorded
    assert st.counters["requests"] == 100
    st.reset()
    st.recording = True
    assert st.counters["requests"] == 0
    st.bump("requests", 7)
    assert st.counters["requests"] == 7


def test_prom_render_and_update():
    from barrage.prom import update_metrics, render_metrics
    update_metrics({"counters": {"requests": 42, "status_4xx": 3},
                    "active": 5})
    text = render_metrics()
    assert "barrage_requests_total 42.0" in text
    assert "barrage_http_4xx_total 3.0" in text
    assert "barrage_active_connections 5.0" in text
    assert "# TYPE barrage_requests_total counter" in text


def test_sparkline():
    from barrage.engine import _spark
    assert _spark([]) == ""
    s = _spark([1, 2, 3, 4, 5])
    assert len(s) == 5 and s[0] == "▁" and s[-1] == "█"
    flat = _spark([5, 5, 5])
    assert len(flat) == 3


def test_config_warmup_field():
    cfg = AttackConfig(target=Target.parse("http://x"),
                       processes=1, load=LoadModel(rate0=100), warmup=5.0)
    assert cfg.warmup == 5.0
    assert cfg.worker_rate(0) == 100
