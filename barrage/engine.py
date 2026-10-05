"""Execution engine: open-loop pacer, multi-process workers, orchestrator.

Layout:
  _run_worker_process  — entry point for each OS process (uvloop + asyncio)
  _worker_main         — vector + scheduler + reporter inside one process
  run_attack           — orchestrates one phase (single vector), returns report
  run_campaign         — runs N phases sequentially, merges reports

Open-loop pacing: each worker ticks every TICK seconds, integrates the
offered-rate function, and fires `run_unit` tasks to match the integral —
offered load is independent of response latency by construction.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import time
from collections import deque
from typing import Deque, Dict, List, Optional

try:
    import uvloop  # type: ignore
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

from .config import AttackConfig, LoadModel
from .metrics import (REPORT_INTERVAL, WorkerStats, aggregate,
                      rates_and_quantiles)
from .brain import AnomalyDetector, CollapseFinder, VectorRecommender

TICK = 0.05          # scheduler tick, seconds
POLL = 0.25          # orchestrator poll, seconds
BASELINE_WINDOW = 3.0   # seconds to observe baseline before probe jumps
PROBE_WINDOW = 6.0      # seconds per probe rate step


# ---------------------------------------------------------------------------
# worker process
# ---------------------------------------------------------------------------
def _run_worker_process(cfg: AttackConfig, worker_id: int, vector_name: str,
                        q, probe_rate, stop_evt):
    if _UVLOOP:
        try:
            uvloop.install()
        except Exception:
            pass
    asyncio.run(_worker_main(cfg, worker_id, vector_name, q, probe_rate, stop_evt))


async def _worker_main(cfg: AttackConfig, worker_id: int, vector_name: str,
                       q, probe_rate, stop_evt):
    from .vectors.base import build_vector

    stats = WorkerStats(worker_id)
    vector = build_vector(vector_name, cfg, stats, worker_id)
    globals()["_WORKER_ID"] = worker_id

    live = {"n": 0}  # live unit-task counter
    tasks = set()

    async def _unit(t: float):
        live["n"] += 1
        try:
            await vector.run_unit(t)
        except asyncio.CancelledError:
            raise
        except Exception:
            stats.bump("error")
        finally:
            live["n"] -= 1

    def _fire(rate: float, dt: float, issued_f: float):
        """(kept for API compat) — bookkeeping lives in _pump."""
        return issued_f + rate * dt, 0

    async def _pump(rate_fn, until, measuring_evt=None):
        """Tick the scheduler, integrating the rate function into firings."""
        issued_f = 0.0
        issued = 0
        t = 0.0
        while t < until and not stop_evt.is_set():
            await asyncio.sleep(TICK)
            t += TICK
            rate = rate_fn(t)
            issued_f += rate * TICK
            n = int(issued_f) - issued
            issued = int(issued_f)
            for _ in range(n):
                if live["n"] >= cfg.max_inflight * 2:
                    stats.bump("issued")
                    stats.bump("dropped")
                    continue
                task = asyncio.create_task(_unit(t))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        if tasks:
            await asyncio.wait(tasks, timeout=5.0)

    async def warmup_phase(measuring_evt: asyncio.Event):
        """Pre-warm phase: half-rate traffic, counters not recorded."""
        try:
            if cfg.warmup <= 0:
                return
            stats.recording = False
            await _pump(lambda t: cfg.worker_rate(t, None) * 0.5,
                        min(cfg.warmup, cfg.duration))
        finally:
            stats.reset()
            stats.recording = True
            measuring_evt.set()

    async def scheduler(measuring_evt: asyncio.Event):
        await measuring_evt.wait()
        await _pump(lambda t: cfg.worker_rate(t, probe_rate.value or None),
                    cfg.duration)

    async def reporter():
        while not stop_evt.is_set():
            await asyncio.sleep(REPORT_INTERVAL)
            try:
                q.put(stats.delta())
            except Exception:
                pass

    measuring_evt = asyncio.Event()
    if cfg.warmup <= 0:
        measuring_evt.set()
    bg_task = asyncio.ensure_future(vector.background())
    try:
        await asyncio.wait_for(
            asyncio.gather(warmup_phase(measuring_evt), scheduler(measuring_evt),
                           reporter(), return_exceptions=True),
            timeout=cfg.duration + cfg.warmup + 10.0,
        )
    except asyncio.TimeoutError:
        pass
    finally:
        bg_task.cancel()
        stop_evt.set()
        try:
            await vector.aclose()
        except Exception:
            pass
        try:
            q.put(stats.final())
        except Exception:
            pass


# ---------------------------------------------------------------------------
# dashboard rendering (rich optional)
# ---------------------------------------------------------------------------
SPARK_CHARS = "▁▂▃▄▅▆▇█"
_recent_p95: Deque[float] = deque(maxlen=40)


def _spark(vals) -> str:
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1.0
    return "".join(SPARK_CHARS[min(7, int((v - lo) / rng * 7.999))]
                   for v in vals)


def _render_line(phase: str, elapsed: float, duration: float, rates: Dict,
                 active: int, note: str = "") -> str:
    if rates:
        _recent_p95.append(rates["p95_ms"])
    spark = _spark(list(_recent_p95))
    return (f"[{phase}] {elapsed:6.1f}/{duration:.0f}s "
            f"rps {rates['rps']:9.0f}  ok {rates['ok_ps']:9.0f}  "
            f"err {rates['err_ps']:7.0f}  p50 {rates['p50_ms']:7.1f}ms  "
            f"p95 {rates['p95_ms']:8.1f}ms  p99 {rates['p99_ms']:9.1f}ms  "
            f"conn {active:6d} {spark}  {note}")


def _try_rich_dashboard():
    try:
        from rich.live import Live
        from rich.table import Table
        return Live, Table
    except ImportError:
        return None, None


# ---------------------------------------------------------------------------
# orchestrator: one phase
# ---------------------------------------------------------------------------
def run_attack(cfg: AttackConfig, vector_name: str,
               quiet: bool = False) -> Dict:
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    probe_rate = ctx.Value("d", 0.0)
    stop_evt = ctx.Event()

    if cfg.load.pattern == "probe":
        probe_rate.value = cfg.probe_start
        globals()["_finder_holder"] = None  # fresh finder per phase

    procs = []
    for i in range(cfg.processes):
        p = ctx.Process(target=_run_worker_process,
                        args=(cfg, i, vector_name, q, probe_rate, stop_evt),
                        daemon=True)
        procs.append(p)

    for p in procs:
        p.start()

    cum = {"counters": {}}
    prev_snap: Optional[Dict] = None
    history: List[Dict] = []
    lat_pool: List[float] = []
    detector = AnomalyDetector()
    finder: Optional[CollapseFinder] = None
    baseline_p95: Optional[float] = None
    baseline_samples: List[float] = []
    last_note = ""
    alarms: List[str] = []

    if cfg.metrics_port:
        from .prom import start_metrics_server, update_metrics
        start_metrics_server(cfg.metrics_port)
    else:
        update_metrics = None

    Live, Table = _try_rich_dashboard()
    use_rich = Live is not None and not quiet and not cfg.quiet

    t0 = time.monotonic()
    interrupted = False
    try:
        if use_rich:
            with Live(refresh_per_second=4) as live:
                while True:
                    elapsed = time.monotonic() - t0
                    if elapsed >= cfg.duration + cfg.warmup + 0.5:
                        break
                    time.sleep(POLL)
                    snap, rates, notes = _poll_queue(
                        q, cum, prev_snap, history, lat_pool)
                    if snap:
                        prev_snap = snap
                    if rates:
                        alarm = detector.update(rates)
                        if alarm:
                            alarms.append(f"{alarm}@{elapsed:.0f}s")
                        if cfg.load.pattern == "probe":
                            notes += _probe_step(cfg, elapsed, rates, finder,
                                                 baseline_samples, probe_rate,
                                                 history)
                        last_note = notes
                    live.update(_rich_table(Table, cfg, vector_name, elapsed,
                                            rates, last_note, history))
        else:
            while True:
                elapsed = time.monotonic() - t0
                if elapsed >= cfg.duration + cfg.warmup + 0.5:
                    break
                time.sleep(POLL)
                snap, rates, notes = _poll_queue(q, cum, prev_snap, history,
                                                 lat_pool)
                if snap:
                    prev_snap = snap
                if rates:
                    alarm = detector.update(rates)
                    if alarm:
                        alarms.append(f"{alarm}@{elapsed:.0f}s")
                    if update_metrics:
                        update_metrics(snap)
                    if cfg.load.pattern == "probe":
                        notes += _probe_step(cfg, elapsed, rates, finder,
                                             baseline_samples, probe_rate,
                                             history)
                    last_note = notes
                    if not quiet and not cfg.quiet:
                        print(_render_line(vector_name, elapsed, cfg.duration,
                                           rates, snap["active"], notes),
                              flush=True)
    except KeyboardInterrupt:
        interrupted = True
        stop_evt.set()

    stop_evt.set()
    finals = _drain_finals(q, timeout=8.0)
    for p in procs:
        p.join(timeout=5.0)
        if p.is_alive():
            p.terminate()

    # merge results: prefer per-worker cumulative finals when all arrived
    workers_final = []
    for f in finals:
        workers_final.append(f)
    total: Dict[str, int] = {}
    if len(workers_final) >= cfg.processes:
        for f in workers_final:
            for k, v in f.get("counters", {}).items():
                total[k] = total.get(k, 0) + v
    else:
        total = dict(cum["counters"])

    elapsed = time.monotonic() - t0
    report = {
        "phase": vector_name,
        "duration_planned": cfg.duration,
        "duration_actual": round(elapsed, 2),
        "interrupted": interrupted,
        "processes": cfg.processes,
        "totals": total,
        "snapshots": len(history),
        "alarms": alarms,
    }
    if lat_pool:
        lat_pool.sort()

        def qp(p):
            i = min(len(lat_pool) - 1, int(p * (len(lat_pool) - 1) + 0.5))
            return round(lat_pool[i] * 1000, 2)

        report["latency_ms"] = {"p50": qp(0.5), "p95": qp(0.95), "p99": qp(0.99)}
    if cfg.load.pattern == "probe":
        f = globals().get("_finder_holder")
        if f is not None:
            report["collapse"] = f.result()
    # downsampled time-series for post-run analysis (decay curves etc.)
    if history:
        step = max(1, len(history) // 200)
        t0m = history[0]["t"]
        report["history"] = [
            {
                "dt": round(h["t"] - t0m, 2),
                "rps": round(h["rates"].get("rps", 0.0), 1),
                "p95_ms": round(h["rates"].get("p95_ms", 0.0), 1),
                "err_rate": round(h["rates"].get("err_rate", 0.0), 4),
            }
            for h in history[::step]
        ]
    return report


def _poll_queue(q, cum: Dict, prev_snap: Optional[Dict], history: List,
                lat_pool: List):
    deltas = []
    while True:
        try:
            item = q.get_nowait()
        except Exception:
            break
        if "final" in item:
            continue
        deltas.append(item)
    if not deltas:
        return None, None, ""
    snap = aggregate(deltas, cum)
    for k, v in snap["counters"].items():
        cum["counters"][k] = v
    dt = (snap["monotonic"] - prev_snap["monotonic"]) if prev_snap else 0.0
    rates = rates_and_quantiles(snap, prev_snap, dt)
    # pool latency samples (bounded)
    for _, secs in snap.get("latencies", []):
        lat_pool.append(secs)
    if len(lat_pool) > 60_000:
        del lat_pool[: len(lat_pool) - 60_000]
    history.append({"t": snap["t"], "rates": rates, "active": snap["active"]})
    if len(history) > 10_000:
        del history[: len(history) - 10_000]

    notes_parts = []
    alarm_seen = rates.get("err_rate", 0) > 0.15 or rates.get("connfail_ps", 0) > 5
    if alarm_seen:
        notes_parts.append("⚠ degradation")
    return snap, rates, " ".join(notes_parts)


# collapse-finder handle for the CURRENT worker process (probe mode)
_finder_holder = None


def _probe_step(cfg, elapsed, rates, finder, baseline_samples, probe_rate,
                history) -> str:
    """Advance the CollapseFinder during probe-pattern runs."""
    note = ""
    p95 = rates.get("p95_ms", 0.0)
    err = rates.get("err_rate", 0.0)
    if elapsed < BASELINE_WINDOW:
        baseline_samples.append(p95)
        return " [baseline]"
    if _finder_holder is None:
        import statistics
        base = max(1.0, statistics.median(baseline_samples))
        f = CollapseFinder(
            baseline_p95=base,
            latency_tolerance=2.0,
            max_err_rate=0.10,
            min_rate=max(10.0, cfg.probe_start * 0.5),
            max_rate=cfg.probe_max,
        )
        globals()["_finder_holder"] = f
    f = _finder_holder
    # observe the effect of the CURRENT rate once per probe window
    current = probe_rate.value
    last_obs = f.history[-1] if f.history else None
    due = (last_obs is None
           or (elapsed - last_obs.get("t_elapsed", 0.0)) >= PROBE_WINDOW)
    if due:
        f.observe(current, p95, err)
        f.history[-1]["t_elapsed"] = elapsed
        nxt = f.next_rate()
        probe_rate.value = nxt
        note = f" [probe {current:.0f}→{nxt:.0f} rps]"
    return note


def _drain_finals(q, timeout: float) -> List[Dict]:
    finals = []
    deadline = time.monotonic() + timeout
    quiet_since: Optional[float] = None
    while time.monotonic() < deadline:
        got = False
        try:
            item = q.get(timeout=0.2)
            if "final" in item:
                finals.append(item)
                got = True
        except Exception:
            pass
        if got or quiet_since is None:
            quiet_since = time.monotonic() if not got else None
        elif time.monotonic() - quiet_since > 1.0:
            break
    return finals


# ---------------------------------------------------------------------------
# campaign: sequential phases
# ---------------------------------------------------------------------------
def run_campaign(cfg: AttackConfig, quiet: bool = False) -> Dict:
    phases = []
    for i, vec in enumerate(cfg.vectors):
        if not quiet:
            print(f"=== phase {i + 1}/{len(cfg.vectors)}: {vec} "
                  f"({cfg.duration:.0f}s, {cfg.processes} procs) ===",
                  flush=True)
        rep = run_attack(cfg, vec, quiet=quiet)
        phases.append(rep)
        if not quiet:
            _print_phase_summary(rep)
        if rep.get("interrupted"):
            break
        # cool-down between phases so results don't bleed into each other
        if i < len(cfg.vectors) - 1 and not quiet:
            time.sleep(2.0)

    # recommender across phases: derive per-phase rates from totals
    scores_total: Dict[str, float] = {}
    for rep in phases:
        tot = rep.get("totals", {})
        dur = max(0.001, rep.get("duration_actual", 1.0))
        rates = {
            "p95_ms": (rep.get("latency_ms") or {}).get("p95", 0.0),
            "connfail_ps": tot.get("connfail", 0) / dur,
            "packets_ps": tot.get("packets", 0) / dur,
            "resets_ps": tot.get("streams_reset", 0) / dur,
        }
        s = VectorRecommender.score(rates, tot)
        for fam, v in s.items():
            scores_total[fam] = scores_total.get(fam, 0.0) + v
    verdict = VectorRecommender.recommend(scores_total) if scores_total else None

    return {
        "target": cfg.target.url,
        "started": time.strftime("%Y-%m-%d %H:%M:%S"),
        "phases": phases,
        "family_scores": scores_total,
        "recommended_vector_family": verdict,
    }


def _print_phase_summary(rep: Dict):
    tot = rep.get("totals", {})
    lat = rep.get("latency_ms", {})
    line = (f"--- {rep['phase']}: {tot.get('issued', 0)} issued, "
            f"{tot.get('requests', 0)} done, {tot.get('success', 0)} ok, "
            f"{tot.get('error', 0)} err, {tot.get('dropped', 0)} dropped, "
            f"{tot.get('timeout', 0)} timeout, {tot.get('connfail', 0)} connfail, "
            f"{tot.get('packets', 0)} pkts, {tot.get('streams_reset', 0)} h2-resets")
    if lat:
        line += f" | p50 {lat['p50']}ms p95 {lat['p95']}ms p99 {lat['p99']}ms"
    print(line, flush=True)
    if "collapse" in rep:
        c = rep["collapse"]
        print(f"--- collapse: safe_rate={c['safe_rate']:.0f} rps, "
              f"collapse_rate={c['collapse_rate']:.0f} rps "
              f"({c['probes']} probes)", flush=True)
