<div align="center">

# ⚡ BARRAGE

**The adaptive stress-testing framework — it doesn't just hit your service,
it *finds the exact load where your service breaks.***

[![CI](https://github.com/akbargasimov/barrage/actions/workflows/ci.yml/badge.svg)](https://github.com/akbargasimov/barrage/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-14%2F14-brightgreen)]()

*6 vectors · open-loop engine · adaptive collapse finder · distributed agents · Prometheus-native*

</div>

---

## Why another load tool?

Most tools answer *"how fast can I send requests?"* — **barrage answers a
different question: at what offered load does your service start to
degrade, and which attack vector hurts it most?**

| | hey / ab / wrk | k6 | **barrage** |
|---|---|---|---|
| Open-loop pacing (load ≠ latency) | ❌ | ⚠️ partial | ✅ rate-function scheduled |
| Adaptive collapse detection | ❌ | ❌ | ✅ binary-search + anomaly EWMA |
| Vector effectiveness ranking | ❌ | ❌ | ✅ damage-per-cost scoring |
| HTTP/2 rapid-reset vector | ❌ | ❌ | ✅ h2 stream churn |
| Connection-hold vectors (slowloris/slowpost) | ❌ | ⚠️ plugins | ✅ built-in |
| Warmup phase → clean measurements | ❌ | manual | ✅ `--warmup` |
| Prometheus endpoint built-in | ❌ | ✅ | ✅ zero-dep |
| Distributed agents | ❌ | ✅ paid cloud | ✅ self-hosted, stdlib-only |
| Dependencies | few | node+deps | **0** (optional extras only) |

## The idea in 30 seconds

```bash
$ barrage run http://staging.internal --pattern probe --duration 90s

  collapse finder: safe 1,250 rps | knee 1,388 rps | 13 probes
  recommended family: protocol-asymmetry

→ your service holds 1,250 req/s with p95 < 2× baseline.
→ past 1,388 req/s, p95 explodes. That's your capacity. Before Black Friday.
```

The **CollapseFinder** binary-searches offered rate: it probes, measures
p95/error-rate against baseline, and narrows the bracket `[safe, collapse]`
until the knee is found — automatically, in one run.

## Install

```bash
git clone https://github.com/akbargasimov/barrage.git
cd barrage
python3 -m venv .venv && ./.venv/bin/pip install -e ".[ui,http2,dev]"
./.venv/bin/python -m pytest tests/ -q     # 14 tests, green
```

## Quick start

```bash
# 1. spin up a sacrificial local target
./.venv/bin/python tests/target_server.py --port 8099 --mode slow

# 2. burst shape: 4× load spikes every 16s, live sparkline dashboard
barrage run http://127.0.0.1:8099 --pattern burst \
    --burst-period 16 --burst-len 4 --burst-factor 4 --rate 300

# 3. adaptive: find the collapse point
barrage run http://127.0.0.1:8099 --pattern probe --duration 80s

# 4. watch it in Grafana while it runs
barrage run http://127.0.0.1:8099 --pattern linear --rate 1k --metrics-port 9091
```

## Six vectors, one engine

| vector | what it exhausts | flag |
|---|---|---|
| `httpflood` | request capacity (keep-alive pool, open-loop) | `--rate` |
| `http2reset` | h2 stream-state churn (CVE-2023-44487 pattern) | `--http2-streams` |
| `slowloris` | concurrent connections via header trickles | `--trickle` |
| `slowpost` | worker threads pinned on endless bodies | `--slow-size` |
| `tcpexhaust` | accept backlog / fd table | `-c` |
| `udpflood` | packet path | `--udp-size` |

Chain them — barrage ranks which family hurt most:

```bash
barrage run http://target -v httpflood http2reset slowloris --duration 30s
# → recommended family: protocol-asymmetry (score 1.53)
```

## Load shapes

`linear` `step` `wave` `burst` `probe` — see `--pattern`. Probe mode is
the adaptive one: baseline window → expanding binary search → knee.

## Distributed

```bash
# on every load box:
barrage agent --port 7000

# from your laptop:
barrage cluster http://boxA:7000 http://boxB:7000 http://boxC:7000 \
    --target http://staging.internal --pattern burst --rate 5k --duration 2m
# → merged totals + worst-case latency quantiles across agents
```

## Honest accounting

`issued` ≠ `done` ≠ `success`. Barrage separates **scheduled**, **executed**,
**succeeded (2xx/3xx)**, **4xx/5xx**, **timed-out**, **connection-failed**,
and **shed-by-backpressure** — because a load tool that inflates its own
numbers is worse than no tool. Status-class counters, wire-byte accounting,
and gzip negotiation are built in.

## Engineering notes

- **Open-loop pacing**: units are scheduled by integrating the rate
  function — offered load is independent of response latency.
- **Multiprocess**: `spawn` + uvloop per worker; stats flow over a
  `multiprocessing.Queue` as delta payloads.
- **Backpressure**: bounded inflight semaphore + explicit `dropped`
  counter — the tool degrades honestly instead of lying.
- **Connection pooling**: Condition-based idle pool, server-`close`
  aware, retry-on-dead-conn.
- **Warmup phase**: `--warmup N` pre-warms caches at half rate, resets
  counters, then measures clean — no more "first 10 seconds look 3× worse"
  artifacts.
- **Zero dependencies** for the core; `rich` (dashboard), `h2` (HTTP/2),
  `pytest` (dev) are optional extras.

## Field notes

Battle-tested against a Cloudflare-fronted production site (with
authorization): 30 rps baseline → p50 428ms; adaptive probe correctly
identified the edge rate-limit wall at ~104 rps sustained; origin never
returned a single 5xx across ~19k requests.

## Tests & CI

14 unit tests + CI matrix (ubuntu/macos × py3.9/3.11/3.13) with a live
smoke run against the bundled target server on every push.

```bash
./.venv/bin/python -m pytest tests/ -q
```

## Legal

**Run only against systems you own or have written authorization to
test.** Unauthorized load generation is illegal in most jurisdictions
(CFAA, etc.). Barrage deliberately ships no amplification/reflection or
IP-spoofing capability.

<div align="center">
<sub>Built for engineers who'd rather find the knee on Tuesday than on Black Friday.</sub>
</div>
