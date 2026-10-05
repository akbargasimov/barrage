"""barrage — adaptive multi-vector stress-testing CLI.

Usage examples:
  barrage run https://your-staging.example.com --pattern linear --rate 2000 --ramp 100 --duration 5m
  barrage run http://127.0.0.1:8080 -v httpflood slowloris --pattern probe --duration 90s
  barrage run http://127.0.0.1:8080 -v http2reset --http2-streams 512 --duration 30s
  barrage run 10.0.0.5 --pattern burst --burst-period 30 --burst-len 5 --burst-factor 4

Only run against targets you own or are authorized to test.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from .config import AttackConfig, Target, parse_duration, parse_rate
from .engine import run_campaign

VECTORS = ("httpflood", "http2reset", "slowloris", "slowpost", "tcpexhaust", "udpflood")

EPILOG = ("example: barrage run http://127.0.0.1:8080 -v httpflood http2reset "
          "--pattern probe --duration 2m --rate 500 --ramp 50")


def _copy_run_args(src_parser, dst_parser):
    """Re-register every option-flag argument from `run` onto `cluster`
    (minus the positional target, which cluster takes via --target)."""
    for action in src_parser._actions:
        if action.dest in ("help", "target") or not action.option_strings:
            continue
        if any(a.dest == action.dest for a in dst_parser._actions):
            continue
        opts = {"dest": action.dest, "default": action.default}
        if action.help:
            opts["help"] = action.help
        if isinstance(action, argparse._StoreTrueAction):
            opts["action"] = "store_true"
        elif isinstance(action, argparse._AppendAction):
            opts["action"] = "append"
        else:
            if getattr(action, "type", None) is not None:
                opts["type"] = action.type
            if getattr(action, "nargs", None) is not None:
                opts["nargs"] = action.nargs
            if getattr(action, "choices", None) is not None:
                opts["choices"] = action.choices
        try:
            dst_parser.add_argument(*action.option_strings, **opts)
        except argparse.ArgumentError:
            pass


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="barrage",
        description="Adaptive multi-vector stress-testing framework. "
                    "Use only on targets you own or are authorized to test.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("agent", help="run a barrage agent (load generator node)")
    a.add_argument("--port", type=int, default=7000)

    def _warmup(sub):
        try:
            sub.add_argument("--warmup", type=float, default=0.0,
                             help="pre-warm caches at half rate, then measure clean")
        except argparse.ArgumentError:
            pass

    def _metrics(sub):
        try:
            sub.add_argument("--metrics-port", type=int, default=None,
                             help="serve Prometheus /metrics during the run")
        except argparse.ArgumentError:
            pass

    r = sub.add_parser("run", help="run a stress campaign", epilog=EPILOG,
                       formatter_class=argparse.RawDescriptionHelpFormatter)
    r.add_argument("target", help="http(s)://host[:port][/path] or host:port")
    r.add_argument("-v", "--vectors", nargs="+", choices=VECTORS,
                   default=["httpflood"],
                   help="attack vectors, run sequentially (default: httpflood)")
    r.add_argument("--duration", default="60s", help="total per phase (e.g. 90s, 5m)")
    r.add_argument("--rate", default="100", help="starting rate in units/s (e.g. 100, 2k, 0.5m)")
    r.add_argument("--ramp", default="0", help="units/s added per second")
    r.add_argument("--pattern", default="linear",
                   choices=["linear", "step", "wave", "burst", "probe"],
                   help="load shape (probe = adaptive collapse finder)")
    r.add_argument("--step-every", type=float, default=10.0)
    r.add_argument("--step-size", default="100")
    r.add_argument("--wave-period", type=float, default=30.0)
    r.add_argument("--burst-period", type=float, default=30.0)
    r.add_argument("--burst-len", type=float, default=5.0)
    r.add_argument("--burst-factor", type=float, default=3.0)
    r.add_argument("-p", "--processes", type=int, default=4)
    r.add_argument("-c", "--connections", type=int, default=256,
                   help="conns per process (httpflood pool cap / resident pool for slow vectors)")
    r.add_argument("--max-inflight", type=int, default=2048)
    r.add_argument("-X", "--methods", nargs="+", default=["GET"])
    r.add_argument("--paths", nargs="+", default=["/"])
    r.add_argument("--data", default='{"ping": 1}')
    r.add_argument("-H", "--header", action="append", default=[],
                   help="extra header 'Name: value' (repeatable)")
    r.add_argument("--no-keepalive", action="store_true")
    r.add_argument("--connect-timeout", type=float, default=3.0)
    r.add_argument("--read-timeout", type=float, default=10.0)
    r.add_argument("--trickle", type=float, default=15.0,
                   help="slowloris header-trickle interval (s)")
    r.add_argument("--slow-size", type=int, default=1_048_576,
                   help="slowpost total body size (bytes)")
    r.add_argument("--udp-size", type=int, default=1024)
    r.add_argument("--http2-streams", type=int, default=256,
                   help="stream open+reset pairs per unit (across all conns)")
    r.add_argument("--warmup", type=float, default=0.0,
                   help="pre-warm caches at half rate for N seconds, then measure clean")
    _metrics(r)
    r.add_argument("--probe-start", default="100")
    r.add_argument("--probe-max", default="2m")
    r.add_argument("--quiet", action="store_true")
    r.add_argument("--json-out", default=None, help="write full report JSON to file")
    r.add_argument("--dry-run", action="store_true", help="print config and exit")

    c = sub.add_parser("cluster", help="fan a run out to barrage agents",
                       epilog=EPILOG,
                       formatter_class=argparse.RawDescriptionHelpFormatter)
    c.add_argument("agents", nargs="+",
                   help="agent URLs, e.g. http://10.0.0.2:7000")
    c.add_argument("--target", required=True, help="target url the agents attack")
    _copy_run_args(r, c)
    _warmup(c)
    _metrics(c)
    return p


def _headers_to_dict(pairs: List[str]) -> dict:
    out = {}
    for h in pairs:
        if ":" not in h:
            raise SystemExit(f"bad header (need 'Name: value'): {h!r}")
        k, v = h.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "agent":
        from .distributed import serve_agent
        serve_agent(args.port)
        return 0

    if args.cmd == "cluster":
        import urllib.request
        from .distributed import run_cluster
        # health check first
        live = []
        for url in args.agents:
            try:
                with urllib.request.urlopen(url.rstrip("/") + "/health",
                                            timeout=5) as resp:
                    if resp.status == 200:
                        live.append(url)
            except Exception:
                pass
        if not live:
            print("error: no reachable agents", file=sys.stderr)
            return 2
        payload = _cluster_payload(args)
        print(f"dispatching to {len(live)} agent(s): {', '.join(live)}",
              flush=True)
        report = run_cluster(live, payload)
        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(report, f, indent=2, default=str)
            print(f"report written: {args.json_out}")
        _print_cluster(report)
        return 0

    try:
        target = Target.parse(args.target)
        load = {
            "linear": lambda: {},
            "step": lambda: {"step_every": args.step_every,
                             "step_size": parse_rate(args.step_size)},
            "wave": lambda: {"wave_period": args.wave_period},
            "burst": lambda: {"burst_period": args.burst_period,
                              "burst_len": args.burst_len,
                              "burst_factor": args.burst_factor},
            "probe": lambda: {},
        }[args.pattern]()
        cfg = AttackConfig(
            target=target,
            vectors=list(args.vectors),
            duration=parse_duration(args.duration),
            processes=max(1, args.processes),
            connections=max(1, args.connections),
            load=type(load, (object,), load)() if False else _load(args, load),
            methods=args.methods,
            paths=args.paths,
            post_data=args.data,
            headers=_headers_to_dict(args.header),
            keepalive=not args.no_keepalive,
            connect_timeout=args.connect_timeout,
            read_timeout=args.read_timeout,
            trickle_interval=args.trickle,
            slowpost_size=args.slow_size,
            udp_payload=args.udp_size,
            http2_streams=args.http2_streams,
            probe_start=parse_rate(args.probe_start),
            probe_max=parse_rate(args.probe_max),
            warmup=args.warmup,
            metrics_port=args.metrics_port,
            quiet=args.quiet,
            dry_run=args.dry_run,
        )
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if cfg.dry_run:
        print(json.dumps(_config_summary(cfg), indent=2))
        return 0

    report = run_campaign(cfg, quiet=args.quiet)

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"report written: {args.json_out}")

    _print_final(report)
    return 0


def _cluster_payload(args) -> dict:
    from .config import parse_duration, parse_rate
    return {
        "target": args.target,
        "vectors": list(args.vectors),
        "duration": parse_duration(args.duration),
        "processes": max(1, args.processes),
        "connections": max(1, args.connections),
        "load": {
            "rate0": parse_rate(args.rate),
            "ramp": parse_rate(args.ramp),
            "pattern": args.pattern,
            "step_every": args.step_every,
            "step_size": parse_rate(args.step_size),
            "wave_period": args.wave_period,
            "burst_period": args.burst_period,
            "burst_len": args.burst_len,
            "burst_factor": args.burst_factor,
        },
        "methods": args.methods,
        "paths": args.paths,
        "post_data": args.data,
        "headers": _headers_to_dict(args.header),
        "keepalive": not args.no_keepalive,
        "trickle_interval": args.trickle,
        "slowpost_size": args.slow_size,
        "udp_payload": args.udp_size,
        "http2_streams": args.http2_streams,
        "probe_start": parse_rate(args.probe_start),
        "probe_max": parse_rate(args.probe_max),
        "warmup": args.warmup,
    }


def _print_cluster(report: dict):
    print()
    print("=" * 62)
    print(f" cluster finished: {report.get('finished')}")
    print(f" agents reporting: {len(report.get('agents_reported', []))}")
    for ph in report.get("phases", []):
        tot = ph.get("totals", {})
        lat = ph.get("latency_ms") or {}
        print(f" [{ph['phase']}] x{ph['agents']} agents: "
              f"{tot.get('issued', 0)} issued, {tot.get('success', 0)} ok, "
              f"{tot.get('error', 0)} err, {tot.get('connfail', 0)} connfail")
        if lat:
            print(f"     latency p50 {lat['p50']}ms / p95 {lat['p95']}ms / "
                  f"p99 {lat['p99']}ms")
    print("=" * 62)


def _load(args, overrides: dict):
    from .config import LoadModel
    params = dict(
        rate0=parse_rate(args.rate),
        ramp=parse_rate(args.ramp),
        pattern=args.pattern,
    )
    params.update(overrides)
    return LoadModel(**params)


def _config_summary(cfg: AttackConfig) -> dict:
    return {
        "target": cfg.target.url,
        "vectors": cfg.vectors,
        "duration_s": cfg.duration,
        "processes": cfg.processes,
        "connections_per_process": cfg.connections,
        "pattern": cfg.load.pattern,
        "rate0": cfg.load.rate0,
        "ramp": cfg.load.ramp,
    }


def _print_final(report: dict):
    print()
    print("=" * 62)
    print(f" target: {report.get('target')}")
    print(f" started: {report.get('started')}")
    for rep in report.get("phases", []):
        tot = rep.get("totals", {})
        lat = rep.get("latency_ms", {})
        print(f" [{rep['phase']}] {tot.get('requests', 0)} units, "
              f"{tot.get('success', 0)} ok, {tot.get('error', 0)} err, "
              f"{tot.get('timeout', 0)} timeout, {tot.get('connfail', 0)} connfail")
        if lat:
            print(f"     latency p50 {lat['p50']}ms / p95 {lat['p95']}ms / "
                  f"p99 {lat['p99']}ms")
        if "collapse" in rep:
            c = rep["collapse"]
            print(f"     collapse finder: safe {c['safe_rate']:.0f} rps | "
                  f"knee {c['collapse_rate']:.0f} rps | {c['probes']} probes")
        if rep.get("alarms"):
            print(f"     anomalies: {', '.join(rep['alarms'][:5])}"
                  + (" ..." if len(rep["alarms"]) > 5 else ""))
    fam = report.get("recommended_vector_family")
    scores = report.get("family_scores") or {}
    if fam:
        top = sorted(scores.items(), key=lambda kv: -kv[1])[:3]
        print(f" recommended family: {fam} "
              f"(top: {', '.join(f'{k}={v:.2f}' for k, v in top)})")
    print("=" * 62)


if __name__ == "__main__":
    sys.exit(main())
