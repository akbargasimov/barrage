"""Distributed mode: remote agents + controller.

  barrage agent --port 7000            # run on each load-generator box
  barrage cluster http://a:7000 http://b:7000 <same options as run>
                                        # fan out, merge reports

Agents expose POST /run (JSON attack config) and GET /health. Each agent
executes the phase with its own local worker processes — controller merges
totals and latency quantiles.

v0.1 note: in probe mode each agent runs its own CollapseFinder instance;
brackets converge independently. For strictly synchronized probing, run
single-agent probe campaigns.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List

from .config import AttackConfig, LoadModel, Target
from .engine import run_attack

_AGENT_VERSION = "barrage-agent/0.1"


# ---------------------------------------------------------------------------
# agent
# ---------------------------------------------------------------------------
def _cfg_from_payload(payload: Dict) -> AttackConfig:
    tgt = Target.parse(payload["target"])
    load = LoadModel(**payload.get("load", {}))
    return AttackConfig(
        target=tgt,
        vectors=payload.get("vectors", ["httpflood"]),
        duration=payload.get("duration", 60.0),
        processes=payload.get("processes", 2),
        connections=payload.get("connections", 128),
        load=load,
        methods=payload.get("methods", ["GET"]),
        paths=payload.get("paths", ["/"]),
        post_data=payload.get("post_data", '{"ping": 1}'),
        headers=payload.get("headers", {}),
        keepalive=payload.get("keepalive", True),
        trickle_interval=payload.get("trickle_interval", 15.0),
        slowpost_size=payload.get("slowpost_size", 1_048_576),
        udp_payload=payload.get("udp_payload", 1024),
        http2_streams=payload.get("http2_streams", 256),
        probe_start=payload.get("probe_start", 100.0),
        probe_max=payload.get("probe_max", 2_000_000.0),
        warmup=payload.get("warmup", 0.0),
        quiet=True,
    )


def make_agent_handler(busy: Dict[str, bool]):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence
            pass

        def _send(self, code: int, obj: Dict):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self._send(200, {"ok": True, "agent": _AGENT_VERSION,
                                 "busy": busy.get("running", False)})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/run":
                self._send(404, {"error": "not found"})
                return
            if busy.get("running"):
                self._send(409, {"error": "agent busy"})
                return
            try:
                n = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(n) or b"{}")
                cfg = _cfg_from_payload(payload)
            except Exception as e:
                self._send(400, {"error": f"bad payload: {e}"})
                return

            busy["running"] = True

            def work():
                try:
                    # one phase per vector, like a mini-campaign
                    reports = [run_attack(cfg, vec, quiet=True)
                               for vec in cfg.vectors]
                    busy["result"] = {"agent": self.client_address[0],
                                      "phases": reports}
                except Exception as e:
                    busy["result"] = {"agent": self.client_address[0],
                                      "error": str(e)}
                finally:
                    busy["running"] = False

            threading.Thread(target=work, daemon=True).start()
            self._send(202, {"accepted": True})

        def do_PUT(self):
            # GET-result-by-PUT: fetch finished result
            if self.path == "/result":
                if busy.get("running"):
                    self._send(202, {"status": "running"})
                elif "result" in busy:
                    self._send(200, busy.pop("result"))
                else:
                    self._send(404, {"status": "no result"})
            else:
                self._send(404, {"error": "not found"})

    return Handler


def serve_agent(port: int):
    busy: Dict[str, bool] = {}
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_agent_handler(busy))
    print(f"barrage agent listening on 0.0.0.0:{port}", flush=True)
    httpd.serve_forever()


# ---------------------------------------------------------------------------
# controller
# ---------------------------------------------------------------------------
def run_cluster(agent_urls: List[str], payload: Dict, timeout: float = 0) -> Dict:
    """Fan a run spec out to agents, wait, merge reports."""
    import urllib.request

    accepted: List[str] = []
    for url in agent_urls:
        url = url.rstrip("/")
        req = urllib.request.Request(
            url + "/run",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
            if body.get("accepted"):
                accepted.append(url)
    if not accepted:
        raise RuntimeError("no agent accepted the run")

    duration = float(payload.get("duration", 60.0))
    deadline = time.monotonic() + max(duration + 30.0, timeout or 0)
    results: Dict[str, Dict] = {}

    while time.monotonic() < deadline and len(results) < len(accepted):
        time.sleep(1.0)
        for url in list(accepted):
            if url in results:
                continue
            try:
                req = urllib.request.Request(url.rstrip("/") + "/result",
                                             method="PUT")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body = json.loads(resp.read())
                if resp.status == 200:
                    results[url] = body
            except Exception:
                pass  # still running or transient

    merged = _merge_reports(results)
    merged["agents"] = accepted
    merged["agents_reported"] = sorted(results)
    return merged


def _merge_reports(results: Dict[str, Dict]) -> Dict:
    phases_by_idx: Dict[int, List[Dict]] = {}
    for res in results.values():
        for i, ph in enumerate(res.get("phases", [])):
            phases_by_idx.setdefault(i, []).append(ph)

    merged_phases = []
    for idx, phs in sorted(phases_by_idx.items()):
        totals: Dict[str, int] = {}
        lat_pool = []
        for ph in phs:
            for k, v in ph.get("totals", {}).items():
                totals[k] = totals.get(k, 0) + int(v)
            lat = ph.get("latency_ms") or {}
            if lat:
                lat_pool.append(lat)
        lat = None
        if lat_pool:
            lat = {
                "p50": round(sorted(l["p50"] for l in lat_pool)[
                    len(lat_pool) // 2], 2),
                "p95": max(l["p95"] for l in lat_pool),
                "p99": max(l["p99"] for l in lat_pool),
            }
        merged_phases.append({
            "phase": phs[0].get("phase"),
            "agents": len(phs),
            "totals": totals,
            "latency_ms": lat,
            "alarms": [a for ph in phs for a in ph.get("alarms", [])][:10],
        })

    return {
        "mode": "cluster",
        "agent_results": list(results.values()),
        "phases": merged_phases,
        "finished": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
