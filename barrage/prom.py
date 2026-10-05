"""Prometheus text-exposition exporter (stdlib only, no prometheus_client).

Serves the latest global snapshot at /metrics on cfg.metrics_port while a
campaign runs. Format: https://prometheus.io/docs/instrumenting/exposition_formats/
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional

_STATE: Dict[str, float] = {}
_LOCK = threading.Lock()

# counter name -> (prometheus name, help, type)
_COUNTER_MAP = {
    "issued": ("barrage_issued_total", "Work units scheduled", "counter"),
    "requests": ("barrage_requests_total", "Work units completed", "counter"),
    "success": ("barrage_success_total", "Successful units (2xx/3xx)", "counter"),
    "error": ("barrage_errors_total", "Failed units", "counter"),
    "timeout": ("barrage_timeouts_total", "Timeouts", "counter"),
    "connfail": ("barrage_connfail_total", "Connection failures", "counter"),
    "reset": ("barrage_resets_total", "Connections/streams reset", "counter"),
    "dropped": ("barrage_dropped_total", "Units shed by backpressure", "counter"),
    "status_2xx": ("barrage_http_2xx_total", "HTTP 2xx responses", "counter"),
    "status_3xx": ("barrage_http_3xx_total", "HTTP 3xx responses", "counter"),
    "status_4xx": ("barrage_http_4xx_total", "HTTP 4xx responses", "counter"),
    "status_5xx": ("barrage_http_5xx_total", "HTTP 5xx responses", "counter"),
    "bytes_sent": ("barrage_bytes_sent_total", "Bytes sent", "counter"),
    "bytes_recv": ("barrage_bytes_received_total", "Wire bytes received", "counter"),
    "connections": ("barrage_connections_total", "Connections opened", "counter"),
    "packets": ("barrage_packets_total", "UDP datagrams sent", "counter"),
    "streams_reset": ("barrage_h2_streams_reset_total",
                      "HTTP/2 streams opened+reset", "counter"),
}


def update_metrics(snapshot: Dict):
    """Called by the orchestrator after each poll."""
    with _LOCK:
        c = snapshot.get("counters", {})
        for k, (pname, _, _) in _COUNTER_MAP.items():
            _STATE[pname] = float(c.get(k, 0))
        _STATE["barrage_active_connections"] = float(snapshot.get("active", 0))


def render_metrics() -> str:
    lines = []
    with _LOCK:
        for pname in sorted(_STATE):
            value = _STATE[pname]
            if pname == "barrage_active_connections":
                lines.append(f"# HELP {pname} Currently open connections")
                lines.append(f"# TYPE {pname} gauge")
            else:
                meta = next((m for m in _COUNTER_MAP.values()
                             if m[0] == pname), None)
                help_txt = meta[1] if meta else pname
                lines.append(f"# HELP {pname} {help_txt}")
                lines.append(f"# TYPE {pname} counter")
            lines.append(f"{pname} {value}")
    return "\n".join(lines) + "\n"


def start_metrics_server(port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def do_GET(self):
            if self.path in ("/metrics", "/"):
                body = render_metrics().encode()
                self.send_response(200)
                self.send_header("Content-Type",
                                 "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd
