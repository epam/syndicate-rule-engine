"""
Health check server for event sources consumer (port 8081).
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        _LOG.debug(format % args)


def run_health_server(port: int = 8081) -> None:
    """
    Start HTTP server for /healthz in a background thread.
    """
    server = HTTPServer(("", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _LOG.info("Health server started on port %s", port)
