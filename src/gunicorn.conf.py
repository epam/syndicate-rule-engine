import gc
import os

bind = "0.0.0.0:8000"
workers = int(os.environ.get('SRE_GUNICORN_WORKERS', '3'))
worker_class = 'sync'
wsgi_app = 'onprem.api.app:make_app()'
timeout = int(os.environ.get('SRE_GUNICORN_WORKERS_TIMEOUT', '60'))
max_requests = 512
max_requests_jitter = 64


def on_starting(server):
    # Import Cloud Custodian providers once in the master so that forked
    # workers share these pages (copy-on-write) instead of each worker
    # importing ~200MB of providers on its first report request.
    try:
        from services.resources import load_cc_providers

        load_cc_providers()
    except Exception:
        server.log.exception('Could not preload Cloud Custodian providers')
    # Exclude preloaded objects from workers' GC scans: scanning writes to
    # object headers, which copies the shared pages into every worker.
    gc.collect()
    gc.freeze()
