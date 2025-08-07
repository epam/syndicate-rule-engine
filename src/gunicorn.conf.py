import os

bind = "0.0.0.0:8000"
workers = int(os.environ.get('SRE_GUNICORN_WORKERS', '3'))
worker_class = 'sync'
wsgi_app = 'onprem.api.app:make_app()'
timeout = 60
max_requests = 512
max_requests_jitter = 64
