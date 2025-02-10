#!/bin/sh

set -e

# Kludge:
export CAAS_CELERY_BROKER_URL="redis://:${REDIS_PASSWORD:-$modular_mongo_password}@${REDIS_DOMAIN}:${REDIS_PORT}/0"

log() { echo "[INFO] $(date) $1" >&2; }

start_server() {
  log "Creating the necessary buckets in Minio"
  python main.py create_buckets

  log "Creating indexes in MongoDB"
  python main.py create_indexes

  log "Creating the necessary engine and token in Vault"
  python main.py init_vault

  log "Initializing"
  python main.py init

  log "Starting server"
  python main.py run --gunicorn
}

start_celeryworker() {
  log "Going to start celeryworker"
  celery -A onprem worker

}

start_celerybeat() {
  log "Going to start celerybeat"
  celery -A onprem beat
}

case "$1" in
  celerybeat)
    start_celerybeat
    ;;
  celeryworker)
    start_celeryworker
    ;;
  *)
    start_server
    ;;
esac
