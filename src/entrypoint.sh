#!/bin/sh

set -e

# Kludge:
export SRE_CELERY_BROKER_URL="${SRE_CELERY_BROKER_URL:-redis://:${REDIS_PASSWORD:-$MODULAR_SDK_MONGO_PASSWORD}@${REDIS_DOMAIN}:${REDIS_PORT}/0}"

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
  exec python main.py run --gunicorn
}

start_celeryworker() {
  log "Going to start celeryworker"
  mkdir -p /data/logs
  exec celery --app=onprem worker --hostname=worker1@%n --logfile=/data/logs/%n-%i.log --statedb=/data/worker.state --loglevel="${SRE_CELERY_LOG_LEVEL:-INFO}" --without-heartbeat --without-gossip --without-mingle -Ofair --concurrency 1 --prefetch-multiplier 1
}

start_celerybeat() {
  log "Going to start celerybeat"
  mkdir -p /data/logs
  exec celery --app=onprem beat --logfile=/data/logs/beat.log --loglevel="${SRE_CELERY_LOG_LEVEL:-INFO}"
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
