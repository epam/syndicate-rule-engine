#!/usr/bin/env bash
# Build rule-engine image and deploy to minikube with local image.
# Use when developing locally; ensures image is built on host and loaded into minikube.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "Ensuring minikube is running..."
minikube status &>/dev/null || minikube start --driver=docker --memory=3g --cpus=2

echo "Building rule-engine:local on host Docker..."
eval $(minikube docker-env -u 2>/dev/null || true)
docker build -t rule-engine:local -f "$REPO_ROOT/src/onprem/Dockerfile-opensource-uv" "$REPO_ROOT"

echo "Loading image into minikube..."
minikube image load rule-engine:local

echo "Upgrading Helm release..."
helm upgrade --install rule-engine "$SCRIPT_DIR" -f "$SCRIPT_DIR/values.yaml" \
  --set image.repository=rule-engine \
  --set image.tag=local \
  --set image.pullPolicy=Never \
  --wait

echo "Done. Check pods: kubectl get pods"
