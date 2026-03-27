#!/usr/bin/env bash
# Local build and installation of rule-engine without S3 Helm repo.
# Does not modify Chart.yaml and Chart.lock — temporarily substitutes them and then restores.
#
# Prerequisites: Minikube running (see deployment/helm/README.md), kubectl context set.

set -e
cd "$(dirname "$0")"

MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-rule-engine}"
BACKUP_CHART="Chart.yaml.bak"
BACKUP_LOCK="Chart.lock.bak"

# Detect platform for Docker build (arm64 on Apple Silicon, amd64 on Intel)
case "$(uname -m)" in
  arm64|aarch64) PLATFORM="linux/arm64" ;;
  x86_64)        PLATFORM="linux/amd64" ;;
  *)             PLATFORM="linux/amd64" ;;
esac

cleanup() {
  if [[ -f "$BACKUP_CHART" ]]; then
    mv "$BACKUP_CHART" Chart.yaml
    echo "↩️  Restored Chart.yaml"
  fi
  if [[ -f "$BACKUP_LOCK" ]]; then
    mv "$BACKUP_LOCK" Chart.lock
    echo "↩️  Restored Chart.lock"
  fi
}
trap cleanup EXIT

# Pre-checks
if ! minikube status -p "$MINIKUBE_PROFILE" &>/dev/null; then
  echo "❌ Minikube profile '$MINIKUBE_PROFILE' is not running."
  echo "   Start it first: minikube start --profile $MINIKUBE_PROFILE ..."
  echo "   See deployment/helm/README.md for full steps."
  exit 1
fi

CURRENT_CTX=$(kubectl config current-context 2>/dev/null || true)
if [[ "$CURRENT_CTX" != "$MINIKUBE_PROFILE" ]]; then
  echo "⚠️  kubectl context is '$CURRENT_CTX' but Minikube profile is '$MINIKUBE_PROFILE'."
  echo "   Switching context: kubectl config use-context $MINIKUBE_PROFILE"
  kubectl config use-context "$MINIKUBE_PROFILE"
fi

cp Chart.yaml "$BACKUP_CHART"
[[ -f Chart.lock ]] && cp Chart.lock "$BACKUP_LOCK"

cp Chart.local.yaml Chart.yaml

REPO_ROOT="$(cd ../../.. && pwd)"  # deployment/helm/rule-engine -> repo root
IMAGE="local/rule-engine:latest"

echo "🔨 Building image: $IMAGE (platform: $PLATFORM)"
docker build --platform "$PLATFORM" -t "$IMAGE" -f "$REPO_ROOT/src/onprem/Dockerfile-opensource-uv" "$REPO_ROOT"

echo "📦 Loading image into Minikube (profile: $MINIKUBE_PROFILE)"
minikube image load -p "$MINIKUBE_PROFILE" "$IMAGE"

echo "📥 Updating dependencies"
helm dependency update

echo "🚀 Upgrading rule-engine"
helm upgrade --install rule-engine . -f values.local.yaml "$@"

