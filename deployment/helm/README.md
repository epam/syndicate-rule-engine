# Helm Charts — Local Setup (Minikube)

For local development without access to the S3 Helm repository. Uses **separate files** (`Chart.local.yaml`, `values.local.yaml`, `local-install.sh`).

## Prerequisites

- Docker
- Minikube
- kubectl
- Helm 3

## Step-by-step

### 1. Start Minikube

```bash
minikube start --driver=docker --container-runtime=containerd -n 1 --interactive=false --memory=max --cpus=2 --profile rule-engine --kubernetes-version=v1.30.0
minikube profile rule-engine
```

Verify it's running:

```bash
minikube status -p rule-engine
```

### 2. Set kubectl context

```bash
kubectl config use-context rule-engine
```

### 3. Create secrets (in default namespace)

```bash
kubectl create secret generic minio-secret --from-literal=username=miniouser --from-literal=password=$(openssl rand -base64 20)
kubectl create secret generic mongo-secret --from-literal=username=mongouser --from-literal=password=$(openssl rand -hex 30)
kubectl create secret generic vault-secret --from-literal=token=$(openssl rand -base64 30)
kubectl create secret generic valkey-secret --from-literal=password=$(openssl rand -hex 30)
kubectl create secret generic rule-engine-secret --from-literal=system-password=$(openssl rand -base64 30)
```

### 4. Install rule-engine

From the repository root:

```bash
./deployment/helm/rule-engine/local-install.sh
```

Or from the chart directory:

```bash
cd deployment/helm/rule-engine
./local-install.sh
```

The script will: build the image, load it into Minikube (profile `rule-engine`), update Helm dependencies, and install/upgrade the release.

### 5. Verify

```bash
kubectl get pods
```

All pods (rule-engine, celerybeat, celeryworker, event-sources-consumer, vault, minio, mongo, valkey) should reach `Running` status.

The Valkey 8.x release provides the Celery broker/result backend and the
event-deduplication cache. Celery/Kombu connection strings intentionally use
the Redis-compatible `redis://` protocol scheme while connecting to Valkey.

For a new installation, create `valkey-secret` as shown above. During an
upgrade from the Redis-based release, the chart also accepts the existing
`redis-secret` as a temporary fallback when `valkey-secret` is absent. The
rule-engine pods and the Valkey pod use the same precedence: `valkey-secret`
first, then `redis-secret`. Create the new secret and remove the old one after
the upgrade has been verified.
