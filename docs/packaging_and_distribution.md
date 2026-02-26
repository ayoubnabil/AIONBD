# Packaging and Distribution

## Goals

This guide defines how to ship AIONBD as production-ready artifacts for common deployment environments.

## Artifact Types

- OCI container image (`Dockerfile`)
- Docker Compose production package (`ops/deploy/docker-compose.prod.yml`)
- systemd service package (`ops/deploy/systemd/aionbd.service`)
- Kubernetes package (`ops/deploy/kubernetes/aionbd.yaml`)
- Helm chart package (`ops/deploy/helm/aionbd`)
- Release tarball bundle (`scripts/package_release.sh`)

## Build the Release Binary

```bash
cargo build --release -p aionbd-server
```

Output binary:

- `target/release/aionbd-server`

## Build a Container Image

```bash
docker build -t ghcr.io/<org>/aionbd-server:<tag> .
```

## Production Compose Package

1. Copy environment template:

```bash
cp ops/deploy/.env.prod.example ops/deploy/.env.prod
```

2. Tune auth, TLS, durability, and memory budget in `ops/deploy/.env.prod`.

3. Launch:

```bash
docker compose -f ops/deploy/docker-compose.prod.yml --env-file ops/deploy/.env.prod up -d
```

## systemd Package

1. Install binary and service file:

```bash
sudo install -m 0755 target/release/aionbd-server /usr/local/bin/aionbd-server
sudo install -m 0644 ops/deploy/systemd/aionbd.service /etc/systemd/system/aionbd.service
```

2. Create `/etc/aionbd/aionbd.env` with runtime variables.

3. Start service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aionbd
sudo systemctl status aionbd
```

## Kubernetes Package

```bash
kubectl apply -f ops/deploy/kubernetes/aionbd.yaml
kubectl -n aionbd rollout status statefulset/aionbd
```

Before production go-live:

- replace image tag with immutable release tag
- switch credentials and TLS materials to Kubernetes Secrets
- size CPU/memory/storage requests for workload

## Helm Package

Install or upgrade:

```bash
helm upgrade --install aionbd ops/deploy/helm/aionbd -n aionbd --create-namespace
```

Set image and sizing values:

```bash
helm upgrade --install aionbd ops/deploy/helm/aionbd \
  -n aionbd --create-namespace \
  --set image.repository=ghcr.io/<org>/aionbd-server \
  --set image.tag=<tag> \
  --set config.AIONBD_MEMORY_BUDGET_MB=1024
```

## Build a Release Bundle

Create a distributable tarball with binary, deployment files, docs, and checksums:

```bash
./scripts/package_release.sh
```

Optional flags:

- `--version <value>`
- `--output <dir>`
- `--skip-build`

Bundle output:

- `dist/aionbd-<version>-linux-<arch>/`
- `dist/aionbd-<version>-linux-<arch>.tar.gz`
- `dist/aionbd-<version>-linux-<arch>.tar.gz.sha256`

## Automated Release Workflow

GitHub workflow `.github/workflows/release.yml` provides:

- Manual trigger (`workflow_dispatch`) with `version` creates and pushes a release tag (`v<version>`).
- Tag-triggered run builds and uploads release bundles (`.tar.gz` + `.sha256`) and publishes a GitHub release.
- Container image is pushed to GHCR as `ghcr.io/<org>/aionbd-server:v<version>`.
- `latest` is only updated for stable tags (`vX.Y.Z`); prerelease tags (`vX.Y.Z-...`) do not overwrite `latest`.

## Production Readiness Checklist

- Keep safe durability defaults unless risk is explicitly accepted.
- Set auth mode and credentials before exposing network access.
- Configure memory budget and request size limits.
- Validate `/live`, `/ready`, and `/metrics/prometheus` in staging.
- Run `scripts/verify_local.sh` and representative benchmark scripts.
