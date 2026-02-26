#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/ops/deploy/docker-compose.mvp.yml"
ENV_FILE="$ROOT_DIR/ops/deploy/.env.mvp"

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not reachable; start docker and retry" >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ROOT_DIR/ops/deploy/.env.mvp.example" "$ENV_FILE"
  echo "created $ENV_FILE from example"
fi

cleanup() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --build

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8080/live" >/dev/null; then
    break
  fi
  sleep 1
done

curl -fsS "http://127.0.0.1:8080/live"
curl -fsS "http://127.0.0.1:8080/ready"
curl -fsS "http://127.0.0.1:8080/metrics" >/dev/null
curl -fsS "http://127.0.0.1:8080/metrics/prometheus" >/dev/null

echo "container MVP smoke check passed"
