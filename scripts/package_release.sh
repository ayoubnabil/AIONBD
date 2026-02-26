#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/dist"
VERSION=""
SKIP_BUILD=0

usage() {
  cat <<USAGE
Usage: scripts/package_release.sh [options]

Options:
  --version <value>      Override version (default: read from server/Cargo.toml)
  --output <dir>         Output directory (default: ./dist)
  --skip-build           Do not run cargo build --release
  -h, --help             Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${ROOT_DIR}/server/Cargo.toml" | head -n 1)"
fi

if [[ -z "$VERSION" ]]; then
  echo "Failed to resolve version from server/Cargo.toml" >&2
  exit 1
fi

ARCH_RAW="$(uname -m)"
case "${ARCH_RAW}" in
  x86_64) TARGET_ARCH="x86_64" ;;
  aarch64|arm64) TARGET_ARCH="aarch64" ;;
  *) TARGET_ARCH="${ARCH_RAW}" ;;
esac

BUNDLE_NAME="aionbd-${VERSION}-linux-${TARGET_ARCH}"
BUNDLE_DIR="${OUTPUT_DIR}/${BUNDLE_NAME}"

mkdir -p "${OUTPUT_DIR}"
rm -rf "${BUNDLE_DIR}"
mkdir -p \
  "${BUNDLE_DIR}/bin" \
  "${BUNDLE_DIR}/config" \
  "${BUNDLE_DIR}/deploy/compose" \
  "${BUNDLE_DIR}/deploy/helm" \
  "${BUNDLE_DIR}/deploy/systemd" \
  "${BUNDLE_DIR}/deploy/kubernetes" \
  "${BUNDLE_DIR}/docs"

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  (cd "${ROOT_DIR}" && cargo build --release -p aionbd-server)
fi

cp "${ROOT_DIR}/target/release/aionbd-server" "${BUNDLE_DIR}/bin/aionbd-server"
cp "${ROOT_DIR}/LICENSE" "${BUNDLE_DIR}/LICENSE"
cp "${ROOT_DIR}/README.md" "${BUNDLE_DIR}/docs/README.md"
cp "${ROOT_DIR}/docs/platform_guide.md" "${BUNDLE_DIR}/docs/platform_guide.md"
cp "${ROOT_DIR}/docs/cloud_operations_guide.md" "${BUNDLE_DIR}/docs/cloud_operations_guide.md"
cp "${ROOT_DIR}/docs/packaging_and_distribution.md" "${BUNDLE_DIR}/docs/packaging_and_distribution.md"
cp "${ROOT_DIR}/docs/whitepaper.md" "${BUNDLE_DIR}/docs/whitepaper.md"

cp "${ROOT_DIR}/ops/deploy/.env.prod.example" "${BUNDLE_DIR}/config/aionbd.env.example"
cp "${ROOT_DIR}/ops/deploy/docker-compose.prod.yml" "${BUNDLE_DIR}/deploy/compose/docker-compose.prod.yml"
cp -R "${ROOT_DIR}/ops/deploy/helm/aionbd" "${BUNDLE_DIR}/deploy/helm/aionbd"
cp "${ROOT_DIR}/ops/deploy/systemd/aionbd.service" "${BUNDLE_DIR}/deploy/systemd/aionbd.service"
cp "${ROOT_DIR}/ops/deploy/kubernetes/aionbd.yaml" "${BUNDLE_DIR}/deploy/kubernetes/aionbd.yaml"
cp "${ROOT_DIR}/ops/deploy/kubernetes/README.md" "${BUNDLE_DIR}/deploy/kubernetes/README.md"

(
  cd "${BUNDLE_DIR}"
  find . -type f ! -name SHA256SUMS | sort | xargs sha256sum > SHA256SUMS
)

TARBALL="${OUTPUT_DIR}/${BUNDLE_NAME}.tar.gz"
rm -f "${TARBALL}" "${TARBALL}.sha256"
(
  cd "${OUTPUT_DIR}"
  tar -czf "${BUNDLE_NAME}.tar.gz" "${BUNDLE_NAME}"
  sha256sum "${BUNDLE_NAME}.tar.gz" > "${BUNDLE_NAME}.tar.gz.sha256"
)

echo "Bundle directory: ${BUNDLE_DIR}"
echo "Bundle archive:   ${TARBALL}"
echo "Bundle checksum:  ${TARBALL}.sha256"
