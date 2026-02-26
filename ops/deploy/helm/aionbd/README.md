# AIONBD Helm Chart

## Install

```bash
helm upgrade --install aionbd ops/deploy/helm/aionbd -n aionbd --create-namespace
```

## Quick configuration

Override values at deploy time:

```bash
helm upgrade --install aionbd ops/deploy/helm/aionbd \
  -n aionbd --create-namespace \
  --set image.repository=ghcr.io/<org>/aionbd-server \
  --set image.tag=<tag> \
  --set config.AIONBD_MEMORY_BUDGET_MB=1024
```

Set secrets through `secretEnv`:

```bash
helm upgrade --install aionbd ops/deploy/helm/aionbd \
  -n aionbd --create-namespace \
  --set secretEnv.AIONBD_AUTH_MODE=api_key \
  --set secretEnv.AIONBD_AUTH_API_KEYS='tenant-a:super-secret'
```

## Notes

- Default chart profile keeps durability safety (`AIONBD_WAL_SYNC_ON_WRITE=true`).
- Use immutable image tags for production rollouts.
- For TLS certs, mount Kubernetes secrets via `extraVolumes` and `extraVolumeMounts`.
