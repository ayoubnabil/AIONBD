# AIONBD Kubernetes Deployment

This folder contains a production starter manifest for Kubernetes clusters.

## Files

- `aionbd.yaml`: namespace, config, statefulset, and service.

## Deploy

```bash
kubectl apply -f ops/deploy/kubernetes/aionbd.yaml
kubectl -n aionbd rollout status statefulset/aionbd
kubectl -n aionbd get pods,svc,pvc
```

## Configure

Edit `ConfigMap` values in `aionbd.yaml` for your environment:

- memory budget (`AIONBD_MEMORY_BUDGET_MB`)
- auth mode and credentials
- TLS settings
- write durability profile
- request and payload limits

## Security notes

- Keep `AIONBD_WAL_SYNC_ON_WRITE=true` for strongest durability.
- Use Kubernetes secrets for auth credentials and TLS keys.
- Restrict public exposure behind ingress or private networking.
