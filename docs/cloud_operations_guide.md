# Cloud Operations Guide

## Scope

This guide defines an operations model for running AIONBD in cloud or hybrid environments with production SLO expectations.

## Environment Strategy

Use three environments with strict promotion gates:

- `dev`: fast iteration, no external exposure.
- `staging`: production-like topology and data shape.
- `prod`: controlled changes, audited config, rollback-ready.

Do not skip staging for config or durability profile changes.

## SLO and Capacity Baseline

Define and track at minimum:

- search p95 and p99 latency by endpoint/mode
- availability of `/ready`
- write error rate and timeout rate
- memory budget utilization ratio
- WAL and checkpoint health indicators

Capacity planning loop:

1. baseline benchmark on target instance family
2. define headroom target (typically 30%+)
3. enforce memory and concurrency limits
4. rehearse failure and recovery path quarterly

## Deployment Model

Recommended production options:

- Kubernetes StatefulSet (preferred for cloud)
- systemd on dedicated VM/bare metal
- Docker Compose for controlled single-node environments

Pin immutable image or binary versions for each rollout.

## Change Management

For each release candidate:

1. run `scripts/verify_local.sh`
2. run representative benchmark command set
3. run soak/chaos dry-run checks
4. validate dashboards and alert rules in staging
5. document durability profile and risk posture

Promote only after all gates pass.

## Security Operations

- enforce non-disabled auth mode before public exposure
- manage secrets in native secret manager (K8s secret / cloud secret store)
- use TLS for in-transit encryption
- restrict ingress and east-west traffic with network policy
- rotate credentials on fixed interval and incident response

## Incident Response Playbook

### Service degraded (latency spike)

1. check in-flight requests and timeout counters
2. inspect memory budget and lock contention metrics
3. reduce traffic or raise query constraints (`limit`, batch size)
4. rollback to previous known-good version if regression confirmed

### Storage degraded or write failures

1. check WAL path health and disk free space
2. verify checkpoint loop progress
3. move to safety-first durability profile
4. if needed, switch to read-only mode behind gateway policy

### Node crash / restart

1. verify recovery from snapshot + WAL replay
2. compare point counts and key collections
3. run smoke query set before reopening traffic

## Backup and Recovery

- schedule periodic backup artifacts (snapshot + WAL)
- test restore procedure in staging on real artifacts
- keep documented RPO/RTO targets and validate monthly

## Governance

Maintain these records per production region:

- deployed version
- active durability profile
- memory budget and hard limits
- auth mode and key rotation timestamp
- latest benchmark and soak validation reports
