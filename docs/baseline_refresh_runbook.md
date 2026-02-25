# AIONBD Baseline Refresh Runbook

This runbook defines how to refresh and validate dry-run baselines used by CI regression checks.

## 1. Scope

Managed baseline files:
- `ops/baselines/soak_pipeline_dryrun_baseline.json`
- `ops/baselines/chaos_pipeline_dryrun_baseline.json`

Primary tooling:
- `scripts/refresh_report_baselines.py`
- `scripts/compare_report_regressions.py`

## 2. When to refresh baselines

Refresh only when baseline changes are intentional, for example:
1. profile definitions changed (`ops/soak/longrun_profiles.json`)
2. dry-run model changed in soak/chaos pipeline scripts
3. expected metrics schema changed

Do not refresh to silence unknown regressions.

## 3. Refresh procedure

Run baseline update:

```bash
python3 scripts/refresh_report_baselines.py --mode update --profiles-file ops/soak/longrun_profiles.json
```

This regenerates both baseline files using dry-run executions of:
- `./scripts/verify_soak.sh`
- `./scripts/verify_chaos.sh`

## 4. Verification procedure

Validate refreshed baselines match current dry-run behavior:

```bash
python3 scripts/refresh_report_baselines.py --mode check --profiles-file ops/soak/longrun_profiles.json
```

Then validate report comparisons directly:

```bash
AIONBD_SOAK_DRY_RUN=1 ./scripts/verify_soak.sh --profiles-file ops/soak/longrun_profiles.json --report-json-path bench/reports/soak_pipeline_report.json --report-path bench/reports/soak_pipeline_report.md
AIONBD_CHAOS_DRY_RUN=1 ./scripts/verify_chaos.sh --report-json-path bench/reports/chaos_pipeline_dryrun_report.json --report-path bench/reports/chaos_pipeline_dryrun_report.md
python3 scripts/compare_report_regressions.py --kind soak --baseline ops/baselines/soak_pipeline_dryrun_baseline.json --current bench/reports/soak_pipeline_report.json
python3 scripts/compare_report_regressions.py --kind chaos --baseline ops/baselines/chaos_pipeline_dryrun_baseline.json --current bench/reports/chaos_pipeline_dryrun_report.json
```

## 5. PR checklist for baseline updates

1. Explain why baseline changes are expected.
2. Include the exact commands used.
3. Attach resulting report artifacts when relevant.
4. Confirm `./scripts/verify_local.sh` passes.
5. Confirm CI jobs `verify-local`, `soak-pipeline-dry-run`, and nightly dry-run checks are green.

## 6. Failure handling

If `--mode check` fails unexpectedly:
1. Re-run once to rule out local invocation mistakes.
2. Diff current report rows vs baseline rows.
3. Confirm profile file and env overrides are what CI uses.
4. If change is intentional, refresh baselines and document rationale.
5. If change is not intentional, fix the underlying pipeline/script regression.

## 7. Notes

- Baseline comparison uses row content, not report generation timestamps.
- Keep baseline files small and deterministic.
- Avoid changing thresholds and baselines in the same commit unless justified.
