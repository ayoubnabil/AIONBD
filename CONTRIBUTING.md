# Contributing to AIONBD

## Mandatory Git Workflow

1. Do not push directly to `main`.
2. Create a feature branch for every change.
3. Work and validate locally on your branch.
4. Ask an expert to review the branch before any merge decision.
5. Merge to `main` only after expert approval.

## Required Review Policy

- Every branch change must be reviewed by an expert.
- The expert decides whether the branch can be merged.
- If you later switch to GitHub PR flow, use `.github/CODEOWNERS` and branch protection.

## File Size Rule

- Keep files small and focused.
- Soft cap: 300 lines for source/documentation files.
- If a file grows too much, split it by responsibility before merging.
- CI enforces this via `scripts/check_file_sizes.sh`.

## Local Quality Checks

Run before asking for expert review:

```bash
./scripts/check_file_sizes.sh
./scripts/verify_local.sh
```
