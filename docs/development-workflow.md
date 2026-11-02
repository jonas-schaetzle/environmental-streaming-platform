# Development Workflow

## Delivery Model

The repository uses a lightweight delivery process suited to a single-maintainer
portfolio project while preserving a visible integration and release history.

```text
dev -> short-lived branch -> focused commits -> local merge to dev -> release PR
```

`AGENTS.md` contains the repository instructions used by Codex. This document is
the human-readable explanation of the same delivery model. GitHub Issues,
CODEOWNERS, elaborate pull-request templates, and release automation remain out of
scope until their operational value justifies the overhead.

## Branch Model

| Branch | Purpose | Example |
|---|---|---|
| `main` | Stable released milestone | Release merge from `dev` |
| `dev` | Integrated state of the next version | Direct merge target |
| `feature/*` | New product or platform capability | `feature/weather-ingestion` |
| `fix/*` | Focused defect correction | `fix/openaq-retry-handling` |
| `refactor/*` | Structural change without new behavior | `refactor/package-layout` |
| `chore/*` | Documentation, CI, tooling, or maintenance | `chore/update-project-documentation` |

Every implementation change starts from an up-to-date `dev` branch. Working
branches should stay short-lived and contain one coherent outcome.

## Commit Standard

Commits use concise imperative subjects without type prefixes:

```text
Add weather observation model
Handle transient OpenAQ failures
Update project documentation
```

Small commits are preferred when they represent independently understandable
steps. Mechanical churn, generated metadata, and unrelated cleanup do not belong in
the same commit as a behavioral change.

## Integration And Release

1. Review the complete working-branch diff.
2. Run the required quality and integration checks.
3. Merge the branch locally into `dev` with `--no-ff`.
4. Use `Merge <source-branch> into dev` as the merge-commit subject.
5. Push `dev` and report the merge as `source-branch -> dev`.
6. Delete the merged working branch locally and remotely.
7. Use a pull request only to release `dev` into `main`.
8. After the release, fast-forward `dev` to the pull-request merge commit so
   `main` and `dev` start the next cycle from the same revision.

Example:

```bash
git merge --no-ff feature/weather-ingestion \
  -m "Merge feature/weather-ingestion into dev"
```

## Quality Gates

| Gate | Required evidence |
|---|---|
| Ruff | `ruff check .` succeeds for every code change |
| Pytest | `pytest` succeeds for every behavioral change |
| Diff review | The complete change matches the intended scope |
| Local integration | Kafka, Spark, or OpenAQ behavior is exercised when the change depends on it |
| Documentation | README, architecture, roadmap, and commands agree with the implementation |

GitHub Actions runs Ruff and Pytest on pushes to `dev` and `main` and on release
pull requests targeting `main`.

## Data-Engineering Review Checklist

Changes to ingestion, processing state, or storage must answer the following
questions before merge:

- What identifies the same source event across retries or replays?
- Which component owns progress, offsets, or checkpoints?
- What happens when the process stops after writing data but before recording
  progress?
- How are malformed, incomplete, late, and duplicate records handled?
- Does the change preserve Kafka trace metadata and source lineage?
- Does an existing checkpoint remain compatible with the new query?
- Does an Iceberg schema, partition, or merge-key change require migration or
  backfill?
- Can the behavior be tested deterministically, and what requires a local
  integration run?

Runtime outputs, checkpoints, producer state, and lake data must not be deleted as
a shortcut during development. Doing so changes replay behavior and requires an
explicit decision.

## Definition Of Done

| Area | Expectation |
|---|---|
| Scope | The change delivers one clear outcome without unrelated refactoring |
| Code | Package ownership and the canonical model remain consistent |
| Tests | Focused tests cover changed behavior and required checks pass |
| Data | Idempotency, replay, checkpoints, late data, and quarantine are addressed |
| Security | No credentials, secrets, or runtime data enter version control |
| Operations | Commands, failure behavior, and migration implications are documented |
| Git | The diff is reviewed and the branch lifecycle is completed |

A change is complete only when code, tests, data behavior, and relevant
documentation agree.
