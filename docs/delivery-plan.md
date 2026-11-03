# Delivery Plan Through 11 December 2026

Planning baseline recorded on 2026-11-03. No cloud deployment is authorized.

## Constraints

- Completion deadline: **2026-12-11**, Europe/Berlin.
- Maintainer availability: about **6 hours/week**, approximately 32.5 hours left.
  Agent implementation work can happen between review sessions; estimates below
  allocate the maintainer's review, verification, decisions, and demo time.
- AWS is required; an existing account is available.
- Additional project expenditure through completion: **EUR 20 total including
  applicable taxes**, not a monthly allowance. November and December share it.
- Develop and gather real source data locally. Use AWS only for bounded integration
  sessions and the final demonstration. Budget alerts are not a spending cap.
- Create no chargeable resources without an itemized calculation and explicit
  approval. Do not assume Free Tier eligibility or any promotional credit.
- Git writes require explicit instruction under AGENTS.md. No commit, merge,
  push, or branch deletion is authorized by this plan.

## Repository Evidence

On 2026-11-03, local main/dev, their origin tracking references, and a live
read-only GitHub branch query all resolve to
`9719a0237af755d4c9541f07dbe194a494f487c5`. The tracked working tree was clean at
the start of this package.
An additional local branch `avv` points to that commit and is left untouched.
Ruff passes; all 95 tests pass. Spark tests require local socket permissions.

Phases 0-2 are complete. Weather contracts, configuration, ingestion, polling,
correction-aware producer state, replay, separate Iceberg sinks, and read-only
weather audits are implemented. A joint product, graphical result view, data
quality alerts, Terraform, and an AWS operating procedure are absent.

Before this package, README/roadmap described delivered weather functionality as
upcoming and architecture used a planned-weather heading for implemented modules.
The deadline and total project budget were absent. This documentation package
corrects those gaps without changing runtime behavior.

## Agreed Completion Scope

1. Reproducible Terraform deployment to one AWS Linux machine.
2. Real OpenAQ/Open-Meteo -> Kafka -> Spark -> Iceberg data path and a documented
   restart check preserving progress and data.
3. Joint air-quality/weather analysis through configured station mappings and UTC.
4. A small result view with time series, city comparison, units, coverage, source
   attribution, and an explicit last-refresh timestamp.
5. Simple data-quality alerts for missing/stale data and quarantined input, with
   thresholds and evidence; output in a report/view is sufficient.
6. An operating runbook, known limitations, teardown verification, and a final demo.

Excluded: Kubernetes, HA, permanent AWS operation, managed Kafka/Spark, a second
stream processor, ML, health-risk claims, user authentication, mobile apps,
notification integrations, and a general historical backfill platform.

## Proposed Implementation Boundaries

These are recommendations for alignment before implementation, not deployed
architecture changes.

- Keep Kafka, Spark in local mode, and the existing Hadoop Iceberg catalog together
  on one EC2 host. Use one canonical writer per weather table and bounded,
  sequential Spark runs. Benchmark memory locally before selecting the instance.
- Keep Kafka data, producer state, Iceberg data, and separate checkpoints on durable
  storage across process/container restart. Compose now configures an explicit
  persistent mount and loopback port binding. Existing brokers require the
  [documented migration](local-kafka.md) before recreation; never discard their
  data to make the new mount work. Host reboot remains a separate acceptance check.
- Build the joint product as a bounded read from trusted tables, with deterministic
  report regeneration. This avoids a new streaming join/checkpoint and lets newer
  accepted weather revisions appear on the next product refresh.
- Join by configured IDs, UTC window, parameter, and compatible units. Keep missing
  weather explicit. Precipitation labelled 11:00 UTC covers 10:00-11:00; point weather
  values must not be labelled hourly means. Freeze/document the mapping used.
- Compare the same air parameter/unit and show station coverage. A selected station
  is not a representative citywide exposure estimate. Show gaps, never zero-fill
  missing observations or imply complete history. Weather is model context.
- Prefer a small locally served view of the generated report. Refresh after bounded
  Spark jobs; show freshness instead of promising continuous low latency. No new
  external service is needed. Select the view technology in its own small package.
- Alerts describe data quality, not medical or regulatory air-quality thresholds.
- Pin the tested Linux/Python/Java/container versions and persist local Terraform
  state outside version control. Keep source API credentials off Terraform
  variables, state, user-data, and committed files; inject them privately at runtime.

## Milestones And Maintainer Time

| Period | Deliverable and acceptance evidence | Planned hours | Buffer |
|---|---|---:|---:|
| Nov 3-8 | Free AWS preflight; agree bounded design; verify real local multi-location path, coverage and restart; start documented local collection | 5 | 1 |
| Nov 9-15 | Deterministic joint product; test UTC alignment, missing matches and accepted weather corrections | 5 | 1 |
| Nov 16-22 | Small result view; time series/city comparison and explainable quality-alert report on real data | 5 | 1 |
| Nov 23-29 | Persistent single-host runtime, Terraform/bootstrap and teardown/runbook reviewed locally; concrete AWS quote approved before provisioning | 5 | 1 |
| Nov 30-Dec 6 | Bounded AWS integration: fresh API values through both paths, restart evidence, recreate deployment and inspect teardown | 4.5 | 1.5 |
| Dec 7-11 | Feature freeze; final AWS demo, evidence export, teardown and final documentation | 2 | 0.5 |
| Total | 26.5 planned hours + 6 hours contingency | 26.5 | 6 |

This is feasible but tight. The main risks are sparse/current source coverage,
finalized OpenAQ windows depending on advancing event-time watermarks, local
collection gaps, Linux bootstrap/memory sizing, AWS permissions and billing access.
The existing audit proves identities/partitions, not complete location/hour coverage.
Do not defer data collection or cloud-readiness discovery to the final week.

If time slips, reduce chart styling, time-range controls, and alert presentation
first. Preserve AWS, the real path, restart evidence, a minimal combined view,
quality alerts, and the runbook. Escalate any threatened deadline or scope change
for alignment rather than silently dropping a required outcome.

## Acceptance And Evidence

- Rebuild from versioned Terraform/bootstrap instructions with declared versions.
- Capture bounded source run reports for configured stations/cities, Kafka delivery,
  canonical and quarantine processing, and Iceberg audit/coverage results.
- Stop/restart processes and reboot the same host, preserving storage and progress.
  Verify offsets, row/business-key counts, unchanged replay, newest weather update,
  and rejection of an older replay. Invalid events retain their Kafka lineage.
- Recreating infrastructure is a separate test from recovering the existing host;
  loss of a deleted volume is not recoverability. Export required evidence first;
  deletion of runtime data/resources needs explicit authorization.
- Demonstrate joint hourly output with gaps and model attribution, comparable city
  series, stale/missing/quarantine alerts, and an unambiguous refresh time.
- Retain evidence locally under ignored data/; public documentation contains only
  sanitized results. Report quality checks and unexecuted checks.
- After each cloud session, verify project resources and storage retention against
  the approved plan. Review delayed billing through completion; no permanent cloud
  endpoint is required after the demonstration.

## Working Package And Next Decision

The documentation package was prepared on the explicitly authorized branch
`chore/aws-preflight-plan`, created from verified current dev. Integration actions
require separate explicit instruction under AGENTS.md; this plan does not grant
ongoing Git authorization. The maintained roadmap and README link this plan and
the [AWS preflight](aws-preflight.md).

Before implementing the joint product and cloud runtime, align on the proposed
bounded product refresh. The local persistence package establishes broker restart
evidence and safe Kafka storage, preserving current broker data; see
[Local Kafka](local-kafka.md). Broader source coverage and host-reboot evidence
remain required. AWS billing/credit confirmation can proceed separately and does
not block cost-free local development.
