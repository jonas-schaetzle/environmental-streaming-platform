# Repository Guidance

## Project Purpose

- Build a production-oriented environmental streaming platform with real data.
- The primary path is OpenAQ -> Kafka -> Spark Structured Streaming -> analytical
  storage and products.
- Prioritize practical, portfolio-relevant outcomes over isolated learning demos.
- Do not reintroduce sample pipelines or tutorial-only code unless explicitly asked.
- Keep the README aligned with user-facing setup, operation, and roadmap changes.
- Treat the Markdown files under `docs/` as the maintained project documentation.

## Architecture Boundaries

- `src/openaq_client.py` owns OpenAQ HTTP access and API-key handling.
- `src/openaq_kafka_producer.py` owns polling, metadata enrichment, and source-side
  deduplication.
- `src/kafka_publisher.py` owns event serialization, Kafka keys, and delivery checks.
- `src/measurement_model.py` owns the canonical contract, normalization, validation,
  and quarantine reasons.
- `src/measurement_kafka_stream.py` owns Spark stream orchestration, checkpoints,
  and valid, quarantine, and hourly aggregate sinks.
- `src/kafka_measurement_consumer.py` is a diagnostic reader and must not commit
  consumer offsets.
- `src/spark_runtime.py` owns local Spark and Java runtime discovery.
- The canonical Kafka topic is `environment.measurements.canonical`.
- Preserve Kafka trace metadata when routing invalid records to quarantine.
- Treat idempotency, replay behavior, checkpoints, and late data as explicit design
  concerns whenever ingestion or processing state changes.
- Prefer structured Spark APIs and the existing canonical model over ad hoc parsing.

## Data And Secrets

- Keep API keys and other secrets in environment variables or the ignored `.env`.
- Never print, commit, or place secrets in fixtures, notebooks, documentation, or
  command examples.
- All runtime outputs, checkpoints, state, and downloaded measurements belong under
  `data/` and must remain untracked.
- Do not delete Kafka topics, checkpoints, producer state, or lake data without an
  explicit request because this changes replay semantics.
- When a schema or normalization rule changes, call out compatibility and migration
  implications.

## Git Workflow

- Start every implementation change from an up-to-date `dev` branch.
- Use a short-lived branch named `feature/*`, `fix/*`, `refactor/*`, or `chore/*`.
- Review and merge completed short-lived branches into `dev` locally, then push
  `dev` directly. Use pull requests only for releases from `dev` to `main`.
- Keep `main` stable and `dev` as the integration branch for the next version.
- Keep commits small and coherent. Use concise imperative subjects without type
  prefixes, such as `Add continuous integration`, `Handle transient OpenAQ
  failures`, or `Refactor Iceberg sink`.
- Review the complete diff and report verification results before recommending a
  merge.
- After every merge and push, report the source and target branches explicitly in
  the form `source-branch -> target-branch`.
- Delete merged local and remote working branches after `dev` is pushed.
- Do not commit, push, merge, or delete branches unless the user explicitly asks.
- GitHub Issues are intentionally not part of the workflow yet.

## Engineering Standards

- Support Python 3.11 or newer and the pinned dependencies in `requirements.txt`.
- Follow existing module boundaries and patterns before adding abstractions.
- Keep changes narrowly scoped; avoid unrelated refactors and generated metadata.
- Add a dependency only when it has a clear operational or domain benefit.
- Use focused behavior tests proportional to risk. Bundle closely related scenarios
  instead of creating a large number of tiny tests.
- Update or add tests when changing source contracts, normalization, validation,
  state handling, checkpoint behavior, or Kafka serialization.
- Prefer deterministic unit tests. Add a local integration check when behavior
  depends on Kafka, Spark, or real OpenAQ responses.

## Verification

- Run `ruff check .` for every code change.
- Run `pytest` for every behavior change.
- For pipeline changes, verify the real local path through Kafka and Spark when the
  required services and API credentials are available.
- Report commands that were not run and any remaining operational risk.
- A change is done only when code, tests, data behavior, and relevant documentation
  agree.

## Collaboration

- Communicate with the user in German; keep code, identifiers, and repository
  documentation in English unless there is a reason to preserve existing German.
- Explain consequential architectural and data-engineering decisions in plain terms.
- The user values learning, but also wants visible real-world progress. Connect
  explanations to the production use case and avoid detours without a practical
  payoff.
- When the user wants to execute commands personally, provide one small step at a
  time and explain what it proves before continuing.
- Otherwise, work end to end with concise progress updates and pause for alignment
  before material architecture or workflow changes.
