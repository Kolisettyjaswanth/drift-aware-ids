# Increment 7 — Repeatable bounded analysis

Status: contract and input audit only. No new runner or new traffic source has been implemented or validated.

## Goal

Allow a user to repeat the existing bounded CSV → Kafka → Spark inference → optional KS drift → saved evidence → FastAPI/React workflow without retraining the RandomForest model or rebuilding the UI for each run.

## Current constraints verified in source

- `src.producer.replay` accepts a CSV with a `Label` column and numeric flow features. It rejects `Target` and duplicate/empty headers. A blank label is allowed at event level, but the CSV still requires the column.
- The producer emits a new UUID `replay_id` and event source rows starting at zero. It prints a JSON delivery summary.
- The Spark consumer requires an explicit `--topic`, `--replay-id`, and `--expected-count`. Its bounded accounting expects exactly the published source rows and a fresh output directory.
- Drift is opt-in: `--enable-drift --drift-reference PATH`. The reference is an existing BENIGN, capture-provenance CSV, not arbitrary new traffic.
- The current source is *flow-feature CSV*, not PCAP or direct NIC capture. The source's `Label` requirement means unlabelled real-world flow CSVs are **not yet supported by the existing producer CLI**.
- Existing training and reference artifacts are reused; do not silently retrain or change them.
- The consumer's configured Kafka topic must have exactly one partition for drift monitoring. Each acceptance attempt uses a new topic and run directory, with the replay fully published before Spark starts.
- Dashboard reads persisted evidence; it does not initiate Kafka/Spark jobs.

## Proposed next implementation, subject to verification

1. Add a **read-only preflight** for a bounded CSV: validate headers and feature compatibility against the existing model feature manifest; report usable rows, invalid numeric fields, and missing/extra features. Never modify input or models.
2. Implement a thin orchestration command using the existing producer/consumer, not a second inference or drift engine. Keep explicit topic creation/partition validation and stop on incomplete delivery or accounting.
3. Verify a fresh run end-to-end with the current model and optional reference; compare run summary, API, and dashboard.
4. Document repeat commands and limitations, including the lack of live capture and the poor held-out PortScan recall.

## Acceptance gates

- A valid bounded capture-aware CSV can be preflighted without Kafka, Spark, or model retraining.
- Missing model features, malformed CSV, invalid labels/header requirements, and oversized input are rejected or clearly reported **before publishing**.
- The orchestrated run uses fresh run/replay IDs and never overwrites existing evidence.
- Producer acknowledgements, Spark transport accounting, inference verification, and optional drift status must all pass before declaring success.
- A failing run is visibly incomplete; no false claim of attack confirmation or live monitoring.
- Existing tests and React build remain green. Do not merge into `master` without explicit approval.

## First local checkpoint

Inspect the actual model artifact and feature-manifest paths, and confirm a representative new input file's schema before implementing preflight. Do not assume that a new CSV has the CIC-IDS2017 columns or that its labels are trustworthy.
