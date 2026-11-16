# Increment 7 — Repeatable bounded analysis

Status: bounded runner implemented; separately authorized inference-only and
drift-enabled real Kafka/Spark acceptance runs passed. User-guided manual UI
checks passed for the screenshot-visible scenarios and the user reported no
browser errors. No DevTools log was captured. Do not repeat acceptance runs for closure.

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

## Implemented command

Run from the repository root using the existing Python 3.11 environment. This
preflight command is read-only and does not contact Kafka or start Spark:

```bash
.venv/bin/python -m scripts.preflight_replay \
  --source data/processed/increment7_sample_1000.csv \
  --features models/baseline_features.json --max-rows 1000
```

The orchestration command below **creates a Kafka topic, publishes, and starts
Spark**. It was exercised by the saved acceptance runs below. Any additional
execution needs separate approval; browser validation uses saved evidence only:

```bash
.venv/bin/python -m scripts.run_analysis \
  --source data/processed/increment7_sample_1000.csv \
  --brokers 127.0.0.1:9092 --max-rows 1000 --batch-size 100 \
  --rate 100 --delivery-timeout 35 --timeout 180
```

Add `--drift-reference PATH` only for an existing suitable BENIGN reference.
The command enables inference on every run and enables drift only when this
argument is supplied. It does not start Docker, the API, or the dashboard.
`--timeout` bounds Spark supervision; `--delivery-timeout` bounds individual
Kafka creation/freshness/delivery operations. Neither is a single wall-clock
deadline for the whole command. The existing producer rate limit also applies.

Each attempt reserves a UUID directory below `data/processed/spark` (override
with `--evidence-root`), a separate replay UUID, and `ids.analysis-<run UUID>`.
Existing directories are rejected. Topic creation must succeed: an existing
topic is never accepted, even if empty. Existing partition validation and a
zero-watermark check enforce one empty partition before publishing. No topic,
volume, model, or previous evidence is deleted. Do not publish concurrently to
the generated topic; contamination fails existing exact Spark accounting.

Before contacting Kafka, the runner enforces 1..10,000 rows, a 64 MiB input
limit, replay-compatible headers/CSV, all required model columns, and model
feature order/classes using `StreamingInference`. It retains an exclusive
`input/<original basename>` snapshot. SHA-256 checks bind the original file,
preflight, and snapshot; original and snapshot are checked again immediately
before publishing. Publishing and source verification both use the snapshot,
so later edits to the original cannot alter a replay. The model/manifest are
fingerprinted and checked around validation, publishing, and Spark execution.
This is accidental-change detection, not protection against hostile concurrent
filesystem writers. Do not edit artifacts or run snapshots during an analysis.

Optional reference loading reuses `prepare_drift_config`/`bound_reference`.
An existing reference hash must still match when the supervisor prepares the
worker; it is never silently rebound to edited data. Drift mode also requires
nonempty `SourceFile` on every source row. It retains the existing 1,000-record,
capture-boundary window rules. Too few eligible rows, capture boundaries, or
invalid monitored values can prevent successful evaluation; no detector rule
or preprocessing is changed to make such a run pass.

The orchestration layer calls existing `publish`, `supervise`, `verify_output`,
`verify_inference`, and drift success checks. Producer attempted, enqueued and
delivered counts must each equal the preflight row count, with no errors,
interrupt, failed deliveries or unconfirmed events. Spark starts only afterward.
Success requires completed exact transport accounting, source identity
verification, exactly one verified prediction per transported source row, and
successful drift evaluation when enabled. The supervisor's explicit reserved
directory option is used only after the runner's exclusive directory creation;
it rejects any existing Spark config, private Spark summary, checkpoint, batches
or predictions. Only orchestrated runs allow the runner's initial incomplete
public summary in that reserved directory. Standalone Spark CLI behavior is unchanged.

## Evidence and count semantics

Alongside the existing Spark config, summary, batches, predictions, checkpoint,
and optional drift evidence, each attempt saves:

- `analysis.json`: UTC creation time, persisted current/final stage, failed
  stage/error when applicable, input and artifact hashes, preflight report,
  delivery/verification results, and counts.
- `spark-summary.json`: intermediate Spark transport/inference/drift result for
  an orchestrated run. Spark workers and the supervisor never publish their
  success directly to the public summary in this mode.
- `producer.json`: actual producer acknowledgements, when publishing returns.
- `input/<basename>`: exact bounded replay input, including invalid values.

`summary.json` retains the existing API contract and adds `analysis` metadata.
It is created unsuccessful before processing begins. The runner atomically
publishes success only after all orchestration gates and final metadata writes
pass. Source verification uses the private Spark result and still recomputes
identity/accounting from committed records; the standalone verifier defaults
to the final public summary as before.
A preflight/publish failure has `success=false`, `accounting_complete=false`,
and a failed stage. A later verification failure preserves completed Spark
transport accounting while setting overall success false. Unknown transport or
prediction counts remain null, not invented zeros. Requested drift that fails
before Spark is reported as monitoring failure rather than disabled.
SIGTERM, Ctrl-C, ordinary exceptions and SystemExit persist unsuccessful
stage-specific evidence. SIGKILL or machine failure cannot execute cleanup;
the last persisted public state remains incomplete and intermediate Spark
evidence is retained, even if Spark itself succeeded. No public-success window
exists between Spark completion and orchestration verification.

Numeric compatibility is distinct from transport validity. The existing fitted
pipeline has a median `SimpleImputer`; inference converts missing/nonfinite
model values to NaN and uses that imputer. Numeric-invalid rows are retained,
transported, and may receive predictions. `analysis.counts` separates source
rows, rows with complete/incomplete required numeric features, transport
valid/invalid records, and actual verified predictions. `invalid_source_rows`
are zero-based and all original feature-quality metadata remains in events.
Blank labels are allowed by the producer; the required `Label` column does not
make labels trustworthy ground truth. Extra columns are reported, not silently
treated as required model inputs.

The existing FastAPI evidence root discovers these run directories and React's
Refresh action reloads the run list and the selected run's detail/drift evidence
without changing the selection or rebuilding the UI. Refresh clears displayed
results while requests are pending and resets pagination. Failed, missing and
incomplete results cannot retain an earlier displayed success. Late aborted
requests cannot repopulate stale results. No job-launch endpoint was added.
Failed runs remain available through the API and the dashboard's Browse all
analyses control.
Discovery sorts by timezone-aware `summary.created_at`, newest first, with
descending run ID as a deterministic tie-break before applying the result cap.
Missing, malformed, naive or oversized timestamp metadata falls back to a
regular non-symlink `config.json` modification time, or otherwise the summary
modification time. This preserves legacy discovery without rewriting saved
runs; copied or manually touched legacy evidence may have approximate ordering.
List/detail endpoint response fields and the dashboard remain compatible.

## Local checks

```bash
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  IDS_KAFKA_INTEGRATION=0 IDS_SPARK_INTEGRATION=0 \
  .venv/bin/python -B -m unittest discover -s tests -v
cd frontend
PATH="/home/jaswanth/.nvm/versions/node/v22.23.5/bin:$PATH" npm run build
NODE_DISABLE_COMPILE_CACHE=1 /home/jaswanth/.nvm/versions/node/v22.23.5/bin/node tests/refresh.test.cjs
```

Use the existing NVM Node installation: the default shell Node/npm resolution
is unsuitable in this environment. No dependency or system installation is
needed. Local test evidence is saved under
`data/processed/increment7-local-sMjzOfnN/` (initial implementation) and
`data/processed/increment7-audit-fixes-4y1ABG0f/` (P1/P2 fixes); see the report for exact
results and execution limitations: [local verification report](increment7_verification.md).

## Verified real acceptance and remaining browser gate

| Mode | Run ID | Delivered | Verified predictions | Drift |
| --- | --- | --- | --- | --- |
| Inference only | `ee5c9e92-7c3d-407d-9836-8d6b643c1b5c` | 1,000 | 1,000 | Disabled |
| Inference and KS | `a67cd39c-3fc8-4b60-ae26-716b02591aaa` | 1,000 | 1,000 | One evaluated 1,000-record window; 10/10 features flagged |

Both separately authorized runs used fresh one-partition topics/directories,
had zero failed/unconfirmed deliveries, exact source identity and transport
accounting, and successful final public summaries. Each produced 10 prediction
batches. Row 65 retained its missing-rate provenance and received a prediction.
Its missing `Flow Bytes/s`/`Flow Packets/s` fields are outside the ten monitored
KS features, so the unchanged planner correctly included it in the drift window.
Actual model classifications were 999 BENIGN and one ATTACK per run; these are
not confirmed incidents or an accuracy measurement.

The existing BENIGN-labelled reference has 40,000 finite rows, 10,000 from each
of Monday, Tuesday, Wednesday and Thursday Morning WebAttacks captures, matching
`data/processed/drift/split_manifest.json`. Its SHA-256 is
`2db894325651cb263656ffe3c506d670b9774e94af22cf7bd4007c03b8317358`.
Capture-group disjointness was checked; raw-row, temporal and training-set
independence are not established by this provenance check. Window size 1,000,
alpha 0.05 (per-feature 0.005), minimum KS 0.10 and minimum two flagged features
were unchanged. The final persisted KS snapshot recomputed byte-identically.

Saved summaries are under `data/processed/spark/<run ID>/`. Detailed execution
receipts are `data/processed/increment7-acceptance-qc9WZJAi/` and
`data/processed/increment7-drift-acceptance-nSF2ATRs/`. FastAPI HTTP routes via
TestClient discovered both runs and served predictions/drift evidence. Subsequent
user-guided browser checks confirmed saved-result rendering, pagination and
Refresh transitions using a disposable fixture. The user reported no browser
errors; no DevTools log was captured. API and rendered component tests are distinct from
these user-confirmed screenshots. Kafka was not needed and remained stopped.
Original acceptance evidence and the disposable fixture are retained unchanged
except for the explicitly simulated states in the copy.

CSV replay is not live network capture. Drift does not prove intrusion or model
degradation. Random-split development accuracy does not establish generalization;
the existing poor held-out PortScan recall remains a limitation. A completed
run establishes pipeline/evidence correctness, not security effectiveness.

## Acceptance gates

- A valid bounded capture-aware CSV can be preflighted without Kafka, Spark, or model retraining.
- Missing model features, malformed CSV, invalid labels/header requirements, and oversized input are rejected or clearly reported **before publishing**.
- The orchestrated run uses fresh run/replay IDs and never overwrites existing evidence.
- Producer acknowledgements, Spark transport accounting, inference verification, and optional drift status must all pass before declaring success.
- A failing run is visibly incomplete; no false claim of attack confirmation or live monitoring.
- Existing tests and React build remain green. Do not merge into `master` without explicit approval.

## Input and model checkpoint

The verified sample is `data/processed/increment7_sample_1000.csv`: 1,000 rows,
999 complete required-feature rows and one numeric-invalid row. The model uses
`models/baseline_random_forest.joblib` and `models/baseline_features.json` (70
features). Compatibility does not establish trustworthy labels or generalization.
