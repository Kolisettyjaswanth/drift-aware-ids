# Streaming drift integration: increment 1

Base master: `369f6022ebdb247be7585588435edc9cab5e890e`.
Branch: `feat/streaming-drift-integration`.

## Implemented in this increment

`src/streaming/drift_reference.py` adds `load_reference(path,
expected_sha256=None)`. It hashes and parses the same byte snapshot, requiring
all ten existing KS features, BENIGN labels, nonempty SourceFile values,
unique stripped headers, numeric finite values, and at least 100 rows. No
imputation, sampling or row dropping occurs. Limits are 64 MiB and 100,000 rows;
the existing 40,000-row reference format fits the row limit. Provenance includes
raw SHA-256, byte/row counts, monitored feature names and per-capture counts.
An expected SHA binds a later worker load to the earlier preflight snapshot.
No local CIC-IDS2017 fingerprint is claimed or invented. CSV provenance alone
cannot prove original row separation or capture-independent evaluation.

`src/streaming/drift_windows.py` adds `plan_windows(records, replay_id, topic,
max_records=1000)`. Input is a bounded snapshot of existing committed transport
records, not mutable append state. Rebuilding the snapshot after a micro-batch
or callback retry yields deterministic windows without double accumulation.
The hard cap cannot exceed the existing 10,000-record pipeline ceiling.

The planner validates the entire input before reporting usable windows:

- Exactly one observed Kafka partition and the expected topic/replay identity.
- Unique nonnegative Kafka offsets, sorted only within that partition.
- Source rows continuous from zero, one source file, valid schema-v1 identities.
- Known source_capture values; each contiguous capture segment is independent.

Windows contain exactly 1,000 records when complete. A capture boundary closes
an unfinished segment; it is never filled using another capture, including a
later return to the same capture. Kafka offset gaps are permitted (offsets are
not source row numbers), but duplicate offsets and source row gaps are errors.

Each window retains event IDs, source rows, Kafka coordinates, source capture,
monitored feature rows, issues, completeness and closure reason. Status is:

| Status | Meaning |
|---|---|
| ready | Complete, valid monitored window; KS has NOT been executed |
| insufficient_data | Partial window or empty input |
| invalid_monitored_features | Missing/null/unrepresentable monitored values; no imputation or dropping |
| monitoring_failure | Malformed transport/schema, identity/order violation or input read failure |

All planned alerts remain null. `planning_complete` means only that the planner
completed its structural checks, NOT successful monitoring, KS evaluation or
statistical acceptance. A structurally valid window with null features is
reported separately; malformed schema values (e.g. raw nonfinite JSON features)
fail schema validation and become monitoring_failure. Failures return no ready
windows. No changes are made to input records.

## Explicitly deferred

- No Spark hooks or new CLI flags yet. Existing transport and inference modes
  are untouched. These helpers do not start Spark or load a model.
- Integration must call reference preflight before Spark startup, and check the
  same fingerprint if the worker loads the CSV again. This increment supplies
  that API but does not claim the startup lifecycle is wired yet.
- The planner checks observed records only. Integration must additionally query
  broker metadata and reject a topic whose configured partition count is not 1,
  even if a current snapshot happens to contain records from only one partition.
- Calls to the existing unchanged `detect_window()`, complete KS result output,
  detector-failure handling, atomic drift persistence and overall worker success
  gating belong to the next increment. No second detector is introduced here.
- Existing transport/prediction completion markers remain authoritative; later
  monitoring failures must preserve their evidence while failing overall success.
- Statistical performance acceptance will require capture-separated data and
  must be distinguished from transport/windowing correctness. Drift does not
  automatically establish model degradation.

## Actual verification

Executed in Work with the available Python 3.12 runtime:

```bash
python -m unittest tests.test_drift_foundations \
  tests.test_drift.DetectorTests tests.test_drift.BoundaryTests -v
python -m compileall -q src/streaming/drift_reference.py \
  src/streaming/drift_windows.py tests/test_drift_foundations.py
```

**38 tests passed, no failures or skips:** 24 new reference/window tests and
14 existing detector/boundary tests. Compilation succeeded. Covered cases
include byte fingerprint verification, safety limits, malformed reference CSV,
null/missing monitored features, cross-batch snapshot growth, repeated/reordered
snapshot construction, nonoverlap, capture boundaries, partition/replay rejection,
continuity, malformed records and committed-input read failures.

The full current-master suite was not run in this increment. Model-dependent,
Spark/Kafka and real CIC-IDS2017 validation require local artifacts/runtime.
No live acceptance, detector execution through Spark, or statistical performance
result is claimed. This is a foundation checkpoint before pipeline wiring.

## Increment 2 checkpoint

The deferred KS execution and immutable persistence adapter is now implemented;
see [increment 2](streaming_drift_increment2.md) for its APIs, outcome semantics,
retry conflict policy and executed test results. Spark/CLI wiring and real-data
acceptance are still deferred. The earlier deferred list records the state at
increment 1 rather than claiming these new helpers are already wired live.
