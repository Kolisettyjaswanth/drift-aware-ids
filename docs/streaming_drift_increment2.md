# Streaming drift integration: increment 2

Branch: `feat/streaming-drift-integration`; starts from increment 1 `5790d57`.
This increment executes KS and persists evidence **without wiring Spark or CLI**.

## API

`src/streaming/drift_evaluation.py` exposes:

```python
report = evaluate_plan(reference, plan, replay_id=expected_replay_id)
report = evaluate_and_persist(
    reference, plan, replay_id=expected_replay_id, run_dir=existing_run_directory
)
```

Inputs are the existing DriftReference returned by load_reference and WindowPlan
returned by plan_windows. Do not manually change reference values or provenance.
The adapter copies reference data and does not mutate either input. It verifies
reference shape/finite values and metadata, and records a second fingerprint of
the exact numeric view used for KS in addition to the original CSV SHA-256.

It calls **src.drift.ks.detect_window unchanged** only for status=ready,
complete=True, 1,000-row windows without planned feature issues. It uses the
existing ten features, alpha .05 (corrected .005), KS .10 and minimum two drifted
features. The existing detector selects asymptotic KS. No KS formula, p-value
calculation or alternate alert rule is implemented in the adapter.

Each persisted window retains replay/capture/source identities, complete source
row and Kafka-coordinate membership, issues and closure reason. Successful
results include the complete original detector return: per-feature statistics,
p-values and decisions, drifted feature names/count and final alert. Feature
rows are represented by a SHA-256 fingerprint rather than duplicated in output;
they remain available in original committed transport evidence.

## Outcomes

| Status | Alert / KS result | success |
|---|---|---|
| evaluated | Existing detector's boolean alert and full result | true only if every window is evaluated |
| insufficient_data | null / null | false |
| invalid_monitored_features | null / null | false |
| monitoring_failure | null for failed window; successful sibling evidence retained | false |

Empty plans are insufficient_data. Planning failures remain monitoring_failure.
`planning_complete` is never used to determine execution success. Mixed complete
and partial windows keep their individual outcomes and are not a complete KS
run. Detector exceptions and malformed/nonfinite detector results become explicit
failures. A successful later window cannot hide an earlier failure.

`evaluation_complete` describes KS computation, while wrapper success also
requires evidence publication. On a persistence failure, computation can be
complete but overall success is false, status=monitoring_failure and
failure_stage=persistence. No caller should substitute a computation flag for
wrapper success. These flags do not imply statistical drift-performance
acceptance or model degradation.

## Deterministic, atomic evidence

Files are written beneath:

`<run_dir>/drift/evaluations/<snapshot_id>.json`

Snapshot identity binds the reference provenance/numeric fingerprint, detector
settings, expected replay and complete planned input snapshot (including feature
values and membership). Growing snapshots get separate identities; a repeated
identical snapshot gets one artifact, not appended records. Canonical JSON uses
sorted keys and prohibits nonfinite numbers. No clocks or UUIDs enter result
identity. The report also records actual Python/library versions and detector
source SHA-256 for execution provenance; no dataset hashes are invented.

Publication on the local Linux/WSL filesystem writes a unique temporary file,
flushes/fsyncs it, and exclusively hard-links the completed inode to its final
name. It never replaces the final path. The directory is fsynced and temporary
files are removed. An identical retry returns persistence status=reused.
Different bytes at an existing snapshot path raise EvidenceConflict, retaining
the original artifact. Concurrent identical publication has the same behavior.

Changing runtime versions, detector outputs or exception outcomes for the same
input snapshot can therefore produce a deliberate consistency failure. Retry in
a **new run directory** when intentionally re-executing under changed conditions;
do not delete or overwrite prior evidence. Conflicts and I/O errors return
success=false from evaluate_and_persist. Partial file publication is not used.
This is a local evidence mechanism, not a distributed transaction/resume system.
The report files can accumulate across bounded snapshots; retention/lifecycle
policy remains a future integration concern.

The adapter never writes, removes or rewrites batches/ or predictions/. Tests
verify that existing transport/prediction bytes survive detector failures.
Real worker success gating and its commit ordering remain for Spark wiring.

## Actual verification

Executed with the available Python 3.12 runtime:

```bash
python -m unittest tests.test_drift_evaluation tests.test_drift_foundations \
  tests.test_drift tests.test_real_validation -v
python -m compileall -q src/streaming/drift_evaluation.py \
  tests/test_drift_evaluation.py
```

**76 discovered: 75 passed, 0 failures/errors, 1 skipped.** All 25 new evaluation
and persistence tests passed. The skip is the pre-existing CIC-IDS2017 reference
and held-out-prediction check because those local data files are absent.
Compilation succeeded.

Tests execute the actual unchanged detector on generated identical/strong-shift
windows, compare its full return with a direct call, verify unevaluated outcomes,
exercise detector failures and mixed outcomes, check atomic output and concurrent
retries, reject inconsistent retry evidence, simulate disk publication failure,
and verify unchanged input/transport/prediction evidence. These generated tests
prove implementation behavior, not real-data statistical performance.

The full inference/model suite and live Kafka/Spark runs were not executed.
They require the user's runtime/artifacts. No live verification is claimed.

## Remaining work (not part of this increment)

Spark opt-in wiring/CLI, pre-Spark reference validation lifecycle, broker
single-partition metadata preflight, worker-level monitoring failure gating and
end-to-end acceptance remain deferred. Pipeline correctness acceptance and
capture-separated statistical performance validation must remain separate.
No PR is created or merged at this checkpoint.
