# Increment 3: opt-in Spark drift monitoring

Branch: `feat/streaming-drift-integration`; base increment: `b59ad684`.
Implemented for local validation. **No live Kafka/Spark run occurred in Work.**

## Behavior and modes

| Flags | Behavior |
|---|---|
| none | Existing transport-only processing |
| --enable-inference | Existing Random Forest processing |
| --enable-drift --drift-reference PATH | Transport plus KS monitoring |
| both enable flags, plus reference | Transport, existing inference, KS monitoring |

Drift is disabled by default. Enabling it requires an explicit reference; supplying
--drift-reference without --enable-drift is rejected to avoid misleading settings.
No model/feature schema, producer, preprocessing or KS detector code is changed.

The supervisor validates and hashes the reference before launching its child,
and stores the absolute path and SHA-256 in config.json. The worker reloads it
against that SHA **before constructing Spark**, then queries Kafka broker
metadata using the already-existing confluent-kafka dependency. Missing topics,
metadata errors or anything other than exactly one configured partition fail.
An unused second partition is still a rejection. Metadata is checked again on
each monitoring update and finalization; records retain their separate observed
partition/offset and source continuity checks from increment 1.

After each transport batch commits (and predictions, when enabled), BatchSink
invokes DriftMonitor. It uses read_records -> plan_windows -> evaluate_and_persist
on the bounded committed snapshot. It never removes or rewrites committed
transport/prediction files on monitoring failure. No separate stateful Spark
window engine or second KS detector is added. Retry evidence remains immutable
according to increment 2; inconsistent same-input evidence fails, never overwrites.

Partial snapshots are normal while accumulating 1,000 rows. They persist explicit
insufficient_data outcomes and null alerts, without aborting intermediate batches.
At query completion, however, **every planned window must be evaluated** and its
evidence successfully published. Any partial tail/capture segment makes the
final run unsuccessful. Invalid monitored features, planning/detector failures,
metadata failures, interruptions or evidence publication failures fail monitoring.
Failures are sticky; later success cannot erase them. Existing total record
limits (default 1,000, ceiling 10,000) remain unchanged.

summary.json contains drift status, success, evaluation completeness, reference
fingerprint, final snapshot ID, evaluated-window count, per-window status/alert,
evidence paths and errors. Worker and supervisor both gate overall success on
completed drift evaluation when enabled. Fallback transport accounting cannot
overwrite existing errors, drift results or inference results. Transport accounting
can be complete even when drift is insufficient; that never makes overall success
true. Preflight reference failures also write a failed summary in the newly
reserved run directory. Missing CLI flags fail immediately before reserving a run.

Both alert=true and alert=false are successful computations if all contracts
and persistence succeed. **Neither says the model is accurate, that attacks are
absent, or that classifier performance degraded.** No new accuracy metric or
production exactly-once guarantee is introduced.

## Detector output contract

The adapter now verifies each feature's decision against the existing
ks.feature_alert helper (no new statistical calculation), then checks the
ordered drifted-feature list/count and final minimum-feature aggregation.
Contradictory booleans, duplicate/wrong feature lists, wrong counts, nonfinite
statistics or inconsistent final alerts become monitoring_failure. The existing
ks.detect_window implementation and thresholds remain unchanged.

## Local verification (user-owned WSL execution)

The prior Spark/inference milestones are accepted as user-verified. No reinstall
or repeated Spark smoke test is requested. Start with the existing healthy broker,
model artifacts and reference CSV. These commands create only a new fixture,
topic and run directory; they never reset Kafka volumes or existing offsets.

### 1. Branch and offline checks

```bash
cd ~/projects/drift-aware-ids
git switch feat/streaming-drift-integration
git pull --ff-only origin feat/streaming-drift-integration
source .venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
python -m unittest discover -s tests -v
```

All available tests must pass. Optional live suites remain skipped without their
explicit environment flags. Model/PySpark artifact-dependent tests need the local
files/runtime; Work has not pretended to run those tests against real artifacts.

### 2. Prepare a bounded, capture-separated 1,000-record correctness fixture

The existing producer starts source_row at zero for the supplied fixture. The
fixture preserves SourceFile and all model columns; it does not modify the source
dataset. Its records are finite on monitored features and BENIGN from a capture
not present in the reference. A single such window is **not** a statistical
false-alarm/power benchmark; this procedure checks pipeline correctness and parity.

```bash
TAG="$(date -u +%Y%m%dT%H%M%S)-$$"
REFERENCE="data/processed/drift/reference.csv"
ACCEPTANCE_CSV="data/processed/drift/streaming_acceptance_$TAG.csv"
TOPIC="ids.drift.acceptance-$TAG"
RUN_DIR="data/processed/spark/drift-$TAG"
PRODUCER_REPORT="data/processed/spark/drift-producer-$TAG.json"
mkdir -p data/processed/spark
python - "$REFERENCE" "$ACCEPTANCE_CSV" <<'PY'
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from src.drift.ks import FEATURES
from src.streaming.drift_reference import load_reference
reference=load_reference(sys.argv[1])
capture='Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv'
assert capture not in reference.provenance['capture_counts'], 'Choose capture-separated evaluation data'
parts=[]
remaining=1000
for chunk in pd.read_csv('data/processed/capture_aware_cicids2017.csv',
                         chunksize=50000, low_memory=False):
    finite=np.isfinite(chunk[FEATURES].to_numpy(dtype=float)).all(axis=1)
    eligible=chunk.SourceFile.eq(capture) & chunk.Label.eq('BENIGN') & finite
    selected=chunk.loc[eligible].head(remaining).copy()
    if not selected.empty:
        parts.append(selected)
        remaining-=len(selected)
    if remaining==0:
        break
assert remaining==0, 'Fewer than 1000 suitable evaluation records'
output=Path(sys.argv[2]);output.parent.mkdir(parents=True,exist_ok=True)
pd.concat(parts,ignore_index=True).to_csv(output,index=False)
print('Prepared:',output,'rows:',1000,'capture:',capture)
PY
```

### 3. Publish to a fresh single-partition topic

```bash
docker compose -f docker/compose.kafka.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
python -m src.producer.replay --source "$ACCEPTANCE_CSV" \
  --topic "$TOPIC" --brokers 127.0.0.1:9092 --max-events 1000 --rate 100 \
  > "$PRODUCER_REPORT"
python - "$PRODUCER_REPORT" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r['success'] and r['delivered']==1000, r
assert r['delivery_failed']==0 and r['unconfirmed']==0, r
print(r)
PY
REPLAY_ID="$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["replay_id"])' "$PRODUCER_REPORT")"
```

Stop if producer verification fails. Do not publish more events into that topic.

### 4. Combined inference + drift and source preservation

```bash
python -m src.streaming.spark_consumer \
  --brokers 127.0.0.1:9092 --topic "$TOPIC" --replay-id "$REPLAY_ID" \
  --expected-count 1000 --max-records 1000 --batch-size 100 --timeout 180 \
  --enable-inference --enable-drift --drift-reference "$REFERENCE" \
  --run-dir "$RUN_DIR"
python -m src.streaming.verify_output --run-dir "$RUN_DIR" --source "$ACCEPTANCE_CSV"
```

Expected: overall success=true, 1,000 consumed/valid records, inference verified
with 1,000 predictions, drift status=evaluated, evaluation_complete=true, one
1,000-row evaluated window and existing contamination/duplicate counters zero.
Its alert may be true OR false. Full feature statistics/membership live in the
referenced immutable evidence JSON files. Ten micro-batches exercise cross-batch
accumulation. Driver-side bounded monitoring and repeated metadata queries add
cost; actual WSL resources/timeout sufficiency remain unmeasured here.

### 5. Verify exact detector parity and prediction evidence

```bash
python - "$RUN_DIR" <<'PY'
import json,sys
from pathlib import Path
from src.streaming.validation import read_records
from src.streaming.drift_reference import load_reference
from src.streaming.drift_windows import plan_windows
from src.streaming.drift_evaluation import evaluate_plan
from src.streaming.verify_inference import verify_inference
root=Path(sys.argv[1]);config=json.loads((root/'config.json').read_text())
summary=json.loads((root/'summary.json').read_text())
assert summary['success'] and summary['drift']['success'], summary
assert summary['consumed']==1000 and summary['valid']==1000, summary
predictions=verify_inference(root)
assert predictions['verified'] and predictions['predictions']==1000, predictions
reference=load_reference(config['drift_reference'],expected_sha256=config['drift_reference_sha256'])
plan=plan_windows(read_records(root),replay_id=config['replay_id'],
                  topic=config['topic'],max_records=config['max_records'])
expected=evaluate_plan(reference,plan,replay_id=config['replay_id'])
path=root/'drift'/'evaluations'/(summary['drift']['snapshot_id']+'.json')
actual=json.loads(path.read_text())
assert expected['success'] and expected['windows']==actual['windows']
assert actual['reference']['sha256']==config['drift_reference_sha256']
print('PASS: 1000 predictions, one complete KS window, exact detector parity')
print('Drift:',summary['drift'])
PY
```

Drift-only test: repeat consumer invocation without --enable-inference and with
a **different run directory**; producer replay/topic may be reused read-only.
To test old transport-only or inference-only modes, omit both new drift flags;
retain existing verification procedures. All run directories must remain fresh.

Safe failure checks (new run directories, no Kafka shutdown/data deletion):

- --enable-drift without reference: immediate nonzero CLI error.
- A missing/bad reference: failed summary; Spark child never starts.
- 100 records with drift enabled: explicit insufficient_data, null alert and
  overall failure at query completion (not a successful drift acceptance).
- A new two-partition topic, even with data only on partition 0: metadata
  rejection. Do not alter an existing topic's partition count to test this.
- Invalid monitored values or detector/publication failures: preserved committed
  transport/prediction evidence and overall failure. Offline tests inject these
  failures without damaging real model or filesystem artifacts.

## Actual Work verification

Availability-aware full unittest discovery: **185 tests: 169 passed, 16 skipped,
0 failures/errors**. Skips were 12 tests needing the unavailable trained model,
one existing test importing real PySpark, the existing dataset check, and two
opt-in Kafka/Spark live tests. Those first 13 tests were marked unavailable only
in a temporary test harness; repository test sources were not changed to hide
missing dependencies. The harness used ordinary unittest discovery, then set
__unittest_skip__/reason on test_streaming_inference.StreamingInferenceTests
when model/feature files were absent and on test_inference_worker.InferenceWorkerTests
when PySpark was absent. Live suites retained their existing opt-in skips.

All 23 new offline tests passed. They exercise contradictory detector outputs,
all four worker/CLI modes, real KS on generated windows with both alert outcomes,
cross-batch retries, combined-mode commit preservation, partial/invalid outcomes,
metadata/fingerprint errors, preflight ordering, interruption, persistence failure,
record caps, and error-preserving fallback accounting. Spark, inference and broker
interfaces are mocked where noted; the real KS/persistence/sink helpers execute.
No trained-model or live Kafka/Spark success is claimed.

Compilation of src/tests and CLI help passed. Actual missing-reference CLI and
preflight file-not-found paths returned nonzero; the latter wrote a failed summary
without launching a child. Runtime is Work Python 3.12; local Python 3.11 and live
integration still need user verification. No PR is created or merged at this
checkpoint; statistical-performance validation remains separate.
