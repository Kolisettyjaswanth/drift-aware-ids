# Kafka -> Spark Structured Streaming acceptance

Status: implemented for user validation; **live acceptance has not run in Work**.
No inference, drift processing, model training, database or live capture is added.

## Audit and compatibility

Inspected master `24a2346e39f841b94cd1fccd5ae778a50066a2b2`, README,
architecture/progress, Kafka transport documentation, schema/producer/client
configuration and the empty src/streaming package. Kafka PR #2 is merged.
The user reports 100 real events delivered/verified, restart persistence and
56 passing tests with one expected skip. These are user-executed results.
Existing Kafka, model, preprocessing and KS behavior is preserved.

Versions: PySpark **3.5.7**, Py4J **0.10.9.7**, Scala binary **2.12** connector
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. Coordinates match the
[Apache Spark 3.5.7 Kafka guide](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
and [published POM](https://central.sonatype.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.5.7).
The connector brings Kafka Java client 3.4.1 transitively; do not replace it with
broker version 3.9.1 jars. Broker and client versions need not match numerically.
Runtime broker compatibility remains pending the user's actual test.

requirements-spark.txt records the already-verified local runtime, not a request
to reinstall it. The runner resolves connector/transitive jars through Spark's
normal Ivy/Maven mechanism. First use needs Maven access and writable Ivy cache;
PySpark installation alone does not include the Kafka connector. Startup checks
Python/JVM Spark versions, Scala 2.12 and KafkaSourceProvider class availability.
These checks have not been executed against a real JVM here.

## Processing and preservation

CSV -> existing producer -> Kafka -> Spark local[2] Kafka source -> foreachBatch
validation -> atomic batch JSONL records + summary. No separate Spark cluster.

A micro-batch callback uses toLocalIterator without prefetch, decodes binary
Kafka values with the existing `src.producer.events.decode_event`, and checks
the Kafka key matches event_id. This is deliberately driver-side validation for
a bounded local milestone, not a scalable distributed parsing design. It avoids
rounding arbitrary Python integers through Spark DoubleType maps or weakening
the existing schema with permissive from_json coercion. No new wire schema is
introduced. Features, labels, source IDs and replayed_at are preserved as
validated; records add only Kafka topic, partition and offset metadata. Invalid
payloads produce explicit invalid records/reasons rather than being filtered out.
Raw invalid bytes are not retained. Metadata warnings are not drift alerts.

Each run directory contains config.json, summary.json, checkpoint/, and
batches/batch-<id>.jsonl. Batch files are replaced atomically by batch ID, so a
foreachBatch retry does not append or inflate counts. Temporary partial batches
are discarded. This is **not** a resumable or production exactly-once sink:
reusing any run directory is rejected. Generated output/checkpoints stay under
already-ignored data/processed/. Do not commit them.

## Count and offset contract

- Publish the entire bounded replay before starting Spark; no concurrent writes.
- Use a new topic and new run directory per acceptance attempt.
- startingOffsets=earliest, failOnDataLoss=true, auto-topic creation disabled.
  Spark tracks offsets in its own checkpoint, independent of Python verifier
  consumer-group commits. No fixed Kafka group.id is forced.
- availableNow snapshots the available Kafka offsets at query startup and drains
  them, possibly across micro-batches. It does not wait for future records.
- Required replay ID and expected source row set **0..N-1** come from the existing
  producer's bounded run from the beginning of the CSV. Nonzero-start replays
  are not this acceptance protocol.
- Pass requires consumed=N, valid=N, invalid=0, unrelated=0, duplicates=0,
  repeated_offsets=0, unexpected_rows=0, unique_expected=N, missing_expected=0.
  Old replays, duplicate IDs at distinct offsets, repeated Kafka offsets,
  wrong topic, extra/missing rows and malformed records cannot silently pass.
- maxOffsetsPerTrigger defaults to 100; batch-size allowed 1..1000. A separate
  total input cap defaults to 1000 (maximum configurable cap 10000). Exceeding
  it fails the run rather than truncating and reporting a false 100-record pass.
- On runtime failure/timeout, accounting_complete=false and any counts cover
  completed batch files only, not the entire Kafka snapshot. Partial batches
  cannot establish acceptance. Valid/invalid are transport schema judgments,
  not classifier correctness. Producer null-valued features with consistent
  invalid_features are valid schema events, not malformed messages.

After success, verify_output compares every stored event with the first N CSV
records, including all feature names/values, null-quality metadata and source
fields. Only replayed_at is excluded from source comparison because it cannot
be reconstructed from a CSV. It is retained unchanged by decoding/output.

## Resources, timeout and cleanup

local[2], 1 GiB driver heap, two default/shuffle partitions, UI disabled and
64 MiB max driver results. Python and JVM native memory are additional; resource
usage is not measured. Intended for the existing 7.6 GiB WSL environment and
single-partition test topic. Not a Spark Docker cluster.

The CLI supervises a separate process group containing Python/Spark JVM/workers.
--timeout (default 180 seconds) covers startup, dependency resolution and query.
On timeout or Ctrl-C it requests group termination, waits at most 10 additional
seconds, then kills remaining group members. Normal worker cleanup stops an
active query and SparkSession. Exit 0 requires completed accounting and all
acceptance checks; every failure returns nonzero. The parent prints final JSON;
summary.json is authoritative if Spark logs also appear on the terminal.
Check actual JVM cleanup in WSL; mock tests and a real generic child-process
termination test do not establish real Spark lifecycle behavior.

This runner owns PYSPARK_SUBMIT_ARGS (1g driver setting) and PYSPARK_PYTHON,
but inherits JAVA_HOME. Do not launch through spark-submit: use python -m so
the supervisor includes JVM/connector startup in its timeout. Slow initial
Maven downloads may need a deliberately larger --timeout after diagnosing the
failure; they are not a reason to repeat the completed Spark smoke test.

## WSL checkpoints (user execution)

### 1. Get the branch; run regression checks

```bash
cd ~/projects/drift-aware-ids
git switch feat/spark-streaming-integration
git pull --ff-only origin feat/spark-streaming-integration
source .venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
python -m unittest discover -s tests -v
```

Do not reinstall or repeat the already-passed PySpark smoke test. Tests should
have no failures. Dataset checks depend on local files; the two live suites
remain skipped unless their explicit environment flags are set.

### 2. Reserve a new topic and output names

Use the already-running broker. This creates a new disposable topic only:

```bash
RUN_TAG="$(date -u +%Y%m%dT%H%M%S)-$$"
TOPIC="ids.spark.acceptance-$RUN_TAG"
RUN_DIR="data/processed/spark/$RUN_TAG"
PRODUCER_REPORT="data/processed/spark/producer-$RUN_TAG.json"
mkdir -p data/processed/spark
docker compose -f docker/compose.kafka.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
```

Do not reuse a prior topic/checkpoint and do not delete broker volumes.

### 3. Finish publishing 100 real records, then validate the acknowledgement

```bash
python -m src.producer.replay \
  --source data/processed/capture_aware_cicids2017.csv \
  --brokers 127.0.0.1:9092 --topic "$TOPIC" --max-events 100 --rate 20 \
  > "$PRODUCER_REPORT"
python - "$PRODUCER_REPORT" <<'PY'
import json, sys
r=json.load(open(sys.argv[1]))
assert r['success'] and r['delivered']==100, r
assert r['delivery_failed']==0 and r['unconfirmed']==0, r
print(r)
PY
REPLAY_ID="$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["replay_id"])' "$PRODUCER_REPORT")"
```

Stop if publishing failed. Do not add more data to this topic.

### 4. Run Spark and check full feature preservation

```bash
python -m src.streaming.spark_consumer \
  --brokers 127.0.0.1:9092 --topic "$TOPIC" --replay-id "$REPLAY_ID" \
  --expected-count 100 --batch-size 100 --max-records 1000 \
  --timeout 180 --run-dir "$RUN_DIR"
python -m src.streaming.verify_output \
  --run-dir "$RUN_DIR" --source data/processed/capture_aware_cicids2017.csv
```

Expected: both exit 0; Spark summary has success=true, accounting_complete=true,
consumed=100, valid=100, invalid=0, unique_expected=100 and all contamination/
duplicate/missing counters zero. Comparison prints success=true and
verified_source_rows=100. Retain producer report, config, summary and batch
outputs locally. Share terminal output before claiming this milestone passed.

### 5. Optional live tests and safe failures

```bash
IDS_SPARK_INTEGRATION=1 python -m unittest tests.test_spark_live -v
```

Uses generated CSVs, creates/deletes only UUID-named `ids-spark-test-*` topics,
and checks a 100-event pass followed by an explicitly malformed-event failure.
It requires the existing confluent-kafka dependency and the Spark connector;
it does not start/stop Docker or change existing Kafka data.

Non-destructive manual failure checks:

- Repeat the Spark command with a wrong --replay-id and **a new run directory**:
  expect nonzero, unrelated=100 and success=false.
- Use --brokers 127.0.0.1:1 and a new run directory: expect a bounded connection
  error or timeout, nonzero, accounting_complete=false; no broker shutdown needed.
- Repeat a previously used --run-dir: expect immediate nonzero rejection.
- Interrupt a new run: expect nonzero and no lingering JVM from that run after
  cleanup. Do not stop the established Kafka container to simulate failures.

No source-topic deletion, retention changes, group-offset reset or volume reset
is required for these tests. Only the optional integration test cleans up its
own generated topics. Keep previous acceptance artifacts for diagnosis.

## Current verification and limits

Work environment lacks PySpark. Real JVM/Kafka/connector execution is unavailable
here and is not claimed. The user-reported PySpark 3.5.7 local[2] smoke test is
accepted; it does not prove the Kafka connector path. Actual 100-record
CIC-IDS2017 Spark acceptance remains **pending user validation**.

Broker-independent tests exercise schema parsing, exact integer preservation,
nulls/invalids, replay/offset accounting, source comparison, batch retry behavior,
config validation, timeouts, failures and query/session cleanup using fakes.
The optional Kafka/Spark suite is skipped unless explicitly enabled. Existing
ML/KS modules are unchanged; the low adjusted KS-error correlation still means
drift is not automatically model degradation.

## Assistant-executed results for this increment

- Python 3.12.14: **78 tests discovered, 75 passed, 3 skipped**. Skips:
  unavailable existing CIC-IDS2017 fixture check, opt-in Kafka test, opt-in
  Spark/Kafka test. All 20 new broker-independent Spark tests passed.
- Compilation of src/scripts/tests and both new CLI help entry points passed.
- Invalid-count CLI configuration returned nonzero before starting Spark.
- Actual supervised missing-runtime CLI returned nonzero with
  `No module named 'pyspark'` and accounting_complete=false; this is successful
  failure handling, not successful Spark execution.
- Actual generic Linux child-process group terminated in the cleanup test.
  Spark query/session lifecycle checks used mocks; JVM cleanup is unverified.
- No Maven jars, real broker or real Spark/CIC-IDS2017 test was executed here.
  Existing PySpark installation/smoke test in WSL is not being repeated.

Files: validation.py implements schema/accounting/options; spark_consumer.py
implements the Spark source, batch sink, supervision and cleanup; verify_output.py
compares source features; requirements-spark.txt pins the optional runtime;
test_spark_streaming.py and test_spark_live.py provide independent/opt-in tests.
README, architecture/progress, Kafka status and tests README link these results.
