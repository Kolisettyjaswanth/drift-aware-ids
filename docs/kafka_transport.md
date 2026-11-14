# Phase 1: Kafka transport

Update: PR #2 is merged at master `24a2346`. The user reports successful
100-event real-data replay/verification and broker restart persistence. These
are user-executed results, not independent Work execution. Original
implementation-time checks/limitations below remain historical evidence.

## Repository audit and design

Base master: `2549b68ec0c4bffad20db5a9310f135fb7acf7db`, merged drift PR #1.
`src/producer/` contained only an empty __init__.py; Docker had placeholder
documentation, no Compose file. ML requirements had no Kafka client.
Preprocessing strips header whitespace, coerces invalid/nonfinite numbers to
missing values, and adds SourceFile to the capture-aware CSV. Baseline models
consume named numeric columns with median imputation; they are not called here.
KS behavior and existing data/model code remain unchanged.

This milestone implements only:

CIC-IDS2017 CSV -> Python replay -> Kafka -> Python verification consumer.

Use `data/processed/capture_aware_cicids2017.csv` as the preferred input, not
ML train/test CSVs. Raw source CSVs with unique stripped headers also work.
Pandas-generated names such as `Fwd Header Length.1` are preserved. Duplicate
raw headers are rejected rather than silently overwriting feature values;
use the existing capture-aware preprocessing output in that case.

There is no Spark, inference, live capture, drift calculation, API or dashboard.
Replay timing is simulation; it is not original packet timing or live traffic.

## Infrastructure

One `apache/kafka:3.9.1` combined broker/controller in KRaft mode, no ZooKeeper.
Pinned optional Python client: `confluent-kafka==2.6.1` in requirements-kafka.txt;
existing ML pins are untouched. The container supplies its own Java runtime;
the host's Java 17 installation is not used by this container.

- WSL host clients: `localhost:9092`, bound to host loopback only.
- Clients attached to Compose network `drift-aware-ids_default`: `kafka:19092`.
- Controller: `kafka:9093`, not published to host.
- Topic: default `ids.network-flows.v1`, explicit creation, one partition,
  replication factor 1. Auto-creation is disabled on broker and clients.
- Health check uses Kafka topic metadata, not just an open TCP port.
- Named data volume persists broker logs across normal shutdown.
- Heap 256–512 MiB; container limit 1.5 GiB and 2 CPUs. Intended for the user's
  approximately 7.6 GiB Docker environment; actual resource use is unmeasured.
- Retention: 24 hours and 256 MiB per partition, 64 MiB segments. Kafka retention
  cleanup is asynchronous; these are not strict disk quota guarantees.

Combined single-node plaintext mode is development-only: no fault tolerance,
authentication, encryption or cross-machine advertised listener support.
`acks=all` on one replica is still one copy. Version pins are reproducibility
choices, not a claim of current security support.

## Event schema v1

```json
{
  "schema_version": 1,
  "event_id": "REPLAY_UUID:0",
  "replay_id": "REPLAY_UUID",
  "source_file": "capture_aware_cicids2017.csv",
  "source_row": 0,
  "source_capture": "Monday-WorkingHours.pcap_ISCX.csv",
  "label": "BENIGN",
  "replayed_at": "2026-11-14T00:00:00+00:00",
  "features": {"Destination Port": 22, "Average Packet Size": 12.5},
  "invalid_features": []
}
```

The example timestamp is illustrative; the producer records the actual UTC
replay time. It never fabricates a capture timestamp. source_row is the
zero-based parsed data-record position in the supplied CSV, not a packet ID,
physical line number or original per-capture position in a combined file.
source_capture comes from SourceFile or is null; label comes from Label or is
null. Neither is an ML feature. Target-bearing training outputs are rejected.

Feature names retain internal whitespace/case/punctuation; outer header
whitespace is stripped exactly as preprocessing does. Integer strings become
Python integers (including large integers); other finite numerics use binary64
floats. Blank, invalid numeric and nonfinite strings become JSON null, and every
null field appears in invalid_features. No feature is imputed, scaled or dropped.
Finite decimal strings can round to binary64, so lexical or arbitrary-precision
decimal equivalence is not promised. Original CSV remains authoritative.

UTF-8 JSON uses allow_nan=False. Schema validation rejects malformed objects,
nonfinite/boolean feature numbers, identity mismatches, bad timestamps, duplicate
JSON keys and inconsistent null metadata. This validates a numeric transport
schema, not completeness against a saved model's full feature manifest.

## Producer behavior

`python -m src.producer.replay --help`

Options: --source (required), --brokers, --topic, --max-events (default 1000,
positive), --rate (default 100 events/sec; 0 unthrottled), --timeout (default
35 seconds, queue-wait and shutdown bounds, not whole-run wall time).

CSV parsing is record-at-a-time. The client queue is bounded to 1,000 messages
or 16 MiB; broker client/one-record overhead is additional. A full queue triggers
poll/retry until the queue deadline. Idempotence and acks=all are enabled;
delivery timeout is 30 seconds. poll/flush dispatch acknowledgement callbacks.

The JSON summary distinguishes attempted publishing of converted events, enqueued, delivered,
delivery_failed and unconfirmed. Only acknowledgement callbacks increment
delivered. CSV/client errors stop the run, retain counts and attempt bounded
flush. Ctrl-C stops enqueueing and flushes pending records. Exit 0 requires no
errors, failed/unconfirmed deliveries or interruption. A short source file may
produce fewer than max-events; compare the actual delivered count.

Idempotence covers producer retries within a session, not exactly-once replay
across reruns. Each run has a new replay UUID. Flush timeout leaves delivery
outcomes uncertain; retrying the entire file may create duplicates. Delivery
means broker acknowledgement, not evidence a consumer read the record.

## Verification consumer behavior

`python -m src.producer.verify_consumer --help`

Options: --group (required), --brokers, --topic, --max-events (default 1000),
--timeout (default 60 seconds total poll-loop bound plus metadata/close time),
--replay-id (optional expected replay UUID).

Starts at earliest when the group has no committed offsets. Automatic commits
are disabled; this verifier never commits offsets, including on close. Use a
fresh group for a repeatable check; existing groups with old committed offsets
can start elsewhere. No inference, dead-letter topic, offset checkpoint or
production processing semantics are introduced.

received counts Kafka data records examined, including malformed records;
valid counts valid schemas, matched counts the requested replay, and
unique_matched excludes repeated IDs. Null Kafka values are malformed. Broker
errors fail the run. Consumer closes on completion, timeout, error or Ctrl-C.
Exit 0 requires the requested number of unique matching valid events with no
malformed records, duplicates, errors or interruption. The receive bound counts
all data records, not only matching replays: unrelated retained traffic causes
a deliberate verification failure. Use a new empty topic per acceptance run.
The in-memory ID set is bounded by max-events.

## Startup/shutdown reference (pending user execution)

These procedures are supplied for the user; Work has not run Docker or WSL.
From repository root in the user's activated Python 3.11 environment:

```bash
pip install -r requirements-kafka.txt
docker compose -f docker/compose.kafka.yml config
docker compose -f docker/compose.kafka.yml up -d --wait
docker compose -f docker/compose.kafka.yml ps
```

Create a fresh acceptance topic (change the suffix for later runs):

```bash
docker compose -f docker/compose.kafka.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic ids.network-flows.acceptance-001 \
  --partitions 1 --replication-factor 1
python -m src.producer.replay \
  --source data/processed/capture_aware_cicids2017.csv \
  --topic ids.network-flows.acceptance-001 --max-events 100 --rate 20
```

Copy replay_id from the producer's JSON summary and use a fresh group:

```bash
python -m src.producer.verify_consumer \
  --topic ids.network-flows.acceptance-001 --group ids-verify-001 \
  --max-events 100 --timeout 60 --replay-id REPLACE_WITH_REPLAY_UUID
```

Normal shutdown preserves logs; destructive reset deletes all local Kafka data:

```bash
docker compose -f docker/compose.kafka.yml down
# Only when intentionally resetting the development broker:
# docker compose -f docker/compose.kafka.yml down -v
```

If startup fails, inspect `docker compose -f docker/compose.kafka.yml logs kafka`.
Validate listeners against the client's location; localhost inside another
container is not the broker. Joining clients must use the Compose network and
kafka:19092. A reused incompatible volume/cluster ID may require an intentional
reset. These troubleshooting cases have not been reproduced here.

## Tests and acceptance

Broker-independent suite: `python -m unittest discover -s tests -v`.
No Kafka client package is required for those unit tests because client creation
is lazy and fake interfaces exercise event delivery/error handling.

Opt-in broker test, after broker startup and client dependency installation:

```bash
IDS_KAFKA_INTEGRATION=1 python -m unittest tests.test_kafka_integration -v
```

It creates a UUID-named temporary topic, publishes ten generated CSV rows,
checks ten unique matching valid events, closes the consumer and deletes the
topic. It uses localhost:9092 unless IDS_KAFKA_BROKERS is set. It starts no
containers and has no CIC-IDS2017 dependency. Topic create/delete permission is
required. Failure cleanup also attempts topic deletion.

Pending user validation:

- Compose config parses with Docker Compose 5.5.0; broker reaches healthy status
  without exceeding practical memory on the 7.6 GiB environment.
- Metadata/topic operations work through internal Kafka listener; WSL Python
  clients connect through the host listener.
- Opt-in integration test passes against the actual broker/client versions.
- Fresh-topic CIC-IDS2017 run: 100 delivered, zero failed/unconfirmed; verifier
  receives 100 valid, unique matching records, zero malformed/duplicates/errors.
- Offline-broker and Ctrl-C runs terminate with truthful summaries/nonzero exit
  rather than reporting delivery success for buffered messages.
- Existing Python 3.11 drift tests remain passing with optional Kafka dependency.

This phase is ready for local validation when code, schema, Compose, passing
unit tests and a draft PR exist. It is not integration-verified until the user's
broker/data checks complete, and it must not be merged without user approval.

## Technical references

- [Apache Kafka 3.9 Docker guide](https://kafka.apache.org/39/getting-started/docker/)
- [Pinned image source and volume permissions](https://github.com/apache/kafka/blob/3.9.1/docker/jvm/Dockerfile)
- [Confluent Python client delivery/poll/flush semantics](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## Assistant-executed verification

- Unit/regression discovery: 57 tests, **55 passed, 2 skipped**. Skips are the
  existing unavailable CIC-IDS2017 prediction/reference check and the opt-in
  real-broker roundtrip. Kafka unit tests include CSV -> produced bytes ->
  verifier roundtrip through fake clients, queue backpressure, callback/flush
  accounting, malformed data, configuration, timeout and shutdown errors.
- `python -m compileall -q src scripts tests`: succeeded.
- Existing Gaussian drift validator rerun in memory: 0/200 no-shift alerts,
  200/200 location and 200/200 scale; acceptance passed. No tracked result
  report was overwritten.
- Both producer and verifier `--help` entry points: succeeded.
- Compose YAML parsed with available PyYAML; static assertions checked the
  single broker, pinned image, host binding, KRaft role and healthcheck.
  This is not Docker Compose semantic validation or container execution.
- Existing drift code and new Kafka code remain separate; no preprocessing,
  model, evaluation or KS source files are part of this milestone's changes.
- Runtime: Python 3.12.14, with existing available ML libraries. The declared
  Python 3.11 runtime and optional confluent-kafka native dependency were not
  installed or independently tested here. No Docker/client/data integration
  success is claimed.
