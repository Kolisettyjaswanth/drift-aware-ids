# Planned Architecture

**Status:** Deployment architecture is planned. Offline research modules and
Phase 1 Kafka transport exist; user reports broker validation succeeded.
Kafka-to-Spark local acceptance code awaits user runtime validation.

## Planned data flow

```text
CIC-IDS2017 dataset
  -> Python Kafka Producer
  -> Apache Kafka
  -> Spark Structured Streaming
  -> Spark MLlib intrusion detection
  -> Alert processing
  -> FastAPI / PostgreSQL
  -> React dashboard
```

The dataset will be explored and prepared before network-flow records are
published by a Python producer. Kafka will provide the stream consumed by
Spark Structured Streaming. The planned Spark MLlib model will score records;
alert processing will make detections available to the FastAPI backend and
PostgreSQL store for the dashboard.

## Research modules

- **Drift monitoring (`src/drift/`):** planned analysis of changes in incoming
  data or model inputs, using statistical methods such as SciPy where suitable.
- **Robustness (`src/robustness/`):** planned evaluation of model behavior
  against evasion attacks. This is an evaluation objective, not an implemented
  attack or defense.
- **Preprocessing and training (`src/preprocessing/`, `src/training/`):**
  planned preparation of CIC-IDS2017 data and baseline model work.

The deployment flow remains planned. Offline preprocessing, scikit-learn model
evaluation and KS drift experiments are implemented. Kafka transport is merged;
Spark local acceptance code awaits runtime validation. API, database and
dashboard integrations remain unimplemented. See drift_validation.md.


## Phase 1 transport milestone

CSV -> Python replay (`src/producer/replay.py`) -> single-node KRaft Kafka
(`docker/compose.kafka.yml`) -> Python verifier
(`src/producer/verify_consumer.py`). Merged via PR #2; user reports Docker,
100-event real-data transport and restart persistence succeeded. The verifier is a transport acceptance tool, not a
replacement for future Spark processing. Feature names are preserved inside a
versioned JSON envelope; label/source metadata stay outside features. See
[kafka_transport.md](kafka_transport.md) for schema, limits and configuration.

## Kafka-to-Spark acceptance milestone

Existing schema-v1 events -> Spark 3.5.7 local[2] Kafka source -> bounded
foreachBatch validation using the shared event validator -> local JSONL records
and exact replay/offset statistics. Local[2] is a development consumer, not a
new cluster. No ML scoring or drift calculation is introduced. See
[spark_streaming.md](spark_streaming.md) for dependency matching, limitations,
checkpoint isolation and pending WSL acceptance.
