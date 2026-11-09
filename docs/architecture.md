# Planned Architecture

**Status:** This describes the intended architecture; it is not implemented.

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

All components and data flows above are plans. No pipeline, model, API, database
integration, or dashboard has been implemented.
