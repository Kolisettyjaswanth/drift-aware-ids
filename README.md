# Drift-Aware Real-Time Intrusion Detection System

## Purpose

This college research prototype replays bounded CIC-IDS2017 network-flow CSVs
through Kafka and Spark, classifies flows with an existing scikit-learn
RandomForest pipeline, optionally evaluates KS drift, and displays saved evidence
through FastAPI and React. It is historical CSV replay, not live packet capture.

## Research objectives

1. Evaluate bounded stream processing using Apache Kafka, Spark Structured
   Streaming, and scikit-learn inference.
2. Evaluate model robustness against evasion attacks.
3. Monitor data drift as the selected research twist.

## Implemented architecture

Flow-feature CSV → Python replay producer → Kafka → Spark local Structured
Streaming → existing median-imputer/RandomForest pipeline → optional KS drift →
saved JSON/JSONL evidence → read-only FastAPI → React dashboard.
The current prototype uses neither Spark MLlib inference nor PostgreSQL.
See [architecture](docs/architecture.md) for boundaries and provenance contracts.

## Technology stack

- Python 3.11
- Apache Kafka
- Java 17 and PySpark 3.5.7 Structured Streaming (local mode)
- scikit-learn RandomForest with the existing fitted median imputer
- Pandas, NumPy, and SciPy
- FastAPI reading persisted evidence files
- React and TypeScript
- Docker and Docker Compose
- Node 22 through NVM for React/Vite
- Git and GitHub

See [setup and teammate handoff](docs/setup.md) for dependency files, commands,
artifact requirements and the limits of clean-clone reproducibility.

## Start here after cloning

A clone contains source, tests and documentation. It does **not** include the
trained model, feature manifest, datasets, drift reference or saved run evidence.
Obtain the existing artifacts from the project owner through a separate approved
transfer; do not retrain or rerun Kafka/Spark just to populate the dashboard.

For a saved-evidence demonstration, follow [setup](docs/setup.md): install the
backend/frontend dependencies in your own environment, restore the saved run
directories, then start the loopback API and Vite. Kafka, Spark, Java and the model
file are not needed just to browse already saved prediction/drift evidence.

## Project layout

- `data/`: raw and processed research data locations.
- `notebooks/`: exploratory analysis and research notebooks.
- `src/preprocessing/`, `src/training/`: existing data preparation, baseline
  training and held-out evaluation code; no retraining is needed for handoff.
- `src/producer/`, `src/streaming/`: replay, transport accounting, Spark inference
  and optional drift monitoring.
- `src/drift/`, `src/robustness/`: drift and robustness research/evaluation tools.
- `src/common/`: reserved shared utilities.
- `backend/`: read-only FastAPI evidence application; database directories remain
  scaffold placeholders.
- `frontend/`: React/TypeScript evidence dashboard and Refresh regression tests.
- `docker/`: local Kafka Compose configuration.
- `models/`: location for generated model artifacts; none are included.
- `tests/`: automated tests as functionality is implemented.
- `configs/`: non-secret configuration and experiment settings.
- `docs/`: architecture, setup, experiment, and progress documentation.
- `scripts/`: existing preparation utilities, read-only preflight and bounded
  analysis orchestration.

## Current status

Increment 7 adds a reusable bounded replay command over the existing
Kafka → Spark → RandomForest → optional KS → saved evidence → FastAPI/React
workflow. See [commands, accounting and verified acceptance](docs/increment7_reusable_analysis.md).
This is historical flow-feature CSV replay, not live capture.

Offline preprocessing, scikit-learn Random Forest, held-out evaluation and KS
experiments and controlled validators are implemented. The user reports the
controlled CIC-IDS2017 gate passed at `6ba34e5`; see
[attributed results and review](docs/controlled_real_validation.md). Phase 1 Kafka
transport is merged through PR #2. The user reports 100/100 real-event replay
and restart-persistence verification; see [Kafka transport](docs/kafka_transport.md).
The existing Spark consumer, inference, optional drift, and evidence API/dashboard
are reused by Increment 7. See [Spark streaming](docs/spark_streaming.md) for the
transport contract and the Increment 7 document above for the two passed real
acceptance runs and user-confirmed manual UI checks, with browser-verification limits recorded.

See [verification](docs/increment7_verification.md) for exact test counts, both
real acceptance run identifiers and manual browser observations. The held-out
PortScan model has poor generalization; development random-split accuracy does
not establish unseen-attack performance. Drift does not prove intrusion or model
degradation, and predictions are not confirmed incidents. This bounded local
prototype is not a production live-monitoring service.
