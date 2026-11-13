# Drift-Aware Real-Time Intrusion Detection System

## Purpose

This college research project plans a real-time intrusion detection system for
network-flow records, using CIC-IDS2017 as its initial dataset. It combines
stream processing and machine learning with a specific research focus on data
drift.

## Research objectives

1. Develop real-time intrusion detection using Apache Kafka, Apache Spark
   Structured Streaming, and Spark MLlib.
2. Evaluate model robustness against evasion attacks.
3. Monitor data drift as the selected research twist.

## Planned architecture

The planned flow is CIC-IDS2017 data → Python Kafka producer → Apache Kafka →
Spark Structured Streaming → Spark MLlib intrusion detection → alert
processing → FastAPI and PostgreSQL → React dashboard. Drift monitoring and
evasion robustness evaluation are planned research modules. See
[`docs/architecture.md`](docs/architecture.md) for details.

## Technology stack

- Python 3.11
- Apache Kafka
- Apache Spark Structured Streaming and Spark MLlib
- Pandas, NumPy, and SciPy
- FastAPI and PostgreSQL
- React and TypeScript
- Docker and Docker Compose
- Git and GitHub

These are planned technologies, not installed or configured dependencies.

## Project layout

- `data/`: raw and processed research data locations.
- `notebooks/`: exploratory analysis and research notebooks.
- `src/preprocessing/`, `src/training/`: planned data preparation and model
  training.
- `src/producer/`, `src/streaming/`: planned Kafka producer and Spark stream
  components.
- `src/drift/`, `src/robustness/`: planned drift monitoring and robustness
  evaluation.
- `src/common/`: planned shared project utilities.
- `backend/`: planned FastAPI application, schemas, services, and database
  layer.
- `frontend/`: planned React and TypeScript dashboard.
- `docker/`: reserved for future container configuration.
- `models/`: location for generated model artifacts; none are included.
- `tests/`: automated tests as functionality is implemented.
- `configs/`: non-secret configuration and experiment settings.
- `docs/`: architecture, setup, experiment, and progress documentation.
- `scripts/`: future repeatable utilities and commands.

## Current status

Offline preprocessing, scikit-learn Random Forest, held-out evaluation and KS
experiments and controlled validators are implemented. The user reports the
controlled CIC-IDS2017 gate passed at `6ba34e5`; see
[attributed results and review](docs/controlled_real_validation.md). Kafka, Spark, API, database,
dashboard and evasion evaluation remain unimplemented.

See [drift validation](docs/drift_validation.md) for commands, actual results,
limitations and file-by-file changes. Earlier "planned" descriptions above
refer to the deployment architecture, not the existing offline experiments.
