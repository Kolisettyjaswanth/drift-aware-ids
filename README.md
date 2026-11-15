# Drift-Aware Real-Time Intrusion Detection System

An academic research system for **bounded replay of recorded network-flow data**, binary intrusion-model inference, and opt-in Kolmogorov–Smirnov (KS) data-drift monitoring. It uses CIC-IDS2017 research data; it is **not live packet capture or production intrusion prevention**.

## Implemented architecture

Capture-aware flow CSV → Python replay producer → Kafka → Spark Structured Streaming bounded consumer → existing **scikit-learn RandomForest** inference + optional KS drift monitoring → persisted JSON/JSONL run evidence → read-only FastAPI → React/TypeScript dashboard.

The Spark transport is implemented; the model is not Spark MLlib. No PostgreSQL-backed storage or live traffic ingestion is implemented.

## Components

- `src/preprocessing/`, `src/training/`: research dataset preparation and baseline model training.
- `src/producer/`: bounded CSV-to-Kafka replay and transport validation.
- `src/streaming/`: Spark bounded consumption, inference, opt-in drift monitoring, evidence verification.
- `src/drift/`, `src/robustness/`: KS experiments, held-out and evasion/perturbation evaluation.
- `backend/app/`: read-only FastAPI evidence endpoints (see [backend README](backend/README.md)).
- `frontend/`: React/TypeScript/Vite evidence dashboard (see [frontend README](frontend/README.md)).
- `data/`, `models/`: local data and model artifacts (not bundled in the repository).
- `tests/`: Python unittest coverage, including opt-in live integration suites.
- `docs/`: architecture, validation, progress, and acceptance evidence.

## Local verification

From the repository root with the project's Python 3.11 virtual environment activated:

```bash
python -m unittest discover -s tests -v
```

To build the dashboard, use Node 22 and run `npm run build` from `frontend/`. For local backend/frontend startup, see [frontend README](frontend/README.md). Running the full Kafka/Spark pipeline requires separately configured local dependencies and input data; the UI only reads saved results.

## Increment 6 status

User-executed acceptance: **250 Python tests, 248 passed, 2 skipped** (optional live Kafka/Spark tests), final frontend production build passed, and seven browser checks passed. See [closure audit](docs/increment6_closure_audit.md) and [progress](docs/progress.md).

**Interpretation:** model predictions are not verified attacks; drift alerts are not attack confirmations. Held-out PortScan recall is poor despite high development-split accuracy. Do not infer operational reliability from the dashboard.

Increment 7 reusable analysis is a separate, pending milestone. No merge into `master` is implied.
