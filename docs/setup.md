# Setup and teammate handoff

The verified local runtime is WSL Ubuntu, Python 3.11.18, Java 17, Node 22
through NVM, and Docker Compose for Kafka. Use Python >=3.11,<3.12 as declared
in `pyproject.toml`. Exact historical execution counts and accepted run IDs are
in [Increment 7 verification](increment7_verification.md).

These are setup instructions for a teammate's environment, not a claim that a
clean machine installation was executed during checkpoint preparation. Pinned
requirements describe the tested local environment; package installation needs
network access and may need platform-specific prerequisites. Do not replace or
upgrade the model's scikit-learn environment without separately verifying it.

## Files Git does not distribute

`.gitignore` excludes `.venv`, `node_modules`, `frontend/dist`, datasets,
`models/*`, checkpoints, logs, temporary fixtures and saved acceptance evidence.
Obtain required existing artifacts separately from the project owner; preserve
their paths and fingerprints. No teammate should silently retrain a substitute
model or fabricate evidence when these files are absent.

| Workflow | Existing artifacts required |
| --- | --- |
| Browse saved results | Entire accepted run directories under `data/processed/spark/`, including summaries, batches, predictions and drift evaluations where enabled |
| Model inference / model-dependent tests | `models/baseline_random_forest.joblib` and `models/baseline_features.json` |
| Bounded replay / CSV preflight | Existing flow-feature CSV such as `data/processed/increment7_sample_1000.csv`, plus the feature manifest |
| Drift-enabled replay | Existing `data/processed/drift/reference.csv` and its provenance `split_manifest.json` |
| Original research experiments | The relevant original/processed CIC-IDS2017 datasets and experiment artifacts; see each experiment's documentation |

The accepted directories are:

- `data/processed/spark/ee5c9e92-7c3d-407d-9836-8d6b643c1b5c/` (inference only).
- `data/processed/spark/a67cd39c-3fc8-4b60-ae26-716b02591aaa/` (inference plus KS).

Do not overwrite an existing run directory during transfer. The source sample,
model/manifest and reference fingerprints are recorded in
[verification](increment7_verification.md) and the owner's local acceptance
receipts. Those local receipt paths are not files distributed by the Git clone.

## Saved-evidence API and dashboard (no Kafka/Spark required)

From the cloned repository root, using an installed Python 3.11:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r backend/requirements.txt
mkdir -p data/processed/spark
```

Restore the separately supplied saved run directories there. An empty evidence
root produces an empty run list; without transferred artifacts the accepted
results cannot be displayed. The API does not run inference or train/load a
model to serve saved records.

Prepare the frontend with an existing Node 22/NVM installation:

```bash
source "$HOME/.nvm/nvm.sh"
nvm use 22
cd frontend
npm ci
npm run build
```

`npm ci` uses the committed lockfile. It is a teammate installation step, not an
instruction for the checkpoint agent to install packages. In separate terminals:

```bash
# Terminal A, repository root
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000

# Terminal B, frontend/ with Node 22 active
npm run dev -- --port 5173 --strictPort
```

Open `http://localhost:5173/` in Chrome/Edge. The Vite development proxy forwards
`/api` to `127.0.0.1:8000`. Health is `http://127.0.0.1:8000/api/health`.
Run both commands in WSL and keep both services on loopback. From the repository
root, `IDS_EVIDENCE_ROOT=/absolute/path/to/runs` may select another saved-evidence
root for the API. Use Ctrl-C in each service terminal to shut down gracefully.

## Full analysis environment and local checks

Only model-dependent tests and analysis/research workflows require the broader
environment. With the Python 3.11 virtual environment active:

```bash
python -m pip install -r requirements.txt -r requirements-kafka.txt -r requirements-spark.txt -r backend/requirements.txt
```

The Kafka client is pinned to 2.6.1; PySpark is 3.5.7 with Scala 2.12 connector
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. Real Spark analysis requires
Java 17 and the connector/transitive jars; first resolution may require Maven
network access. An installed PySpark package alone does not include that
connector. Restore the existing model/manifest before model-dependent tests;
the complete 303-test baseline cannot be reproduced from source alone without
its required local artifacts. Some dataset smoke checks can skip when fixtures
are unavailable; report actual results, not the owner's historical counts.

```bash
# Repository root, with required artifacts restored
PYTHONDONTWRITEBYTECODE=1 IDS_KAFKA_INTEGRATION=0 IDS_SPARK_INTEGRATION=0 .venv/bin/python -B -m unittest discover -s tests -v
NODE_DISABLE_COMPILE_CACHE=1 node frontend/tests/refresh.test.cjs
# frontend/
npm run build
```

Refresh tests use the installed React/TypeScript dependencies and Node's built-in
test API; no additional test dependency is needed. They are in-memory component
tests, not browser automation. Historical results were 303 Python tests (301
passed, two optional integration skips), five Refresh tests passed, and a passing
production build. A Linux process/SIGKILL test in the suite concerns only its own
isolated test child, not project services.

Do not start Kafka/Spark merely to browse evidence or repeat the already passed
acceptance runs. A new analysis must be separately authorized, use a new topic
and run directory, and follow [the bounded runner contract](increment7_reusable_analysis.md).
Kafka configuration is in `docker/compose.kafka.yml`; no PostgreSQL setup or
Spark MLlib model is part of the current implementation. Do not delete volumes
or previous evidence as a setup step.

## Scientific limits

CIC-IDS2017 flow-feature CSV replay is not live packet capture. KS drift means
distribution change, not intrusion or proven model degradation. RandomForest
labels are predictions, not confirmed incidents. The existing held-out PortScan
recall is poor; random-split development accuracy does not establish
generalization. Normal completion verifies pipeline/evidence accounting, not
production security effectiveness.
