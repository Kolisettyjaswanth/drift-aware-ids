# Read-only evidence API

`app/main.py` serves saved run discovery, summaries, prediction pages/counts and
authoritative drift evidence. It does not launch Kafka/Spark, capture traffic,
train a model or write run evidence. PostgreSQL is not used; unused database and
schema directories remain scaffold placeholders.

From repository root, with Python 3.11 and backend requirements installed:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

The default evidence root is `data/processed/spark`; override it with
`IDS_EVIDENCE_ROOT=/absolute/path/to/runs`. Saved runs are excluded from Git and
must be supplied separately. Missing evidence does not cause the API to generate
or retrain anything. See [teammate setup](../docs/setup.md) for artifact paths,
dependency instructions, frontend startup and limitations.
