# Read-only evidence API (Increment 6A)

FastAPI serves saved, bounded Spark replay evidence from `data/processed/spark` (override with `IDS_EVIDENCE_ROOT`). It does not perform inference or drift detection itself, capture live traffic, or write to a database.

## Run locally

From the repository root, with the Python virtual environment activated:

```bash
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

## Endpoints

- `GET /api/health`: health check
- `GET /api/runs`: saved run IDs
- `GET /api/runs/{run_id}`: validated run summary
- `GET /api/runs/{run_id}/predictions?offset=0&limit=50`: paginated verified predictions
- `GET /api/runs/{run_id}/prediction-counts`: verified whole-run class counts
- `GET /api/runs/{run_id}/drift`: persisted drift evidence or explicit disabled/incomplete status

Tests: `python -m unittest discover -s tests -v` from repository root.

No PostgreSQL integration, job-submission API, or production deployment is included in Increment 6. See `docs/increment6_closure_audit.md`.
