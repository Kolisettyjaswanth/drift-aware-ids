# Read-only IDS evidence dashboard (Increment 6B)

React + TypeScript + Vite. This UI only reads existing FastAPI endpoints. It does not run inference, calculate drift, or imply continuous live monitoring.

## Development

First follow [teammate setup](../docs/setup.md): use Python 3.11 and Node 22,
install `backend/requirements.txt`, and restore separately supplied saved run
directories. Git does not include datasets, models or saved evidence. Kafka and
Spark are unnecessary for this read-only dashboard workflow.

Start the FastAPI backend from the repository root:

```bash
source .venv/bin/activate
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

In a second WSL terminal:

```bash
cd ~/projects/drift-aware-ids/frontend
npm ci
npm run build
npm run dev -- --port 5173 --strictPort
```

Open the URL printed by Vite (normally http://127.0.0.1:5173). The Vite dev proxy sends `/api` requests to FastAPI on port 8000. Do not expose either development server publicly.

Refresh regression tests (from `frontend/`, with dependencies installed):

```bash
NODE_DISABLE_COMPILE_CACHE=1 node tests/refresh.test.cjs
```

These render the actual component in memory with controlled requests; they do
not constitute browser automation. Refresh reloads both the list and selected
run details while preserving selection and hiding stale results. Shut down
Vite and FastAPI with Ctrl-C in their respective terminals.

## Interpretation

`disabled` means drift monitoring was not enabled, not that drift was absent. `insufficient_data` means no valid drift conclusion. Model predictions are distinct from KS drift alerts. The held-out intrusion model has known poor attack recall; the UI does not certify its performance.
