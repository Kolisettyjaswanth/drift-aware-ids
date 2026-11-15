# Read-only IDS evidence dashboard (Increment 6B)

React + TypeScript + Vite. This UI only reads existing FastAPI endpoints. It does not run inference, calculate drift, or imply continuous live monitoring.

## Development

Start the FastAPI backend from the repository root:

```bash
source .venv/bin/activate
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

In a second WSL terminal:

```bash
cd ~/projects/drift-aware-ids/frontend
npm install
npm run build
npm run dev
```

Open the URL printed by Vite (normally http://127.0.0.1:5173). The Vite dev proxy sends `/api` requests to FastAPI on port 8000. Do not expose either development server publicly.

## Interpretation

`disabled` means drift monitoring was not enabled, not that drift was absent. `insufficient_data` means no valid drift conclusion. Model predictions are distinct from KS drift alerts. The held-out intrusion model has known poor attack recall; the UI does not certify its performance.
