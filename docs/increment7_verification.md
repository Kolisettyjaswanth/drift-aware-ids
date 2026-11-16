# Increment 7 local verification

Status: implementation, local regression/build checks, and both separately
authorized real Kafka/Spark acceptance runs passed. User-guided manual UI checks
passed for the screenshot-visible scenarios, and the user reported no browser
errors during testing. No DevTools Console log was captured. No acceptance run
was repeated during closure work, and no model
training, dependency installation, commit or merge was performed.

## Verified real acceptance runs

| Mode | Run | Replay | Delivered / predictions | Result |
| --- | --- | --- | --- | --- |
| Inference only | `ee5c9e92-7c3d-407d-9836-8d6b643c1b5c` | `a2ff1536-a804-4703-9dfc-6e37fca21afc` | 1,000 / 1,000 | PASS; drift disabled |
| Inference and KS | `a67cd39c-3fc8-4b60-ae26-716b02591aaa` | `51f8f7ca-3a02-4e3c-833c-18b90635fa8d` | 1,000 / 1,000 | PASS; one evaluated window, 10/10 features flagged |

Topics are `ids.analysis-<run ID>`. Both runs have fresh one-partition topics,
zero failed/unconfirmed producer deliveries, exactly 1,000 unique transport
records, zero missing/duplicate/unrelated/invalid transport records, all-row
source verification, and ten verified prediction batches. Public success was
observed false through source/inference checks, then true only at complete.
Final evidence lives in `data/processed/spark/<run ID>/`.

Receipts: `data/processed/increment7-acceptance-qc9WZJAi/` and
`data/processed/increment7-drift-acceptance-nSF2ATRs/`. Each contains exact
commands/environment, stdout/stderr, reference/input checks where applicable,
public-summary observations, API responses and independent verification JSON.
Both runs were found through FastAPI TestClient routes using the default
evidence root. That is in-process HTTP routing evidence, not browser validation.

The reference `data/processed/drift/reference.csv` has 40,000 BENIGN-labelled
rows: 10,000 each from Monday, Tuesday, Wednesday and Thursday Morning WebAttacks,
matching `split_manifest.json`. SHA-256:
`2db894325651cb263656ffe3c506d670b9774e94af22cf7bd4007c03b8317358`.
All ten monitored features are finite; zero rows were dropped/imputed. Capture
groups are disjoint, but raw-row, temporal and training-set independence are
not established. Existing settings stayed window=1,000, alpha=0.05 with
per-feature alpha=0.005, minimum KS=0.10 and minimum two flagged features.

The final drift snapshot is
`7a3cd21e98f6c556786e7377912c216708ab2702166dbbe51da6817bd226c1c2`.
Its canonical report recomputed byte-identically from the saved transport and
bound reference. Nine expected intermediate insufficient-data snapshots and the
final evaluated snapshot are retained. Row 65 is included: its missing rate
fields are not KS-monitored features, and the fitted model's median imputer
allows its prediction. The actual model output was 999 BENIGN / one ATTACK in
each run. Drift is distribution change, not intrusion or model degradation;
predictions are not confirmed incidents and do not establish generalization.

## Current closure work

The selected-run Refresh fix reloads details/drift with the list, retains the
selected run, clears old results immediately, resets pagination and ignores
late aborted responses. Failed/missing/incomplete responses cannot preserve a
previous success. `frontend/tests/refresh.test.cjs` runs the actual TSX/HTTP
client in memory with existing React/TypeScript and a small state/effect
scheduler. It renders HTML assertions; it is explicitly not a DOM/browser test.
Regression-test fixtures exist only in memory. The separate manual browser
fixture described below is an isolated copy; neither modifies accepted runs.

New evidence directory: `data/processed/increment7-closure-35Ztaa7O/`.

| Check | Actual result | Evidence |
| --- | --- | --- |
| Refresh component regressions | 5 passed, 0 skipped | `refresh-tests-final.log` |
| Focused Increment 7 Python tests | 53 passed, 0 skipped; 0.393 s | `increment7-tests.log` |
| Full Python regression | 303 run, 301 passed, 2 opt-in integration skips; 3.798 s | `python-tests.log` |
| React production build | TypeScript/Vite passed; 28 modules; 526 ms | `react-build.log` |
| Manual browser UI | PASS for screenshot-visible scenarios confirmed by the user | `data/processed/increment7-browser-mhry7CAU/browser-verification.json` |
| Browser errors during testing | No errors reported by user; no DevTools capture | `data/processed/increment7-browser-mhry7CAU/manual-signoff.json` |

The closure preservation audit hashed 420 protected files, including all saved
Spark run files, model/feature manifest, sample, drift reference/split manifest
and the original 1.1 GiB capture-aware dataset. No hash changed. Both accepted
runs still pass source and inference verification; fresh in-process FastAPI GET
checks returned HTTP 200 for discovery, detail, drift and prediction counts.
Discovery lists eleven runs with both accepted runs present. Results are in
`final-preservation-and-evidence.json` and `api-verification.json` in the closure
receipt. All original run evidence remains outside the Git change set.

The first Refresh test attempt exposed a test-harness cross-realm Error mismatch
(4/5 passed); sharing the Error constructor between the in-memory module contexts
fixed the harness. The final direct Node test command passes. The initial
`node --test` child wrapper reported failure; direct execution of the node:test
file produces complete TAP results without an additional test subprocess.
These initial attempts are not counted as passes or hidden as product results.

Commands from repository root:

```bash
NODE_DISABLE_COMPILE_CACHE=1 /home/jaswanth/.nvm/versions/node/v22.23.5/bin/node frontend/tests/refresh.test.cjs
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" .venv/bin/python -B -m unittest tests.test_increment7_preflight tests.test_increment7_runner tests.test_increment7_discovery -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" IDS_KAFKA_INTEGRATION=0 IDS_SPARK_INTEGRATION=0 timeout 180s .venv/bin/python -B -m unittest discover -s tests -v
git diff --check
```

The full Python suite ran outside the sandbox due to the previously verified
TestClient hang. Skips are the explicit Kafka roundtrip and Spark/Kafka live
suites; neither was enabled. Existing accepted runs were reused for verification.
No Kafka/Spark acceptance run was started. The build used the existing NVM Node
22 installation with repository-local npm cache and temporary paths.

## User-guided manual browser results and reproducible procedure

The user operated the existing browser while the agent ran only loopback
FastAPI/Vite. Evidence is retained in `data/processed/increment7-browser-mhry7CAU/`:
`manual-observations.jsonl`, server logs, disposable fixture states,
`failed-fixture-api-check.json` and `browser-verification.json`.

User screenshots confirmed prediction counts (999 normal / one possible attack /
1,000 total), prediction-table rendering, forward pagination through record 1,000,
Previous navigation to 961–980, and inference-only monitoring disabled. Selection
of the drift UUID showed one 1,000-record sample marked Change detected with KS
statistics. The disposable `refresh-demo` was observed incomplete, then completed
after app Refresh without changing selection, then failed after another Refresh.
The final failed state displayed the simulated validation failure and hid the
previous counts/table. Component verification badges correctly retained completed
record accounting while marking overall processing unsuccessful.

The test changed only `fixture/runs/refresh-demo`, copied from accepted evidence.
All 150 original acceptance-run files were checked unchanged during transitions;
the final broader audit confirmed all 420 protected files unchanged, including
the original dataset, model, manifest, reference and original saved Spark evidence.
Direct prediction requests to the failed fixture intentionally returned HTTP 409.
No unexpected server 5xx or Vite errors were observed. Browser Console output was
not captured; the user subsequently reported no errors during testing, recorded
in `manual-signoff.json`. This is manual observation, not automated Console
verification. The optional missing-run
browser scenario was not executed; it remains covered by component regressions.

Both development servers were stopped with Ctrl-C after visual testing. Listener
verification found ports 8000, 5173 and 9092 closed and the confirmed server PIDs
absent. Kafka remained stopped. The fixture and all logs are retained.

Existing Windows Chrome and Edge binaries were found. No Playwright, Selenium,
Pyppeteer, Puppeteer or configured browser-control tool is available in this
session. Browser interaction was manual, supported by user screenshots, not an
automated browser test. The following original procedure is retained for
reproducibility; see the scoped results above for what actually passed. Do not
repeat it or create another acceptance run merely because it remains documented.

1. From repository root start the read-only evidence API in terminal A:

   ```bash
   PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
   ```

2. In terminal B start Vite, without installing dependencies:

   ```bash
   cd /home/jaswanth/projects/drift-aware-ids/frontend
   PATH="/home/jaswanth/.nvm/versions/node/v22.23.5/bin:$PATH" NODE_DISABLE_COMPILE_CACHE=1 npm_config_cache="$PWD/../data/processed/increment7-closure-35Ztaa7O/npm-cache" npm run dev -- --port 5173 --strictPort
   ```

3. Open `http://localhost:5173` in the existing Windows Chrome/Edge. If WSL
   loopback forwarding is unavailable, stop and report it; do not change firewall
   or Docker configuration. Select each accepted run by its UUID. Expect 1,000
   classifications, 999 predicted normal / one possible attack, and working
   Next/Previous pagination. Inference-only: Not checked. Drift-enabled: Change
   detected, one flagged/evaluated sample, and ten flagged features in statistics.
   Check browser Console and Network for unexpected errors. Record actual results.

4. Test Refresh transitions only with a disposable copy. Stop terminal A with
   Ctrl-C, then create the isolated fixture (destination must not exist):

   ```bash
   cd /home/jaswanth/projects/drift-aware-ids
   .venv/bin/python -B - <<'PY'
   import json, shutil
   from pathlib import Path
   base = Path('data/processed/increment7-closure-35Ztaa7O/browser-fixture')
   base.mkdir(exist_ok=False)
   source = Path('data/processed/spark/ee5c9e92-7c3d-407d-9836-8d6b643c1b5c')
   fixture = base / 'runs' / 'refresh-demo'
   shutil.copytree(source, fixture)
   complete = (fixture / 'summary.json').read_text()
   (base / 'completed-summary.json').write_text(complete)
   pending = json.loads(complete)
   pending.update(success=False, accounting_complete=False, inference=None,
                  errors=['Disposable fixture: simulated incomplete state'])
   pending['analysis'].update(success=False, accounting_complete=False,
                              status='incomplete', stage='browser-fixture')
   (fixture / 'summary.json').write_text(json.dumps(pending))
   PY
   IDS_EVIDENCE_ROOT=data/processed/increment7-closure-35Ztaa7O/browser-fixture/runs PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
   ```

5. Refresh the run list and select `refresh-demo`. Expect Analysis incomplete and
   no verified classifications. In terminal C publish the completed fixture
   summary atomically, then click Refresh without changing the selected run:

   ```bash
   cd /home/jaswanth/projects/drift-aware-ids
   .venv/bin/python -B - <<'PY'
   from pathlib import Path
   base = Path('data/processed/increment7-closure-35Ztaa7O/browser-fixture')
   temporary = base / 'runs/refresh-demo/summary.tmp'
   temporary.write_bytes((base / 'completed-summary.json').read_bytes())
   temporary.replace(base / 'runs/refresh-demo/summary.json')
   PY
   ```

   Expect the same selection, Analysis finished, 1,000 classifications and counts.
   Simulate a failed validation using only the copied summary, then Refresh:

   ```bash
   .venv/bin/python -B - <<'PY'
   import json
   from pathlib import Path
   base = Path('data/processed/increment7-closure-35Ztaa7O/browser-fixture')
   failed = json.loads((base / 'completed-summary.json').read_text())
   failed.update(success=False, errors=['Disposable fixture: simulated validation failure'])
   failed['analysis'].update(success=False, status='failed', failed_stage='browser-fixture')
   temporary = base / 'runs/refresh-demo/summary.tmp'
   temporary.write_text(json.dumps(failed))
   temporary.replace(base / 'runs/refresh-demo/summary.json')
   PY
   ```

   Expect Analysis incomplete, the simulated failure message and no displayed
   successful prediction result. To test missing-run handling without deleting anything, rename only the
   disposable copy from `refresh-demo` to `refresh-demo-retained`, then Refresh:

   ```bash
   .venv/bin/python -B -c 'from pathlib import Path; p=Path("data/processed/increment7-closure-35Ztaa7O/browser-fixture/runs/refresh-demo"); p.rename(p.with_name("refresh-demo-retained"))'
   ```

   Expect HTTP 404 feedback and no stale success. Retain the fixture and all
   acceptance evidence. Stop only the API/Vite terminals with Ctrl-C afterward.
   Record browser console/network results and confirm original hashes before
   declaring browser closure passed. Never start Kafka/Spark for these steps.

## Historical P1/P2 audit correction verification

Both blocking audit findings are fixed. Public success is owned solely by the
runner: it first saves an incomplete `summary.json`, while orchestrated Spark
workers and supervision use `spark-summary.json`. Every orchestration stage is
persisted, source verification uses the private Spark result, and public success
is the final atomic write after every gate and the final metadata write pass.
Standalone Spark and source-verifier defaults are unchanged. SIGTERM, Ctrl-C,
unexpected exceptions and SystemExit persist failures; SIGKILL leaves the last
public state incomplete while retaining private Spark evidence. Regression tests
include actual SIGTERM and a SIGKILL of an isolated broker-independent test child
after fake Spark success, plus worker/supervisor isolation and final-write failure.

Discovery now sorts all eligible runs by timezone-aware UTC creation metadata
before applying the cap, with descending run ID to break ties. Legacy/malformed
metadata falls back to regular config-file modification time, or summary-file
modification time when no regular config is available. Tests cover 101 UUID runs
with the lexically smallest run newest, equal timestamps, timezone offsets,
summary updates, missing/naive/malformed/oversized/deeply nested metadata, and
symlink config exclusion. Existing list/detail response fields are preserved.

Files changed for these corrections:

- `scripts/run_analysis.py`
- `src/streaming/spark_consumer.py`
- `src/streaming/verify_output.py`
- `backend/app/main.py`
- `tests/test_increment7_runner.py`
- `tests/test_increment7_discovery.py`
- `docs/increment7_reusable_analysis.md`
- `docs/increment7_verification.md`

New retained evidence: `data/processed/increment7-audit-fixes-4y1ABG0f/`.
Earlier evidence was not overwritten. No test failed during these corrections.
The read-only review additionally found and hardened the deeply nested JSON
fallback, then the focused and full suites were rerun on that final code.

| Final check | Result | Evidence file |
| --- | --- | --- |
| Targeted runner/discovery tests | 40 run, 40 passed, no skips; 0.320 s; exit 0 | `targeted-tests-post-audit.log` |
| All focused Increment 7 tests | 53 run, 53 passed, no skips; exit 0 | `increment7-tests-post-audit.log` |
| Full Python regression | 303 run, 301 passed, 2 opt-in integration skips; 3.492 s; exit 0 | `python-tests-post-audit.log` |
| React production build | TypeScript/Vite passed; 28 modules; 519 ms; exit 0 | `react-build.log` |

Exact verification commands (from repository root except the build):

```bash
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  .venv/bin/python -B -m unittest \
  tests.test_increment7_runner tests.test_increment7_discovery -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  .venv/bin/python -B -m unittest tests.test_increment7_preflight \
  tests.test_increment7_runner tests.test_increment7_discovery -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  IDS_KAFKA_INTEGRATION=0 IDS_SPARK_INTEGRATION=0 \
  timeout 180s .venv/bin/python -B -m unittest discover -s tests -v
git diff --check
```

The full suite ran outside the sandbox with approval because of the previously
verified TestClient sandbox hang. Its only skips remain the explicit Kafka and
Spark/Kafka integration suites. No real integration or external installation ran.
The build used the existing NVM Node installation from `frontend/`:

```bash
PATH="/home/jaswanth/.nvm/versions/node/v22.23.5/bin:$PATH" \
  npm_config_cache="$PWD/../data/processed/increment7-audit-fixes-4y1ABG0f/npm-cache" \
  TMPDIR="$PWD/../data/processed" npm run build
```

At this historical checkpoint, real Kafka/Spark acceptance was still pending;
the verified runs above supersede that status. SIGKILL cannot run process cleanup or
write a new failure reason; it can only leave the already-persisted incomplete
state. Legacy file-time ordering may be approximate after copying or manual
timestamp edits. No model, input, prior run, topic or volume was changed.

## Changes

- `scripts/run_analysis.py` orchestrates existing preflight, producer, Spark
  supervisor, source comparison, inference verification and optional drift.
  It reserves fresh identities, requires creation of one empty Kafka partition,
  binds a retained input snapshot with SHA-256, and saves stage-specific results.
- `src/streaming/spark_consumer.py` accepts an explicit exclusively reserved
  directory from the runner while rejecting existing Spark evidence. Its
  default CLI still rejects every existing run directory.
- `src/streaming/drift_monitor.py` checks an already supplied reference hash
  when preparing configuration, including supervisor preparation.
- Tests cover preflight malformed CSV/headers, blank labels, bounds, snapshots,
  changing source/model/reference, incomplete acknowledgements, interrupts,
  partial Spark accounting, source/prediction failures, optional drift results,
  API-readable summaries, and supervisor directory preservation.

No second inference/drift engine, service, job-launch API, or UI was introduced.
The existing model's fitted median imputer remains authoritative.

## Initial implementation results (before the P1/P2 audit)

Evidence directory: `data/processed/increment7-local-sMjzOfnN/` (ignored local
artifacts, retained). Commands ran from repository root unless noted.

| Check | Result | Evidence |
| --- | --- | --- |
| Python runtime and dependency consistency | Python 3.11.18; `pip check`: `No broken requirements found.` | Inspection output |
| Focused preflight/runner tests | 38 run, 38 passed, 0 skipped; 0.287 s; exit 0 | `focused-tests.log` |
| Final complete Python regression | 288 run, 286 passed, 2 skipped; 3.381 s; exit 0 | `python-tests-final.log` |
| React production build | TypeScript and Vite passed; 28 modules transformed; Vite build 546 ms; exit 0 | `react-build.log` |
| Offline real sample/model check | 1,000 source rows, 999 numeric-valid, 1 numeric-invalid, 1,000 actual predictions including row 65; exit 0 | `offline-inference.json` |
| Actual runner CLI with `--max-rows 10001` | Exit 1, rejected at preflight before Kafka; compatible failed summary persisted | `cli-rejected.json`, `rejected-runs/<UUID>/summary.json` |

Focused command:

```bash
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  .venv/bin/python -B -m unittest \
  tests.test_increment7_preflight tests.test_increment7_runner -v
```

Final regression command (executed outside the sandbox after approval):

```bash
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/data/processed" \
  IDS_KAFKA_INTEGRATION=0 IDS_SPARK_INTEGRATION=0 \
  timeout 180s .venv/bin/python -B -m unittest discover -s tests -v
```

Exact final footer:

```text
Ran 288 tests in 3.381s

OK (skipped=2)
```

The skips are `test_csv_broker_roundtrip` and
`test_valid_replay_and_explicit_malformed_failure`; both require explicit live
integration flags. No other regression tests were skipped.

Production build command, from `frontend/`:

```bash
PATH="/home/jaswanth/.nvm/versions/node/v22.23.5/bin:$PATH" \
  npm_config_cache="$PWD/../data/processed/increment7-local-sMjzOfnN/npm-cache" \
  TMPDIR="$PWD/../data/processed" npm run build
```

Node v22.23.5 was used; Vite reported v6.4.6. Existing installed dependencies
were used without network access or installation.

Actual rejection command:

```bash
.venv/bin/python -B -m scripts.run_analysis \
  --source data/processed/increment7_sample_1000.csv --max-rows 10001 \
  --evidence-root data/processed/increment7-local-sMjzOfnN/rejected-runs
```

It returned `success=false`, `accounting_complete=false`,
`failed_stage="preflight"`, and the exact error:
`max_rows must be between 1 and 10000 (Spark cap)`.

## Real input and immutable model evidence

The offline check used `csv_events` and the actual `StreamingInference` model
on all 1,000 sample rows, with synthetic Kafka coordinates only. It did not
contact Kafka or execute Spark. Predictions were counted from the returned
model outputs, rather than inferred from input validity.

- Required model features: 70.
- Zero-based invalid source row: 65; missing `Flow Bytes/s` and `Flow Packets/s`.
- Fitted imputer strategy: median.
- All 1,000 rows received predictions, including row 65.
- Input SHA-256: `20c998bf0d81d226b1c353268c30c5f63c6f792c4a35e3dbe91c3217fbf07141`.
- Model SHA-256 before and after: `f080418e1d130afb33343cb0bb3a8f181b4deefa27c4a4c734833e965da9846a`.
- Manifest SHA-256 before and after: `70605c79d9126ad6aa7eff8935080a326bf167fbf9dc9e0002d3c23c763c134f`.

Numeric validity describes required feature completeness. Transport validity
describes schema/identity checks, and prediction counts describe actual saved
classifications. Neither prediction nor drift is confirmed intrusion evidence.
Random-split accuracy and this local execution do not establish generalization;
the existing held-out PortScan limitation remains unchanged.

## Sandbox execution limitation and resolution

The first complete run stalled at the existing
`test_increment6a_api.EvidenceApiTests.test_drift_authoritative_snapshot_compact`.
It was interrupted, not counted as a pass. The log remains `python-tests.log`.
An isolated diagnostic with `timeout 20s` and a ten-second faulthandler dump
returned exit 124; its exact first output was:

```text
test_drift_authoritative_snapshot_compact (tests.test_increment6a_api.EvidenceApiTests.test_drift_authoritative_snapshot_compact) ... Timeout (0:00:10)!
```

`api-diagnostic.log` retains the full traceback: the calling thread was waiting
in Starlette TestClient/AnyIO while its event-loop thread waited in `select`.
The same suite passed outside the sandbox after permission was granted:
first 287 tests, then the final 288 after adding a reference-rebinding regression.
This supports an environment-dependent sandbox hang; no application/test
workaround or dependency change was introduced.

## Remaining closure

Real inference-only and drift-enabled acceptance are already verified; do not
repeat them. Requested manual visual checks passed and the user reported no
browser errors; the optional missing-run browser case was not performed.
Preserve all saved attempts. Confirm college report/PPT format and required
content before producing final evaluation artifacts. No commit or merge is
authorized. See [run instructions](increment7_reusable_analysis.md).
