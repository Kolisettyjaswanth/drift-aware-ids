# Progress

## Completed research and pipeline milestones

- CIC-IDS2017 preparation and audit, development RandomForest, capture-held-out evaluation, KS drift validation, and evasion/perturbation research components.
- Kafka bounded CSV replay, Spark Structured Streaming transport, saved model inference, optional drift monitoring, and persisted run evidence.
- Increment 6A: read-only FastAPI evidence endpoints, including run summaries, drift evidence, verified predictions and counts.
- Increment 6B: React/TypeScript/Vite saved-evidence dashboard, search, reliability disclosures, pagination, and responsive layouts.

## Increment 6 acceptance (user-executed, November 2026)

- Python `unittest`: 250 tests run, 248 passed, 2 intentionally skipped (opt-in live Kafka and Spark integration tests), no failures.
- Final React `npm run build`: passed; Vite transformed 28 modules.
- Browser: seven checks passed, including next/previous prediction pagination and 390px mobile layout.
- Git feature branch `feat/increment6b-react-dashboard` was clean and synchronized at `238e4ca` before this documentation update.
- See [Increment 6 closure audit](increment6_closure_audit.md) for scope, evidence, and limitations.

## Research limitations and future work

This is bounded recorded flow replay, **not live packet capture or production intrusion prevention**. The model is scikit-learn RandomForest, not Spark MLlib. Held-out PortScan recall was extremely poor and should not be obscured by development-split accuracy. Drift alerts indicate distribution change, not confirmed malicious traffic. PostgreSQL persistence and deployment hardening are not part of Increment 6. The two opt-in live integration tests were skipped in the final suite, so that run does not independently revalidate live broker behavior.

Increment 7 reusable input analysis remains separate and paused pending Increment 6 closure. Do not merge into `master` without approval.
