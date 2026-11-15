# Increment 4 — Drift/error validation (draft)

## Scope

This increment validates the existing held-out PortScan drift/error experiment before rerunning it. It does **not** retrain the classifier, change the KS rule, modify Kafka/Spark, or claim that drift causes classifier errors.

Existing analysis: `python -m src.drift.analyze_drift_vs_errors`.
The analysis uses 1,000-row windows, ten monitored features, Bonferroni-corrected KS tests, KS >= 0.10 and at least two flagged features for an alert. It computes classifier error rate, attack fraction, attack recall, benign false-positive rate, raw Pearson correlation and prevalence-adjusted descriptive correlation.

## WSL verification (only after reviewing this draft)

```bash
cd ~/projects/drift-aware-ids
git fetch origin
git switch -c feat/increment4-drift-model-validation --track origin/feat/increment4-drift-model-validation
source .venv/bin/activate
python -m unittest discover -s tests -v
python -m src.drift.increment4_preflight
python -m src.drift.analyze_drift_vs_errors
```

Expected local inputs:
- `data/processed/drift/reference.csv`
- `data/processed/drift/heldout_portscan_predictions.csv`

Generated local reports:
- `models/increment4_preflight.json`
- `models/drift_vs_errors_portscan.json`

Do not commit large data files or local reports. Send terminal output and report summary for review. Passing tests alone is not acceptance of scientific conclusions.

## Known limitations and gates

- The prediction export identifies the capture, but not original raw CSV row IDs.
- A 1,000-record window is not a verified chronological time window.
- The preflight fingerprints both input files and checks monitored features, binary labels, prediction consistency and single-capture membership; it does not prove the reference was excluded from model training.
- The existing drift/error analyzer is exploratory; prevalence adjustment is linear and descriptive, not causal.
- The original held-out capture model may encounter attack types absent from training; this must be interpreted explicitly.
- Real-world false-alarm rates, temporal generalization and unseen-capture performance are not established by the controlled Monday validation.

## Review checklist

1. Offline tests pass in WSL.
2. Preflight verifies the actual local input files and emits SHA-256 fingerprints.
3. Existing drift/error report runs without error, with aligned complete windows.
4. Report correlation, adjusted correlation, alert rate, errors and attack fractions; do not select only favorable metrics.
5. Inspect whether further experiments are warranted before accepting the increment.
6. Merge only with explicit user approval.
