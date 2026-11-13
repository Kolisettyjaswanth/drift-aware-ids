# Drift validation increment

Base: `8f287c137aa4e80fedbfa69cb9ec82c8581dac9d`. Branch: `feat/drift-validation`.

## Scope and behavior

`src/drift/ks.py` owns KS comparisons and the significance-plus-effect-size rule.
`detect_window` is the strict reusable API: alpha .05/feature count, KS >= .10,
window size 1000, at least 100 reference values, and at least two drifted features.
Missing columns, nonnumeric/nonfinite values, and incomplete/oversized windows
raise ValueError; they never become a silent negative alert.

Eight existing KS experiment scripts delegate through a compatibility adapter.
They retain their original `auto`/`asymp` method, sample cleaning, window selection,
output schemas, and one-feature or two-feature alert rule. Finite-input KS results
are regression-tested against SciPy. Invalid values that previously propagated
NaN statistics now fail explicitly. The opt-in `drop` policy reports insufficient
samples as an error. Historical experiments are exploratory, not one standardized
benchmark; do not compare their alert rates as equivalent rules.

## Reproduce

From repository root, in the project's Python environment:

```bash
python -m unittest discover -s tests -v
python -m src.drift.validate_synthetic
python -m compileall -q src scripts tests
python -m src.drift.analyze_drift_vs_errors
```

The last command needs excluded reference and held-out prediction CSVs.
It now adds raw and attack-prevalence-adjusted correlations to the JSON report.
Adjustment regresses each variable on an intercept and attack fraction, then
correlates residuals. At least four finite aligned windows are required; constant
variables or effectively zero residual variation yield JSON null. This is linear,
descriptive adjustment, not evidence of causality. It does not assert the user's
previous 0.994566/0.10210 results without the original input files.

## Predefined synthetic acceptance criteria

Five seeds (0–4), each with one independent 10,000-row Gaussian reference and
40 independently generated 1,000-row windows, with ten features. Each unchanged
window is paired with copies shifted on the first two features: +1.5 location,
or multiplication by 4 for scale. No labels, attack mixture or classifier are
involved. Input generation and thresholds do not tune themselves to results.

- No shift: <=5% alerts across 200 windows.
- Each strong shift: >=90% alerts across 200 windows.
- One-feature shift: report the feature but no two-feature window alert (unit test).
- Report per-seed counts, pooled rates, descriptive Wilson intervals, dependency
  versions and deterministic input fingerprint in `validation/synthetic.json`.

These are strong known shifts, not a claim of sensitivity to all drift types.
Marginal KS cannot detect every joint-distribution shift.

## Actual verification

18 unit/integration-smoke tests discovered: **17 passed, 1 skipped**.
Compilation succeeded for src, scripts and tests.
Synthetic validation: no shift **0/200**, location **200/200**, scale **200/200**.
All three predefined synthetic rate criteria passed. Each seed had 0/40,
40/40 and 40/40 respectively. Wilson intervals are descriptive because windows
share references; they are not an independence guarantee.

Executed with Python 3.12.14, NumPy 2.3.5, pandas 2.2.3, SciPy 1.17.0.
The repository declares Python 3.11 and different exact pins: that environment
has not been verified here. No dependency pins were changed.

## Initial assistant execution limits (historical)

The dataset smoke test skips explicitly when reference.csv or
heldout_portscan_predictions.csv is missing. Raw/processed CIC-IDS2017, trained
models and prediction outputs are absent from this checkout. Consequently the
following were not run: same-capture disjoint benign validation, paired real-data
shift validation at fixed attack fraction, all historical capture experiments,
held-out model retraining, and reproduction of the observed raw/adjusted
correlations. These are unavailable checks, not passes.

At that point, remaining checks required input fingerprints,
record counts, capture separation, locked thresholds and results. Synthetic
success alone does not authorize Kafka/Spark advancement.

## Changed files and verification

- `ks.py`: shared comparison/rule and strict window API; input, boundary,
  two-feature and legacy parity tests.
- Eight KS callers (`detect_drift`, `evaluate_benign_control`,
  `evaluate_matched_reference`, `evaluate_friday_groups`, `evaluate_ddos_capture`,
  `evaluate_broad_reference`, `compare_reference_captures`,
  `analyze_drift_vs_errors`): shared implementation; compilation and broad-reference
  synthetic smoke test. Full dataset regressions remain unavailable.
- `correlation.py`: raw/OLS-residual correlations; known residual, constant,
  degenerate and invalid-input tests.
- `validate_synthetic.py`: deterministic controlled acceptance runner; executed.
- `tests/test_drift.py`: executable tests and explicit dataset skip.
- `docs/validation/synthetic.json`: generated evidence from the executed runner.
- README, architecture, progress, setup, experiments and tests documentation:
  distinguish implemented offline code from pending real-time services.

## Controlled real-data runner added

The remaining same-capture gate is implemented in `src/drift/validate_real.py`.
See [protocol and WSL commands](controlled_real_validation.md). The user now reports real-data acceptance passed at `6ba34e5`: 0/200 unchanged
and 200/200 for each shift, with 27/27 local tests on Python 3.11.18. These are
user-executed results, not independent assistant execution. See the linked
protocol for final review findings and evidence attribution.
