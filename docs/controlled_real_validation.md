# Controlled CIC-IDS2017 validation gate

## Protocol (fixed before running real data)

Use the original single Monday BENIGN capture. The loader requires Label and
all ten monitored features, strips header/label whitespace, rejects mixed attack
labels or multiple SourceFile values, and excludes nonfinite/nonnumeric rows
once before sampling. Excluded counts are recorded; missing columns are errors.

For seeds 0,1,2,3,4, select 50,000 eligible row identities without replacement:
10,000 reference rows and 40 disjoint windows of 1,000 evaluation rows. Reference
and evaluation identities cannot overlap within a seed. Seeds can reuse rows.

Each window has three paired scenarios:

1. Unchanged traffic.
2. Add `5 * max(reference population standard deviation, 1)` to Average Packet
   Size and Packet Length Mean. The offset uses reference data only.
3. Multiply those same two features by 10.

Every scenario uses src/drift/ks.py: alpha=.05 (Bonferroni over ten features),
KS >= .10, minimum two drifted features, asymptotic KS, strict finite input,
1,000-row windows. No thresholds are fitted to evaluation results. Labels,
row IDs, all other features, and zero attack prevalence remain identical.
Artificial shifts are deliberately strong; they are not realistic packet edits.

Aggregate acceptance over 200 paired windows per scenario:
unchanged alert rate <= .05, each shifted alert rate >= .90. The command exits
0 only on acceptance, 1 on measured rate failure; input/runtime errors also
exit nonzero and must not be confused with measured failure. Failed measured
experiments still write their report. Do not tune failed scenarios after seeing
results and present them as this predefined experiment.

## Exact WSL commands

```bash
cd ~/projects/drift-aware-ids
git switch feat/drift-validation
git pull --ff-only origin feat/drift-validation
source .venv/bin/activate
python --version
python -m unittest discover -s tests -v
python -m src.drift.validate_real \
  --input data/raw/cicids2017/MachineLearningCVE/Monday-WorkingHours.pcap_ISCX.csv \
  --output models/controlled_real_validation.json
```

If your extracted folder differs, change only --input to the Monday CSV path.
The default dataset and model files remain untracked. Report review:

```bash
python - <<'PY'
import json
from pathlib import Path
r=json.loads(Path('models/controlled_real_validation.json').read_text())
print('Passed:', r['passed'])
print('Aggregate:', json.dumps(r['aggregate'], indent=2))
for seed in r['seeds']:
    print('Seed:', seed['seed'], 'alerts:', seed['alerts'], 'rates:', seed['rates'])
print('Versions:', r['versions'])
print('Provenance:', r['provenance'])
PY
```

## Report and interpretation

The report retains raw-file SHA-256, eligible-data fingerprint, excluded row
counts, reference/evaluation fingerprints, exact reference/window row IDs,
per-scenario fingerprints and feature statistics, labels fingerprint, attack
fraction, per-seed rates, aggregate rates and descriptive Wilson intervals.
It also records Python/NumPy/pandas/SciPy versions, validator hash, current Git
commit and worktree dirty status. Keep the original input CSV to replay row IDs.

Disjoint identities do not imply distinct feature vectors: original duplicate
flows may remain. Randomized windows do not establish chronology, independence,
or deployment false-alert rates. Seeds share source data and windows share
references, so pooled confidence intervals are descriptive. Benign-only results
do not establish behavior at nonzero attack prevalence. Correlated marginal
features do not constitute two independent pieces of evidence. Zero-heavy
features can resist multiplicative shifts; actual failures must be reported.

## Verification status

User-executed local results, reported for commit `6ba34e5` on
`feat/drift-validation`, Python 3.11.18:

| Check | Reported result |
|---|---|
| Automated tests | 27 passed, 0 failed, 0 skipped |
| Controlled real-data gate | passed |
| Unchanged | 0/200 alerts |
| Location shift | 200/200 alerts |
| Scale shift | 200/200 alerts |

These reported rates satisfy the predefined acceptance thresholds. The assistant
has not independently run CIC-IDS2017 or inspected the local JSON report; no
local dataset fingerprint, dependency versions beyond the reported Python, or
per-seed records are asserted here. Retain the original generated JSON report
and raw input locally for provenance and replay.

Separately, assistant-executed checks at review: 27 tests discovered, 26 passed,
one existing CIC-IDS2017 prediction/reference smoke test skipped because those
inputs are unavailable; compilation succeeded. Runtime: Python 3.12.14 with
NumPy 2.3.5, pandas 2.2.3, SciPy 1.17.0. Earlier assistant-executed full-size
uniform fixture CLI produced 0/200 unchanged and 200/200 each shift; that fixture
is not real-data evidence.

Earlier user-reported analysis: 286 real-data windows, raw correlation
0.9945655976175768 and adjusted correlation 0.10210084333683445. These are separate
from the paired benign gate and remain descriptive rather than causal evidence.

## Final review of code at 6ba34e5

No blocking issue found for merging this bounded offline drift-validation
increment, based on code review plus the explicitly attributed local results.
Reference/evaluation identities are disjoint within each seed; reference-only
location parameters avoid evaluation-driven tuning; paired copies preserve
labels, row identities and non-target features. Every new gate scenario uses
the same strict detector and thresholds. Correlation adjustment uses OLS
residuals with an intercept. The PR changes only drift code, tests and related
documentation; no Kafka/Spark or application changes were found.

Nonblocking limitations/follow-ups:

- Average Packet Size and Packet Length Mean are related, so the two-feature
  rule is not independent evidence. This is a controlled sensitivity test.
- Duplicate feature vectors can occur across disjoint identities. Shared
  references and overlapping seeds prevent treating 200 windows as independent.
- Historical one-feature/two-feature and auto/asymp rules remain intentional;
  compare only like-for-like reports. The new strict gate uses two features.
- Synthetic input fingerprint covers evaluation windows, not reference arrays;
  fixed seeds/versions reproduce the references. The real validator fingerprints
  both reference and evaluation inputs.
- Direct execution of tests/test_drift.py reaches unittest.main before the final
  test class; use the documented unittest discovery command to include all tests.
- Same-capture benign validation does not establish deployment accuracy, unseen
  attack detection, temporal drift detection or causal drift/error dependence.

Recommendation: ready for user review and merge as the scoped offline
validation increment; not a claim that the entire real-time IDS is complete.
The PR remains draft and unmerged. No Kafka/Spark implementation was begun.

## Files in this increment

- src/drift/validate_real.py: loading, sample separation, paired transformations,
  strict detector orchestration, provenance and report/exit behavior.
- tests/test_real_validation.py: nine automated tests using generated fixtures.
- docs/controlled_real_validation.md: protocol, commands, evidence and limitations.
- docs/drift_validation.md and docs/progress.md: link the remaining gate and
  distinguish reported local verification from this environment's execution.
