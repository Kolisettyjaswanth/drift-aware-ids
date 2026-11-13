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

The user reports Python 3.11.18, prior 18/18 tests, prior synthetic acceptance,
286 real-data windows, raw correlation 0.9945655976175768 and adjusted correlation
0.10210084333683445. These are user-reported local results, not this gate's results.

In the development environment: 27 tests discovered, 26 passed, one existing
CIC-IDS2017 prediction/reference smoke test skipped for unavailable files.
New tests cover sample separation, deterministic sampling, pairing, no mutation,
loader filtering/provenance, fixed settings, strong shifts on fixtures, aggregate
failure and exact acceptance boundaries. CLI help and compilation were checked.
A full-size 50,000-row generated uniform fixture exercised the CLI: 0/200
unchanged, 200/200 location and 200/200 scale alerts, exit 0. Fixture success is
not CIC-IDS2017 acceptance. Attempting the real Monday path failed with
FileNotFoundError because the CSV is absent. Real CIC-IDS2017 acceptance remains pending execution
of the command above. No Kafka/Spark work or master merge is authorized here.

## Files in this increment

- src/drift/validate_real.py: loading, sample separation, paired transformations,
  strict detector orchestration, provenance and report/exit behavior.
- tests/test_real_validation.py: nine automated tests using generated fixtures.
- docs/controlled_real_validation.md: protocol, commands, evidence and limitations.
- docs/drift_validation.md and docs/progress.md: link the remaining gate and
  distinguish reported local verification from this environment's execution.
