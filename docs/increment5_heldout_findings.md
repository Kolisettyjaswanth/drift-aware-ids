# Increment 5 — Full held-out robustness findings

## Verification status

WSL experiment completed using 300,000 sampled training records, 286,467 held-out PortScan rows, 70 model features, and nine configurations (three features × strengths 0%, 5%, 10%). User verified the full report and clean Git branch.

- Dataset SHA-256: `57d5e1ce125b5aa911bc9e1bcc92d7c5bc851df1caacc0395010f13945f2b1c4`
- Audit SHA-256: `fef5e20dd9af18d73ea10b41f77fcb92545f3680613a11f49a6d3594b3cd27b0`
- Baseline confusion matrix, rows actual BENIGN/ATTACK and columns predicted BENIGN/ATTACK: `[[126928,609],[158778,152]]`
- Baseline attack recall: `152/158930 = 0.0009563958975649657` (0.09564%).
- Baseline benign false-positive rate: `609/127537 = 0.0047750848773297164` (0.47751%).
- Baseline exactly matches the earlier Increment 4 held-out evaluation.

| Feature | Relative increase | Prediction flips | Benign false positives | Attack true positives | Newly missed previously detected attacks |
|---|---:|---:|---:|---:|---:|
| Average Packet Size | 0% | 0 | 609 | 152 | 0 |
| Average Packet Size | 5% | 2 | 607 | 152 | 0 |
| Average Packet Size | 10% | 25 | 584 | 152 | 0 |
| Packet Length Mean | 0% | 0 | 609 | 152 | 0 |
| Packet Length Mean | 5% | 11 | 598 | 152 | 0 |
| Packet Length Mean | 10% | 11 | 598 | 152 | 0 |
| Packet Length Std | 0% | 0 | 609 | 152 | 0 |
| Packet Length Std | 5% | 1 | 608 | 152 | 0 |
| Packet Length Std | 10% | 8 | 601 | 152 | 0 |

Each nonzero configuration changed only benign false positives into true negatives. Conditional evasion rate is 0/152 = 0% **for each tested configuration**. Do not add flip counts across configurations as if they were disjoint records.

## Interpretation

These particular independent synthetic feature increases did not induce any additional attack false negatives. They modestly reduced false positives. This is **not evidence of broad evasion resistance**, because 158,778/158,930 PortScan attacks were already missed before perturbation. The model exhibits severe held-out-capture generalization failure. A null result under a narrow feature edit cannot establish network-level robustness.

## Limitations and next acceptance gates

1. Feature edits are independent and do not enforce flow consistency, correlated feature invariants, or packet-level feasibility.
2. Only three packet-length features and two nonzero positive strengths were tested; no generalization to other features, perturbation directions, or attackers.
3. Training sample row identities and serialized fitted model artifact are not persisted; source and audit fingerprints plus deterministic seed are recorded, but model artifact provenance is incomplete.
4. The report includes aggregate confusion matrices, not per-row transition identities. Current observed matrices plus reported flip counts support the conclusion, but a future report should persist explicit transition counts and verify they sum to flips.
5. Final closure requires regression tests for report invariants, a review of model/data provenance and resource use, and a clean full test suite. Do not merge until explicit approval.
