# Increment 4 — Training capture separation (WSL evidence)

User-executed on 2026-11-15: `python -m src.training.sample_capture_data` using local capture-aware CIC-IDS2017 CSV. This result was supplied as terminal output; it has not been independently rerun in the GitHub environment.

## Observed results

- Eligible records after excluding held-out capture: **2,544,276**.
- Training sample: **300,000** records from **7** captures.
- `Holdout present: False`.
- `PortScan` absent from training labels.
- Training label counts:

| Label | Count |
| --- | ---: |
| BENIGN | 252904 |
| DoS Hulk | 27281 |
| DDoS | 15091 |
| DoS GoldenEye | 1266 |
| FTP-Patator | 960 |
| SSH-Patator | 695 |
| DoS slowloris | 678 |
| DoS Slowhttptest | 648 |
| Bot | 220 |
| Web Attack Brute Force (source contains encoding replacement character) | 163 |
| Web Attack XSS (source contains encoding replacement character) | 78 |
| Web Attack Sql Injection (source contains encoding replacement character) | 9 |
| Infiltration | 5 |
| Heartbleed | 2 |

BENIGN fraction: **252,904 / 300,000 = 84.3013%**. The absent PortScan training class and severe held-out PortScan failure must be reported together; class imbalance alone does not establish causation.

## What was verified, and what was not

The executed sampling function excludes all rows whose `SourceFile` matches the held-out PortScan capture. The holdout capture is also absent from the drift reference, as checked separately. The output does not independently fingerprint the training sample, establish row-identity-level disjointness across all source artifacts, or prove any chronological separation.

The held-out model evaluation and drift/error report were generated before this run, so this is a corroborating reproduction of the sampler's exclusion rule, not proof that the identical sampled row identities were used by the earlier model training execution.

Increment 4 remains open pending final review and explicit user approval to merge.
