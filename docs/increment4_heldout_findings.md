# Increment 4 — Held-out PortScan evidence (WSL reported)

## Status and provenance

The user executed the checks on the Increment 4 feature branch on 2026-11-15. Results below are transcribed from terminal output, not independently rerun against the local CIC-IDS2017 dataset by the reviewer. The local Git working tree was clean. This evidence is **not** acceptance of Increment 4 or evidence of production readiness.

- Tests: 193 discovered, 191 passed, 2 opt-in Kafka/Spark tests skipped.
- Reference: 40,000 rows; SHA-256 `2db894325651cb263656ffe3c506d670b9774e94af22cf7bd4007c03b8317358`.
- Prediction export: 286,467 rows; SHA-256 `f6c017b1a97e6a984d8b1234b51de69b6fadd078a3d6a416d8633b5aa6e6b89d`.
- Holdout: `Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv`.
- Training: 300,000 rows. Unseen held-out label: `PortScan`.
- 286 complete 1,000-record windows; 467 trailing records excluded from window analysis.

## Model performance on held-out capture

| Metric | Value |
| --- | ---: |
| Accuracy | 0.4436113060143053 |
| Precision (attack) | 0.19973718791064388 |
| Recall (attack) | 0.0009563958975649657 |
| F1 (attack) | 0.0019036764751927159 |
| True negatives | 126,928 |
| False positives | 609 |
| False negatives | 158,778 |
| True positives | 152 |

Attack recall is ~0.096%, i.e. 158,778 of 158,930 attacks missed. The held-out PortScan label is absent from training, so this is an unseen-attack generalization test, **not** a measure of detection quality for attacks represented in training.

## Drift versus classifier errors

| Metric | Value |
| --- | ---: |
| Alerted windows | 248/286 |
| Non-alerted windows | 38/286 |
| Drift alert rate | 0.8671328671328671 |
| Mean error over complete windows | 0.5572902097902097 |
| Mean error in alerted windows | 0.6413749999999999 |
| Mean error in non-alerted windows | 0.008526315789473684 |
| Raw mean-KS/error Pearson correlation | 0.9945655976175768 |
| Prevalence-adjusted Pearson correlation | 0.10210084333683445 |

The dramatic reduction after linear adjustment for attack fraction is consistent with attack prevalence being a strong confounder. It **does not** establish causation, statistical significance, or that drift alarms reliably forecast future model errors. The windows are contiguous exported record blocks, not independently verified chronological windows; adjacent windows and capture mixtures can be dependent.

## Outstanding acceptance work

1. Verify reference provenance and exclusion of held-out capture from reference/training using existing lineage artifacts.
2. Validate interpretation of window attack prevalence and error distributions; avoid reporting aggregate alert rate as deployment false-positive rate.
3. Review whether additional cross-capture controls are necessary for the bounded scientific claim.
4. Preserve unchanged KS thresholds and working Kafka/Spark integration.
5. Require explicit user approval before merging PR #6.
