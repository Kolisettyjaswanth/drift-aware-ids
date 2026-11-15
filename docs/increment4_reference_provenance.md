# Increment 4 — Reference capture provenance (user-executed WSL verification)

## Status

The user ran a read-only verification against `data/processed/drift/reference.csv` and `data/processed/drift/split_manifest.json` on 2026-11-15. All eight checks returned PASS and the script ended with `PROVENANCE CHECK PASSED`. These are user-reported local results, not independently rerun by the reviewer.

| Check | Result |
| --- | --- |
| Actual reference captures match manifest | PASS |
| Reference contains only BENIGN labels | PASS |
| PortScan holdout excluded from reference | PASS |
| PortScan holdout assigned to evaluation group | PASS |
| Reference and calibration capture sets disjoint | PASS |
| Reference and evaluation capture sets disjoint | PASS |
| Calibration and evaluation capture sets disjoint | PASS |
| Reference row count matches manifest | PASS |

Actual reference captures:
- `Monday-WorkingHours.pcap_ISCX.csv`
- `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv`
- `Tuesday-WorkingHours.pcap_ISCX.csv`
- `Wednesday-workingHours.pcap_ISCX.csv`

Reference rows: **40,000**. Reference SHA-256 (from separate successful preflight): `2db894325651cb263656ffe3c506d670b9774e94af22cf7bd4007c03b8317358`.

## Interpretation

This verifies the reference CSV's capture membership and benign labels against the manifest, and the manifest's group disjointness. It does **not** establish raw-record identity provenance, chronological independence, or reference-versus-training row disjointness. The training sampler excludes the PortScan holdout by SourceFile in code, but this check did not rerun the sampler. The held-out model metrics list PortScan as unseen.

The PortScan evaluation result is a severe unseen-attack failure, not evidence of production-ready intrusion detection. Preserve the reported confusion matrix and the raw versus prevalence-adjusted drift/error correlations without changing thresholds to optimize reported results.
