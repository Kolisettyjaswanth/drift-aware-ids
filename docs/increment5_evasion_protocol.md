# Increment 5 — Offline evasion robustness protocol (proposed)

## Objective and boundaries

Quantify how the **existing held-out capture classifier** changes predictions under constrained feature perturbations. This is an offline, authorized dataset evaluation; it is **not** packet generation, live attack deployment, or proof of feasible network evasion.

Preserve Kafka, Spark, drift KS thresholds, and Increment 4 artifacts. No model retraining, no dashboard, and no automatic merges.

## Audit findings

- `src/training/evaluate_heldout_capture.py` currently trains a 100-tree RandomForest pipeline from a 300,000-row capture-excluding sample and evaluates PortScan, but does **not persist the fitted pipeline or feature ordering**.
- Its exported held-out predictions contain 10 drift features, whereas the classifier uses additional features. Therefore **the prediction CSV cannot be used to recompute perturbed model predictions**.
- The held-out PortScan attack recall is ~0.096%; nearly all attacks are **already missed**. A metric defined as "fraction of all attacks missed after perturbation" would be misleadingly high even with no perturbation.
- The training sample excludes the PortScan capture; the reference split is capture-separated from this holdout.

## Proposed measurable acceptance criteria

1. Reuse exactly one deterministic, capture-separated fitted classifier and its full feature order for clean and perturbed predictions. Fit once per experiment run; do not compare separate retrainings.
2. Compute baseline confusion matrix, attack recall, benign false-positive rate, and baseline-correctly-detected attack count. Compare to Increment 4 only when the pipeline and dataset match.
3. Use a **small explicitly approved allowlist of numeric, non-label features** and bounded changes; preserve nonnegative constraints and record every changed value. Reject NaN, infinities, missing columns, or changed labels. No claim that independent tabular perturbations are physically realizable.
4. Report conditional evasion rate = originally correctly detected attacks that become predicted benign / originally correctly detected attacks. Report denominator; mark **not estimable** when zero. Also report absolute changes in attack recall and benign FPR, and how many samples were modified.
5. Fix random seeds, data fingerprints, feature order, perturbation configuration, and output schema. Include no-perturbation control and a negative-control feature/strength.
6. Add synthetic fixture unit tests for constraints, unchanged inputs, reproducibility, denominator handling, and report consistency. Execute full offline regression suite; retain two opt-in integration skips explicitly.
7. Produce one machine-readable result and one concise research interpretation, including limitations and negative results.

## First implementation decision

Before adding perturbation code, choose whether to (A) refactor the existing held-out evaluation into a reusable fit/evaluate API or (B) add an isolated robustness runner that imports its sampler and reconstructs the same pipeline. Prefer **A** if the refactor preserves the current CLI outputs and is covered by regression tests.

Do not interpret arbitrary CIC-IDS2017 feature edits as feasible real network behavior. Any stronger claim requires packet/flow-level feasibility checks beyond this increment.
