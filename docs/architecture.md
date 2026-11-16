# Implemented prototype architecture

The current implementation is a bounded historical network-flow CSV replay and
saved-evidence explorer. Two real 1,000-record Kafka/Spark runs (inference-only
and inference plus KS) passed. See [Increment 7 verification](increment7_verification.md)
for run IDs and attributed evidence, not claims of live packet capture or
production security performance.

```text
Capture-aware CIC-IDS2017 flow-feature CSV
  -> bounded read-only preflight and SHA-256-bound input snapshot
  -> existing Python replay producer
  -> fresh one-partition Kafka topic
  -> Spark 3.5.7 local[2] Structured Streaming / foreachBatch
       -> event validation and exact transport/provenance accounting
       -> existing scikit-learn median-imputer + RandomForest pipeline
       -> optional KS comparison against an existing BENIGN-labelled reference
  -> saved JSON/JSONL records, predictions, drift snapshots and final summary
  -> read-only FastAPI evidence endpoints
  -> React/TypeScript dashboard
```

Spark MLlib inference and PostgreSQL were early design ideas; they are not part
of this runtime. The fitted model is reused without retraining. Inference and
validation in `foreachBatch` are deliberately bounded driver-side operations,
not a scalable distributed scoring design. Kafka uses the existing single-node
KRaft Compose configuration; this is not a multi-node production deployment.

## Component boundaries

- `scripts/preflight_replay.py` validates bounded CSV/schema/numeric compatibility.
- `scripts/run_analysis.py` binds a retained input snapshot, creates fresh
  identities/topic, checks exact producer acknowledgements, calls the existing
  Spark supervisor and source/inference verifiers, and finalizes public evidence.
- `src/producer/` preserves source rows/captures, labels and invalid-feature
  metadata in schema-v1 events. It is replay, not NIC/PCAP ingestion.
- `src/streaming/` handles bounded transport, model inference and optional
  drift windows/evaluation/persistence. Numeric-invalid rows remain transportable
  and can receive predictions through the model's existing median imputer.
- `src/drift/` contains the shared KS detector and offline drift experiments.
- `src/robustness/`, `src/training/`, `src/preprocessing/` retain existing
  research, held-out evaluation and preparation tools; handoff does not run them.
- `backend/app/` reads persisted evidence. It does not start jobs or write runs.
- `frontend/` displays the saved classifications and drift conclusions separately.

## Evidence and success contract

Each analysis uses fresh run/replay IDs and a topic whose creation must succeed.
Publishing completes before Spark starts. Transport acceptance checks source
rows 0..N-1, intended replay/topic, unique IDs/offsets and exact counts. Model
features, source identity and invalid-field provenance are retained.

Orchestrated Spark writes `spark-summary.json`; public `summary.json` remains
unsuccessful until the orchestration layer completes source, inference and
optional drift verification. A failed later gate preserves completed transport
accounting without claiming overall success. Signal/exception handling preserves
stage-specific evidence. SIGKILL cannot perform cleanup, so the last public
incomplete state remains authoritative until finalization actually occurs.

Drift uses unchanged 1,000-record capture-boundary windows and ten monitored
features. Reference loading is strict, finite, provenance-aware and fingerprint
bound. Insufficient/invalid windows do not receive invented boolean alerts;
immutable snapshots preserve partial and final evaluation states. See
[the reusable analysis contract](increment7_reusable_analysis.md).

## Reading evidence and reproducing the environment

The API/dashboard can display already saved results with Kafka/Spark stopped.
Models, datasets and saved evidence are intentionally ignored by Git and must
be transferred separately. See [setup](setup.md) for minimal evidence-viewing
dependencies and the additional full-analysis requirements. The earlier
[Kafka transport](kafka_transport.md) and [Spark streaming](spark_streaming.md)
documents retain historical milestone details; current integration status is
recorded in Increment 7 verification.

Drift means feature-distribution change, not proof of intrusion, model
degradation or causal error. Model labels are not confirmed incidents. The poor
held-out PortScan recall remains a limitation; random-split development accuracy
does not establish generalization. No production alerting database or automatic
remediation layer is claimed.
