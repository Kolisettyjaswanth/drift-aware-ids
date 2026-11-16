# Scripts

Project utilities and repeatable research commands.

- `python -m scripts.preflight_replay --source PATH`: read-only bounded CSV
  compatibility audit; no Kafka/Spark connection.
- `python -m scripts.run_analysis --source PATH`: creates a fresh Kafka topic,
  publishes a bounded snapshot, runs existing Spark inference/optional drift,
  and saves verified evidence. Requires authorization for a real integration run.

See [Increment 7](../docs/increment7_reusable_analysis.md) for limits, arguments,
count semantics, local checks, and the two verified real acceptance runs.
