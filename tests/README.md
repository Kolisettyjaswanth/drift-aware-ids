# Tests

Run `python -m unittest discover -s tests -v` from repository root.
Tests use unittest (no added test dependency). A CIC-IDS2017 smoke test skips
explicitly when reference and prediction files are unavailable.
Run `python -m src.drift.validate_synthetic` separately for 600 controlled windows.
See docs/drift_validation.md for acceptance criteria and actual evidence.

Kafka transport unit tests use fake clients and do not need a broker or the
optional client package. `test_kafka_integration.py` is skipped unless explicitly
enabled with IDS_KAFKA_INTEGRATION=1; it expects an already-running broker and
requirements-kafka.txt installed. See docs/kafka_transport.md.
