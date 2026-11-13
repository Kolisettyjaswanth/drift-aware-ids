# Tests

Run `python -m unittest discover -s tests -v` from repository root.
Tests use unittest (no added test dependency). A CIC-IDS2017 smoke test skips
explicitly when reference and prediction files are unavailable.
Run `python -m src.drift.validate_synthetic` separately for 600 controlled windows.
See docs/drift_validation.md for acceptance criteria and actual evidence.
