"""Read-only bounded compatibility check for the existing replay and model contract."""
import argparse
import csv
import json
from pathlib import Path

from src.producer.events import csv_events


def preflight(source, features_path, *, max_rows=1000):
    if type(max_rows) is not int or not 1 <= max_rows <= 100000:
        raise ValueError("max_rows must be between 1 and 100000")
    source = Path(source)
    features_path = Path(features_path)
    features = json.loads(features_path.read_text(encoding="utf-8"))
    if (not isinstance(features, list) or not features or
            any(not isinstance(f, str) or not f for f in features) or
            len(set(features)) != len(features)):
        raise ValueError("Invalid model feature manifest")
    with source.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        try:
            headers = [h.strip() for h in next(reader)]
        except StopIteration as exc:
            raise ValueError("Empty CSV") from exc
    if not headers or any(not h for h in headers) or len(set(headers)) != len(headers):
        raise ValueError("Empty or duplicate CSV headers")
    if "Target" in headers or "Label" not in headers:
        raise ValueError("Replay requires Label and rejects Target")
    missing = [name for name in features if name not in headers]
    if missing:
        raise ValueError(f"Missing required model features: {missing}")
    checked = 0
    invalid_rows = 0
    invalid_fields = {}
    invalid_source_rows = []
    for event in csv_events(source, "preflight"):
        if checked == max_rows:
            raise ValueError(f"Input exceeds max_rows={max_rows}; use a bounded CSV")
        checked += 1
        invalid = [name for name in features if event["features"][name] is None]
        if invalid:
            invalid_rows += 1
            invalid_source_rows.append(checked - 1)
            for name in invalid:
                invalid_fields[name] = invalid_fields.get(name, 0) + 1
    if checked == 0:
        raise ValueError("CSV has no data rows")
    if invalid_rows == checked:
        raise ValueError("No records have all required numeric model features")
    return {
        "status": "compatible",
        "source": str(source),
        "rows": checked,
        "valid_rows": checked - invalid_rows,
        "invalid_rows": invalid_rows,
        "invalid_source_rows": invalid_source_rows,
        "invalid_fields": invalid_fields,
        "required_features": len(features),
        "extra_columns": [name for name in headers if name not in features and name not in {"Label", "SourceFile"}],
        "note": "Schema and numeric compatibility only; not a model performance or attack validation",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--features", default="models/baseline_features.json")
    parser.add_argument("--max-rows", type=int, default=1000)
    args = parser.parse_args()
    try:
        result = preflight(args.source, args.features, max_rows=args.max_rows)
    except (OSError, ValueError, csv.Error, UnicodeError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "rejected", "reason": str(exc)}))
        raise SystemExit(1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
