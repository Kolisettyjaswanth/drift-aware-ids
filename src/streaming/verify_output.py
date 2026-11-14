"""Compare Spark-validated events with the first N input CSV records."""
import argparse
import json
from pathlib import Path
from itertools import islice
from src.producer.events import csv_events, validate_event
from src.streaming.validation import read_records, summarize


def verify(run_dir, source):
    root = Path(run_dir)
    config = json.loads((root/'config.json').read_text())
    summary = json.loads((root/'summary.json').read_text())
    if not summary['success'] or not summary['accounting_complete']:
        raise ValueError('Spark acceptance did not finish successfully')
    records = list(read_records(root)) # acceptance cap <=10000, not entire source CSV
    stats = summarize(records, config['replay_id'],config['expected_count'],
                      config['topic'],config['max_records'])
    if not stats['success']:
        raise ValueError('Stored records fail acceptance accounting')
    actual = {r['event']['source_row']: validate_event(r['event']) for r in records}
    count = 0
    for expected in islice(csv_events(source,config['replay_id']),config['expected_count']):
        received = dict(actual[expected['source_row']])
        # Replay time is preserved in output, but cannot be recreated from CSV.
        received.pop('replayed_at')
        expected.pop('replayed_at')
        if received != expected:
            raise ValueError(f'Feature/metadata mismatch at row {expected["source_row"]}')
        count += 1
    if count != config['expected_count']:
        raise ValueError('CSV has too few source records')
    return {'success':True,'verified_source_rows':count,
            'comparison':'all schema-v1 fields except unreconstructable replayed_at'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir',required=True)
    p.add_argument('--source',required=True)
    args=p.parse_args()
    try:
        result=verify(args.run_dir,args.source)
    except Exception as exc:
        result={'success':False,'errors':[str(exc)]}
    print(json.dumps(result))
    return 0 if result['success'] else 1


if __name__=='__main__':
    raise SystemExit(main())
