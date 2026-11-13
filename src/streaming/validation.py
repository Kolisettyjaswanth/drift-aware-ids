"""Pure-Python schema-v1 validation and bounded acceptance accounting."""
import json
from pathlib import Path
from src.producer.events import decode_event
from src.producer.kafka_config import check_endpoint

SPARK_VERSION = '3.5.7'
CONNECTOR = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7'


def source_options(brokers, topic, batch_size):
    check_endpoint(brokers, topic)
    if type(batch_size) is not int or not 1 <= batch_size <= 1000:
        raise ValueError('batch_size must be between 1 and 1000')
    return {'kafka.bootstrap.servers': brokers, 'subscribe': topic,
            'startingOffsets': 'earliest', 'failOnDataLoss': 'true',
            'maxOffsetsPerTrigger': str(batch_size),
            'kafka.allow.auto.create.topics': 'false',
            'kafka.request.timeout.ms': '10000',
            'kafka.default.api.timeout.ms': '15000',
            'fetchOffset.numRetries': '1', 'kafkaConsumer.pollTimeoutMs': '10000'}


def validate_record(row):
    """Keep validated event values unchanged; never coerce features to Spark doubles."""
    result = {'kafka': {'topic': row['topic'], 'partition': row['partition'],
                        'offset': row['offset']}}
    try:
        event = decode_event(row['value'])
        key = row['key']
        if key is None or bytes(key).decode('utf-8') != event['event_id']:
            raise ValueError('Kafka key/event identity mismatch')
        result.update(valid=True, event=event)
    except (ValueError, TypeError, UnicodeError) as exc:
        result.update(valid=False, reason=str(exc)[:300])
    return result


def summarize(records, replay_id, expected_count, topic, max_records=1000):
    if not isinstance(replay_id, str) or not replay_id.strip():
        raise ValueError('Expected replay_id required')
    if type(expected_count) is not int or not 1 <= expected_count <= max_records <= 10000:
        raise ValueError('Expected count must be within a safety cap <=10000')
    stats = dict(consumed=0, valid=0, invalid=0, unrelated=0,
                 duplicates=0, repeated_offsets=0, unexpected_rows=0)
    rows, offsets = set(), set()
    for record in records:
        stats['consumed'] += 1
        if stats['consumed'] > max_records:
            raise ValueError('Input safety cap exceeded; accounting incomplete')
        coordinates = record['kafka']
        position = (coordinates['topic'], coordinates['partition'], coordinates['offset'])
        if position in offsets:
            stats['repeated_offsets'] += 1
        offsets.add(position)
        if coordinates['topic'] != topic:
            stats['unrelated'] += 1
        if not record['valid']:
            stats['invalid'] += 1
            continue
        event = record['event']
        stats['valid'] += 1
        if event['replay_id'] != replay_id:
            stats['unrelated'] += 1
            continue
        index = event['source_row']
        if not 0 <= index < expected_count:
            stats['unexpected_rows'] += 1
        if index in rows:
            stats['duplicates'] += 1
        rows.add(index)
    stats['unique_expected'] = len(rows.intersection(range(expected_count)))
    stats['missing_expected'] = expected_count-stats['unique_expected']
    stats['success'] = (stats['consumed'] == expected_count and
                        stats['valid'] == expected_count and
                        not any(stats[k] for k in ('invalid','unrelated','duplicates',
                                                  'repeated_offsets','unexpected_rows',
                                                  'missing_expected')))
    return stats


def read_records(run_dir):
    for path in sorted((Path(run_dir)/'batches').glob('batch-*.jsonl')):
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                yield json.loads(line)
