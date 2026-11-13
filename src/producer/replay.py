"""Bounded CSV -> Kafka replay, reporting delivery acknowledgements."""
import argparse
from itertools import islice
import json
import math
import time
import uuid
from src.producer.events import csv_events, encode_event
from src.producer.kafka_config import producer_config, positive, require_topic


def publish(client, events, topic, *, max_events=1000, rate=100., timeout=35.):
    if type(max_events) is not int or max_events <= 0:
        raise ValueError('max_events must be a positive integer')
    positive(timeout, 'timeout')
    if not math.isfinite(rate) or rate < 0:
        raise ValueError('rate must be finite and nonnegative')
    stats = dict(attempted=0, enqueued=0, delivered=0, delivery_failed=0,
                 unconfirmed=0, errors=[], interrupted=False)
    def error(message):
        if len(stats['errors']) < 5:
            stats['errors'].append(str(message))
    def delivery(err, msg):
        if err is None:
            stats['delivered'] += 1
        else:
            stats['delivery_failed'] += 1
            error(err)
    try:
        require_topic(client, topic, min(timeout, 10.))
        start = time.monotonic()
        for event in islice(events, max_events):
            stats['attempted'] += 1
            value = encode_event(event)
            deadline = time.monotonic() + timeout
            while True:
                try:
                    client.produce(topic, key=event['event_id'].encode(), value=value,
                                   on_delivery=delivery)
                    stats['enqueued'] += 1
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Producer queue remained full')
                    client.poll(.1)
            client.poll(0)
            if rate:
                time.sleep(max(0., stats['enqueued']/rate - (time.monotonic()-start)))
    except KeyboardInterrupt:
        stats['interrupted'] = True
    except Exception as exc:
        error(exc)
    finally:
        try:
            remaining = client.flush(timeout)
            if remaining:
                error(f'{remaining} events still queued after flush timeout')
        except Exception as exc:
            error(exc)
        stats['unconfirmed'] = stats['enqueued']-stats['delivered']-stats['delivery_failed']
    stats['success'] = not (stats['errors'] or stats['interrupted'] or
                            stats['delivery_failed'] or stats['unconfirmed'])
    return stats


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True)
    p.add_argument('--brokers', default='localhost:9092')
    p.add_argument('--topic', default='ids.network-flows.v1')
    p.add_argument('--max-events', type=int, default=1000)
    p.add_argument('--rate', type=float, default=100., help='Events/sec; 0 is unthrottled')
    p.add_argument('--timeout', type=float, default=35.)
    args = p.parse_args()
    replay_id = str(uuid.uuid4())
    try:
        config = producer_config(args.brokers, args.topic)
        # Validate limits before importing or connecting a client.
        if args.max_events <= 0 or not math.isfinite(args.rate) or args.rate < 0:
            raise ValueError('Invalid event limit/rate')
        positive(args.timeout, 'timeout')
        from confluent_kafka import Producer
        stats = publish(Producer(config), csv_events(args.source, replay_id), args.topic,
                        max_events=args.max_events, rate=args.rate, timeout=args.timeout)
    except Exception as exc:
        stats = {'success': False, 'errors': [str(exc)]}
    stats['replay_id'] = replay_id
    print(json.dumps(stats, allow_nan=False))
    return 0 if stats['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
