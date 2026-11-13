"""Read and validate transport events; no predictions or drift calculations."""
import argparse
import json
import time
from src.producer.events import decode_event
from src.producer.kafka_config import consumer_config, positive, require_topic


def consume(client, topic, *, max_events=1000, timeout=60., replay_id=None):
    if type(max_events) is not int or max_events <= 0:
        raise ValueError('max_events must be positive integer')
    positive(timeout, 'timeout')
    stats = dict(received=0, valid=0, malformed=0, matched=0, duplicates=0,
                 errors=[], interrupted=False)
    seen = set() # bounded by max_events; no whole-stream accumulation
    try:
        require_topic(client, topic, min(timeout, 10.))
        client.subscribe([topic])
        deadline = time.monotonic()+timeout
        while stats['received'] < max_events and time.monotonic() < deadline:
            msg = client.poll(min(1., max(0., deadline-time.monotonic())))
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            stats['received'] += 1
            try:
                event = decode_event(msg.value())
            except ValueError:
                stats['malformed'] += 1
                continue # do not commit malformed input
            stats['valid'] += 1
            if replay_id is None or event['replay_id'] == replay_id:
                stats['matched'] += 1
                if event['event_id'] in seen:
                    stats['duplicates'] += 1
                seen.add(event['event_id'])
        # This verifier never commits: fresh groups replay retained records.
    except KeyboardInterrupt:
        stats['interrupted'] = True
    except Exception as exc:
        stats['errors'].append(str(exc))
    finally:
        try:
            client.close()
        except Exception as exc:
            stats['errors'].append(str(exc))
    stats['unique_matched'] = len(seen)
    stats['success'] = (stats['received'] == max_events and not stats['malformed'] and
                        not stats['errors'] and not stats['interrupted'] and
                        stats['unique_matched'] == max_events)
    return stats


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--brokers', default='localhost:9092')
    p.add_argument('--topic', default='ids.network-flows.v1')
    p.add_argument('--group', required=True)
    p.add_argument('--max-events', type=int, default=1000)
    p.add_argument('--timeout', type=float, default=60.)
    p.add_argument('--replay-id', help='Require every bounded record to match this replay')
    args = p.parse_args()
    try:
        config = consumer_config(args.brokers, args.topic, args.group)
        if args.max_events <= 0:
            raise ValueError('Invalid event limit')
        positive(args.timeout, 'timeout')
        from confluent_kafka import Consumer
        stats = consume(Consumer(config), args.topic, max_events=args.max_events,
                        timeout=args.timeout, replay_id=args.replay_id)
    except Exception as exc:
        stats = {'success': False, 'errors': [str(exc)]}
    print(json.dumps(stats, allow_nan=False))
    return 0 if stats['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
