"""Bounded development client settings; Kafka imported only at runtime."""
import math
import re


def check_endpoint(brokers, topic):
    if not isinstance(brokers, str) or not brokers.strip():
        raise ValueError('Brokers required')
    if not isinstance(topic, str) or not re.fullmatch(r'[a-zA-Z0-9_.-]{1,249}', topic) or topic in {'.', '..'}:
        raise ValueError('Invalid Kafka topic')


def positive(value, name):
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')


def producer_config(brokers, topic):
    check_endpoint(brokers, topic)
    return {'bootstrap.servers': brokers, 'client.id': 'ids-csv-replay',
            'enable.idempotence': True, 'acks': 'all',
            'delivery.timeout.ms': 30000, 'request.timeout.ms': 10000,
            'queue.buffering.max.messages': 1000, 'queue.buffering.max.kbytes': 16384,
            'linger.ms': 5, 'allow.auto.create.topics': False}


def consumer_config(brokers, topic, group):
    check_endpoint(brokers, topic)
    if not isinstance(group, str) or not group.strip():
        raise ValueError('Consumer group required')
    return {'bootstrap.servers': brokers, 'group.id': group,
            'client.id': 'ids-transport-verifier', 'enable.auto.commit': False,
            'auto.offset.reset': 'earliest', 'allow.auto.create.topics': False}


def require_topic(client, topic, timeout):
    metadata = client.list_topics(topic=topic, timeout=timeout)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise RuntimeError(f'Topic unavailable: {topic}')
