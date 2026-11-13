# Development Kafka

`compose.kafka.yml` defines one pinned KRaft broker, internal/host listeners,
health check, resource limits and persistent volume. No ZooKeeper or Spark.
See [Kafka transport](../docs/kafka_transport.md) for startup/shutdown, topic
creation, event schema and pending user validation. No Docker execution has
been performed by Work for this milestone.
