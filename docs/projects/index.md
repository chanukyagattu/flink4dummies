---
title: Hands-on projects
sidebar_label: Overview
description: Five progressively harder projects that force you to use every concept in the guide.
---

# Hands-on projects

<PageMeta level="intermediate" time="4 min" />

Reading about watermarks teaches you what they are. Watching a job stall because of an
idle partition teaches you what they *do*.

These five projects are ordered so that each one forces you to confront a concept you
could otherwise skim past. Each ends with a **break it on purpose** section — that is
the part that does the teaching.

<CardGrid>
  <Card to="/docs/flink/projects/clickstream" level="🟢 Project 1" title="Clickstream analytics">
    Event time, watermarks, tumbling windows, incremental aggregation. The smallest
    genuinely representative job.
  </Card>
  <Card to="/docs/flink/projects/sessionization" level="🟡 Project 2" title="Sessionization">
    Keyed state, event-time timers, coalescing, precise cleanup. Written by hand rather
    than with session windows, so you can see the machinery.
  </Card>
  <Card to="/docs/flink/projects/fraud-detection" level="🟡 Project 3" title="Fraud detection">
    A per-key state machine with timers, side outputs, and the state-leak discipline
    that keeps it alive for months.
  </Card>
  <Card to="/docs/flink/projects/dynamic-rules" level="🔴 Project 4" title="Dynamic rules engine">
    Broadcast state, the bootstrap problem, and changing business logic without a
    redeploy.
  </Card>
  <Card to="/docs/flink/projects/exactly-once-pipeline" level="🔴 Project 5" title="Exactly-once pipeline">
    Kafka to Kafka with two-phase commit, then deliberately crash it and prove no
    duplicates appeared.
  </Card>
</CardGrid>

## Setup

All five use the same local stack. Two containers plus Flink is enough.

```yaml title="docker-compose.yml"
services:
  kafka:
    image: apache/kafka:3.9.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

  jobmanager:
    image: flink:2.3
    ports: ["8081:8081"]
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        state.backend.type: rocksdb
        state.backend.incremental: true
        execution.checkpointing.interval: 10s
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        state.savepoints.dir: file:///tmp/flink-savepoints
        execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

  taskmanager:
    image: flink:2.3
    depends_on: [jobmanager]
    command: taskmanager
    scale: 2                      # two, so you can kill one
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
```

```bash
docker compose up -d
open http://localhost:8081

# create the topics used across the projects, with several partitions
# so you can make one of them go quiet on purpose
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic clicks --partitions 4
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic transactions --partitions 4
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic rules --partitions 1 \
  --config cleanup.policy=compact
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic results --partitions 4
```

<Callout type="prod" title="Two TaskManagers is the important bit">

With one TaskManager you cannot meaningfully test recovery — killing it kills the job.
With two, you can `docker compose kill` one and watch the job fail over, restore state,
rewind offsets, and reproduce identical output.

That five-second experiment is worth more than any chapter in this guide.

</Callout>

## The event generator

Every project uses the same producer. Give it deliberately imperfect data — that is
the point.

```java title="Generator.java"
public class Generator {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Random rnd = new Random(42);            // seeded: reproducible runs
            String[] pages = {"/", "/search", "/product", "/cart", "/checkout"};
            long now = System.currentTimeMillis();

            for (int i = 0; i < 1_000_000; i++) {
                String user = "u" + rnd.nextInt(1_000);

                // deliberately imperfect: most events are slightly out of order,
                // and 1 in 500 is VERY late. This is what real data looks like.
                long lateness = rnd.nextInt(100) < 99
                    ? rnd.nextInt(5_000)            // up to 5s out of order
                    : rnd.nextInt(120_000);         // occasionally 2 minutes
                long eventTime = now + i * 10L - lateness;

                String json = """
                    {"userId":"%s","page":"%s","eventTime":%d}
                    """.formatted(user, pages[rnd.nextInt(pages.length)], eventTime);

                producer.send(new ProducerRecord<>("clicks", user, json));
                if (i % 1000 == 0) Thread.sleep(10);
            }
        }
    }
}
```

<Callout type="try" title="Before you start Project 1">

Run the generator and, in another terminal, consume the topic raw:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clicks --from-beginning | head -50
```

Look at the `eventTime` values. They do not increase monotonically. Some go
*backwards* by two minutes.

Every design decision in the next five projects exists because of what you are looking
at.

</Callout>

## Next

**[Project 1 — clickstream analytics](/docs/flink/projects/clickstream)**
