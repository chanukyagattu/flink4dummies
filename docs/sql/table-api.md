---
title: Table API and SQL
sidebar_label: Table API & SQL
description: Dynamic tables, changelog streams, retractions — the mental model that makes streaming SQL make sense.
---

# Table API and SQL

<PageMeta level="intermediate" time="12 min" prereq={[['Windows', '/docs/flink/windows/why-windows'], ['State', '/docs/flink/state/why-state']]} />

<Objectives>

- Explain the dynamic table / changelog duality in one sentence
- Predict whether a given query produces an append, upsert or retract stream
- Choose between SQL and DataStream on capability rather than familiarity

</Objectives>

## The one idea

Streaming SQL only makes sense once you accept this:

<Callout type="key">

**A stream and a table are the same thing viewed differently.**

- A **table** is the current state — the result of applying every change so far.
- A **stream** is the sequence of changes that produced it.

Flink calls this a **dynamic table**: a table whose contents change over time, and
whose changes are themselves a stream (the **changelog**).

`SELECT` over a dynamic table produces another dynamic table, whose changelog is
what your sink receives.

</Callout>

```text
STREAM (changelog)                 TABLE (materialised)

+I (alice, 1)                      alice → 1
+I (bob,   1)                      alice → 1, bob → 1
-U (alice, 1)                      (retract the old row)
+U (alice, 2)                      alice → 2, bob → 1
```

Those `+I`, `-U`, `+U`, `-D` markers are **row kinds**, and they are the thing that
makes streaming SQL behave differently from batch SQL. Everything surprising about
Flink SQL traces back to them.

## Three kinds of result

Which one your query produces determines what your sink must support.

<Compare>
  <CompareCard title="Append-only" rows={[
    ['Row kinds', '+I only'],
    ['Produced by', 'Filters, projections, windowed aggregations, interval joins'],
    ['Sink needs', 'Nothing special — any append sink works'],
    ['Example', 'SELECT * FROM orders WHERE amount > 0'],
  ]} />
  <CompareCard title="Upsert" rows={[
    ['Row kinds', '+I, +U, -D on a primary key'],
    ['Produced by', 'GROUP BY with a key the sink can match on'],
    ['Sink needs', 'A primary key and upsert support'],
    ['Example', 'SELECT page, COUNT(*) FROM clicks GROUP BY page'],
  ]} />
  <CompareCard title="Retract" rows={[
    ['Row kinds', '+I, -U, +U, -D'],
    ['Produced by', 'Non-windowed aggregation without a key, outer joins, ranks'],
    ['Sink needs', 'To handle explicit retractions'],
    ['Example', 'A LEFT JOIN that later finds its match'],
  ]} />
</Compare>

<Callout type="mistake" title="The number-one Flink SQL surprise">

```sql
-- unbounded aggregation: emits a NEW row every time the count changes
SELECT page, COUNT(*) FROM clicks GROUP BY page;
```

Into an append-only sink (a plain Kafka topic, an S3 file sink) this writes:

```text
(home, 1) (home, 2) (home, 3) (home, 4) …
```

Every intermediate value, forever. The consumer sees "duplicates" that are not
duplicates at all — they are the changelog.

Two correct fixes:

1. **Use an upsert sink** with a primary key, so later values replace earlier ones:
   `'connector' = 'upsert-kafka'`, or a JDBC sink with a `PRIMARY KEY`.
2. **Use a windowed aggregation**, which is append-only because each window emits
   exactly one final row.

</Callout>

## Getting started

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment t = StreamTableEnvironment.create(env);

t.executeSql("""
    CREATE TABLE clicks (
      user_id    STRING,
      page       STRING,
      event_time TIMESTAMP_LTZ(3),
      WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'clicks',
      'properties.bootstrap.servers' = 'kafka:9092',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
    )
    """);

t.executeSql("""
    CREATE TABLE page_counts (
      window_start TIMESTAMP(3),
      page         STRING,
      cnt          BIGINT,
      PRIMARY KEY (window_start, page) NOT ENFORCED
    ) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'page-counts',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'json',
      'value.format' = 'json'
    )
    """);

t.executeSql("""
    INSERT INTO page_counts
    SELECT window_start, page, COUNT(*)
    FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, page
    """);
```

Note the `WATERMARK FOR` clause. That single line is the SQL equivalent of an entire
`WatermarkStrategy` — and everything from
[Level 3](/docs/flink/watermarks/what-is-a-watermark) still applies underneath it.

## Windowing table-valued functions

The modern syntax. `GROUP BY TUMBLE(...)` is the legacy form; prefer these.

```sql
-- TUMBLE: fixed, non-overlapping
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))

-- HOP: sliding. State multiplier = size / slide, exactly as in DataStream.
FROM TABLE(HOP(TABLE clicks, DESCRIPTOR(event_time),
                INTERVAL '20' SECOND, INTERVAL '1' MINUTE))

-- CUMULATE: running total within a period, emitted at intervals.
-- "Revenue so far today, updated hourly." No DataStream equivalent.
FROM TABLE(CUMULATE(TABLE orders, DESCRIPTOR(event_time),
                     INTERVAL '1' HOUR, INTERVAL '1' DAY))

-- SESSION
FROM TABLE(SESSION(TABLE clicks PARTITION BY user_id,
                    DESCRIPTOR(event_time), INTERVAL '30' MINUTE))
```

<Callout type="prod" title="CUMULATE is the underrated one">

Cumulative windows solve a requirement that comes up constantly and is awkward
everywhere else: *"the running total for the current day, refreshed every hour"*.

In DataStream you would build it with a daily window and an early-firing trigger, and
then deal with multiple emissions per window. `CUMULATE` gives you one clean row per
step, append-only, with the state bounded to one day.

</Callout>

## The settings that matter

```sql
-- 1. State retention for unbounded operations. Without this, regular joins and
--    unbounded aggregations retain state FOREVER.
SET 'table.exec.state.ttl' = '36h';

-- 2. Mini-batch: one state read-modify-write per batch instead of per record.
--    Frequently a large throughput win on aggregations.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';

-- 3. Two-phase aggregation — the SQL equivalent of manual salting.
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';

-- 4. Emit early / late results for long windows
SET 'table.exec.emit.early-fire.enabled' = 'true';
SET 'table.exec.emit.early-fire.delay' = '10s';
SET 'table.exec.emit.late-fire.enabled' = 'true';
```

<Callout type="mistake">

Forgetting `table.exec.state.ttl` on a job containing a regular join or an unbounded
`GROUP BY`. There is no default bound. The job runs beautifully for weeks and then
dies with state nobody was watching.

Set it, and be explicit about what it means: rows older than the TTL will silently
stop matching. That is a correctness trade you are making deliberately.

</Callout>

## Mixing SQL and DataStream

They interoperate cleanly, and the best pipelines use both.

```java
// DataStream → Table
Table clicks = t.fromDataStream(clickStream,
    Schema.newBuilder()
        .column("userId", DataTypes.STRING())
        .column("page", DataTypes.STRING())
        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("event_time", "SOURCE_WATERMARK()")
        .build());

t.createTemporaryView("clicks", clicks);

// do the relational work in SQL
Table counts = t.sqlQuery("""
    SELECT window_start, page, COUNT(*) AS cnt
    FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, page
    """);

// Table → DataStream, for the custom logic SQL cannot express
DataStream<Row> out = t.toDataStream(counts);          // append-only results
DataStream<Row> changelog = t.toChangelogStream(counts); // if it retracts

changelog.keyBy(r -> r.getFieldAs("page"))
         .process(new MyCustomAlerting());
```

`SOURCE_WATERMARK()` propagates the DataStream's existing watermark strategy into the
Table API rather than declaring a second one — which is what you want, and what people
usually get wrong.

## When to use which

| Task | Use |
| --- | --- |
| Filter, project, aggregate, window | **SQL** — shorter, optimised, less to get wrong |
| Temporal join against a versioned table | **SQL** — `FOR SYSTEM_TIME AS OF` has no clean DataStream equivalent |
| Lookup join to an external system | **SQL** — async lookup is built in |
| Top-N, deduplication | **SQL** — see [streaming SQL patterns](/docs/flink/sql/streaming-patterns) |
| Cumulative windows | **SQL** — no DataStream equivalent |
| Per-key state machine with timers | **DataStream** |
| Dynamic rules via broadcast state | **DataStream** |
| Custom sink protocols | **DataStream** |
| Anything where you must control state layout precisely | **DataStream** |

<Callout type="prod" title="A good default architecture">

Do the relational 80% in SQL, convert to a `DataStream` for the awkward 20%, and
convert back if you need to sink relationally.

You get the optimiser, less code, and fewer state-layout mistakes on the common path —
while keeping full control exactly where you need it.

</Callout>

<Expert>

**The optimiser is real.** Flink SQL runs a cost-based optimiser (Calcite) that pushes
filters into sources, prunes projections, reorders joins, and chooses one-phase or
two-phase aggregation. `EXPLAIN PLAN FOR` shows you the result, and reading it is the
fastest way to understand why a query is slow.

**Changelog normalisation costs state.** Reading an `upsert-kafka` source produces a
changelog whose completeness Flink must reconstruct, which requires holding the
previous value per key — a `ChangelogNormalize` operator with unbounded state unless a
TTL is set. It appears in the plan and surprises people who thought they had a
stateless read.

**`NOT ENFORCED` primary keys.** Flink does not validate them. Declaring a primary key
that is not actually unique produces silently wrong upsert results, because rows will
overwrite each other.

**Non-determinism is checked.** Flink 1.16+ detects non-deterministic updates in
changelog pipelines (for example, joining on a column that can change) and fails the
plan rather than producing corrupt results. If you hit that error, the query genuinely
is ambiguous — read the message rather than working around it.

**SQL Gateway.** Flink ships a REST-based SQL Gateway with HiveServer2 compatibility,
which is how you give analysts a JDBC endpoint onto streaming tables without them
touching Java.

</Expert>

<Callout type="remember">

A dynamic table and its changelog are the same thing. Whether your query is append,
upsert or retract determines what the sink must support. Set `table.exec.state.ttl` on
anything unbounded. Use SQL for the relational 80% and DataStream for the rest.

</Callout>

## Next

**[Streaming SQL patterns](/docs/flink/sql/streaming-patterns)** — top-N, deduplication, CDC.
