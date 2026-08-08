---
title: Timestamp assignment
sidebar_label: Timestamp assignment
description: Getting event time out of your records and into Flink — where to do it, how to do it, and the traps.
---

# Timestamp assignment

<PageMeta level="intermediate" time="7 min" prereq={[['Out-of-order and late', '/docs/flink/time/out-of-order-and-late']]} />

<Objectives>

- Attach event time to records with a `WatermarkStrategy`
- Explain why assigning at the source beats assigning downstream
- Handle records with missing, bogus, or future timestamps

</Objectives>

## What is it?

Flink does not know which field of your object is "the time". You have to tell
it. That is timestamp assignment, and it comes bundled with watermark generation
in a single object: `WatermarkStrategy`.

```java
WatermarkStrategy<Click> strategy = WatermarkStrategy
    .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))   // watermarks
    .withTimestampAssigner((click, recordTimestamp) -> click.eventTime());  // timestamps
```

Two responsibilities in one object:

- **Timestamp assigner** — "the event time of this record is `click.eventTime()`"
- **Watermark generator** — "given the timestamps I have seen, event time has progressed to *here*"

The second argument to the assigner, `recordTimestamp`, is any timestamp the
source already attached — for Kafka, the broker's record timestamp. Useful as a
fallback.

## Where to attach it

**At the source. Almost always at the source.**

```java
// ✅ Preferred — inside fromSource
DataStream<Click> clicks = env.fromSource(kafkaSource, strategy, "clicks");

// ⚠️ Works, but loses per-split watermarking
DataStream<Click> clicks = env
    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "clicks")
    .map(this::parse)
    .assignTimestampsAndWatermarks(strategy);
```

The difference matters more than it looks.

<Callout type="key">

A Kafka source subtask may read **several partitions**. Each partition is
individually ordered, but their merge is not.

Passing the strategy to `fromSource` gives you **per-split watermark generation**:
Flink tracks a separate watermark per partition and emits the *minimum*. Assigning
downstream instead gives you one generator over the already-merged stream — so a
partition that is 30 seconds ahead drags the watermark forward and makes the other
partitions' records look late.

Same code, same bound, materially different correctness.

</Callout>

Legitimate reasons to assign downstream:

- The source emits raw bytes and the timestamp is only readable after parsing
- You are reading from a source that has no watermark support

If you must, do it as early as possible, and immediately after the parse.

## The built-in strategies

```java
// 1. Bounded out-of-orderness — the one you will use 95% of the time
WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                 .withTimestampAssigner((e, ts) -> e.eventTime());

// 2. Perfectly ordered input — a strong claim; be sure
WatermarkStrategy.<T>forMonotonousTimestamps()
                 .withTimestampAssigner((e, ts) -> e.eventTime());

// 3. No event time at all — processing-time-only jobs
WatermarkStrategy.noWatermarks();

// 4. Custom
WatermarkStrategy.forGenerator(ctx -> new MyGenerator());
```

`forMonotonousTimestamps` is equivalent to a zero bound. It is correct for a
single Kafka partition written by a single well-behaved producer, and wrong the
moment you have two partitions. Use it only when you can defend it.

## The two modifiers you should nearly always add

```java
WatermarkStrategy
    .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((e, ts) -> e.eventTime())
    .withIdleness(Duration.ofMinutes(1))       // ← add this
    .withWatermarkAlignment(                    // ← and often this
        "orders-group", Duration.ofSeconds(30), Duration.ofSeconds(1));
```

- **`withIdleness`** — if a partition produces nothing for a minute, stop letting it hold the global watermark back. Without this, one quiet partition freezes every window in the job. This is the single most common cause of "my job runs but emits nothing".
- **`withWatermarkAlignment`** — stop any one source from racing more than 30 seconds ahead of the others. Critical when replaying history, where one partition can be hours ahead and cause everything else to be classified late.

Both are covered properly in
[watermark propagation and idleness](/docs/flink/watermarks/propagation-and-idleness).

## Handling bad timestamps

Real data has missing, zero, and future timestamps. Decide before it bites you.

```java
.withTimestampAssigner((click, recordTimestamp) -> {
    long t = click.eventTime();

    // Missing → fall back to the broker's timestamp, then to now.
    if (t <= 0) {
        t = recordTimestamp > 0 ? recordTimestamp : System.currentTimeMillis();
    }

    // Producer clock is in the future → clamp. An event stamped 2049 would
    // push the watermark decades forward and make EVERY other record late.
    long now = System.currentTimeMillis();
    if (t > now + Duration.ofMinutes(5).toMillis()) {
        t = now;
    }
    return t;
})
```

<Callout type="mistake" title="The single-record time bomb">

One malformed record with `eventTime = 4102444800000` (year 2100) will:

1. push the watermark to the year 2100,
2. immediately fire and purge every open window,
3. classify every subsequent normal record as late and drop it,
4. and keep doing so **forever**, because the watermark never goes backwards.

Your job stays green, throughput looks fine, and output silently stops being
correct. Clamping future timestamps is a five-line defence against a very bad
day.

</Callout>

<Callout type="prod" title="A timestamp health check worth shipping">

Emit these as metrics from the assigner and put them on a dashboard:

- `eventTimeLag = processingTime - eventTime`, as p50/p95/p99 — this is what your bound should be tuned to
- count of records with missing timestamps
- count of records clamped as future-dated

The first tells you whether your watermark bound is still right. The other two
tell you when an upstream producer changed before your results do.

</Callout>

<Callout type="hood">

`WatermarkStrategy` is a factory for two things:

```java
TimestampAssigner<T> createTimestampAssigner(Context ctx);
WatermarkGenerator<T> createWatermarkGenerator(Context ctx);
```

`WatermarkGenerator` has two methods:

- `onEvent(event, timestamp, output)` — called for every record. `BoundedOutOfOrdernessWatermarks` just records `maxTimestamp` here; it does **not** emit.
- `onPeriodicEmit(output)` — called on a timer, every `pipeline.auto-watermark-interval` (default **200ms**). This is where the watermark is actually emitted, as `maxTimestamp - outOfOrderness - 1`.

That `-1` is not cosmetic: a watermark of value *W* asserts "no more events with
timestamp **≤ W**", so to still accept an event exactly at `maxTimestamp - bound`
the watermark must sit one millisecond below it.

Two consequences: watermarks are emitted on a **timer, not per record**, so with
the default interval your event-time progress is quantised to 200ms; and a
completely idle source emits nothing at all unless you configure idleness.

</Callout>

<Expert>

**Punctuated watermarks.** If your stream contains explicit end-of-period markers
(some CDC and financial feeds do), emit the watermark from `onEvent` and leave
`onPeriodicEmit` empty. This gives exact, immediate event-time progress with no
heuristic bound at all — the ideal case, when your data supports it.

**Table API / SQL.** The equivalent is declared in DDL:

```sql
CREATE TABLE clicks (
  user_id STRING,
  page STRING,
  event_time TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH ('connector' = 'kafka', ...);
```

Same semantics, same trade-offs, less code. `table.exec.source.idle-timeout` is
the SQL equivalent of `withIdleness`.

**Timestamps in nanoseconds or seconds.** Flink event time is **milliseconds
since the Unix epoch**. Feeding it seconds puts your events in 1970; feeding it
microseconds puts them in the year 55000 — with the consequences described above.
Convert explicitly and assert the range in tests.

</Expert>

<Callout type="remember">

Assign at the source so each partition gets its own watermark. Always add
`withIdleness`. Clamp future timestamps — one bad record can silently kill event
time for the lifetime of the job.

</Callout>

## Next

**[Level 3 — what is a watermark?](/docs/flink/watermarks/what-is-a-watermark)** — the mechanism behind all of this.
