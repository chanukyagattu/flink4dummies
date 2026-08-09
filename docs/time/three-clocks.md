---
title: The three clocks
sidebar_label: The three clocks
description: Event time, processing time, ingestion time — why they disagree, and why choosing wrongly makes your results unreproducible.
---

# The three clocks

<PageMeta level="intermediate" time="10 min" prereq={[['Your first job', '/docs/flink/basics/first-job']]} docs="docs/concepts/timely-stream-processing/" />

<Objectives>

- Say what each of the three clocks measures and who controls it
- Explain why processing time makes results non-reproducible
- Choose the right clock for a given requirement, and justify it

</Objectives>

## The problem in one picture

One event. Three different "times". They can be minutes or hours apart.

```text
 12:03:15          12:07:42          12:07:44
    │                 │                 │
    ▼                 ▼                 ▼
 user taps        record lands     Flink's window
 "Buy" on a       in Kafka         operator finally
 train, in a                       processes it
 tunnel
    │                 │                 │
 EVENT TIME      INGESTION TIME   PROCESSING TIME
```

Which of these three is "the time of the purchase"? Finance says 12:03:15. A
naive stream job says 12:07:44. They will produce different daily totals, and one
of them will be in a report someone acts on.

<Callout type="key">

**Event time** — when the thing happened, in the real world. Lives inside the
record. Set by the producer.

**Ingestion time** — when the record entered your system. Set by the broker or by
the Flink source.

**Processing time** — whatever the machine's wall clock says when the operator
looks at the record. Set by nobody; it is just `System.currentTimeMillis()`.

</Callout>

## Why the difference is not an edge case

Every one of these is normal, not exotic:

| Cause | Typical gap |
| --- | --- |
| Mobile device offline, buffering events | seconds to **days** |
| Network retry / at-least-once producer | milliseconds to seconds |
| Kafka consumer lag during a traffic spike | seconds to minutes |
| A Flink job restarting and replaying from an old offset | **hours** |
| Backfilling a year of history through the same pipeline | **months** |

That fourth row deserves attention. Whenever your job restarts, it rewinds to the
last checkpoint and reprocesses. During that catch-up, processing time races
ahead of event time at enormous speed. A job written against processing time
produces *completely different output* during recovery than it did the first
time — which means a crash silently corrupts your results.

## The reproducibility argument

This is the argument that should decide it for you.

```text
EVENT TIME
  Run the job today          →  window 12:00–12:01 contains exactly events A,B,C
  Replay the same data next  →  window 12:00–12:01 contains exactly events A,B,C
  year on a faster cluster       (identical, forever)

PROCESSING TIME
  Run the job today          →  window 12:00–12:01 contains A,B,C
  Replay next year           →  window 12:00–12:01 contains… whatever the machine
                                 happened to have read by then. Different every time.
```

<Callout type="mental">

Event time is **the timestamp written on the letter**. Processing time is **the
day you happened to open your post**.

If you file your bank statements by the day you opened the envelope, your records
are useless the moment the post is delayed. Nobody does that with letters. Plenty
of people do it with streams.

</Callout>

## The honest trade-off

Event time is not free, and it is not always right.

<Compare>
  <CompareCard title="Event time" rows={[
    ['Correct?', 'Yes — results depend only on the data'],
    ['Reproducible?', 'Yes, forever, on any cluster, at any speed'],
    ['Latency', 'Higher — you must wait for stragglers before closing a window'],
    ['Needs', 'A trustworthy timestamp in every record, plus a watermark strategy'],
    ['Fails when', 'Producer clocks are wrong, or records have no timestamp at all'],
    ['Use for', 'Analytics, billing, aggregation, joins, sessionisation, anything audited'],
  ]} />
  <CompareCard title="Processing time" rows={[
    ['Correct?', 'Only if "now" is genuinely what you mean'],
    ['Reproducible?', 'No. Never. Not even once.'],
    ['Latency', 'Lowest possible — nothing is ever waited for'],
    ['Needs', 'Nothing'],
    ['Fails when', 'The job restarts, lags, backfills, or replays — i.e. in production'],
    ['Use for', 'Rate limiting, liveness checks, cache TTLs, "alert if silent for 5 min"'],
  ]} />
</Compare>

Processing time is not a beginner mistake in *all* cases. "Has this machine sent
me anything in the last five minutes?" is genuinely a processing-time question —
the absence of data has no event time. Just be sure the question is about *your
system*, not about *the world*.

### Ingestion time: the compromise nobody needs

Ingestion time stamps the record when it enters Flink. It is more stable than
processing time (all downstream operators agree on it) but it is still not
reproducible, because it depends on when your system happened to receive things.

In practice it is rarely the right answer. If your records genuinely have no
usable timestamp, ingestion time is a reasonable fallback — and you should treat
that as a data-quality bug to fix upstream, not a design.

## Choosing, in practice

| Requirement | Clock | Why |
| --- | --- | --- |
| Revenue per hour | Event | It must match the ledger and survive a replay |
| Sessionise user activity | Event | The gap between actions is a real-world quantity |
| Alert if a sensor is silent for 5 min | **Processing** | Silence has no event time; you are measuring your own system |
| Fraud: 3 failed logins within 60s | Event | The attacker's timing is what matters, not your lag |
| Rate-limit an outbound API to 100/s | **Processing** | You are protecting a real resource, right now |
| Join orders to shipments | Event | Otherwise the join window shifts under replay |
| Dashboard "requests in the last minute" | Either — but say which | Two different, both-defensible metrics |

## Setting it in code

<Callout type="version">

In Flink 1.x you set a job-wide `TimeCharacteristic`:

```java
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);   // Flink 1.x
```

This was **deprecated in 1.12 and removed**. In Flink 2.x there is no global
setting. The clock is decided **per operator**, by which window assigner and
which timer service you use — and event time is the default assumption. Any
tutorial that opens with `setStreamTimeCharacteristic` predates 2020.

</Callout>

```java
// Event time: attach a WatermarkStrategy at the source, then use
// event-time window assigners.
DataStream<Click> clicks = env.fromSource(
    kafkaSource,
    WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((e, ts) -> e.eventTime()),
    "clicks");

clicks.keyBy(Click::page)
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))   // event time
      .aggregate(new CountAgg());

// Processing time: no watermarks needed, different assigner.
clicks.keyBy(Click::page)
      .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
      .aggregate(new CountAgg());
```

Inside a `ProcessFunction` you get both, explicitly:

```java
ctx.timestamp()                        // this record's EVENT time
ctx.timerService().currentWatermark()  // event-time progress so far
ctx.timerService().currentProcessingTime()  // wall clock
ctx.timerService().registerEventTimeTimer(t);      // fires when watermark passes t
ctx.timerService().registerProcessingTimeTimer(t); // fires when the clock passes t
```

<Callout type="hood">

Event time is not a mode Flink runs in — it is **data that flows with your
records**. A `StreamRecord` carries a `long` timestamp, and separate control
messages called *watermarks* travel down the same channels announcing how far
event time has progressed.

If nothing assigns a timestamp, the field is null and every event-time window
gets a null timestamp, and Flink throws:

```text
Record has Long.MIN_VALUE timestamp (= no timestamp marker).
Is the time characteristic set to 'ProcessingTime',
or did you forget to call
'DataStream.assignTimestampsAndWatermarks(...)'?
```

That error message is the single most useful one in Flink. It means exactly what
it says.

</Callout>

<Expert>

**Clock skew across producers.** Event time is only as trustworthy as the
producers' clocks. A mobile fleet with unsynchronised clocks will produce events
timestamped in the *future*, which pushes your watermark forward and causes
correct, on-time events from other devices to be classified late.

Defences, roughly in order of preference: stamp event time at a gateway you
control; clamp obviously-bogus timestamps at ingestion; or use `withIdleness`
plus [watermark alignment](/docs/flink/watermarks/propagation-and-idleness) to
stop one bad source from dragging global time.

**Timestamps survive operators, mostly.** A record's event-time timestamp is
carried through `map`, `filter`, `flatMap` automatically. But a window's *output*
record gets the timestamp of the window's `maxTimestamp` — so chained windows
work, but the semantics of the second window are "windows of window results", not
"windows of original events". This surprises people building multi-stage
aggregations.

</Expert>

<Callout type="remember">

Event time is a property of the data. Processing time is a property of your
cluster's mood. Use event time for anything that must still be true tomorrow.

</Callout>

## Next

**[Out-of-order and late events](/docs/flink/time/out-of-order-and-late)** — what actually happens when the times disagree.
