---
title: Watermark generation
sidebar_label: Generation
description: The WatermarkGenerator interface, periodic vs punctuated emission, and writing a custom generator that does not break your job.
---

# Watermark generation

<PageMeta level="advanced" time="8 min" prereq={[['What is a watermark?', '/docs/flink/watermarks/what-is-a-watermark']]} />

<Objectives>

- Read and implement the `WatermarkGenerator` interface
- Choose between periodic and punctuated emission
- Avoid the three ways a custom generator silently breaks event time

</Objectives>

## The interface

Two methods. That is the whole extension point.

```java
public interface WatermarkGenerator<T> {

    /** Called for EVERY record. Usually just updates internal state. */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /** Called on a timer (pipeline.auto-watermark-interval, default 200ms). */
    void onPeriodicEmit(WatermarkOutput output);
}
```

Where you emit determines the style:

| Style | Emits from | Behaviour |
| --- | --- | --- |
| **Periodic** | `onPeriodicEmit` | Watermark advances on a clock tick. The default, and what all built-ins do. |
| **Punctuated** | `onEvent` | Watermark advances only when a specific record says so. Exact, but only possible if your data carries markers. |

## The built-in, in full

`BoundedOutOfOrdernessWatermarks` is about fifteen lines. Reading it removes most
of the mystery.

```java
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        // start at the smallest possible value, so any real event moves it
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);   // record only
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```

Three things worth noticing:

1. **`onEvent` does not emit.** It only tracks the maximum. All emission happens on the timer.
2. **Emission is unconditional.** It emits every 200ms even if the value has not changed; downstream ignores non-advancing watermarks.
3. **The `-1`** keeps an event arriving exactly at `maxTimestamp - bound` on time, because watermark *W* means "nothing more with timestamp ≤ W".

## Punctuated: when your data tells you

Some streams contain explicit end-of-period markers — CDC transaction boundaries,
end-of-day markers in financial feeds, batch-complete signals. If yours does, you
can have *exact* event time with no heuristic at all.

```java
public class PunctuatedWatermarks implements WatermarkGenerator<Event> {

    @Override
    public void onEvent(Event e, long eventTimestamp, WatermarkOutput output) {
        if (e.isEndOfPeriodMarker()) {
            // The producer GUARANTEES nothing older is coming. No bound needed.
            output.emitWatermark(new Watermark(e.periodEnd()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // nothing — emission is driven entirely by the data
    }
}
```

<Callout type="prod">

Punctuated watermarks are strictly better than heuristic ones **when the guarantee
is real**. Zero unnecessary latency, zero late records.

They are strictly worse when it is not: if a marker is ever lost or delayed, event
time simply stops, and unlike a periodic generator there is nothing to nudge it
forward. Pair them with a watchdog metric on `currentOutputWatermark`.

</Callout>

## A custom generator worth writing

A genuinely useful pattern: an **adaptive bound** that widens when the stream is
messy and narrows when it is clean, so you are not permanently paying worst-case
latency.

```java
public class AdaptiveWatermarks<T> implements WatermarkGenerator<T> {

    private final long minBound;
    private final long maxBound;
    private long maxTimestamp = Long.MIN_VALUE;
    private long observedLateness = 0;   // largest reordering seen recently

    public AdaptiveWatermarks(Duration min, Duration max) {
        this.minBound = min.toMillis();
        this.maxBound = max.toMillis();
    }

    @Override
    public void onEvent(T event, long ts, WatermarkOutput output) {
        if (ts < maxTimestamp) {
            // this record was out of order by exactly this much
            observedLateness = Math.max(observedLateness, maxTimestamp - ts);
        }
        maxTimestamp = Math.max(maxTimestamp, ts);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long bound = Math.min(maxBound, Math.max(minBound, observedLateness));
        output.emitWatermark(new Watermark(maxTimestamp - bound - 1));
        // decay, so a single bad burst does not widen the bound forever
        observedLateness = (long) (observedLateness * 0.95);
    }
}
```

Wire it up:

```java
WatermarkStrategy<Click> strategy = WatermarkStrategy
    .<Click>forGenerator(ctx -> new AdaptiveWatermarks<>(
            Duration.ofSeconds(1), Duration.ofMinutes(2)))
    .withTimestampAssigner((e, ts) -> e.eventTime())
    .withIdleness(Duration.ofMinutes(1));
```

<Callout type="mistake" title="Three ways a custom generator quietly breaks your job">

**1. Emitting a watermark that goes backwards.** Flink will not crash; downstream
operators simply ignore it. Your event time appears to stall for no visible
reason. Always track a maximum.

**2. Emitting from `onEvent` on every record.** A watermark is a broadcast control
message to every downstream channel. Emitting per record on a million-record-per-
second stream floods the network with control traffic and can cost more than the
data itself. Emit on the timer unless you have punctuation.

**3. Forgetting that the generator is per-split.** With per-split watermarking,
`onEvent` sees only one partition's records. State inside the generator is
per-partition, and any assumption about global maxima is wrong.

</Callout>

<Callout type="hood" title="Where the generator actually runs">

With `env.fromSource(source, strategy, name)`, the source operator creates **one
generator per split**. A Kafka source subtask reading 3 partitions has 3
generators, each tracking its own maximum. The operator emits the **minimum**
across them, and a split marked idle is excluded from that minimum.

With `assignTimestampsAndWatermarks(strategy)` on a `DataStream`, there is **one
generator per subtask**, over the already-merged records — so per-partition
tracking is gone and you get exactly the "fast partition drags the watermark
forward" problem described in
[timestamp assignment](/docs/flink/time/timestamp-assignment).

Same strategy object, different granularity, different correctness. This is the
strongest argument for assigning at the source.

</Callout>

<Expert>

**`WatermarkOutput` has two methods.** `emitWatermark(w)` and
`markIdle()`/`markActive()`. `withIdleness` is implemented by wrapping your
generator in a `WatermarksWithIdleness` that calls `markIdle()` after the timeout
elapses without an event. You can call these yourself in a custom generator when
you have better information than a timeout — for example, a source that knows a
partition has been revoked.

**Watermark alignment is enforced elsewhere.** `withWatermarkAlignment` is not
implemented in the generator; it is coordinated between the source operator and
the `SourceCoordinator` on the JobManager, which periodically collects watermarks
across subtasks and instructs sources that are too far ahead to pause reading.
That is why it works across subtasks and even across different sources sharing an
alignment group.

**Testing generators.** `WatermarkGenerator` is a plain object with no Flink
runtime dependency. Unit-test it directly: feed events, call `onPeriodicEmit` with
a mock `WatermarkOutput`, assert the emitted values. This is far faster than
end-to-end tests and catches the "goes backwards" bug immediately.

</Expert>

<Callout type="remember">

Built-ins track a maximum in `onEvent` and emit on a 200ms timer. Punctuated
generators emit from `onEvent` and are exact when your data has real markers. One
generator per split at the source; one per subtask downstream — and that
difference is a correctness difference.

</Callout>

## Next

**[Propagation and idleness](/docs/flink/watermarks/propagation-and-idleness)** — why one quiet partition stops your entire job.
