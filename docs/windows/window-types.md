---
title: Window types
sidebar_label: Window types
description: Tumbling, sliding, session, global and count windows — with the state cost of each spelled out.
---

# Window types

<PageMeta level="intermediate" time="10 min" prereq={[['Why windows?', '/docs/flink/windows/why-windows']]} docs="docs/dev/datastream/operators/windows/" />

<Objectives>

- Choose the right assigner for a requirement, first time
- Calculate the state cost of a sliding window before deploying it
- Explain why session windows need merging and what that costs

</Objectives>

## Tumbling — fixed, non-overlapping

The default choice. Each record belongs to exactly one window.

```text
 ├───────┤───────┤───────┤───────┤
 0      1m      2m      3m      4m

 record at 1:30  →  window [1m, 2m)   exactly one window
```

```java
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))

// with an offset — useful for time zones and for staggering load
.window(TumblingEventTimeWindows.of(Duration.ofDays(1), Duration.ofHours(-8)))
```

**State cost:** one accumulator per key per open window. Cheapest option.

**Use for:** almost everything. Hourly revenue, per-minute counts, daily
aggregates.

## Sliding — fixed, overlapping

A window of size *S* emitted every *slide* interval. Records land in multiple
windows.

```text
 size = 1m, slide = 20s

 [────── window A ──────]
        [────── window B ──────]
               [────── window C ──────]
 0     20s    40s    1m    1m20  1m40

 record at 30s → belongs to A, B and C   ← three windows
```

```java
.window(SlidingEventTimeWindows.of(
    Duration.ofMinutes(1),    // size
    Duration.ofSeconds(20)))  // slide
```

<Callout type="mistake" title="Sliding windows are a state multiplier — do the arithmetic first">

Each record is written into `size / slide` windows.

| Size | Slide | Windows per record | Effect |
| --- | --- | --- | --- |
| 1 min | 20s | 3 | Fine |
| 1 hour | 1 min | **60** | 60× the state and 60× the write cost |
| 24 hours | 1 min | **1440** | Do not do this |

That last row: 10 million keys × 1440 windows × 100 bytes ≈ **1.4 TB of state**,
for what is usually a dashboard nobody watches at that resolution.

Before deploying a sliding window, compute `size / slide` and multiply by your key
count. If the number frightens you, use a tumbling window plus a downstream
rolling sum, or accept a coarser slide.

</Callout>

**Use for:** moving averages, "last 5 minutes, refreshed every 30 seconds",
trend detection — where the slide is a meaningful fraction of the size.

## Session — dynamic, activity-driven

No fixed boundaries. A window closes after a **gap** of inactivity.

```text
 gap = 30 min

 ●  ●  ●        (45 min of nothing)        ●  ●
 └─session 1──┘                            └session 2┘
```

```java
// fixed gap
.window(EventTimeSessionWindows.withGap(Duration.ofMinutes(30)))

// dynamic gap — per record. Mobile users might get a longer gap than desktop.
.window(EventTimeSessionWindows.withDynamicGap(
    (Click c) -> c.isMobile() ? Duration.ofMinutes(45).toMillis()
                              : Duration.ofMinutes(15).toMillis()))
```

<Callout type="hood" title="Why session windows are structurally different">

Every record initially creates **its own window** of `[timestamp, timestamp + gap)`.
Then Flink **merges** overlapping windows.

```text
event at 10:00 → window [10:00, 10:30)
event at 10:05 → window [10:05, 10:35)
                 overlap → MERGE → [10:00, 10:35)
event at 10:20 → window [10:20, 10:50)
                 overlap → MERGE → [10:00, 10:50)
```

Consequences that matter:

- The window's end moves forward as activity continues, so a session's duration is unbounded — an active user's session state is never purged.
- Merging requires a `MergingWindowAssigner` and a mergeable accumulator, which is why `ReduceFunction` and `AggregateFunction` work but a `FoldFunction` never did.
- A late record can **merge two previously separate sessions into one**, retracting two results and emitting one.
- State cost is higher and less predictable than for time windows.

</Callout>

**Use for:** user sessions, device connection periods, "bursts of related
activity". This is genuinely the right tool for those, despite the cost.

## Global — you supply the trigger

One window per key, no end. Fires only when a custom trigger says so.

```java
.window(GlobalWindows.create())
.trigger(CountTrigger.of(100))     // fire every 100 records
```

**Use for:** count-based windows, and as a building block for custom logic.

**Do not use** without a trigger — the window never fires and state grows forever.
This is a real way to OOM a cluster.

## Count windows

Convenience wrappers over `GlobalWindows`.

```java
.countWindow(100)         // tumbling: every 100 records
.countWindow(100, 10)     // sliding: last 100 records, every 10
```

<Callout type="mistake">

Count windows have **no timeout**. A key that reaches 99 records and then goes
quiet holds those 99 records in state forever, and never emits.

For low-traffic keys this is not an edge case — it is the normal case. If you use
count windows, pair them with a timer that flushes on inactivity, which in
practice means writing a `KeyedProcessFunction` instead.

</Callout>

## Choosing

| Requirement | Assigner |
| --- | --- |
| Revenue per hour | Tumbling, 1 hour |
| Requests per minute for a dashboard | Tumbling, 1 minute |
| Moving average over 5 min, updated every 30s | Sliding (5m, 30s) — 10 windows per record |
| How long was this user active? | Session, gap tuned to your product |
| Every 1000 events, emit a summary | Global + `CountTrigger` |
| Daily totals in the user's local time | Tumbling 1 day with an offset, keyed by time zone |
| "Last 60 seconds", exactly, per record | **Not a window** — `KeyedProcessFunction` with timers |

## Event time vs processing time assigners

Every assigner has both variants:

```java
TumblingEventTimeWindows.of(...)        // uses watermarks
TumblingProcessingTimeWindows.of(...)   // uses the wall clock
SlidingEventTimeWindows.of(...)
SlidingProcessingTimeWindows.of(...)
EventTimeSessionWindows.withGap(...)
ProcessingTimeSessionWindows.withGap(...)
```

The processing-time variants ignore watermarks entirely: no lateness, no waiting,
no reproducibility. They are correct for questions about *your system*, and wrong
for questions about *the world*. See [the three clocks](/docs/flink/time/three-clocks).

<Callout type="prod" title="Windows in SQL">

The Table API expresses the same things, with the same costs:

```sql
-- tumbling
SELECT window_start, window_end, COUNT(*)
FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

-- sliding (HOP) — same state multiplier applies
SELECT window_start, window_end, COUNT(*)
FROM TABLE(HOP(TABLE clicks, DESCRIPTOR(event_time),
                INTERVAL '20' SECOND, INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

-- CUMULATE: a running total within a period, emitted at intervals.
-- Very useful for "revenue so far today, updated hourly" and it has NO
-- DataStream equivalent — one of the genuine reasons to reach for SQL.
SELECT window_start, window_end, SUM(amount)
FROM TABLE(CUMULATE(TABLE orders, DESCRIPTOR(event_time),
                     INTERVAL '1' HOUR, INTERVAL '1' DAY))
GROUP BY window_start, window_end;
```

</Callout>

<Expert>

**Window alignment and the thundering herd.** Tumbling windows are aligned to the
epoch, so *every* key's 1-minute window ends at the same instant. At high key
counts that produces a sawtooth: a burst of state reads, emissions and purges
every minute, then quiet. If that burst is causing latency spikes, stagger the
offset by a per-key hash, or use a custom trigger that jitters the fire time.

**Windows on windows.** The output of a window carries the timestamp of its
`maxTimestamp`, so you can window a windowed stream. The second window operates on
*aggregate records*, not original events — useful for two-phase aggregation, but
remember that lateness semantics do not compose the way people expect.

**`allowedLateness` and sliding windows multiply.** Lateness *L* on a sliding
window keeps `(size + L) / slide` windows alive per key rather than
`size / slide`. On the 1-hour/1-minute example, adding 10 minutes of lateness
takes you from 60 to 70 live windows per key.

</Expert>

<Callout type="remember">

Tumbling by default. Compute `size / slide` before using sliding. Session windows
merge, which is powerful and expensive. Global and count windows need a trigger
and a timeout, or they leak.

</Callout>

## Next

**[Window functions](/docs/flink/windows/window-functions)** — and the choice that decides whether your state is megabytes or gigabytes.
