---
title: Window functions
sidebar_label: Window functions
description: reduce, aggregate, process — and why the wrong choice multiplies your state by the number of records in a window.
---

# Window functions

<PageMeta level="intermediate" time="9 min" prereq={[['Window types', '/docs/flink/windows/window-types']]} docs="docs/dev/datastream/operators/windows/" />

<Objectives>

- Choose between `reduce`, `aggregate` and `process` on state cost, not convenience
- Combine incremental aggregation with window metadata, getting both
- Write an `AggregateFunction` that is correct under merging

</Objectives>

## The choice that matters

There are three ways to compute a window, and they differ by **orders of
magnitude** in memory.

<Compare>
  <CompareCard title="reduce / aggregate" rows={[
    ['State per window', 'ONE accumulator'],
    ['When computed', 'Incrementally, as each record arrives'],
    ['Memory', 'O(1) per window'],
    ['Window metadata', 'Not available (unless combined — see below)'],
    ['Use', 'Almost always'],
  ]} />
  <CompareCard title="process" rows={[
    ['State per window', 'EVERY record in the window'],
    ['When computed', 'All at once, when the window fires'],
    ['Memory', 'O(n) per window'],
    ['Window metadata', 'Full access: start, end, key, side outputs, timers'],
    ['Use', 'Only when you truly need all records — median, percentiles, ordering'],
  ]} />
</Compare>

<Callout type="key">

`aggregate` keeps **one object per window**. `process` keeps **every record**.

A page receiving 10,000 clicks a minute, over 1,000 pages, with a 1-minute window:

- `aggregate` → 1,000 accumulators ≈ **a few hundred KB**
- `process` → 10,000,000 buffered records ≈ **gigabytes**

Same output. Same code shape. Four orders of magnitude of difference.

</Callout>

## ReduceFunction — same type in, same type out

The simplest. Combine two values of the same type into one.

```java
// running maximum temperature per sensor per minute
.reduce((a, b) -> a.temperature() > b.temperature() ? a : b)
```

Constraint: input and output types must be identical. That rules out anything
where the accumulator differs from the record — which is most things, including
an average.

## AggregateFunction — the general workhorse

Three types: input, accumulator, output. This is what you should reach for.

```java
/** Average order value. IN=Order, ACC=(sum,count), OUT=Double. */
public class AvgOrderValue
        implements AggregateFunction<Order, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return Tuple2.of(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Order o, Tuple2<Double, Long> acc) {
        return Tuple2.of(acc.f0 + o.amount(), acc.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> acc) {
        return acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1;
    }

    /** REQUIRED for session windows, and for two-phase aggregation. */
    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}
```

<Callout type="mistake">

Implementing `merge` incorrectly — or as `throw new UnsupportedOperationException()`
because "we only use tumbling windows".

It compiles, it passes tests, and it fails at runtime the day someone switches to
session windows or the optimiser enables two-phase aggregation. `merge` must be
associative and commutative: `merge(a, b)` must equal `merge(b, a)`, and grouping
must not matter. If your accumulator cannot satisfy that (a "first value seen",
for instance), say so explicitly rather than leaving a trap.

</Callout>

## ProcessWindowFunction — full access, full cost

```java
public class TopPages
        extends ProcessWindowFunction<Click, Report, String, TimeWindow> {

    @Override
    public void process(String key, Context ctx,
                        Iterable<Click> allRecords,      // ← every record, buffered
                        Collector<Report> out) {
        long start = ctx.window().getStart();
        long end   = ctx.window().getEnd();

        // legitimate reason to be here: you need the whole population
        List<Long> latencies = new ArrayList<>();
        allRecords.forEach(c -> latencies.add(c.latencyMs()));
        Collections.sort(latencies);
        long p95 = latencies.get((int) (latencies.size() * 0.95));

        out.collect(new Report(key, start, end, p95));

        // also available here, and nowhere else:
        ctx.output(sideTag, something);          // side outputs
        ctx.globalState();                       // state across all windows for this key
        ctx.windowState();                       // state scoped to this window
    }
}
```

Legitimate reasons to use it: exact percentiles, median, top-N by a full ordering,
anything needing side outputs from the window itself, or window start/end in the
output.

Not legitimate: "I wanted the window start time in my output." There is a way to
get that without buffering anything.

## The pattern you should actually use

Combine them. Aggregate incrementally, then decorate with metadata at fire time.

```java
clicks
  .keyBy(Click::page)
  .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
  .aggregate(
      new CountAgg(),                // ← incremental: ONE accumulator per window
      new AddWindowMetadata());      // ← runs ONCE at fire time, over the result
```

```java
/** Receives the single aggregate result, not the records. */
public class AddWindowMetadata
        extends ProcessWindowFunction<Long, Report, String, TimeWindow> {

    @Override
    public void process(String page, Context ctx,
                        Iterable<Long> aggResult,        // exactly ONE element
                        Collector<Report> out) {
        out.collect(new Report(
            page,
            ctx.window().getStart(),
            ctx.window().getEnd(),
            aggResult.iterator().next()));
    }
}
```

<Callout type="key">

This two-argument form gives you **O(1) state and full window metadata**. It is
the right default for nearly every windowed aggregation you will write, and most
people discover it far too late.

</Callout>

## Choosing

| You need | Use |
| --- | --- |
| count, sum, min, max, average | `aggregate` |
| Same as above plus window start/end | `aggregate` + `ProcessWindowFunction` |
| Exact median or p95 | `process` — you need the population. Consider approximation instead. |
| Top-10 by score | `aggregate` with a bounded heap accumulator, **not** `process` |
| Distinct count | `aggregate` with HyperLogLog if approximate is acceptable; `process` with a set if not |
| Emit side outputs from the window | `process` (or the combined form) |
| Anything at high volume | `aggregate`, and think hard before anything else |

<Callout type="prod" title="Top-N without buffering">

The instinct is to buffer everything and sort. You do not have to:

```java
public class TopN implements AggregateFunction<Item, PriorityQueue<Item>, List<Item>> {
    public PriorityQueue<Item> add(Item item, PriorityQueue<Item> acc) {
        acc.offer(item);
        if (acc.size() > 10) acc.poll();   // bounded at 10, forever
        return acc;
    }
    // ...
}
```

State is bounded at 10 items per window regardless of throughput. The same trick
works for approximate distinct counts (HyperLogLog), approximate quantiles
(t-digest), and heavy hitters (count-min sketch). Learning the sketch data
structures pays for itself repeatedly in streaming.

</Callout>

<Expert>

**Where the accumulator lives.** With `aggregate`, the accumulator is stored in an
`AggregatingState` in the state backend, keyed by `(key, window)`. Each record
does a read-modify-write. On RocksDB that is a deserialise-merge-serialise cycle
per record — which is why the accumulator's serialiser matters at high throughput,
and why a `Tuple2` accumulator beats a POJO with Kryo fallback.

**`ProcessWindowFunction` state.** `ctx.windowState()` is purged with the window;
`ctx.globalState()` is **not** — it persists across all windows for that key,
forever, unless you clear it. Using `globalState` without a cleanup path is a
common slow state leak.

**Firing more than once.** With `allowedLateness` or a custom trigger, the window
function runs again on each fire. With `aggregate`, the accumulator carries
forward and the new result is cumulative. With `process`, the `Iterable` contains
all retained records including previously-processed ones. Your sink must treat
these as upserts keyed by `(key, windowStart)`, not appends.

</Expert>

<Callout type="remember">

`aggregate` keeps one accumulator; `process` keeps every record. Combine them to
get incremental state plus window metadata. Implement `merge` properly. And reach
for a sketch before you reach for buffering.

</Callout>

## Next

**[Triggers and lateness](/docs/flink/windows/triggers-and-lateness)** — controlling when a window fires.
