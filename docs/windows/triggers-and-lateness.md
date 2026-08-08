---
title: Triggers and lateness
sidebar_label: Triggers & lateness
description: When a window fires, how to fire it early, and what happens to records that arrive after it did.
---

# Triggers and lateness

<PageMeta level="advanced" time="9 min" prereq={[['Window functions', '/docs/flink/windows/window-functions']]} />

<Objectives>

- Explain the default trigger's behaviour precisely
- Emit early results without breaking downstream consumers
- Reason about window state lifetime with allowed lateness

</Objectives>

## The default trigger

```java
// EventTimeTrigger — what you get if you specify nothing
onElement()   → CONTINUE          // just accumulate
onEventTime() → FIRE_AND_PURGE?   // when watermark ≥ window end: FIRE
```

In words: **fire once, when the watermark passes the window's end**. That is all.

`TriggerResult` has four values, and the difference between them is worth knowing:

| Result | Emits? | Keeps state? |
| --- | --- | --- |
| `CONTINUE` | no | yes |
| `FIRE` | yes | **yes** — the window can fire again |
| `PURGE` | no | no — state is dropped |
| `FIRE_AND_PURGE` | yes | no |

The default is `FIRE` (not `FIRE_AND_PURGE`) precisely so that `allowedLateness`
can re-fire the window later with a corrected result.

## Firing early

The default means a 1-hour window emits nothing for an hour. Sometimes you want
a preview.

```java
.window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
.trigger(ContinuousEventTimeTrigger.of(Duration.ofMinutes(1)))
.aggregate(new CountAgg());
// emits a running result every minute of event time, plus the final one
```

Or a custom trigger — early results at a count threshold, final result at the
watermark:

```java
public class EarlyResultTrigger extends Trigger<Object, TimeWindow> {

    private final ReducingStateDescriptor<Long> countDesc =
        new ReducingStateDescriptor<>("count", Long::sum, Long.class);

    @Override
    public TriggerResult onElement(Object el, long ts, TimeWindow w, TriggerContext ctx)
            throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countDesc);
        count.add(1L);

        // make sure the final, authoritative fire still happens
        ctx.registerEventTimeTimer(w.maxTimestamp());

        if (count.get() >= 1000) {
            count.clear();
            return TriggerResult.FIRE;      // FIRE, not FIRE_AND_PURGE:
        }                                   // keep accumulating
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow w, TriggerContext ctx) {
        return time == w.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long t, TimeWindow w, TriggerContext c) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow w, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(countDesc).clear();
    }
}
```

<Callout type="mistake" title="Early firing changes your output contract">

Once a window can fire more than once, downstream receives **multiple results for
the same window**. If your sink appends, you have just created duplicates — and
they will look exactly like a fault-tolerance bug, which will waste a lot of
somebody's time.

Every multi-firing window needs a sink that upserts on
`(key, windowStart)`. Emit the window start in the record so the sink has
something to key on. Then early firing is safe and genuinely useful.

</Callout>

## Allowed lateness and state lifetime

This is the part people get wrong, so here is the exact rule.

```java
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
.allowedLateness(Duration.ofMinutes(5))
```

```text
window [12:00, 12:01)

watermark reaches 12:01  →  FIRE. Result emitted. State KEPT.
late record at 12:00:30  →  FIRE again. Corrected result emitted.
late record at 12:00:45  →  FIRE again. Corrected result emitted.
watermark reaches 12:06  →  state PURGED (windowEnd + lateness)
late record at 12:00:50  →  too late. Dropped, or side-outputted.
```

The cost is state, and it is easy to underestimate:

```text
1-minute windows, no lateness       →  ~1 window live per key
1-minute windows, 5 min lateness    →  ~6 windows live per key
                                       6× the state, 6× the checkpoint size

1-hour window, 1-min slide, 10 min lateness
   → (60 + 10) / 1 = 70 windows live per key
```

<Callout type="prod" title="Lateness is a state decision, not just a correctness decision">

Before setting `allowedLateness`, compute:

```text
extra state ≈ existing window state × (lateness / windowSize)
```

Then check that against your checkpoint size budget. A change that looks like
"catch a few more records" can double your checkpoint duration and push a
comfortably-running job into checkpoint timeouts — see the
[runbook](/docs/flink/production/runbook).

</Callout>

## The complete late-data configuration

```java
OutputTag<Click> lateTag = new OutputTag<>("late-clicks"){};

SingleOutputStreamOperator<Report> results = clicks
    .keyBy(Click::page)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .allowedLateness(Duration.ofMinutes(2))    // correct within 2 min
    .sideOutputLateData(lateTag)               // capture the rest
    .aggregate(new CountAgg(), new AddWindowMetadata());

results.sinkTo(upsertSink);                    // handles re-fires

results.getSideOutput(lateTag)
       .map(c -> new LateRecord(c, System.currentTimeMillis()))
       .sinkTo(deadLetterSink);                // measured, not lost
```

Three decisions, all explicit:

1. **2 minutes of correction** — sized from the measured lateness distribution
2. **Everything beyond that is captured**, not silently dropped
3. **The sink upserts**, so re-fires are safe

<Callout type="hood" title="Where the window's cleanup timer comes from">

`WindowOperator` registers a cleanup timer at `window.maxTimestamp() + allowedLateness`.
When it fires, the window's contents state, the trigger's state, and the merging-window
metadata are all cleared.

Two subtleties:

- With `allowedLateness = 0`, the cleanup timer coincides with the firing timer, so `FIRE` is immediately followed by cleanup — indistinguishable from `FIRE_AND_PURGE` in effect.
- Cleanup is driven by the **watermark**, so a stalled watermark means windows never get cleaned up. An idle partition therefore causes unbounded state growth *as well as* missing output. Two symptoms, one cause.

</Callout>

<Expert>

**Triggers have state, and it must be cleared.** `ctx.getPartitionedState(...)`
inside a trigger creates keyed state scoped to `(key, window)`. If you do not
clear it in `clear()`, it survives the window's purge and leaks. This is one of
the most common custom-trigger bugs.

**`PURGE` does not delete the window.** It clears the window's *contents*. The
window object itself and its cleanup timer remain, and a subsequent record for
that window re-creates the contents. `PURGE` therefore does not mean "this window
is finished".

**Merging windows and triggers.** For session windows, `Trigger.onMerge` is called
when windows merge, and a trigger with timers must re-register them for the merged
window. `EventTimeTrigger` handles this; a naive custom trigger will not, and the
merged session will silently never fire.

**SQL equivalents.** `table.exec.emit.early-fire.enabled` plus
`early-fire.delay` gives early results; `table.exec.emit.late-fire.enabled` plus
`late-fire.delay` gives late corrections. The state implications are identical,
and the Table API will emit retractions rather than upserts — which your sink must
also handle.

</Expert>

<Callout type="remember">

The default trigger fires once, at the watermark. Early firing means multiple
results per window, so the sink must upsert. Allowed lateness multiplies window
state by `lateness / windowSize`. And a stalled watermark stops cleanup as well as
firing.

</Callout>

## Next

**[Level 5 — why state?](/docs/flink/state/why-state)** — the thing that makes everything else hard.
