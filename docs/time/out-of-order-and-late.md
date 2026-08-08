---
title: Out-of-order and late events
sidebar_label: Out-of-order and late
description: The difference between "arrived in the wrong order" and "arrived too late", and why only one of them is a problem.
---

# Out-of-order and late events

<PageMeta level="intermediate" time="8 min" prereq={[['The three clocks', '/docs/flink/time/three-clocks']]} />

<Objectives>

- Distinguish out-of-order from late — they are not synonyms
- Explain why "just buffer and sort" is not a solution
- Describe the three things Flink can do with a late record

</Objectives>

## Two different words for two different things

People use these interchangeably. They are not the same, and the difference is
the whole design space.

<Compare>
  <CompareCard title="Out-of-order" rows={[
    ['Means', 'Arrived after an event with a LATER event time'],
    ['Is it a problem?', 'No — completely normal, and Flink handles it invisibly'],
    ['Frequency', 'Constant. Most records in a partitioned system are out of order relative to some other record.'],
    ['Cost', 'You wait a little before emitting results'],
  ]} />
  <CompareCard title="Late" rows={[
    ['Means', 'Arrived after Flink already DECIDED its time period was complete'],
    ['Is it a problem?', 'Yes — its window may already have fired and its state may be gone'],
    ['Frequency', 'Should be rare. If it is not, your bound is wrong.'],
    ['Cost', 'A wrong result, a corrected result, or a dropped record — you choose'],
  ]} />
</Compare>

<Callout type="key">

Out-of-order is about **other events**. Late is about **the watermark**.

An event is out of order if something with a bigger timestamp arrived first. An
event is late if the watermark has already passed its timestamp. You can be out
of order without being late — that is the normal, healthy case.

</Callout>

## Watch it happen

Step through the lab below one record at a time. Blue is on time, amber is late,
red was dropped entirely.

<WatermarkLab />

Do these three things in it, in order:

1. **Leave the bound at 3 and step through.** Notice events arrive out of order constantly, and nothing bad happens. Windows fire with the right contents.
2. **Set the bound to 0 and reset.** Now the watermark equals the newest timestamp seen. Almost every out-of-order record becomes late. This is what "no tolerance" costs you.
3. **Set the bound to 10 and reset.** Nothing is ever late — but look at how long a window waits before firing. That delay is your end-to-end latency, and you paid it on *every* window, to protect against a straggler that usually was not coming.

That trade-off — steps 2 and 3 — is the entire tuning problem, and there is no
setting that avoids it.

## Why "just buffer and sort" does not work

The obvious fix: hold records in memory, sort by timestamp, release in order.

It fails for three independent reasons.

**1. You never know when to release.** To emit the record with timestamp
`12:00:01`, you must know that nothing older is still coming. On an unbounded
stream you cannot know that. Ever. You can only *decide* it.

**2. The buffer is unbounded.** Holding "enough" records means holding
potentially all of them. A device offline for three days would require three days
of buffered data to guarantee correct ordering.

**3. It converts a latency problem into a memory problem** without solving
anything. You still have to decide when to release, which is the original
question.

<Callout type="mental">

Flink's answer is not to sort. It is to **declare a policy and act on it**:

> "I will assume events are at most 10 seconds out of order. On that assumption I
> will call a period complete and emit a result. If I turn out to be wrong, here
> is what I will do about it."

That declaration is the [watermark](/docs/flink/watermarks/what-is-a-watermark).
The "what I will do about it" is allowed lateness and side outputs.

Notice this is a *heuristic*, not a proof. Flink is not guessing randomly — it is
making an explicit, tunable, documented bet, and giving you the tools to handle
losing it.

</Callout>

## The three things you can do with a late record

Once a record arrives behind the watermark, you have exactly three options. Pick
consciously; the default picks for you.

### 1. Drop it (the default)

```java
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
.aggregate(new CountAgg());
// records behind the watermark are silently discarded
```

Fine when a tiny error is acceptable and simplicity matters. **Dangerous because
it is silent** — nothing logs, nothing alerts, your numbers are just slightly
wrong.

Always monitor it:

```java
// metric: numLateRecordsDropped, per window operator
```

Alert if it is non-zero for anything that matters.

### 2. Allow lateness — re-fire the window with a corrected result

```java
.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
.allowedLateness(Duration.ofMinutes(5))
.aggregate(new CountAgg());
```

The window keeps its state for 5 extra minutes after firing. Each late record
that arrives in that period triggers **another emission** with an updated result.

Two costs, both real:

- **State lives longer.** With a 1-minute window and 5 minutes of lateness, you hold six windows' state per key instead of one. On a large key space that is a six-fold increase in checkpoint size.
- **Downstream sees multiple results per window.** Your sink must handle that — an upsert by window key, not an append. Otherwise you have just invented duplicates.

### 3. Side output — route late records somewhere else

```java
OutputTag<Click> lateTag = new OutputTag<>("late-clicks"){};

SingleOutputStreamOperator<Result> main = clicks
    .keyBy(Click::page)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .sideOutputLateData(lateTag)
    .aggregate(new CountAgg());

main.getSideOutput(lateTag)
    .sinkTo(deadLetterSink);   // count them, inspect them, reprocess them
```

This is the production-grade choice. Nothing is silently lost, you can measure
your actual lateness distribution, and you can reprocess later in a batch job if
the numbers matter.

<Callout type="prod" title="How to actually pick the bound">

Do not guess. Measure.

1. Run a job that computes `processingTime - eventTime` per record and emits a histogram.
2. Look at the distribution — it is usually long-tailed, not normal.
3. Set the bound near the **p99**, not the max. The max is one phone in a tunnel; sizing for it makes every window slow for everyone.
4. Route the remaining ~1% to a side output and count them.
5. Alert if that count moves. A jump means an upstream producer changed, and you will find out on your terms rather than from a finance report.

A bound tuned to p99 with a side output beats a bound tuned to p100 with no
visibility, every time.

</Callout>

<Callout type="mistake">

Setting `allowedLateness` to something enormous (say an hour) "to be safe". You
have just multiplied your window state by sixty and made every checkpoint sixty
times slower, to catch records you are not measuring and whose sink may not even
handle updates.

Measure first. Side-output second. Allowed lateness only for the narrow band
where correction is genuinely worth the state.

</Callout>

<Expert>

**Late is per-operator, not per-job.** Lateness is determined by the watermark
*at the operator that receives the record*. A record can be on time at the first
window and late at a second, downstream window — because that operator's
watermark has advanced further. This is why chained aggregations sometimes lose
data in ways that surprise people.

**Allowed lateness and cleanup.** With allowed lateness *L*, window state is
purged when `watermark > windowEnd + L`. Until then it is retained *even for
windows that already fired*. In the Table API the analogous knob is
`table.exec.emit.late-fire`, and the state-retention behaviour is the same.

**Interaction with idle sources.** If a source partition goes idle, the watermark
stops advancing, so *nothing* is late — and nothing fires either. "Zero late
records" is not automatically good news; it can mean event time is frozen. Check
`currentOutputWatermark` before celebrating.

</Expert>

<Callout type="remember">

Out-of-order is normal and free. Late means the watermark already passed you.
Drop, correct, or side-output — choose deliberately, and measure the ones you
drop.

</Callout>

## Next

**[Timestamp assignment](/docs/flink/time/timestamp-assignment)** — getting the time into the record in the first place.
