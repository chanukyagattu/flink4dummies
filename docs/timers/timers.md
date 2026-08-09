---
title: Timers
sidebar_label: Timers
description: Scheduling a callback in the future — the mechanism behind sessions, timeouts, inactivity detection and precise state cleanup.
---

# Timers

<PageMeta level="intermediate" time="10 min" prereq={[['Serialization & evolution', '/docs/flink/state/serialization-and-evolution']]} docs="docs/dev/datastream/operators/process_function/" />

<Objectives>

- Register, cancel and coalesce timers correctly
- Choose between event-time and processing-time timers on requirement, not habit
- Avoid the timer explosion that OOMs a job at high key cardinality

</Objectives>

## What is it?

A timer is **a callback scheduled at a future time, bound to a key**.

```java
ctx.timerService().registerEventTimeTimer(t);       // fires when the WATERMARK passes t
ctx.timerService().registerProcessingTimeTimer(t);  // fires when the WALL CLOCK passes t

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<Out> out) {
    // runs with the key context restored — your keyed state is accessible here
}
```

<Callout type="mental">

A timer is a **sticky note to your future self, filed under a key**.

"When event time reaches 12:30, come back and look at what I remember about user
U123." Flink stores the note, and when time reaches 12:30 it hands you back the
note *and* U123's state, as if you had never left.

</Callout>

## Why they exist: absence has no event

Windows react to events. Timers react to **the absence of events**, which is a
whole class of requirement that windows structurally cannot express.

| Requirement | Why a window cannot do it |
| --- | --- |
| "Alert if no heartbeat for 5 minutes" | Silence produces no record, so no window is created |
| "Close the session after 30 min of inactivity" | The session's end is defined by what did *not* happen |
| "If the order is unpaid after 15 min, cancel it" | There is no "unpaid" event |
| "Clean up this key's state if unused for a week" | Nothing arrives to trigger cleanup |
| "Emit a result 3 seconds after the first event of a burst" | The window boundary is data-dependent |

## Event time vs processing time

<Compare>
  <CompareCard title="Event-time timers" rows={[
    ['Fires when', 'The watermark passes the timestamp'],
    ['During replay', 'Fires at the same logical point — deterministic'],
    ['If the watermark stalls', 'NEVER fires'],
    ['Reproducible', 'Yes'],
    ['Use for', 'Sessions, business timeouts, anything derived from event time'],
  ]} />
  <CompareCard title="Processing-time timers" rows={[
    ['Fires when', 'The wall clock passes the timestamp'],
    ['During replay', 'Fires immediately, all at once — non-deterministic'],
    ['If the watermark stalls', 'Still fires'],
    ['Reproducible', 'No'],
    ['Use for', 'Liveness checks, flushing buffers, rate limiting'],
  ]} />
</Compare>

<Callout type="mistake" title="The replay trap">

Processing-time timers during a 3-day backfill: **every** timer for those 3 days
fires within the first few seconds, because wall clock is already past all of
them.

A "cancel unpaid orders after 15 minutes" rule implemented with a processing-time
timer will, during a replay, cancel essentially every order in the history. If
that job writes to a real system, you have an incident.

Rule: if the requirement is expressed in terms of the data's timeline, it is an
**event-time** timer. Processing time is only for statements about *your system
right now*.

</Callout>

## The coalescing pattern

Timer registration is idempotent per `(key, timestamp)` — registering the same
timestamp twice creates one timer. But a *different* timestamp creates a *new*
one, which is how timer counts explode.

```java
// ❌ One timer per event. A user with 10,000 events has 10,000 pending timers.
public void processElement(Event e, Context ctx, Collector<Out> out) {
    ctx.timerService().registerEventTimeTimer(e.eventTime() + TIMEOUT);
}

// ✅ Coalesced: cancel the previous, register one. Always exactly one per key.
private transient ValueState<Long> currentTimer;

public void processElement(Event e, Context ctx, Collector<Out> out) throws Exception {
    Long previous = currentTimer.value();
    if (previous != null) {
        ctx.timerService().deleteEventTimeTimer(previous);
    }
    long next = e.eventTime() + TIMEOUT;
    ctx.timerService().registerEventTimeTimer(next);
    currentTimer.update(next);
}

public void onTimer(long ts, OnTimerContext ctx, Collector<Out> out) throws Exception {
    currentTimer.clear();
    // ... do the work, clear the state
}
```

<Callout type="key">

**Track your timer's timestamp in state, and delete the old one before
registering a new one.**

Without this, timer count grows with *event* count instead of *key* count. Timers
are checkpointed, so this shows up as growing checkpoint size and slow recovery,
often long before it shows up as memory pressure.

</Callout>

A cheaper variant when precision is not critical — round timers to a granularity
so repeated registrations collapse naturally:

```java
long coalesced = (e.eventTime() + TIMEOUT) / 1000 * 1000;   // round to the second
ctx.timerService().registerEventTimeTimer(coalesced);
```

At most one timer per key per second, no cancellation bookkeeping, at the cost of
up to a second of imprecision. For most timeout logic that is a good trade.

## A complete example: session with timeout

```java
public class SessionWithTimeout
        extends KeyedProcessFunction<String, Event, Session> {

    private static final long GAP = Duration.ofMinutes(30).toMillis();

    private transient ValueState<Session> session;
    private transient ValueState<Long> timer;

    @Override
    public void open(OpenContext ctx) {
        session = getRuntimeContext().getState(
            new ValueStateDescriptor<>("session", Session.class));
        timer = getRuntimeContext().getState(
            new ValueStateDescriptor<>("timer", Types.LONG));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<Session> out)
            throws Exception {

        Session s = session.value();
        if (s == null) s = new Session(ctx.getCurrentKey(), e.eventTime());
        s.add(e);
        session.update(s);

        // coalesce: cancel the old expiry, set the new one
        Long old = timer.value();
        if (old != null) ctx.timerService().deleteEventTimeTimer(old);

        long expiry = e.eventTime() + GAP;
        ctx.timerService().registerEventTimeTimer(expiry);
        timer.update(expiry);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Session> out)
            throws Exception {
        Session s = session.value();
        if (s != null) {
            s.close(ts - GAP);
            out.collect(s);        // emit the completed session
        }
        session.clear();           // and free the state
        timer.clear();
    }
}
```

This is roughly what `EventTimeSessionWindows` does internally, and writing it
yourself gives you control over the emitted record, early results, and per-key gap
logic.

## Where timers live

<Callout type="hood">

Timers are **keyed state**. They are checkpointed, restored, and redistributed on
rescale exactly like any other keyed state.

Two storage options:

| Factory | Stored in | Trade-off |
| --- | --- | --- |
| `heap` (default) | JVM heap, in a priority queue per key group | Fast; memory grows with timer count |
| `rocksdb` | RocksDB, alongside your state | Slower per timer; bounded memory |

```yaml
state.backend.rocksdb.timer-service.factory: rocksdb
```

The default catches people out: a job with `EmbeddedRocksDBStateBackend` and tens
of millions of timers still keeps **all of them on the heap** and OOMs. If your
key cardinality is large and every key has a timer, switch the factory.

Firing order: timers fire in **timestamp order** within a key group. When a
watermark jumps forward, all timers up to it fire in one batch, in order, before
processing continues.

</Callout>

<Expert>

**Timers are deduplicated per (key, timestamp, type).** Registering the same event
-time timestamp twice for the same key produces one timer. Event-time and
processing-time timers are separate namespaces, so the same timestamp in both
gives you two callbacks.

**`onTimer` and `processElement` never run concurrently.** Both execute in the
task's single mailbox thread, so keyed state access from `onTimer` needs no
synchronisation. It also means a slow `onTimer` blocks record processing — and a
watermark jump that fires a million timers at once will stall the pipeline
visibly.

**Timer state and rescaling.** Timers redistribute by key group with the rest of
keyed state. But the *heap* timer service keeps an in-memory priority queue per
key group, so a job with hundreds of millions of timers can be slow to restore
even when the state itself is small.

**Deleting a timer that does not exist is a no-op**, not an error. So defensive
`deleteEventTimeTimer` calls are safe and cheap.

**Timers in windows.** `WindowOperator` registers a cleanup timer at
`window.maxTimestamp() + allowedLateness`. Millions of open windows means millions
of timers — another reason a stalled watermark hurts twice.

</Expert>

<Callout type="try">

Take the session example and break it deliberately:

1. Remove the `deleteEventTimeTimer` call. Run with a few thousand keys and many events each. Watch checkpoint size climb even though the *state* is unchanged — those are timers.
2. Change both timer calls to processing time. Replay a day of historical data and watch every session close at once, in the first second.
3. Stop producing to one partition, without `withIdleness`. The event-time timers never fire, sessions never close, and state grows forever — three symptoms, one cause.

</Callout>

<Callout type="remember">

Timers handle absence, which windows cannot. Event time for anything about the
data; processing time only for statements about your system now. Always coalesce.
And move the timer service to RocksDB when key cardinality is high.

</Callout>

## Next

**[Level 7 — joins](/docs/flink/joins)** — combining two infinite streams.
