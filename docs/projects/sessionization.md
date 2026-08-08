---
title: "Project 2 — Sessionization"
sidebar_label: "2 · Sessionization"
description: Build session windows by hand with keyed state and event-time timers, so you can see exactly what they cost.
---

# Project 2 — Sessionization

<PageMeta level="intermediate" time="16 min" prereq={[['Project 1', '/docs/flink/projects/clickstream'], ['Timers', '/docs/flink/timers']]} />

**Goal:** group each user's activity into sessions, closing a session after 30 minutes
of inactivity.

**Teaches:** keyed state, event-time timers, timer coalescing, precise state cleanup —
and why `EventTimeSessionWindows` costs what it costs.

We build it by hand rather than using the built-in session window. The built-in is
what you should ship; building it once is how you understand it.

---

## The job

```java title="SessionJob.java"
public class SessionJob {

  public record Click(String userId, String page, long eventTime) {}
  public record Session(String userId, long start, long end,
                        int events, List<String> pages) {}

  private static final long GAP = Duration.ofMinutes(30).toMillis();

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000);
    env.setMaxParallelism(720);

    DataStream<Click> clicks = env
        .fromSource(kafkaSource(), watermarks(), "clicks")
        .map(SessionJob::parse).name("parse").uid("parse");

    clicks.keyBy(Click::userId)
          .process(new Sessionizer())
          .name("sessionize").uid("sessionize")
          .print();

    env.execute("sessionization");
  }
}
```

```java title="Sessionizer.java"
public class Sessionizer extends KeyedProcessFunction<String, Click, Session> {

  private static final long GAP = Duration.ofMinutes(30).toMillis();

  /** The session being built for the current key. */
  private transient ValueState<SessionState> session;
  /** The timestamp of the timer we currently have registered, so we can cancel it. */
  private transient ValueState<Long> timerTs;

  @Override
  public void open(OpenContext ctx) {
    session = getRuntimeContext().getState(
        new ValueStateDescriptor<>("session", SessionState.class));
    timerTs = getRuntimeContext().getState(
        new ValueStateDescriptor<>("timer", Types.LONG));
  }

  @Override
  public void processElement(Click click, Context ctx, Collector<Session> out)
      throws Exception {

    // ── 1. late arrivals: this event predates a session we already closed ──
    // Without this check, a very late click resurrects a closed session and
    // emits a second, overlapping one.
    long wm = ctx.timerService().currentWatermark();
    if (click.eventTime() + GAP < wm) {
        // its session would already have expired — ignore, or side-output
        return;
    }

    // ── 2. extend or start the session ─────────────────────────────────────
    SessionState s = session.value();
    if (s == null) {
        s = new SessionState(click.eventTime());
    }
    s.add(click);
    session.update(s);

    // ── 3. COALESCE the timer: cancel the old one, register exactly one ────
    // Without this, a user with 10,000 clicks has 10,000 pending timers.
    Long previous = timerTs.value();
    if (previous != null) {
        ctx.timerService().deleteEventTimeTimer(previous);
    }
    long expiry = click.eventTime() + GAP;
    ctx.timerService().registerEventTimeTimer(expiry);
    timerTs.update(expiry);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Session> out)
      throws Exception {
    SessionState s = session.value();
    if (s != null) {
        out.collect(new Session(
            ctx.getCurrentKey(), s.start(), timestamp - GAP,
            s.eventCount(), s.pages()));
    }
    // ── 4. CLEAN UP. This is the line that keeps the job alive for months. ──
    session.clear();
    timerTs.clear();
  }
}
```

## The four things that make it correct

<Callout type="key">

**1. The late-arrival guard.** A click whose session would already have expired must
not start a new one retroactively. Skipping this check produces overlapping sessions
that are extremely hard to diagnose downstream.

**2. Timer coalescing.** Cancel the previous timer before registering a new one. This
is the difference between *one timer per user* and *one timer per click*. Timers are
checkpointed, so the second option shows up as checkpoint growth long before it shows
up as memory pressure.

**3. Event-time timers, not processing-time.** With processing time, a replay of three
days of history would close every session in the first second. With event time, a
replay produces byte-identical output to the live run.

**4. `clear()` in `onTimer`.** Every `update()` needs a reachable `clear()`. Delete
these two lines and the job works perfectly — for about six weeks.

</Callout>

## Why the built-in is different

`EventTimeSessionWindows.withGap(30 min)` does something subtly more complex: each
event creates its **own** window of `[t, t + gap)`, and overlapping windows are then
**merged**.

```text
event at 10:00 → window [10:00, 10:30)
event at 10:05 → window [10:05, 10:35)   → MERGE → [10:00, 10:35)
event at 10:20 → window [10:20, 10:50)   → MERGE → [10:00, 10:50)
```

That merging is why:

- session windows need a `MergingWindowAssigner` and a mergeable accumulator (your `AggregateFunction.merge` must be correct);
- a **late** event can merge two previously separate sessions into one, retracting two results and emitting one;
- their state cost is higher and less predictable than a hand-rolled version.

Our version cannot merge sessions retroactively — which is a limitation, and also
exactly why it is cheaper and easier to reason about.

<Callout type="prod">

Ship the built-in unless you need something it does not give you: a custom emitted
record, early results for long-running sessions, per-user gap logic beyond
`withDynamicGap`, or tight control over state.

Build it by hand once, so that when the built-in behaves surprisingly you know why.

</Callout>

---

## Break it on purpose

### 1. Remove the timer coalescing

Delete the `deleteEventTimeTimer` call. Run the generator for a few minutes.

**What to look at:** checkpoint size in the UI. The *state* is unchanged — the same
sessions, the same data — but the checkpoint grows steadily. Those are timers.

**The lesson:** timers are state. Uncoalesced timers scale with event count, not key
count. → [Timers](/docs/flink/timers)

### 2. Remove `session.clear()`

Run for ten minutes with 1,000 distinct users.

**What to look at:** `lastCheckpointSize` climbing and never falling, even though every
session has closed.

**The lesson:** this is the single most common cause of a job that dies three months
after launch. → [TTL and growth](/docs/flink/state/ttl-and-growth)

### 3. Switch to processing-time timers

```java
ctx.timerService().registerProcessingTimeTimer(
    ctx.timerService().currentProcessingTime() + GAP);
```

Restart the job from `earliest-offset` so it replays.

**What happens:** during the catch-up, wall clock is already far past every timer, so
every session closes almost immediately. Sessions that should span 20 minutes of user
activity are emitted milliseconds apart with wrong boundaries.

**The lesson:** processing-time timers are not reproducible, and replay is when you
find out. → [The three clocks](/docs/flink/time/three-clocks)

### 4. Rescale mid-session

```bash
flink stop --savepointPath file:///tmp/flink-savepoints <jobId>
flink run -s file:///tmp/flink-savepoints/savepoint-xxx -p 8 target/projects.jar
```

**What happens:** in-progress sessions survive. Users whose key group moved to a
different subtask continue their session on the new subtask, with their state intact.

**What to look at:** the [KeyBy lab](/docs/flink/basics/parallelism-and-subtasks) with
your parallelism values, to see which users moved.

**The lesson:** this is key-group redistribution, working. → [Rescaling](/docs/flink/fault-tolerance/rescaling)

---

## Extensions

- **Emit early results.** Emit a partial session every 5 minutes of event time so
  downstream sees live sessions, and mark the final emission with a flag. Now your sink
  must upsert on `(userId, sessionStart)`.
- **Dynamic gap.** Give mobile users 45 minutes and desktop users 15. Notice that
  `withDynamicGap` on the built-in evaluates the gap per *record*, which has subtle
  consequences for merging.
- **Add a TTL backstop.** Set a 3-day TTL on the session state, so a key whose timer was
  somehow lost still gets cleaned up. Belt and braces.

## Next

**[Project 3 — fraud detection](/docs/flink/projects/fraud-detection)**
