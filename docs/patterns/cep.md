---
title: Complex event processing
sidebar_label: CEP
description: Detecting patterns across a sequence of events — the library, the state cost, and when to write a ProcessFunction instead.
---

# Complex event processing

<PageMeta level="advanced" time="12 min" prereq={[['Timers', '/docs/flink/timers'], ['Joins', '/docs/flink/joins']]} docs="docs/libs/cep/" />

<Objectives>

- Express a sequence pattern with the CEP library and read it back correctly
- Distinguish the four contiguity modes, which is where most CEP bugs live
- Decide honestly between CEP and a hand-written `KeyedProcessFunction`

</Objectives>

## The problem

Aggregation asks *"how many?"*. CEP asks *"did this happen, in this order, within
this time?"*.

```text
LOGIN_FAILED
   ↓  (within 60s)
LOGIN_FAILED
   ↓  (within 60s)
LOGIN_FAILED
   ↓
LOGIN_SUCCESS          ← this sequence is a credential-stuffing signal
```

No window computes that. The pattern is about **order and proximity**, and it can
straddle any window boundary you pick.

<Callout type="mental">

CEP is **a regular expression over a stream of events**, per key.

`begin("a").next("b").followedBy("c").within(60s)` is doing for events what
`/ab.*c/` does for characters — except the alphabet is your event types, the
matching is per key, and there is a time bound on the whole match.

</Callout>

## A pattern, end to end

```java
Pattern<LoginEvent, ?> suspicious = Pattern
    .<LoginEvent>begin("failures")
        .where(SimpleCondition.of(e -> e.type() == FAILED))
        .times(3).within(Duration.ofSeconds(60))   // 3 failures within a minute
    .next("success")                               // IMMEDIATELY after — no gap
        .where(SimpleCondition.of(e -> e.type() == SUCCESS))
    .within(Duration.ofMinutes(2));                // the whole match, within 2 min

PatternStream<LoginEvent> matches = CEP.pattern(
    logins.keyBy(LoginEvent::userId),              // ALWAYS keyBy first
    suspicious);

DataStream<Alert> alerts = matches.process(
    new PatternProcessFunction<LoginEvent, Alert>() {
        @Override
        public void processMatch(Map<String, List<LoginEvent>> match,
                                 Context ctx, Collector<Alert> out) {
            List<LoginEvent> failures = match.get("failures");
            LoginEvent success = match.get("success").get(0);
            out.collect(new Alert(success.userId(), failures.size(), success.ip()));
        }
    });
```

Three things to notice:

1. **`keyBy` first, always.** CEP over a non-keyed stream runs at parallelism 1 and matches across unrelated users — almost never what you want.
2. **The match is a `Map` from pattern name to the events that filled it.** Name your stages meaningfully; those names are your API.
3. **`within` on the whole pattern** bounds state. Without it, partial matches are retained indefinitely.

## Contiguity: the part that causes bugs

Four ways to say "then". Choosing the wrong one produces a pattern that either never
matches or matches far too much.

| Combinator | Means | Matches `A C B`? |
| --- | --- | --- |
| `next(B)` | **Strictly** next — nothing in between | ❌ |
| `followedBy(B)` | Next matching, **skipping** non-matching events | ✅ |
| `followedByAny(B)` | Like `followedBy`, but keeps looking for *more* matches | ✅ (and continues) |
| `notNext(B)` / `notFollowedBy(B)` | B must **not** occur | — |

```text
stream:  A  X  B

.begin("a").next("b")        → NO match  (X came between)
.begin("a").followedBy("b")  → match     (X ignored)
```

<Callout type="mistake" title="`next` is stricter than people expect">

`next` means *the very next event for this key*. Any unrelated event — a heartbeat,
a page view, a keep-alive — breaks the match.

In real streams, keys almost always carry mixed event types, so `next` silently
matches nothing. Use `followedBy` unless you specifically mean "with absolutely
nothing in between", and filter the stream to the relevant event types first.

</Callout>

<Callout type="mistake" title="`followedByAny` is a combinatorial explosion">

```text
stream:  A  B1  B2  B3

.followedBy("b")     → 1 match  (A,B1)
.followedByAny("b")  → 3 matches (A,B1) (A,B2) (A,B3)
```

With longer patterns this multiplies. A three-stage `followedByAny` pattern over a
busy key can produce thousands of matches from a handful of events, and the state to
track them grows accordingly.

Use it only when you genuinely want every combination, and bound it tightly with
`within`.

</Callout>

## Quantifiers and conditions

```java
.times(3)                    // exactly 3
.times(2, 5)                 // between 2 and 5
.oneOrMore()                 // 1+
.optional()                  // 0 or 1
.greedy()                    // take as many as possible
.consecutive()               // the repetitions must be strictly contiguous
.allowCombinations()         // all subsets of the repetitions
```

**Iterative conditions** are where CEP gets genuinely expressive — a condition that
can look at the events already matched:

```java
.<Txn>begin("escalating")
    .where(SimpleCondition.of(t -> t.amount() > 100))
    .oneOrMore()
    .until(new IterativeCondition<Txn>() {
        @Override
        public boolean filter(Txn value, Context<Txn> ctx) throws Exception {
            // stop when the running total exceeds a threshold
            double sum = value.amount();
            for (Txn prior : ctx.getEventsForPattern("escalating")) {
                sum += prior.amount();
            }
            return sum > 10_000;
        }
    })
```

That is a stateful predicate over the partial match — something a window cannot
express at all.

## Timeouts: the pattern that did *not* complete

Often the interesting signal is the pattern **failing** to complete. An order placed
and never paid; a session started and never finished.

```java
OutputTag<Timeout> timedOut = new OutputTag<>("timed-out"){};

SingleOutputStreamOperator<Alert> result = CEP
    .pattern(orders.keyBy(Order::id), pattern)
    .process(new PatternProcessFunction<Order, Alert>()
             implements TimedOutPartialMatchHandler<Order> {

        @Override
        public void processMatch(Map<String, List<Order>> m, Context ctx,
                                 Collector<Alert> out) {
            out.collect(Alert.completed(m));
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Order>> m, Context ctx) {
            // the pattern started but never finished within `within`
            ctx.output(timedOut, new Timeout(m.get("placed").get(0)));
        }
    });

result.getSideOutput(timedOut).sinkTo(unpaidOrdersSink);
```

<Callout type="key">

`within` is not just a filter — it is the **state bound** for CEP. Partial matches
are retained until they either complete or expire.

Without `within`, a partial match sits in state forever waiting for a second stage
that may never come. On a high-cardinality key space that is unbounded state growth
with no cleanup path.

Every production CEP pattern needs a `within`.

</Callout>

## After-match skip strategy

Once a match is found, what happens to the events in it?

```java
AfterMatchSkipStrategy.noSkip()            // default — events can start new matches
AfterMatchSkipStrategy.skipToNext()
AfterMatchSkipStrategy.skipPastLastEvent() // no overlapping matches at all
AfterMatchSkipStrategy.skipToFirst("name")
AfterMatchSkipStrategy.skipToLast("name")

Pattern.begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
```

`noSkip` is the default and it means matches **overlap**. Five consecutive failed
logins with a "3 failures" pattern produce three overlapping matches, and therefore
three alerts for one incident.

`skipPastLastEvent` is usually what an alerting use case wants.

## Event time and lateness

CEP operates in event time by default and buffers events until the watermark makes
ordering certain. That has consequences:

- A **stalled watermark** means no pattern ever completes and no timeout ever fires — the CEP equivalent of [Scenario 1](/docs/flink/production/runbook).
- **Late events** are dropped by default; `sideOutputLateData` on the `PatternStream` captures them.
- CEP's latency is bounded below by your out-of-orderness bound, like everything else in event time.

## Should you use CEP at all?

An honest comparison. CEP is elegant and it is not always the right tool.

<Compare>
  <CompareCard title="CEP library" rows={[
    ['Good at', 'Declarative multi-stage sequences with quantifiers'],
    ['Readable', 'Very — the pattern reads like the requirement'],
    ['State control', 'Implicit — you cannot see or tune what it retains'],
    ['Debugging', 'Hard. A pattern that does not match gives you no signal.'],
    ['Dynamic patterns', 'Not supported — patterns are compiled into the job graph'],
    ['Cost', 'Can be high with followedByAny or loose quantifiers'],
  ]} />
  <CompareCard title="KeyedProcessFunction" rows={[
    ['Good at', 'Anything, including everything CEP does'],
    ['Readable', 'Less — the state machine is explicit and verbose'],
    ['State control', 'Total. You decide exactly what is retained and cleared.'],
    ['Debugging', 'Straightforward — log the state transitions'],
    ['Dynamic patterns', 'Yes, via broadcast state'],
    ['Cost', 'Exactly what you wrote'],
  ]} />
</Compare>

<Callout type="prod" title="A practical rule">

Use **CEP** when the pattern is genuinely multi-stage with quantifiers, is stable,
and the readability of the declarative form is worth giving up state control.

Use a **`KeyedProcessFunction`** when the logic is a simple state machine (two or
three stages), when the thresholds must be configurable at runtime, or when state
size is a concern. The fraud detector in
[keyed state](/docs/flink/state/keyed-state) is 40 lines and you can see every byte
it retains.

The most common production regret is choosing CEP for a rule that later needed to be
changed without a redeploy. CEP patterns are compiled into the job graph — you cannot
change them at runtime. If "the fraud team wants to tune the threshold" is a
foreseeable requirement, use broadcast state and a `ProcessFunction` from the start.

</Callout>

## Production use cases

| Domain | Pattern | Notes |
| --- | --- | --- |
| **Security** | N failed logins then a success | Classic; use `skipPastLastEvent` |
| **Fraud** | Small test transaction then a large one | Short `within`; low state |
| **Machine monitoring** | Temperature rising N times consecutively | `consecutive()` + iterative condition |
| **Logistics** | Shipment scanned at A, not scanned at B within 24h | `notFollowedBy` + timeout side output |
| **User journey** | View → cart → checkout, without abandon | Timeout is the interesting output |
| **SLA breach** | Request received, no response within 5s | Pure timeout detection |

Notice how many of them care about the **timeout** rather than the match. That is
usually the signal.

<Expert>

**CEP is an NFA.** The library compiles a pattern into a non-deterministic finite
automaton and stores its state — the partial matches and their computation states —
in Flink keyed state. So it rescales and checkpoints like any other keyed state, and
its size is driven by the number of live partial matches per key.

**`SharedBuffer`** is the data structure holding matched events across partial
matches, with reference counting so an event referenced by several partial matches is
stored once. It is efficient, but `followedByAny` and loose quantifiers still
multiply the *number* of partial matches, which is what actually costs you.

**MATCH_RECOGNIZE.** SQL has the same capability:

```sql
SELECT * FROM logins
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES FIRST(F.event_time) AS first_fail, S.ip AS ip
  PATTERN (F{3} S) WITHIN INTERVAL '1' MINUTE
  DEFINE F AS F.type = 'FAILED', S AS S.type = 'SUCCESS'
) AS m;
```

Same engine underneath, considerably less code, and `WITHIN` is available. Worth
reaching for before the Java API if the rest of your pipeline is SQL.

**Dynamic CEP.** There is no supported way to change a pattern at runtime. Teams that
need it build a rule interpreter over broadcast state instead — which is why the
[dynamic rules project](/docs/flink/projects/dynamic-rules) exists.

</Expert>

<Callout type="remember">

CEP is a regex over a keyed event stream. `next` is stricter than you think;
`followedByAny` is more expensive than you think. `within` is mandatory — it is the
state bound. And the timeout output is often the interesting one.

</Callout>

## Next

**[Table API and SQL](/docs/flink/sql/table-api)** — the declarative half of Flink.
