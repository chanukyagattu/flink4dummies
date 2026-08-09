---
title: Why state?
sidebar_label: Why state?
description: State is the memory Flink keeps between events — and the reason checkpoints, key groups and rescaling all exist.
---

# Why state?

<PageMeta level="intermediate" time="7 min" prereq={[['Triggers & lateness', '/docs/flink/windows/triggers-and-lateness']]} docs="docs/concepts/stateful-stream-processing/" />

<Objectives>

- Explain why a `HashMap` in your function is not state
- Name the guarantees Flink-managed state provides that raw memory does not
- Distinguish keyed state from operator state on ownership grounds

</Objectives>

## The difference in one picture

```text
STATELESS                          STATEFUL

event ──▶ f(event) ──▶ output      event ──▶ f(event, state) ──▶ output
                                                  │    ▲
                                                  └────┘
                                              new state
```

Stateless: the output depends only on this event. `map`, `filter`, most parsing.

Stateful: the output depends on this event **and everything that came before it**.
Counting, averaging, deduplication, joins, windows, sessions, fraud detection —
essentially everything interesting.

## Why not just use a HashMap?

```java
public class Counter extends RichMapFunction<Click, Long> {
    private Map<String, Long> counts = new HashMap<>();   // ← looks fine

    public Long map(Click c) {
        return counts.merge(c.page(), 1L, Long::sum);
    }
}
```

This works. It also has four defects, and each one is fatal in production.

**1. It vanishes on restart.** TaskManager restarts, heap is gone, every count
returns to zero. Nothing logs it. Your dashboard just quietly halves.

**2. It cannot be rescaled.** Increase parallelism from 4 to 8 and Flink has no
idea this map exists, so it cannot split it. The new subtasks start empty; the old
data is lost.

**3. It grows without bound.** No TTL, no eviction, no visibility. One entry per
page forever, including the ones you saw once in 2024.

**4. It is invisible.** Not in the checkpoint size metric, not in the UI, not in
any dashboard. You find out it exists when the JVM dies.

<Callout type="key">

Flink-managed state is the same data structure with four guarantees bolted on:

| Guarantee | What it buys you |
| --- | --- |
| **Checkpointed** | Survives crashes — restored exactly as of a consistent point |
| **Partitioned** | Split across subtasks by key, automatically |
| **Redistributable** | Moves correctly when you change parallelism |
| **Observable** | Shows up in checkpoint size, in the UI, and in your alerts |

</Callout>

The same counter, done properly:

```java
public class Counter extends RichMapFunction<Click, Long> {

    private transient ValueState<Long> count;   // transient: not serialised with the function

    @Override
    public void open(OpenContext ctx) {
        count = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
    }

    @Override
    public Long map(Click c) throws Exception {
        Long current = count.value();           // null on first access — always check
        long next = (current == null ? 0 : current) + 1;
        count.update(next);
        return next;
    }
}
```

Note there is no key in that code. `ValueState` is **automatically scoped to the
current key** — Flink sets the key context before calling your function. Which is
why this only works after a `keyBy`.

## Two kinds of state

<Compare>
  <CompareCard title="Keyed state" rows={[
    ['Scoped to', 'One key — automatically, invisibly'],
    ['Requires', 'A keyBy before it'],
    ['Types', 'ValueState, ListState, MapState, ReducingState, AggregatingState'],
    ['Rescaling', 'Redistributed by key group — exact and automatic'],
    ['Size', 'Can be enormous — terabytes, on RocksDB'],
    ['Use for', '95% of everything: counts, sessions, joins, dedup, windows'],
  ]} />
  <CompareCard title="Operator state" rows={[
    ['Scoped to', 'One operator subtask — not to any key'],
    ['Requires', 'Nothing'],
    ['Types', 'ListState, UnionListState, BroadcastState'],
    ['Rescaling', 'Redistributed by an explicit rule you choose'],
    ['Size', 'Should be small — it is not designed for volume'],
    ['Use for', 'Source offsets, sink buffers, broadcast config'],
  ]} />
</Compare>

You will write keyed state constantly and operator state almost never — unless you
are writing a connector, in which case it is the other way round.

## The mental model

<Callout type="mental">

Keyed state is **a giant distributed map**:

```text
(operator, key, stateName) ──▶ value
```

Flink owns that map. It decides which subtask holds which keys, persists it on
every checkpoint, and reshuffles it when parallelism changes.

Your function never sees the map. It just says `count.value()` and Flink returns
the value for whatever key is currently being processed. The key is ambient
context, like a thread-local — which is exactly why forgetting the `keyBy` gives
you a runtime error rather than a compile error.

</Callout>

## What state costs you

Every guarantee has a price, and being aware of it early prevents unpleasant
surprises.

| Cost | Detail |
| --- | --- |
| **Checkpoint size and duration** | State must be written durably on every checkpoint. 500 GB of state means 500 GB to snapshot (less with incremental checkpoints). |
| **Recovery time** | On restore, state must be *downloaded* before processing resumes. This is often much slower than the checkpoint that wrote it. |
| **Rescaling time** | Changing parallelism means physically redistributing state. |
| **Serialisation on every access** | With RocksDB, each read is a deserialise and each write a serialise. Your serialiser choice becomes a throughput factor. |
| **Memory or disk** | Heap state competes with everything else in the JVM; RocksDB competes for managed memory and local disk. |

<Callout type="prod">

Treat state size as a first-class SLI. Alert on:

- `lastCheckpointSize` growing steadily over days — something is not being cleaned up
- `lastCheckpointDuration` approaching your checkpoint interval — you are about to start overlapping checkpoints
- State size per key trending upward — a specific key is accumulating

Almost every "the job was fine for three months and then died" story is unbounded
state growth that nobody was watching.

</Callout>

<Callout type="mistake">

Storing things in state that do not need to be there: raw payloads you already
extracted a field from, full JSON when you need one number, historical records
"just in case".

State is the most expensive resource in a Flink job. Every byte is checkpointed,
transferred, restored and redistributed. Store the minimum that makes your
computation correct.

</Callout>

<Callout type="remember">

State is memory that Flink owns: checkpointed, partitioned, redistributable,
observable. A HashMap in your function has none of those properties. Keyed state
for almost everything; operator state for connectors.

</Callout>

## Next

**[Keyed state](/docs/flink/state/keyed-state)** — the five types, and key groups.
