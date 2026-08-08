---
title: Keyed state
sidebar_label: Keyed state
description: The five keyed state types, when to use each, and the key-group indirection that makes rescaling possible.
---

# Keyed state

<PageMeta level="intermediate" time="12 min" prereq={[['Why state?', '/docs/flink/state/why-state']]} />

<Objectives>

- Choose the right state primitive, especially `MapState` over `ValueState` of a map
- Explain key groups and why `maxParallelism` is irreversible
- Write a `KeyedProcessFunction` that does not leak state

</Objectives>

## The five types

All of them are scoped to the current key automatically.

```java
// 1. ValueState — a single value
ValueState<Long> count = getRuntimeContext().getState(
    new ValueStateDescriptor<>("count", Long.class));
count.value(); count.update(5L); count.clear();

// 2. ListState — an append-only list
ListState<Event> buffer = getRuntimeContext().getListState(
    new ListStateDescriptor<>("buffer", Event.class));
buffer.add(e); buffer.get(); buffer.update(newList); buffer.clear();

// 3. MapState — a map, with per-entry access
MapState<String, Long> perItem = getRuntimeContext().getMapState(
    new MapStateDescriptor<>("per-item", String.class, Long.class));
perItem.put("a", 1L); perItem.get("a"); perItem.remove("a");
perItem.entries(); perItem.keys(); perItem.contains("a");

// 4. ReducingState — auto-combined on add, same type in and out
ReducingState<Long> sum = getRuntimeContext().getReducingState(
    new ReducingStateDescriptor<>("sum", Long::sum, Long.class));
sum.add(5L);        // combined immediately
sum.get();

// 5. AggregatingState — auto-combined, different input and output types
AggregatingState<Order, Double> avg = getRuntimeContext().getAggregatingState(
    new AggregatingStateDescriptor<>("avg", new AvgFunction(), TypeInformation.of(...)));
```

## The choice that matters most

<Callout type="key">

**Never store a collection inside `ValueState`. Use `MapState` or `ListState`.**

```java
// ❌ Every access serialises and deserialises the ENTIRE map
ValueState<HashMap<String, Long>> bad;
HashMap<String, Long> m = bad.value();   // deserialise 10,000 entries
m.merge(item, 1L, Long::sum);            // change one
bad.update(m);                           // serialise 10,000 entries

// ✅ Touches one entry
MapState<String, Long> good;
good.put(item, good.get(item) == null ? 1L : good.get(item) + 1);
```

On RocksDB the difference is not marginal. Each entry of a `MapState` is a
separate key in the underlying store, so a read is one lookup. With
`ValueState<HashMap>`, every single access serialises the whole collection —
turning an O(1) operation into O(n) and generating enormous GC pressure.

This one substitution has rescued more Flink jobs than any other single change.

</Callout>

The same applies to `ListState` vs `ValueState<List>`: `ListState.add()` appends
without reading the existing list, which on RocksDB is a merge operator rather
than a read-modify-write.

## A complete, correct example

A fraud detector: alert when a small transaction is immediately followed by a
large one within a minute. It demonstrates state, timers, and — importantly —
cleanup.

```java
public class FraudDetector extends KeyedProcessFunction<String, Txn, Alert> {

    private static final double SMALL = 1.00;
    private static final double LARGE = 500.00;

    private transient ValueState<Boolean> sawSmall;
    private transient ValueState<Long> timerTs;

    @Override
    public void open(OpenContext ctx) {
        sawSmall = getRuntimeContext().getState(
            new ValueStateDescriptor<>("saw-small", Types.BOOLEAN));
        timerTs = getRuntimeContext().getState(
            new ValueStateDescriptor<>("timer", Types.LONG));
    }

    @Override
    public void processElement(Txn txn, Context ctx, Collector<Alert> out)
            throws Exception {

        if (Boolean.TRUE.equals(sawSmall.value())) {
            if (txn.amount() > LARGE) {
                out.collect(new Alert(txn.accountId(), txn.amount()));
            }
            // pattern resolved either way: clean up NOW, do not wait for the timer
            cleanUp(ctx);
        }

        if (txn.amount() < SMALL) {
            sawSmall.update(true);
            long t = ctx.timerService().currentProcessingTime() + 60_000;
            ctx.timerService().registerProcessingTimeTimer(t);
            timerTs.update(t);
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Alert> out)
            throws Exception {
        // a minute passed with no large transaction — forget it
        timerTs.clear();
        sawSmall.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long t = timerTs.value();
        if (t != null) ctx.timerService().deleteProcessingTimeTimer(t);
        timerTs.clear();
        sawSmall.clear();     // ← without this line, state grows forever
    }
}
```

<Callout type="mistake" title="The state leak that kills jobs three months in">

Every `state.update()` must have a corresponding path to `state.clear()`.

In the example above, delete the `sawSmall.clear()` in `cleanUp` and the job works
perfectly. For a while. Then, one entry per account that ever made a small
transaction accumulates forever, and six weeks later checkpoints take 20 minutes
and the job starts failing.

Three defences, in order of reliability:

1. **Clear explicitly** when the logic is finished with a key — as above
2. **Register a timer** to clear state after a period of inactivity
3. **Configure [state TTL](/docs/flink/state/ttl-and-growth)** as a backstop

Use all three. TTL is a safety net, not a design.

</Callout>

## Key groups: how rescaling is possible

Flink does not map a key directly to a subtask. There is an indirection, and it is
the reason stateful rescaling works at all.

```text
key ──murmurHash(key.hashCode())──▶ key group ──range assignment──▶ subtask
                                    0 … maxParallelism-1
```

```java
keyGroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
subtask  = keyGroup * parallelism / maxParallelism;
```

Key groups are the **atomic unit of state redistribution**. Each subtask owns a
contiguous *range* of them, so rescaling moves whole ranges in bulk instead of
rehashing every key individually.

<KeyByLab />

Change parallelism in the lab and watch the highlighted rows — those are the keys
whose state must physically move during a rescale.

<Callout type="mistake" title="maxParallelism is forever">

`maxParallelism` is fixed at the **first checkpoint** and **cannot be changed**
without discarding all state. Change it and your savepoint will not restore.

```java
env.setMaxParallelism(720);   // set this before your first production deploy
```

Why 720? It divides evenly by 1,2,3,4,5,6,8,9,10,12,15,16,18,20,24,30,36,40,45,48,
60,72,80,90,120,144,180,240,360 — so key groups distribute evenly at almost any
parallelism you might choose. A value like 1000 divides badly and gives you
permanently uneven subtasks.

Do not set it enormously high "to be safe": every key group carries metadata in
every checkpoint, so 32768 key groups adds real overhead for a job that will never
exceed parallelism 100.

Rules of thumb: set it explicitly, make it at least 4× your expected maximum
parallelism, and pick a highly composite number.

</Callout>

## State and the key: three rules

```java
// 1. Keyed state ONLY works after keyBy. Otherwise:
//    java.lang.IllegalStateException: Keyed state can only be used on a 'keyed stream'
stream.keyBy(Click::page).process(new MyKeyedFunction());

// 2. You cannot read another key's state. Ever. There is no API for it,
//    because that key may live on a different machine.

// 3. Never mutate the key. Flink caches state handles per key; changing a
//    key object after keyBy corrupts state lookups in ways that are extremely
//    hard to debug. Keys must be immutable and have stable hashCode/equals.
```

That third rule deserves emphasis: your key type must have a **deterministic,
stable `hashCode()`**. Arrays do not (identity hash). Enums do. Records and
Strings do. A POJO with a default `hashCode` does not — and it will work fine on
one machine and fail mysteriously across a shuffle.

<Callout type="hood" title="What a state access actually does">

On the **heap** backend (`HashMapStateBackend`): state is a nested Java map,
`Map<KeyGroup, Map<Key, Map<Namespace, Value>>>`. Access is a hash lookup and a
reference return. No serialisation. Very fast, bounded by heap.

On **RocksDB** (`EmbeddedRocksDBStateBackend`): each state entry is a key-value
pair in an embedded LSM store on local disk. The composite key is
`keyGroup | key | namespace | stateName`. Every read deserialises, every write
serialises. Roughly an order of magnitude slower per access, but state can far
exceed memory.

The `namespace` component is how windows are stored — the window object is the
namespace. It is also why a window's state and a `ProcessFunction`'s state for the
same key are cleanly separated.

</Callout>

<Expert>

**State handles are cached per key.** `ValueState` objects returned in `open()` are
long-lived; Flink swaps the underlying key context before each `processElement`.
This is why you must fetch descriptors in `open()` and never per record — fetching
per record allocates and defeats the cache.

**Key group ranges must be contiguous.** `KeyGroupRange` is a start/end pair, which
is what makes restore efficient: on RocksDB, restoring a subtask means reading a
contiguous byte range from the checkpoint rather than filtering individual keys.
It also means key groups cannot be assigned to balance load — you cannot manually
move a hot key group off a busy subtask.

**Reading state offline.** The **State Processor API** lets you load a savepoint as
a `DataStream`, inspect it, modify it, and write a new savepoint. This is the
supported way to: audit what is actually in state, bootstrap a new job with
historical state, or surgically fix corrupted entries. It is the tool people wish
they had known about during an incident.

</Expert>

<Callout type="remember">

`MapState` over `ValueState` of a map — always. Every `update()` needs a matching
`clear()` path. Key groups make rescaling possible, and `maxParallelism` is
irreversible, so set it deliberately before your first production checkpoint.

</Callout>

## Next

**[Operator and broadcast state](/docs/flink/state/operator-and-broadcast-state)** — state that is not keyed.
