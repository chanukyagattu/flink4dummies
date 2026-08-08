---
title: Performance
sidebar_label: Performance
description: The seven changes that actually move throughput, in the order you should try them — and the skew fix everyone needs eventually.
---

# Performance

<PageMeta level="advanced" time="11 min" prereq={[['Async I/O', '/docs/flink/scale/async-io']]} />

<Objectives>

- Work through performance problems in order of expected payoff
- Implement two-phase aggregation to defeat key skew
- Recognise the settings that are folklore rather than tuning

</Objectives>

## Measure first

```text
1. Find the bottleneck operator      → Backpressure tab
2. Determine if it is CPU-bound      → busy% high AND CPU high?
                                       or busy% high and CPU LOW (= waiting)?
3. Then change ONE thing
4. Measure again
```

Every hour spent tuning the wrong operator is an hour wasted. The
[backpressure page](/docs/flink/scale/backpressure) is step 1, and it is not
optional.

## The changes, ordered by payoff

### 1. Filter and project early

```java
// before the shuffle, before the state, before everything
stream.filter(this::isRelevant)
      .map(o -> new Slim(o.id(), o.amount()))    // drop 90% of the bytes
      .keyBy(Slim::id)
      .process(...);
```

Every record you drop is one you do not serialise, do not send over the network, do
not store, and do not checkpoint. Frequently the single biggest win available, and
frequently overlooked because it feels too simple.

### 2. Fix your serialisers

A type falling back to Kryo can cost 4–10× on every shuffle and every state access.

```java
env.getConfig().disableGenericTypes();   // turn a silent cost into a build failure
```

See [serialization](/docs/flink/state/serialization-and-evolution). This is a
one-line change with double-digit percentage payoffs and is almost always available.

### 3. Use `MapState`, not `ValueState` of a collection

```java
// ❌ deserialises the whole map on every access
ValueState<HashMap<String, Long>> perItem;

// ✅ one entry per access
MapState<String, Long> perItem;
```

On RocksDB this converts an O(n) operation into O(1). See
[keyed state](/docs/flink/state/keyed-state).

### 4. Aggregate incrementally

```java
// ❌ buffers every record in the window
.process(new MyWindowFunction())

// ✅ one accumulator per window
.aggregate(new MyAggregate(), new AddWindowMetadata())
```

Orders of magnitude of state difference. See
[window functions](/docs/flink/windows/window-functions).

### 5. Put RocksDB on fast local disk

```yaml
state.backend.rocksdb.localdir: /mnt/nvme/rocksdb
taskmanager.memory.managed.fraction: 0.5
```

RocksDB on a network-attached volume is dramatically slower than on local NVMe.
On Kubernetes that means instance storage, not an EBS volume. This is an
infrastructure decision that frequently outweighs every code change on this list.

### 6. Do not block the mailbox

Any synchronous external call in an operator caps throughput at
`1 / latency` records per second and lengthens checkpoint alignment. Move it to
[Async I/O](/docs/flink/scale/async-io).

### 7. Then, and only then, add parallelism

Once none of the above applies and the operator is genuinely CPU-bound, more
subtasks help. Before that, they do not.

## Key skew: the problem parallelism cannot solve

```text
keyBy(country)

US:     40% of traffic  →  subtask 3 is on fire
GB:      8%             →  subtask 7 is fine
… 200 other countries    →  everyone else is idle
```

Adding parallelism does nothing: `US` still hashes to exactly one subtask.

### The fix: two-phase aggregation

Split the hot key artificially, aggregate the pieces in parallel, then combine the
partial results.

```java
// PHASE 1 — salt the key so one logical key spreads across N subtasks
DataStream<Partial> partials = orders
    .map(o -> new Keyed(o.country() + "#" + ThreadLocalRandom.current().nextInt(64), o))
    .keyBy(Keyed::saltedKey)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new PartialSum());     // 64 parallel partial sums per country

// PHASE 2 — strip the salt and combine the (much smaller) partials
DataStream<Total> totals = partials
    .map(p -> p.withKey(p.key().split("#")[0]))
    .keyBy(Total::country)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new CombineSum());     // 64 records per country per minute
```

```text
Phase 1: 40% of traffic spread over 64 subtasks — balanced
Phase 2: 64 partial results per country per minute — trivial volume
```

<Callout type="key">

Two-phase aggregation is the standard answer to skew, and it works because **phase 2
handles far less data than phase 1**. You are trading one shuffle for balance.

It only works for aggregations that are **associative and commutative** — sum,
count, min, max, and any sketch. It does not work for "the first record" or
"the median" without an approximation.

</Callout>

In SQL, this is the `MiniBatch` plus split-aggregation optimisation, and it is
configuration rather than code:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'table.optimizer.distinct-agg.split.enabled' = 'true';
```

Mini-batch alone often gives a large improvement on high-throughput SQL
aggregations: it buffers records and does one state read-modify-write per batch
instead of per record. The cost is exactly `allow-latency` of extra latency.

## Detecting skew

| Signal | Where |
| --- | --- |
| One subtask's `numRecordsIn` far above its siblings | UI → operator → Subtasks |
| One subtask's checkpoint state size far above siblings | UI → Checkpoints → per subtask |
| One subtask busy, siblings idle | Backpressure tab |
| Consumer lag concentrated on one partition | Kafka metrics |

That last one is important: if the *producer's* partitioning is skewed, the skew
exists before Flink sees it, and no Flink-side change fixes it. That is an upstream
conversation.

## Memory

```yaml
taskmanager.memory.process.size: 8g
taskmanager.memory.managed.fraction: 0.4       # RocksDB, sorting, hashing
taskmanager.memory.network.fraction: 0.1       # buffers
taskmanager.memory.jvm-overhead.fraction: 0.1
```

Rough guidance:

- **RocksDB state** → raise managed memory (0.5–0.6). It is the block cache.
- **Heap state** → lower managed memory, raise task heap.
- **High parallelism / many shuffles** → raise the network fraction if you see `InsufficientBuffers` errors.

<Callout type="mistake" title="Settings that are folklore">

Things people change that rarely help, and sometimes hurt:

- **Dozens of individual RocksDB options.** Use `PredefinedOptions` unless you have profiled RocksDB specifically.
- **`taskmanager.numberOfTaskSlots` set very high.** Slots share CPU and share managed memory; more slots means less memory per RocksDB instance.
- **Disabling operator chaining in production.** It removes the single largest performance feature in the runtime.
- **Very large network buffers.** Increases in-flight data, which lengthens checkpoint alignment. Buffer *debloating* is usually what you actually want.
- **Raising `maxConcurrentCheckpoints`.** Multiplies memory pressure and usually makes slow checkpoints slower.

</Callout>

<Callout type="prod" title="A realistic tuning session">

```text
Symptom:  throughput plateaued at 40k/s, target is 150k/s

1. Backpressure tab → the KeyedProcess operator is busy, everything upstream is red
2. CPU on those TaskManagers → 25%. Busy but not computing: it is WAITING.
3. Look at the code → a ValueState<HashMap> with ~5,000 entries per key
4. Change to MapState                                    → 40k → 95k/s
5. Backpressure moves to the sink                        → next bottleneck
6. Sink is a JDBC upsert, one row per statement          → batch to 500 → 95k → 160k/s
7. Target met. Stop.
```

Two lessons: the bottleneck **moves** after every fix, so re-measure each time; and
neither change involved adding a single machine.

</Callout>

<Expert>

**Object reuse.** `env.getConfig().enableObjectReuse()` lets operators reuse record
objects instead of allocating fresh ones, which meaningfully reduces GC pressure in
long chains. It is only safe if no operator retains a reference to a record after
processing it. Retaining one produces silent data corruption, which is why it is off
by default.

**Latency tracking is a debugging tool, not a production setting.**
`metrics.latency.interval` injects latency markers through the topology. It is
genuinely useful for finding where latency accumulates, and it is expensive. Turn it
on to investigate, then turn it off.

**Credit-based flow control and buffer count.** `taskmanager.network.memory.buffers-per-channel`
and `floating-buffers-per-gate` control how much data can be in flight. More buffers
means better throughput under bursty load and longer checkpoint alignment. Buffer
debloating adjusts this dynamically and is the better default.

**Profile, do not guess.** For a genuinely CPU-bound operator, attach an async
profiler to a TaskManager and look at the flame graph. It is common for the hot
frames to be serialisation or `hashCode`, not your business logic — which points at a
completely different fix than "add parallelism".

</Expert>

<Callout type="remember">

Measure, change one thing, measure again. Filter early, fix serialisers, use
`MapState`, aggregate incrementally, fast local disk, never block. Two-phase
aggregation for skew. Parallelism last.

</Callout>

## Next

**[Level 10 — deployment](/docs/flink/production/deployment)** — running this for real.
