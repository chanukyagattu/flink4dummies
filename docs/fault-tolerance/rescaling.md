---
title: Rescaling
sidebar_label: Rescaling
description: Changing the parallelism of a stateful job — how key groups make it possible, and what it costs.
---

# Rescaling

<PageMeta level="advanced" time="9 min" prereq={[['Exactly-once', '/docs/flink/fault-tolerance/exactly-once']]} />

<Objectives>

- Explain how keyed and operator state are each redistributed
- Perform a rescale safely and know what to watch afterwards
- Decide whether autoscaling is appropriate for a given job

</Objectives>

## Why this is hard

Rescaling a **stateless** job is trivial: stop it, start more copies, done.

Rescaling a **stateful** job means physically relocating data that is currently
distributed across machines, without losing or duplicating a single key.

```text
BEFORE (parallelism 2)              AFTER (parallelism 4)

subtask 0: keys A,C,E,G             subtask 0: keys A,E
subtask 1: keys B,D,F,H             subtask 1: keys C,G
                                    subtask 2: keys B,F
                                    subtask 3: keys D,H
```

Every key's state must land on exactly one new subtask, and it must be the subtask
that the new hash function will route that key's future records to. Otherwise a
record arrives at a subtask that does not have its history.

## Keyed state: key groups

This is the mechanism, and it is worth understanding rather than trusting.

```text
key ──hash──▶ key group ──contiguous range──▶ subtask
              0 … maxParallelism-1
```

Key groups are the **atomic unit of redistribution**. With `maxParallelism = 128`:

```text
parallelism 2:   subtask 0 → key groups   0–63
                 subtask 1 → key groups  64–127

parallelism 4:   subtask 0 → key groups   0–31
                 subtask 1 → key groups  32–63
                 subtask 2 → key groups  64–95
                 subtask 3 → key groups  96–127
```

Because ranges are contiguous, the new subtask 0 reads a **prefix** of what the old
subtask 0 held. On RocksDB that is a contiguous byte range in the checkpoint files
— a bulk copy rather than a key-by-key rehash.

<KeyByLab />

Set "parallelism now" to 2 and "after rescale" to 4 and expand the arithmetic
table. The highlighted rows are the keys whose state physically moves.

<Callout type="mistake" title="maxParallelism is immutable, and it is the ceiling">

`maxParallelism` is baked into the first checkpoint and **cannot be changed**. It
is also the hard upper bound on parallelism, forever.

```java
env.setMaxParallelism(720);   // before your first production deploy
```

Pick a highly composite number so key groups distribute evenly at many
parallelisms. 720 divides by 1,2,3,4,5,6,8,9,10,12,15,16,18,20,24,30,36,40,45,48,
60,72,80,90,120,144,180,240,360. A value like 1000 divides badly and leaves you
with permanently unbalanced subtasks.

Do not set it absurdly high: every key group carries per-checkpoint metadata, so
32768 key groups on a job that will never exceed parallelism 100 is pure overhead.

If you must change it, the only path is the State Processor API: read the savepoint,
write a new one with a different `maxParallelism`.

</Callout>

## Operator state: two rules

Operator state has no keys, so redistribution is explicit.

```text
ListState (even split)         UnionListState (broadcast)

before: [a,b] [c,d]            before: [a,b] [c,d]
after:  [a] [b] [c] [d]        after:  [a,b,c,d] × 4 subtasks
        ↑ dealt round-robin            ↑ everyone gets everything
```

A Kafka source uses `ListState` for partition assignments: on rescale, the union of
all assigned partitions is redistributed evenly. Scaling from 2 to 4 subtasks
against a 12-partition topic moves from 6 partitions each to 3 each.

Broadcast state is trivially rescalable — every new subtask gets a copy.

## Doing it

```bash
# 1. stop with a savepoint
flink stop --savepointPath s3://bucket/savepoints <jobId>

# 2. restart at the new parallelism
flink run -s s3://bucket/savepoints/savepoint-xyz -p 16 -d my-job.jar
```

### What to watch afterwards

| Signal | Why |
| --- | --- |
| Restore duration | State redistribution is I/O-heavy; a large state can take many minutes |
| Per-subtask record counts | Are the new subtasks evenly loaded, or did you just expose skew? |
| Checkpoint size per subtask | Should be roughly even; a large outlier means an unbalanced key-group range |
| Watermark progress | New subtasks start with no watermark; expect a warm-up |
| Consumer lag | It will spike during the stop and should recover; if it does not, you did not add enough capacity |

<Callout type="prod" title="Scaling up does not always help">

Three cases where adding parallelism changes nothing:

**Source-bound.** A Kafka source cannot exceed the topic's partition count. Going
from 12 to 24 subtasks against 12 partitions gives you 12 idle subtasks — and,
without `withIdleness`, [stalled watermarks](/docs/flink/watermarks/propagation-and-idleness).

**Key skew.** If one key is 40% of traffic, its subtask is 40% of the work no matter
how many subtasks exist. Fix the skew ([two-phase aggregation](/docs/flink/scale/performance)),
not the parallelism.

**Sink-bound.** If the database accepts 25,000 writes/s, the pipeline runs at
25,000/s. More Flink subtasks just means more connections competing for the same
capacity — often making it worse.

Find the bottleneck with the [backpressure lab](/docs/flink/scale/backpressure)
*before* rescaling.

</Callout>

## Autoscaling

The Flink Kubernetes Operator can rescale automatically, using the adaptive
scheduler.

```yaml
job:
  autoscaler:
    enabled: true
    metrics.window: 5m
    target.utilization: 0.7
    scale-up.grace-period: 1m
    scale-down.interval: 30m       # be much more reluctant to scale down
```

It watches consumer lag, busy time and backpressure, then rescales via
savepoint-and-restore. Every scale event is a brief interruption plus a state
redistribution, which is why the scale-down interval should be long.

<Callout type="mistake">

Enabling autoscaling on a job with very large state. Each scale event means a
savepoint, a restore, and a full state redistribution — potentially many minutes of
downtime, repeatedly, whenever traffic wobbles.

Autoscaling suits jobs with small-to-moderate state and genuinely variable load.
For a job with 2 TB of state, provision for peak and leave it alone.

</Callout>

<Expert>

**Rescaling is not free even when it works.** Restore reads and rewrites state.
Scaling from 8 to 16 subtasks with 500 GB of state means every subtask downloads
its new key-group range from object storage. Restore time is dominated by network
throughput, not by CPU.

**Uneven key-group ranges.** If `parallelism` does not divide `maxParallelism`
evenly, some subtasks get one more key group than others. With 128 key groups at
parallelism 12, some subtasks hold 11 key groups and others 10 — a permanent ~10%
imbalance. Another argument for a highly composite `maxParallelism`.

**In-place rescaling.** The adaptive scheduler can change parallelism without an
external savepoint by taking an internal checkpoint and redeploying. This is what
the operator's autoscaler uses, and it is considerably faster than a full
stop-and-start — but it still redistributes state.

**Rescaling changes nothing about `maxParallelism` — including on restore from a
savepoint written at a different one.** If you inherit a job with
`maxParallelism = 128` and need 300 subtasks, the only path is a State Processor
API rewrite. Plan for the maximum you will ever need at design time.

</Expert>

<Callout type="remember">

Key groups make stateful rescaling possible; `maxParallelism` fixes their count
forever. Rescale via stop-with-savepoint. And check whether the bottleneck is the
source, a skewed key, or the sink before assuming parallelism is the answer.

</Callout>

## Next

**[Level 9 — backpressure](/docs/flink/scale/backpressure)** — finding the real bottleneck.
