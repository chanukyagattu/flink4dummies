---
title: Interview questions
sidebar_label: Interview questions
description: Beginner to staff level, each with a short answer, a deep answer, the common misconception, and a production example.
---

# Interview questions

<PageMeta level="intermediate" time="20 min" />

Real questions, in the order interviews tend to go. Each has a **short answer** (what
you say), a **deep answer** (what they follow up with), the **misconception** they
are probing for, and a **production example** to make it concrete.

---

## 🟢 Beginner

### What is Apache Flink?

**Short:** A distributed engine for stateful computation over data streams.

**Deep:** The emphasis belongs on *stateful*. Plenty of things can transform a
stream. What Flink adds is consistent, fault-tolerant, rescalable state over an
unbounded input — plus event-time semantics so results are reproducible.

**Misconception:** "Flink is like Spark but for streaming." Spark Structured
Streaming is micro-batch; Flink is event-at-a-time. That difference shows up in
latency floors and in how naturally per-key timers work.

---

### What does `keyBy` do?

**Short:** It partitions the stream so all records with the same key go to the same
subtask.

**Deep:** It computes `murmurHash(key.hashCode()) % maxParallelism` to get a key
group, then maps that key group to a subtask. It forces a network shuffle and breaks
operator chaining. It is also the precondition for keyed state: without it, keyed
state throws at runtime.

**Misconception:** That it is just a `GROUP BY`. It is a physical data-movement
operation with real serialisation and network cost.

**Production:** A `keyBy` on a low-cardinality field with a dominant value creates
skew that no amount of parallelism fixes.

---

### What is parallelism?

**Short:** How many parallel subtasks an operator runs as.

**Deep:** It is not the number of machines. Each subtask is a thread with its own
slice of state. It is bounded above by `maxParallelism`, and for sources by the
input's partition count.

**Misconception:** "More parallelism means faster." Only if the bottleneck is
CPU-bound. If the operator is blocked on a database, 32 blocked threads are no better
than 8.

---

## 🟡 Intermediate

### What is a watermark?

**Short:** A claim, flowing with the stream, that event time has progressed to T —
meaning Flink believes it has seen everything up to T.

**Deep:** For the standard generator, `watermark = maxTimestampSeen - bound - 1ms`.
It is emitted on a 200ms timer, not per record. An operator's watermark is the
**minimum** across all its inputs. It fires windows, fires event-time timers, cleans
up joins, and defines what "late" means.

**Misconception:** That it is a guarantee. It is a heuristic — a tunable trade-off
between latency and completeness. Records can and do arrive behind it.

**Production:** One Kafka partition with no traffic contributes `Long.MIN_VALUE` to
the minimum and freezes every window in the job, with no error. `withIdleness` is the
fix.

---

### Why do we need event time?

**Short:** Because processing time is not reproducible.

**Deep:** Run the same input twice with processing-time windows and you get different
results, because "now" differed. Worse, during recovery a job reprocesses at enormous
speed, so a crash silently changes the output. Event time depends only on the data,
so the results are identical live, on replay, and on a faster cluster next year.

**Misconception:** That event time is always right. "Alert if a device is silent for
5 minutes" is genuinely a processing-time question — silence has no event time.

---

### How do checkpoints work?

**Short:** The coordinator injects a barrier at the sources; it flows with the data;
each operator snapshots when the barrier reaches it.

**Deep:** Barriers define a **cut** in the stream, not a moment in time. Each
operator aligns barriers across its inputs, takes a fast synchronous snapshot and a
slow asynchronous upload, forwards the barrier, and acknowledges. When every task has
acknowledged, the coordinator writes metadata and calls `notifyCheckpointComplete` —
which is when 2PC sinks commit.

**Misconception:** That the job pauses. Nothing pauses globally; that is the entire
point of the barrier design.

---

### What is the difference between keyed and operator state?

**Short:** Keyed state is scoped to a key; operator state is scoped to a subtask.

**Deep:** Keyed state redistributes automatically by key group and can be terabytes.
Operator state redistributes by an explicit rule — even split (`ListState`) or full
replication (`UnionListState`) — and should be small. You write keyed state
constantly; operator state mostly appears in connectors.

---

## 🔴 Advanced

### How does barrier alignment work, and why does it hurt under backpressure?

**Short:** An operator blocks each input that has delivered the barrier until every
input has, so its snapshot contains exactly the pre-barrier prefix of all inputs.

**Deep:** Without alignment, post-barrier records from a fast channel would be folded
into a snapshot they do not belong in, and would be counted twice on restore. Under
backpressure, barriers move slowly through congested channels, so alignment can take
minutes. That lengthens checkpoints, which times them out, which triggers restarts —
a feedback loop.

Unaligned checkpoints break the loop by forwarding the barrier immediately and
snapshotting the in-flight buffers instead. Checkpoint duration becomes independent
of backpressure, at the cost of size and slower recovery.

**Misconception:** That unaligned checkpoints fix backpressure. They fix the
*checkpoint*; the bottleneck is still there.

---

### Why can one Kafka partition hold back the whole job's watermark?

**Short:** An operator's watermark is the minimum across all inputs, and after a
shuffle every downstream subtask has a channel from every upstream subtask.

**Deep:** So the slowest or quietest split sets the pace for everything downstream of
the first shuffle. A partition with no traffic never emits a watermark at all,
contributing `Long.MIN_VALUE`. The job stays RUNNING, consumes records, checkpoints
fine, and emits nothing — while state grows because no window ever purges.

`withIdleness` excludes silent splits from the minimum.
`withWatermarkAlignment` solves the opposite problem, where one split races ahead
during a backfill and forces everything else to be buffered.

**Production:** This is the most common Flink support ticket that exists.

---

### How does Flink redistribute keyed state during rescaling?

**Short:** By key group. Each subtask owns a contiguous range of key groups, and
rescaling reassigns ranges.

**Deep:**

```text
keyGroup = murmurHash(key.hashCode()) % maxParallelism
subtask  = keyGroup * parallelism / maxParallelism
```

Key groups exist precisely so state can move in bulk: ranges are contiguous, so on
RocksDB a subtask reads a contiguous byte range from the checkpoint rather than
rehashing individual keys.

The consequence people miss: `maxParallelism` is fixed at the first checkpoint and
**cannot be changed**. It is also the permanent ceiling on parallelism. The only
escape is rewriting the savepoint with the State Processor API.

**Misconception:** That rescaling is free. It is a full state redistribution, which
for large state is minutes of I/O.

---

### What does exactly-once actually guarantee?

**Short:** That state reflects each record exactly once — not that each record is
processed once.

**Deep:** Records after the last checkpoint are genuinely reprocessed on recovery;
that *is* the mechanism. Correctness comes from state being rewound to the matching
point, so reprocessing converges to the same state.

End-to-end requires more: a replayable source, deterministic logic, and a sink that
either uses two-phase commit or is idempotent. With 2PC, data is invisible until the
covering checkpoint completes — so the checkpoint interval becomes your end-to-end
latency.

**Misconception:** That `EXACTLY_ONCE` in the config gives you exactly-once rows in
Postgres. It does not. The sink has to participate.

---

## ⚫ Expert / Staff

### A checkpoint suddenly grew from 2 GB to 500 GB. Walk me through it.

```text
1. Confirm it is state, not in-flight data.
   Unaligned checkpoints include network buffers — did they get enabled,
   or did backpressure spike?

2. UI → Checkpoints → per-operator size. One operator will dominate.

3. Is it one SUBTASK or all of them?
   One subtask → key skew, or a hot key with unbounded state.
   All subtasks → a systemic change.

4. Then, by operator type:
   Window operator     → is the watermark advancing? A stalled watermark means
                         windows never purge. Check currentOutputWatermark.
   Join operator       → did someone widen an interval, or add a regular join?
   ProcessFunction     → is there a clear() path for every update()?
   Any                 → did the key space change? A new client version emitting
                         a UUID per request turns a bounded key space into an
                         unbounded one overnight.

5. Correlate with deploys — including UPSTREAM deploys. A producer change
   is the most common cause and the least likely to be in your change log.

6. Confirm with evidence: take a savepoint, read it with the State Processor
   API, count keys and inspect a sample. Do not guess.
```

**What they are testing:** whether you reason systematically or start changing
settings.

---

### How would you design a Flink job with 10 TB of keyed state?

```text
STATE BACKEND
  RocksDB, incremental checkpoints, local NVMe (never network storage),
  managed memory fraction 0.5–0.6, local recovery enabled.

PARTITIONING
  maxParallelism set high but composite (e.g. 1440), parallelism sized so
  each subtask holds 50–100 GB. Check key-group balance explicitly.

CHECKPOINTING
  Long interval (5 min+) — checkpoint cost dominates otherwise.
  Unaligned with a 30s aligned timeout.
  Consider the changelog state backend if duration VARIANCE is the problem.

RECOVERY
  This is the hard part, and it is what separates a good answer from a great
  one. 10 TB must be DOWNLOADED before processing resumes. Measure restore
  time explicitly — it is your real RTO, and it is usually far worse than
  checkpoint time. Local recovery, region failover, and a warm node pool all
  matter more than checkpoint tuning here.

STATE HYGIENE
  TTL on everything. Timers for precise cleanup. Audit that the key space is
  genuinely bounded — at 10 TB, an unbounded key space is not survivable.

RESCALING
  Avoid it. Every rescale is a 10 TB redistribution. Provision for peak.
  Autoscaling is inappropriate at this size.

SERIALISATION
  disableGenericTypes in CI. At 10 TB, a Kryo fallback on a hot type is a
  multi-terabyte and multi-hour tax.
```

---

### How would you handle extreme key skew?

**Short:** Two-phase aggregation with a salted key.

**Deep:**

```java
// phase 1: spread the hot key over N subtasks
.map(o -> new Keyed(o.country() + "#" + rand(64), o))
.keyBy(Keyed::saltedKey).window(...).aggregate(new PartialSum())
// phase 2: strip the salt, combine the small partials
.map(p -> p.withKey(p.key().split("#")[0]))
.keyBy(Total::country).window(...).aggregate(new CombineSum())
```

It works because phase 2 handles 64 records per key per window instead of millions.
It requires the aggregation to be associative and commutative — fine for sum, count,
min, max and sketches; not possible for "first record" or an exact median without
approximation.

Alternatives worth mentioning: fix the producer's partitioning if the skew originates
upstream; use a local pre-aggregation (`MiniBatch` in SQL) to reduce shuffle volume;
or, if one key is pathological, route it to a dedicated job.

**Misconception:** That more parallelism helps. The hot key still hashes to one
subtask.

---

### How do you safely upgrade a stateful production job?

```text
1. Every stateful operator has an explicit, unchanged uid().
2. flink stop --savepointPath s3://...     (never flink cancel)
3. Record the savepoint path.
4. Restore in STAGING against a production-shaped savepoint first.
   A restore that works on empty state proves nothing.
5. flink run -s <savepoint> new.jar
6. Watch: restore duration, watermark progress, first successful checkpoint,
   output rate, consumer lag.
7. Keep the savepoint until the new version has been healthy for a full
   retention period. Rollback = start the OLD jar from the SAME savepoint.
```

Compatibility rules to state: you can add and remove fields on POJO/Avro state; you
cannot rename or retype them; you cannot evolve Kryo state at all; and
`--allowNonRestoredState` should be used only when you know exactly which state you
are discarding.

**What they are testing:** whether you have done this, or only read about it. The
tell is whether you mention the rollback path.

---

### How do you reason about event-time correctness across multiple inputs?

**Short:** The watermark is the minimum across inputs, so correctness is bounded by
the slowest input — and completeness is a policy, not a fact.

**Deep:** For a two-input operator, the operator's notion of "now" is
`min(wm_left, wm_right)`. That has three consequences worth naming:

1. A low-volume side with a stalled watermark freezes cleanup for the high-volume side, so state grows on the busy stream because of the quiet one.
2. During a backfill the two sides diverge wildly, so you need `withWatermarkAlignment` in a shared group to keep in-flight buffering bounded.
3. "Correct" means "correct given the bound you chose". You should be able to state your bound, why you chose it (measured p99 lateness), what you do with the tail (side output), and how you would know if it changed (a lag histogram plus an alert).

**What they are testing:** whether you treat completeness as an engineering decision
with evidence behind it, rather than as something the framework handles.

---

<Callout type="prod" title="How to prepare">

For each of these, prepare three things:

1. **The one-sentence answer** — for the first pass
2. **The mechanism** — for the follow-up
3. **A time you saw it in production** — this is what actually distinguishes candidates

If you have not seen it in production, the labs on the
[watermark](/docs/flink/watermarks/what-is-a-watermark),
[keyBy](/docs/flink/basics/parallelism-and-subtasks) and
[checkpoint](/docs/flink/fault-tolerance/checkpoints) pages let you honestly say
"I have watched this happen and here is what it looks like".

</Callout>

## Related

- [Confusions](/docs/flink/reference/confusions) — the fifteen pairs interviewers probe
- [Cheat sheets](/docs/flink/reference/cheat-sheets) — for the day before
- [How Flink really works](/docs/flink/internals/how-flink-really-works) — if you can narrate this, you can answer anything above
