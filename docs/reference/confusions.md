---
title: Flink confusions
sidebar_label: Confusions
description: Fifteen pairs of concepts that people mix up, side by side, with the distinction that actually matters.
---

# Flink confusions

<PageMeta level="intermediate" time="10 min" />

A quick-reference sheet. Each card pair is one distinction that causes real bugs.

---

## Event time vs Processing time

<Compare>
  <CompareCard title="Event time" rows={[
    ['Is', 'When the thing happened, in the world'],
    ['Set by', 'The producer, inside the record'],
    ['Reproducible', 'Yes — forever, on any cluster'],
    ['Advances via', 'Watermarks'],
    ['Use for', 'Aggregation, billing, joins, anything audited'],
  ]} />
  <CompareCard title="Processing time" rows={[
    ['Is', 'What the machine clock says right now'],
    ['Set by', 'Nobody — System.currentTimeMillis()'],
    ['Reproducible', 'No. Never.'],
    ['Advances via', 'The wall clock'],
    ['Use for', 'Liveness checks, rate limits, cache expiry'],
  ]} />
</Compare>

> **The trap:** processing time is easier and works fine until the job restarts, lags, or backfills — that is, until production.

---

## Timestamp vs Watermark

<Compare>
  <CompareCard title="Timestamp" rows={[
    ['Belongs to', 'One record'],
    ['Says', '"I happened at T"'],
    ['A fact', 'Yes'],
    ['Travels', 'Attached to the record'],
  ]} />
  <CompareCard title="Watermark" rows={[
    ['Belongs to', 'The stream'],
    ['Says', '"I believe everything up to T has arrived"'],
    ['A fact', 'No — a decision under uncertainty'],
    ['Travels', 'As its own control message, broadcast downstream'],
  ]} />
</Compare>

> **The trap:** a watermark is a claim, not a guarantee. Nothing stops a record from arriving behind it.

---

## Out-of-order vs Late

<Compare>
  <CompareCard title="Out-of-order" rows={[
    ['Means', 'Arrived after something with a LATER timestamp'],
    ['Reference point', 'Other records'],
    ['A problem?', 'No — completely normal'],
    ['Handled by', 'The out-of-orderness bound'],
  ]} />
  <CompareCard title="Late" rows={[
    ['Means', 'Arrived after the WATERMARK passed its timestamp'],
    ['Reference point', 'The watermark'],
    ['A problem?', 'Yes — its window may already be gone'],
    ['Handled by', 'allowedLateness or sideOutputLateData'],
  ]} />
</Compare>

---

## Checkpoint vs Savepoint

<Compare>
  <CompareCard title="Checkpoint" rows={[
    ['Triggered by', 'Flink, automatically'],
    ['Owned by', 'Flink'],
    ['For', 'Automatic failure recovery'],
    ['Format', 'State backend native'],
    ['Incremental', 'Yes, with RocksDB'],
    ['Deleted', 'Automatically'],
  ]} />
  <CompareCard title="Savepoint" rows={[
    ['Triggered by', 'You'],
    ['Owned by', 'You'],
    ['For', 'Upgrades, rescaling, migration'],
    ['Format', 'Canonical, portable'],
    ['Incremental', 'No, by default'],
    ['Deleted', 'Only by you'],
  ]} />
</Compare>

> **The trap:** `flink cancel` on a stateful job. Use `flink stop --savepointPath`.

---

## Checkpoint vs Snapshot

Not different things — different scopes of the same word.

| Term | Scope |
| --- | --- |
| **State snapshot** | One operator subtask's state at a point in time |
| **Checkpoint** | All subtasks' snapshots, at the same consistent cut, plus metadata |
| **Savepoint** | A checkpoint you triggered, in a portable format, that Flink will not delete |

> "Snapshot" is the building block. "Checkpoint" and "savepoint" are what you get when you coordinate snapshots across the whole job.

---

## Keyed state vs Operator state

<Compare>
  <CompareCard title="Keyed state" rows={[
    ['Scoped to', 'One key'],
    ['Needs', 'A keyBy'],
    ['Rescales by', 'Key groups — automatic and exact'],
    ['Size', 'Can be terabytes'],
    ['Use for', 'Almost everything'],
  ]} />
  <CompareCard title="Operator state" rows={[
    ['Scoped to', 'One subtask'],
    ['Needs', 'Nothing'],
    ['Rescales by', 'An explicit rule: split or replicate'],
    ['Size', 'Should be small'],
    ['Use for', 'Connectors: offsets, buffers, broadcast config'],
  ]} />
</Compare>

---

## Parallelism vs Task slots

<Compare>
  <CompareCard title="Parallelism" rows={[
    ['Is', 'How many subtasks an operator runs as'],
    ['Set on', 'The job or the operator'],
    ['Bounded by', 'maxParallelism, and by source partitions'],
  ]} />
  <CompareCard title="Task slot" rows={[
    ['Is', 'A fixed share of a TaskManager MEMORY'],
    ['Set on', 'The TaskManager (numberOfTaskSlots)'],
    ['Bounded by', 'Nothing — but slots share CPU freely'],
  ]} />
</Compare>

> **The key fact:** because subtasks of *different* operators share a slot, a job needs as many slots as its **widest operator's parallelism** — not the sum of all of them.

---

## Operator vs Subtask vs Task

| Term | Means |
| --- | --- |
| **Operator** | What you wrote — one `map`, one `window` |
| **Subtask** | One parallel instance of an operator. **Owns state.** The unit of everything. |
| **Task** | What actually runs in a slot: a chain of subtasks executing in one thread |

> A three-operator chain at parallelism 4 = 3 operators, 12 subtasks, 4 tasks.

---

## Exactly-once vs Exactly-once side effects

<Compare>
  <CompareCard title="Exactly-once (what Flink gives)" rows={[
    ['Guarantees', 'STATE reflects each record once'],
    ['Records reprocessed?', 'Yes — that is the recovery mechanism'],
    ['Needs', 'Checkpointing plus a replayable source'],
    ['Free?', 'Yes, essentially'],
  ]} />
  <CompareCard title="End-to-end exactly-once" rows={[
    ['Guarantees', 'External systems see each result once'],
    ['Records reprocessed?', 'Yes — but writes are absorbed'],
    ['Needs', 'A transactional or idempotent SINK'],
    ['Free?', 'No — 2PC ties latency to the checkpoint interval'],
  ]} />
</Compare>

---

## Kafka offset vs Flink checkpoint

| | Kafka offset | Flink checkpoint |
| --- | --- | --- |
| Stored where | In Flink's checkpoint (not `__consumer_offsets`) | S3 / HDFS |
| Contains | A read position | A read position **plus all derived state** |
| Rewound how | Together with state, on restore | As a unit |

> Flink commits offsets to Kafka *for monitoring only*. It never reads them back on restore. Resetting a consumer group does nothing to a running job.

---

## Replay vs Duplicate processing

Same event, different framing.

| Term | Perspective |
| --- | --- |
| **Replay** | Flink's: "I rewound to a consistent point and am reprocessing" |
| **Duplicate processing** | Yours: "this record went through my code twice" |

Both are true. They are correct if state was rewound too, and if the sink absorbs
the repeated write. They are a bug only when the sink appends blindly, or when the
processing is non-deterministic.

---

## State TTL vs Window cleanup

<Compare>
  <CompareCard title="State TTL" rows={[
    ['Clock', 'PROCESSING time'],
    ['Triggered by', 'Access, or background compaction'],
    ['Precision', 'Approximate — lazy deletion'],
    ['During replay', 'Nothing expires — wall clock barely moves'],
    ['Use as', 'A backstop'],
  ]} />
  <CompareCard title="Window cleanup" rows={[
    ['Clock', 'EVENT time'],
    ['Triggered by', 'The watermark passing windowEnd + lateness'],
    ['Precision', 'Exact'],
    ['During replay', 'Identical to live — deterministic'],
    ['Use as', 'The design'],
  ]} />
</Compare>

---

## Savepoint vs Externalized checkpoint

Both are durable and both can be restored from. The differences that matter:

| | Externalized checkpoint | Savepoint |
| --- | --- | --- |
| Format | Backend-native | Canonical (portable) |
| Switch state backends | ❌ | ✅ |
| Rescale | Usually works | Designed for it |
| Cleanup | Flink may delete it | Never |
| Take one on demand | ❌ | ✅ |

> An externalized checkpoint is a fine emergency restore point. A savepoint is what you plan an upgrade around.

---

## Aligned vs Unaligned checkpoints

<Compare>
  <CompareCard title="Aligned" rows={[
    ['Fast channel', 'BLOCKED during alignment'],
    ['Snapshot contains', 'Operator state only'],
    ['Size', 'Smaller'],
    ['Under backpressure', 'Duration explodes'],
    ['Recovery', 'Faster'],
  ]} />
  <CompareCard title="Unaligned" rows={[
    ['Fast channel', 'Not blocked — barrier overtakes records'],
    ['Snapshot contains', 'State PLUS in-flight buffers'],
    ['Size', 'Larger'],
    ['Under backpressure', 'Duration roughly constant'],
    ['Recovery', 'Slower — in-flight data must be re-injected'],
  ]} />
</Compare>

> **Best setting for most jobs:** unaligned enabled, with `aligned-checkpoint-timeout: 30s`. Aligned when healthy, unaligned automatically when not.

---

## `name()` vs `uid()`

| | `name()` | `uid()` |
| --- | --- | --- |
| Purpose | Display in the UI, metrics, logs | **Identity for state matching** |
| Changing it | Harmless | **Orphans that operator's state** |
| If omitted | Auto-generated from the class | Auto-generated **from graph structure** |
| Consequence of omitting | Unreadable dashboards | Adding an unrelated operator upstream breaks your savepoint |

> Set `uid()` on every stateful operator, in version one. It cannot be added retroactively without losing state.

---

## `ValueState` of a map vs `MapState`

| | `ValueState<HashMap>` | `MapState` |
| --- | --- | --- |
| Reading one entry | Deserialises the **entire** map | Deserialises one entry |
| Writing one entry | Serialises the entire map | Serialises one entry |
| Complexity | O(n) per access | O(1) per access |
| On RocksDB | Catastrophic at size | Correct |

> The single most common performance fix in Flink.

<Callout type="remember">

If you can explain each of these fifteen pairs in one sentence, you can debug most
Flink incidents and answer most Flink interview questions.

</Callout>

## Next

**[Cheat sheets](/docs/flink/reference/cheat-sheets)** — one page per subsystem.
