---
title: Glossary
sidebar_label: Glossary
description: Every Flink term, with a one-line definition, why it matters, and where it is explained.
---

# Glossary

<PageMeta level="beginner" time="10 min" />

Alphabetical. Each entry: what it is, why it matters, and where to read more.

---

**Allowed lateness** — Extra time after a window fires during which late records
still update it. *Matters because:* it multiplies window state by
`1 + lateness/windowSize`. → [Triggers and lateness](/docs/flink/windows/triggers-and-lateness)

**Alignment** — An operator waiting for a checkpoint barrier on every input channel
before snapshotting. *Matters because:* its duration explodes under backpressure and
is the usual reason checkpoints are slow. → [Barriers and alignment](/docs/flink/fault-tolerance/barriers-and-alignment)

**Application mode** — One Flink cluster per job, with `main()` running on the
JobManager. *Matters because:* it is the production deployment mode. → [Deployment](/docs/flink/production/deployment)

**Async I/O** — An operator that issues many concurrent external requests instead of
blocking on each. *Matters because:* a blocking call caps throughput at `1/latency`
per subtask. → [Async I/O](/docs/flink/scale/async-io)

**At-least-once** — Records may be processed more than once after a failure.
*Matters because:* acceptable only if the sink is idempotent. → [Exactly-once](/docs/flink/fault-tolerance/exactly-once)

**Backpressure** — A slow operator blocking its upstream via credit-based flow
control. *Matters because:* it determines your real throughput and lengthens
checkpoint alignment. → [Backpressure](/docs/flink/scale/backpressure)

**Barrier** — A marker injected into the stream at the sources that defines the cut
for a checkpoint. *Matters because:* it is how a consistent snapshot is taken without
pausing anything. → [Barriers and alignment](/docs/flink/fault-tolerance/barriers-and-alignment)

**Broadcast state** — Identical state replicated on every subtask, fed by a broadcast
stream. *Matters because:* it is the dynamic-rules pattern. → [Operator and broadcast state](/docs/flink/state/operator-and-broadcast-state)

**CEP** — Complex Event Processing: detecting patterns across a sequence of events.
*Matters because:* fraud, security and monitoring rules are pattern problems, not
aggregation problems.

**Chaining** — Fusing consecutive operators into one thread with no serialisation.
*Matters because:* it is the single largest performance feature in the runtime, and
`keyBy` breaks it. → [Parallelism and subtasks](/docs/flink/basics/parallelism-and-subtasks)

**Checkpoint** — An automatic, consistent, durable snapshot of all state plus source
positions. *Matters because:* it is the only reason a stateful streaming job can
survive a crash. → [Checkpoints](/docs/flink/fault-tolerance/checkpoints)

**CheckpointCoordinator** — The JobManager component that triggers checkpoints,
collects acknowledgements, and declares completion. *Matters because:* its
`notifyCheckpointComplete` call is what makes 2PC sinks commit.

**Credit-based flow control** — The mechanism by which a consumer tells a producer
how many buffers it has free. *Matters because:* it is how backpressure propagates.

**DataStream API** — The imperative Java API for streaming. *Matters because:* it is
the only API in Flink 2.x for custom state and timers (the DataSet API was removed).

**Event time** — When something happened, per the record itself. *Matters because:*
it is the only clock that gives reproducible results. → [Three clocks](/docs/flink/time/three-clocks)

**Exactly-once** — State reflects each record exactly once, despite reprocessing.
*Matters because:* it does **not** mean each record is processed once, and it does
**not** automatically extend to external systems. → [Exactly-once](/docs/flink/fault-tolerance/exactly-once)

**ExecutionGraph** — The parallel graph, with one vertex per subtask, built by the
JobMaster. *Matters because:* it is what gets scheduled and what gets restarted.

**Failover region** — The smallest pipelined subgraph that must restart together.
*Matters because:* in streaming it is usually the whole job.

**Idleness** — Marking a source split inactive so it stops holding back the watermark
minimum. *Matters because:* without it, one quiet partition freezes every window in
the job. → [Propagation and idleness](/docs/flink/watermarks/propagation-and-idleness)

**Incremental checkpoint** — Uploading only changed RocksDB SST files. *Matters
because:* it is the difference between a 400 GB and a 3 GB checkpoint — and it makes
recovery slower. → [State backends](/docs/flink/state/state-backends)

**Interval join** — Joining two streams within a time interval relative to each
other. *Matters because:* it is the join you usually want, and its interval is your
state budget. → [Joins](/docs/flink/joins)

**JobGraph** — The graph after operator chaining; what the client submits. *Matters
because:* it is what the Flink UI shows you.

**JobManager** — The coordinating process: Dispatcher, JobMaster, ResourceManager,
CheckpointCoordinator. *Matters because:* it holds no data, so HA is cheap and
skipping it is inexcusable. → [Architecture](/docs/flink/basics/architecture)

**Key group** — The atomic unit of keyed-state redistribution;
`murmurHash(key.hashCode()) % maxParallelism`. *Matters because:* it is what makes
stateful rescaling possible. → [Keyed state](/docs/flink/state/keyed-state)

**Keyed state** — State scoped automatically to the current key. *Matters because:*
it is 95% of the state you will write, and it requires a `keyBy`.

**`keyBy`** — Partitioning a stream by a key so all records for that key reach one
subtask. *Matters because:* it is the boundary that makes state consistent, and it
forces a network shuffle.

**Kryo** — Flink's fallback serialiser for types it cannot analyse. *Matters
because:* it is 4–10× slower and **cannot be schema-evolved**. → [Serialization](/docs/flink/state/serialization-and-evolution)

**Late record** — One arriving after the watermark passed its timestamp. *Matters
because:* by default it is dropped silently. → [Out-of-order and late](/docs/flink/time/out-of-order-and-late)

**Mailbox model** — Each task runs a single thread interleaving record processing
with control actions. *Matters because:* blocking user code blocks checkpoint
barriers.

**`maxParallelism`** — The number of key groups; the permanent ceiling on
parallelism. *Matters because:* it is **immutable after the first checkpoint**.
→ [Rescaling](/docs/flink/fault-tolerance/rescaling)

**Operator** — A logical transformation you wrote. *Matters because:* it is not the
same as a subtask, which is what runs.

**Operator state** — State scoped to a subtask rather than a key. *Matters because:*
it is how connectors remember offsets, and `UnionListState` does not scale.

**Parallelism** — How many subtasks an operator runs as. *Matters because:* it is not
"the number of machines", and raising it does nothing for a non-CPU-bound bottleneck.

**Processing time** — The machine's wall clock. *Matters because:* it produces
different results on every replay.

**RocksDB** — The embedded LSM store used for large keyed state. *Matters because:*
it takes state off-heap and enables incremental checkpoints, at the cost of
serialisation on every access.

**Savepoint** — A user-triggered, portable, permanently-retained snapshot. *Matters
because:* every stateful upgrade and rescale goes through one. → [Savepoints](/docs/flink/fault-tolerance/savepoints)

**Session window** — A window defined by a gap of inactivity. *Matters because:* it
merges, which makes it powerful and expensive.

**Side output** — A secondary output stream from an operator, addressed by an
`OutputTag`. *Matters because:* it is how late records get captured instead of lost.

**Slot** — A fixed share of a TaskManager's **memory** (not CPU). *Matters because:*
subtasks of different operators share one, so a job needs slots equal to its widest
operator's parallelism.

**Slot sharing group** — A named group controlling which operators may share a slot.
*Matters because:* it lets you isolate an operator with an unusual memory profile.

**State backend** — Where live state is stored: `HashMapStateBackend` (heap) or
`EmbeddedRocksDBStateBackend`. *Matters because:* it is a separate decision from
where checkpoints are written.

**State Processor API** — A batch API for reading, modifying and writing savepoints.
*Matters because:* it turns "we have to drop state" into a small batch job.

**Subtask** — One parallel instance of an operator. *Matters because:* it is the unit
of state ownership, checkpointing, metrics and failure.

**TaskManager** — The worker process holding slots, memory and state. *Matters
because:* all your data lives here, and it is what dies.

**Timer** — A callback scheduled at a future event time or processing time, bound to
a key. *Matters because:* it is the only way to react to the *absence* of events.
→ [Timers](/docs/flink/timers)

**Trigger** — The policy deciding when a window fires. *Matters because:* early
firing means multiple results per window, which your sink must handle.

**TTL** — Automatic expiry of state entries, measured in **processing** time.
*Matters because:* nothing expires during a fast replay. → [TTL and growth](/docs/flink/state/ttl-and-growth)

**Two-phase commit (2PC)** — Pre-commit on the barrier, commit on
`notifyCheckpointComplete`. *Matters because:* it makes the checkpoint interval your
end-to-end latency.

**`uid()`** — The stable identity used to match savepoint state to an operator.
*Matters because:* without it, adding an unrelated operator upstream orphans your
state. → [Savepoints](/docs/flink/fault-tolerance/savepoints)

**Unaligned checkpoint** — Snapshotting in-flight buffers instead of waiting for
alignment. *Matters because:* it makes checkpoint duration independent of
backpressure.

**Watermark** — A claim that event time has progressed to T. *Matters because:* it
fires windows, fires timers, cleans up joins, and defines "late" — and it is the
minimum across all inputs. → [What is a watermark?](/docs/flink/watermarks/what-is-a-watermark)

**Watermark alignment** — Pausing sources that race too far ahead of the group.
*Matters because:* it prevents state explosion during backfills.

**Window** — A bounded slice of an unbounded stream. *Matters because:* it is how an
infinite stream produces finite answers.

## Next

**[Interview questions](/docs/flink/reference/interview)** — beginner to staff level.
