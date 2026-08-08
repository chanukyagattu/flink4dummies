---
title: Cheat sheets
sidebar_label: Cheat sheets
description: One compact reference page per subsystem — time, watermarks, state, checkpoints, performance, debugging, and a production checklist.
---

# Cheat sheets

<PageMeta level="intermediate" time="8 min" />

Compact references. Nothing here is explained — every line links back to a chapter
that does.

---

## The one-page mental model

```text
Kafka
  ↓
Source                      ← offsets are checkpointed, not stored in Kafka
  ↓
Timestamp assigner          ← event time enters the system here
  ↓
Watermark generator         ← per split; emits min across splits, every 200ms
  ↓
map / filter (chained)      ← same thread, no serialisation
  ↓
keyBy                       ← hash → key group → subtask. Breaks the chain.
  ↓
Keyed state                 ← owned by ONE subtask, checkpointed, rescalable
  ↓
Windows / timers            ← fired by the WATERMARK, not the clock
  ↓
Aggregation / join / CEP
  ↓
Sink                        ← 2PC: pre-commit on barrier, commit on complete
  ↓
Kafka / S3 / database
```

---

## Time

```java
// event time — the default and the right answer
WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((e, ts) -> e.eventTime())
    .withIdleness(Duration.ofMinutes(1))
    .withWatermarkAlignment("group", Duration.ofSeconds(30), Duration.ofSeconds(1));

// perfectly ordered input — a strong claim
WatermarkStrategy.forMonotonousTimestamps()

// no event time at all
WatermarkStrategy.noWatermarks()
```

| Clock | Reproducible | Use for |
| --- | --- | --- |
| Event | ✅ | Aggregation, billing, joins, sessions |
| Processing | ❌ | Liveness, rate limits, cache expiry |
| Ingestion | ❌ | Fallback when records have no timestamp |

Inside a `ProcessFunction`:

```java
ctx.timestamp()                             // this record's event time
ctx.timerService().currentWatermark()       // event-time progress
ctx.timerService().currentProcessingTime()  // wall clock
```

---

## Watermarks

```text
watermark = max timestamp seen − out-of-orderness bound − 1ms

operator watermark = MINIMUM across all input channels
```

| Symptom | Cause | Fix |
| --- | --- | --- |
| No output at all | One idle split | `withIdleness` |
| `No Watermark` on every subtask | No assigner | Attach a `WatermarkStrategy` |
| Watermark in the future | A bogus timestamp | Clamp; restart with clean state |
| OOM during replay | One split racing ahead | `withWatermarkAlignment` |
| Results very delayed | Bound too large | Lower it; side-output the tail |

Metrics: `currentOutputWatermark`, `currentInputWatermark`, `numLateRecordsDropped`.

---

## Windows

```java
TumblingEventTimeWindows.of(Duration.ofMinutes(1))
SlidingEventTimeWindows.of(size, slide)          // state × (size / slide)
EventTimeSessionWindows.withGap(Duration.ofMinutes(30))
GlobalWindows.create()                            // needs a trigger!
```

```java
.allowedLateness(Duration.ofMinutes(5))          // state × (1 + lateness/size)
.sideOutputLateData(lateTag)                     // capture, do not drop
.aggregate(new Agg(), new AddWindowMetadata())   // O(1) state + metadata
```

| Function | State per window |
| --- | --- |
| `reduce` / `aggregate` | One accumulator |
| `process` | **Every record** |
| `aggregate` + `ProcessWindowFunction` | One accumulator, plus full metadata |

---

## State

```java
ValueState<T>          state.value() / update() / clear()
ListState<T>           add() / get() / update() / clear()
MapState<K,V>          put() / get() / remove() / entries()      ← prefer this
ReducingState<T>       add()   — combined on write
AggregatingState<I,O>  add()   — combined on write
```

Rules:

1. **`MapState`, never `ValueState<Map>`**
2. Every `update()` needs a reachable `clear()`
3. `keyBy` before any keyed state, always
4. Keys must be immutable with a stable `hashCode()`
5. Set `maxParallelism` explicitly, before the first checkpoint — it is irreversible

```java
StateTtlConfig.newBuilder(Duration.ofDays(7))
    .setUpdateType(UpdateType.OnCreateAndWrite)
    .cleanupInRocksdbCompactFilter(1000)
    .build();                   // TTL uses PROCESSING time — nothing expires in a replay
```

---

## Checkpoints

```java
env.enableCheckpointing(60_000);
cfg.setMinPauseBetweenCheckpoints(30_000);
cfg.setCheckpointTimeout(600_000);
cfg.setMaxConcurrentCheckpoints(1);
cfg.setTolerableCheckpointFailureNumber(3);
cfg.setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION);
cfg.enableUnalignedCheckpoints(true);
// + execution.checkpointing.aligned-checkpoint-timeout: 30s
```

```text
Duration = Start Delay + Alignment + Sync + Async

Alignment / Start Delay dominates  →  BACKPRESSURE problem
Async dominates                    →  STATE SIZE or STORAGE problem
Sync dominates                     →  rare; state backend problem
```

---

## Savepoints

```bash
flink savepoint <jobId> s3://bucket/savepoints          # while running
flink stop --savepointPath s3://bucket/savepoints <id>  # the correct shutdown
flink run -s s3://.../savepoint-abc -d job.jar          # restore
flink run -s s3://.../savepoint-abc -p 32 -d job.jar    # restore + rescale
flink savepoint --dispose s3://.../savepoint-abc        # delete
```

Never `flink cancel` a stateful job. Set `uid()` on every stateful operator.

---

## Exactly-once

End-to-end requires **all five**:

```text
1. Replayable source            (Kafka, Kinesis, files)
2. EXACTLY_ONCE checkpointing
3. Deterministic processing     (no wall clock, no randomness, no external state)
4. Transactional or idempotent sink
5. Consumers reading committed data only
```

```java
.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
.setTransactionalIdPrefix("my-sink-v1")        // unique per job
.setProperty("transaction.timeout.ms", "900000")
// consumers: isolation.level=read_committed
```

Cost: data is invisible until the checkpoint completes → **checkpoint interval is
your end-to-end latency**.

---

## Performance

In order of expected payoff:

```text
1. Filter and project BEFORE the keyBy
2. Fix serialisers      → env.getConfig().disableGenericTypes()
3. MapState, not ValueState<Map>
4. aggregate(), not process()
5. RocksDB on local NVMe + managed memory 0.4–0.5
6. Async I/O for any external call
7. THEN add parallelism
```

Skew:

```text
keyBy(hotKey + "#" + random(64))  →  aggregate  →  strip salt  →  keyBy  →  aggregate
```

SQL:

```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
```

---

## Debugging

```text
Symptom                       First thing to check
─────────────────────────────────────────────────────────────
No output                     currentOutputWatermark per subtask
Slow checkpoints              Checkpoint duration BREAKDOWN
State growing                 Per-operator state size; what do you keyBy on?
Duplicates                    numRestarts + sink delivery guarantee
Low throughput                Backpressure tab → last busy, non-red operator
Missing results               numLateRecordsDropped
Restart loop                  The FIRST exception, not the latest
One subtask hot               Per-subtask numRecordsIn → skew
```

---

## Production checklist

```text
CORRECTNESS
  ☐ Event time, not processing time
  ☐ WatermarkStrategy at the SOURCE (per-split watermarks)
  ☐ withIdleness configured
  ☐ withWatermarkAlignment for anything that will be backfilled
  ☐ Future timestamps clamped
  ☐ Late data side-outputted, not silently dropped

STATE
  ☐ maxParallelism set explicitly (highly composite, e.g. 720)
  ☐ uid() on EVERY stateful operator
  ☐ Every update() has a clear() path
  ☐ TTL configured as a backstop
  ☐ MapState, not ValueState<Map>
  ☐ disableGenericTypes() passing in CI

FAULT TOLERANCE
  ☐ Checkpointing enabled, to S3/HDFS
  ☐ RETAIN_ON_CANCELLATION
  ☐ Incremental checkpoints (RocksDB)
  ☐ Unaligned + aligned-checkpoint-timeout 30s
  ☐ tolerable-failed-checkpoints 2–3
  ☐ Exponential restart backoff
  ☐ JobManager HA enabled

DEPLOYMENT
  ☐ Application mode
  ☐ upgradeMode: savepoint
  ☐ RocksDB on local NVMe
  ☐ local-recovery enabled
  ☐ Image pinned by digest
  ☐ Restore tested against a PRODUCTION-SHAPED savepoint

OBSERVABILITY
  ☐ Metrics exported (Prometheus)
  ☐ Alert: watermark lag
  ☐ Alert: checkpoint failures
  ☐ Alert: checkpoint duration vs interval
  ☐ Alert: state size growth
  ☐ Alert: restart count
  ☐ Alert: consumer lag
```

## Next

**[Glossary](/docs/flink/reference/glossary)** — every term, defined.
