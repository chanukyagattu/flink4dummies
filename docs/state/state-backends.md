---
title: State backends
sidebar_label: State backends
description: HashMapStateBackend vs EmbeddedRocksDBStateBackend — the trade-off, the tuning knobs, and how to choose.
---

# State backends

<PageMeta level="advanced" time="9 min" prereq={[['TTL & growth', '/docs/flink/state/ttl-and-growth']]} docs="docs/ops/state/state_backends/" />

<Objectives>

- Choose a backend from state size and latency requirements
- Separate the two independent decisions: where state lives, and where snapshots go
- Name the three RocksDB knobs that actually move the needle

</Objectives>

## Two decisions, not one

People conflate these constantly. They are orthogonal.

```text
1. WHERE DOES LIVE STATE LIVE?   → state backend
      HashMapStateBackend        (JVM heap)
      EmbeddedRocksDBStateBackend (local disk, off-heap memory)

2. WHERE DO SNAPSHOTS GO?        → checkpoint storage
      JobManagerCheckpointStorage (JobManager heap — testing only)
      FileSystemCheckpointStorage (S3, HDFS, ...)
```

```java
// production: RocksDB for live state, S3 for snapshots
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));  // true = incremental
env.getCheckpointConfig().setCheckpointStorage("s3://bucket/checkpoints");
```

<Callout type="version">

Flink 1.13 renamed these. If you see `MemoryStateBackend`, `FsStateBackend` or
`RocksDBStateBackend` in a tutorial, it predates 2021:

| Old (removed) | New |
| --- | --- |
| `MemoryStateBackend` | `HashMapStateBackend` + `JobManagerCheckpointStorage` |
| `FsStateBackend` | `HashMapStateBackend` + `FileSystemCheckpointStorage` |
| `RocksDBStateBackend` | `EmbeddedRocksDBStateBackend` + `FileSystemCheckpointStorage` |

The rename exists precisely to force the two decisions apart.

</Callout>

## The comparison

<Compare>
  <CompareCard title="HashMapStateBackend" rows={[
    ['State lives in', 'JVM heap, as Java objects'],
    ['Serialisation per access', 'None'],
    ['Speed', 'Very fast — a hash lookup'],
    ['Max size', 'Bounded by heap. Realistically a few GB per subtask.'],
    ['Checkpoints', 'Full only — the whole state, every time'],
    ['GC pressure', 'High at large sizes; long pauses'],
    ['Use for', 'Small state, lowest latency, dev and test'],
  ]} />
  <CompareCard title="EmbeddedRocksDBStateBackend" rows={[
    ['State lives in', 'An embedded LSM store on local disk, off-heap'],
    ['Serialisation per access', 'Every read and every write'],
    ['Speed', 'Roughly 5–10× slower per access'],
    ['Max size', 'Bounded by local disk. Terabytes in production.'],
    ['Checkpoints', 'Incremental — only changed SST files are uploaded'],
    ['GC pressure', 'Minimal — state is off-heap'],
    ['Use for', 'Anything with real state. The production default.'],
  ]} />
</Compare>

<Callout type="key">

The decision is almost always: **how big is your state?**

- Under ~1 GB per subtask, latency-critical → heap
- Anything larger, or growing, or you are not sure → RocksDB

And there is a second reason RocksDB usually wins even at moderate sizes:
**incremental checkpoints**. With heap state, a 5 GB state means uploading 5 GB
every checkpoint. With RocksDB incremental, it means uploading only the SST files
that changed — often tens of megabytes. That difference dominates your checkpoint
duration.

</Callout>

## Why RocksDB is slower, precisely

```text
HEAP:     state.value()  →  hashmap lookup  →  return object reference
                              ~50 nanoseconds

ROCKSDB:  state.value()  →  serialise key
                         →  RocksDB get (memtable, then block cache, then SST on disk)
                         →  deserialise value
                         →  return a NEW object
                              ~1–10 microseconds
```

Practical consequences:

- Access patterns matter far more. `MapState.get(k)` reads one entry; `ValueState<HashMap>` deserialises everything. On heap the difference is small; on RocksDB it is enormous.
- Your serialiser choice becomes a throughput factor. A Kryo fallback on a hot state type can halve throughput. See [serialization](/docs/flink/state/serialization-and-evolution).
- Local disk type matters. RocksDB on network storage (EBS gp2, NFS) is dramatically slower than on a local NVMe SSD. This is one of the highest-leverage infrastructure choices in a Flink deployment.

## The RocksDB knobs that matter

Most RocksDB tuning is folklore. Three things genuinely move the needle.

### 1. Managed memory

```yaml
taskmanager.memory.managed.fraction: 0.4    # default; raise for large state
```

Flink gives RocksDB a slice of *managed memory* for its block cache and write
buffers. More managed memory means more of your working set is cached and fewer
disk reads. If RocksDB is your bottleneck, this is the first dial.

### 2. Predefined options

```java
EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
// or FLASH_SSD_OPTIMIZED for NVMe
```

These bundles set dozens of RocksDB options sensibly. Use them instead of tuning
individual options, unless you have profiled and know exactly what you are doing.

### 3. Local directory placement

```yaml
state.backend.rocksdb.localdir: /mnt/nvme/rocksdb
```

Put it on fast local disk. On Kubernetes that means a local SSD or an
`emptyDir` on instance storage — **not** a network-attached volume. This single
change is frequently worth more than every other tuning combined.

<Callout type="prod" title="Two features worth knowing about">

**Timers in RocksDB.** By default, timers are stored on the heap even when state
is in RocksDB. A job with tens of millions of timers can therefore OOM despite
having "RocksDB state":

```yaml
state.backend.rocksdb.timer-service.factory: rocksdb
```

Slower per timer, but bounded memory. Necessary for high-cardinality timer
workloads.

**Local recovery.** Keeps a copy of state on the TaskManager's local disk in
addition to remote storage:

```yaml
state.backend.local-recovery: true
```

When a job restarts on the *same* TaskManager — by far the most common case for a
transient failure — state is read from local disk instead of downloaded from S3.
Recovery time can drop from minutes to seconds. It costs local disk and nothing
else. Turn it on.

</Callout>

<Callout type="mistake">

Using `JobManagerCheckpointStorage` in production. It stores checkpoints in the
JobManager's **heap**. It exists for tests and toy jobs. In production it will
either OOM the JobManager or lose everything when the JobManager restarts —
possibly both.

Checkpoint storage must be a durable, distributed filesystem. S3, GCS, ABFS,
HDFS. Nothing else.

</Callout>

<Expert>

**Incremental checkpoints are not free.** They upload SST files, so a checkpoint's
*size* is small but the *number* of files retained grows, and restore must
reconstruct state from many files across many checkpoints. Recovery from an
incremental checkpoint is often slower than from a full one. RocksDB periodically
consolidates, but if restore time is your SLA, measure it rather than assuming.

**Changelog state backend.** Flink can decouple checkpointing from RocksDB
compaction by continuously writing a state changelog to durable storage
(`state.backend.changelog.enabled`). Checkpoints become very fast and very
regular, at the cost of extra write traffic and slower recovery. Worth evaluating
when checkpoint *duration variance* is your problem — for example when you need
tight end-to-end latency with a two-phase-commit sink.

**Switching backends requires a savepoint, not a checkpoint.** Savepoints are
written in a canonical, backend-independent format, so heap → RocksDB is a
savepoint-restore away. Checkpoints are backend-specific and cannot be moved
across backends.

**Managed memory is shared across slots.** Multiple RocksDB instances in one
TaskManager share the managed memory pool via a shared write-buffer manager and
block cache. So more slots per TaskManager means less memory per RocksDB instance
— a real reason to prefer fewer slots for large-state jobs.

</Expert>

<Callout type="remember">

Two decisions: where live state lives (backend) and where snapshots go (storage).
RocksDB plus S3 plus incremental plus local recovery is the production default.
Put RocksDB on fast local disk. Never use JobManager checkpoint storage outside a
test.

</Callout>

## Next

**[Serialization and schema evolution](/docs/flink/state/serialization-and-evolution)** — the hidden throughput factor.
