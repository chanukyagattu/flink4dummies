---
title: Flink in the ecosystem
sidebar_label: Flink in the ecosystem
description: The layered Flink stack, updated for Flink 2.x — and an honest comparison with Kafka Streams, Spark, and the rest.
---

# Flink in the ecosystem

<PageMeta level="beginner" time="8 min" prereq={[['Why this is hard', '/docs/flink/foundations/why-this-is-hard']]} />

<Objectives>

- Read the layered Flink stack and say what each layer is responsible for
- Spot what is wrong with the "Flink ecosystem" diagram you will find in most tutorials
- Choose between Flink, Kafka Streams, and Spark for a concrete requirement

</Objectives>

## The stack

Flink is layered. Each layer only depends on the one below it, which is why you
can write SQL and get the same runtime guarantees as hand-written Java.

<div style={{background: 'var(--fb-surface)', border: '1px solid var(--fb-border)', borderRadius: '10px', padding: '1.2rem', margin: '1.6rem 0', overflowX: 'auto'}}>
<svg viewBox="0 0 860 470" role="img" aria-label="The Apache Flink stack in five layers: storage, deploy, runtime engine, API abstraction, and libraries.">

  <text x="8" y="34" fontSize="11" fill="var(--fb-text-dim)" fontWeight="600" letterSpacing="0.08em">LIBRARIES</text>
  <rect x="120" y="14" width="200" height="34" rx="7" fill="var(--fb-violet-soft)" stroke="var(--fb-violet)" />
  <text x="220" y="36" fontSize="13" fill="var(--fb-violet)" textAnchor="middle" fontWeight="620">CEP</text>
  <rect x="332" y="14" width="200" height="34" rx="7" fill="var(--fb-violet-soft)" stroke="var(--fb-violet)" />
  <text x="432" y="36" fontSize="13" fill="var(--fb-violet)" textAnchor="middle" fontWeight="620">State Processor API</text>
  <rect x="544" y="14" width="200" height="34" rx="7" fill="var(--fb-violet-soft)" stroke="var(--fb-violet)" />
  <text x="644" y="36" fontSize="13" fill="var(--fb-violet)" textAnchor="middle" fontWeight="620">Async I/O</text>

  <text x="8" y="104" fontSize="11" fill="var(--fb-text-dim)" fontWeight="600" letterSpacing="0.08em">API</text>
  <rect x="120" y="66" width="300" height="70" rx="8" fill="var(--fb-sage-soft)" stroke="var(--fb-sage)" strokeWidth="1.6" />
  <text x="270" y="94" fontSize="15" fill="var(--fb-sage)" textAnchor="middle" fontWeight="680">Table API &amp; SQL</text>
  <text x="270" y="114" fontSize="11" fill="var(--fb-text-dim)" textAnchor="middle">declarative · unified batch + stream</text>
  <rect x="432" y="66" width="312" height="70" rx="8" fill="var(--fb-sage-soft)" stroke="var(--fb-sage)" strokeWidth="1.6" />
  <text x="588" y="94" fontSize="15" fill="var(--fb-sage)" textAnchor="middle" fontWeight="680">DataStream API</text>
  <text x="588" y="114" fontSize="11" fill="var(--fb-text-dim)" textAnchor="middle">imperative · full control of state and time</text>

  <text x="8" y="190" fontSize="11" fill="var(--fb-text-dim)" fontWeight="600" letterSpacing="0.08em">ENGINE</text>
  <rect x="120" y="154" width="624" height="76" rx="8" fill="var(--fb-blue-soft)" stroke="var(--fb-blue)" strokeWidth="2" />
  <text x="432" y="182" fontSize="16" fill="var(--fb-blue)" textAnchor="middle" fontWeight="700">Flink Runtime — distributed dataflow engine</text>
  <text x="432" y="203" fontSize="11.5" fill="var(--fb-text-dim)" textAnchor="middle">scheduling · network stack · state backends · checkpointing · watermarks · timers</text>
  <text x="432" y="220" fontSize="11.5" fill="var(--fb-text-dim)" textAnchor="middle">everything above compiles down to this, so everything above gets the same guarantees</text>

  <text x="8" y="284" fontSize="11" fill="var(--fb-text-dim)" fontWeight="600" letterSpacing="0.08em">DEPLOY</text>
  <rect x="120" y="252" width="146" height="52" rx="7" fill="var(--fb-amber-soft)" stroke="var(--fb-amber)" />
  <text x="193" y="275" fontSize="13" fill="var(--fb-amber)" textAnchor="middle" fontWeight="620">Local JVM</text>
  <text x="193" y="292" fontSize="10.5" fill="var(--fb-text-dim)" textAnchor="middle">tests, IDE</text>
  <rect x="278" y="252" width="146" height="52" rx="7" fill="var(--fb-amber-soft)" stroke="var(--fb-amber)" />
  <text x="351" y="275" fontSize="13" fill="var(--fb-amber)" textAnchor="middle" fontWeight="620">Standalone</text>
  <text x="351" y="292" fontSize="10.5" fill="var(--fb-text-dim)" textAnchor="middle">bare metal, Docker</text>
  <rect x="436" y="252" width="146" height="52" rx="7" fill="var(--fb-amber-soft)" stroke="var(--fb-amber)" />
  <text x="509" y="275" fontSize="13" fill="var(--fb-amber)" textAnchor="middle" fontWeight="620">Kubernetes</text>
  <text x="509" y="292" fontSize="10.5" fill="var(--fb-text-dim)" textAnchor="middle">the default in 2026</text>
  <rect x="594" y="252" width="150" height="52" rx="7" fill="var(--fb-amber-soft)" stroke="var(--fb-amber)" />
  <text x="669" y="275" fontSize="13" fill="var(--fb-amber)" textAnchor="middle" fontWeight="620">YARN</text>
  <text x="669" y="292" fontSize="10.5" fill="var(--fb-text-dim)" textAnchor="middle">legacy Hadoop estates</text>

  <text x="8" y="374" fontSize="11" fill="var(--fb-text-dim)" fontWeight="600" letterSpacing="0.08em">STORAGE</text>
  <rect x="120" y="326" width="624" height="124" rx="8" fill="var(--fb-surface-2)" stroke="var(--fb-border)" />
  <text x="140" y="350" fontSize="12" fill="var(--fb-text-dim)" fontStyle="italic">Streams (sources)</text>
  <text x="140" y="372" fontSize="12.5" fill="var(--fb-text)">Kafka · Pulsar · Kinesis · RabbitMQ · CDC (Debezium)</text>
  <text x="140" y="400" fontSize="12" fill="var(--fb-text-dim)" fontStyle="italic">Lakes and tables (sinks)</text>
  <text x="140" y="422" fontSize="12.5" fill="var(--fb-text)">S3/HDFS · Parquet · Iceberg · Delta · Paimon · JDBC · Elasticsearch</text>
  <text x="140" y="442" fontSize="11" fill="var(--fb-text-dim)">Checkpoint storage lives here too — S3, HDFS, or any durable filesystem</text>
</svg>
</div>

Read it bottom-up: Flink **owns no storage**. It reads from other people's
systems, computes, and writes to other people's systems. That is deliberate, and
it is why Flink shows up in so many architectures — it is a compute layer, not a
platform you have to migrate onto.

<Callout type="version" title="What the classic diagram gets wrong">

Search for "Flink ecosystem" and you will find a diagram with **DataSet (batch)**
next to **DataStream (streaming)**, plus **Gelly**, **FlinkML**, and **Zeppelin**.

That diagram described Flink 1.x, and much of it is gone:

| In the old diagram | Status in Flink 2.3 |
| --- | --- |
| `DataSet` API | **Removed in Flink 2.0.** Use `DataStream` in `BATCH` mode, or Table/SQL. |
| Scala `DataStream` / `DataSet` API | **Removed in Flink 2.0** (FLIP-265). Java only. |
| `SourceFunction`, `SinkFunction`, Sink V1 | **Removed in Flink 2.0.** Use the `Source` and `Sink` V2 interfaces. |
| Gelly (graph) | Effectively dormant; not part of a modern stack. |
| FlinkML | Superseded; ML on streams is normally done via Async I/O to a model server. |
| Zeppelin | Still works, but the SQL Gateway / SQL Client is the mainstream path now. |
| `flink-conf.yaml` | Replaced by standard-YAML `config.yaml`. |

If a tutorial teaches you `DataSet` or the Scala API, it predates 2025 and will
not compile against a current Flink. The stack diagram above is the 2026 one.

</Callout>

## Which API should you use?

They are not tiers of skill — they are different trade-offs, and mature systems
use both.

<Compare>
  <CompareCard title="Table API & SQL" rows={[
    ['Best for', 'Aggregations, joins, filtering, anything expressible relationally'],
    ['You write', 'SELECT … GROUP BY TUMBLE(…)'],
    ['Flink handles', 'Operator choice, state layout, retraction, incremental aggregation, optimisation'],
    ['Give up', 'Fine control over state shape and timer logic'],
    ['Reach for it', 'First. Drop to DataStream only when SQL cannot express the thing.'],
  ]} />
  <CompareCard title="DataStream API" rows={[
    ['Best for', 'Per-key custom logic, timers, event-driven applications, custom state machines'],
    ['You write', 'KeyedProcessFunction with explicit ValueState and timers'],
    ['Flink handles', 'Distribution, state persistence, time, fault tolerance'],
    ['Give up', 'Query optimisation — your operator order is what runs'],
    ['Reach for it', 'When the logic is "on this event, look at what I remember about this key, and maybe schedule something"'],
  ]} />
</Compare>

They interoperate: `StreamTableEnvironment` converts a `DataStream` to a `Table`
and back, so you can do the relational 80% in SQL and the awkward 20% in Java
within one job.

## Flink vs the alternatives

Honest comparison, no vendor framing.

| | **Flink** | **Kafka Streams** | **Spark Structured Streaming** |
| --- | --- | --- | --- |
| **Model** | Event-at-a-time, true streaming | Event-at-a-time, library | Micro-batch (continuous mode is limited) |
| **Deployment** | A cluster you run | A JAR you run — no cluster | A Spark cluster |
| **Latency** | Milliseconds | Milliseconds | Batch interval, typically 100ms+ |
| **Sources / sinks** | Very broad | Kafka only | Very broad |
| **State size** | Very large — RocksDB, incremental checkpoints, TB-scale in production | Large, RocksDB, but per-instance | Large, but recovery is coarser |
| **Event time / watermarks** | The most complete implementation | Good | Good |
| **Rescaling stateful jobs** | Savepoint → change parallelism → restore | Kafka consumer group rebalance | Restart |
| **Batch with the same code** | Yes, first-class | No | Yes |
| **Operational weight** | Highest | Lowest | Middle |

Practical rules of thumb:

- **Kafka in, Kafka out, one team, moderate state, no cluster appetite** → Kafka Streams. Do not stand up a Flink cluster to do a windowed count between two topics.
- **Already all-in on Spark, latency of seconds is fine, the team knows Spark** → Structured Streaming. The marginal value of Flink will not repay the new operational surface.
- **Many sources and sinks, sub-second latency, large keyed state, event-time correctness that must survive rescaling and upgrades, or you are building a *platform* other teams deploy onto** → Flink. This is the case it was designed for, and where nothing else is really close.

<Callout type="prod" title="Where StreamForge sits">

[StreamForge](https://chanukyagattu.github.io/stream-forge/docs/intro) is a layer *above* the DataStream API: you describe a
`source | transform | sink` graph in YAML, and it compiles to a typed Flink job
graph with connectors, exactly-once emission, and catalog registration wired up.

Everything this guide teaches about time, state, and checkpoints still applies —
StreamForge does not hide the runtime, it removes the boilerplate around it. When
a StreamForge pipeline misbehaves, you debug it as a Flink job, using
[the runbook](/docs/flink/production/runbook).

</Callout>

<Callout type="remember">

Flink is a compute engine, not a storage system. One runtime underneath every
API, so SQL and Java get identical correctness guarantees. And the ecosystem
diagram in most tutorials is from a Flink that no longer exists.

</Callout>

## Next

**[Flink architecture](/docs/flink/basics/architecture)** — what is actually running when you submit a job.
