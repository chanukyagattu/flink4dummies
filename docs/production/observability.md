---
title: Observability
sidebar_label: Observability
description: The twelve metrics that matter, the six alerts every Flink job should have, and a dashboard layout that works.
---

# Observability

<PageMeta level="advanced" time="9 min" prereq={[['Deployment', '/docs/flink/production/deployment']]} docs="docs/ops/metrics/" />

<Objectives>

- Build a dashboard that answers "is this job healthy?" in five seconds
- Configure six alerts that catch the failures Flink does not report as failures
- Know which symptoms are silent and therefore must be alerted on explicitly

</Objectives>

## The problem with "job status: RUNNING"

Flink reports a job as RUNNING in all of these situations:

- The watermark has been frozen for six hours and no window has fired
- Checkpoints have failed 40 times in a row
- Throughput is 5% of normal because of a back-pressured sink
- State has grown from 2 GB to 400 GB over three weeks
- Every late record is being silently dropped

<Callout type="key">

**Job status is not a health signal.** It tells you the process exists. The failures
that actually hurt are all silent, and every one of them is detectable from metrics
you already have.

</Callout>

## The twelve metrics

| # | Metric | Meaning | Watch for |
| --- | --- | --- | --- |
| 1 | `numRecordsInPerSecond` | Throughput per operator | Drops, or flatlining at zero |
| 2 | **`currentOutputWatermark`** | Event-time progress | Frozen, or far behind wall clock |
| 3 | **`lastCheckpointDuration`** | How long checkpoints take | Approaching the interval |
| 4 | **`lastCheckpointSize`** | State volume | Steady growth over days |
| 5 | `numberOfFailedCheckpoints` | Checkpoint reliability | Any sustained non-zero |
| 6 | `checkpointAlignmentTime` | Time waiting for barriers | Seconds instead of milliseconds |
| 7 | `backPressuredTimeMsPerSecond` | Blocked on downstream | Sustained above ~200 |
| 8 | `busyTimeMsPerSecond` | Doing work | Near 1000 on one operator = bottleneck |
| 9 | `numLateRecordsDropped` | Silently discarded records | Any change from the baseline |
| 10 | **`numRestarts`** | Job stability | Any increase |
| 11 | `records-lag-max` (Kafka) | End-to-end lag | Growing |
| 12 | `Status.JVM.Memory.Heap.Used` + GC time | Memory pressure | Sawtooth, long pauses |

The four in bold are the ones to look at first when something is wrong.

## The six alerts

These catch the silent failures. Thresholds are starting points — tune to your job.

```yaml
# 1. Event time has stalled — catches idle partitions, dead producers, stuck watermarks
- alert: FlinkEventTimeStalled
  expr: time() - (flink_taskmanager_job_task_operator_currentOutputWatermark / 1000) > 600
  for: 10m
  annotations:
    summary: "Event time is more than 10 minutes behind wall clock"

# 2. Checkpoints failing — the job cannot recover, even though it is running
- alert: FlinkCheckpointsFailing
  expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[15m]) > 2
  for: 5m

# 3. Checkpoint duration approaching the interval — about to spiral
- alert: FlinkCheckpointDurationHigh
  expr: flink_jobmanager_job_lastCheckpointDuration > 30000   # half of a 60s interval
  for: 15m

# 4. State growing — weeks of warning before it becomes an incident
- alert: FlinkStateGrowing
  expr: |
    flink_jobmanager_job_lastCheckpointSize
      / avg_over_time(flink_jobmanager_job_lastCheckpointSize[7d]) > 1.5
  for: 1h

# 5. Restart loop — a permanent failure needs a human, not another retry
- alert: FlinkRestartLoop
  expr: increase(flink_jobmanager_job_numRestarts[1h]) > 5

# 6. Consumer lag growing — the best end-to-end health signal there is
- alert: FlinkConsumerLagGrowing
  expr: deriv(flink_taskmanager_job_task_operator_records_lag_max[15m]) > 0
  for: 30m
```

<Callout type="prod" title="If you only add one, add the first">

`FlinkEventTimeStalled` catches idle partitions, dead upstream producers, frozen
watermarks, and severe lag — the entire class of "running but producing nothing"
failures, which is the most common and most confusing Flink incident.

It is one alert covering half the runbook.

</Callout>

## A dashboard that works

Four rows, in order of how you actually diagnose:

```text
ROW 1 — IS IT ALIVE?
  Job uptime  ·  numRestarts  ·  records in/s  ·  records out/s

ROW 2 — IS TIME MOVING?
  Watermark lag (wall clock − watermark)   ← the single most important panel
  Consumer lag per partition
  Late records dropped

ROW 3 — CAN IT RECOVER?
  Checkpoint duration (with the interval drawn as a threshold line)
  Checkpoint size (7-day trend)
  Failed checkpoints
  Alignment duration

ROW 4 — WHERE IS THE BOTTLENECK?
  busyTime per operator (stacked)
  backPressuredTime per operator
  Heap used and GC time per TaskManager
```

Row 2 is the one people leave out and the one that would have caught the incident.

## Logging

```xml
<!-- log4j2.properties: keep the noise down, keep the signal -->
<Logger name="org.apache.flink.runtime.checkpoint" level="INFO"/>
<Logger name="org.apache.flink.runtime.executiongraph" level="INFO"/>
<Logger name="org.apache.kafka" level="WARN"/>
<Root level="WARN"/>
```

The two INFO loggers give you a line per checkpoint and a line per state transition
— exactly what you want in a post-incident timeline, without drowning in Kafka
client chatter.

<Callout type="mistake">

Logging per record, even at DEBUG. At 100,000 records/s a single log line per record
generates gigabytes per minute, blocks the mailbox thread on I/O, and turns a
throughput problem into an outage.

If you must inspect records, sample: log one in every 10,000, or use a side output
routed to a topic you can query.

</Callout>

## Custom metrics

Instrument your business logic, not just the runtime.

```java
public class OrderProcessor extends KeyedProcessFunction<String, Order, Result> {

    private transient Counter invalid;
    private transient Histogram eventTimeLag;
    private transient Gauge<Integer> pendingCount;

    @Override
    public void open(OpenContext ctx) {
        MetricGroup g = getRuntimeContext().getMetricGroup().addGroup("orders");
        invalid = g.counter("invalid");
        eventTimeLag = g.histogram("eventTimeLagMs",
            new DescriptiveStatisticsHistogram(10_000));
        g.gauge("pending", () -> pending.size());
    }

    @Override
    public void processElement(Order o, Context ctx, Collector<Result> out) {
        eventTimeLag.update(System.currentTimeMillis() - o.eventTime());
        if (!o.isValid()) { invalid.inc(); return; }
        // ...
    }
}
```

That `eventTimeLagMs` histogram is the one that tells you whether your
out-of-orderness bound is still the right number. It is worth adding to every
event-time job.

<Expert>

**Metric scope and cardinality.** Flink's default metric identifiers include job ID,
task ID and subtask index, which at high parallelism produces very high cardinality
in Prometheus. Configure `metrics.scope.*` to trim what you do not need — a job with
parallelism 256 and default scopes can generate hundreds of thousands of series.

**Latency markers.** `metrics.latency.interval` injects markers that measure
end-to-end latency through the topology. Genuinely useful for finding *where*
latency accumulates, and expensive enough that it should not be left on. Investigate
with it, then turn it off.

**`numRecordsIn` on a chained operator.** Chained operators report metrics for the
whole chain, so per-operator record counts inside a chain are not available. This is
one of the few legitimate reasons to `disableChaining()` — temporarily, in staging,
to attribute a cost.

**Watermark metrics are per subtask.** `currentOutputWatermark` on the operator is an
aggregate; the useful view is the *minimum* across subtasks, because that is what
drives everything downstream. Alert on the minimum, not the average.

**The REST API for automation.** `/jobs/:id/checkpoints`, `/jobs/:id/backpressure`
and `/jobs/:id/vertices/:id/subtasktimes` expose everything the UI shows. Scripting
against these lets you build pre-deploy health gates — refuse to upgrade a job whose
last three checkpoints failed.

</Expert>

<Callout type="remember">

RUNNING is not healthy. Alert on watermark lag, checkpoint failures, checkpoint
duration, state growth, restarts, and consumer lag. Put watermark lag on the
dashboard where you will see it.

</Callout>

## Next

**[The production runbook](/docs/flink/production/runbook)** — seven real failures, by symptom.
