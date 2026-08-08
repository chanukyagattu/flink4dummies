---
title: "Project 1 — Clickstream analytics"
sidebar_label: "1 · Clickstream"
description: Page views per minute in event time — the smallest job that is genuinely representative of production.
---

# Project 1 — Clickstream analytics

<PageMeta level="beginner" time="15 min" prereq={[['Projects overview', '/docs/flink/projects']]} />

**Goal:** count page views per minute, in event time, correctly, with late data
accounted for rather than silently dropped.

**Teaches:** event time, watermarks, tumbling windows, incremental aggregation, side
outputs.

---

## The job

```java title="ClickstreamJob.java"
public class ClickstreamJob {

  public record Click(String userId, String page, long eventTime) {}
  public record PageCount(String page, long windowStart, long windowEnd, long count) {}

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000);
    env.setMaxParallelism(720);                    // set it BEFORE the first checkpoint

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("clicks")
        .setGroupId("clickstream")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperty("partition.discovery.interval.ms", "60000")
        .build();

    WatermarkStrategy<String> watermarks = WatermarkStrategy
        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((json, kafkaTs) -> {
            long t = JSON.parse(json).eventTime();
            long now = System.currentTimeMillis();
            // clamp future timestamps — one bad record can freeze the job forever
            return t > now + 300_000 ? now : t;
        })
        .withIdleness(Duration.ofMinutes(1))
        .withWatermarkAlignment("clicks", Duration.ofSeconds(30), Duration.ofSeconds(1));

    DataStream<Click> clicks = env
        .fromSource(source, watermarks, "clicks")
        .map(ClickstreamJob::parse).name("parse").uid("parse");

    OutputTag<Click> lateTag = new OutputTag<>("late-clicks"){};

    SingleOutputStreamOperator<PageCount> counts = clicks
        .keyBy(Click::page)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .allowedLateness(Duration.ofSeconds(30))
        .sideOutputLateData(lateTag)
        .aggregate(new CountAgg(), new AddWindowMetadata())
        .name("count-per-minute").uid("count-per-minute");

    counts.print().name("results");

    counts.getSideOutput(lateTag)
          .map(c -> "DROPPED: " + c)
          .print().name("late");

    env.execute("clickstream");
  }

  /** O(1) state per window — one long, not a buffer of records. */
  static class CountAgg implements AggregateFunction<Click, Long, Long> {
    public Long createAccumulator()          { return 0L; }
    public Long add(Click c, Long acc)       { return acc + 1; }
    public Long getResult(Long acc)          { return acc; }
    public Long merge(Long a, Long b)        { return a + b; }
  }

  /** Receives ONE element — the aggregate — not the records. */
  static class AddWindowMetadata
      extends ProcessWindowFunction<Long, PageCount, String, TimeWindow> {
    public void process(String page, Context ctx, Iterable<Long> agg,
                        Collector<PageCount> out) {
      out.collect(new PageCount(page,
          ctx.window().getStart(), ctx.window().getEnd(),
          agg.iterator().next()));
    }
  }
}
```

## Why each line is there

| Line | Without it |
| --- | --- |
| `enableCheckpointing(10_000)` | Every restart starts from zero, silently |
| `setMaxParallelism(720)` | You are stuck with the default forever |
| `forBoundedOutOfOrderness(10s)` | Zero tolerance — most records become late |
| The timestamp clamp | One malformed record freezes event time permanently |
| `withIdleness` | One quiet partition and the job emits nothing, forever |
| `withWatermarkAlignment` | The first backfill OOMs the cluster |
| `sideOutputLateData` | Late records vanish with no trace |
| `aggregate` + `ProcessWindowFunction` | `process` alone buffers every record |
| `uid()` | The first upgrade loses all state |

## Run it

```bash
mvn clean package
java -cp target/projects.jar Generator &          # start producing
flink run -d -p 4 target/projects.jar             # or run main() in your IDE
```

Expected output — one row per page per minute, arriving about ten seconds after each
minute ends (that is your bound):

```text
PageCount[page=/, windowStart=..., count=4127]
PageCount[page=/search, windowStart=..., count=1893]
DROPPED: Click[userId=u42, page=/cart, eventTime=...]
```

Those `DROPPED` lines are the 1-in-500 very-late records from the generator. In a
naïve job they would be invisible.

---

## Break it on purpose

This is the part that teaches. Do all four.

### 1. Remove `withIdleness`, then silence a partition

```bash
# produce to only one partition
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic clicks --property parse.key=false
```

**What happens:** the job keeps consuming. `numRecordsIn` climbs. Checkpoints succeed.
Output stops completely. No error appears anywhere.

**What to look at:** UI → the window operator → Watermarks tab. One subtask shows
`No Watermark`.

You have just reproduced the most common Flink production incident, on purpose, in a
safe place. → [Scenario 1](/docs/flink/production/runbook)

### 2. Set the bound to `Duration.ZERO`

**What happens:** results appear about ten seconds sooner. The `DROPPED` lines
multiply enormously.

**The lesson:** latency and completeness are the same dial. There is no setting that
gives you both.

### 3. Swap `aggregate` for `process`

```java
.process(new ProcessWindowFunction<Click, PageCount, String, TimeWindow>() {
    public void process(String page, Context ctx, Iterable<Click> all,
                        Collector<PageCount> out) {
        long n = 0;
        for (Click c : all) n++;     // every record was buffered to do this
        out.collect(new PageCount(page, ctx.window().getStart(),
                                  ctx.window().getEnd(), n));
    }
})
```

**What happens:** identical output. Watch checkpoint size in the UI — it grows by
orders of magnitude, because every record in every open window is now retained.

**The lesson:** two functions that produce the same answer can differ by a thousandfold
in state. → [Window functions](/docs/flink/windows/window-functions)

### 4. Kill a TaskManager mid-window

```bash
docker compose kill --signal=SIGKILL taskmanager-1
```

**What happens:** the job fails, restarts, restores from the last checkpoint, rewinds
Kafka offsets, reprocesses, and produces **identical** output for the affected windows.

**What to look at:** the Exceptions tab for the failure, the Checkpoints tab for which
checkpoint it restored from, and the offsets it rewound to.

This is the whole of [Level 8](/docs/flink/fault-tolerance/failure-model), in one
command.

---

## Extensions

- Add a second aggregation — unique users per page per minute — using a HyperLogLog accumulator rather than a `Set`. Compare state sizes.
- Write the results to `upsert-kafka` keyed on `(window_start, page)` so the allowed-lateness re-fires become updates instead of duplicates.
- Replace the whole job with the SQL from [streaming patterns](/docs/flink/sql/streaming-patterns) and compare the plans.

## Next

**[Project 2 — sessionization](/docs/flink/projects/sessionization)**
