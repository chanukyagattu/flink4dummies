---
title: Your first Flink job
sidebar_label: Your first job
description: A complete, runnable Flink 2.x project — word count, then a real event-time click counter with state and windows.
---

# Your first Flink job

<PageMeta level="beginner" time="14 min" prereq={[['From code to cluster', '/docs/flink/basics/from-code-to-cluster']]} docs="docs/dev/datastream/overview/" />

<Objectives>

- Build and run a Flink job from an empty directory
- Read every line of a real pipeline and say what it does
- Recognise the four pieces every Flink job has: source, transform, key, sink

</Objectives>

## The project

Flink 2.x needs **Java 17 or newer**. Two files.

```xml title="pom.xml"
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.streamforge</groupId>
  <artifactId>flink-bible-examples</artifactId>
  <version>1.0</version>

  <properties>
    <flink.version>2.3.0</flink.version>
    <maven.compiler.release>17</maven.compiler.release>
  </properties>

  <dependencies>
    <!-- provided: the cluster already has these on its classpath -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <!-- connectors are NOT provided: they must be in your fat JAR -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
      <version>4.0.0-2.0</version>
    </dependency>
  </dependencies>
</project>
```

<Callout type="prod" title="provided vs compile — the classic first bug">

`flink-streaming-java` is `provided` because the cluster already has it. Bundling
it into your JAR creates two copies of the same classes on the classpath and
produces baffling `ClassCastException` or `LinkageError` failures at runtime.

Connectors are the opposite: they are **not** on the cluster classpath, so they
must be in your JAR (via the Maven Shade plugin). Getting these two backwards
accounts for a large share of "it works in my IDE but not on the cluster".

Note also that Flink connectors version independently of Flink itself —
`flink-connector-kafka:4.0.0-2.0` means "Kafka connector 4.0.0, built for Flink
2.0.x". Do not assume the connector version matches the Flink version.

</Callout>

## Job 1 — word count, with the parts labelled

```java title="WordCount.java"
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

  private static final String[] LINES = {
      "flink processes streams",
      "streams never end",
      "flink keeps state"
  };

  public static void main(String[] args) throws Exception {

    // ── 1. The environment: everything hangs off this ────────────────────
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    // ── 2. SOURCE — where records come from ──────────────────────────────
    // DataGeneratorSource is the modern replacement for fromElements/
    // fromCollection for anything resembling a real stream: it is a proper
    // Source V2, so it checkpoints and rate-limits correctly.
    DataGeneratorSource<String> generator = new DataGeneratorSource<>(
        idx -> LINES[(int) (idx % LINES.length)],
        30,                                        // stop after 30 records
        RateLimiterStrategy.perSecond(5),          // 5 records/second
        org.apache.flink.api.common.typeinfo.Types.STRING);

    DataStream<String> lines = env.fromSource(
        generator, WatermarkStrategy.noWatermarks(), "lines");

    // ── 3. TRANSFORM — stateless, one record in, N records out ───────────
    DataStream<Tuple2<String, Long>> words = lines
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String w : line.split(" ")) out.collect(Tuple2.of(w, 1L));
        })
        .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
            org.apache.flink.api.common.typeinfo.Types.STRING,
            org.apache.flink.api.common.typeinfo.Types.LONG))
        .name("split-into-words");

    // ── 4. KEY + AGGREGATE — the first stateful thing in this job ────────
    // keyBy sends every occurrence of a given word to the SAME subtask,
    // which is the only reason the running sum can be correct.
    DataStream<Tuple2<String, Long>> counts = words
        .keyBy(t -> t.f0)
        .sum(1)                       // a running sum held in keyed state
        .name("count-per-word");

    // ── 5. SINK — where records go ───────────────────────────────────────
    counts.print().name("stdout");

    // ── 6. Nothing above has run yet. Now it does. ───────────────────────
    env.execute("word count");
  }
}
```

Run it:

```bash
mvn -q clean package
# in an IDE: just run main(). Flink starts a mini-cluster automatically.
```

Output — note that each line is an **update**, not a final answer:

```text
1> (flink,1)
2> (processes,1)
1> (streams,1)
1> (streams,2)     ← the count for "streams" changed
2> (never,1)
1> (flink,2)       ← and for "flink"
```

<Callout type="key">

A streaming job emits a **stream of updates**, not one final result. `(flink,1)`
was not wrong — it was correct given the data seen so far. This is the mental
shift from batch, and it is why sinks need to think about idempotency and
[upserts](/docs/flink/fault-tolerance/exactly-once).

</Callout>

The `1>` and `2>` prefixes are subtask indices. Every occurrence of `streams`
lands on the same one, because `keyBy` put it there.

## Job 2 — a real pipeline: clicks per minute, in event time

Word count has no time in it, which makes it a poor model of real work. Here is
the smallest job that is genuinely representative.

```java title="ClicksPerMinute.java"
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;

public class ClicksPerMinute {

  public record Click(String userId, String page, long eventTime) {}

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // Checkpoint every 30s. Without this, state is never durable and a crash
    // loses everything. This one line is the difference between a demo and a job.
    env.enableCheckpointing(30_000);

    // ── SOURCE ───────────────────────────────────────────────────────────
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setTopics("clicks")
        .setGroupId("clicks-per-minute")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    // ── WATERMARK STRATEGY — the most important five lines in the job ─────
    // "Event time comes from the record itself, and I will tolerate events
    //  arriving up to 10 seconds out of order."
    WatermarkStrategy<String> watermarks = WatermarkStrategy
        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((json, recordTimestamp) -> parse(json).eventTime())
        // If a Kafka partition goes quiet, do not let it hold the whole job's
        // event time hostage. See Level 3.
        .withIdleness(Duration.ofMinutes(1));

    DataStream<Click> clicks = env
        .fromSource(source, watermarks, "clicks-kafka")
        .map(ClicksPerMinute::parse)
        .name("parse").uid("parse");

    // ── KEY + WINDOW + AGGREGATE ─────────────────────────────────────────
    clicks
        .keyBy(Click::page)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .aggregate(new CountAgg())        // incremental: O(1) state per window
        .name("clicks-per-minute").uid("clicks-per-minute")
        .print();

    env.execute("clicks per minute");
  }

  static Click parse(String json) { /* your JSON library here */ return null; }
}
```

Four lines are doing the real work, and each one is a chapter of this guide:

| Line | What it decides | Chapter |
| --- | --- | --- |
| `forBoundedOutOfOrderness(10s)` | How long to wait for stragglers before declaring a minute complete | [Watermarks](/docs/flink/watermarks/what-is-a-watermark) |
| `keyBy(Click::page)` | Which subtask owns which page's state | [Keyed state](/docs/flink/state/keyed-state) |
| `TumblingEventTimeWindows.of(1 min)` | Which minute an event belongs to — by *event* time, not arrival | [Windows](/docs/flink/windows/window-types) |
| `enableCheckpointing(30s)` | Whether any of the above survives a crash | [Checkpoints](/docs/flink/fault-tolerance/checkpoints) |

<Callout type="mistake" title="The four beginner traps in this exact job">

1. **No `enableCheckpointing`.** The job runs beautifully until a pod restarts, then every count starts from zero and nothing tells you.
2. **No `withIdleness`.** One quiet Kafka partition and the watermark never advances, so no window ever fires and the job emits *nothing*, forever, with no error. See [propagation and idleness](/docs/flink/watermarks/propagation-and-idleness).
3. **No `uid()`.** The first time you add an operator and try to restore from a savepoint, the state does not match and you lose it.
4. **Reaching for `ProcessWindowFunction` first.** It buffers every record in the window. `aggregate()` keeps one accumulator per window. On a busy page that is the difference between megabytes and gigabytes — see [window functions](/docs/flink/windows/window-functions).

</Callout>

## Running it for real

```bash
# package a fat JAR (shade plugin, connectors included)
mvn clean package

# submit to a cluster
flink run -d -p 4 target/flink-bible-examples-1.0.jar

# what is running
flink list

# stop it and keep the state
flink stop --savepointPath s3://bucket/savepoints <jobId>

# start again from that state
flink run -s s3://bucket/savepoints/savepoint-abc123 -d target/...jar
```

That last pair — `stop --savepointPath` then `run -s` — is how every stateful
Flink upgrade works. It is worth practising on a toy job before you need it at
2am. Details in [savepoints](/docs/flink/fault-tolerance/savepoints).

<Callout type="try">

Three experiments on Job 2, in increasing order of insight:

1. Set the out-of-orderness bound to `Duration.ZERO`. Feed it slightly unordered data. Watch results appear faster and be quietly wrong.
2. Kill a TaskManager mid-window. Watch the job restart, rewind Kafka offsets, and reproduce identical output.
3. Publish to only one of three Kafka partitions with `withIdleness` removed. Nothing is emitted, no error appears. Now you have seen the single most common Flink support ticket, on purpose, in a safe place.

</Callout>

<Callout type="remember">

Every Flink job is source → transform → key → sink, plus two lines that decide
whether it is real: a watermark strategy and `enableCheckpointing`.

</Callout>

## Next

**[Level 2 — the three clocks](/docs/flink/time/three-clocks)** — the idea everything else depends on.
