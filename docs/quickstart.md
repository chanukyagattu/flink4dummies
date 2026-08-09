---
title: Quickstart — run Flink in 5 minutes
sidebar_label: ⚡ Quickstart (5 min)
description: Get a real Flink job running and producing output before you read a single word of theory.
---

# Quickstart

<PageMeta level="beginner" time="5 min" />

<Objectives>

- Have a Flink cluster running locally
- Have a real job running on it, producing output you can watch
- Know which three lines of that job are the ones that matter

</Objectives>

No theory on this page. Copy, paste, run, watch. The explanations are waiting for
you afterwards and they will make far more sense once you have seen the thing
move.

**You need:** Docker, and Java 17+ if you want to compile the job yourself.

---

## 1. Start a cluster (60 seconds)

```bash title="docker-compose.yml"
services:
  jobmanager:
    image: flink:2.3
    ports: ["8081:8081"]
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:2.3
    depends_on: [jobmanager]
    command: taskmanager
    scale: 2
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
```

```bash
docker compose up -d
open http://localhost:8081        # the Flink Web UI
```

You now have a JobManager and two TaskManagers with 8 task slots between them.
The UI is empty because nothing is running yet.

---

## 2. Run the bundled example (30 seconds)

Flink ships with examples. Run one before writing anything of your own:

```bash
docker compose exec jobmanager \
  flink run /opt/flink/examples/streaming/TopSpeedWindowing.jar
```

Watch the UI. A job appears, moves to **RUNNING**, and you can click into it to
see operators, subtasks, records in and out.

That is a distributed streaming job executing across two machines. It took you
ninety seconds.

---

## 3. Now your own job

Two files. This one counts words in a generated stream — the smallest program
that still contains every part of a real Flink job.

```xml title="pom.xml"
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.flink4dummies</groupId>
  <artifactId>quickstart</artifactId>
  <version>1.0</version>

  <properties>
    <flink.version>2.3.0</flink.version>
    <maven.compiler.release>17</maven.compiler.release>
  </properties>

  <dependencies>
    <!-- 'provided': the cluster already has these. Bundling them breaks things. -->
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
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-datagen</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
```

```java title="src/main/java/Quickstart.java"
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Quickstart {

  private static final String[] LINES = {
      "flink processes streams",
      "streams never end",
      "flink keeps state"
  };

  public static void main(String[] args) throws Exception {

    // 1. The environment. Everything hangs off this.
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    // 2. SOURCE — where records come from.
    //    5 lines per second, 60 of them, then stop.
    DataGeneratorSource<String> generator = new DataGeneratorSource<>(
        idx -> LINES[(int) (idx % LINES.length)],
        60,
        RateLimiterStrategy.perSecond(5),
        Types.STRING);

    DataStream<String> lines =
        env.fromSource(generator, WatermarkStrategy.noWatermarks(), "lines");

    // 3. TRANSFORM — stateless. One line in, many words out.
    DataStream<Tuple2<String, Long>> words = lines
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            for (String w : line.split(" ")) out.collect(Tuple2.of(w, 1L));
        })
        .returns(Types.TUPLE(Types.STRING, Types.LONG))
        .name("split");

    // 4. KEY + AGGREGATE — the first stateful thing here.
    //    keyBy sends every occurrence of a word to the SAME subtask,
    //    which is the only reason the running count can be correct.
    words.keyBy(t -> t.f0)
         .sum(1)
         .name("count")
         .print();

    // 5. Nothing above has run yet. THIS starts it.
    env.execute("quickstart");
  }
}
```

```bash
mvn clean package
```

Run it — either straight from your IDE (Flink starts a mini-cluster
automatically) or on the cluster you started in step 1:

```bash
docker compose cp target/quickstart-1.0.jar jobmanager:/tmp/
docker compose exec jobmanager flink run /tmp/quickstart-1.0.jar
docker compose logs -f taskmanager        # the output lands here
```

---

## 4. What you should see

```text
1> (flink,1)
2> (processes,1)
1> (streams,1)
1> (streams,2)        ← the count for "streams" CHANGED
2> (never,1)
1> (flink,2)
1> (keeps,1)
1> (streams,3)
```

Two things in that output are worth noticing right now.

**Every line is an update, not a final answer.** `(flink,1)` was not wrong — it
was correct given what had been seen so far. A streaming job emits a *stream of
results*, and this is the single biggest mental shift from batch.

**The `1>` and `2>` prefixes are subtask numbers.** Every occurrence of
`streams` landed on subtask 1, every time. That is `keyBy` doing its job: all
records for a key go to one place, which is what makes the running count
possible.

---

## 5. The three lines that matter

Out of that whole program, three lines carry the weight:

| Line | What it decides | Where it is explained |
| --- | --- | --- |
| `env.setParallelism(2)` | How many parallel copies of each operator run — and this is *not* "how many machines" | [Parallelism and subtasks](/docs/flink/basics/parallelism-and-subtasks) |
| `.keyBy(t -> t.f0)` | Which subtask owns which key's state. A network shuffle, not a function call. | [Keyed state](/docs/flink/state/keyed-state) |
| `env.execute(...)` | Nothing runs before this. Your `main()` builds a graph; it does not process data. | [From code to cluster](/docs/flink/basics/from-code-to-cluster) |

And one line that is conspicuously **missing**, which is the difference between
this toy and anything real:

```java
env.enableCheckpointing(30_000);
```

Without it, all that counted state is lost the moment a TaskManager restarts —
silently, with no error. That one line is [Level 8](/docs/flink/fault-tolerance/checkpoints).

---

## 6. Break it (30 seconds, and worth it)

While the job is running:

```bash
docker compose kill --signal=SIGKILL taskmanager-1
```

Watch the UI. The job fails and restarts. Because we never enabled
checkpointing, **the counts start again from zero** — and nothing anywhere tells
you that happened.

You have just seen, on purpose, the failure that this entire guide exists to
help you avoid.

```bash
docker compose down       # when you are finished
```

---

## Where next

<CardGrid>
  <Card to="/docs/flink/foundations/what-is-an-event" level="Recommended" title="Start the guide properly">
    Now that you have seen it run, the concepts have something to attach to.
    Begin at what an event is and work forward.
  </Card>
  <Card to="/docs/flink/basics/first-job" level="More code" title="A production-shaped job">
    The same idea with Kafka, event time, watermarks and checkpointing — and an
    explanation of why each line is there.
  </Card>
  <Card to="/docs/flink/projects" level="Learn by building" title="Five full projects">
    Clickstream, sessionization, fraud detection, dynamic rules, exactly-once.
    Each ends by breaking on purpose.
  </Card>
  <Card to="/docs/flink/learning-path" level="Not sure" title="Pick a route">
    Five reading paths depending on why you are here.
  </Card>
</CardGrid>

<Callout type="version">

Everything on this page is **Apache Flink 2.3**. If you are on the 1.20 LTS
line, the `DataGeneratorSource` import path differs and `OpenContext` is
`Configuration` — see the version notes throughout the guide. Anything older
than Flink 2.0 has a different API surface entirely.

Official reference: [Flink documentation](https://nightlies.apache.org/flink/flink-docs-stable/) ·
[First steps](https://nightlies.apache.org/flink/flink-docs-stable/docs/try-flink/local_installation/)

</Callout>
