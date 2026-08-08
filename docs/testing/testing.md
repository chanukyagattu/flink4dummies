---
title: Testing Flink jobs
sidebar_label: Testing
description: Unit tests, operator harnesses, MiniCluster integration tests — and how to test the things that actually break.
---

# Testing Flink jobs

<PageMeta level="advanced" time="12 min" prereq={[['Savepoints', '/docs/flink/fault-tolerance/savepoints']]} />

<Objectives>

- Choose the right test level for a given piece of logic
- Drive watermarks and timers explicitly with an operator test harness
- Write the four tests that catch the failures this guide keeps describing

</Objectives>

## The three levels

```text
1. UNIT           plain JUnit over your function's logic
                  fast (milliseconds), no Flink runtime

2. HARNESS        OneInputStreamOperatorTestHarness
                  you control watermarks, timers, and state directly
                  fast (tens of ms), full Flink semantics

3. INTEGRATION    MiniClusterWithClientResource
                  a real cluster in-process, real checkpoints
                  slow (seconds), tests the whole job
```

Most teams write level 1 and level 3 and skip level 2 — which is a mistake, because
**level 2 is where you test time**, and time is where the bugs are.

## Level 1 — unit

Keep business logic in plain, testable objects and the Flink API thin around them.

```java
class FraudRulesTest {
    @Test
    void smallThenLargeIsSuspicious() {
        FraudRules rules = new FraudRules(1.00, 500.00);
        assertTrue(rules.isSuspicious(0.50, 900.00));
        assertFalse(rules.isSuspicious(50.00, 900.00));
    }
}
```

No Flink involved. If most of your logic can be tested this way, your job is well
factored.

## Level 2 — the operator test harness

This is the one worth learning. It gives you direct control over the things that
cause production incidents.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>${flink.version}</version>
  <type>test-jar</type>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-test-utils</artifactId>
  <version>${flink.version}</version>
  <scope>test</scope>
</dependency>
```

```java
class SessionTrackerTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Event, Session> harness;

    @BeforeEach
    void setUp() throws Exception {
        SessionTracker fn = new SessionTracker();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(fn),
            Event::userId,
            Types.STRING);
        harness.open();
    }

    @Test
    void sessionClosesAfterGap() throws Exception {
        // records with EXPLICIT event-time timestamps
        harness.processElement(new StreamRecord<>(event("u1", 1_000), 1_000));
        harness.processElement(new StreamRecord<>(event("u1", 2_000), 2_000));

        // nothing emitted yet — the gap has not elapsed in EVENT time
        assertThat(harness.extractOutputValues()).isEmpty();

        // drive the watermark forward past the session gap. THIS is the point
        // of the harness: you control time, precisely, with no sleeping.
        harness.processWatermark(new Watermark(2_000 + GAP + 1));

        List<Session> out = harness.extractOutputValues();
        assertThat(out).hasSize(1);
        assertThat(out.get(0).eventCount()).isEqualTo(2);
    }

    @Test
    void stateIsClearedAfterTheSessionCloses() throws Exception {
        harness.processElement(new StreamRecord<>(event("u1", 1_000), 1_000));
        harness.processWatermark(new Watermark(1_000 + GAP + 1));

        // the state leak test: no timers and no keyed state should remain
        assertThat(harness.numEventTimeTimers()).isZero();
        assertThat(harness.numKeyedStateEntries()).isZero();
    }

    @Test
    void stateSurvivesASnapshotAndRestore() throws Exception {
        harness.processElement(new StreamRecord<>(event("u1", 1_000), 1_000));

        // snapshot, close, and restore into a fresh harness — a real recovery
        OperatorSubtaskState snapshot = harness.snapshot(1L, 1_000L);
        harness.close();

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new SessionTracker()),
            Event::userId, Types.STRING);
        harness.initializeState(snapshot);
        harness.open();

        harness.processElement(new StreamRecord<>(event("u1", 2_000), 2_000));
        harness.processWatermark(new Watermark(2_000 + GAP + 1));

        // the session must contain BOTH events — the first one survived the restore
        assertThat(harness.extractOutputValues().get(0).eventCount()).isEqualTo(2);
    }
}
```

<Callout type="key">

Four harness methods do most of the work:

| Method | Tests |
| --- | --- |
| `processWatermark(new Watermark(t))` | Event-time timers, window firing, lateness |
| `setProcessingTime(t)` | Processing-time timers, without sleeping |
| `numKeyedStateEntries()` | **State leaks** — the bug that kills jobs months later |
| `snapshot()` + `initializeState()` | Recovery, without a cluster |

That third one is the highest-value assertion in this entire page. A test that state
returns to zero after the logical lifecycle completes catches the single most common
production failure in stateful streaming.

</Callout>

## Level 3 — MiniCluster integration tests

```java
class ClicksPerMinuteIT {

    @RegisterExtension
    static final MiniClusterExtension CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    @Test
    void countsClicksPerMinuteInEventTime() throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CollectSink.VALUES.clear();

        env.fromData(
              click("home", 1_000),
              click("home", 2_000),
              click("about", 3_000),
              click("home", 65_000))       // the next minute
           .assignTimestampsAndWatermarks(
               WatermarkStrategy.<Click>forMonotonousTimestamps()
                   .withTimestampAssigner((c, ts) -> c.eventTime()))
           .keyBy(Click::page)
           .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
           .aggregate(new CountAgg())
           .addSink(new CollectSink());

        env.execute();

        assertThat(CollectSink.VALUES)
            .containsExactlyInAnyOrder(
                new Result("home", 0, 2),
                new Result("about", 0, 1),
                new Result("home", 60_000, 1));
    }

    /** Sinks run on other threads; a static collection is the standard idiom. */
    private static class CollectSink implements SinkFunction<Result> {
        static final List<Result> VALUES =
            Collections.synchronizedList(new ArrayList<>());
        @Override public void invoke(Result value, Context ctx) {
            VALUES.add(value);
        }
    }
}
```

<Callout type="prod">

A bounded source in an integration test emits `Long.MAX_VALUE` as its final
watermark, which fires every open window. That is convenient — and it means **your
test does not exercise your watermark configuration at all**.

A job whose integration tests pass can still stall forever in production on an idle
partition. Watermark behaviour must be tested at the harness level, where you control
the watermarks yourself.

</Callout>

## The four tests every event-time job should have

These map directly to the failures in the [runbook](/docs/flink/production/runbook).

### 1. Out-of-order within the bound lands correctly

```java
harness.processElement(new StreamRecord<>(click(5_000), 5_000));
harness.processElement(new StreamRecord<>(click(3_000), 3_000));  // out of order
harness.processWatermark(new Watermark(60_001));
// assert BOTH are counted in the [0, 60s) window
```

### 2. Beyond the bound goes to the side output, not nowhere

```java
harness.processWatermark(new Watermark(60_001));                  // window fires
harness.processElement(new StreamRecord<>(click(30_000), 30_000)); // now late
assertThat(harness.getSideOutput(lateTag)).hasSize(1);
```

If this test fails by finding nothing anywhere, you have proved that your job silently
drops data.

### 3. An idle source does not stall the job

```java
// two inputs; only one produces
harness.processElement1(new StreamRecord<>(a(1_000), 1_000));
harness.processWatermark1(new Watermark(60_001));
// input 2 is silent — without idleness handling, nothing fires
harness.processWatermark2(Watermark.MAX_WATERMARK);   // simulate idleness
assertThat(harness.extractOutputValues()).isNotEmpty();
```

This is the regression test for the most common Flink outage.

### 4. State returns to zero

```java
// run a complete logical lifecycle
harness.processElement(...);
harness.processWatermark(new Watermark(farFuture));
assertThat(harness.numKeyedStateEntries()).isZero();
assertThat(harness.numEventTimeTimers()).isZero();
```

## Testing SQL

```java
class PageCountsSqlTest {

    @Test
    void countsPerMinute() {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment t = TableEnvironment.create(settings);

        // 'datagen' and 'values' connectors exist precisely for this
        t.executeSql("""
            CREATE TABLE clicks (
              page STRING,
              event_time TIMESTAMP_LTZ(3),
              WATERMARK FOR event_time AS event_time
            ) WITH ('connector' = 'values', 'data-id' = 'clicks-data')
            """);

        TableResult result = t.executeSql("""
            SELECT window_start, page, COUNT(*)
            FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
            GROUP BY window_start, page
            """);

        try (CloseableIterator<Row> it = result.collect()) {
            // assert on the emitted rows, INCLUDING their RowKind
        }
    }
}
```

Assert on `RowKind` as well as values. A query you believed was append-only but which
emits `-U`/`+U` will break an append sink in production, and this is where you find
out.

## Testing serialisers and schema evolution

Cheap, and it prevents a class of upgrade disaster.

```java
@Test
void stateTypeIsAPojoNotKryo() {
    TypeInformation<UserProfile> info = TypeInformation.of(UserProfile.class);
    assertThat(info).isInstanceOf(PojoTypeInfo.class);   // fails loudly on Kryo
}

@Test
void v2CanReadV1State() throws Exception {
    // 1. snapshot with the v1 class
    // 2. initializeState into an operator using the v2 class
    // 3. assert the values survived and the new field defaulted
}
```

And in CI:

```java
env.getConfig().disableGenericTypes();   // job submission fails on any Kryo type
```

<Callout type="prod" title="Testcontainers for the real thing">

For end-to-end confidence, run a real Kafka:

```java
@Container
static final KafkaContainer KAFKA =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
```

Then test the things only a real broker exposes: transactional sink commits,
`read_committed` visibility, offset rewind after a forced failure, and partition
reassignment. These are slow tests — a handful of them, run on merge rather than on
every commit.

</Callout>

<Expert>

**`OneInputStreamOperatorTestHarness` vs the keyed variant.** Use the keyed one for
anything with keyed state; it sets the key context per record, which the plain harness
does not. Testing a `KeyedProcessFunction` with the non-keyed harness produces
confusing `IllegalStateException`s about keyed state.

**Two-input harnesses.** `KeyedTwoInputStreamOperatorTestHarness` lets you drive two
watermarks independently, which is the only practical way to test the minimum rule,
`CoProcessFunction` logic, and broadcast state bootstrapping.

**`harness.getOutput()` includes watermarks.** The raw output queue contains
`StreamRecord` *and* `Watermark` elements. `extractOutputValues()` filters to records.
When testing watermark propagation, inspect the raw queue — that is how you assert
that your operator forwards watermarks correctly.

**Testing checkpoint semantics.** `MiniClusterWithClientResource` plus a source that
throws once, deterministically, after N records, lets you assert exactly-once
behaviour end to end. Combine with `RestartStrategies.fixedDelayRestart(1, 0)` so the
test terminates.

**Flink's own test utilities are worth reading.** `flink-test-utils` contains
`FailingSource`, `SuccessException` and similar helpers used by Flink's own test
suite, and they are the shortest path to writing a good recovery test.

</Expert>

<Callout type="remember">

Unit-test the logic, harness-test the time, MiniCluster-test the job. The harness is
where watermarks, timers, state leaks and recovery get tested — and a bounded source
in an integration test proves nothing about your watermark configuration.

</Callout>

## Next

**[Hands-on projects](/docs/flink/projects)** — build the things this guide describes.
