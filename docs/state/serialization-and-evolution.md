---
title: Serialization and schema evolution
sidebar_label: Serialization & evolution
description: Why Kryo is quietly costing you half your throughput, and how to change a state class without losing state.
---

# Serialization and schema evolution

<PageMeta level="advanced" time="9 min" prereq={[['State backends', '/docs/flink/state/state-backends']]} />

<Objectives>

- Detect a Kryo fallback and understand why it matters
- Choose a serialiser deliberately: POJO, Avro, or Kryo
- Change a state class in production without losing state

</Objectives>

## Why this page exists

Serialisation is invisible until it is 40% of your CPU. It affects:

- **Throughput** — every network shuffle serialises every record
- **State access** — on RocksDB, every read and write
- **Checkpoint size and duration** — bigger encodings mean more bytes to upload
- **Whether you can change your code later** — schema evolution

## How Flink picks a serialiser

In order of preference:

| Your type | Serialiser | Speed |
| --- | --- | --- |
| `int`, `long`, `String`, primitives | Built-in | Fastest |
| `Tuple2`, `Tuple3`, … | Tuple | Very fast |
| A valid **POJO** | `PojoSerializer` | Fast, and evolvable |
| Avro `SpecificRecord` | Avro | Fast, and evolvable |
| Anything else | **Kryo** | **Slow, and not evolvable** |

### What counts as a POJO

Flink's rules are stricter than the common meaning of the word:

```java
public class Click {              // ✅ public class
    public String userId;         // ✅ public fields,
    public long eventTime;        //    or private with public getters AND setters

    public Click() {}             // ✅ public no-arg constructor
    public Click(String u, long t) { ... }
}
```

All of: public class, public no-arg constructor, and every field either public or
having a conventional getter *and* setter. Miss any one and you silently get Kryo.

<Callout type="mistake" title="Java records are not POJOs">

```java
public record Click(String userId, long eventTime) {}   // → Kryo
```

Records are immutable and have no no-arg constructor and no setters, so they fail
the POJO test. They are pleasant to write and quietly slow in state and shuffles.

Options, in order of preference: use Avro-generated classes for anything stored in
state; register a custom `TypeInformation`; or use a POJO for state types while
keeping records for internal, non-persisted values.

</Callout>

### Detecting the fallback

```java
env.getConfig().disableGenericTypes();
// Flink now FAILS at job submission on any type that would use Kryo,
// instead of silently degrading.
```

Turn this on in CI. It converts a slow, invisible performance problem into a loud
build failure. There is a good argument for leaving it on in production too — a
type that falls back to Kryo is also a type you cannot evolve.

You can also just look:

```java
TypeInformation<Click> info = TypeInformation.of(Click.class);
System.out.println(info);
// PojoType<Click, fields = [eventTime: Long, userId: String]>   ✅
// GenericType<Click>                                            ❌ Kryo
```

## The performance difference

Rough orders of magnitude, per record — measure your own, but the ranking is
stable:

| Serialiser | Relative cost | Bytes |
| --- | --- | --- |
| Built-in / Tuple | 1× | smallest |
| POJO | 1.2× | small |
| Avro | 1.5× | small, schema-aware |
| **Kryo** | **4–10×** | larger |

On a job doing a million records per second through a keyed shuffle, moving one
hot type from Kryo to POJO is regularly a double-digit percentage throughput
improvement for a one-line change.

## Schema evolution

You deployed a job. Its state contains `UserProfile` objects. Now you need a new
field. Will your savepoint still restore?

```java
// v1                                v2
class UserProfile {                  class UserProfile {
    public String id;                    public String id;
    public String name;                  public String name;
}                                        public String country;   // ← new
```

### What is allowed

| Change | POJO | Avro | Kryo |
| --- | --- | --- | --- |
| **Add** a field | ✅ (null / default) | ✅ (with a default) | ❌ |
| **Remove** a field | ✅ (ignored) | ✅ | ❌ |
| **Rename** a field | ❌ (looks like remove + add) | ✅ (with an alias) | ❌ |
| **Change** a field's type | ❌ | ⚠️ only per Avro's promotion rules | ❌ |
| Change the **class name** | ❌ | ❌ | ❌ |

<Callout type="key">

**Kryo state cannot be evolved. At all.**

If a class serialised by Kryo changes in any way, restore fails or — worse —
silently deserialises garbage. This, not performance, is the strongest reason to
care about serialisers: it determines whether you can change your code later.

</Callout>

### The safe procedure

1. **Take a savepoint** before deploying. Always. This is your undo button.
2. **Add fields, never rename or retype.** To rename, add the new field, dual-write both for a release, migrate, then remove the old one in a later release.
3. **Test the restore in staging** against a *production-shaped* savepoint, not an empty one. A restore that works on empty state proves nothing.
4. **Keep `uid()` stable.** Changing an operator's uid orphans its state entirely — the state is still in the savepoint, but nothing claims it.

```java
// Restore with an explicit escape hatch, used deliberately and temporarily
flink run -s s3://bucket/savepoints/savepoint-abc123 \
          --allowNonRestoredState \
          my-job.jar
```

<Callout type="mistake">

Using `--allowNonRestoredState` as a habit because a restore failed.

It tells Flink "discard any state that no operator claims". Sometimes that is
exactly right — you removed an operator on purpose. Often it means you changed a
`uid()` by accident and have just silently thrown away the state that flag was
supposed to protect.

Read the error first. Understand *which* state is unclaimed. Then decide.

</Callout>

## Avro when evolution matters

If your state schema will change — and it will — Avro is the most robust option.

```java
// generated from a .avsc schema
public class UserProfile extends SpecificRecordBase { ... }
```

Flink detects `SpecificRecord` and uses `AvroSerializer`, which carries the writer
schema in the savepoint and resolves it against the reader schema on restore.
Aliases give you real renames; defaults give you real additions.

The cost is a build-time code-generation step and a schema file to maintain. On
any long-lived stateful job that is a good trade.

<Callout type="hood" title="How restore actually resolves a serialiser">

A savepoint stores, for each state descriptor, a **serialiser snapshot** — a
serialised description of the writer's schema. On restore Flink calls
`resolveSchemaCompatibility(oldSnapshot)` and gets one of:

- `COMPATIBLE_AS_IS` — proceed
- `COMPATIBLE_AFTER_MIGRATION` — read with the old serialiser, write with the new one; this happens lazily as entries are touched
- `INCOMPATIBLE` — restore fails

`KryoSerializer` returns `INCOMPATIBLE` for essentially any change, because Kryo's
encoding depends on field order and registered class IDs and carries no schema.
That is the mechanical reason for the rule above.

</Callout>

<Expert>

**Register Kryo types if you must use Kryo.** `env.getConfig().registerKryoType(Foo.class)`
replaces the full class name written with every instance with a small integer ID.
For deeply nested Kryo objects this is a meaningful size reduction — but it also
makes the encoding depend on registration *order*, which becomes another thing you
must not change.

**`disableGenericTypes` and connectors.** Some connector or third-party types
legitimately need Kryo. If enabling the flag fails on a type you do not control,
register a custom serialiser for that type rather than abandoning the check.

**Serialisers are stateful and not thread-safe.** Flink duplicates them per
operator. If you write a custom `TypeSerializer`, implement `duplicate()`
correctly — returning `this` when the serialiser has mutable internal buffers is a
data-corruption bug that appears only under parallelism.

**State Processor API for hard migrations.** When evolution is impossible — a
renamed class, a restructured object graph — read the old savepoint with the State
Processor API, transform the objects in a batch job, and write a new savepoint.
This is the supported escape hatch, and it turns "we have to drop state" into an
afternoon of work.

</Expert>

<Callout type="remember">

Kryo is slow and cannot be evolved. Turn on `disableGenericTypes` in CI. Add
fields, never rename. Take a savepoint before every deploy, and test restores
against production-shaped state.

</Callout>

## Next

**[Level 6 — timers](/docs/flink/timers)** — scheduling the future.
