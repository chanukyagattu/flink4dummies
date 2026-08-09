---
title: Async I/O
sidebar_label: Async I/O
description: Why a synchronous database call in map() destroys throughput, and how to call external systems properly.
---

# Async I/O

<PageMeta level="advanced" time="8 min" prereq={[['Kafka & Flink', '/docs/flink/scale/kafka-and-flink']]} docs="docs/dev/datastream/operators/asyncio/" />

<Objectives>

- Quantify why a blocking call in `map()` caps your throughput
- Implement `AsyncFunction` with correct timeout and error handling
- Choose between ordered and unordered mode on requirement, not habit

</Objectives>

## The arithmetic

```java
// ❌ the natural thing to write, and it is a disaster
stream.map(order -> {
    Customer c = database.lookup(order.customerId());   // 10 ms round trip
    return new Enriched(order, c);
});
```

Each subtask processes records **one at a time** in its mailbox thread. A 10ms call
means:

```text
1 record / 10 ms = 100 records per second, per subtask

parallelism 32 → 3,200 records/s

your topic produces 200,000 records/s
```

You are 60× short, and the CPU is at 3% because every thread is asleep waiting on a
socket.

<Callout type="key">

More parallelism does not fix this. It creates more threads that are all asleep, and
more concurrent connections to a database that was not asking for them.

The problem is not throughput per thread. It is that the thread is **blocked**.

</Callout>

There is a second, worse consequence. The mailbox thread also processes
[checkpoint barriers](/docs/flink/fault-tolerance/barriers-and-alignment). A blocked
`map()` blocks the barrier, which lengthens alignment, which times out checkpoints.
A slow lookup becomes a fault-tolerance failure.

## Async I/O

Send many requests concurrently; complete each as it returns.

```java
public class CustomerLookup extends RichAsyncFunction<Order, Enriched> {

    private transient AsyncDatabaseClient client;

    @Override
    public void open(OpenContext ctx) {
        client = new AsyncDatabaseClient(...);   // must be a genuinely async client
    }

    @Override
    public void asyncInvoke(Order order, ResultFuture<Enriched> future) {
        client.lookupAsync(order.customerId())
              .thenAccept(customer ->
                  future.complete(List.of(new Enriched(order, customer))))
              .exceptionally(ex -> {
                  future.completeExceptionally(ex);
                  return null;
              });
        // note: this method RETURNS IMMEDIATELY. Nothing blocks.
    }

    @Override
    public void timeout(Order order, ResultFuture<Enriched> future) {
        // ALWAYS implement this. The default fails the job.
        future.complete(List.of(new Enriched(order, Customer.UNKNOWN)));
    }

    @Override
    public void close() {
        client.close();
    }
}
```

```java
AsyncDataStream.unorderedWait(
    stream,
    new CustomerLookup(),
    5, TimeUnit.SECONDS,   // timeout per request
    100);                  // capacity: max in-flight requests per subtask
```

Now:

```text
100 concurrent requests × (1 / 10 ms) = 10,000 records/s per subtask
parallelism 32 → 320,000 records/s
```

A hundredfold improvement from the same 10ms backend, because the thread stops
waiting.

<Callout type="mistake" title="Wrapping a blocking client in a thread pool is not async">

```java
// ❌ this is NOT async I/O
public void asyncInvoke(Order o, ResultFuture<Enriched> f) {
    executor.submit(() -> f.complete(List.of(blockingClient.lookup(o))));
}
```

It works, and it moves the blocking to a pool you now have to size, monitor and
shut down. Your concurrency is capped by the pool, the threads are still blocked,
and you have added a queue that is invisible to Flink's backpressure mechanism.

Use a client with a genuinely non-blocking API — an async JDBC driver, an async
HTTP client, the AWS SDK v2 async clients. If none exists, the thread pool is an
acceptable last resort, but size it deliberately and never larger than the target
system's capacity.

</Callout>

## Ordered vs unordered

<Compare>
  <CompareCard title="unorderedWait" rows={[
    ['Emits', 'As soon as each result is ready'],
    ['Order', 'NOT preserved — fast lookups overtake slow ones'],
    ['Latency', 'Lowest'],
    ['State', 'Smaller'],
    ['Watermarks', 'Order is still preserved relative to watermarks'],
    ['Use for', 'Enrichment where order does not matter — the common case'],
  ]} />
  <CompareCard title="orderedWait" rows={[
    ['Emits', 'In input order — a fast result waits for slower predecessors'],
    ['Order', 'Preserved exactly'],
    ['Latency', 'Higher — head-of-line blocking'],
    ['State', 'Larger — completed results are buffered'],
    ['Watermarks', 'Preserved'],
    ['Use for', 'Downstream logic that genuinely depends on record order'],
  ]} />
</Compare>

Default to `unorderedWait`. Genuine order dependence after an enrichment step is
rarer than people assume, and `orderedWait` reintroduces head-of-line blocking —
one slow lookup stalls everything behind it.

Note the nuance in the table: even in unordered mode, records never cross a
**watermark** boundary. Event-time correctness is preserved either way.

## Capacity and timeout

```java
AsyncDataStream.unorderedWait(stream, fn, timeout, timeUnit, capacity);
```

**Capacity** is the maximum in-flight requests per subtask. It is also your
backpressure valve: when it is full, the operator stops accepting records and
pressure propagates upstream naturally.

```text
total concurrent load on the backend = capacity × parallelism

capacity 100, parallelism 32 → 3,200 concurrent requests
```

<Callout type="mistake">

Setting capacity high "for throughput" without checking what the backend can take.
`capacity=1000` at parallelism 64 means 64,000 concurrent requests. You have built a
denial-of-service tool aimed at your own database.

Size it as: `capacity = target_backend_concurrency / parallelism`. Then measure the
backend's latency under that load — if p99 latency climbs, you are past its capacity
and throughput will get worse, not better.

</Callout>

**Timeout** must be handled. The default `timeout()` implementation **fails the
job**, which turns a slow backend into a restart loop.

```java
@Override
public void timeout(Order order, ResultFuture<Enriched> future) {
    timeoutCounter.inc();                                  // measure it
    future.complete(List.of(new Enriched(order, Customer.UNKNOWN)));  // degrade
    // or: future.complete(Collections.emptyList());       // drop
    // or: future.completeExceptionally(...)               // fail deliberately
}
```

Decide explicitly: degrade, drop, or fail. All three are legitimate; silently
failing the job is not.

## Retries

```java
AsyncRetryStrategy<Enriched> retry =
    new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<Enriched>(3, 100L)
        .ifException(ex -> ex instanceof TimeoutException)
        .build();

AsyncDataStream.unorderedWaitWithRetry(stream, fn, 5, TimeUnit.SECONDS, 100, retry);
```

Retries consume capacity, so effective concurrency drops while retrying. If the
backend is failing, retries make the pressure worse — pair them with a circuit
breaker in the client rather than retrying into a wall.

<Callout type="prod" title="Cache before you call">

The cheapest external call is the one you do not make.

```java
private transient Cache<String, Customer> cache;   // Caffeine, size-bounded

@Override
public void asyncInvoke(Order o, ResultFuture<Enriched> f) {
    Customer cached = cache.getIfPresent(o.customerId());
    if (cached != null) {
        f.complete(List.of(new Enriched(o, cached)));   // no I/O at all
        return;
    }
    client.lookupAsync(o.customerId()).thenAccept(c -> {
        cache.put(o.customerId(), c);
        f.complete(List.of(new Enriched(o, c)));
    });
}
```

On a realistic access distribution, a modest bounded cache commonly removes 90%+ of
lookups. Bound it by size, give it a TTL that matches how stale you can tolerate,
and expose the hit rate as a metric.

Note the trade: cached values are not reproducible across a replay. If the
enrichment must be historically exact, use a
[temporal join](/docs/flink/joins) instead.

</Callout>

<Expert>

**Async I/O state.** In-flight requests are stored in operator state and included in
checkpoints. On restore they are **re-issued**, which means your external call must
be idempotent or harmless to repeat. A lookup is fine; a POST that creates something
is not.

**Async I/O does not create threads.** The operator does not manage a thread pool —
completion happens on whatever thread your async client uses, and results are handed
back to the mailbox via the `ResultFuture`. This is why the client must be genuinely
async: Flink provides the plumbing, not the concurrency.

**Never mutate keyed state from a callback.** The callback runs on the client's
thread, outside the key context. Keyed state access from there is a data race. Do
the state work in `processElement` of a subsequent operator, or use the ordered
variant and handle state downstream.

**Async I/O and watermarks.** The operator holds a watermark until all requests
issued before it have completed, preserving event-time semantics. A permanently
stuck request therefore stalls the watermark — another reason the timeout must be
finite and handled.

**Table API lookup joins use this underneath.** `FOR SYSTEM_TIME AS OF proc_time`
with an async-capable connector compiles to an Async I/O operator, with capacity and
timeout exposed as `lookup.async` options. Same mechanism, less code.

</Expert>

<Callout type="remember">

Never block the mailbox thread. Async I/O turns 100 records/s into 10,000 with the
same backend. Use `unorderedWait`, always implement `timeout()`, size capacity from
the backend's real limits, and cache first.

</Callout>

## Next

**[Performance](/docs/flink/scale/performance)** — the tuning that actually matters.
