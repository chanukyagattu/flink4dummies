---
title: What is an event?
sidebar_label: What is an event?
description: The smallest idea in streaming, explained without jargon — and why "a row in a table" and "an event" are not the same thing.
---

# What is an event?

<PageMeta level="beginner" time="6 min" />

<Objectives>

- Describe an event in one sentence, without using the word "stream"
- Explain why an event has *two* different times attached to it
- Recognise why "the current state of the world" and "the events that produced it" are different data

</Objectives>

## What is it?

An event is **a record that something happened, at a particular moment**.

That is the whole definition. Not "a message", not "a row", not "a Kafka thing".
Something happened, and we wrote it down.

```json
{
  "userId": "U123",
  "type": "purchase",
  "amount": 42.50,
  "eventTime": "2026-08-07T12:03:15.412Z"
}
```

Three things make this an event rather than just data:

1. **It is about the past.** It already happened. You cannot un-purchase it.
2. **It is immutable.** If the user refunds later, that is a *new* event, not an edit to this one.
3. **It has a time.** Not "when we stored it" — when it *happened*.

## Why do we need this idea?

Because most software throws the third point away, and then cannot answer
questions about it.

Consider a normal database table:

| user_id | balance |
| --- | --- |
| U123 | 108.50 |

That row tells you the answer *right now*. It cannot tell you:

- What was the balance at 12:03?
- How many times did it drop below zero this month?
- Did the fraud pattern happen before or after the password change?

The table stored the **result**. The events are the **reasons**. Once you delete
the reasons, no amount of cleverness gets them back.

<Callout type="mental">

A database row is a photograph. An event is a frame of film.

The photograph tells you what things look like now. The film tells you how they
got that way — and lets you replay it, in slow motion, from any point.

</Callout>

## Real-world analogy

A bank statement.

Your bank does not store "Chanukya has ₹8,412". It stores a list of every
deposit and withdrawal, forever, in order. Your balance is *derived* by adding
them all up.

This is not an implementation detail — it is the reason banks can answer "why is
this number what it is?" and your average CRUD app cannot.

## The two times every event has

This will matter enormously later, so meet it now.

```text
User taps "Buy" on a train, in a tunnel, at 12:03:15
   │
   │   phone has no signal for 4 minutes
   ▼
Event arrives at the server at 12:07:42
```

That single event now has two timestamps:

| Name | Value | Means |
| --- | --- | --- |
| **Event time** | 12:03:15 | When the thing actually happened |
| **Processing time** | 12:07:42 | When our system got around to seeing it |

They differ by 4 minutes and 27 seconds. On a bad day they differ by hours.

Nearly every hard problem in this guide is a consequence of that gap. We will
spend all of [Level 2](/docs/flink/time/three-clocks) on it.

<Callout type="mistake">

The most common beginner mistake in streaming is using processing time because
it is easier, then being surprised when results change every time you rerun the
same data.

Processing time is not reproducible. Run the same input twice and you get two
different answers, because "now" was different. Event time is reproducible
forever.

</Callout>

## Tiny example

Four events from one user, in the order they *happened*:

```text
12:00:01  U123  login
12:00:05  U123  view_item     item=847
12:00:09  U123  add_to_cart   item=847
12:00:31  U123  purchase      amount=42.50
```

From this you can compute things a `users` table never could: time from view to
purchase (26 seconds), whether they hesitated, whether this session looks like
the ones that convert.

Now here is the same data as it might actually arrive at your server:

```text
12:00:02  U123  login
12:00:06  U123  view_item
12:00:33  U123  purchase        ← arrived before add_to_cart
12:00:41  U123  add_to_cart     ← phone retried after a network blip
```

The purchase appears to happen before the add-to-cart. Any code that trusts
arrival order now computes nonsense. Any code that trusts the `eventTime` field
is fine.

<Callout type="hood" title="What a system actually stores">

In Flink, an event is called a **record**. Internally it is a
`StreamRecord<T>` — your object `T` plus an optional `long` timestamp in
milliseconds since the epoch. That `long` is the event time.

If you never set it, it is null, and every event-time feature in Flink silently
does nothing useful. Setting it correctly is
[Level 2, timestamp assignment](/docs/flink/time/timestamp-assignment).

</Callout>

## What happens at scale?

At ten events per second, none of this matters — you could process events with a
`for` loop.

At two million events per second, from thirty thousand devices, across four
regions, with phones that go through tunnels: the gap between event time and
processing time becomes the central engineering problem, and "just sort them"
stops being possible because the stream never ends and you never know if the
straggler is still coming.

That is the problem Flink exists to solve.

<Callout type="remember">

An event is an immutable fact about the past, stamped with the time it happened —
not the time you found out about it.

</Callout>

## Next

**[What is a stream?](/docs/flink/foundations/what-is-a-stream)** — what happens when the events never stop.
