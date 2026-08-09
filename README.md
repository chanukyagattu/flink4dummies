# Flink for Dummies

**Apache Flink explained simply enough for a beginner, deeply enough for an expert.**

📖 **[Read it →](https://chanukyagattu.github.io/flink4dummies/)**

Not an API reference — a textbook. You start at *"what is an event?"* and finish
able to design, tune, debug, recover and rescale a stateful streaming system that
handles real money and real traffic.

**61 pages · 4 interactive labs · 5 runnable projects · baselined on Apache Flink 2.3**

⚡ **In a hurry?** [Quickstart](https://chanukyagattu.github.io/flink4dummies/docs/flink/quickstart) — a real Flink job running on a real cluster in five minutes, before any theory.

---

## Why this exists

Most Flink material is one of two things: a quickstart that gets you to
`WordCount` and stops, or the official reference, which is excellent and assumes
you already know what a watermark is *for*.

This sits in between. Every concept gets a plain-language explanation, a
real-world analogy, a diagram and a tiny example **before** any code — then the
internals, the failure modes, and what changes at scale.

Every page answers the same five questions:

```
1. What problem does this solve?
2. What is the mental model?
3. What is Flink doing internally?
4. What happens when it goes wrong?
5. What changes when the system becomes huge?
```

## Interactive labs

Four widgets that run real Flink logic in the browser, not animations.

| Lab | What it does |
| --- | --- |
| **Watermark Lab** | Step a stream forward one record at a time. Watch the watermark advance, windows fire, and records go late or get dropped. Every number uses Flink's actual `maxTs − bound − 1ms` rule. |
| **KeyBy & Rescaling Lab** | A faithful port of `MathUtils.murmurHash` and `KeyGroupRangeAssignment`. The subtask assignments it shows are the ones your job would really produce. Change parallelism and see exactly which state moves. |
| **Checkpoint Lab** | Ten-step walkthrough of a barrier travelling through a topology, aligned vs unaligned, then a crash and a restore. |
| **Backpressure Lab** | Computes `busy` / `backPressured` / `idle` the way the Flink UI reports them. Practise finding the bottleneck. |

## What's covered

```
Level 0   Events, streams, batch vs streaming, why streaming is hard
Level 1   Architecture, parallelism, subtasks, your first job
Level 2   Event time, processing time, out-of-order, lateness
Level 3   Watermarks — generation, propagation, idleness, debugging
Level 4   Windows, triggers, incremental aggregation
Level 5   Keyed / operator / broadcast state, TTL, backends, serialization
Level 6   Timers
Level 7   Joins and CEP
Level 8   Checkpoints, barriers, savepoints, exactly-once, rescaling
Level 9   Backpressure, Kafka, async I/O, performance
Level 10  Deployment, observability, production runbook
Level 11  How Flink really works — one event's complete journey

Plus       Table API & SQL · testing · 5 hands-on projects ·
           6 reference architectures · confusions · cheat sheets ·
           glossary · interview questions (beginner → staff)
```

## Where to start

| You are | Start at |
| --- | --- |
| Want to run it first | [Quickstart (5 min)](https://chanukyagattu.github.io/flink4dummies/docs/flink/quickstart) |
| New to streaming | [What is an event?](https://chanukyagattu.github.io/flink4dummies/docs/flink/foundations/what-is-an-event) |
| Already writing Flink jobs | [Watermarks](https://chanukyagattu.github.io/flink4dummies/docs/flink/watermarks/what-is-a-watermark) |
| Debugging production right now | [Runbook](https://chanukyagattu.github.io/flink4dummies/docs/flink/production/runbook) |
| Preparing for an interview | [Confusions](https://chanukyagattu.github.io/flink4dummies/docs/flink/reference/confusions) → [Interview questions](https://chanukyagattu.github.io/flink4dummies/docs/flink/reference/interview) |
| A learn-by-building type | [Five projects](https://chanukyagattu.github.io/flink4dummies/docs/flink/projects) |

## Running locally

```bash
npm install
npm start          # http://localhost:3000/flink4dummies/
npm run build      # fails on any broken link
```

Requires Node 18+.

## Repo layout

```
docs/                    the 60 pages (Markdown + MDX components)
src/components/Flink/    the 4 labs + shared primitives
src/css/flink-theme.css  the charcoal / muted-blue / sage theme
src/pages/index.tsx      landing page
sidebars.ts              shared by this site and any embedding site
integration/             sync script for embedding into another Docusaurus site
INTEGRATION.md           how StreamForge embeds this guide
```

## Embedding this guide elsewhere

The content is designed to be mounted as a second Docusaurus docs instance in
another site — it is embedded in
[StreamForge](https://chanukyagattu.github.io/stream-forge/) this way. See
[INTEGRATION.md](./INTEGRATION.md).

## Contributing

Corrections are very welcome, particularly:

- anything factually wrong or out of date against current Flink
- explanations that did not land — those are bugs
- production war stories worth adding to the runbook

Open an issue or a PR. CI builds the site and fails on broken links.

## Licence

Documentation is [CC BY 4.0](./LICENSE). Code samples are additionally MIT, so
you can lift them into your own projects freely.

Apache Flink, Apache Kafka and Apache Iceberg are trademarks of the Apache
Software Foundation. This project is not affiliated with or endorsed by the ASF.
