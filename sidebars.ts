import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * The Flink for Dummies sidebar.
 *
 * Ordering is pedagogical, not alphabetical: each level assumes only the levels
 * above it.
 *
 * These doc ids are RELATIVE to this repo's `docs/` directory, and both the
 * standalone site and any embedding site (e.g. stream-forge) point a docs
 * plugin instance at that same directory with `routeBasePath: 'docs/flink'`.
 * That keeps ids, routes and the absolute `/docs/flink/...` links in the
 * Markdown identical in both places — so this file can be shared verbatim.
 */
export const flinkSidebar: SidebarsConfig[string] = [
  'index',
  'quickstart',
  'learning-path',

  // Deliberately near the top: the runnable material should be discoverable
  // without scrolling past eleven levels of theory first.
  {
    type: 'category',
    label: '🧪 Hands-on projects',
    items: [
      'projects/index',
      'projects/clickstream',
      'projects/sessionization',
      'projects/fraud-detection',
      'projects/dynamic-rules',
      'projects/exactly-once-pipeline',
    ],
  },

  {
    type: 'category',
    label: '🟢 Level 0 — Before Flink',
    collapsed: false,
    items: [
      'foundations/what-is-an-event',
      'foundations/what-is-a-stream',
      'foundations/batch-vs-streaming',
      'foundations/why-this-is-hard',
      'foundations/flink-in-the-ecosystem',
    ],
  },
  {
    type: 'category',
    label: '🟢 Level 1 — Flink basics',
    items: [
      'basics/architecture',
      'basics/parallelism-and-subtasks',
      'basics/from-code-to-cluster',
      'basics/first-job',
    ],
  },
  {
    type: 'category',
    label: '🟡 Level 2 — Time',
    items: [
      'time/three-clocks',
      'time/out-of-order-and-late',
      'time/timestamp-assignment',
    ],
  },
  {
    type: 'category',
    label: '🟡 Level 3 — Watermarks',
    items: [
      'watermarks/what-is-a-watermark',
      'watermarks/generation',
      'watermarks/propagation-and-idleness',
      'watermarks/debugging',
    ],
  },
  {
    type: 'category',
    label: '🟡 Level 4 — Windows & aggregation',
    items: [
      'windows/why-windows',
      'windows/window-types',
      'windows/window-functions',
      'windows/triggers-and-lateness',
    ],
  },
  {
    type: 'category',
    label: '🟡 Level 5 — State',
    items: [
      'state/why-state',
      'state/keyed-state',
      'state/operator-and-broadcast-state',
      'state/ttl-and-growth',
      'state/state-backends',
      'state/serialization-and-evolution',
    ],
  },
  {
    type: 'category',
    label: '🟡 Level 6 — Timers',
    items: ['timers/timers'],
  },
  {
    type: 'category',
    label: '🔴 Level 7 — Joins & patterns',
    items: [
      'joins/joins',
      'patterns/cep',
    ],
  },
  {
    type: 'category',
    label: '🔴 Level 8 — Fault tolerance',
    items: [
      'fault-tolerance/failure-model',
      'fault-tolerance/checkpoints',
      'fault-tolerance/barriers-and-alignment',
      'fault-tolerance/savepoints',
      'fault-tolerance/exactly-once',
      'fault-tolerance/rescaling',
    ],
  },
  {
    type: 'category',
    label: '🔴 Level 9 — Running at scale',
    items: [
      'scale/backpressure',
      'scale/kafka-and-flink',
      'scale/async-io',
      'scale/performance',
    ],
  },
  {
    type: 'category',
    label: '🔴 Level 10 — Production',
    items: [
      'production/deployment',
      'production/observability',
      'production/runbook',
    ],
  },
  {
    type: 'category',
    label: '⚫ Level 11 — Internals',
    items: ['internals/how-flink-really-works'],
  },

  {
    type: 'category',
    label: '🟡 Table API & SQL',
    items: [
      'sql/table-api',
      'sql/streaming-patterns',
    ],
  },
  {
    type: 'category',
    label: '🔴 Testing',
    items: ['testing/testing'],
  },
  {
    type: 'category',
    label: '🏛 Architecture',
    items: [
      'architecture/lambda-vs-kappa',
      'architecture/patterns',
    ],
  },

  {
    type: 'category',
    label: '📎 Reference',
    items: [
      'reference/confusions',
      'reference/cheat-sheets',
      'reference/glossary',
      'reference/interview',
    ],
  },
];

const sidebars: SidebarsConfig = {flinkSidebar};

export default sidebars;
