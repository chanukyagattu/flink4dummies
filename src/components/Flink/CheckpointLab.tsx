import React, {useMemo, useState} from 'react';

/* =========================================================================
   CheckpointLab — walk a checkpoint barrier through a real topology,
   then kill a TaskManager and watch the job come back.

   Toggle ALIGNED vs UNALIGNED to see the one thing that actually differs:
   what happens to the fast channel while the slow channel catches up.
   ========================================================================= */

type NodeState = 'running' | 'blocked' | 'snapshot' | 'acked' | 'crashed' | 'restoring';

type Node = {id: string; label: string; sub: string; x: number; y: number};

const NODES: Node[] = [
  {id: 'src0', label: 'Source', sub: 'kafka p0',  x: 40,  y: 40},
  {id: 'src1', label: 'Source', sub: 'kafka p1',  x: 40,  y: 150},
  {id: 'agg',  label: 'KeyedProcess', sub: 'keyed state', x: 300, y: 95},
  {id: 'sink', label: 'Sink',   sub: '2PC / txn', x: 570, y: 95},
];

const EDGES: Array<[string, string]> = [
  ['src0', 'agg'],
  ['src1', 'agg'],
  ['agg', 'sink'],
];

type Step = {
  title: string;
  narration: string;
  node: Partial<Record<string, NodeState>>;
  barriers: string[];          // edges (as "a->b") currently carrying the barrier
  buffered?: number;           // records held in-flight / buffered
  jm: string;                  // JobManager status line
  alignedOnly?: boolean;
  unalignedOnly?: boolean;
};

function buildSteps(unaligned: boolean): Step[] {
  const steps: Step[] = [
    {
      title: 'Steady state',
      narration:
        'The job is processing records. Each source has a Kafka offset. The KeyedProcess operator holds keyed state in the state backend. Nothing is durable yet — if this TaskManager died right now, all of that state would be gone.',
      node: {src0: 'running', src1: 'running', agg: 'running', sink: 'running'},
      barriers: [],
      jm: 'CheckpointCoordinator idle — next trigger in 30s',
    },
    {
      title: 'Coordinator triggers checkpoint 42',
      narration:
        'The CheckpointCoordinator on the JobManager decides it is time. It sends a trigger message to every SOURCE task only — never to the middle of the graph. That is the whole trick: the barrier rides the data path, so no operator ever has to be paused globally.',
      node: {src0: 'snapshot', src1: 'snapshot', agg: 'running', sink: 'running'},
      barriers: [],
      jm: 'triggerCheckpoint(42) → sources',
    },
    {
      title: 'Sources snapshot their offsets and inject the barrier',
      narration:
        'Each source records its current Kafka offset as its state, then injects barrier 42 into its output stream, right between two records. Everything before the barrier belongs to checkpoint 42; everything after it does not. The sources keep reading immediately — no stall.',
      node: {src0: 'acked', src1: 'acked', agg: 'running', sink: 'running'},
      barriers: ['src0->agg', 'src1->agg'],
      jm: 'ack src0 (offset 8,214) · ack src1 (offset 7,903)',
    },
    {
      title: 'Barrier from partition 0 arrives — partition 1 is lagging',
      narration: unaligned
        ? 'Channel 0 delivers barrier 42. Channel 1 is slow. In UNALIGNED mode the operator does not wait. It forwards the barrier downstream straight away and snapshots the in-flight records still sitting in the network buffers of the not-yet-barriered channel as part of the checkpoint.'
        : 'Channel 0 delivers barrier 42. Channel 1 is slow. In ALIGNED mode the operator stops consuming channel 0 and buffers whatever arrives there, so that no post-barrier record can be folded into pre-barrier state. This wait is the alignment, and its duration is the metric that spikes when you are backpressured.',
      node: {src0: 'running', src1: 'running', agg: unaligned ? 'snapshot' : 'blocked', sink: 'running'},
      barriers: unaligned ? ['agg->sink'] : ['src1->agg'],
      buffered: unaligned ? 0 : 6,
      jm: 'checkpoint 42 in progress — alignment',
    },
    {
      title: unaligned ? 'In-flight data is part of the snapshot' : 'Barrier from partition 1 arrives — alignment done',
      narration: unaligned
        ? 'The operator writes its keyed state AND the buffered in-flight records into the checkpoint. The checkpoint is bigger, but its duration no longer depends on how backpressured the slowest channel is. That is the trade: size and write amplification in exchange for predictable checkpoint time.'
        : 'Both channels have now delivered barrier 42. The operator has seen exactly the pre-barrier prefix of both inputs. It snapshots its keyed state asynchronously — the synchronous phase is just a copy-on-write handle flip — and forwards the barrier downstream.',
      node: {src0: 'running', src1: 'running', agg: 'snapshot', sink: 'running'},
      barriers: ['agg->sink'],
      jm: 'ack agg (state 1.4 GB, async upload in progress)',
    },
    {
      title: 'Sink pre-commits',
      narration:
        'The barrier reaches the sink. A two-phase-commit sink flushes its buffered writes and PRE-COMMITS its transaction — the data is written to Kafka/S3 but is not yet readable by consumers. It reports success to the coordinator and waits.',
      node: {src0: 'running', src1: 'running', agg: 'acked', sink: 'snapshot'},
      barriers: [],
      jm: 'ack sink (txn pre-committed)',
    },
    {
      title: 'Checkpoint 42 complete',
      narration:
        'Every task has acknowledged. The coordinator writes the checkpoint metadata file that points at all the state handles, and only then calls notifyCheckpointComplete on every task. The sink commits its transaction and the data becomes visible. This ordering is the entire basis of end-to-end exactly-once.',
      node: {src0: 'running', src1: 'running', agg: 'acked', sink: 'acked'},
      barriers: [],
      jm: '✔ checkpoint 42 COMPLETE — metadata at s3://…/chk-42/_metadata',
    },
    {
      title: '💥 TaskManager crashes',
      narration:
        'The machine running the KeyedProcess subtask dies. Its heap, its RocksDB working directory, its Kafka read position — all gone. The JobManager notices via missed heartbeats and fails the whole job graph (or just the failover region, if you configured region failover).',
      node: {src0: 'crashed', src1: 'crashed', agg: 'crashed', sink: 'crashed'},
      barriers: [],
      jm: '✖ TaskManager lost — restart strategy: fixed-delay, attempt 1/3',
    },
    {
      title: 'Restore from checkpoint 42',
      narration:
        'The scheduler redeploys every task and hands each subtask its slice of the checkpoint. The KeyedProcess subtask reloads its key groups from s3://…/chk-42/. The sources reset their Kafka offsets to 8,214 and 7,903 — the exact positions recorded when the barrier was injected.',
      node: {src0: 'restoring', src1: 'restoring', agg: 'restoring', sink: 'restoring'},
      barriers: [],
      jm: 'restoreLatestCheckpointedState(42)',
    },
    {
      title: 'Replay and continue',
      narration:
        'Processing resumes from those offsets. Records after 8,214 are read from Kafka a SECOND time — Flink reprocesses them. That is not a bug; it is the mechanism. Correctness comes from the fact that state was rewound to the matching point, so reprocessing lands the system in exactly the state it would have reached without the crash.',
      node: {src0: 'running', src1: 'running', agg: 'running', sink: 'running'},
      barriers: [],
      jm: 'RUNNING — 1 restart, lag recovering',
    },
  ];
  return steps;
}

const STATE_STYLE: Record<NodeState, {fill: string; stroke: string; tag: string}> = {
  running:   {fill: 'var(--fb-surface-2)',  stroke: 'var(--fb-border)',   tag: 'running'},
  blocked:   {fill: 'var(--fb-amber-soft)', stroke: 'var(--fb-amber)',    tag: 'aligning — channel blocked'},
  snapshot:  {fill: 'var(--fb-blue-soft)',  stroke: 'var(--fb-blue)',     tag: 'snapshotting'},
  acked:     {fill: 'var(--fb-sage-soft)',  stroke: 'var(--fb-sage)',     tag: 'acked ✔'},
  crashed:   {fill: 'var(--fb-rose-soft)',  stroke: 'var(--fb-rose)',     tag: 'lost 💥'},
  restoring: {fill: 'var(--fb-violet-soft)',stroke: 'var(--fb-violet)',   tag: 'restoring'},
};

export default function CheckpointLab() {
  const [unaligned, setUnaligned] = useState(false);
  const [i, setI] = useState(0);
  const steps = useMemo(() => buildSteps(unaligned), [unaligned]);
  const step = steps[Math.min(i, steps.length - 1)];
  const pos = Object.fromEntries(NODES.map((n) => [n.id, n]));

  return (
    <div className="fb-sim">
      <div className="fb-sim__head">
        <span className="fb-sim__title">🧪 Checkpoint Lab</span>
        <span className="fb-sim__hint">Step {i + 1} of {steps.length}</span>
      </div>

      <div className="fb-sim__controls">
        <div className="fb-sim__control">
          <label htmlFor="cp-mode">Checkpoint mode</label>
          <select id="cp-mode" value={unaligned ? 'u' : 'a'}
                  onChange={(e) => {setUnaligned(e.target.value === 'u'); setI(0);}}>
            <option value="a">Aligned (default)</option>
            <option value="u">Unaligned</option>
          </select>
        </div>
        <div className="fb-sim__control" style={{alignSelf: 'end'}}>
          <div className="fb-btn-row" style={{marginTop: 0}}>
            <button className="fb-btn" disabled={i === 0} onClick={() => setI((v) => v - 1)}>← Back</button>
            <button className="fb-btn fb-btn--primary" disabled={i >= steps.length - 1}
                    onClick={() => setI((v) => v + 1)}>Next →</button>
            <button className="fb-btn" onClick={() => setI(0)}>↺</button>
          </div>
        </div>
      </div>

      <div className="fb-sim__stage">
        <svg viewBox="0 0 760 230" role="img" aria-label={step.title}>
          {/* edges */}
          {EDGES.map(([a, b]) => {
            const from = pos[a], to = pos[b];
            const x1 = from.x + 150, y1 = from.y + 30;
            const x2 = to.x, y2 = to.y + 30;
            const active = step.barriers.includes(`${a}->${b}`);
            return (
              <g key={`${a}->${b}`}>
                <line x1={x1} y1={y1} x2={x2} y2={y2}
                      stroke={active ? 'var(--fb-blue)' : 'var(--fb-border)'}
                      strokeWidth={active ? 2.5 : 1.5} />
                {active && (
                  <g>
                    <rect x={(x1 + x2) / 2 - 5} y={(y1 + y2) / 2 - 16} width={10} height={32}
                          rx={2} fill="var(--fb-blue)" />
                    <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 22} fontSize="10"
                          fill="var(--fb-blue)" textAnchor="middle" fontWeight="700">
                      barrier 42
                    </text>
                  </g>
                )}
              </g>
            );
          })}

          {/* buffered records indicator */}
          {step.buffered ? (
            <g>
              <text x={225} y={30} fontSize="10.5" fill="var(--fb-amber)" textAnchor="middle" fontWeight="600">
                {step.buffered} records buffered
              </text>
              {Array.from({length: step.buffered}).map((_, k) => (
                <circle key={k} cx={200 + k * 9} cy={44} r={3.2} fill="var(--fb-amber)" />
              ))}
            </g>
          ) : null}

          {/* nodes */}
          {NODES.map((n) => {
            const st = (step.node[n.id] ?? 'running') as NodeState;
            const s = STATE_STYLE[st];
            return (
              <g key={n.id}>
                <rect x={n.x} y={n.y} width={150} height={60} rx={9} fill={s.fill} stroke={s.stroke} strokeWidth={1.6} />
                <text x={n.x + 12} y={n.y + 22} fontSize="12.5" fill="var(--fb-text)" fontWeight="650">{n.label}</text>
                <text x={n.x + 12} y={n.y + 37} fontSize="10.5" fill="var(--fb-text-dim)">{n.sub}</text>
                <text x={n.x + 12} y={n.y + 52} fontSize="10" fill={s.stroke} fontWeight="600">{s.tag}</text>
              </g>
            );
          })}

          {/* JobManager */}
          <rect x={40} y={200} width={680} height={24} rx={6}
                fill="var(--fb-surface-2)" stroke="var(--fb-border)" />
          <text x={52} y={216} fontSize="11" fill="var(--fb-text-dim)">
            JobManager / CheckpointCoordinator:
          </text>
          <text x={250} y={216} fontSize="11" fill="var(--fb-blue)" fontFamily="var(--fb-font-mono)">
            {step.jm}
          </text>
        </svg>
      </div>

      <div className="fb-sim__log" style={{maxHeight: 'none'}} role="status" aria-live="polite">
        <div><b>{i + 1}. {step.title}</b></div>
        <div style={{marginTop: '0.4rem', fontFamily: 'var(--ifm-font-family-base)', fontSize: '0.9rem'}}>
          {step.narration}
        </div>
      </div>
    </div>
  );
}
