import React, {useMemo, useState} from 'react';

/* =========================================================================
   BackpressureLab
   A pipeline runs at the speed of its slowest stage. This widget computes
   busyTimeMsPerSecond / backPressuredTimeMsPerSecond / idleTimeMsPerSecond
   the same way the Flink Web UI reports them, so you can practise reading
   the pattern: the bottleneck is the LAST operator that is busy and NOT
   back-pressured.
   ========================================================================= */

const STAGES = ['Source (Kafka)', 'Map / parse', 'KeyedProcess', 'Sink (JDBC)'];

export default function BackpressureLab() {
  const [rate, setRate] = useState(90_000);          // records/s the source could read
  const [caps, setCaps] = useState([120_000, 150_000, 80_000, 25_000]);

  const model = useMemo(() => {
    const effective = [Math.min(rate, caps[0]), caps[1], caps[2], caps[3]];
    const throughput = Math.min(...effective);
    const bottleneck = effective.lastIndexOf(throughput);
    return effective.map((cap, i) => {
      const busy = Math.min(1, throughput / cap);
      const isBottleneck = i === bottleneck;
      const upstreamOfBottleneck = i < bottleneck;
      return {
        cap,
        busy,
        back: upstreamOfBottleneck ? 1 - busy : 0,
        idle: upstreamOfBottleneck || isBottleneck ? 0 : 1 - busy,
        isBottleneck,
      };
    }).map((s) => ({...s, throughput, bottleneck}));
  }, [rate, caps]);

  const throughput = model[0].throughput;
  const bnIdx = model[0].bottleneck;
  const lag = Math.max(0, rate - throughput);

  return (
    <div className="fb-sim">
      <div className="fb-sim__head">
        <span className="fb-sim__title">🧪 Backpressure Lab</span>
        <span className="fb-sim__hint">
          Read the bars the way you would read the Flink UI, then find the bottleneck.
        </span>
      </div>

      <div className="fb-sim__controls">
        <div className="fb-sim__control">
          <label htmlFor="bp-rate">Records available in Kafka <output>{k(rate)}/s</output></label>
          <input id="bp-rate" type="range" min={10_000} max={200_000} step={5_000} value={rate}
                 onChange={(e) => setRate(+e.target.value)} />
        </div>
        {STAGES.map((s, i) => (
          <div className="fb-sim__control" key={s}>
            <label htmlFor={`bp-${i}`}>{s} capacity <output>{k(caps[i])}/s</output></label>
            <input id={`bp-${i}`} type="range" min={5_000} max={200_000} step={5_000} value={caps[i]}
                   onChange={(e) => {
                     const next = [...caps];
                     next[i] = +e.target.value;
                     setCaps(next);
                   }} />
          </div>
        ))}
      </div>

      <div className="fb-sim__stage">
        <svg viewBox="0 0 820 250" role="img"
             aria-label={`Pipeline throughput ${k(throughput)} records per second, limited by ${STAGES[bnIdx]}.`}>
          {model.map((s, i) => {
            const y = 20 + i * 55;
            const barX = 200, barW = 520;
            return (
              <g key={i}>
                <text x={12} y={y + 20} fontSize="12.5"
                      fill={s.isBottleneck ? 'var(--fb-rose)' : 'var(--fb-text)'}
                      fontWeight={s.isBottleneck ? 700 : 500}>
                  {STAGES[i]}
                </text>
                <text x={12} y={y + 35} fontSize="10.5" fill="var(--fb-text-dim)">
                  capacity {k(s.cap)}/s
                </text>

                <rect x={barX} y={y} width={barW} height={30} rx={5}
                      fill="var(--fb-surface-2)" stroke="var(--fb-border)" />
                <rect x={barX} y={y} width={barW * s.busy} height={30}
                      fill="var(--fb-sage)" rx={5} />
                <rect x={barX + barW * s.busy} y={y} width={barW * s.back} height={30}
                      fill="var(--fb-rose)" />
                <rect x={barX + barW * (s.busy + s.back)} y={y} width={barW * s.idle} height={30}
                      fill="var(--fb-surface-2)" />

                <text x={barX + 8} y={y + 20} fontSize="11" fill="var(--fb-bg)" fontWeight="700">
                  busy {(s.busy * 100).toFixed(0)}%
                </text>
                {s.back > 0.02 && (
                  <text x={barX + barW * s.busy + 8} y={y + 20} fontSize="11"
                        fill="var(--fb-bg)" fontWeight="700">
                    back-pressured {(s.back * 100).toFixed(0)}%
                  </text>
                )}
                {s.idle > 0.02 && (
                  <text x={barX + barW * (s.busy + s.back) + 8} y={y + 20} fontSize="11"
                        fill="var(--fb-text-dim)">
                    idle {(s.idle * 100).toFixed(0)}%
                  </text>
                )}
                {s.isBottleneck && (
                  <text x={barX + barW + 8} y={y + 20} fontSize="15" fill="var(--fb-rose)">◀</text>
                )}
              </g>
            );
          })}

          <text x={12} y={240} fontSize="12" fill="var(--fb-text-dim)">
            Pipeline throughput:
          </text>
          <text x={130} y={240} fontSize="12" fill="var(--fb-blue)" fontWeight="700">
            {k(throughput)}/s
          </text>
          <text x={210} y={240} fontSize="12" fill={lag > 0 ? 'var(--fb-amber)' : 'var(--fb-sage)'}>
            {lag > 0
              ? `consumer lag growing at ${k(lag)}/s — the job will never catch up`
              : 'keeping up with the topic'}
          </text>
        </svg>
      </div>

      <div className="fb-legend">
        <span><i className="fb-swatch" style={{background: 'var(--fb-sage)'}} /> busy — doing useful work</span>
        <span><i className="fb-swatch" style={{background: 'var(--fb-rose)'}} /> back-pressured — blocked waiting for a downstream buffer</span>
        <span><i className="fb-swatch" style={{background: 'var(--fb-surface-2)', border: '1px solid var(--fb-border)'}} /> idle — waiting for input</span>
      </div>

      <div className="fb-sim__log" style={{maxHeight: 'none'}} role="status" aria-live="polite">
        <div>
          <b>Diagnosis:</b> the bottleneck is <b>{STAGES[bnIdx]}</b> — it is the last operator
          that is busy without being back-pressured. Everything upstream of it shows red;
          everything downstream shows idle.
        </div>
        <div style={{marginTop: '0.4rem', color: 'var(--fb-text-dim)'}}>
          Making an upstream operator faster changes nothing. Raising {STAGES[bnIdx]} capacity
          moves the bottleneck somewhere else — try it.
        </div>
      </div>
    </div>
  );
}

const k = (n: number) => (n >= 1000 ? `${(n / 1000).toFixed(0)}k` : String(n));
