import React, {useMemo, useState} from 'react';

/* =========================================================================
   WatermarkLab
   Feed events (in arrival order) into a watermark generator and watch:
     · the watermark advance behind the max observed timestamp
     · tumbling windows fire when the watermark passes their end
     · events become LATE the moment they land behind the watermark
   Every number on screen is computed with the same rule Flink uses for
   BoundedOutOfOrdernessWatermarks: W = maxObservedTimestamp - bound - 1ms.
   ========================================================================= */

type Ev = {id: number; t: number}; // t = event time, in seconds

// Deliberately out of order: this is what a real Kafka partition looks like.
const DEFAULT_EVENTS: Ev[] = [
  {id: 1, t: 2},  {id: 2, t: 5},  {id: 3, t: 3},  {id: 4, t: 9},
  {id: 5, t: 7},  {id: 6, t: 12}, {id: 7, t: 6},  {id: 8, t: 15},
  {id: 9, t: 14}, {id: 10, t: 19},{id: 11, t: 11},{id: 12, t: 22},
  {id: 13, t: 21},{id: 14, t: 27},{id: 15, t: 24},
];

const T_MAX = 30;
const W = 900;  // svg viewbox width
const H = 230;

export default function WatermarkLab() {
  const [bound, setBound] = useState(3);        // bounded out-of-orderness (s)
  const [winSize, setWinSize] = useState(10);   // tumbling window size (s)
  const [lateness, setLateness] = useState(0);  // allowed lateness (s)
  const [cursor, setCursor] = useState(0);      // how many events have arrived
  const [events, setEvents] = useState<Ev[]>(DEFAULT_EVENTS);

  const x = (t: number) => 60 + (t / T_MAX) * (W - 100);

  /* ---- replay the stream up to `cursor` and record what happened ---- */
  const state = useMemo(() => {
    let maxTs = -Infinity;
    let wm = -Infinity;
    const arrived: Array<Ev & {late: boolean; dropped: boolean; wmAt: number}> = [];
    const log: Array<{k: 'ok' | 'warn' | 'bad' | 'fire'; text: string}> = [];
    const fired = new Set<number>();

    for (let i = 0; i < cursor; i++) {
      const e = events[i];

      // 1. The record arrives. Is it behind the watermark we already emitted?
      const late = wm !== -Infinity && e.t < wm;
      // Allowed lateness keeps the window state alive a bit longer.
      const dropped = wm !== -Infinity && e.t < wm - lateness;

      arrived.push({...e, late, dropped, wmAt: wm});
      if (dropped) {
        log.push({k: 'bad', text: `e${e.id} (t=${e.t}s) arrived — DROPPED. Watermark is ${fmt(wm)}s and allowed lateness is ${lateness}s, so its window state is already gone.`});
      } else if (late) {
        log.push({k: 'warn', text: `e${e.id} (t=${e.t}s) arrived LATE — watermark is already ${fmt(wm)}s. Allowed lateness (${lateness}s) still covers it, so the window re-fires with an updated result.`});
      } else {
        log.push({k: 'ok', text: `e${e.id} (t=${e.t}s) arrived on time.`});
      }

      // 2. Timestamp assigner observes it; watermark generator advances maxTs.
      if (e.t > maxTs) maxTs = e.t;
      const newWm = maxTs - bound;
      if (newWm > wm || wm === -Infinity) {
        const prev = wm;
        wm = newWm;
        if (prev !== wm) {
          log.push({k: 'ok', text: `  → watermark advances to ${fmt(wm)}s  (max seen ${maxTs}s − bound ${bound}s)`});
        }
      }

      // 3. Any window whose end is now <= watermark is complete: it fires.
      for (let s = 0; s < T_MAX; s += winSize) {
        const end = s + winSize;
        if (!fired.has(s) && wm >= end) {
          fired.add(s);
          const members = arrived.filter((a) => !a.dropped && a.t >= s && a.t < end);
          log.push({
            k: 'fire',
            text: `  ⚡ window [${s}s, ${end}s) FIRES — count=${members.length} {${members.map((m) => 'e' + m.id).join(', ')}}`,
          });
        }
      }
    }
    return {maxTs, wm, arrived, log, fired};
  }, [cursor, bound, winSize, lateness, events]);

  const done = cursor >= events.length;

  return (
    <div className="fb-sim">
      <div className="fb-sim__head">
        <span className="fb-sim__title">🧪 Watermark Lab</span>
        <span className="fb-sim__hint">
          Step the stream forward one record at a time. Change the bound and watch what breaks.
        </span>
      </div>

      <div className="fb-sim__controls">
        <div className="fb-sim__control">
          <label htmlFor="wl-bound">
            Bounded out-of-orderness <output>{bound}s</output>
          </label>
          <input id="wl-bound" type="range" min={0} max={10} value={bound}
                 onChange={(e) => setBound(+e.target.value)} />
        </div>
        <div className="fb-sim__control">
          <label htmlFor="wl-win">
            Tumbling window size <output>{winSize}s</output>
          </label>
          <input id="wl-win" type="range" min={5} max={15} step={5} value={winSize}
                 onChange={(e) => setWinSize(+e.target.value)} />
        </div>
        <div className="fb-sim__control">
          <label htmlFor="wl-late">
            Allowed lateness <output>{lateness}s</output>
          </label>
          <input id="wl-late" type="range" min={0} max={8} value={lateness}
                 onChange={(e) => setLateness(+e.target.value)} />
        </div>
      </div>

      <div className="fb-sim__stage">
        <svg viewBox={`0 0 ${W} ${H}`} role="img"
             aria-label={`Event time axis. Watermark is at ${fmt(state.wm)} seconds. ${cursor} of ${events.length} events have arrived.`}>
          {/* window bands */}
          {range(0, T_MAX, winSize).map((s) => {
            const isFired = state.fired.has(s);
            return (
              <g key={s}>
                <rect x={x(s)} y={38} width={x(s + winSize) - x(s)} height={92}
                      fill={isFired ? 'var(--fb-sage-soft)' : 'var(--fb-surface-2)'}
                      stroke="var(--fb-border)" strokeDasharray="3 3" />
                <text x={x(s) + 6} y={54} fontSize="11" fill="var(--fb-text-dim)">
                  [{s}, {s + winSize})
                </text>
                {isFired && (
                  <text x={x(s) + 6} y={70} fontSize="11" fill="var(--fb-sage)" fontWeight="600">
                    ⚡ fired
                  </text>
                )}
              </g>
            );
          })}

          {/* event-time axis */}
          <line x1={50} y1={130} x2={W - 30} y2={130} stroke="var(--fb-border)" strokeWidth="1.5" />
          {range(0, T_MAX + 1, 5).map((t) => (
            <g key={t}>
              <line x1={x(t)} y1={130} x2={x(t)} y2={136} stroke="var(--fb-border)" />
              <text x={x(t)} y={150} fontSize="11" fill="var(--fb-text-dim)" textAnchor="middle">{t}s</text>
            </g>
          ))}
          <text x={W - 30} y={168} fontSize="11" fill="var(--fb-text-dim)" textAnchor="end">
            event time →
          </text>

          {/* events that have arrived */}
          {state.arrived.map((e, i) => {
            const color = e.dropped ? 'var(--fb-rose)'
                        : e.late   ? 'var(--fb-amber)'
                        : 'var(--fb-blue)';
            const y = 112 - (i % 3) * 20;
            return (
              <g key={e.id}>
                <circle cx={x(e.t)} cy={y} r={7} fill={color} />
                <text x={x(e.t)} y={y + 3.5} fontSize="8" fill="var(--fb-bg)"
                      textAnchor="middle" fontWeight="700">{e.id}</text>
              </g>
            );
          })}

          {/* not-yet-arrived events, ghosted at the bottom */}
          {events.slice(cursor).map((e) => (
            <circle key={'g' + e.id} cx={x(e.t)} cy={176} r={5}
                    fill="none" stroke="var(--fb-border)" strokeWidth="1.5" />
          ))}
          {cursor < events.length && (
            <text x={50} y={196} fontSize="11" fill="var(--fb-text-dim)">
              ○ still upstream (arrival order: {events.slice(cursor).map((e) => `e${e.id}@${e.t}s`).join(', ')})
            </text>
          )}

          {/* max observed timestamp */}
          {state.maxTs > -Infinity && (
            <g>
              <line x1={x(state.maxTs)} y1={30} x2={x(state.maxTs)} y2={130}
                    stroke="var(--fb-text-dim)" strokeWidth="1" strokeDasharray="2 4" />
              <text x={x(state.maxTs)} y={24} fontSize="10.5" fill="var(--fb-text-dim)" textAnchor="middle">
                max seen {state.maxTs}s
              </text>
            </g>
          )}

          {/* the watermark itself */}
          {state.wm > -Infinity && (
            <g>
              <line x1={x(Math.max(0, state.wm))} y1={30} x2={x(Math.max(0, state.wm))} y2={140}
                    stroke="var(--fb-blue)" strokeWidth="2.5" />
              <rect x={x(Math.max(0, state.wm)) - 42} y={12} width={84} height={17} rx={4}
                    fill="var(--fb-blue)" />
              <text x={x(Math.max(0, state.wm))} y={24.5} fontSize="11" fill="var(--fb-bg)"
                    textAnchor="middle" fontWeight="700">
                W = {fmt(state.wm)}s
              </text>
            </g>
          )}
        </svg>
      </div>

      <div className="fb-legend">
        <span><i className="fb-swatch" style={{background: 'var(--fb-blue)'}} /> on-time event</span>
        <span><i className="fb-swatch" style={{background: 'var(--fb-amber)'}} /> late (within allowed lateness)</span>
        <span><i className="fb-swatch" style={{background: 'var(--fb-rose)'}} /> dropped</span>
        <span><i className="fb-swatch" style={{background: 'var(--fb-sage)'}} /> window fired</span>
      </div>

      <div className="fb-btn-row">
        <button className="fb-btn fb-btn--primary" disabled={done}
                onClick={() => setCursor((c) => Math.min(c + 1, events.length))}>
          ▶ Next record
        </button>
        <button className="fb-btn" disabled={done} onClick={() => setCursor(events.length)}>
          ⏩ Run to end
        </button>
        <button className="fb-btn" onClick={() => setCursor(0)}>↺ Reset</button>
        <button className="fb-btn" onClick={() => {setEvents(shuffle(DEFAULT_EVENTS)); setCursor(0);}}>
          🔀 Reshuffle arrival order
        </button>
      </div>

      <div className="fb-sim__log" role="log" aria-live="polite">
        {state.log.length === 0
          ? <div style={{color: 'var(--fb-text-dim)'}}>Nothing has arrived yet. The watermark is −∞: Flink has no evidence about event time at all, so no window can be considered complete.</div>
          : state.log.map((l, i) => (
              <div key={i} className={l.k === 'fire' ? 'ok' : l.k}>
                {l.k === 'fire' ? <b>{l.text}</b> : l.text}
              </div>
            ))}
      </div>
    </div>
  );
}

function fmt(n: number) {
  return n === -Infinity ? '−∞' : String(n);
}
function range(a: number, b: number, step: number) {
  const out: number[] = [];
  for (let i = a; i < b; i += step) out.push(i);
  return out;
}
function shuffle<T>(arr: T[]): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
