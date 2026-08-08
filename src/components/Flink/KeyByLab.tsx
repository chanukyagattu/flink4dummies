import React, {useMemo, useState} from 'react';

/* =========================================================================
   KeyByLab — key → key group → subtask, and what rescaling actually moves.

   The hash functions below are faithful ports of what Flink runs:
     · Java String.hashCode()
     · org.apache.flink.util.MathUtils#murmurHash
     · KeyGroupRangeAssignment#assignToKeyGroup
     · KeyGroupRangeAssignment#computeOperatorIndexForKeyGroup
   So the subtask numbers this widget shows are the subtask numbers your job
   would actually use for those key strings.
   ========================================================================= */

const DEFAULT_KEYS = [
  'user-1', 'user-2', 'user-3', 'user-4', 'user-5',
  'user-6', 'user-7', 'user-8', 'user-9', 'user-10',
];

/** Java's String.hashCode(), 32-bit signed. */
function javaHashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

/** Flink's MathUtils.murmurHash(int) — returns a non-negative int. */
function murmurHash(code: number): number {
  let c = code | 0;
  c = Math.imul(c, 0xcc9e2d51);
  c = (c << 15) | (c >>> 17);
  c = Math.imul(c, 0x1b873593);
  c = (c << 13) | (c >>> 19);
  c = (Math.imul(c, 5) + 0xe6546b64) | 0;
  c ^= c >>> 16;
  c = Math.imul(c, 0x85ebca6b);
  c ^= c >>> 13;
  c = Math.imul(c, 0xc2b2ae35);
  c ^= c >>> 16;
  return c & 0x7fffffff;
}

const keyGroupOf = (key: string, maxP: number) => murmurHash(javaHashCode(key)) % maxP;
const subtaskOf = (kg: number, maxP: number, p: number) => Math.floor((kg * p) / maxP);

export default function KeyByLab() {
  const [maxP, setMaxP] = useState(128);
  const [p, setP] = useState(3);
  const [pAfter, setPAfter] = useState(5);
  const [keysText, setKeysText] = useState(DEFAULT_KEYS.join(', '));

  const keys = useMemo(
    () => keysText.split(',').map((k) => k.trim()).filter(Boolean).slice(0, 24),
    [keysText],
  );

  const rows = useMemo(
    () => keys.map((key) => {
      const kg = keyGroupOf(key, maxP);
      return {
        key,
        kg,
        before: subtaskOf(kg, maxP, p),
        after: subtaskOf(kg, maxP, pAfter),
      };
    }),
    [keys, maxP, p, pAfter],
  );

  const moved = rows.filter((r) => r.before !== r.after).length;
  const buckets = (n: number, pick: (r: typeof rows[0]) => number) =>
    Array.from({length: n}, (_, i) => rows.filter((r) => pick(r) === i));

  const before = buckets(p, (r) => r.before);
  const after = buckets(pAfter, (r) => r.after);
  const skew = before.length ? Math.max(...before.map((b) => b.length)) : 0;
  const even = rows.length / Math.max(p, 1);

  return (
    <div className="fb-sim">
      <div className="fb-sim__head">
        <span className="fb-sim__title">🧪 KeyBy & Rescaling Lab</span>
        <span className="fb-sim__hint">
          Real Flink hashing. Change parallelism and see exactly which keys — and therefore which state — move.
        </span>
      </div>

      <div className="fb-sim__controls">
        <div className="fb-sim__control">
          <label htmlFor="kb-max">Max parallelism (key groups) <output>{maxP}</output></label>
          <select id="kb-max" value={maxP} onChange={(e) => setMaxP(+e.target.value)}>
            {[4, 8, 16, 32, 64, 128, 256, 512].map((v) => <option key={v} value={v}>{v}</option>)}
          </select>
        </div>
        <div className="fb-sim__control">
          <label htmlFor="kb-p">Parallelism now <output>{p}</output></label>
          <input id="kb-p" type="range" min={1} max={8} value={p}
                 onChange={(e) => setP(+e.target.value)} />
        </div>
        <div className="fb-sim__control">
          <label htmlFor="kb-pa">Parallelism after rescale <output>{pAfter}</output></label>
          <input id="kb-pa" type="range" min={1} max={8} value={pAfter}
                 onChange={(e) => setPAfter(+e.target.value)} />
        </div>
        <div className="fb-sim__control">
          <label htmlFor="kb-keys">Keys (comma separated)</label>
          <input id="kb-keys" type="text" value={keysText}
                 onChange={(e) => setKeysText(e.target.value)} />
        </div>
      </div>

      <div className="fb-sim__stage">
        <svg viewBox="0 0 900 300" role="img"
             aria-label={`Key distribution across ${p} subtasks before rescale and ${pAfter} subtasks after. ${moved} of ${rows.length} keys change subtask.`}>
          <text x={20} y={22} fontSize="12.5" fill="var(--fb-text-dim)" fontWeight="600">
            BEFORE — parallelism {p}
          </text>
          {before.map((bucket, i) => (
            <Slot key={'b' + i} x={20 + i * (860 / p)} y={34} w={860 / p - 12}
                  label={`subtask ${i}`} keys={bucket.map((r) => r.key)}
                  tone={bucket.length > even * 1.6 ? 'hot' : 'normal'} />
          ))}

          <text x={20} y={172} fontSize="12.5" fill="var(--fb-text-dim)" fontWeight="600">
            AFTER — parallelism {pAfter} (restored from a savepoint)
          </text>
          {after.map((bucket, i) => (
            <Slot key={'a' + i} x={20 + i * (860 / pAfter)} y={184} w={860 / pAfter - 12}
                  label={`subtask ${i}`} keys={bucket.map((r) => r.key)} tone="normal" />
          ))}
        </svg>
      </div>

      <div className="fb-legend">
        <span><b style={{color: 'var(--fb-blue)'}}>{moved}</b>&nbsp;of&nbsp;{rows.length} keys move to a different subtask — that is exactly the state Flink must reshuffle.</span>
        {skew > even * 1.6 && (
          <span style={{color: 'var(--fb-amber)'}}>⚠ one subtask holds {skew} keys vs an even share of {even.toFixed(1)} — that is key skew.</span>
        )}
      </div>

      <details className="fb-expert" style={{marginTop: '1rem'}}>
        <summary>Show the arithmetic for every key</summary>
        <table style={{fontSize: '0.84rem', width: '100%'}}>
          <thead>
            <tr>
              <th>key</th><th>hashCode</th><th>murmurHash</th>
              <th>key group<br /><small>mod {maxP}</small></th>
              <th>subtask @{p}</th><th>subtask @{pAfter}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key} style={r.before !== r.after ? {background: 'var(--fb-amber-soft)'} : undefined}>
                <td><code>{r.key}</code></td>
                <td><code>{javaHashCode(r.key)}</code></td>
                <td><code>{murmurHash(javaHashCode(r.key))}</code></td>
                <td><code>{r.kg}</code></td>
                <td><code>{r.before}</code></td>
                <td><code>{r.after}</code>{r.before !== r.after ? ' ←moved' : ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </div>
  );
}

function Slot({
  x, y, w, label, keys, tone,
}: {
  x: number; y: number; w: number; label: string; keys: string[]; tone: 'normal' | 'hot';
}) {
  return (
    <g>
      <rect x={x} y={y} width={Math.max(w, 40)} height={106} rx={8}
            fill={tone === 'hot' ? 'var(--fb-amber-soft)' : 'var(--fb-surface-2)'}
            stroke={tone === 'hot' ? 'var(--fb-amber)' : 'var(--fb-border)'} />
      <text x={x + 8} y={y + 18} fontSize="11.5" fill="var(--fb-text-dim)" fontWeight="600">{label}</text>
      <text x={x + Math.max(w, 40) - 8} y={y + 18} fontSize="11.5" fill="var(--fb-blue)"
            textAnchor="end" fontWeight="700">{keys.length}</text>
      {keys.slice(0, 6).map((k, i) => (
        <text key={k} x={x + 8} y={y + 36 + i * 12.5} fontSize="10.5" fill="var(--fb-text)">
          {k.length > 14 ? k.slice(0, 13) + '…' : k}
        </text>
      ))}
      {keys.length > 6 && (
        <text x={x + 8} y={y + 36 + 6 * 12.5} fontSize="10.5" fill="var(--fb-text-dim)">
          +{keys.length - 6} more
        </text>
      )}
    </g>
  );
}
