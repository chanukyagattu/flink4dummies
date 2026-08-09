import React from 'react';
import Link from '@docusaurus/Link';

/* =========================================================================
   PageMeta — difficulty, reading time, prerequisites
   Usage (MDX):
     <PageMeta level="intermediate" time="12 min"
               prereq={[['Event time', '/docs/flink/time/three-clocks']]} />
   ========================================================================= */

type Level = 'beginner' | 'intermediate' | 'advanced' | 'expert';

const LEVELS: Record<Level, {dot: string; label: string}> = {
  beginner:     {dot: '🟢', label: 'Beginner'},
  intermediate: {dot: '🟡', label: 'Intermediate'},
  advanced:     {dot: '🔴', label: 'Advanced'},
  expert:       {dot: '⚫', label: 'Expert / Internals'},
};

/**
 * The single place the documentation baseline is declared. Every page renders
 * it, because readers arrive on deep pages from search — not via the index —
 * and API guidance ages fast enough that an unversioned page is a liability.
 */
export const FLINK_VERSION = '2.3';
const FLINK_DOCS = 'https://nightlies.apache.org/flink/flink-docs-stable/';

export function PageMeta({
  level = 'beginner',
  time,
  prereq = [],
  docs,
  version = FLINK_VERSION,
}: {
  level?: Level;
  time?: string;
  prereq?: Array<[string, string]>;
  /** Path under the official Flink docs, or a full URL. */
  docs?: string;
  version?: string;
}) {
  const l = LEVELS[level];
  const docsHref = docs
    ? docs.startsWith('http')
      ? docs
      : FLINK_DOCS + docs.replace(/^\//, '')
    : undefined;

  return (
    <div className="fb-meta">
      <span className={`fb-chip fb-chip--${level}`} aria-label={`Difficulty: ${l.label}`}>
        <span aria-hidden="true">{l.dot}</span> {l.label}
      </span>
      {time && (
        <span className="fb-chip" aria-label={`Estimated reading time ${time}`}>
          ⏱ {time}
        </span>
      )}
      <span
        className="fb-chip fb-chip--version"
        title="Every API and behaviour on this page is written against this Flink version."
        aria-label={`Written against Apache Flink ${version}`}>
        Flink {version}
      </span>
      {docsHref && (
        <a
          className="fb-chip fb-chip--docs"
          href={docsHref}
          target="_blank"
          rel="noopener noreferrer">
          Official docs ↗
        </a>
      )}
      {prereq.length > 0 && (
        <span className="fb-meta__prereq">
          <strong>Before this page:</strong>{' '}
          {prereq.map(([label, href], i) => (
            <React.Fragment key={href}>
              {i > 0 && ' · '}
              <Link to={href}>{label}</Link>
            </React.Fragment>
          ))}
        </span>
      )}
      {prereq.length === 0 && (
        <span className="fb-meta__prereq">
          <strong>Before this page:</strong> nothing. Start here.
        </span>
      )}
    </div>
  );
}

/* =========================================================================
   Objectives — "what you'll be able to do after this page"
   ========================================================================= */
export function Objectives({children}: {children: React.ReactNode}) {
  return (
    <div className="fb-objectives">
      <div className="fb-objectives__title">You will be able to</div>
      {children}
    </div>
  );
}

/* =========================================================================
   Callout — 💡 Mental Model, ⚠️ Common Mistake, 🔍 Under the Hood, …
   Usage: <Callout type="mental">…</Callout>
   ========================================================================= */
type CalloutType =
  | 'mental' | 'mistake' | 'hood' | 'prod' | 'remember' | 'try' | 'version' | 'key';

const CALLOUTS: Record<CalloutType, {icon: string; title: string; cls: string}> = {
  mental:   {icon: '💡', title: 'Mental Model',     cls: 'mental'},
  key:      {icon: '🎯', title: 'Key Idea',         cls: 'mental'},
  mistake:  {icon: '⚠️', title: 'Common Mistake',   cls: 'mistake'},
  hood:     {icon: '🔍', title: 'Under the Hood',   cls: 'hood'},
  prod:     {icon: '🚀', title: 'Production Tip',   cls: 'prod'},
  remember: {icon: '🧠', title: 'Remember',         cls: 'remember'},
  try:      {icon: '🧪', title: 'Try It Yourself',  cls: 'try'},
  version:  {icon: '📌', title: 'Version Note',     cls: 'version'},
};

export function Callout({
  type = 'mental',
  title,
  children,
}: {
  type?: CalloutType;
  title?: string;
  children: React.ReactNode;
}) {
  const c = CALLOUTS[type] ?? CALLOUTS.mental;
  return (
    <div className={`fb-callout fb-callout--${c.cls}`}>
      <div className="fb-callout__title">
        <span className="fb-callout__icon" aria-hidden="true">{c.icon}</span>
        {title ?? c.title}
      </div>
      {children}
    </div>
  );
}

/* =========================================================================
   Expert — collapsible "Expert in 30 seconds"
   ========================================================================= */
export function Expert({
  title = 'Expert in 30 seconds',
  children,
}: {
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <details className="fb-expert">
      <summary>{title}</summary>
      {children}
    </details>
  );
}

/* =========================================================================
   Compare / CompareCard — side-by-side confusion killers
   ========================================================================= */
export function Compare({children}: {children: React.ReactNode}) {
  return <div className="fb-compare">{children}</div>;
}

export function CompareCard({
  title,
  rows,
}: {
  title: string;
  rows: Array<[string, string]>;
}) {
  return (
    <div className="fb-compare__card">
      <h4>{title}</h4>
      <dl>
        {rows.map(([k, v]) => (
          <React.Fragment key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </React.Fragment>
        ))}
      </dl>
    </div>
  );
}

/* =========================================================================
   Cards — landing page / roadmap grid
   ========================================================================= */
export function CardGrid({children}: {children: React.ReactNode}) {
  return <div className="fb-grid">{children}</div>;
}

export function Card({
  to,
  level,
  title,
  children,
}: {
  to: string;
  level?: string;
  title: string;
  children?: React.ReactNode;
}) {
  return (
    <Link className="fb-card" to={to}>
      {level && <div className="fb-card__level">{level}</div>}
      <div className="fb-card__title">{title}</div>
      <div className="fb-card__desc">{children}</div>
    </Link>
  );
}
