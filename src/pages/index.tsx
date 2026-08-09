import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

/**
 * Landing page. Deliberately short: the job of this page is to get someone
 * into the guide at the right entry point, not to explain Flink.
 */
export default function Home(): React.JSX.Element {
  return (
    <Layout
      title="Flink for Dummies"
      description="Apache Flink explained simply enough for a beginner, deeply enough for an expert. From 'what is an event?' to checkpoint barrier alignment.">
      <main className="fb-home">
        <section className="fb-hero">
          <h1 className="fb-hero__title">Flink for Dummies</h1>
          <p className="fb-hero__tagline">
            Apache Flink explained simply enough for a beginner,
            <br />
            deeply enough for an expert.
          </p>
          <p className="fb-hero__sub">
            Not an API reference — a textbook. You start at <em>“what is an
            event?”</em> and finish able to design, tune, debug, recover and
            rescale a stateful streaming system that handles real money and real
            traffic.
          </p>
          <div className="fb-hero__cta">
            <Link className="fb-btn fb-btn--primary" to="/docs/flink/quickstart">
              ⚡ Run Flink in 5 minutes →
            </Link>
            <Link className="fb-btn" to="/docs/flink/">
              Read the guide
            </Link>
            <Link className="fb-btn" to="/docs/flink/projects">
              Five projects
            </Link>
          </div>
          <p className="fb-hero__meta">
            61 pages · 4 interactive labs · 5 runnable projects · Apache Flink 2.3
          </p>
        </section>

        <section className="fb-home__section">
          <h2>What makes it different</h2>
          <div className="fb-grid">
            <div className="fb-card fb-card--static">
              <div className="fb-card__title">Mental models first</div>
              <div className="fb-card__desc">
                Every concept gets a plain-language explanation, an analogy, a
                diagram and a tiny example <em>before</em> any code. Then the
                internals, the failure modes, and what changes at scale.
              </div>
            </div>
            <div className="fb-card fb-card--static">
              <div className="fb-card__title">Interactive, not decorative</div>
              <div className="fb-card__desc">
                Four labs run real Flink logic in the browser — including a
                faithful port of <code>MathUtils.murmurHash</code>, so the
                subtask assignments shown are the ones your job would produce.
              </div>
            </div>
            <div className="fb-card fb-card--static">
              <div className="fb-card__title">Written for production</div>
              <div className="fb-card__desc">
                A runbook organised by symptom, six reference architectures, and
                five runnable projects that each end with{' '}
                <em>break it on purpose</em>.
              </div>
            </div>
          </div>
        </section>

        <section className="fb-home__section">
          <h2>Where to start</h2>
          <div className="fb-grid">
            <Link className="fb-card" to="/docs/flink/quickstart">
              <div className="fb-card__level">Want to run it first</div>
              <div className="fb-card__title">⚡ Quickstart</div>
              <div className="fb-card__desc">
                Docker up, a real job running, output on your screen — in five
                minutes, before any theory.
              </div>
            </Link>
            <Link className="fb-card" to="/docs/flink/foundations/what-is-an-event">
              <div className="fb-card__level">Never done streaming</div>
              <div className="fb-card__title">What is an event?</div>
              <div className="fb-card__desc">
                Begin at the beginning. No distributed systems knowledge assumed.
              </div>
            </Link>
            <Link className="fb-card" to="/docs/flink/watermarks/what-is-a-watermark">
              <div className="fb-card__level">Already write Flink jobs</div>
              <div className="fb-card__title">Watermarks</div>
              <div className="fb-card__desc">
                The idea most people use without understanding. Includes the
                interactive lab.
              </div>
            </Link>
            <Link className="fb-card" to="/docs/flink/production/runbook">
              <div className="fb-card__level">Something is broken now</div>
              <div className="fb-card__title">Production runbook</div>
              <div className="fb-card__desc">
                Seven real failures, organised by symptom rather than by concept.
              </div>
            </Link>
            <Link className="fb-card" to="/docs/flink/projects">
              <div className="fb-card__level">Learn by building</div>
              <div className="fb-card__title">Five projects</div>
              <div className="fb-card__desc">
                Runnable against a local Docker stack, each ending in a
                deliberate failure.
              </div>
            </Link>
          </div>
        </section>
      </main>
    </Layout>
  );
}
