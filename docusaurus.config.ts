import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {themes as prismThemes} from 'prism-react-renderer';

/**
 * Standalone site for Flink for Dummies.
 *
 * IMPORTANT — `routeBasePath: 'docs/flink'`
 *
 * The docs live at `docs/` in this repo but are served under `/docs/flink/...`.
 * That is deliberate: the embedding site (stream-forge) mounts this same
 * directory as a second docs plugin instance with the identical routeBasePath.
 * Because both sites produce the same doc ids AND the same routes, the
 * absolute `/docs/flink/...` links inside the Markdown resolve correctly in
 * both places, and `sidebars.ts` can be shared verbatim.
 *
 * Change this and you must change it in stream-forge too.
 */
const config: Config = {
  title: 'Flink for Dummies',
  tagline:
    'Apache Flink explained simply enough for a beginner, deeply enough for an expert.',
  favicon: 'img/favicon.svg',

  url: 'https://chanukyagattu.github.io',
  baseUrl: '/flink4dummies/',

  organizationName: 'chanukyagattu',
  projectName: 'flink4dummies',
  trailingSlash: false,

  // Fail the build on a broken link rather than shipping one.
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'warn',

  i18n: {defaultLocale: 'en', locales: ['en']},

  markdown: {
    mermaid: true,
    hooks: {onBrokenMarkdownLinks: 'throw'},
  },
  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      {
        docs: {
          path: 'docs',
          routeBasePath: 'docs/flink',
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/chanukyagattu/flink4dummies/tree/main/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {customCss: './src/css/custom.css'},
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/social-card.png',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {hideable: true, autoCollapseCategories: false},
    },
    navbar: {
      title: 'Flink for Dummies',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'flinkSidebar',
          position: 'left',
          label: 'The guide',
        },
        {
          to: '/docs/flink/quickstart',
          label: 'Quickstart',
          position: 'left',
        },
        {
          to: '/docs/flink/projects',
          label: 'Projects',
          position: 'left',
        },
        {
          to: '/docs/flink/reference/interview',
          label: 'Interview prep',
          position: 'left',
        },
        {
          href: 'https://chanukyagattu.github.io/stream-forge/',
          label: 'StreamForge',
          position: 'right',
        },
        {
          href: 'https://github.com/chanukyagattu/flink4dummies',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Start',
          items: [
            {label: 'Quickstart (5 min)', to: '/docs/flink/quickstart'},
            {label: 'Start here', to: '/docs/flink/'},
            {label: 'Learning path', to: '/docs/flink/learning-path'},
            {label: 'Hands-on projects', to: '/docs/flink/projects'},
          ],
        },
        {
          title: 'The hard parts',
          items: [
            {label: 'Watermarks', to: '/docs/flink/watermarks/what-is-a-watermark'},
            {label: 'Checkpoints', to: '/docs/flink/fault-tolerance/checkpoints'},
            {label: 'Exactly-once', to: '/docs/flink/fault-tolerance/exactly-once'},
            {label: 'How Flink really works', to: '/docs/flink/internals/how-flink-really-works'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Cheat sheets', to: '/docs/flink/reference/cheat-sheets'},
            {label: 'Glossary', to: '/docs/flink/reference/glossary'},
            {label: 'Interview questions', to: '/docs/flink/reference/interview'},
            {label: 'Production runbook', to: '/docs/flink/production/runbook'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/chanukyagattu/flink4dummies'},
            {label: 'StreamForge', href: 'https://chanukyagattu.github.io/stream-forge/'},
            {label: 'Apache Flink docs', href: 'https://nightlies.apache.org/flink/flink-docs-stable/'},
          ],
        },
      ],
      copyright:
        'Flink for Dummies — built by Chanukya Gattu. Documentation baseline: Apache Flink 2.3. Apache Flink is a trademark of the Apache Software Foundation.',
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
      additionalLanguages: ['java', 'sql', 'bash', 'yaml', 'json', 'properties'],
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
