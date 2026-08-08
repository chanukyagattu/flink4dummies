# Embedding this guide in the StreamForge website

This repo is the **canonical home** of the content. StreamForge embeds it as a
second Docusaurus docs instance, pinned to a released tag. Nothing here ever
enters the `stream-forge` Java build or the Flink runtime image — it lives in a
different repository entirely, which is the point.

---

## The key idea

Both sites mount **the same `docs/` directory** with **the same
`routeBasePath: 'docs/flink'`**.

```
this repo                       standalone site            stream-forge
docs/watermarks/x.md      →     /docs/flink/watermarks/x   /docs/flink/watermarks/x
doc id                          watermarks/x               watermarks/x
```

Because ids and routes are identical in both places:

- the absolute `/docs/flink/...` links in the Markdown resolve correctly in both
- `sidebars.ts` is shared verbatim
- there is exactly one copy of the content, and no rewriting step

Change `routeBasePath` in one place and you must change it in the other.

---

## Steps in `stream-forge`

### 1. Drop in the sync script

```bash
mkdir -p website/scripts
cp integration/scripts/sync-flink4dummies.sh website/scripts/
chmod +x website/scripts/sync-flink4dummies.sh
```

### 2. Wire it into the npm lifecycle

`website/package.json`:

```json
{
  "scripts": {
    "prestart": "./scripts/sync-flink4dummies.sh",
    "prebuild": "./scripts/sync-flink4dummies.sh",
    "start": "docusaurus start",
    "build": "docusaurus build"
  }
}
```

### 3. Ignore the synced copy

`website/.gitignore`:

```
external/
```

The tutorial is a pinned dependency, not vendored source. Bump the version by
editing `VERSION` in the sync script (or setting `FLINK4DUMMIES_VERSION` in CI).

### 4. Register the docs instance

`website/docusaurus.config.ts` — add to `plugins`:

```ts
plugins: [
  [
    '@docusaurus/plugin-content-docs',
    {
      id: 'flink',
      path: 'external/flink4dummies/docs',
      routeBasePath: 'docs/flink',
      sidebarPath: './external/flink4dummies/sidebars.ts',
      editUrl: 'https://github.com/chanukyagattu/flink4dummies/tree/main/',
    },
  ],
],
```

### 5. Add the navbar item

In `themeConfig.navbar.items`, **after** `Examples` and **before** `Reference`:

```ts
{
  type: 'docSidebar',
  docsPluginId: 'flink',
  sidebarId: 'flinkSidebar',
  position: 'left',
  label: 'Flink',
},
```

Resulting navbar:

```
StreamForge  Platform  Guides  Use Cases  Examples  Flink  Reference     GitHub
```

### 6. Make the components resolvable

The four labs live in this repo. Point the website's `MDXComponents` at the
synced copy — add an alias in `website/docusaurus.config.ts`:

```ts
plugins: [
  // ...the docs instance above...
  function flinkAliasPlugin() {
    return {
      name: 'flink4dummies-alias',
      configureWebpack: () => ({
        resolve: {
          alias: {
            '@flink4dummies': path.resolve(__dirname, 'external/flink4dummies/src'),
          },
        },
      }),
    };
  },
],
```

Then in `website/src/theme/MDXComponents.tsx` (merge with yours if it exists):

```tsx
import MDXComponents from '@theme-original/MDXComponents';
import {
  PageMeta, Objectives, Callout, Expert,
  Compare, CompareCard, CardGrid, Card,
} from '@flink4dummies/components/Flink/Primitives';
import WatermarkLab from '@flink4dummies/components/Flink/WatermarkLab';
import KeyByLab from '@flink4dummies/components/Flink/KeyByLab';
import CheckpointLab from '@flink4dummies/components/Flink/CheckpointLab';
import BackpressureLab from '@flink4dummies/components/Flink/BackpressureLab';

export default {
  ...MDXComponents,
  PageMeta, Objectives, Callout, Expert,
  Compare, CompareCard, CardGrid, Card,
  WatermarkLab, KeyByLab, CheckpointLab, BackpressureLab,
};
```

### 7. Import the theme

`website/src/css/custom.css`, as the **last** line:

```css
@import '../../external/flink4dummies/src/css/flink-theme.css';
```

### 8. Enable Mermaid, if it is not already

```bash
cd website && npm i @docusaurus/theme-mermaid
```

```ts
markdown: { mermaid: true },
themes: ['@docusaurus/theme-mermaid'],
```

### 9. Build

```bash
cd website && npm run build
```

---

## CI notes

**Fetch depth.** The docs use `showLastUpdateTime`, which shells out to `git log`.
In GitHub Actions that needs full history:

```yaml
- uses: actions/checkout@v4
  with: {fetch-depth: 0}
```

**Path filters.** A docs change should never redeploy Flink pipelines. In the
`stream-forge` pipeline workflow:

```yaml
on:
  push:
    paths-ignore:
      - 'website/**'
      - '**/*.md'
```

**Docker context.** Even though the tutorial is a separate repo now, the synced
`external/` directory exists at build time. Keep it out of the image:

```
# .dockerignore
website/
external/
node_modules/
*.md
```

**Verify the JAR is clean:**

```bash
mvn clean package
jar tf target/stream-forge-*.jar | grep -iE '\.(md|css|tsx)$|flink4dummies'
# expect: no output
```

---

## Canonical URL

This repo's site is canonical; StreamForge is a syndicated copy. To avoid
splitting search rankings, set a canonical override on the embedded instance —
add to the front matter of embedded pages via a `beforeDefaultRemarkPlugins`
hook, or simply accept the duplication if the StreamForge copy is meant to be
the primary surface. If you prefer StreamForge as canonical, flip `url`/`baseUrl`
here and point this repo's README at StreamForge instead.

---

## Route collision check

The default docs instance uses `routeBasePath: 'docs'`; this one uses
`docs/flink`. Nested route base paths across instances are supported, and the
default instance has no `docs/flink` content, so there is no collision. The
`npm run build` in step 9 is the proof — Docusaurus fails loudly on duplicate
routes.
