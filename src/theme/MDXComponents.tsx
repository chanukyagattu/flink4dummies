/**
 * Swizzled MDXComponents.
 *
 * Registering the Flink Bible components here makes them available in every
 * .md / .mdx page WITHOUT an import line at the top of each file. That keeps
 * the docs readable as plain Markdown while still allowing rich components.
 *
 * If website/src/theme/MDXComponents.tsx already exists in your repo, merge
 * the `...FlinkComponents` spread into your existing default export instead of
 * replacing the file.
 */
import MDXComponents from '@theme-original/MDXComponents';

import {
  PageMeta,
  Objectives,
  Callout,
  Expert,
  Compare,
  CompareCard,
  CardGrid,
  Card,
} from '@site/src/components/Flink/Primitives';

import WatermarkLab from '@site/src/components/Flink/WatermarkLab';
import KeyByLab from '@site/src/components/Flink/KeyByLab';
import CheckpointLab from '@site/src/components/Flink/CheckpointLab';
import BackpressureLab from '@site/src/components/Flink/BackpressureLab';

export default {
  ...MDXComponents,
  PageMeta,
  Objectives,
  Callout,
  Expert,
  Compare,
  CompareCard,
  CardGrid,
  Card,
  WatermarkLab,
  KeyByLab,
  CheckpointLab,
  BackpressureLab,
};
