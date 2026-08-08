#!/usr/bin/env bash
#
# Pull the Flink for Dummies content into the StreamForge website build.
#
# Run from the `website/` directory. Wired as a `prebuild`/`prestart` npm
# script, so `npm run build` and `npm start` both pick it up automatically.
#
# The synced directory is gitignored: this repo does NOT vendor the tutorial,
# it pins a released tag of it.
set -euo pipefail

REPO="https://github.com/chanukyagattu/flink4dummies.git"
VERSION="${FLINK4DUMMIES_VERSION:-v1.0.0}"   # override in CI to test main
TARGET="external/flink4dummies"

echo "==> Syncing Flink for Dummies @ ${VERSION}"

rm -rf "${TARGET}"
mkdir -p "$(dirname "${TARGET}")"

git clone --quiet --depth 1 --branch "${VERSION}" "${REPO}" "${TARGET}"

# Keep only what the website needs; drop the standalone site's own scaffolding
# so it can never be mistaken for part of this build.
rm -rf "${TARGET}/.git" \
       "${TARGET}/node_modules" \
       "${TARGET}/build" \
       "${TARGET}/docusaurus.config.ts" \
       "${TARGET}/package.json" \
       "${TARGET}/src/pages" \
       "${TARGET}/.github"

echo "==> Synced $(find "${TARGET}/docs" -name '*.md' | wc -l | tr -d ' ') pages"
