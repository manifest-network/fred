#!/usr/bin/env bash
# The checked-out release commit must have merged into main. A branch merely
# descended from main is insufficient: it may contain unreviewed commits.
set -euo pipefail
if ! git merge-base --is-ancestor HEAD refs/remotes/origin/main; then
  echo '::error::Release commit is not in origin/main history.' >&2
  exit 1
fi
