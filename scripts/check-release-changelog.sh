#!/usr/bin/env bash
# The pushed tag must release the newest dated CHANGELOG section: vX.Y.Z and
# its candidates vX.Y.Z-rc.N all release [X.Y.Z].
set -euo pipefail
tag="${1:?usage: check-release-changelog.sh <tag>}"
version="${tag#v}"
base="${version%%-*}"
section='^## \[([0-9]+\.[0-9]+\.[0-9]+)\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$'
newest=""
while IFS= read -r line; do
  if [[ $line =~ $section ]]; then
    newest="${BASH_REMATCH[1]}"
    break
  fi
done < CHANGELOG.md
if [[ -z "$newest" || "$newest" != "$base" ]]; then
  echo "::error::Tag ${tag} does not release the newest CHANGELOG section [${newest:-none}]." >&2
  exit 1
fi
