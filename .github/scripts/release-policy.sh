#!/usr/bin/env bash
# Classification is shared by release.yml and PR CI; invalid version tags fail closed.
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: release-policy.sh <version-tag>" >&2
  exit 2
fi

tag="$1"
# Only canonical vMAJOR.MINOR.PATCH tags may publish a stable GHCR image.
if [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  printf 'stable\n'
elif [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-([0-9A-Za-z]+([.-][0-9A-Za-z]+)*)$ ]]; then
  # Test builds never become the default GitHub release or GHCR :latest.
  printf 'prerelease\n'
else
  echo "refusing release: invalid version tag '$tag'" >&2
  exit 2
fi
