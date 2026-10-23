#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
policy='.github/scripts/release-policy.sh'
release_workflow='.github/workflows/release.yml'

check_kind() {
  local expected="$1" tag="$2" got
  got="$(bash "$policy" "$tag")"
  if [[ "$got" != "$expected" ]]; then
    echo "FAIL: tag $tag expected $expected, got $got" >&2
    exit 1
  fi
}

check_invalid() {
  local tag="$1"
  if bash "$policy" "$tag" >/dev/null 2>&1; then
    echo "FAIL: unsafe version tag $tag must be rejected" >&2
    exit 1
  fi
}

# The positive controls exercise real Bash policy used by the publishing job.
for tag in v0.0.0 v1.2.3 v24.10.23; do
  check_kind stable "$tag"
done
for tag in v0.0.0-test v1.2.3-rc.1 v24.10.23-test-20261023 v1.2.3-beta.9; do
  check_kind prerelease "$tag"
done

# Negative controls: malformed tags must never publish an allegedly stable image.
for tag in v1.2 v1.2.3rc1 v1.2.3- v1.2.3--test v01.2.3 \
           v1.02.3 v1.2.03 v1.2.3-rc..1 v1.2.3+dev v1.2.3.foo \
           v1.2.3-test_bad v-try v1.2.3/test ''; do
  check_invalid "$tag"
done

# Prevent a detached/unreferenced policy script from giving a false green CI.
grep -Fq 'bash .github/scripts/release-policy.sh "$VERSION"' "$release_workflow"
grep -Fq 'release_kind: ${{ steps.classify.outputs.kind }}' "$release_workflow"
grep -Fq 'RELEASE_KIND: ${{ steps.classify.outputs.kind }}' "$release_workflow"
grep -Fq 'if: ${{ needs.binary.outputs.release_kind == '\''stable'\'' }}' "$release_workflow"
grep -Fq -- '--prerelease --latest=false' "$release_workflow"
grep -Fq 'if [[ "$RELEASE_KIND" == "prerelease" ]]; then' "$release_workflow"

echo 'PASS: stable, prerelease and unsafe tag policy; workflow integration assertions'
