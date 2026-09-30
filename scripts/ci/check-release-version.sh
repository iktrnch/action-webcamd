#!/usr/bin/env bash
set -euo pipefail

tag="${1:?usage: scripts/ci/check-release-version.sh vX.Y.Z}"
version="$(scripts/ci/release-metadata.sh)"
tag_version="${tag#v}"

[[ "$tag" == v* && "$tag_version" == "$version" ]] || {
    echo "Version mismatch:" >&2
    echo "Cargo.toml: $version" >&2
    echo "Tag:        $tag" >&2
    exit 1
}

echo "Release version: $version"
