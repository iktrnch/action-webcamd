#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 2 ]] || {
    echo "usage: $0 <tag> <asset-directory>" >&2
    exit 2
}

tag="$1"
asset_directory="$(realpath "$2")"
repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

shopt -s nullglob
deb_packages=("$asset_directory"/*.deb)
rpm_packages=("$asset_directory"/*.rpm)
checksum_file="$asset_directory/SHA256SUMS"

[[ ${#deb_packages[@]} -eq 1 ]] || {
    echo "expected exactly one Debian package in $asset_directory" >&2
    exit 1
}
[[ ${#rpm_packages[@]} -eq 1 ]] || {
    echo "expected exactly one RPM package in $asset_directory" >&2
    exit 1
}
[[ -f "$checksum_file" ]] || {
    echo "missing checksum file: $checksum_file" >&2
    exit 1
}

(cd "$asset_directory" && sha256sum --check SHA256SUMS)
gh release create "$tag" \
    --repo "$repository" \
    --verify-tag \
    --generate-notes \
    "${deb_packages[0]}" \
    "${rpm_packages[0]}" \
    "$checksum_file"
