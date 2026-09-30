#!/usr/bin/env bash
# Generate a source AUR PKGBUILD and .SRCINFO from Cargo metadata.
set -euo pipefail

[[ $# -eq 3 ]] || {
    echo "usage: $0 <output-directory> <source-url> <sha256>" >&2
    exit 2
}

output_directory="$1"
source_url="$2"
source_sha="$3"
version="$(scripts/ci/release-metadata.sh)"
template=packaging/aur/PKGBUILD.source.in

[[ "$source_sha" =~ ^[0-9a-f]{64}$ ]] || {
    echo "invalid SHA-256 digest: $source_sha" >&2
    exit 2
}

mkdir -p "$output_directory"
sed \
    -e "s|@VERSION@|$version|g" \
    -e "s|@URL@|$source_url|g" \
    -e "s|@SHA256@|$source_sha|g" \
    "$template" > "$output_directory/PKGBUILD"

if grep -q '@[A-Z][A-Z_]*@' "$output_directory/PKGBUILD"; then
    echo "unresolved placeholder in rendered PKGBUILD" >&2
    exit 1
fi

(cd "$output_directory" && makepkg --printsrcinfo > .SRCINFO)
