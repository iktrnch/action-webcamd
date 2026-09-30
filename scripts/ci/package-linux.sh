#!/usr/bin/env bash
# Package one already-built binary as Debian and RPM packages with nFPM.
set -euo pipefail

[[ $# -eq 3 ]] || {
    echo "usage: $0 <binary> <amd64> <output-directory>" >&2
    exit 2
}

binary="$(realpath "$1")"
arch="$2"
output_directory="$3"

[[ -x "$binary" ]] || {
    echo "release binary is missing or not executable: $binary" >&2
    exit 1
}

case "$arch" in
    amd64)
        rpm_arch=x86_64
        ;;
    *)
        echo "unsupported Linux architecture: $arch" >&2
        exit 2
        ;;
esac

version="$(scripts/ci/release-metadata.sh)"
mkdir -p "$output_directory"
config="$(mktemp)"
trap 'rm -f "$config"' EXIT

sed \
    -e "s|@VERSION@|$version|g" \
    -e "s|@ARCH@|$arch|g" \
    -e "s|@BINARY@|$binary|g" \
    packaging/nfpm.yaml.in > "$config"

if grep -q '@[A-Z][A-Z_]*@' "$config"; then
    echo "unresolved placeholder in rendered nFPM configuration" >&2
    exit 1
fi

nfpm package \
    --config "$config" \
    --packager deb \
    --target "$output_directory/action-webcamd_${version}_${arch}.deb"
nfpm package \
    --config "$config" \
    --packager rpm \
    --target "$output_directory/action-webcamd-${version}-1.${rpm_arch}.rpm"
