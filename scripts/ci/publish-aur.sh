#!/usr/bin/env bash
set -euo pipefail

runner_temp="${RUNNER_TEMP:?RUNNER_TEMP is required}"
version="$(scripts/ci/release-metadata.sh)"
source_url="https://github.com/iktrnch/action-webcamd/archive/refs/tags/v$version.tar.gz"
source_sha="$(curl -fsSL "$source_url" | sha256sum | cut -d' ' -f1)"
repository_directory=action-webcamd-aur
stage="$(mktemp -d "$runner_temp/action-webcamd-aur.XXXXXX")"
trap 'rm -rf "$stage"' EXIT

git clone ssh://aur@aur.archlinux.org/action-webcamd.git "$repository_directory"
chmod 0777 "$stage"

docker run --rm \
    -v "$PWD:/work:ro" \
    -v "$stage:/output" \
    -w /work \
    archlinux:base-devel bash -euc "
        pacman -Syu --noconfirm --needed cargo python
        useradd --create-home builder
        su builder -c 'cd /work && scripts/ci/generate-aur.sh /output $source_url $source_sha'
    "

install -m 0644 "$stage/PKGBUILD" "$repository_directory/PKGBUILD"
install -m 0644 "$stage/.SRCINFO" "$repository_directory/.SRCINFO"
rm -rf "$stage"
trap - EXIT

git -C "$repository_directory" add PKGBUILD .SRCINFO
if git -C "$repository_directory" diff --cached --quiet; then
    echo "AUR package already describes action-webcamd $version"
    exit 0
fi

git -C "$repository_directory" \
    -c user.name='action-webcamd release bot' \
    -c user.email='release@users.noreply.github.com' \
    commit -m "update to $version"
git -C "$repository_directory" push
