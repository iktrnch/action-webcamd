#!/usr/bin/env bash
set -euo pipefail

version="$(scripts/ci/release-metadata.sh)"
temporary_directory="$(mktemp -d)"
fixture_directory="$temporary_directory/fixtures"
trap 'rm -rf "$temporary_directory"' EXIT

mkdir -p "$fixture_directory"
archive="action-webcamd-$version.tar.gz"
git archive \
    --format=tar.gz \
    --prefix="action-webcamd-$version/" \
    --output="$fixture_directory/$archive" \
    HEAD
source_sha="$(sha256sum "$fixture_directory/$archive" | cut -d' ' -f1)"

# The source checkout and fixture are read-only. Keep the package build tree
# inside the container so files created by the builder never change ownership
# of a host path.
docker run --rm \
    -v "$PWD:/work:ro" \
    -v "$fixture_directory:/fixtures:ro" \
    -w /work \
    archlinux:base-devel bash -euc "
        pacman -Syu --noconfirm --needed cargo clang ffmpeg namcap pkgconf python systemd
        useradd --create-home builder
        install -d -m 0755 -o builder -g builder /tmp/action-webcamd-package
        su builder -c 'cd /work && scripts/ci/generate-aur.sh /tmp/action-webcamd-package file:///fixtures/$archive $source_sha'
        su builder -c 'cd /tmp/action-webcamd-package && makepkg --force --noconfirm'
        namcap /tmp/action-webcamd-package/PKGBUILD /tmp/action-webcamd-package/*.pkg.tar.zst
        pacman -U --noconfirm /tmp/action-webcamd-package/*.pkg.tar.zst
        test -x /usr/bin/action-webcamd
        test -f /usr/lib/systemd/system/action-webcamd.service
        test -f /usr/share/licenses/action-webcamd/LICENSE
        command -v ffmpeg >/dev/null
        ! ldd /usr/bin/action-webcamd | grep -q 'not found'
        systemd-analyze verify /usr/lib/systemd/system/action-webcamd.service
        test ! -e /etc/systemd/system/multi-user.target.wants/action-webcamd.service
    "
