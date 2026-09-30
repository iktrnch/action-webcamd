#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 2 ]] || {
    echo "usage: $0 <deb|rpm> <package-directory>" >&2
    exit 2
}

package_kind="$1"
package_directory="$(realpath "$2")"

case "$package_kind" in
    deb)
        docker run --rm \
            -v "$package_directory:/packages:ro" \
            ubuntu:24.04 bash -euc '
                export DEBIAN_FRONTEND=noninteractive
                apt-get update --quiet
                apt-get install --no-install-recommends --quiet=2 --yes /packages/*.deb
                test -x /usr/bin/action-webcamd
                test -f /usr/lib/systemd/system/action-webcamd.service
                dpkg-query --listfiles action-webcamd |
                    grep -Fx /usr/share/doc/action-webcamd/README.md
                test -f /usr/share/licenses/action-webcamd/LICENSE
                command -v ffmpeg >/dev/null
                ! ldd /usr/bin/action-webcamd | grep -q "not found"
                systemd-analyze verify /usr/lib/systemd/system/action-webcamd.service
                test ! -e /etc/systemd/system/multi-user.target.wants/action-webcamd.service
            '
        ;;
    rpm)
        docker run --rm \
            -v "$package_directory:/packages:ro" \
            fedora:latest bash -euc '
                dnf install --assumeyes --quiet /packages/*.rpm
                rpm --query action-webcamd
                test -x /usr/bin/action-webcamd
                test -f /usr/lib/systemd/system/action-webcamd.service
                rpm --query --list action-webcamd |
                    grep -Fx /usr/share/doc/action-webcamd/README.md
                test -f /usr/share/licenses/action-webcamd/LICENSE
                command -v ffmpeg >/dev/null
                ! ldd /usr/bin/action-webcamd | grep -q "not found"
                systemd-analyze verify /usr/lib/systemd/system/action-webcamd.service
                test ! -e /etc/systemd/system/multi-user.target.wants/action-webcamd.service
            '
        ;;
    *)
        echo "unsupported Linux package kind: $package_kind" >&2
        exit 2
        ;;
esac
