#!/usr/bin/env bash
set -euo pipefail

cargo metadata --no-deps --format-version 1 | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
package = next(
    package for package in metadata["packages"]
    if package["name"] == "action-webcamd"
)
print(package["version"])
'
