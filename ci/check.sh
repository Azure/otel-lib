#! /usr/bin/env bash

# DESCRIPTION: Ensures the following properties for Git-tracked files
# reachable from the current directory:
# - Files (except for SVGs, see below) MUST have ending newlines
# - Files MUST NOT have lines with trailing whitespace
# - Rust sources MUST include the Microsoft copyright header
# - Rust sources MUST be formatted with `rustfmt`
# - Rust crate entry point files MUST set the following inner
#   attributes:
#   - #![deny(rust_2018_idioms)]
#   - #![warn(clippy::all, clippy::pedantic)]
#
# PARAMETERS: NONE

set -euxo pipefail

REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"
CURDIR="$(pwd -P)"

# NOTE: "CI" is used instead of "GITHUB_ACTIONS" because "build-deps.sh"
# does not use CI-specific features.
if [ "${CI:-}" = "true" ]; then
    . "${REPOSITORY_ROOT}/ci/minimal-deps.sh"
fi

# NOTE: This script uses null-terminated strings and whole lines; unset
# IFS.
IFS=

# NOTE: Require ending newlines.
# NOTE: SVGs are excluded from this check since PlantUML produces SVG
# outputs without trailing newlines.
git grep -Ilz '' -- ':!*.svg' \
| while read -r -d '' FILE; do \
    test -z "$(tail -c 1 "${FILE}")"
done

# NOTE: Reject trailing spaces.
git grep -EIn '\s+$' && exit 1

HEADER="$(
cat <<EOF
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
EOF
)"

# NOTE: Require copyright headers.
git ls-files -z -- '*.rs' \
| while read -r -d '' FILE; do \
    test "$(head -n 2 "${FILE}")" = "${HEADER}"
done

MANIFEST="$(
    cargo locate-project \
        ${1:+--manifest-path="${1}"} \
        --message-format=plain
)"

# NOTE: Require standard lints.
cargo metadata --format-version=1 --manifest-path="${MANIFEST}" --no-deps \
| jq -r '.packages[].targets[].src_path | select(startswith($cwd))' \
    --arg cwd "${CURDIR}" \
| while read -r FILE; do
    grep -q '^#!\[deny(rust_2018_idioms)\]$' "${FILE}"
    grep -q '^#!\[warn(clippy::all, clippy::pedantic)\]$' "${FILE}"
done

# NOTE: Require `rustfmt` pass.
cargo fmt --manifest-path="${MANIFEST}" --verbose --all --check
