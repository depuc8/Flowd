#!/usr/bin/env bash
# Builds a release flowd binary with DynamoDB support and packages it with
# the deployment files into dist/flowd-<version>-<arch>.tar.gz.
#
#   deploy/ec2/build-release.sh
#
# Expects the AWS SDK in .deps (scripts/bootstrap-deps.sh). Build on the same
# OS/architecture as the target instances (or in a matching container).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD="$ROOT/build/ec2-release"
PREFIX="$BUILD/stage"

# $ORIGIN is expanded by the dynamic linker, not the shell.
# shellcheck disable=SC2016
cmake -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH="$ROOT/.deps" -DFLOWD_WITH_DYNAMODB=ON \
    -DFLOWD_BUILD_TESTS=OFF -DFLOWD_BUILD_BENCHMARKS=OFF \
    -DCMAKE_INSTALL_RPATH='$ORIGIN/../lib'
cmake --build "$BUILD" --parallel
cmake --install "$BUILD" --prefix "$PREFIX"

# Bundle the shared AWS SDK libraries next to the binary.
mkdir -p "$PREFIX/lib"
cp -a "$ROOT"/.deps/lib/libaws-*.so* "$ROOT"/.deps/lib/libs2n*.so* "$PREFIX/lib/" 2>/dev/null || true
cp "$ROOT"/deploy/ec2/{flowd-worker.service,worker.env.example,install-worker.sh} "$PREFIX/"

VERSION="$("$PREFIX/bin/flowd" version | awk '{print $2}')"
mkdir -p "$ROOT/dist"
TARBALL="$ROOT/dist/flowd-$VERSION-$(uname -m).tar.gz"
tar -C "$PREFIX" -czf "$TARBALL" .
echo "$TARBALL"
