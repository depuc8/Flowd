#!/usr/bin/env bash
# Builds the third-party dependencies that are not reliably available from
# system package managers into a local prefix (default: ./.deps).
#
#   * libcurl        - only if no development package is found (AWS SDK needs it)
#   * AWS SDK for C++ - core + dynamodb components only
#
# GoogleTest and nlohmann/json are fetched by CMake (FetchContent) when they
# are not installed, so they are not handled here.
#
# Usage: scripts/bootstrap-deps.sh [prefix]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX="$(realpath -m "${1:-${FLOWD_DEPS_PREFIX:-$ROOT/.deps}}")"
WORK="$PREFIX/src"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"

AWS_SDK_VERSION="${AWS_SDK_VERSION:-1.11.920}"
CURL_VERSION="${CURL_VERSION:-8.23.0}"

mkdir -p "$PREFIX" "$WORK"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"

log() { printf '\033[1;34m[deps]\033[0m %s\n' "$*"; }

build_curl() {
    if pkg-config --exists libcurl; then
        log "libcurl $(pkg-config --modversion libcurl) found, skipping"
        return
    fi
    log "building libcurl $CURL_VERSION into $PREFIX"
    local tag="curl-${CURL_VERSION//./_}"
    local dir="$WORK/curl-$CURL_VERSION"
    if [[ ! -d "$dir" ]]; then
        curl -fsSL "https://github.com/curl/curl/releases/download/$tag/curl-$CURL_VERSION.tar.gz" \
            | tar -xz -C "$WORK"
    fi
    cmake -S "$dir" -B "$dir/build" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$PREFIX" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DBUILD_SHARED_LIBS=ON \
        -DBUILD_CURL_EXE=OFF \
        -DBUILD_TESTING=OFF \
        -DCURL_USE_OPENSSL=ON \
        -DCURL_USE_LIBPSL=OFF \
        -DCURL_USE_LIBSSH2=OFF \
        -DUSE_NGHTTP2=OFF \
        -DUSE_LIBIDN2=OFF \
        -DCURL_BROTLI=OFF \
        -DCURL_ZSTD=OFF
    cmake --build "$dir/build" -j "$JOBS"
    cmake --install "$dir/build"
}

build_aws_sdk() {
    if [[ -f "$PREFIX/lib/cmake/AWSSDK/AWSSDKConfig.cmake" ]]; then
        log "AWS SDK already installed in $PREFIX, skipping"
        return
    fi
    log "building AWS SDK for C++ $AWS_SDK_VERSION (core, dynamodb)"
    local dir="$WORK/aws-sdk-cpp"
    if [[ ! -d "$dir" ]]; then
        git clone --depth 1 --branch "$AWS_SDK_VERSION" --recurse-submodules --shallow-submodules \
            https://github.com/aws/aws-sdk-cpp.git "$dir"
    fi
    cmake -S "$dir" -B "$dir/build" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$PREFIX" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="$PREFIX" \
        -DBUILD_ONLY="dynamodb" \
        -DBUILD_SHARED_LIBS=ON \
        -DENABLE_TESTING=OFF \
        -DAUTORUN_UNIT_TESTS=OFF \
        -DMINIMIZE_SIZE=ON \
        -DCPP_STANDARD=20
    cmake --build "$dir/build" -j "$JOBS"
    cmake --install "$dir/build"
}

build_curl
build_aws_sdk

log "done. Configure Flowd with: cmake --preset release -DCMAKE_PREFIX_PATH=$PREFIX -DFLOWD_WITH_DYNAMODB=ON"
