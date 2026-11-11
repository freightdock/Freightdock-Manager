#!/bin/sh
# Builds static (musl) binaries of the manager and the worker in a throwaway rust:alpine container, so they run on any
# x86_64 Linux whatever its glibc. Needs Docker, and Contracts, Manager and Worker checked out side by side.
#
#   Freightdock-Manager/deploy/build-static.sh        ->  dist/freightdock-manager, dist/freightdock-worker (beside the checkouts)
set -eu
here=$(cd "$(dirname "$0")/../.." && pwd)
for d in Freightdock-Contracts Freightdock-Manager Freightdock-Worker; do
  [ -d "$here/$d" ] || { echo "missing $here/$d: check out Freightdock-Contracts, -Manager and -Worker side by side" >&2; exit 1; }
done
mkdir -p "$here/dist"
docker volume create freightdock-cargo-cache >/dev/null
docker run --rm \
  -v "$here:/src" -v freightdock-cargo-cache:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/src/dist/.target -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
  rust:1-alpine sh -euc '
    apk add --no-cache musl-dev >/dev/null
    cd /src/Freightdock-Manager && cargo build --release --locked
    cd /src/Freightdock-Worker && cargo build --release --locked
    cp /src/dist/.target/release/freightdock-manager /src/dist/.target/release/freightdock-worker /src/dist/
    chown -R "$HOST_UID:$HOST_GID" /src/dist
  '
ls -l "$here/dist/freightdock-manager" "$here/dist/freightdock-worker"
