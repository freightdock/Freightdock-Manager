#!/bin/sh
# Installs a Freightdock node on the node itself, as root, from the Manager repository's releases:
#
#   curl -fsSL https://raw.githubusercontent.com/freightdock/Freightdock-Manager/main/install.sh | sudo sh -s -- first
#   curl -fsSL .../install.sh | sudo sh -s -- worker <manager> <join-token>      a worker for that cluster
#   curl -fsSL .../install.sh | sudo sh -s -- join   <manager> <manager-token>   a manager (and worker) joining it
#   curl -fsSL .../install.sh | sudo sh -s -- upgrade                            the newest binaries, settings kept
#
# `first` makes a whole cluster on this node (a manager and a worker) and prints the lines that add the others, with
# their tokens. <manager> is a manager's address (port 7450 unless given). A worker token comes from
# `freightdock-manager join-token`, a manager one from `freightdock-manager manager-token`, each good once.
#
# Options:
#   --ui <address>       with `first`: run the Freightdock UI at https://<address>, a free address of the LAN that
#                        becomes the UI's wherever it runs
#   --ui-image <image>   the UI's image (default ghcr.io/freightdock/freightdock-ui, the release's channel)
#   --labels k=v,...     the worker's labels
#   --advertise <addr>   the address the others reach this node at (default: the one of the default route)
#
# FREIGHTDOCK_VERSION picks a release (a tag such as v1.0.0, or `edge`, the newest build of main); the default is the
# latest release, else edge. FREIGHTDOCK_DOWNLOAD takes the files from a folder of the same names instead (testing).
set -eu

REPO_URL=https://github.com/freightdock/Freightdock-Manager
BIN=/usr/local/bin
UNITS=/etc/systemd/system
CONF=/etc/freightdock
MANAGER_DATA=/var/lib/freightdock-manager

say() { printf '%s\n' "$*" >&2; }
die() { say "freightdock: $*"; exit 1; }

usage() {
  sed -n '2,23p' "$0" 2>/dev/null | sed 's/^# \{0,1\}//' >&2 || true
  die "usage: install.sh first|worker <manager> <join-token>|join <manager> <manager-token>|upgrade [options]"
}

# --- what was asked
role=${1:-}
[ -n "$role" ] || usage
shift
manager='' token=''
case $role in
  first|upgrade) ;;
  worker|join)
    [ $# -ge 2 ] || die "$role needs a manager's address and a token"
    manager=$1 token=$2
    shift 2
    ;;
  -h|--help|help) usage ;;
  *) die "unknown role $role: first, worker, join or upgrade" ;;
esac
ui_address='' ui_image='' labels='' advertise=''
while [ $# -gt 0 ]; do
  case $1 in
    --ui) ui_address=${2:?--ui needs an address}; shift 2 ;;
    --ui-image) ui_image=${2:?--ui-image needs an image}; shift 2 ;;
    --labels) labels=${2:?--labels needs k=v,...}; shift 2 ;;
    --advertise) advertise=${2:?--advertise needs an address}; shift 2 ;;
    *) die "unknown option $1" ;;
  esac
done
[ -z "$ui_address" ] || [ "$role" = first ] || die "--ui is for first; on a running cluster: freightdock-manager ui <image> <address>"
case $manager in
  '') ;;
  \[*\]:*) ;;                                     # [v6]:port
  *:*:*) manager="[$manager]:7450" ;;             # a bare IPv6 address
  *:*) ;;                                         # host:port
  *) manager="$manager:7450" ;;
esac

# --- this node
[ "$(id -u)" = 0 ] || die "run as root (curl ... | sudo sh -s -- $role ...)"
[ -d /run/systemd/system ] || die "this node does not run systemd, which keeps the agents running"
case $(uname -m) in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) die "no build for $(uname -m): amd64 and arm64 only" ;;
esac
if command -v curl >/dev/null; then fetch() { curl -fsSL -o "$2" "$1"; }
elif command -v wget >/dev/null; then fetch() { wget -q -O "$2" "$1"; }
else die "needs curl or wget"; fi
command -v sha256sum >/dev/null || die "needs sha256sum (coreutils)"
if [ "$role" != upgrade ]; then
  command -v docker >/dev/null && docker info >/dev/null 2>&1 \
    || die "Docker is not running here; install it first (for one: curl -fsSL https://get.docker.com | sh)"
  [ ! -e "$CONF/worker.env" ] && [ ! -e "$CONF/manager.env" ] \
    || die "Freightdock is installed here already ($CONF); upgrade replaces its binaries"
  if [ -z "$advertise" ]; then
    advertise=$(ip -4 route get 192.0.2.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -n 1)
    [ -n "$advertise" ] || die "could not tell this node's address: give it with --advertise <address>"
  fi
fi

# --- the release
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
version=${FREIGHTDOCK_VERSION:-}
url_of() {
  if [ -n "${FREIGHTDOCK_DOWNLOAD:-}" ]; then echo "$FREIGHTDOCK_DOWNLOAD/$1"
  elif [ "$version" = latest ]; then echo "$REPO_URL/releases/latest/download/$1"
  else echo "$REPO_URL/releases/download/$version/$1"; fi
}
if [ -z "$version" ]; then
  version=latest
  fetch "$(url_of SHA256SUMS)" "$tmp/SHA256SUMS" 2>/dev/null || { version=edge; say "no release yet: taking edge, the newest build of main"; }
fi
[ -s "$tmp/SHA256SUMS" ] || fetch "$(url_of SHA256SUMS)" "$tmp/SHA256SUMS" || die "no release $version at $REPO_URL"
get() { # <asset> <name to keep it under>
  fetch "$(url_of "$1")" "$tmp/$1" || die "could not download $1 ($version)"
  (cd "$tmp" && grep "  $1\$" SHA256SUMS | sha256sum -c - >/dev/null 2>&1) || die "$1 does not match its checksum"
  [ -z "${2:-}" ] || mv "$tmp/$1" "$tmp/$2"
}
case $version in
  edge) channel=edge ;;
  *) channel=latest ;;
esac
ui_image=${ui_image:-ghcr.io/freightdock/freightdock-ui:$channel}

# Puts a downloaded binary and its unit in place.
place() { # manager|worker
  install -m 755 "$tmp/freightdock-$1" "$BIN/freightdock-$1"
  install -m 644 "$tmp/freightdock-$1.service" "$UNITS/freightdock-$1.service"
}

if [ "$role" = upgrade ]; then
  done_any=''
  for part in manager worker; do
    [ -x "$BIN/freightdock-$part" ] || continue
    get "freightdock-$part-linux-$arch" "freightdock-$part"
    get "freightdock-$part.service"
    place "$part"
    done_any="$done_any $part"
  done
  [ -n "$done_any" ] || die "nothing of Freightdock is installed here"
  systemctl daemon-reload
  for part in $done_any; do systemctl restart "freightdock-$part"; done
  say "upgraded to $version:$done_any"
  exit 0
fi

# The overlay drives wireguard-tools and nftables, a floating address arping, the live view conntrack; without them the
# worker runs off the overlay and says why.
if ! { command -v wg && command -v nft && command -v arping && command -v conntrack; } >/dev/null 2>&1; then
  say "installing wireguard-tools, nftables, arping and conntrack for the overlay"
  if command -v apt-get >/dev/null; then
    apt-get update -qq >/dev/null 2>&1 || true
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq wireguard-tools nftables iputils-arping conntrack >/dev/null 2>&1
  elif command -v dnf >/dev/null; then dnf install -y -q wireguard-tools nftables iputils conntrack-tools >/dev/null
  elif command -v apk >/dev/null; then apk add -q wireguard-tools nftables iputils conntrack-tools
  fi || say "could not install them: the worker runs without the overlay until they are there"
fi

mkdir -p "$CONF"
chmod 700 "$CONF"
if [ "$role" != worker ]; then
  get "freightdock-manager-linux-$arch" freightdock-manager
  get freightdock-manager.service
  place manager
  host=$advertise
  case $host in *:*) host="[$host]" ;; esac
  {
    echo "FREIGHTDOCK_MANAGER_ADVERTISE=$host:7450"
    echo "FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN=$MANAGER_DATA/local-worker.token"
    if [ "$role" = join ]; then
      echo "FREIGHTDOCK_MANAGER_JOIN=$manager"
      echo "FREIGHTDOCK_MANAGER_JOIN_TOKEN=$token"
    fi
  } | (umask 077 && cat >"$CONF/manager.env")
  systemctl daemon-reload
  systemctl enable -q freightdock-manager
  systemctl restart freightdock-manager
  say "manager started; waiting for it"
  for _ in $(seq 60); do [ -s "$MANAGER_DATA/local-worker.token" ] && break; sleep 1; done
  [ -s "$MANAGER_DATA/local-worker.token" ] || die "the manager did not start: journalctl -u freightdock-manager"
fi

get "freightdock-worker-linux-$arch" freightdock-worker
get freightdock-worker.service
place worker
{
  if [ "$role" = worker ]; then
    echo "FREIGHTDOCK_MANAGERS=$manager"
    echo "FREIGHTDOCK_JOIN_TOKEN=$token"
  else
    # The worker beside a manager dials it and takes the token it leaves.
    echo "FREIGHTDOCK_MANAGERS=127.0.0.1:7450"
    echo "FREIGHTDOCK_JOIN_TOKEN_FILE=$MANAGER_DATA/local-worker.token"
  fi
  [ -z "$labels" ] || echo "FREIGHTDOCK_LABELS=$labels"
} | (umask 077 && cat >"$CONF/worker.env")
systemctl daemon-reload
systemctl enable -q freightdock-worker
systemctl restart freightdock-worker
say "worker started"

[ "$role" = first ] || { say "done: this node is in the cluster of $manager"; exit 0; }

fdm() { FREIGHTDOCK_MANAGER_DATA=$MANAGER_DATA "$BIN/freightdock-manager" "$@"; }
if [ -n "$ui_address" ]; then
  # The UI needs a node to run on (the worker heard) and an address of the pool.
  fdm floating add "$ui_address" >/dev/null 2>&1 || true
  ok=''
  for _ in $(seq 30); do
    if fdm ui "$ui_image" "$ui_address" >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
  done
  [ -n "$ok" ] && say "the UI starts at https://$ui_address (its setup token: docker logs freightdock-ui-1)" \
    || say "the UI did not start yet: freightdock-manager ui $ui_image $ui_address"
fi
worker_token=$(fdm join-token 2>/dev/null) || worker_token='<freightdock-manager join-token>'
manager_token=$(fdm manager-token 2>/dev/null) || manager_token='<freightdock-manager manager-token>'
line="curl -fsSL https://raw.githubusercontent.com/freightdock/Freightdock-Manager/main/install.sh | sudo sh -s --"
cat >&2 <<EOF

A Freightdock cluster runs on this node ($advertise). Each token below is good once, for a day;
\`freightdock-manager join-token\` and \`manager-token\` make more.

Add a worker, on that node:
  $line worker $host $worker_token

Add a manager (and worker), on that node; managers go 1, 3, 5:
  $line join $host $manager_token
EOF
[ -n "$ui_address" ] || cat >&2 <<EOF

Run the UI at a free address of the LAN:
  freightdock-manager floating add <address> && freightdock-manager ui $ui_image <address>
EOF
