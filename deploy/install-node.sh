#!/bin/sh
# Installs a Freightdock cluster node over ssh (as root), from the machine that holds the built binaries
# (Freightdock-Manager/deploy/build-static.sh puts them in dist/). The host needs systemd, and Docker to run workloads;
# a worker's overlay needs `wg` and `nft` (and `arping` for floating addresses), installed here with apt-get where there is one.
#
#   install-node.sh first  <host>                  manager + worker on one host: a whole cluster on its own
#   install-node.sh join   <host> <manager-host>   manager + worker, joining the cluster of <manager-host>
#   install-node.sh worker <host> <manager-host>   a worker only, for the cluster of <manager-host>
#
# <host> is the address the others reach it at (it is advertised as <host>:7450). Set LABELS=key=value,... for the
# worker's labels, and UI_IMAGE=<image> UI_ADDRESS=<a free LAN address> with `first` to run the Freightdock UI on the
# new cluster at https://<UI_ADDRESS>, an address of the pool that stays the UI's wherever it runs (nothing is reached
# on a node's or a manager's own address; `freightdock-manager ui <image> [address]` does it later). A cluster starts with `first` on one node; add managers in twos (1, 3, 5): with two, losing either
# stops every change.
set -eu
here=$(cd "$(dirname "$0")/../.." && pwd)
role=${1:?first, join or worker}
host=${2:?the host to install on}
via=${3:-}
labels=${LABELS:-}
ui_image=${UI_IMAGE:-}
ui_address=${UI_ADDRESS:-}
ssh_() { ssh -o BatchMode=no "root@$1" "$2"; }
fdm() { ssh_ "$via" "FREIGHTDOCK_MANAGER_DATA=/var/lib/freightdock-manager freightdock-manager $1"; }

case $role in
  first) ;;
  join|worker) [ -n "$via" ] || { echo "$role needs the host of a manager already in the cluster" >&2; exit 1; } ;;
  *) echo "unknown role $role: first, join or worker" >&2; exit 1 ;;
esac
if [ -n "$ui_image" ] && [ -z "$ui_address" ]; then
  echo "UI_IMAGE needs UI_ADDRESS: the free LAN address the UI is reached at" >&2; exit 1
fi

if [ "$role" != worker ]; then
  join_lines=""
  if [ "$role" = join ]; then
    token=$(fdm manager-token 2>/dev/null)
    join_lines="FREIGHTDOCK_MANAGER_JOIN=$via:7450
FREIGHTDOCK_MANAGER_JOIN_TOKEN=$token"
  fi
  scp -q "$here/dist/freightdock-manager" "$here/Freightdock-Manager/deploy/freightdock-manager.service" "root@$host:"
  ssh_ "$host" "install -m 755 freightdock-manager /usr/local/bin/ && mv freightdock-manager.service /etc/systemd/system/ \
    && mkdir -p /etc/freightdock && umask 077 && printf '%s\n' 'FREIGHTDOCK_MANAGER_ADVERTISE=$host:7450' \
       'FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN=/var/lib/freightdock-manager/local-worker.token' > /etc/freightdock/manager.env \
    && { [ -z '$join_lines' ] || printf '%s\n' '$join_lines' >> /etc/freightdock/manager.env; } \
    && systemctl daemon-reload && systemctl enable freightdock-manager && systemctl restart freightdock-manager"
  echo "manager on $host"
  # The worker beside it dials its own manager and takes the token that manager leaves for it.
  worker_env="FREIGHTDOCK_MANAGERS=127.0.0.1:7450
FREIGHTDOCK_JOIN_TOKEN_FILE=/var/lib/freightdock-manager/local-worker.token"
else
  token=$(fdm join-token 2>/dev/null)
  worker_env="FREIGHTDOCK_MANAGERS=$via:7450
FREIGHTDOCK_JOIN_TOKEN=$token"
fi
[ -z "$labels" ] || worker_env="$worker_env
FREIGHTDOCK_LABELS=$labels"

# The overlay (G9) drives wireguard-tools and nftables, a floating address (G10.3) arping, the live view (G9.8)
# conntrack; without them the worker runs, off the overlay (or holding no floating address, or reporting no
# traffic), and says why.
ssh_ "$host" "command -v wg >/dev/null && command -v nft >/dev/null && command -v arping >/dev/null && command -v conntrack >/dev/null \
  || { command -v apt-get >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq wireguard-tools nftables iputils-arping conntrack >/dev/null; } \
  || echo 'install wireguard-tools, nftables, arping (iputils) and conntrack for the overlay' >&2"
scp -q "$here/dist/freightdock-worker" "$here/Freightdock-Worker/deploy/freightdock-worker.service" "root@$host:"
ssh_ "$host" "install -m 755 freightdock-worker /usr/local/bin/ && mv freightdock-worker.service /etc/systemd/system/ \
  && mkdir -p /etc/freightdock && umask 077 && printf '%s\n' '$worker_env' > /etc/freightdock/worker.env \
  && systemctl daemon-reload && systemctl enable freightdock-worker && systemctl restart freightdock-worker"
echo "worker on $host"

if [ "$role" = first ] && [ -n "$ui_image" ]; then
  # The UI needs a node to run on and an address of the pool: wait for the worker to be heard, then ask for it.
  ssh_ "$host" "FREIGHTDOCK_MANAGER_DATA=/var/lib/freightdock-manager freightdock-manager floating add '$ui_address'" || true
  for _ in $(seq 30); do
    ssh_ "$host" "FREIGHTDOCK_MANAGER_DATA=/var/lib/freightdock-manager freightdock-manager ui '$ui_image' '$ui_address'" && break
    sleep 2
  done
fi
