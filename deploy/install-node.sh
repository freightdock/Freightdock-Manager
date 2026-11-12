#!/bin/sh
# Installs a Freightdock cluster node over ssh (as root), from the machine that holds the built binaries
# (Freightdock-Manager/deploy/build-static.sh puts them in dist/). The host needs systemd, and Docker to run workloads;
# a worker's overlay needs `wg` and `nft`, installed here with apt-get where there is one.
#
#   install-node.sh first  <host>                  manager + worker on one host: a whole cluster on its own
#   install-node.sh join   <host> <manager-host>   manager + worker, joining the cluster of <manager-host>
#   install-node.sh worker <host> <manager-host>   a worker only, for the cluster of <manager-host>
#
# <host> is the address the others reach it at (it is advertised as <host>:7450). Set LABELS=key=value,... for the
# worker's labels, and UI_IMAGE=<image> with `first` to run the Freightdock UI on the new cluster (at port 7443 of
# every manager; `freightdock-manager ui <image>` does it later, or on another cluster). A cluster starts with `first` on one node; add managers in twos (1, 3, 5): with two, losing either
# stops every change.
set -eu
here=$(cd "$(dirname "$0")/../.." && pwd)
role=${1:?first, join or worker}
host=${2:?the host to install on}
via=${3:-}
labels=${LABELS:-}
ui_image=${UI_IMAGE:-}
ssh_() { ssh -o BatchMode=no "root@$1" "$2"; }
fdm() { ssh_ "$via" "FREIGHTDOCK_MANAGER_DATA=/var/lib/freightdock-manager freightdock-manager $1"; }

case $role in
  first) ;;
  join|worker) [ -n "$via" ] || { echo "$role needs the host of a manager already in the cluster" >&2; exit 1; } ;;
  *) echo "unknown role $role: first, join or worker" >&2; exit 1 ;;
esac

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

# The overlay (G9) drives wireguard-tools and nftables; without them the worker runs, off the overlay, and says why.
ssh_ "$host" "command -v wg >/dev/null && command -v nft >/dev/null \
  || { command -v apt-get >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq wireguard-tools nftables >/dev/null; } \
  || echo 'install wireguard-tools and nftables for the overlay' >&2"
scp -q "$here/dist/freightdock-worker" "$here/Freightdock-Worker/deploy/freightdock-worker.service" "root@$host:"
ssh_ "$host" "install -m 755 freightdock-worker /usr/local/bin/ && mv freightdock-worker.service /etc/systemd/system/ \
  && mkdir -p /etc/freightdock && umask 077 && printf '%s\n' '$worker_env' > /etc/freightdock/worker.env \
  && systemctl daemon-reload && systemctl enable freightdock-worker && systemctl restart freightdock-worker"
echo "worker on $host"

if [ "$role" = first ] && [ -n "$ui_image" ]; then
  # The UI needs a node to run on: wait for the worker to be heard, then ask for it.
  for _ in $(seq 30); do
    ssh_ "$host" "FREIGHTDOCK_MANAGER_DATA=/var/lib/freightdock-manager freightdock-manager ui '$ui_image'" && break
    sleep 2
  done
fi
