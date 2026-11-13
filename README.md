# freightdock-manager

The manager-agent of a Freightdock cluster: the control plane and its database. It owns the cluster's state,
replicated between managers with Raft (openraft), enrols workers and managers with join tokens, keeps a long-lived
mutual-TLS gRPC stream to each worker that connects to it, tracks their health with the other managers, and sends
them commands. One manager is a whole cluster; add managers to survive losing one. The plan, and what has been done, is in
[ROADMAP.md](ROADMAP.md).

```
src/state.rs      the cluster state as a deterministic state machine over log entries
src/store.rs      propose (forwarded to the leader), read, membership; a thin face over Raft
src/raft/         openraft: the log file, the state machine and its snapshot, the network between managers
src/peer.rs       ManagerPeer: Raft, Propose, Join, AddVoter, Heard (manager certificates only, but Join)
src/ca.rs         the cluster CA: manager server and client certificates, worker client certificates
src/tls.rs        the listener (client certificate optional at the handshake), TLS to the other managers
src/grpc.rs       Enrollment and WorkerSession, from freightdock-proto (the Contracts repository)
src/sessions.rs   connected workers and their liveness, merged with what the other managers hear
src/compose.rs    Compose files read into a stack's services (the part a worker can run)
src/stacks.rs     placement of a stack's workloads, and the reconcile of each worker against the state
src/admin.rs      the HTTP/JSON admin API the CLI and the UI use
deploy/           static build, install over ssh, systemd unit
```

The repositories, side by side: **Freightdock-Contracts** (the gRPC contract), **Freightdock-Manager** (this), **Freightdock-Worker** (the
worker-agent), **Freightdock-Network** (G9), **Freightdock-UI** (the UI, an image the managers run).

## Try it on one machine

```sh
# terminal 1: a manager that leaves a token for the worker beside it
FREIGHTDOCK_MANAGER_DATA=./data FREIGHTDOCK_MANAGER_ADVERTISE=127.0.0.1:7450 FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN=./data/local-worker.token cargo run

# terminal 2: the worker (from the Worker checkout)
cd ../Freightdock-Worker && FREIGHTDOCK_MANAGERS=127.0.0.1 FREIGHTDOCK_JOIN_TOKEN_FILE=../Freightdock-Manager/data/local-worker.token FREIGHTDOCK_WORKER_DATA=./data cargo run

# terminal 3: look and command
FREIGHTDOCK_MANAGER_DATA=./data cargo run -q -- status
FREIGHTDOCK_MANAGER_DATA=./data cargo run -q -- command <node> '{"op":"ping"}'
FREIGHTDOCK_MANAGER_DATA=./data cargo run -q -- command <node> '{"op":"run_container","workload_id":"web","image":"nginx:alpine"}'
FREIGHTDOCK_MANAGER_DATA=./data cargo run -q -- command <node> '{"op":"remove_container","workload_id":"web"}'
```

## Install

Build once on a machine with Docker (Contracts, Manager and Worker checked out side by side); the binaries are
static and run on any x86_64 Linux with systemd:

```sh
Freightdock-Manager/deploy/build-static.sh                               # -> dist/freightdock-manager, dist/freightdock-worker
Freightdock-Manager/deploy/install-node.sh first  192.0.2.11              # manager + worker: a whole cluster on one node
Freightdock-Manager/deploy/install-node.sh join   192.0.2.12 192.0.2.11    # another manager + worker, joining it
Freightdock-Manager/deploy/install-node.sh join   192.0.2.13 192.0.2.11    # a third (managers go 1, 3, 5)
Freightdock-Manager/deploy/install-node.sh worker 192.0.2.21 192.0.2.11    # a worker only
```

The script works over `ssh root@<host>`; `LABELS=key=value,...` sets the worker's labels, and `UI_IMAGE=<image>`
with `first` runs the Freightdock UI on the new cluster (below). It writes
`/etc/freightdock/manager.env` and `worker.env`, which are the whole configuration (below). By hand: a worker join
token from `freightdock-manager join-token`, a manager one from `freightdock-manager manager-token`, each good once.

With two managers, losing either stops every change (a majority of two is two), so grow from one manager to three.
Port 7450/tcp must be open between managers and from workers to managers; nothing needs to be open on a worker.

## The UI

`freightdock-manager ui <image> [address]` runs the Freightdock UI (the Freightdock-UI repository's image) as the
managers' own stack, `freightdock`: one workload, placed on a node like any other and moved when that node fails, on a
one-node cluster too once it is back. Like every stack it is reached on an address of the pool, at
`https://<address>` (port 443 there, the container's 7443): the address given, else the one it has, else the first
of the pool free at 443. That address stays the UI's wherever it runs; nothing answers for it on a node's or a
manager's own address, and with no pool it is refused (`floating add` one first). Its certificate names that address.
`ui status` shows where it runs, `ui off` removes it.

It needs no volume and no token copied by hand: its database is kept by the managers, and its API token is derived
from the cluster CA's key, which every manager holds; the state keeps only the token's hash (API token
`freightdock-ui`, admin). The token, the CA and the managers' addresses reach the UI only in the command that starts
it, filled in by the manager that sends it, never written to the cluster's state. A stack of one's own cannot be called
`freightdock`, and its environment is never filled.

## Settings

| Variable | Default | What |
|---|---|---|
| `FREIGHTDOCK_MANAGER_DATA` | `/var/lib/freightdock-manager` | State log, CA, manager id, admin token |
| `FREIGHTDOCK_MANAGER_BIND` | `0.0.0.0:7450` | The worker API (gRPC, mutual TLS) |
| `FREIGHTDOCK_MANAGER_API_BIND` | `127.0.0.1:7451` | The admin HTTP API, or `off` |
| `FREIGHTDOCK_MANAGER_API_TLS_BIND` | `0.0.0.0:7452` | The same API over TLS for the network (API tokens), or `off` |
| `FREIGHTDOCK_MANAGER_ADVERTISE` | `<hostname>:7450` | Where the others reach this manager, comma separated (the first is its Raft address) |
| `FREIGHTDOCK_MANAGER_JOIN` | | First start only: join the cluster of the manager at `host[:port]` |
| `FREIGHTDOCK_MANAGER_JOIN_TOKEN` | | With it: a token from `manager-token` (`FDMT1-...`) |
| `FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN` | | Leave a worker join token in this file for the worker on this node |
| `FREIGHTDOCK_HEARTBEAT_MS` | `2000` | How often workers send a heartbeat |
| `FREIGHTDOCK_SUSPECT_MS` | `5000` | Silent this long: suspect |
| `FREIGHTDOCK_UNREACHABLE_MS` | `12000` | Silent this long: unreachable (which never means stopped) |
| `RUST_LOG` | `freightdock_manager=info,warn` | How much it logs |

## Admin API

All under `http://127.0.0.1:7451`, with `Authorization: Bearer <data>/admin.token`, or over the network at
`https://<manager>:7452` with an API token (`freightdock-manager api-token create <name> [read|admin]`, kept by the
managers only as its hash, good at every manager; a read token only reads). The TLS certificate is the cluster CA's
(`freightdock-manager ca` prints it), for the name `manager.freightdock.internal` whatever address reaches the manager:
a client trusts that CA alone and checks that name. `GET /v1/health` and `GET /v1/ca` need no token.

| | |
|---|---|
| `GET /v1/cluster` | cluster id, manager id, managers, Raft members (voter, leader, online as this manager hears them), `quorate`, applied index, CA fingerprint |
| `GET /v1/nodes`, `GET /v1/nodes/{id}` | nodes with their last report, labels (`labels` set here, `all_labels` merged) and live health (from any manager that hears them); one node also with `containers`, as it last reported them |
| `PUT /v1/nodes/{id}/labels` | `{"labels": {...}}`: the node's labels, all of them |
| `POST /v1/nodes/{id}/authorize` | lets a node that came back fenced run workloads again |
| `DELETE /v1/nodes/{id}` | forget a node: its certificate opens nothing more, its stream is cut |
| `POST /v1/join-tokens` | `{"ttl_s": 86400}` gives `{"token": "FDCT1-..."}`, for a worker |
| `POST /v1/manager-tokens` | `{"ttl_s": 3600}` gives `{"token": "FDMT1-..."}`, for a manager |
| `GET /v1/events?after=&node=` | recent node events |
| `GET /v1/recoveries?stack=` | the recovery history, newest first |
| `GET /v1/api-tokens`, `POST /v1/api-tokens`, `DELETE /v1/api-tokens/{id}` | API tokens: `{"name", "role": "read" or "admin"}` gives the token once |
| `GET /v1/ca` | the cluster CA's certificate (PEM), no token |
| `GET /v1/floating`, `POST /v1/floating`, `DELETE /v1/floating/{address}` | the pool of floating addresses (G10.3), each with its holder and what stacks expose on it; `{"address"}` adds one (400 if not a LAN's unicast IPv4 address outside 10.240.0.0/16, 409 if there) or a range, `192.0.2.10-20` or `192.0.2.8/29` (at most 1024, those already there kept), answering `{"added": n}`; an address is announced on the LAN only once a stack exposes on it; one in use is not removed (409) |
| `PUT /v1/stacks/{name}/exposures` | `{"exposures": [{"service", "container_port", "protocol", "address", "port"}]}`: what the stack exposes on the pool (G10.5), all of it; a port is unique per address and protocol; answers the stack |
| `GET /v1/ui-db?after=` | the UI's database (admin token): its snapshot and changes, or only the changes after `after` |
| `POST /v1/ui-db/changes`, `PUT /v1/ui-db/snapshot` | `{"seq", "data"}`: the next change only (else 409); `{"head", "data"}`: a snapshot at the latest change only |
| `GET /v1/ui`, `PUT /v1/ui`, `DELETE /v1/ui` | the UI: on or off, its image and its stack; `{"image"}` runs it or moves it to that image; off |
| `POST /v1/nodes/{id}/commands` | `{"op": "ping"}` and the other ops; answers the command (202) |
| `GET /v1/commands?node=`, `GET /v1/commands/{id}` | commands and their status |
| `GET /v1/stacks`, `GET /v1/stacks/{name}` | stacks with each workload's node, status, container and last command; one with its Compose file |
| `POST /v1/stacks` | `{"name", "compose", "nodes"?, "exposures"?}`: make or change a stack, on the nodes named (ids, `name` labels or hostnames; `[]` any, left out as it was) with its exposures on the pool (left out: as they were); answers it and the file's warnings |
| `POST /v1/stack-check` | the same body, changing nothing: the services with the ports the file asks for, the warnings, the nodes resolved, and where the new workloads would go (`placements`) or why not (`unplaced`) |
| `POST /v1/stacks/{name}/start`, `/stop`, `/restart`; `DELETE /v1/stacks/{name}` | run, stop, restart (stop, and the reconcile starts it) or remove |
| `GET /v1/storage` | the cluster's shared storage (G11): each with its driver's capabilities and the volumes on it, with the node holding each volume for one writer |
| `POST /v1/storage`, `DELETE /v1/storage/{name}` | `{"name", "driver": "nfs"\|"path", "source", "options"?}`: add storage; take it away (refused while a stack keeps a volume on it) |

### Stacks

What a stack's services are reached on from outside the cluster, and nothing else leaves it: an **exposure** on a
pool address (`freightdock-manager stack expose app web:80=192.0.2.10:443`, or the UI), answered on that address
only, the port its own there so other stacks may use it on other addresses. Anything else is internal, reached by
name inside the stack. A Compose file's `ports:` open nothing, on a node's own address or anywhere: they are what the
file asks for, which the UI offers to expose on the pool when it deploys the stack (two stacks may both ask for 80).
The UI too: nothing a container serves is answered on a node's or a manager's own address. The address and port
follow the workloads wherever they run.

A stack may be given the nodes it runs on (`stack deploy app app.yml --on w3,w4`, by id, `name` label or hostname, or the UI): its
workloads are placed only there, moved there when the list changes, and wait (unplaced) while none of them can take
them. None given, it runs on any healthy node.

A stack is a Compose file. Its services become workloads, one per replica, with the id `<stack>.<service>.<n>`; the
manager that takes the deploy places each new one on a healthy node with Docker that meets its constraints, has its
room for its reservation (and, off the overlay, the managers' own stack's host port free), on the nodes chosen for the
stack if any, preferring nodes that meet its preferences, then spread over a label,
then away from its other replicas, then the least loaded. From then on the manager each worker is connected to
compares what the worker reports in every heartbeat with what the state says and sends a command for the difference:
run (a missing container, one made for an older spec, one not on its overlay address, or one that exited and its
restart policy runs again), stop (a stopped stack), remove (a stack container the state no longer holds). One
command per workload at a time; a run again waits the policy's delay (5 s by default), doubling to 5 minutes, and
stops after its `max_attempts` (none by default) until the workload has run for its `window` (10 s). The count is
each manager's own, in memory: a manager that restarts counts again. Supported Compose: `image`, `command`,
`environment`, `labels`, `ports` (ranges too: each port of the range), `volumes` (named or absolute),
`container_name`, `restart` (`no`, `always`, `unless-stopped`, `on-failure[:N]`), `deploy.replicas`,
`deploy.restart_policy` (`condition`, `delay`, `max_attempts`, `window`), `deploy.placement` (`constraints` such as
`node.labels.ssd==true` or `node.hostname!=old`; `preferences` with `spread: node.labels.<key>` or
`prefer: <constraint>`), `deploy.resources` (`limits` on the container, `reservations` for placement),
`x-freightdock: {ha: false}` (pinned) and `x-freightdock: {apart: [db, other-stack/db]}` (never on a node with
those services' workloads, both ways); `depends_on` and `healthcheck` are set aside with a warning, and any other
key is refused by name. Variables (`$VAR`, `${VAR}`, `${VAR:-default}` and Compose's other forms) are filled from
the stack's env file, as is an environment name given with no value. A workload whose node no longer meets its
constraints (its labels changed) is put on one that does, by the leader, without a deploy; one with nowhere to go
stays. The CLI: `freightdock-manager stack deploy|ls|ps|start|stop|restart|rm`.

A deploy may give options beside the file, kept with the stack, each left out as it was: `--description <text>`,
`--env-file <file>`, `--selector zone=a,disk!=hdd` (node labels every workload needs), `--mode
stateless|pinned|shared:<storage>|file` (every workload moves with local volumes; waits for its node; moves with its
named volumes on that storage, one writer; or as the file says) and `--name <service>=<name>` (a container name of
its own, over `container_name`: one workload's in the cluster, `-<n>` added per replica when it has more, and a name
for it inside the stack). The API takes the same as `description`, `env`, `selector`, `mode`
(`{"kind": "shared", "storage": "nas"}`, null for the file) and `names`.

Finished commands are dropped from the state an hour after they finished, or past the newest 1000, each workload's
latest kept.

### Fencing

A node the cluster no longer hears is fenced before anything it ran is trusted to be gone. The worker fences itself:
with no manager contact for N seconds it stops its managed containers and refuses new ones; after N+X+Y it reboots the
host (`FREIGHTDOCK_FENCE_REBOOT=if-needed`, the default, only when a container would not stop; `always`; `never`).
systemd's watchdog (`WatchdogSec=30` in the unit) restarts a worker that hangs, and a restarted or rebooted worker
starts fenced. The leader manager follows each unreachable node: fencing requested, in progress, then fenced, either
presumed (its timers have run out, plus 10 s) or proven by a provider. A node that comes back fenced waits for
authorization (`freightdock-manager authorize <node>`, the UI, or by itself when `FREIGHTDOCK_FENCE_AUTO_AUTHORIZE` is
on); a node heard again before it fenced is simply at rest again. Managers cut off from the quorum drop their workers'
sessions, so those workers fence.

A node labelled `fence.provider=proxmox`, `proxmox.node=<pve node>`, `proxmox.vmid=<id>` (and `proxmox.type=lxc`, the
default, or `qemu`) is powered off through the Proxmox API instead, and is fenced once Proxmox says the guest is
stopped; a failure is tried again after a minute. The API token needs `VM.PowerMgmt` and `VM.Audit` on the guest and is read from a
file; the server's certificate is pinned by its SHA-256 fingerprint. A token that can do nothing else:

```sh
pveum role add FreightdockFence --privs "VM.PowerMgmt,VM.Audit"
pveum user add freightdock-fence@pve
pveum user token add freightdock-fence@pve fence --privsep 1     # prints the secret once
for id in 201 202; do                                             # the workers' guests
  pveum acl modify /vms/$id --users freightdock-fence@pve --roles FreightdockFence
  pveum acl modify /vms/$id --tokens 'freightdock-fence@pve!fence' --roles FreightdockFence
done
openssl x509 -in /etc/pve/local/pve-ssl.pem -noout -fingerprint -sha256
```

| Variable | Default | |
|---|---|---|
| `FREIGHTDOCK_FENCE` | `on` | Managers: follow and fence unreachable nodes. Workers: fence themselves |
| `FREIGHTDOCK_FENCE_STOP_S`, `_STORAGE_S`, `_REBOOT_S` | `30`, `15`, `15` | N, X and Y; the managers send theirs to each worker |
| `FREIGHTDOCK_FENCE_AUTO_AUTHORIZE` | `on` | Authorize a node that came back fenced by itself |
| `FREIGHTDOCK_FENCE_REBOOT` | `if-needed` | Worker: `if-needed`, `always` or `never` |
| `FREIGHTDOCK_FENCE_PROXMOX_URL` | none | e.g. `https://pve:8006` |
| `FREIGHTDOCK_FENCE_PROXMOX_TOKEN_FILE` | none | A file holding `user@realm!name=secret` |
| `FREIGHTDOCK_FENCE_PROXMOX_FINGERPRINT` | none | The server certificate's SHA-256, `AA:BB:...` |
| `FREIGHTDOCK_RECOVERY_DELAY_S` | `0` | How long a node stays fenced before its workloads move |

### Recovery

Once a node is fenced (and `FREIGHTDOCK_RECOVERY_DELAY_S` has passed), or comes back fenced, the leader places its
stateless workloads, and those whose volumes are all on shared storage, on other nodes as a deploy would. A workload
with a volume on its node waits for it (its data is there), and so does a pinned one (`x-freightdock: {ha: false}`);
one with nowhere to go is tried again each second.
When the node runs again, the workloads that moved are removed from it. `freightdock-manager recoveries [stack]`
shows what moved where and what stayed and why.

### Shared storage

Storage is given to the cluster once, and a stack keeps a named volume on it from its Compose file:

```sh
freightdock-manager storage add nas nfs 192.0.2.50:/export/freightdock vers=4.2,hard   # the workers mount it
freightdock-manager storage add shared path /mnt/shared   # a directory already shared on every node
```

```yaml
services:
  db:
    image: postgres:17
    volumes: ["pg:/var/lib/postgresql/data"]
volumes:
  pg:
    x-freightdock: {storage: nas}                # access: one (the default) or many
```

The volume is the directory `<stack>/<volume>` on the storage. An `nfs` storage is mounted by the worker at
`<data>/storage/<name>` the first time a workload needs it; a `path` storage is only looked for (a mount the host
has, or a bind mount into a container that cannot mount NFS itself, such as an unprivileged LXC). A volume for one
writer (`access: one`, a database's files) is used by one workload, and the managers lease it to one node at a time:
a workload that moves runs on its new node only once the old one let the volume go (its container there is gone) or
is fenced. `access: many` is for what the application itself keeps safe to share. A fencing worker unmounts its
`nfs` storage once its workloads are stopped (`FREIGHTDOCK_FENCE_STORAGE_S` after); a `path` storage is the host's to
take away, so with one a provider such as Proxmox is what makes a fenced node let go of it. A plain named volume
stays local to each node, as before.

Workers in unprivileged Proxmox LXCs cannot mount NFS. Mount the export on the Proxmox host (a systemd mount unit,
so nothing in the cluster-wide `storage.cfg` changes), bind it into each worker and add it as a `path` storage:

```sh
pct set 201 -mp0 /mnt/freightdock,mp=/srv/freightdock            # on the host, for each worker
freightdock-manager storage add nas path /srv/freightdock
```

The LXC's root writes as uid 100000 on the export, so the export has to let it (its owner, or open permissions).

## Build

```sh
cargo test                 # unit tests, and tests/cluster.rs: real managers in one process
cargo clippy --all-targets -- -D warnings
```

GPL-3.0-only, as Freightdock.
