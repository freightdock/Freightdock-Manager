# freightdock-manager

The manager-agent of a Freightdock cluster: the control plane and its database. It owns the cluster's state,
replicated between managers with Raft (openraft), enrols workers and managers with join tokens, keeps a long-lived
mutual-TLS gRPC stream to each worker that connects to it, tracks their health with the other managers, and sends
them commands. One manager is a whole cluster; add managers to survive losing one. The plan is in
[ROADMAP.md](ROADMAP.md), what has been done in [PROGRESS.md](PROGRESS.md).

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
Freightdock-Manager/deploy/install-node.sh first  10.0.0.11              # manager + worker: a whole cluster on one node
Freightdock-Manager/deploy/install-node.sh join   10.0.0.12 10.0.0.11    # another manager + worker, joining it
Freightdock-Manager/deploy/install-node.sh join   10.0.0.13 10.0.0.11    # a third (managers go 1, 3, 5)
Freightdock-Manager/deploy/install-node.sh worker 10.0.0.21 10.0.0.11    # a worker only
```

The script works over `ssh root@<host>`; `LABELS=key=value,...` sets the worker's labels, and `UI_IMAGE=<image>`
with `first` runs the Freightdock UI on the new cluster (below). It writes
`/etc/freightdock/manager.env` and `worker.env`, which are the whole configuration (below). By hand: a worker join
token from `freightdock-manager join-token`, a manager one from `freightdock-manager manager-token`, each good once.

With two managers, losing either stops every change (a majority of two is two), so grow from one manager to three.
Port 7450/tcp must be open between managers and from workers to managers; nothing needs to be open on a worker.

## The UI

`freightdock-manager ui <image>` runs the Freightdock UI (the Freightdock-UI repository's image) as the managers' own
stack, `freightdock`: one workload, placed on a node like any other and moved when that node fails, on a one-node
cluster too once it is back. Every manager answers for it at port 7443, forwarding the connection as plain TCP to the
node it runs on (port 7453 there), so the UI's own TLS goes end to end and any manager's address reaches it.
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
| `FREIGHTDOCK_MANAGER_UI_BIND` | `0.0.0.0:7443` | Where this manager answers for the UI, forwarded to its node, or `off` |
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
| `POST /v1/stacks` | `{"name", "compose"}`: make or change a stack; answers it and the file's warnings |
| `POST /v1/stacks/{name}/start`, `/stop`, `/restart`; `DELETE /v1/stacks/{name}` | run, stop, restart (stop, and the reconcile starts it) or remove |

### Stacks

What a stack's services are reached on from outside the cluster, and nothing else leaves it: an **exposure** on a
pool address (`freightdock-manager stack expose app web:80=192.0.2.10:443`, or the UI), answered on that address
only, the port its own there so other stacks may use it on other addresses; a Compose file's `ports:`, answered on
every node's own address and never on a pool address; anything else is internal, reached by name inside the stack.
The address and port follow the workloads wherever they run.

A stack is a Compose file. Its services become workloads, one per replica, with the id `<stack>.<service>.<n>`; the
manager that takes the deploy places each new one on a healthy node with Docker that meets its constraints, has its
room for its reservation (and its host ports free, only for a workload off the overlay: on it, the routing mesh
answers a published port on every node, so replicas may share one), preferring nodes that meet its preferences, then spread over a label,
then away from its other replicas, then the least loaded. From then on the manager each worker is connected to
compares what the worker reports in every heartbeat with what the state says and sends a command for the difference:
run (a missing or exited container, or one made for an older spec), stop (a stopped stack), remove (a stack
container the state no longer holds). One command per workload at a time; one that keeps failing is tried again
after 5 s, doubling to 5 minutes. Supported Compose: `image`, `command`, `environment`, `labels`, `ports`, `volumes`
(named or absolute), `deploy.replicas`, `deploy.placement` (`constraints` such as `node.labels.ssd==true` or
`node.hostname!=old`; `preferences` with `spread: node.labels.<key>` or `prefer: <constraint>`),
`deploy.resources` (`limits` on the container, `reservations` for placement) and `x-freightdock: {ha: false}` (pinned); `restart`, `depends_on` and `healthcheck` are set aside with a warning, and any
other key is refused by name. The CLI: `freightdock-manager stack deploy|ls|ps|start|stop|restart|rm`.

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
file; the server's certificate is pinned by its SHA-256 fingerprint.

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
stateless workloads on other nodes as a deploy would. A workload with volumes waits for its node (its data is
there), and so does a pinned one (`x-freightdock: {ha: false}`); one with nowhere to go is tried again each second.
When the node runs again, the workloads that moved are removed from it. `freightdock-manager recoveries [stack]`
shows what moved where and what stayed and why.

## Build

```sh
cargo test                 # unit tests, and tests/cluster.rs: real managers in one process
cargo clippy --all-targets -- -D warnings
```

GPL-3.0-only, as Freightdock.
