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
src/admin.rs      the HTTP/JSON admin API the CLI and the UI use
deploy/           static build, install over ssh, systemd unit
```

The repositories, side by side: **Freightdock-Contracts** (the gRPC contract), **Freightdock-Manager** (this), **Freightdock-Worker** (the
worker-agent), **Freightdock-Network** (G9), **Freightdock** (the UI, an image the managers run).

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

The script works over `ssh root@<host>`; `LABELS=key=value,...` sets the worker's labels. It writes
`/etc/freightdock/manager.env` and `worker.env`, which are the whole configuration (below). By hand: a worker join
token from `freightdock-manager join-token`, a manager one from `freightdock-manager manager-token`, each good once.

With two managers, losing either stops every change (a majority of two is two), so grow from one manager to three.
Port 7450/tcp must be open between managers and from workers to managers; nothing needs to be open on a worker.

## Settings

| Variable | Default | What |
|---|---|---|
| `FREIGHTDOCK_MANAGER_DATA` | `/var/lib/freightdock-manager` | State log, CA, manager id, admin token |
| `FREIGHTDOCK_MANAGER_BIND` | `0.0.0.0:7450` | The worker API (gRPC, mutual TLS) |
| `FREIGHTDOCK_MANAGER_API_BIND` | `127.0.0.1:7451` | The admin HTTP API, or `off` |
| `FREIGHTDOCK_MANAGER_ADVERTISE` | `<hostname>:7450` | Where the others reach this manager, comma separated (the first is its Raft address) |
| `FREIGHTDOCK_MANAGER_JOIN` | | First start only: join the cluster of the manager at `host[:port]` |
| `FREIGHTDOCK_MANAGER_JOIN_TOKEN` | | With it: a token from `manager-token` (`FDMT1-...`) |
| `FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN` | | Leave a worker join token in this file for the worker on this node |
| `FREIGHTDOCK_HEARTBEAT_MS` | `2000` | How often workers send a heartbeat |
| `FREIGHTDOCK_SUSPECT_MS` | `5000` | Silent this long: suspect |
| `FREIGHTDOCK_UNREACHABLE_MS` | `12000` | Silent this long: unreachable (which never means stopped) |
| `RUST_LOG` | `freightdock_manager=info,warn` | How much it logs |

## Admin API

All under `http://127.0.0.1:7451`, with `Authorization: Bearer <data>/admin.token`.

| | |
|---|---|
| `GET /v1/cluster` | cluster id, manager id, managers, Raft members (voter, leader), applied index, CA fingerprint |
| `GET /v1/nodes`, `GET /v1/nodes/{id}` | nodes with their last report and live health (from any manager that hears them) |
| `DELETE /v1/nodes/{id}` | forget a node: its certificate opens nothing more, its stream is cut |
| `POST /v1/join-tokens` | `{"ttl_s": 86400}` gives `{"token": "FDCT1-..."}`, for a worker |
| `POST /v1/manager-tokens` | `{"ttl_s": 3600}` gives `{"token": "FDMT1-..."}`, for a manager |
| `GET /v1/events?after=&node=` | recent node events |
| `POST /v1/nodes/{id}/commands` | `{"op": "ping"}` and the other ops; answers the command (202) |
| `GET /v1/commands?node=`, `GET /v1/commands/{id}` | commands and their status |

## Build

```sh
cargo test                 # unit tests, and tests/cluster.rs: real managers in one process
cargo clippy --all-targets -- -D warnings
```

GPL-3.0-only, as Freightdock.
