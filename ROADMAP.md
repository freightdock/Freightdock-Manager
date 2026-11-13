# Freightdock cluster: roadmap

The plan (`Freightdock-plan.md`) split into goals, ranked by importance and in the order they have to be done. Each
goal ends with something that runs on the test cluster, not just code that compiles. Tick items off in the change that
does them, and write what was done in [PROGRESS.md](PROGRESS.md) so the next person can pick up from there.

Status marks: **[x]** done, **[~]** in progress, **[ ]** not started.

## The order, at a glance

| Rank | Goal | Why it is here | Needs | Status |
|---|---|---|---|---|
| 1 | G1 Manager and worker talk | Everything else rides on this link | | **done** |
| 2 | G2 The link is hardened and observable | Cheap now, expensive to retrofit; the test cluster must tell us what happens | G1 | **done** but CI (blocked on publishing) |
| 3 | G3 Manager high availability (Raft) | Success criteria 1 and 2 of the first release; changes how every write works, so before anything writes much | G2 | **done** |
| 4 | G4 Workloads and Compose stacks | The point of the product: deploy and manage stacks across workers | G3 | **done** (first slice; see G4) |
| 4a | G8 first slice: a Cluster page in Freightdock | Pulled forward (2026-11-11) so every goal from here is seen in the browser as it is built | G3 | **done** |
| 5 | G5 Scheduling and labels | Workloads need placing once there is more than one worker | G4 | **done** |
| 6 | G6 Fencing | A worker must be able to fence itself before anything is recovered elsewhere (plan rule 10, 11) | G4 | done |
| 7 | G7 Failure detection and stateless recovery | Success criteria 4 and 5; safe only with G6 | G5, G6 | done |
| 8 | G8 UI: Freightdock as the cluster's UI | Usable by people, not just the CLI; can start in parallel once G4's API settles | G4 | **done** |
| 9 | G9 Internal networking (Network repo) | Stable workload IPs across workers; postponed by the plan's first release | G7 | **done** (2026-11-12, small items left) |
| 10 | G10 External networking | Exposing services without per-deploy setup | G9 | done but item 4 (the client's address across nodes), agreed for later |
| 11 | G11 Shared storage | Moving stateful workloads safely; needs fencing and storage leases | G6, G7 | not started |
| 12 | G12 Advanced | Ceph, replicated storage, gateway redundancy, anti-affinity, more fencing providers | G9 to G11 | not started |

G1 to G7 plus basic monitoring is the plan's **first working release** (plan section 14). G6 is ranked before G7
although the plan has recovery first: recovering a workload whose old copy may still be running is the one mistake the
plan forbids outright, so the fencing state machine comes first and recovery is built on it.

Cross-cutting work (security, testing, docs) is listed inside each goal rather than as a goal of its own, so it is never
left for later.

## Repositories

Every core part is a repository of its own, named `Freightdock-<part>` (the owner's rules, 2026-11-11). The new
ones were renamed so locally. The UI is **Freightdock-UI**, a new repository (owner's decision, 2026-11-12) started
from Freightdock's `feat/cluster-ui` work without its history; Freightdock itself stays as it is (the standalone
product, archived later), its `feat/cluster-ui` branch local and never pushed.

| Repository | Crate(s) | Role | State |
|---|---|---|---|
| **Freightdock-Contracts** | `freightdock-proto` | Shared, versioned API contract: protobuf + gRPC, join tokens, shared names. Every repo depends on it and on no other repo's internals. | new, local |
| **Freightdock-Manager** | `freightdock-manager` | manager-agent and the cluster's database: replicated state (Raft), enrolment, sessions, commands; later scheduler, IPAM, fencing coordination, users. Holds this roadmap and the install script. | new, local |
| **Freightdock-Worker** | `freightdock-worker` | worker-agent: runs what the managers place on its host. | new, local (moved out of Agent) |
| **Freightdock-Network** | `freightdock-network` (library) | The network-agent (G9): the node's WireGuard key, the plan and its application; the worker runs it. | local |
| **Freightdock-UI** | `freightdock` | The UI only, its own image, which the managers run as a workload (G8). Database replicated by the managers (G8.1). | new, local |
| **Freightdock** | `freightdock` | The standalone product, left as it is; the Portainer import comes back from here later. Its local branch `feat/cluster-ui` holds the UI work up to G8.2 (G8.1 in a stash), now in Freightdock-UI. | kept, unchanged |
| **Agent** | `freightdock-agent`, `freightdock-tunnel` | The single-host tunnel agent of the standalone Freightdock (remote Docker hosts over a tunnel). Not part of the cluster: **deprecated**, its job is the worker's. Freightdock-UI no longer depends on it (G8.3); only the standalone Freightdock does. Branch `feat/cluster-worker` holds nothing now. | deprecated |

While nothing is published the crates point at each other by path (`../Freightdock-Contracts`), so the checkouts sit side by side.

## Decisions (and why)

1. **Rust, tonic gRPC, prost, protox.** The stack Freightdock and Agent already use. protox compiles `.proto` in Rust:
   no `protoc` anywhere.
2. **One long-lived bidirectional stream per worker** (`WorkerSession.Open`). The worker dials out (works behind NAT,
   nothing to open on workers); the manager pushes commands down the same stream.
3. **Join token + cluster CA + mutual TLS from day one**, the shape of the existing agent's FDJT1 with its own prefix
   (FDCT1). Managers present a certificate for the fixed name `manager.freightdock.internal`, so any IP or name works.
   A node is known by its certificate's SHA-256; removing the node revokes it at once.
4. **State is a deterministic state machine over a log** (`state.rs` + `store.rs`). Raft replaces only `store.rs`.
5. **Heartbeats are not in the log.** Liveness is per manager, in memory; only transitions that matter become entries.
6. **Commands are idempotent by id and their status only moves forward.** The worker answers a resend with what it
   already did; unfinished commands are resent on every connect.
7. **Workloads are found by label** (`io.freightdock.workload=<id>`) and created with restart policy `no`, so Docker
   never restarts a managed workload behind the cluster's back.
8. **The admin API is HTTP/JSON on loopback** with a bearer token from the data folder; gRPC stays internal.
9. **Fencing before recovery** (see the order above).
10. **One repository per core part** (UI, manager, worker, network, contracts); they meet only through Contracts and
    the manager's API.
11. **Freightdock is only the UI**, an image of its own that the managers run (and keep running) as a cluster
    workload. Users, sessions and settings live in the manager's replicated state, behind the manager API, so the UI
    holds nothing and can run anywhere, or move.
12. **One node is a whole cluster, and that is how a cluster starts.** A single host runs manager, worker (and, once
    G8 is there, the UI) by itself, with nothing outside it; more nodes join it later. Nothing may assume more than one
    node, and each goal is checked on a single node too. `Freightdock-Manager/deploy/install-node.sh first <host>` is that start.
13. **One command per role** (owner's requirement, 2026-11-11): when done, a single command run on the node sets up a
    whole stack (`first`), a manager that joins (`join`) or a worker (`worker`), with everything that role needs:
    manager, worker, network-agent, and the UI on the first node. The agents run on the host under systemd (they
    manage Docker and the host's network, so they must not depend on Docker); what the cluster runs, the UI included,
    runs as containers. Today's `install-node.sh` works over ssh from another machine; it becomes that command.
14. **No standalone Freightdock** (owner's decision, 2026-11-12): the single-host product (Freightdock managing Docker
    hosts directly: local socket, tunnel agent, environments) goes; a one-node cluster does that job. Freightdock keeps
    its sign-in, including SSO (OIDC) and LDAP. The Portainer import is set aside until everything else is done.
15. **External integrations last** (owner's decision, 2026-11-12): the Proxmox fencing provider stays in the code but
    is not tried or extended until then; the same for other outside systems.
16. **The UI's database is replicated by the managers** (owner's choice, 2026-11-12): Freightdock keeps its SQLite
    and its SQL. Every write's change set (SQLite's session extension) is committed to the managers through Raft
    before the write counts, numbered so only one UI instance can write (a stale one is refused and reloads); a UI
    starting on any node rebuilds its database from the managers' snapshot and change sets. Reads stay local.

## G1: Manager and worker talk (done, 2026-11-11)

- [x] Contracts: `worker.proto` v1 (Enrollment, WorkerSession, commands, statuses, resources, capabilities), FDCT1 token.
- [x] Manager: cluster CA, cluster id, join tokens, enrolment, mTLS worker API, session registry, health (healthy /
      suspect / unreachable, configurable), commands with delivery and resend, admin API, CLI.
- [x] Worker: node UUID, enrolment, mTLS session, hello with labels / resources / capabilities, heartbeats every 2 s,
      executor (ping, docker info, run / stop / remove container by workload), failover over a manager list.
- [x] In-process end-to-end test (`Agent/worker/tests/e2e.rs`).
- [x] Static binaries (`Freightdock-Manager/deploy/build-static.sh`) and systemd units.
- [x] Running on the test cluster (three Proxmox LXCs), all tests in PROGRESS.md passing.

## G2: The link is hardened and observable (done, 2026-11-11, but 6 and 7)

Ranked inside the goal:

1. [x] Network cut test (a firewall drop, not a process stop): worker gives up on the manager in about 10 s, manager
       shows suspect then unreachable, both recover when the link returns.
2. [x] Manager-side command expiry: a pending command past its deadline becomes `expired`, so nothing runs late.
3. [x] The worker keeps its command journal on disk, so a restart does not forget what it already did.
4. [x] Node events (connected, disconnected, health changes, enrolled, removed) in memory, `GET /v1/events`.
5. [x] A `status` CLI command: cluster, managers, nodes and recent events in one view.
6. [ ] CI for Contracts and Manager (tests, clippy, gitleaks), as Agent has. **Blocked** on publishing (question 2).
7. [x] Certificate renewal before expiry. Managers (done 2026-11-12): their server and client certificates (90 days)
       are made again at the first handshake after 30 days (`tls::Rotating`), so a manager that runs for months never
       shows an expired one. Workers (5 years, done 2026-11-12): in its last year the welcome says
       `renew_certificate`, the worker asks `WorkerSession.Renew` with its current certificate for one for a new key,
       keeps both whole (`renewal.json`, finished at the next start if cut off) and uses it from the next stream; the
       managers keep the one before it good until the next renewal (`NodeCertRenewed`), so a worker that lost the new
       one is not locked out.

## G3: Manager high availability (Raft) (done, 2026-11-11)

1. [x] openraft behind `store.rs`; the log in a JSON-lines file; snapshots of `ClusterState`; the pre-Raft log migrated.
2. [x] Manager-to-manager gRPC (`manager.proto` in Contracts), mutual TLS with manager certificates.
3. [x] Any manager takes any request: a follower forwards `propose` to the leader.
4. [x] Manager join: a manager join token; the new manager gets the CA key over the authenticated join, joins as a
       learner, then a voter. Membership changes through consensus.
5. [x] Quorum loss stops every change (proposals fail fast with "no quorum"); reads still answer.
6. [x] Welcome's `managers` list comes from the replicated membership, so workers learn of new managers.
7. [x] Health transitions agreed across managers: a node is unreachable for the cluster only when no manager hears it.
8. [x] Test: three managers, kill the leader, workers reconnect elsewhere and commands carry on.
9. [x] One node runs it all: the manager leaves a worker join token for the worker beside it
       (`FREIGHTDOCK_MANAGER_LOCAL_WORKER_TOKEN` / `FREIGHTDOCK_JOIN_TOKEN_FILE`), and `deploy/install-node.sh` sets up
       a node as `first` (a whole cluster), `join` (manager + worker joining one) or `worker`.

## G4: Workloads and Compose stacks

1. [x] Stack, service and workload identities in the state; desired versus actual state per workload.
2. [x] Workers report actual state per workload; the manager reconciles desired against actual.
3. [x] Compose parsing on the manager (the subset mapping to RunContainer: image, command, env, labels, ports,
       volumes, deploy.replicas). Resources (limits) wait for G5.
4. [x] Deploy, start, stop, restart, update, remove a stack through the admin API and CLI, and the UI's Cluster
       stacks page.
5. [ ] Left for later: `${VAR}` interpolation and an env file; networks between a stack's services (G9); port
       ranges; `container_name`; old finished commands are never pruned from the state (they should be, past an age);
       the restart policy of a crashing workload is a fixed backoff, not configurable.

## G5: Scheduling and labels

1. [x] Node labels editable through the API, CLI and UI (`NodeLabelsSet`; they go over the worker's own).
2. [x] Placement: required and excluded labels (`deploy.placement.constraints`, `==` / `!=` on node.id, hostname,
       platform and labels), preferred labels (`preferences: - prefer: ...`), spread over a label, replicas of a
       service kept apart, host ports, reservations against the node's CPUs and memory; limits on the container.
       A deploy moves a workload whose node no longer meets its constraints.
3. [x] HA flag (`x-freightdock: {ha: false}` pins) beside the replica count, kept in the state; G7 acts on it.
4. [ ] Left for later: re-placing by itself when labels change (today at the next deploy); anti-affinity between
       different services; a move starts the new copy without waiting for the old to stop (fine for stateless; G6/G7
       and G11 make it safe for the rest).

## G6: Fencing

1. [x] The node fencing state machine on the manager (suspect, unreachable, fencing requested, in progress, fenced,
       failed, awaiting authorization, authorized), in the replicated state; the leader drives it.
2. [x] Worker self-fencing timers (N, N+X, N+X+Y), configurable; stop managed workloads at N. X (storage) does
       nothing yet: there are no storage leases until G11.
3. [x] A local watchdog so a frozen worker-agent still fences: systemd's (`Type=notify`, `WatchdogSec=30`). Its limit:
       a restarted worker starts fenced, but a host frozen as a whole (kernel, not the agent) is only covered by
       the reboot step or an external provider.
4. [x] After a reboot: no managed workload starts until the managers authorize it (the hello's fencing state).
5. [x] External fencing provider; Proxmox as the first (per node labels, pinned certificate, token from a file).
       Checked on the cluster only up to "no settings"; a real power-off waits for a Proxmox API token.

## G7: Failure detection and stateless recovery

1. [x] Recovery scheduling for stateless workloads, after a recovery delay (`FREIGHTDOCK_RECOVERY_DELAY_S`, from
       fenced) and only once the old node is fenced; placed as a deploy would. Kinds (plan 5.2): stateless moves,
       volumes (local data) and `ha: false` (pinned) wait for their node. The state refuses a move off a node that is
       not fenced, or of a workload that may not move. A removed node's workloads get a node too.
2. [x] Duplicate execution prevention: the returning node's reconcile removes the containers of workloads that
       moved (the state no longer places them there), and runs nothing before it is authorized.
3. [x] Recovery history in the replicated state (latest 200): `GET /v1/recoveries`, CLI `recoveries`, the UI.

## G8: UI (Freightdock as the cluster's UI only)

First slice, pulled forward and done 2026-11-11 (Freightdock branch `feat/cluster-ui`): the Cluster page (managers,
nodes, health, events, commands, join tokens) through Freightdock's own sign-in, against the manager's admin API on the
same host. Each later goal adds its pages there (stacks with G4, and so on).

1. [x] Users, sessions and settings in the manager's replicated state: Freightdock's whole database, replicated row
       by row through Raft (decision 16), one writer at a time. Signing in stays Freightdock's (with SSO and LDAP);
       the manager API takes tokens (G8.2).
2. [x] The manager API reachable from the UI: a second port (7452), TLS with the cluster CA for the managers' fixed
       name, API tokens (read or admin) in the replicated state; Freightdock takes several `https://` managers and goes
       on to the next when one cannot be reached. The users of item 1 come in front of it later.
3. [x] Freightdock trimmed to the UI (Freightdock-UI, 2026-11-12): no Docker of its own, no agents, no stacks,
       templates, webhooks, registries, image updates, notifications, history, backups, MCP or Portainer import; two
       roles (Administrator, user). Its own image: the binary alone, no socket. Set aside to come back through the
       managers: a node's Docker pages via its worker, registries, notifications, backups of the managers' state, the
       MCP server, the Portainer import (last).
4. [x] A cluster section: nodes, health, stacks, events, fencing, recoveries (the Cluster and Cluster stacks pages).
5. [x] The managers run the UI as a system workload (G4's workloads): placed on a node, recovered when that node
       goes, on the single-node cluster too; reachable through any manager. How (2026-11-12):
       - A reserved stack, `freightdock`, with one service `ui` (workload `freightdock.ui.1`): `PUT /v1/ui {"image"}`,
         `DELETE /v1/ui`, CLI `freightdock-manager ui <image>|off|status`, `install-node.sh` with `UI_IMAGE`. A user's
         stack may not take the name. No volume: the UI's database is the managers' (item 1), so it is stateless and
         G7 moves it like any stateless workload.
       - Its secrets are never in the replicated state. The spec holds placeholders (`@freightdock:token`, `:ca`,
         `:managers`, `:names`); the manager that sends the run command fills them (`Manager::wire`), and only for
         the `freightdock` stack's workloads. The API token is derived from the cluster CA's key, which every manager
         holds, so each fills in the same one; the state keeps its hash only (API token `freightdock-ui`, admin).
       - Every manager forwards 7443 (`FREIGHTDOCK_MANAGER_UI_BIND`) as plain TCP to the UI's node at 7453, looked up
         at each connection from where its worker connected from; the UI's TLS goes end to end. The UI's certificate
         names every manager's host (`FREIGHTDOCK_TLS_NAMES` filled in), and its CA travels in the UI's database
         snapshot, so the certificate a browser trusted once is still trusted after a move.
       - A worker whose pull fails runs the image already on its node, if there is one (images built or loaded on
         the node, never pushed).

## G9 to G12

Internal networking (Network repo, overlay chosen after writing down the plan's 6.6 requirements), external IPs and
ports, shared storage with leases and capabilities, then the advanced items, in that order.

### G9: internal networking

The design is Freightdock-Network's [DESIGN.md](../Freightdock-Network/DESIGN.md), agreed 2026-11-12: a WireGuard
mesh (`fd0`) with each workload's address routed to its node, the network-agent a library the worker runs, a /24 and
bridge per stack that talks only to itself (nftables), names through the hosts file.

1. [x] Contracts: the node's WireGuard key in its report, the desired network state down the session.
2. [x] Manager: IPAM (cluster range, a /24 per stack, an address per workload kept across moves), the network state
       per node.
3. [x] Network: the agent (`ip`, `wg`, `nft`: the interface, peers, routes, proxy ARP, the table), tested against a
       fake runner and by hand on two nodes.
4. [x] Worker: runs the agent; workloads on their stack's network with their address and the hosts of their stack.
5. [x] Test cluster (2026-11-12): services reaching each other across nodes by name, two stacks kept apart, a moved
       workload reached at the same address, published ports and the internet still working, the UI on the overlay.
6. [x] After a move, the stack's containers left on the old node forget the moved address's MAC (`nsenter ... ip
       neigh flush`) when its container is removed there, so they reach it at once; a node removes a stack's network
       when no container of the stack is left on it. Left: a container made before the worker knew of the overlay is
       not made again by itself (only possible on a cluster that ran a G9 manager with older workers).
7. [x] The network in the UI: each workload's address on the stack and node pages, each node's overlay state.
8. [x] Owner's wish (2026-11-12), once the network part is done: a live, interactive view of the network that shows
       the communication between workloads and nodes. Agreed 2026-11-12: built from the flows (who talks to whom).
       - [x] Each worker reports its share of the traffic at every heartbeat (Freightdock-Network `traffic.rs`, from
             conntrack, each connection counted by one node); the managers keep the latest per node, pass it between
             them and answer `GET /v1/traffic` with its ends named.
       - [x] The UI's live view (`/cluster/network`, `freightdock:cluster-ui-11`): outside, a column per node with
             its workloads, a moving line per pair of ends sized by its traffic, focus on hover or click, and the
             flows as a table.

### G10: external networking

Agreed 2026-11-12 (Freightdock-Network's DESIGN.md): a routing mesh first, floating external IPs next; the client's
address across nodes later.

1. [x] The routing mesh: every node answers each published port (`table inet freightdock`, before Docker's rules)
       and forwards it over the overlay to the workloads behind it, spread at random; a node's overlay address
       (`10.240.0.<n>`, in Raft, `NodeAddressed` for nodes from before); a published port is one stack's in the
       cluster and never the host's own (22, 7450 to 7452, 51820), refused at deploy.
2. [x] Docker no longer publishes a stack workload's port itself (the mesh does), so a service's replicas are not
       held to one per node by their port; loopback access to a port (and the UI's target on a one-node cluster)
       goes through the node's own address instead. A workload its node reports not running takes no new
       connections (one not heard of yet still does). A workload off the overlay (all 254 stack networks taken)
       keeps Docker's publishing and holds its port on its node.
3. [x] Floating external IPs: an address given to the cluster (`freightdock-manager floating add <address>`,
       `/v1/floating`), held by one worker at a time (`FloatingHeld`, refused while the holder may still answer),
       given by the leader to the healthy worker at rest holding fewest, moved only once its holder is fenced. The
       worker puts it on `fd-ext` and announces it with a gratuitous ARP; a node being fenced is sent none, and a
       worker that fences itself takes them off first. Every published port answers on it. In the UI (`freightdock:cluster-ui-10`):
       a card on the Cluster page, each address with its holder, added and removed by an Administrator.
4. [ ] Later: the client's address kept for connections forwarded to another node (policy routing on the replies).
5. [x] The pool (owner, 2026-11-13): a stack picks an address and port from the pool for a service, outside its
       Compose file, a port unique per address (two stacks share 443 on two addresses); a pool address answers only
       its exposures, a Compose `ports:` only the nodes' own addresses, the rest is internal, and the UI says which.
       - [x] Contracts `Exposure.address`; Network: the rules per address, Compose ports kept off the pool; Manager:
             `Stack.exposures`, `StackExposed`, `FloatingInUse`, `PUT /v1/stacks/{name}/exposures`,
             `stack expose`, `stack ps` says how each service is reached.
       - [x] The UI (`freightdock:cluster-ui-12`): "Reached from outside" per stack (pool, every worker, internal
             only), the exposures editor, what each pool address exposes ("Cluster" 17).
6. [x] Only the pool and inside (owner, 2026-11-13: "it should only be accessable via the ip pool adresses and
       internally"): a Compose `ports:` opens nothing anywhere (no mesh rule, no Docker publishing) but for the
       managers' own UI stack; it is what the file asks for, offered by the UI as an exposure on a free pool address.
       The cross-stack port check went with it. Pool ranges (`192.0.2.10-20`, a prefix), an unused address not
       announced. A stack runs on the nodes chosen for it (`Stack.nodes`, `stack deploy --on`), moved there when the
       choice changes; a deploy takes its exposures too, checked whole; `POST /v1/stack-check` answers what a deploy
       would do. In the UI (`freightdock:cluster-ui-15`): the deployer page, search and filters ("Cluster" 19, 20).

## Open questions for the owner

1. ~~Freightdock's role~~ Answered 2026-11-11: the UI only, its own image, run by the managers (decision 11).
2. Publish Contracts, Manager, Worker and Network under the freightdock GitHub organisation? Blocks G2.6 (CI).
3. ~~Worker in Agent or its own repository~~ Answered 2026-11-11: its own (decision 10); done.
4. For G3: two more manager LXCs, or managers on the worker containers too? For now every test LXC runs a manager
   and a worker (decision 12 makes that the normal shape anyway).
5. ~~G8.1: how Freightdock's data gets into the managers' state~~ Answered 2026-11-12: Freightdock keeps its SQLite
   and SQL; each write's change set is committed to the managers through Raft, and a UI starting anywhere rebuilds its
   database from them (decision 16).
6. ~~Per-node Docker pages~~ Answered 2026-11-12: kept, rebuilt to go through the workers (after G8.1 and G8.3);
   the direct-Docker code goes with the standalone mode.
