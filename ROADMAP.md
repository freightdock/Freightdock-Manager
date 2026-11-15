# Freightdock cluster: roadmap

The plan (`Freightdock-plan.md`) split into goals, ranked by importance and in the order they have to be done. Each
goal ends with something that runs on the test cluster, not just code that compiles. Tick items off in the change that
does them, so the next person can pick up from there.

Status marks: **[x]** done, **[~]** in progress, **[ ]** not started.

## The order, at a glance

| Rank | Goal | Why it is here | Needs | Status |
|---|---|---|---|---|
| 1 | G1 Manager and worker talk | Everything else rides on this link | | **done** |
| 2 | G2 The link is hardened and observable | Cheap now, expensive to retrofit; the test cluster must tell us what happens | G1 | **done** |
| 3 | G3 Manager high availability (Raft) | Success criteria 1 and 2 of the first release; changes how every write works, so before anything writes much | G2 | **done** |
| 4 | G4 Workloads and Compose stacks | The point of the product: deploy and manage stacks across workers | G3 | **done** (2026-11-13) |
| 4a | G8 first slice: a Cluster page in Freightdock | Pulled forward (2026-11-11) so every goal from here is seen in the browser as it is built | G3 | **done** |
| 5 | G5 Scheduling and labels | Workloads need placing once there is more than one worker | G4 | **done** (all of it, 2026-11-13) |
| 6 | G6 Fencing | A worker must be able to fence itself before anything is recovered elsewhere (plan rule 10, 11) | G4 | done |
| 7 | G7 Failure detection and stateless recovery | Success criteria 4 and 5; safe only with G6 | G5, G6 | done |
| 8 | G8 UI: Freightdock as the cluster's UI | Usable by people, not just the CLI; can start in parallel once G4's API settles | G4 | **done** |
| 9 | G9 Internal networking (Network repo) | Stable workload IPs across workers; postponed by the plan's first release | G7 | **done** (2026-11-13) |
| 10 | G10 External networking | Exposing services without per-deploy setup | G9 | **done** (2026-11-13) |
| 11 | G11 Shared storage | Moving stateful workloads safely; needs fencing and storage leases | G6, G7 | first slice done (2026-11-13) |
| 12 | G12 Advanced | Ceph, replicated storage, gateway redundancy, anti-affinity, more fencing providers | G9 to G11 | scoped; metrics done (2026-11-14) |

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
    its sign-in, including SSO (OIDC) and LDAP. The Portainer import is set aside until everything else is done. (Back, 2026-11-14: Compose stacks into cluster stacks.)
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
- [x] Running on the test cluster (three Proxmox LXCs), all cluster tests passing.

## G2: The link is hardened and observable (done, 2026-11-11; 6 and 7 since)

Ranked inside the goal:

1. [x] Network cut test (a firewall drop, not a process stop): worker gives up on the manager in about 10 s, manager
       shows suspect then unreachable, both recover when the link returns.
2. [x] Manager-side command expiry: a pending command past its deadline becomes `expired`, so nothing runs late.
3. [x] The worker keeps its command journal on disk, so a restart does not forget what it already did.
4. [x] Node events (connected, disconnected, health changes, enrolled, removed) in memory, `GET /v1/events`.
5. [x] A `status` CLI command: cluster, managers, nodes and recent events in one view.
6. [x] CI for Contracts, Network, Manager and Worker (gitleaks with Freightdock's rules, tests, clippy as errors),
       each checking out the repositories it needs beside it; written 2026-11-13, runs from the first push.
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
5. [x] The rest (2026-11-13): `${VAR}` interpolation (all of Compose's forms) and an env file kept with the stack;
       port ranges; `container_name` and names given at deploy; finished commands pruned past an hour or the newest
       1000 (`CommandsPruned`, each workload's latest kept); the restart policy (`restart`, `deploy.restart_policy`:
       condition, delay, max_attempts, window; a limit's attempts kept in the state from 2026-11-13, `WorkloadAttempted`
       and `WorkloadSettled`, so a manager that takes over counts on). Networks between a stack's services came with G9.
6. [x] A deploy's options (owner, 2026-11-13: "a lable selector, and a way to choose the state of the stack ... a
       custom name ... a description"): `StackOptions` kept with the stack, applied over the file
       (`compose::apply_options`): description, env file, label selector, mode (stateless, pinned, shared storage),
       container names; in the API, the CLI and the UI's deployer.

## G5: Scheduling and labels

1. [x] Node labels editable through the API, CLI and UI (`NodeLabelsSet`; they go over the worker's own).
2. [x] Placement: required and excluded labels (`deploy.placement.constraints`, `==` / `!=` on node.id, hostname,
       platform and labels), preferred labels (`preferences: - prefer: ...`), spread over a label, replicas of a
       service kept apart, host ports, reservations against the node's CPUs and memory; limits on the container.
       A deploy moves a workload whose node no longer meets its constraints.
3. [x] HA flag (`x-freightdock: {ha: false}` pins) beside the replica count, kept in the state; G7 acts on it.
4. [x] Re-placing by itself when a node no longer qualifies (2026-11-13: the leader's `replace_round`,
       `StackPlaced`; a service with nowhere to go stays and holds back nothing else); anti-affinity between services
       (`x-freightdock.apart`, of the stack or another, both ways). Tried on the test cluster: a label swap moved two
       workloads in seconds, the third kept off a node by `apart` stayed.
5. [x] A move waits for the old copy to stop (2026-11-13), as Docker's `stop-first`: the workload keeps the node it
       left (`Workload.leaving`) until that node's manager says its copy is gone (`WorkloadLeft`), or the node is
       fenced or removed; `deploy.update_config.order: start-first` starts the new copy beside the old. Tried on the
       test cluster: a writer on a volume for many moved w3 to w4, its lines in the shared file in two runs, never
       interleaved, 7 s apart; the stack said what it waited for.

## G6: Fencing

1. [x] The node fencing state machine on the manager (suspect, unreachable, fencing requested, in progress, fenced,
       failed, awaiting authorization, authorized), in the replicated state; the leader drives it.
2. [x] Worker self-fencing timers (N, N+X, N+X+Y), configurable; stop managed workloads at N; at N+X the worker
       unmounts the cluster's NFS storage (G11).
3. [x] A local watchdog so a frozen worker-agent still fences: systemd's (`Type=notify`, `WatchdogSec=30`). Its limit:
       a restarted worker starts fenced, but a host frozen as a whole (kernel, not the agent) is only covered by
       the reboot step or an external provider.
4. [x] After a reboot: no managed workload starts until the managers authorize it (the hello's fencing state).
5. [x] External fencing provider; Proxmox as the first (per node labels, pinned certificate, token from a file).
       Tried on the test cluster 2026-11-13 against a real Proxmox VE 9 (a token for the workers' LXCs only): a
       worker cut off was requested at 12 s, powered off by Proxmox and fenced, verified, at 16 s (the self-fencing
       path takes about 85 s); started again, it was authorized and at rest within 2 s.
6. [x] Every setting from the UI (owner, 2026-11-13): the timings, fencing and the Proxmox provider are the
       cluster's replicated settings (`SettingsSet`, `/v1/settings`, CLI `settings`), each over a manager's own
       variable; the workers take new timers on the same stream. The UI's Integrations page ("Cluster" 23). Tried on
       the test cluster: timers changed live on both workers, the Proxmox provider moved from the managers' files into
       the settings (files removed), and w4 fenced through it, verified, in 8 s.

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
       MCP server, the Portainer import (last). The MCP server and the Portainer import are back (2026-11-14): MCP's tools call
       the cluster routes; a Portainer Compose stack comes in as a cluster stack, made stopped unless asked
       (`POST /v1/stacks` takes `"stopped": true`).
       Install is one line per role on the node itself (2026-11-14): `install.sh first|worker|join|upgrade`, from
       the release workflow's static amd64/arm64 builds (`edge` from main, `vX.Y.Z` from tags), checked by SHA256SUMS.
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
       - Reached on an address of the pool at 443 (2026-11-13, `ui <image> [address]`), as every stack is: never on a
         node's or a manager's own address. At first every manager forwarded 7443 to the UI's node at 7453; that
         made the UI answer on the managers' addresses, against the rule that a service keeps one address of its own,
         so it went. The UI's certificate names the managers' hosts and its pool address (`FREIGHTDOCK_TLS_NAMES`
         filled in), and its CA travels in the UI's database snapshot, so the certificate a browser trusted once is
         still trusted after a move.
       - A worker whose pull fails runs the image already on its node, if there is one (images built or loaded on
         the node, never pushed).
6. [x] Backups of the managers' state (2026-11-13), the first of item 3's set-aside features to come back: the
       replicated state (stacks, nodes, storage, settings, the UI's database) and the cluster CA, sealed with a
       passphrase (PBKDF2-HMAC-SHA256, 600 000 rounds, to AES-256-GCM; `backup.rs`). Downloaded on demand
       (`POST /v1/backup`, CLI `backup <file>`, the Integrations page) or written by each manager to `<data>/backups/`
       on a schedule (settings `backup_every_h`, `backup_keep`, a write-only `backup_passphrase`; none without it).
       `restore <file>` readies a new manager's empty data folder: the CA in place, the state taken whole at its first
       start (`Entry::StateRestored`, only by a cluster that has none; commands not yet sent are expired, never sent
       twice). The workers' certificates stay good, since the CA is the same.
7. [x] A node's Docker through its worker (2026-11-13), item 3's next: `DockerRequest`/`DockerReply` on the worker's
       stream, not commands (a read is large and only for now, so nothing goes to the log). The manager holding the
       stream waits for the reply by id; another forwards through `ManagerPeer.Docker`. Reads: info, containers,
       inspect, logs (at most 5000 lines, 1 MiB), images, volumes, networks. Changes: start, stop, restart and remove
       a container, remove, pull and prune images, remove a volume or network; never the cluster's own (a workload's
       container, a stack's network or volume), which the worker refuses. `GET`/`POST /v1/nodes/{id}/docker`, CLI
       `docker <node> <action>`, a worker's `docker_requests` capability (an older worker is told apart).
8. [x] Private registries (2026-11-13), item 3's next: a login per registry in the replicated state (`registry.rs`;
       the password never shown), filled into `RunContainer.auth` and a page's pull by the manager that sends it, never
       kept with a command. `GET`/`PUT`/`DELETE /v1/registries/{host}`, a sign-in test (v2 Basic or Bearer), CLI
       `registry ls|add|test|rm`, the UI's Integrations page.
9. [x] Notifications (2026-11-13): channels (webhook, e-mail, ntfy, Gotify) in the replicated state (`notify.rs`),
       their secrets never shown and their last outcome kept (`ChannelOutcome`). The leader's watch tells what changed
       since its last look: node health, fencing, recoveries, workload containers that died, failed runs, managers; a
       manager tells its own failed backup. Ten minutes' quiet per event and target. `GET /v1/notifications`,
       `PUT`/`DELETE /v1/notifications/{name}`, a test, CLI `notify ls|set|test|rm`, the UI's Integrations page.
       Not told: a container's failing health check (the workers do not report health yet).

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
       when no container of the stack is left on it. A container not on its overlay address (made before the worker
       knew the overlay) is made again (2026-11-13: workers report each container's addresses; tried on the cluster
       by moving one onto Docker's bridge: made again on its address in 5 s).
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
4. [x] The client's address kept across nodes, on by default (owner, 2026-11-13: "ip adress passthrough needs to be
       added by default"): a connection the mesh forwards to another node goes by an IP-in-IP tunnel inside the
       overlay (`fdt-<n>`, marked after the DNAT, an `ip rule` per node) and its replies come back by it, so the
       workload sees the client (Network's DESIGN.md, G10.4). Tried both ways on the test cluster.
5. [x] The pool (owner, 2026-11-13): a stack picks an address and port from the pool for a service, outside its
       Compose file, a port unique per address (two stacks share 443 on two addresses); a pool address answers only
       its exposures, a Compose `ports:` only the nodes' own addresses, the rest is internal, and the UI says which.
       - [x] Contracts `Exposure.address`; Network: the rules per address, Compose ports kept off the pool; Manager:
             `Stack.exposures`, `StackExposed`, `FloatingInUse`, `PUT /v1/stacks/{name}/exposures`,
             `stack expose`, `stack ps` says how each service is reached.
       - [x] The UI (`freightdock:cluster-ui-12`): "Reached from outside" per stack (pool, every worker, internal
             only), the exposures editor, what each pool address exposes ("Cluster" 17).
6. [x] Only the pool and inside (owner, 2026-11-13: "it should only be accessable via the ip pool adresses and
       internally"): a Compose `ports:` opens nothing anywhere (no mesh rule, no Docker publishing), the UI's
       neither (since 2026-11-13 on its own pool address, G8.5); it is what the file asks for, offered by the UI as an exposure on a free pool address.
       The cross-stack port check went with it. Pool ranges (`192.0.2.10-20`, a prefix), an unused address not
       announced. A stack runs on the nodes chosen for it (`Stack.nodes`, `stack deploy --on`), moved there when the
       choice changes; a deploy takes its exposures too, checked whole; `POST /v1/stack-check` answers what a deploy
       would do. In the UI (`freightdock:cluster-ui-15`): the deployer page, search and filters ("Cluster" 19, 20).

### G11: shared storage

Built 2026-11-13 (plan 4.6, 5.2, 7.1, 8), the first slice; the README's "Shared storage" says how it is used.

1. [x] Storage definitions in the replicated state (`StorageAdded`, `StorageRemoved`, refused while a stack keeps
       a volume on it), each driver's capabilities (plan 8.3): `nfs` (the worker mounts it; it can take writes away)
       and `path` (a directory the host shares, for nodes that cannot mount NFS, such as unprivileged LXCs).
       `/v1/storage`, CLI `storage ls|add|rm`.
2. [x] A Compose volume on storage (`x-freightdock: {storage, access}`), kept on the stack; `access: one` is one
       workload's only; the worker makes the storage ready and binds `<stack>/<volume>` from it.
3. [x] Leases (`LeaseTaken`, `LeaseReleased`): a volume for one writer is leased to one node; another takes it only
       once the holder let it go or is fenced (or gone). The reconcile takes the lease before a run and lets it go
       when nothing of the volume runs on the node any more; stops and removals go before runs, so a move hands over.
4. [x] Recovery: a workload whose volumes are all on shared storage moves once its node is fenced, as a stateless one
       does. Fencing unmounts the node's NFS storage at N+X.
5. [x] Tried on the test cluster with a `path` storage (a local directory on each worker; the workers are
       unprivileged LXCs, which may not mount NFS): moves both ways handed over with no overlap, and a worker cut off
       was fenced, its writer stopped and run on the other worker 45 s after its last write.
6. [x] The owner's NFS share on the test cluster (2026-11-13): mounted on the Proxmox host, bound into each worker
       LXC and added as a `path` storage. A volume for many was written from both workers and each saw the other's
       files; a volume for one moved between them handing over the same file 7 s apart, never two writers.
       The storage step tried 2026-11-13 on a worker in a VM that mounts the export itself (`nfs` storage): cut off
       from the managers only (the export still in reach), it fenced at 30 s, its writer's last write was at 38 s (the
       container's stop grace), the export was unmounted at 45 s, and the managers presumed it fenced at 85 s; back, it
       was authorized, mounted the export again and its writer went on. The same VM fenced through Proxmox (`proxmox.type=qemu`):
       powered off and verified 14 s after the cut, its last write at 12 s.
7. [x] The UI (`freightdock:cluster-ui-17`): storage on the Cluster page (added and removed by an Administrator),
       volumes and their holders on the stacks page, the volumes on storage in the deployer ("Cluster" 21).
8. [x] Storage health (2026-11-13): each worker probes the storage it made ready every 15 s on a thread of its own
       (an NFS export mounted at its root, else a write would land on the node's own disk; its space; a small file
       written, synced, read back and removed), a probe stuck past 10 s said as such and never piled on, and reports it
       in its heartbeat (`StorageReport`); the managers pass it on, `GET /v1/storage` shows it per node, the leader
       tells `storage.unhealthy` and `storage.back`, and a node whose storage fails takes no new workload with volumes
       on it (one there already stays, so a passing hiccup moves nothing).
9. [ ] Left for later: read-only remounts, SMB, block storage and the cluster filesystems (plan 8.2), replicated
       storage (G12).

### G12: advanced

Scoped 2026-11-14 against the plan's phase 11. Much of it came with the earlier goals:

1. [x] Anti-affinity: `x-freightdock: {apart: [...]}` (G4), both ways and across stacks; excluded labels are
       constraints with `!=`; spread and preferences; resource-aware placement with reservations.
2. [x] Gateway redundancy: the floating addresses (G10.3) move to a healthy worker once their holder is fenced, and
       every node answers the mesh's ports.
3. [x] Storage-aware placement and its health (G11.8).
4. [x] Monitoring (2026-11-14): `GET /v1/metrics`, Prometheus' text format from any manager for the whole cluster
       (`metrics.rs`), with a read token over the TLS API.
5. [x] Redfish fencing (2026-11-14, `redfish.rs`): a node labelled `fence.provider=redfish` is powered off through
       its BMC (`ForceOff`, pinned certificate, one cluster account in the settings) and fenced once the BMC says its
       power is off. Tried only against a fake BMC in the tests: the test cluster has no server with a BMC.
6. [x] Replicated storage and Ceph (2026-11-14). A `cephfs` storage (`ceph.rs` on both sides): the worker's kernel
       mounts it (mount(2), the key in the call only), its keys kept apart and filled into a command as it is sent,
       and a fenced node's mounts evicted by the leader through a `CephCommand` (the `ceph` CLI in the Ceph image on
       a healthy worker), so Ceph blocklists them. Tried on three VMs with Ceph 20 (cephadm, one 16 GB OSD each):
       a writer on vm2 cut off from the managers stopped at 30 s, a stray writer holding the mount stopped at the
       forced unmount (CephFS aborts the session), the writer ran on vm3 25 s after the managers presumed vm2
       fenced, and vm2's clients were evicted 33 s after (the first worker asked had to pull the image; the retry
       on another took 1.6 s). Ceph made by the managers (`CephSetup`, `Cephadm` steps on the workers, the disks
       each node offers): tried on the three VMs with their 16 GB disks, 16 steps done the first time, the storage
       made from the keys step and a writer running on it. The UI does it from its Integrations page: torn down and set
       up again from there by ticking the three disks, the same 16 steps done, the writer running on it again.
7. [x] Shells (2026-11-14, `shell.rs` on both sides): an administrator's shell on a node (a login shell as root on a
       terminal of the worker's own) or in a container (`docker exec` with a terminal), as a WebSocket from the admin
       API (`/v1/nodes/{id}/shell`), on a gRPC stream of its own the worker dials back (WorkerSession.Shell) so its
       output never holds up the heartbeats, through ManagerPeer.Shell when the node is another manager's. The
       cluster's `shells` setting turns them off; `FREIGHTDOCK_WORKER_HOST_SHELL=off` turns the host's off on a node.
       Tried from the UI on the test cluster: root on vm1, idle 20 s and on, and sh in a busybox container on vm3 with
       its exit code back.
8. [x] Failure tests (2026-11-15, `qa/chaos.py`): a dead leader, a cut-off follower, a cut-off leader, the manager
       holding the workers' streams cut off for two minutes, and a worker cut off from every manager, on the test
       cluster, with a watcher that fails the run if a one-replica workload ever runs on two nodes at once. 37 of 37
       checks passed. Found on the way: a stranded workload's reason named another workload of its stack (fixed);
       a moved workload is back only as fast as its image is pulled on its new node (about 100 s for one not there).
9. [x] Upgrades from the managers (2026-11-15, `release.rs`): the cluster's release in the state and its binaries in
       the managers' folders (uploaded, or a published release fetched and checked against its SHA256SUMS), rolled
       out a node at a time by the leader, workers first and itself last, each back on the version and healthy before
       the next; the UI's Upgrades page and `freightdock-manager release` / `upgrade`. Workers fetch the binary from
       their manager over their session. The worker and manager say `--version`, refuse arguments they do not take,
       and a worker older than the managers is marked outdated.

## Open questions for the owner

1. ~~Freightdock's role~~ Answered 2026-11-11: the UI only, its own image, run by the managers (decision 11).
2. ~~Publish under the freightdock GitHub organisation~~ Answered 2026-11-13: yes, all of them, pushed once the
   roadmap is finished (the repositories are not made yet). CI is written now and runs from the first push.
3. ~~Worker in Agent or its own repository~~ Answered 2026-11-11: its own (decision 10); done.
4. For G3: two more manager LXCs, or managers on the worker containers too? For now every test LXC runs a manager
   and a worker (decision 12 makes that the normal shape anyway).
5. ~~G8.1: how Freightdock's data gets into the managers' state~~ Answered 2026-11-12: Freightdock keeps its SQLite
   and SQL; each write's change set is committed to the managers through Raft, and a UI starting anywhere rebuilds its
   database from them (decision 16).
6. ~~Per-node Docker pages~~ Answered 2026-11-12: kept, rebuilt to go through the workers (after G8.1 and G8.3);
   the direct-Docker code goes with the standalone mode.
7. ~~The deployer's suggested pool address~~ Answered 2026-11-13: a Compose port is offered as an exposure on a free
   pool address, as built ("i think thats fine").
8. ~~Storage for G11~~ Given 2026-11-13: an NFS share on the owner's LAN for the test cluster (its address is not
   written here).
9. ~~Replicated storage or Ceph (G12.6)~~ Answered 2026-11-14: yes, on three VMs with a 16 GB disk each, and Ceph
   set up from the UI too (select the workers and disks).
