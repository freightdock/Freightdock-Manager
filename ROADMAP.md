# Freightdock cluster: roadmap

The plan (`Freightdock-plan.md`) split into goals, ranked by importance and in the order they have to be done. Each
goal ends with something that runs on the test cluster, not just code that compiles. Tick items off in the change that
does them, and write what was done in [PROGRESS.md](PROGRESS.md) so the next person can pick up from there.

Status marks: **[x]** done, **[~]** in progress, **[ ]** not started.

## The order, at a glance

| Rank | Goal | Why it is here | Needs | Status |
|---|---|---|---|---|
| 1 | G1 Manager and worker talk | Everything else rides on this link | | **done** |
| 2 | G2 The link is hardened and observable | Cheap now, expensive to retrofit; the test cluster must tell us what happens | G1 | **done** but CI (blocked) and cert renewal (deferred) |
| 3 | G3 Manager high availability (Raft) | Success criteria 1 and 2 of the first release; changes how every write works, so before anything writes much | G2 | **done** |
| 4 | G4 Workloads and Compose stacks | The point of the product: deploy and manage stacks across workers | G3 | **next** |
| 4a | G8 first slice: a Cluster page in Freightdock | Pulled forward (2026-11-11) so every goal from here is seen in the browser as it is built | G3 | **done** |
| 5 | G5 Scheduling and labels | Workloads need placing once there is more than one worker | G4 | not started |
| 6 | G6 Fencing | A worker must be able to fence itself before anything is recovered elsewhere (plan rule 10, 11) | G4 | not started |
| 7 | G7 Failure detection and stateless recovery | Success criteria 4 and 5; safe only with G6 | G5, G6 | not started |
| 8 | G8 UI: Freightdock as the cluster's UI | Usable by people, not just the CLI; can start in parallel once G4's API settles | G4 | not started |
| 9 | G9 Internal networking (Network repo) | Stable workload IPs across workers; postponed by the plan's first release | G7 | not started |
| 10 | G10 External networking | Exposing services without per-deploy setup | G9 | not started |
| 11 | G11 Shared storage | Moving stateful workloads safely; needs fencing and storage leases | G6, G7 | not started |
| 12 | G12 Advanced | Ceph, replicated storage, gateway redundancy, anti-affinity, more fencing providers | G9 to G11 | not started |

G1 to G7 plus basic monitoring is the plan's **first working release** (plan section 14). G6 is ranked before G7
although the plan has recovery first: recovering a workload whose old copy may still be running is the one mistake the
plan forbids outright, so the fencing state machine comes first and recovery is built on it.

Cross-cutting work (security, testing, docs) is listed inside each goal rather than as a goal of its own, so it is never
left for later.

## Repositories

Every core part is a repository of its own, named `Freightdock-<part>` (the owner's rules, 2026-11-11). The new
ones were renamed so locally; the UI (today `Freightdock`) becomes `Freightdock-UI` when it is trimmed to the UI (G8.3),
a rename the owner does on GitHub.

| Repository | Crate(s) | Role | State |
|---|---|---|---|
| **Freightdock-Contracts** | `freightdock-proto` | Shared, versioned API contract: protobuf + gRPC, join tokens, shared names. Every repo depends on it and on no other repo's internals. | new, local |
| **Freightdock-Manager** | `freightdock-manager` | manager-agent and the cluster's database: replicated state (Raft), enrolment, sessions, commands; later scheduler, IPAM, fencing coordination, users. Holds this roadmap and the install script. | new, local |
| **Freightdock-Worker** | `freightdock-worker` | worker-agent: runs what the managers place on its host. | new, local (moved out of Agent) |
| **Freightdock-Network** | none yet | network-agent (G9). | placeholder |
| **Freightdock** (to be **Freightdock-UI**) | `freightdock` | The UI only, its own image, which the managers run as a workload (G8). Untouched so far. | untouched |
| **Agent** | `freightdock-agent`, `freightdock-tunnel` | The single-host tunnel agent of today's Freightdock (remote Docker hosts over a tunnel). Not part of the cluster: **deprecated**, its job is the worker's. Today's Freightdock still depends on it (git tag v1.0.0) until G8.3 removes that; then it is archived. Branch `feat/cluster-worker` holds nothing now. | deprecated |

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
7. [ ] (deferred to before the first release) Certificate renewal before expiry (worker certificates last 5 years, server certificates 90 days and are made
       again at each start; renewal over the session is enough for now).

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

1. [ ] Stack, service and workload identities in the state; desired versus actual state per workload.
2. [ ] Workers report actual state per workload; the manager reconciles desired against actual.
3. [ ] Compose parsing on the manager (the subset mapping to RunContainer: image, command, env, labels, then ports,
       volumes and resources as the contract grows).
4. [ ] Deploy, start, stop, restart, update, remove a stack through the admin API and CLI.

## G5: Scheduling and labels

1. [ ] Node labels editable through the API (today only from the worker's environment).
2. [ ] Placement: required, preferred and excluded labels; resources reported against requests.
3. [ ] HA flag and replica count as separate settings (plan 5.3); pinned / non-HA workloads.

## G6: Fencing

1. [ ] The node fencing state machine on the manager (suspect, unreachable, fencing requested, in progress, fenced,
       failed, awaiting authorization).
2. [ ] Worker self-fencing timers (N, N+X, N+X+Y), configurable; stop managed workloads at N.
3. [ ] A local watchdog so a frozen worker-agent still fences.
4. [ ] After a reboot: no managed workload starts until the managers authorize it (the hello's fencing state).
5. [ ] External fencing provider interface; Proxmox as the first provider.

## G7: Failure detection and stateless recovery

1. [ ] Recovery scheduling for stateless workloads, after a recovery delay and only once the old node is fenced or
       the workload type allows it.
2. [ ] Duplicate execution prevention (a returning node is told to remove a workload that moved).
3. [ ] Recovery event history.

## G8: UI (Freightdock as the cluster's UI only)

First slice, pulled forward and done 2026-11-11 (Freightdock branch `feat/cluster-ui`): the Cluster page (managers,
nodes, health, events, commands, join tokens) through Freightdock's own sign-in, against the manager's admin API on the
same host. Each later goal adds its pages there (stacks with G4, and so on).

1. [ ] Users, sessions and settings in the manager's replicated state; login and roles on the manager API (the
       admin token stays for the CLI on the manager's host).
2. [ ] The manager API reachable from the UI: HTTP on the cluster port or a second port, TLS, behind the users above
       (today it is loopback-only).
3. [ ] Freightdock trimmed to the UI: its server becomes a client of the manager API; its SQLite, local Docker
       access and tunnel agent go (or stay only where they serve the UI itself). Its own image.
4. [ ] A cluster section: nodes, health, stacks, events.
5. [ ] The managers run the UI as a system workload (G4's workloads): placed on a node, recovered when that node
       goes, on the single-node cluster too; reachable through any manager.

## G9 to G12

Internal networking (Network repo, overlay chosen after writing down the plan's 6.6 requirements), external IPs and
ports, shared storage with leases and capabilities, then the advanced items, in that order.

## Open questions for the owner

1. ~~Freightdock's role~~ Answered 2026-11-11: the UI only, its own image, run by the managers (decision 11).
2. Publish Contracts, Manager, Worker and Network under the freightdock GitHub organisation? Blocks G2.6 (CI).
3. ~~Worker in Agent or its own repository~~ Answered 2026-11-11: its own (decision 10); done.
4. For G3: two more manager LXCs, or managers on the worker containers too? For now every test LXC runs a manager
   and a worker (decision 12 makes that the normal shape anyway).
