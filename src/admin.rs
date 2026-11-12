//! The manager's HTTP API for people and the UI: nodes, join tokens and commands, as JSON. Bound to loopback by
//! default and signed in by the bearer token in `<data>/admin.token`; the UI's server will reach it the same way until
//! its own sign-in is wired to the cluster.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use freightdock_proto::token::JoinToken;

use crate::state::{CommandRecord, CommandStatus, Entry, Op, Rejected};
use crate::store::ProposeError;
use crate::{Manager, now_ms};

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<ProposeError> for ApiError {
    fn from(e: ProposeError) -> Self {
        match e {
            ProposeError::Rejected(r @ (Rejected::NodeUnknown | Rejected::CommandUnknown | Rejected::StackUnknown)) => ApiError(StatusCode::NOT_FOUND, r.to_string()),
            ProposeError::Rejected(r) => ApiError(StatusCode::CONFLICT, r.to_string()),
            e @ (ProposeError::Unavailable(_) | ProposeError::NotLeader(_)) => ApiError(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            ProposeError::Io(e) => {
                tracing::error!("{e:#}");
                ApiError(StatusCode::SERVICE_UNAVAILABLE, "the change could not be committed".into())
            }
        }
    }
}

type R<T> = Result<T, ApiError>;

fn authed(m: &Manager, h: &HeaderMap) -> R<()> {
    let given = h.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    let ok = !m.admin_token.is_empty() && given.len() == m.admin_token.len() && given.bytes().zip(m.admin_token.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;
    if ok { Ok(()) } else { Err(ApiError(StatusCode::UNAUTHORIZED, "a valid bearer token is needed".into())) }
}

pub fn router(m: Arc<Manager>) -> Router {
    Router::new()
        .route("/v1/health", get(|| async { Json(json!({ "ok": true })) }))
        .route("/v1/cluster", get(cluster))
        .route("/v1/nodes", get(nodes))
        .route("/v1/nodes/{id}", get(node).delete(remove_node))
        .route("/v1/nodes/{id}/commands", post(create_command))
        .route("/v1/nodes/{id}/labels", axum::routing::put(set_labels))
        .route("/v1/nodes/{id}/authorize", post(authorize))
        .route("/v1/join-tokens", post(join_token))
        .route("/v1/manager-tokens", post(manager_token))
        .route("/v1/commands", get(commands))
        .route("/v1/events", get(events))
        .route("/v1/commands/{id}", get(command))
        .route("/v1/stacks", get(stacks).post(deploy_stack))
        .route("/v1/stacks/{name}", get(stack).delete(remove_stack))
        .route("/v1/stacks/{name}/{action}", post(stack_action))
        .with_state(m)
}

async fn cluster(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let managers = m.managers();
    let voters = m.store.voters();
    let leader = m.store.leader();
    let raft = raft_members(m.store.members(), &voters, leader, m.store.id);
    Ok(Json(m.store.read(|s| {
        json!({
            "cluster_id": s.cluster_id,
            "manager_id": m.manager_id,
            "managers": managers,
            "raft": raft,
            "applied_index": s.applied,
            "nodes": s.nodes.len(),
            "ca_sha256": m.ca.fingerprint(),
            "version": env!("CARGO_PKG_VERSION"),
        })
    })))
}

fn node_json(m: &Manager, n: &crate::state::Node) -> Value {
    json!({
        "id": n.id,
        "hostname": n.hostname,
        "enrolled_at": n.enrolled_at,
        "reported_at": n.reported_at,
        "report": n.report,
        "labels": n.labels,
        "all_labels": n.all_labels(),
        "fencing": n.fencing,
        "liveness": m.sessions.liveness(&n.id),
    })
}

async fn nodes(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let list: Vec<Value> = m.store.read(|s| s.nodes.values().map(|n| node_json(&m, n)).collect());
    Ok(Json(json!(list)))
}

async fn node(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<Json<Value>> {
    authed(&m, &h)?;
    m.store.read(|s| s.nodes.get(&id).map(|n| node_json(&m, n))).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such node".into()))
}

async fn remove_node(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::NodeRemoved { node_id: id.clone(), at: now_ms() }).await?;
    m.sessions.kick(&id);
    tracing::info!(node = %id, "node removed");
    m.events.push("removed", Some(&id), "removed from the cluster");
    Ok(StatusCode::NO_CONTENT)
}

/// Lets a node that came back fenced run workloads again (plan 7.4); the manager it is connected to tells it.
async fn authorize(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<Json<Value>> {
    authed(&m, &h)?;
    m.authorize(&id, "an administrator").await?;
    m.store.read(|s| s.nodes.get(&id).map(|n| node_json(&m, n))).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such node".into()))
}

#[derive(Deserialize)]
struct LabelsBody {
    labels: std::collections::BTreeMap<String, String>,
}

/// A label key: what Compose constraints can name (`node.labels.<key>`).
fn valid_label_key(k: &str) -> bool {
    !k.is_empty() && k.len() <= 63 && k.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-/".contains(&b))
}

/// Sets the node's API labels, all of them (G5.1); they go over the worker's own. Placement reads them at the next
/// deploy.
async fn set_labels(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>, Json(b): Json<LabelsBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    if b.labels.len() > 64 || !b.labels.iter().all(|(k, v)| valid_label_key(k) && v.len() <= 255 && !v.chars().any(char::is_control)) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "a label key is 1 to 63 of letters, digits and . _ - /; a value at most 255 characters; 64 labels at most".into()));
    }
    let shown = b.labels.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(", ");
    m.store.propose(Entry::NodeLabelsSet { node_id: id.clone(), labels: b.labels, at: now_ms() }).await?;
    m.events.push("labels", Some(&id), if shown.is_empty() { "labels cleared".to_string() } else { format!("labels set: {shown}") });
    m.store.read(|s| s.nodes.get(&id).map(|n| node_json(&m, n))).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such node".into()))
}

#[derive(Deserialize)]
struct TokenBody {
    /// How long the token is good for, in seconds; a day when not given, a week at most.
    ttl_s: Option<i64>,
}

async fn join_token(State(m): State<Arc<Manager>>, h: HeaderMap, body: Option<Json<TokenBody>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let ttl = body.and_then(|b| b.ttl_s).unwrap_or(86_400).clamp(60, 7 * 86_400);
    let token = JoinToken { ca_sha256: m.ca.fingerprint(), secret: hex::encode(rand::random::<[u8; 32]>()) };
    let expires_at = now_ms() + ttl * 1000;
    m.store.propose(Entry::JoinTokenIssued { secret_hash: token.secret_hash(), expires_at, at: now_ms(), for_manager: false }).await?;
    Ok(Json(json!({ "token": token.to_string(), "expires_at": expires_at, "managers": m.managers() })))
}

/// A token for another manager to join with (`FREIGHTDOCK_MANAGER_JOIN_TOKEN`). Good once, an hour by default.
async fn manager_token(State(m): State<Arc<Manager>>, h: HeaderMap, body: Option<Json<TokenBody>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let ttl = body.and_then(|b| b.ttl_s).unwrap_or(3600).clamp(60, 7 * 86_400);
    let token = JoinToken { ca_sha256: m.ca.fingerprint(), secret: hex::encode(rand::random::<[u8; 32]>()) };
    let expires_at = now_ms() + ttl * 1000;
    m.store.propose(Entry::JoinTokenIssued { secret_hash: token.secret_hash(), expires_at, at: now_ms(), for_manager: true }).await?;
    Ok(Json(json!({ "token": token.to_manager_string(), "expires_at": expires_at, "join": m.advertise.first() })))
}

#[derive(Deserialize)]
struct CommandBody {
    #[serde(flatten)]
    op: Op,
    /// How long the worker has to start it, in seconds; a minute when not given.
    deadline_s: Option<i64>,
}

async fn create_command(State(m): State<Arc<Manager>>, h: HeaderMap, Path(node_id): Path<String>, Json(b): Json<CommandBody>) -> R<(StatusCode, Json<CommandRecord>)> {
    authed(&m, &h)?;
    if let Op::RunContainer { workload_id, .. } = &b.op
        && crate::stacks::is_stack_workload(workload_id)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "an id of the form stack.service.number is a stack's; pick another".into()));
    }
    let now = now_ms();
    let c = CommandRecord {
        id: uuid::Uuid::new_v4().to_string(),
        node_id: node_id.clone(),
        op: b.op,
        config_version: m.store.read(|s| s.applied),
        created_at: now,
        deadline: now + b.deadline_s.unwrap_or(60).clamp(1, 86_400) * 1000,
        status: CommandStatus::Pending,
        message: String::new(),
        result_json: String::new(),
        updated_at: now,
    };
    // Delivered as it is applied, by whichever manager its node is connected to (`deliver_applied`).
    m.store.propose(Entry::CommandCreated { command: c.clone() }).await?;
    Ok((StatusCode::ACCEPTED, Json(c)))
}

#[derive(Deserialize)]
struct CommandsQuery {
    node: Option<String>,
}

async fn commands(State(m): State<Arc<Manager>>, h: HeaderMap, Query(q): Query<CommandsQuery>) -> R<Json<Vec<CommandRecord>>> {
    authed(&m, &h)?;
    let mut v: Vec<CommandRecord> = m.store.read(|s| s.commands.values().filter(|c| q.node.as_ref().is_none_or(|n| &c.node_id == n)).cloned().collect());
    v.sort_by_key(|c| std::cmp::Reverse(c.created_at));
    v.truncate(200);
    Ok(Json(v))
}

async fn command(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<Json<CommandRecord>> {
    authed(&m, &h)?;
    m.store.read(|s| s.commands.get(&id).cloned()).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such command".into()))
}

#[derive(Deserialize)]
struct EventsQuery {
    after: Option<u64>,
    node: Option<String>,
}

async fn events(State(m): State<Arc<Manager>>, h: HeaderMap, Query(q): Query<EventsQuery>) -> R<Json<Vec<crate::events::Event>>> {
    authed(&m, &h)?;
    let v = match q.after {
        Some(a) => m.events.after(a, 500),
        None => m.events.last(100),
    };
    Ok(Json(v.into_iter().filter(|e| q.node.as_ref().is_none_or(|n| e.node_id.as_ref() == Some(n))).collect()))
}

/// The latest command per workload, for its status.
fn last_commands(s: &crate::state::ClusterState) -> std::collections::HashMap<&str, &CommandRecord> {
    let mut last: std::collections::HashMap<&str, &CommandRecord> = std::collections::HashMap::new();
    for c in s.commands.values() {
        if let Some(w) = c.op.workload_id()
            && last.get(w).is_none_or(|l| l.created_at <= c.created_at)
        {
            last.insert(w, c);
        }
    }
    last
}

/// One word for where a workload is: running or stopped as wanted; pending while a command is open or one is due;
/// failed when its last command failed; unplaced with no node; unknown when its node has not reported.
fn workload_status(run: bool, placed: bool, actual: Option<&freightdock_proto::v1::WorkloadState>, generation: u64, reported: bool, last: Option<&CommandRecord>) -> &'static str {
    if !placed {
        return "unplaced";
    }
    let up = actual.is_some_and(|a| a.state == "running");
    if run && up && actual.is_some_and(|a| a.generation == generation) {
        return "running";
    }
    if !run && !up {
        return "stopped";
    }
    match last {
        Some(c) if !c.status.is_terminal() => "pending",
        Some(c) if matches!(c.status, CommandStatus::Failed | CommandStatus::Rejected | CommandStatus::Expired) => "failed",
        _ if !reported => "unknown",
        _ => "pending",
    }
}

fn stack_json(m: &Manager, s: &crate::state::ClusterState, st: &crate::state::Stack, with_compose: bool) -> Value {
    let last = last_commands(s);
    let mut reports: std::collections::HashMap<&str, Option<Vec<freightdock_proto::v1::WorkloadState>>> = std::collections::HashMap::new();
    let workloads: Vec<Value> = s
        .workloads
        .values()
        .filter(|w| w.stack == st.name)
        .map(|w| {
            let run = !st.stopped;
            let report = w.node_id.as_deref().map(|n| reports.entry(n).or_insert_with(|| m.sessions.reported(n)).clone()).unwrap_or(None);
            let actual = report.as_ref().and_then(|r| r.iter().find(|a| a.workload_id == w.id).cloned());
            let lc = last.get(w.id.as_str()).copied();
            json!({
                "id": w.id,
                "service": w.service,
                "slot": w.slot,
                "node_id": w.node_id,
                "hostname": w.node_id.as_ref().and_then(|n| s.nodes.get(n)).map(|n| n.hostname.clone()),
                "generation": w.generation,
                "desired": if run { "running" } else { "stopped" },
                "status": workload_status(run, w.node_id.is_some(), actual.as_ref(), w.generation, report.is_some(), lc),
                "actual": actual.map(|a| json!({ "state": a.state, "generation": a.generation, "exit_code": a.exit_code, "container_id": a.container_id, "image": a.image })),
                "last_command": lc.map(|c| json!({ "id": c.id, "op": c.op, "status": c.status, "message": c.message, "at": c.updated_at })),
            })
        })
        .collect();
    let services: serde_json::Map<String, Value> = st.services.iter().map(|(k, v)| (k.clone(), json!({ "image": v.image, "replicas": v.replicas, "ports": v.ports, "ha": v.ha, "limits": v.limits, "placement": { "constraints": v.placement.constraints.iter().map(ToString::to_string).collect::<Vec<_>>(), "prefer": v.placement.prefer.iter().map(ToString::to_string).collect::<Vec<_>>(), "spread": v.placement.spread, "reserve": v.placement.reserve } }))).collect();
    let mut v = json!({ "name": st.name, "stopped": st.stopped, "created_at": st.created_at, "updated_at": st.updated_at, "services": services, "workloads": workloads });
    if with_compose {
        v["compose"] = json!(st.compose);
    }
    v
}

async fn stacks(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| json!(s.stacks.values().map(|st| stack_json(&m, s, st, false)).collect::<Vec<_>>()))))
}

async fn stack(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<Json<Value>> {
    authed(&m, &h)?;
    m.store.read(|s| s.stacks.get(&name).map(|st| stack_json(&m, s, st, true))).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such stack".into()))
}

#[derive(Deserialize)]
struct DeployBody {
    name: String,
    compose: String,
}

/// Makes or changes a stack from a Compose file: its new workloads are placed on connected nodes with Docker, and the
/// reconcile on each node's manager does the rest.
async fn deploy_stack(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<DeployBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let name = b.name.trim().to_string();
    if !crate::state::valid_stack_name(&name) {
        return Err(ApiError(StatusCode::BAD_REQUEST, Rejected::BadName.to_string()));
    }
    let parsed = crate::compose::parse(&b.compose).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let placements = m.store.read(|s| {
        let eligible: Vec<String> = s.nodes.values().filter(|n| n.report.as_ref().is_some_and(|r| r.docker) && m.sessions.liveness(&n.id).health == crate::sessions::Health::Healthy).map(|n| n.id.clone()).collect();
        crate::stacks::place(s, &name, &parsed.services, &eligible)
    });
    let placements = placements.map_err(|e| ApiError(StatusCode::CONFLICT, e))?;
    let existed = m.store.read(|s| s.stacks.contains_key(&name));
    m.store.propose(Entry::StackDeployed { name: name.clone(), compose: b.compose, services: parsed.services, placements, at: now_ms() }).await?;
    m.events.push("stack_deployed", None, format!("stack {name} {}", if existed { "updated" } else { "deployed" }));
    let v = m.store.read(|s| s.stacks.get(&name).map(|st| stack_json(&m, s, st, false))).unwrap_or(Value::Null);
    Ok((if existed { StatusCode::OK } else { StatusCode::CREATED }, Json(json!({ "stack": v, "warnings": parsed.warnings }))))
}

async fn remove_stack(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::StackRemoved { name: name.clone(), at: now_ms() }).await?;
    m.events.push("stack_removed", None, format!("stack {name} removed"));
    Ok(StatusCode::NO_CONTENT)
}

/// start, stop, or restart (each running workload is stopped, and the reconcile starts it again).
async fn stack_action(State(m): State<Arc<Manager>>, h: HeaderMap, Path((name, action)): Path<(String, String)>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let stopped = match action.as_str() {
        "start" => false,
        "stop" => true,
        "restart" => {
            let todo: Vec<(String, String)> = m.store.read(|s| match s.stacks.get(&name) {
                None => Err(ApiError(StatusCode::NOT_FOUND, Rejected::StackUnknown.to_string())),
                Some(st) if st.stopped => Err(ApiError(StatusCode::CONFLICT, "the stack is stopped; start it".into())),
                Some(_) => Ok(s.workloads.values().filter(|w| w.stack == name).filter_map(|w| Some((w.node_id.clone()?, w.id.clone()))).collect()),
            })?;
            let (mut sent, mut busy) = (0, 0);
            for (node_id, workload_id) in todo {
                let now = now_ms();
                let c = CommandRecord { id: uuid::Uuid::new_v4().to_string(), node_id, op: Op::StopContainer { workload_id, timeout_s: 0 }, config_version: m.store.read(|s| s.applied), created_at: now, deadline: now + 60_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now };
                match m.store.propose(Entry::CommandCreated { command: c }).await {
                    Ok(_) => sent += 1,
                    Err(ProposeError::Rejected(Rejected::WorkloadBusy)) => busy += 1,
                    Err(e) => return Err(e.into()),
                }
            }
            m.events.push("stack_restarted", None, format!("stack {name} restarting"));
            return Ok(Json(json!({ "restarting": sent, "busy": busy })));
        }
        _ => return Err(ApiError(StatusCode::NOT_FOUND, "the actions are start, stop and restart".into())),
    };
    m.store.propose(Entry::StackStopped { name: name.clone(), stopped, at: now_ms() }).await?;
    m.events.push(if stopped { "stack_stopped" } else { "stack_started" }, None, format!("stack {name} {}", if stopped { "stopped" } else { "started" }));
    Ok(Json(json!({ "stopped": stopped })))
}

/// The Raft members for the API. Ids are 63-bit numbers, beyond what a JavaScript number holds exactly, so they go out
/// as strings.
fn raft_members(members: Vec<(u64, String)>, voters: &[u64], leader: Option<u64>, me: u64) -> Vec<Value> {
    members.into_iter().map(|(id, addr)| json!({ "raft_id": id.to_string(), "addr": addr, "voter": voters.contains(&id), "leader": leader == Some(id), "me": id == me })).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(status: CommandStatus) -> CommandRecord {
        CommandRecord { status, ..crate::state::tests::command("c", "n") }
    }

    #[test]
    fn a_workload_s_status_says_where_it_is() {
        let up = freightdock_proto::v1::WorkloadState { state: "running".into(), generation: 4, ..Default::default() };
        assert_eq!(workload_status(true, true, Some(&up), 4, true, None), "running");
        assert_eq!(workload_status(true, true, Some(&up), 5, true, Some(&cmd(CommandStatus::Accepted))), "pending", "an old spec being replaced");
        assert_eq!(workload_status(true, true, None, 4, true, Some(&cmd(CommandStatus::Failed))), "failed");
        assert_eq!(workload_status(true, true, None, 4, false, None), "unknown");
        assert_eq!(workload_status(false, true, None, 4, true, None), "stopped");
        assert_eq!(workload_status(false, true, Some(&up), 4, true, None), "pending", "still to stop");
        assert_eq!(workload_status(true, false, None, 4, true, None), "unplaced");
    }

    #[test]
    fn a_label_key_is_what_a_constraint_can_name() {
        assert!(valid_label_key("zone") && valid_label_key("example.com/rack-1"));
        assert!(!valid_label_key("") && !valid_label_key("a b") && !valid_label_key("a=b"));
    }

    #[test]
    fn raft_ids_go_out_as_strings_since_javascript_rounds_big_numbers() {
        let v = raft_members(vec![(5271529257891035005, "a:7450".into()), (7, "b:7450".into())], &[5271529257891035005], Some(5271529257891035005), 7);
        assert_eq!(v[0]["raft_id"], "5271529257891035005");
        assert_eq!((v[0]["leader"].clone(), v[0]["voter"].clone(), v[0]["me"].clone()), (json!(true), json!(true), json!(false)));
        assert_eq!((v[1]["voter"].clone(), v[1]["me"].clone()), (json!(false), json!(true)));
    }
}
