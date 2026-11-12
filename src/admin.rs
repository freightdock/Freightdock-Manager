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

use crate::state::{ApiRole, CommandRecord, CommandStatus, Entry, Op, Rejected};
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

/// What the bearer token `given` may do: the admin token (on the manager's host) everything, an API token what its role
/// says (G8.2), anything else nothing.
pub fn role_of(admin_token: &str, s: &crate::state::ClusterState, given: &str) -> Option<ApiRole> {
    let admin = !admin_token.is_empty() && given.len() == admin_token.len() && given.bytes().zip(admin_token.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;
    if admin {
        return Some(ApiRole::Admin);
    }
    given.starts_with(API_TOKEN_PREFIX).then(|| s.api_token(&freightdock_proto::sha256_hex(given)).map(|t| t.role)).flatten()
}

/// Whether `role` may make a request with `method`: a read token only reads.
pub fn allowed(role: ApiRole, method: &axum::http::Method) -> bool {
    role == ApiRole::Admin || matches!(*method, axum::http::Method::GET | axum::http::Method::HEAD)
}

pub const API_TOKEN_PREFIX: &str = "FDAT1-";

fn bearer(h: &HeaderMap) -> &str {
    h.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("")
}

fn authed(m: &Manager, h: &HeaderMap) -> R<ApiRole> {
    m.store.read(|s| role_of(&m.admin_token, s, bearer(h))).ok_or_else(|| ApiError(StatusCode::UNAUTHORIZED, "a valid bearer token is needed".into()))
}

/// Every route but health and the CA's certificate needs a token, and a read token only reads.
async fn guard(State(m): State<Arc<Manager>>, req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    use axum::response::IntoResponse;
    if matches!(req.uri().path(), "/v1/health" | "/v1/ca") {
        return next.run(req).await;
    }
    match authed(&m, req.headers()) {
        Ok(role) if allowed(role, req.method()) => next.run(req).await,
        Ok(_) => ApiError(StatusCode::FORBIDDEN, "this token only reads".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

pub fn router(m: Arc<Manager>) -> Router {
    Router::new()
        .route("/v1/health", get(|| async { Json(json!({ "ok": true })) }))
        .route("/v1/ca", get(ca_pem))
        .route("/v1/ui-db", get(ui_db))
        .route("/v1/ui-db/changes", post(ui_db_change))
        .route("/v1/ui-db/snapshot", axum::routing::put(ui_db_snapshot))
        .route("/v1/api-tokens", get(api_tokens).post(create_api_token))
        .route("/v1/api-tokens/{id}", axum::routing::delete(revoke_api_token))
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
        .route("/v1/recoveries", get(recoveries))
        .route("/v1/commands/{id}", get(command))
        .route("/v1/ui", get(ui_status).put(ui_deploy).delete(ui_remove))
        .route("/v1/stacks", get(stacks).post(deploy_stack))
        .route("/v1/stacks/{name}", get(stack).delete(remove_stack))
        .route("/v1/stacks/{name}/{action}", post(stack_action))
        .layer(axum::middleware::from_fn_with_state(m.clone(), guard))
        // A snapshot of the UI's database is the whole of it.
        .layer(axum::extract::DefaultBodyLimit::max(UI_DB_MAX))
        .with_state(m)
}

async fn cluster(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let managers = m.managers();
    let voters = m.store.voters();
    let leader = m.store.leader();
    let mut raft = raft_members(m.store.members(), &voters, leader, m.store.id);
    // Online: this one, and each other whose reports keep coming. Quorate: a majority of the voters online.
    for r in &mut raft {
        r["online"] = json!(r["me"] == true || r["addr"].as_str().is_some_and(|a| m.sessions.manager_online(a)));
    }
    let online_voters = raft.iter().filter(|r| r["voter"] == true && r["online"] == true).count();
    let quorate = !voters.is_empty() && online_voters * 2 > voters.len() && leader.is_some();
    Ok(Json(m.store.read(|s| {
        json!({
            "cluster_id": s.cluster_id,
            "manager_id": m.manager_id,
            "managers": managers,
            "raft": raft,
            "quorate": quorate,
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
        // Its overlay as it last said (G9); the key alone is in its report.
        "network": m.sessions.overlay(&n.id).map(|r| json!({ "public_key": r.public_key, "listen_port": r.listen_port, "applied_version": r.applied_version, "error": r.error })),
    })
}

async fn nodes(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let list: Vec<Value> = m.store.read(|s| s.nodes.values().map(|n| node_json(&m, n)).collect());
    Ok(Json(json!(list)))
}

async fn node(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let mut v = m.store.read(|s| s.nodes.get(&id).map(|n| node_json(&m, n))).ok_or(ApiError(StatusCode::NOT_FOUND, "no such node".into()))?;
    // What runs there as the node last said (any manager's word), for a page of the node.
    let containers: Option<Vec<Value>> = m.sessions.reported(&id).map(|l| l.iter().map(|w| json!({ "workload_id": w.workload_id, "container_id": w.container_id, "state": w.state, "image": w.image, "generation": w.generation, "exit_code": w.exit_code })).collect());
    v["containers"] = json!(containers);
    Ok(Json(v))
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

/// The largest UI database snapshot (or change) the managers take, as sent (base64 in JSON).
pub const UI_DB_MAX: usize = 48 << 20;

fn admin_only(role: ApiRole) -> R<()> {
    if role == ApiRole::Admin { Ok(()) } else { Err(ApiError(StatusCode::FORBIDDEN, "this needs an admin token".into())) }
}

#[derive(Deserialize)]
struct UiDbQuery {
    after: Option<u64>,
}

/// The UI's database (G8.1): from `after` on, the changes after it when the managers still hold them all; else the
/// snapshot and every change after it. Admin only: it holds the UI's users and secrets.
async fn ui_db(State(m): State<Arc<Manager>>, h: HeaderMap, Query(q): Query<UiDbQuery>) -> R<Json<Value>> {
    admin_only(authed(&m, &h)?)?;
    Ok(Json(m.store.read(|s| {
        let d = &s.ui_db;
        match q.after {
            Some(a) if a >= d.snapshot_head && a <= d.head && d.snapshot.is_some() => json!({ "head": d.head, "changes": d.changes.iter().filter(|c| c.seq > a).collect::<Vec<_>>() }),
            _ => json!({ "head": d.head, "snapshot_head": d.snapshot_head, "snapshot": d.snapshot, "changes": d.changes }),
        }
    })))
}

#[derive(Deserialize)]
struct UiDbChangeBody {
    seq: u64,
    data: String,
}

async fn ui_db_change(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<UiDbChangeBody>) -> R<StatusCode> {
    admin_only(authed(&m, &h)?)?;
    m.store.propose(Entry::UiDbChange { seq: b.seq, data: b.data.into(), at: now_ms() }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct UiDbSnapshotBody {
    head: u64,
    data: String,
}

async fn ui_db_snapshot(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<UiDbSnapshotBody>) -> R<StatusCode> {
    admin_only(authed(&m, &h)?)?;
    m.store.propose(Entry::UiDbSnapshot { head: b.head, data: b.data.into(), at: now_ms() }).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// The cluster CA's certificate (public), for a client of the API over TLS to trust. Check its fingerprint against
/// `freightdock-manager status` before trusting it.
async fn ca_pem(State(m): State<Arc<Manager>>) -> Response {
    ([(axum::http::header::CONTENT_TYPE, "application/x-pem-file")], m.ca.cert_pem.clone()).into_response()
}

#[derive(Deserialize)]
struct ApiTokenBody {
    name: String,
    role: Option<ApiRole>,
}

fn api_token_json(t: &crate::state::ApiToken) -> Value {
    json!({ "id": t.id, "name": t.name, "role": t.role, "created_at": t.created_at })
}

/// A new API token (G8.2): shown once, kept only as its hash. Read-only unless `role` says admin.
async fn create_api_token(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<ApiTokenBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let token = format!("{API_TOKEN_PREFIX}{}", hex::encode(rand::random::<[u8; 32]>()));
    let id = uuid::Uuid::new_v4().to_string();
    let role = b.role.unwrap_or(ApiRole::Read);
    let name = b.name.trim().to_string();
    m.store.propose(Entry::ApiTokenIssued { id: id.clone(), name: name.clone(), role, secret_hash: freightdock_proto::sha256_hex(&token), at: now_ms() }).await?;
    m.events.push("api_token", None, format!("API token {name} ({role:?}) made").to_lowercase());
    Ok((StatusCode::CREATED, Json(json!({ "id": id, "name": name, "role": role, "token": token, "ca_sha256": m.ca.fingerprint() }))))
}

async fn api_tokens(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| json!(s.api_tokens.values().map(api_token_json).collect::<Vec<_>>()))))
}

async fn revoke_api_token(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::ApiTokenRevoked { id: id.clone(), at: now_ms() }).await?;
    m.events.push("api_token", None, format!("API token {} revoked", &id[..id.len().min(8)]));
    Ok(StatusCode::NO_CONTENT)
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

/// The recovery history (G7), newest first; `?stack=` for one stack's.
async fn recoveries(State(m): State<Arc<Manager>>, h: HeaderMap, Query(q): Query<std::collections::HashMap<String, String>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let prefix = q.get("stack").map(|s| format!("{s}."));
    Ok(Json(m.store.read(|s| {
        let host = |n: &Option<String>| n.as_ref().and_then(|n| s.nodes.get(n)).map(|n| n.hostname.clone());
        json!(s
            .recoveries
            .iter()
            .rev()
            .filter(|r| prefix.as_ref().is_none_or(|p| r.workload_id.starts_with(p.as_str())))
            .map(|r| json!({ "at": r.at, "workload_id": r.workload_id, "from": r.from, "from_hostname": host(&r.from), "to": r.to, "to_hostname": host(&r.to), "reason": r.reason }))
            .collect::<Vec<_>>())
    })))
}

/// The latest command per workload on the node it is placed on now, for its status (one that cleaned up after it
/// on a node it moved off is not its status).
fn last_commands(s: &crate::state::ClusterState) -> std::collections::HashMap<&str, &CommandRecord> {
    let mut last: std::collections::HashMap<&str, &CommandRecord> = std::collections::HashMap::new();
    for c in s.commands.values() {
        if let Some(w) = c.op.workload_id()
            && s.workloads.get(w).is_some_and(|wl| wl.node_id.as_deref() == Some(c.node_id.as_str()))
            && last.get(w).is_none_or(|l| l.created_at <= c.created_at)
        {
            last.insert(w, c);
        }
    }
    last
}

/// One word for where a workload is: running or stopped as wanted; pending while a command is open or one is due;
/// failed when its last command failed; unplaced with no node; unknown when its node has not reported; fenced (set
/// by the caller) while its node is in fencing.
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
            let fenced = w.node_id.as_ref().and_then(|n| s.nodes.get(n)).is_some_and(|n| n.fencing.state != crate::state::FenceState::None);
            json!({
                "id": w.id,
                "service": w.service,
                "slot": w.slot,
                "node_id": w.node_id,
                "hostname": w.node_id.as_ref().and_then(|n| s.nodes.get(n)).map(|n| n.hostname.clone()),
                "generation": w.generation,
                "address": s.address(&w.id),
                "desired": if run { "running" } else { "stopped" },
                "status": if fenced { "fenced" } else { workload_status(run, w.node_id.is_some(), actual.as_ref(), w.generation, report.is_some(), lc) },
                "recovery": s.recovery_kind(w),
                "actual": actual.map(|a| json!({ "state": a.state, "generation": a.generation, "exit_code": a.exit_code, "container_id": a.container_id, "image": a.image })),
                "last_command": lc.map(|c| json!({ "id": c.id, "op": c.op, "status": c.status, "message": c.message, "at": c.updated_at })),
            })
        })
        .collect();
    let services: serde_json::Map<String, Value> = st.services.iter().map(|(k, v)| (k.clone(), json!({ "image": v.image, "replicas": v.replicas, "ports": v.ports, "ha": v.ha, "limits": v.limits, "placement": { "constraints": v.placement.constraints.iter().map(ToString::to_string).collect::<Vec<_>>(), "prefer": v.placement.prefer.iter().map(ToString::to_string).collect::<Vec<_>>(), "spread": v.placement.spread, "reserve": v.placement.reserve } }))).collect();
    let mut v = json!({ "name": st.name, "subnet": s.subnet(&st.name), "stopped": st.stopped, "created_at": st.created_at, "updated_at": st.updated_at, "services": services, "workloads": workloads });
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

/// The UI as the managers run it (G8.5): its image, where it is and how it is, or `{"enabled": false}`.
async fn ui_status(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| match s.stacks.get(crate::ui::STACK) {
        None => json!({ "enabled": false, "port": crate::ui::UI_PORT }),
        Some(st) => json!({ "enabled": true, "port": crate::ui::UI_PORT, "image": st.services.get(crate::ui::SERVICE).map(|sp| sp.image.clone()), "stack": stack_json(&m, s, st, false) }),
    })))
}

#[derive(Deserialize)]
struct UiBody {
    image: String,
}

/// Runs the UI from `image`, or moves it to another image: its API token first (derived, so the same on every
/// manager; only its hash in the state), then the managers' own stack.
async fn ui_deploy(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<UiBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let image = b.image.trim().to_string();
    if image.is_empty() || image.len() > 256 || image.chars().any(char::is_whitespace) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "an image, as docker pull takes it".into()));
    }
    let hash = freightdock_proto::sha256_hex(crate::ui::token(&m.ca.key_pem()));
    if m.store.read(|s| s.api_token(&hash).is_none()) {
        m.store.propose(Entry::ApiTokenIssued { id: crate::ui::TOKEN_ID.into(), name: crate::ui::TOKEN_NAME.into(), role: ApiRole::Admin, secret_hash: hash, at: now_ms() }).await?;
    }
    let services: std::collections::BTreeMap<String, crate::state::ServiceSpec> = [(crate::ui::SERVICE.to_string(), crate::ui::spec(&image))].into();
    let compose = format!("# The managers' own stack: the Freightdock UI (PUT /v1/ui).\nservices:\n  {}:\n    image: {image}\n", crate::ui::SERVICE);
    let placements = m.store.read(|s| crate::stacks::place(s, crate::ui::STACK, &services, &m.eligible(s))).map_err(|e| ApiError(StatusCode::CONFLICT, e))?;
    let existed = m.store.read(|s| s.stacks.contains_key(crate::ui::STACK));
    m.store.propose(Entry::StackDeployed { name: crate::ui::STACK.into(), compose, services, placements, at: now_ms() }).await?;
    m.events.push("ui", None, format!("the UI {} ({image})", if existed { "updated" } else { "deployed" }));
    let (code, Json(v)) = (if existed { StatusCode::OK } else { StatusCode::CREATED }, ui_status(State(m), h).await?);
    Ok((code, Json(v)))
}

/// Stops running the UI: its stack and its token go.
async fn ui_remove(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<StatusCode> {
    authed(&m, &h)?;
    if m.store.read(|s| s.stacks.contains_key(crate::ui::STACK)) {
        m.store.propose(Entry::StackRemoved { name: crate::ui::STACK.into(), at: now_ms() }).await?;
    }
    if m.store.read(|s| s.api_tokens.contains_key(crate::ui::TOKEN_ID)) {
        m.store.propose(Entry::ApiTokenRevoked { id: crate::ui::TOKEN_ID.into(), at: now_ms() }).await?;
    }
    m.events.push("ui", None, "the UI removed".to_string());
    Ok(StatusCode::NO_CONTENT)
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
    if name == crate::ui::STACK {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{} is the managers' own stack, for the UI: PUT /v1/ui", crate::ui::STACK)));
    }
    let parsed = crate::compose::parse(&b.compose).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    if let Some(why) = m.store.read(|s| crate::stacks::port_conflict(s, &name, &parsed.services)) {
        return Err(ApiError(StatusCode::CONFLICT, why));
    }
    let placements = m.store.read(|s| {
        let eligible = m.eligible(s);
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
    if name == crate::ui::STACK {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{} is the managers' own stack, for the UI: DELETE /v1/ui", crate::ui::STACK)));
    }
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

    #[test]
    fn the_admin_token_does_everything_an_api_token_its_role_and_a_read_token_only_reads() {
        let mut s = crate::state::tests::cluster();
        let token = format!("{API_TOKEN_PREFIX}{}", "ab".repeat(32));
        s.apply_at(9, &Entry::ApiTokenIssued { id: "t".into(), name: "ui".into(), role: ApiRole::Read, secret_hash: freightdock_proto::sha256_hex(&token), at: 1 }).unwrap();
        assert_eq!(role_of("adm1n", &s, "adm1n"), Some(ApiRole::Admin));
        assert_eq!(role_of("adm1n", &s, &token), Some(ApiRole::Read));
        assert_eq!(role_of("adm1n", &s, "adm1N"), None);
        assert_eq!(role_of("", &s, ""), None, "no admin token: nothing matches it");
        assert_eq!(role_of("adm1n", &s, &format!("{API_TOKEN_PREFIX}{}", "cd".repeat(32))), None);
        use axum::http::Method;
        assert!(allowed(ApiRole::Read, &Method::GET));
        assert!(!allowed(ApiRole::Read, &Method::POST) && !allowed(ApiRole::Read, &Method::DELETE));
        assert!(allowed(ApiRole::Admin, &Method::DELETE));
    }

    #[test]
    fn a_workload_s_last_command_is_the_latest_on_the_node_it_is_on_now() {
        let mut s = crate::state::tests::cluster();
        s.workloads.insert("a.web.1".into(), crate::state::Workload { id: "a.web.1".into(), stack: "a".into(), service: "web".into(), slot: 1, node_id: Some("new".into()), spec: crate::state::tests::spec("nginx", 1), generation: 1, host: None });
        let on = |id: &str, node: &str, at| CommandRecord { op: crate::state::Op::RemoveContainer { workload_id: "a.web.1".into() }, created_at: at, ..crate::state::tests::command(id, node) };
        s.commands.insert("run".into(), on("run", "new", 5));
        s.commands.insert("cleanup".into(), on("cleanup", "old", 9));
        assert_eq!(last_commands(&s)["a.web.1"].id, "run", "the clean-up on the node it left is not its status");
    }

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
