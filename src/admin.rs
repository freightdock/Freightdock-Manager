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
            ProposeError::Rejected(r @ (Rejected::NodeUnknown | Rejected::CommandUnknown | Rejected::StackUnknown | Rejected::FloatingUnknown | Rejected::StorageUnknown | Rejected::RegistryUnknown | Rejected::ChannelUnknown)) => ApiError(StatusCode::NOT_FOUND, r.to_string()),
            ProposeError::Rejected(r @ (Rejected::BadAddress | Rejected::BadExposure | Rejected::BadStorage | Rejected::VolumeShared | Rejected::BadSettings | Rejected::BadRegistry | Rejected::BadChannel)) => ApiError(StatusCode::BAD_REQUEST, r.to_string()),
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
        .route("/v1/floating", get(floating).post(add_floating))
        .route("/v1/traffic", get(traffic))
        .route("/v1/floating/{address}", axum::routing::delete(remove_floating))
        .route("/v1/storage", get(storage).post(add_storage))
        .route("/v1/storage/{name}", axum::routing::delete(remove_storage))
        .route("/v1/settings", get(settings).put(set_settings))
        .route("/v1/settings/proxmox/fingerprint", post(proxmox_fingerprint))
        .route("/v1/settings/proxmox/test", post(proxmox_test))
        .route("/v1/backup", post(take_backup))
        .route("/v1/registries", get(registries))
        .route("/v1/registries/{host}", axum::routing::put(set_registry).delete(remove_registry))
        .route("/v1/registries/{host}/test", post(test_registry))
        .route("/v1/notifications", get(notifications))
        .route("/v1/notifications/{name}", axum::routing::put(set_channel).delete(remove_channel))
        .route("/v1/notifications/{name}/test", post(test_channel))
        .route("/v1/backups", get(backups))
        .route("/v1/backups/{name}", get(backup_file))
        .route("/v1/cluster", get(cluster))
        .route("/v1/nodes", get(nodes))
        .route("/v1/nodes/{id}", get(node).delete(remove_node))
        .route("/v1/nodes/{id}/commands", post(create_command))
        .route("/v1/nodes/{id}/labels", axum::routing::put(set_labels))
        .route("/v1/nodes/{id}/authorize", post(authorize))
        .route("/v1/nodes/{id}/docker", get(docker_read).post(docker_change))
        .route("/v1/join-tokens", post(join_token))
        .route("/v1/manager-tokens", post(manager_token))
        .route("/v1/commands", get(commands))
        .route("/v1/events", get(events))
        .route("/v1/recoveries", get(recoveries))
        .route("/v1/commands/{id}", get(command))
        .route("/v1/ui", get(ui_status).put(ui_deploy).delete(ui_remove))
        .route("/v1/stacks", get(stacks).post(deploy_stack))
        .route("/v1/stack-check", post(stack_check))
        .route("/v1/stacks/{name}", get(stack).delete(remove_stack))
        .route("/v1/stacks/{name}/{action}", post(stack_action))
        .route("/v1/stacks/{name}/exposures", axum::routing::put(set_exposures))
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

impl From<crate::docker::DockerError> for ApiError {
    fn from(e: crate::docker::DockerError) -> Self {
        use crate::docker::DockerError::*;
        let status = match &e {
            UnknownNode => StatusCode::NOT_FOUND,
            UnknownAction(_) | Refused(_) => StatusCode::BAD_REQUEST,
            NotConnected => StatusCode::SERVICE_UNAVAILABLE,
            Older => StatusCode::CONFLICT,
            NoAnswer => StatusCode::GATEWAY_TIMEOUT,
            Peer(_) => StatusCode::BAD_GATEWAY,
        };
        ApiError(status, e.to_string())
    }
}

#[derive(Deserialize)]
struct DockerAsk {
    action: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    params: std::collections::HashMap<String, String>,
}

/// A look at the node's Docker (`?action=containers`, `&target=` for one container's inspect or logs, other query
/// keys passed on as the request's params): through the worker, now.
async fn docker_read(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>, Query(mut q): Query<std::collections::HashMap<String, String>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let action = q.remove("action").unwrap_or_default();
    if !crate::docker::READS.contains(&action.as_str()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{action:?} is not a read of a node's Docker; a change is a POST")));
    }
    let target = q.remove("target").unwrap_or_default();
    Ok(Json(m.docker(&id, &action, &target, q).await?))
}

/// A change to the node's Docker: `{action, target, params}`. Never one of the cluster's own (the worker refuses).
async fn docker_change(State(m): State<Arc<Manager>>, h: HeaderMap, Path(id): Path<String>, Json(b): Json<DockerAsk>) -> R<Json<Value>> {
    authed(&m, &h)?;
    if !crate::docker::CHANGES.contains(&b.action.as_str()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{:?} is not a change to a node's Docker; a read is a GET", b.action)));
    }
    let on = if b.target.is_empty() { b.params.get("image").cloned().unwrap_or_default() } else { b.target.clone() };
    let what = if on.is_empty() { b.action.clone() } else { format!("{} {on}", b.action) };
    // Done in a task of its own, so a caller that goes before the answer does not stop it half-recorded: the worker
    // does it all the same, and it is an event all the same.
    let task = tokio::spawn(async move {
        let v = m.docker(&id, &b.action, &b.target, b.params).await?;
        m.events.push("docker", Some(&id), format!("Docker: {what}"));
        Ok::<_, crate::docker::DockerError>(v)
    });
    let v = task.await.map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))??;
    Ok(Json(v))
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

/// The cluster's traffic now (G9.8): every node's latest report, its ends named.
async fn traffic(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let nodes: Vec<String> = m.store.read(|s| s.nodes.keys().cloned().collect());
    let reports: Vec<_> = nodes.into_iter().filter_map(|n| m.sessions.traffic(&n).map(|r| (n, r))).collect();
    Ok(Json(m.store.read(|s| crate::traffic::view(s, &reports))))
}

#[derive(Deserialize)]
struct ExposuresBody {
    exposures: Vec<crate::state::Exposure>,
}

/// A stack's exposures on the pool (G10.5), all of them; answers the stack.
async fn set_exposures(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>, Json(b): Json<ExposuresBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let what = b.exposures.iter().map(|e| format!("{}:{} to {}:{}/{}", e.address, e.port, e.service, e.container_port, e.protocol)).collect::<Vec<_>>().join(", ");
    m.store.propose(Entry::StackExposed { name: name.clone(), exposures: b.exposures, at: now_ms() }).await?;
    m.events.push("stack_exposed", None, if what.is_empty() { format!("stack {name} exposes nothing on the pool") } else { format!("stack {name} exposes {what}") });
    m.store.read(|s| s.stacks.get(&name).map(|st| stack_json(&m, s, st, false))).map(Json).ok_or(ApiError(StatusCode::NOT_FOUND, "no such stack".into()))
}

#[derive(Deserialize)]
struct FloatingBody {
    address: String,
}

/// The floating addresses (G10.3), each with its holder (none while it waits for one).
async fn floating(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| {
        json!(s
            .floating
            .iter()
            .map(|(a, f)| {
                // What the address exposes: the pool is what leaves the cluster on purpose (G10.5).
                let on: Vec<Value> = s.stacks.values().flat_map(|st| st.exposures.iter().filter(|e| e.address == *a).map(|e| json!({ "stack": st.name, "service": e.service, "port": e.port, "protocol": e.protocol, "container_port": e.container_port }))).collect();
                json!({ "address": a, "holder": f.holder, "since": f.since, "exposures": on })
            })
            .collect::<Vec<_>>())
    })))
}

/// One address (409 if the pool has it), or a range of them (`crate::state::pool_range`), added whole; answers how
/// many were new.
async fn add_floating(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<FloatingBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let written = b.address.trim().to_string();
    if !written.contains(['-', '/']) {
        m.store.propose(Entry::FloatingAdded { address: written.clone(), at: now_ms() }).await?;
        m.events.push("floating", None, format!("floating address {written} added"));
        return Ok((StatusCode::CREATED, Json(json!({ "added": 1 }))));
    }
    let Some(addresses) = crate::state::pool_range(&written) else {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{written:?} is not an address, a range (192.0.2.10-20) or a prefix (192.0.2.8/29) of at most {} addresses", crate::state::POOL_RANGE_MAX)));
    };
    let added = m.store.read(|s| addresses.iter().filter(|a| !s.floating.contains_key(*a)).count());
    m.store.propose(Entry::FloatingPoolAdded { addresses, at: now_ms() }).await?;
    m.events.push("floating", None, format!("floating addresses {written} added to the pool"));
    Ok((StatusCode::CREATED, Json(json!({ "added": added }))))
}

async fn remove_floating(State(m): State<Arc<Manager>>, h: HeaderMap, Path(address): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::FloatingRemoved { address: address.clone(), at: now_ms() }).await?;
    m.events.push("floating", None, format!("floating address {address} removed"));
    Ok(StatusCode::NO_CONTENT)
}

/// The cluster's shared storage (G11): each with what its driver can do, the stacks keeping volumes on it, and the
/// volumes for one writer with the node each is leased to.
async fn storage(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| {
        Value::Array(
            s.storage
                .values()
                .map(|st| {
                    let volumes: Vec<Value> = s
                        .stacks
                        .values()
                        .flat_map(|stack| stack.volumes.iter().filter(|(_, v)| v.storage == st.name).map(move |(name, v)| (stack, name, v)))
                        .map(|(stack, name, v)| json!({ "stack": stack.name, "volume": name, "access": v.access, "holder": s.leases.get(&format!("{}/{name}", stack.name)).map(|l| &l.node_id) }))
                        .collect();
                    json!({ "name": st.name, "driver": st.driver, "source": st.source, "options": st.options, "added_at": st.added_at, "capabilities": st.driver.capabilities(), "volumes": volumes })
                })
                .collect(),
        )
    })))
}

#[derive(Deserialize)]
struct StorageBody {
    name: String,
    driver: crate::state::StorageDriver,
    source: String,
    #[serde(default)]
    options: String,
}

async fn add_storage(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<StorageBody>) -> R<StatusCode> {
    authed(&m, &h)?;
    let storage = crate::state::Storage { name: b.name.trim().to_string(), driver: b.driver, source: b.source.trim().to_string(), options: b.options.trim().to_string(), added_at: now_ms() };
    let name = storage.name.clone();
    m.store.propose(Entry::StorageAdded { storage }).await?;
    m.events.push("storage", None, format!("storage {name} added"));
    Ok(StatusCode::CREATED)
}

async fn remove_storage(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::StorageRemoved { name: name.clone(), at: now_ms() }).await?;
    m.events.push("storage", None, format!("storage {name} removed"));
    Ok(StatusCode::NO_CONTENT)
}

/// The private registries' logins, without their passwords.
async fn registries(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    Ok(Json(m.store.read(|s| json!(s.registries.values().map(|r| json!({ "host": r.host, "username": r.username, "at": r.at })).collect::<Vec<_>>()))))
}

#[derive(Deserialize)]
struct LoginBody {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: Option<String>,
}

/// Sets a registry's login (`{username, password}`); the password may be left out to keep the one set.
async fn set_registry(State(m): State<Arc<Manager>>, h: HeaderMap, Path(host): Path<String>, Json(b): Json<LoginBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let host = host.trim().to_ascii_lowercase();
    let password = match (b.password.filter(|p| !p.is_empty()), m.store.read(|s| s.registries.get(&host).map(|r| r.password.clone()))) {
        (Some(p), _) => crate::settings::Secret(p),
        (None, Some(old)) => old,
        (None, None) => return Err(ApiError(StatusCode::BAD_REQUEST, "a new registry login needs its password (or token)".into())),
    };
    let registry = crate::registry::Registry { host: host.clone(), username: b.username.trim().to_string(), password, at: now_ms() };
    m.store.propose(Entry::RegistrySet { registry }).await?;
    m.events.push("registry", None, format!("the login for {host} was set"));
    Ok(Json(json!({ "host": host, "username": b.username.trim() })))
}

async fn remove_registry(State(m): State<Arc<Manager>>, h: HeaderMap, Path(host): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::RegistryRemoved { host: host.clone(), at: now_ms() }).await?;
    m.events.push("registry", None, format!("the login for {host} was removed"));
    Ok(StatusCode::NO_CONTENT)
}

/// Signs in to the registry with the login given (the password kept when left out) or the one set. Changes nothing.
async fn test_registry(State(m): State<Arc<Manager>>, h: HeaderMap, Path(host): Path<String>, body: Option<Json<LoginBody>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    if !crate::registry::valid_host(&host) {
        return Err(ApiError(StatusCode::BAD_REQUEST, Rejected::BadRegistry.to_string()));
    }
    let kept = m.store.read(|s| s.registries.get(&host).cloned());
    let given = body.map(|b| b.0);
    let user = given.as_ref().map(|b| b.username.trim().to_string()).filter(|u| !u.is_empty()).or_else(|| kept.as_ref().map(|k| k.username.clone()));
    let pass = given.and_then(|b| b.password).filter(|p| !p.is_empty()).or_else(|| kept.map(|k| k.password.0));
    let (Some(user), Some(pass)) = (user, pass) else { return Err(ApiError(StatusCode::BAD_REQUEST, "a login to try needs a user and a password".into())) };
    let said = crate::registry::test(&host, &user, &pass).await.map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("{e:#}")))?;
    Ok(Json(json!({ "ok": true, "message": said })))
}

/// The notification channels, without their secrets, and the events and kinds there are.
async fn notifications(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let channels: Vec<Value> = m.store.read(|s| s.notifications.values().map(crate::notify::Channel::shown).collect());
    let events: Vec<Value> = crate::notify::EVENTS.iter().map(|(e, says)| json!({ "event": e, "says": says })).collect();
    Ok(Json(json!({ "channels": channels, "events": events, "kinds": crate::notify::KINDS })))
}

#[derive(Deserialize)]
struct ChannelBody {
    kind: String,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    config: crate::notify::Config,
    /// Left out: the one set is kept; empty: there is none.
    #[serde(default)]
    secret: Option<String>,
}

fn yes() -> bool {
    true
}

/// Sets a channel whole (`{kind, enabled, events, config, secret}`).
async fn set_channel(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>, Json(b): Json<ChannelBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let secret = match b.secret {
        Some(s) if s.is_empty() => None,
        Some(s) => Some(crate::settings::Secret(s)),
        None => m.store.read(|s| s.notifications.get(&name).and_then(|c| c.secret.clone())),
    };
    let config = b.config.checked(&b.kind, secret.is_some()).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    let mut events: Vec<String> = vec![];
    for e in b.events {
        if !events.contains(&e) {
            events.push(e);
        }
    }
    let channel = crate::notify::Channel { name: name.clone(), kind: b.kind, enabled: b.enabled, events, config, secret, at: now_ms(), last_sent_at: None, last_error: None };
    m.store.propose(Entry::ChannelSet { channel }).await?;
    m.events.push("notify", None, format!("notification channel {name} was set"));
    Ok(Json(m.store.read(|s| s.notifications.get(&name).map(crate::notify::Channel::shown)).unwrap_or(Value::Null)))
}

async fn remove_channel(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<StatusCode> {
    authed(&m, &h)?;
    m.store.propose(Entry::ChannelRemoved { name: name.clone(), at: now_ms() }).await?;
    m.events.push("notify", None, format!("notification channel {name} was removed"));
    Ok(StatusCode::NO_CONTENT)
}

/// Sends a test through the channel as it is set, and says how it went.
async fn test_channel(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let Some(ch) = m.store.read(|s| s.notifications.get(&name).cloned()) else { return Err(ApiError(StatusCode::NOT_FOUND, Rejected::ChannelUnknown.to_string())) };
    m.send_through(&ch, &crate::notify::Event::test()).await.map_err(|e| ApiError(StatusCode::BAD_GATEWAY, e))?;
    Ok(Json(json!({ "ok": true, "message": format!("sent through {name}") })))
}

/// The cluster's settings, each with where its value comes from, and what only this manager's environment sets.
async fn settings(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let cluster = m.store.read(|s| s.settings.clone());
    Ok(Json(json!({ "settings": crate::settings::view(&cluster, &m.own), "manager_id": m.manager_id, "manager": m.local })))
}

/// Changes some of the cluster's settings (`Settings::patched`): checked with the values they are used with.
async fn set_settings(State(m): State<Arc<Manager>>, h: HeaderMap, Json(patch): Json<Value>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let bad = |e: String| ApiError(StatusCode::BAD_REQUEST, e);
    let next = m.store.read(|s| s.settings.clone()).patched(&patch).map_err(bad)?;
    next.check().map_err(bad)?;
    next.over(&m.own).check().map_err(bad)?;
    m.store.propose(Entry::SettingsSet { settings: next.clone(), at: now_ms() }).await?;
    let keys: Vec<&str> = patch.as_object().into_iter().flatten().map(|(k, _)| k.as_str()).collect();
    m.events.push("settings", None, format!("settings changed: {}", keys.join(", ")));
    Ok(Json(json!({ "settings": crate::settings::view(&next, &m.own) })))
}

#[derive(Deserialize)]
struct BackupBody {
    passphrase: String,
}

fn backup_response(name: &str, bytes: Vec<u8>) -> Response {
    ([("content-type", "application/octet-stream".to_string()), ("content-disposition", format!("attachment; filename=\"{name}\""))], bytes).into_response()
}

/// A backup of the cluster now, sealed with the passphrase given, handed out and kept nowhere.
async fn take_backup(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<BackupBody>) -> R<Response> {
    authed(&m, &h)?;
    if b.passphrase.chars().count() < crate::backup::MIN_PASSPHRASE {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("a backup's passphrase is at least {} characters", crate::backup::MIN_PASSPHRASE)));
    }
    let bytes = m.backup(b.passphrase).await.map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    m.events.push("backup", None, "a backup was downloaded".to_string());
    Ok(backup_response(&crate::backup::file_name(now_ms()), bytes))
}

/// This manager's scheduled backups, newest first.
async fn backups(State(m): State<Arc<Manager>>, h: HeaderMap) -> R<Json<Value>> {
    authed(&m, &h)?;
    let list: Vec<Value> = crate::backup::list(&crate::backup::dir(&m.data_dir))
        .into_iter()
        .map(|(name, size)| json!({ "taken_at": crate::backup::taken_at(&name), "name": name, "size": size, "manager": m.manager_id }))
        .collect();
    Ok(Json(json!({ "backups": list })))
}

async fn backup_file(State(m): State<Arc<Manager>>, h: HeaderMap, Path(name): Path<String>) -> R<Response> {
    authed(&m, &h)?;
    let bytes = crate::backup::read(&crate::backup::dir(&m.data_dir), &name).ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no such backup on this manager".into()))?;
    Ok(backup_response(&name, bytes))
}

#[derive(Deserialize)]
struct UrlBody {
    url: String,
}

/// The certificate the Proxmox API at `url` shows, to compare with the one Proxmox shows and pin.
async fn proxmox_fingerprint(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<UrlBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let fp = crate::proxmox::fingerprint_of(&b.url).await.map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("{e:#}")))?;
    Ok(Json(json!({ "fingerprint": crate::settings::colons(&fp) })))
}

/// Tries the Proxmox provider: the settings given (`{url, fingerprint, token}`, the token kept when left out) or
/// the ones in use. Answers Proxmox's version and what it says of each node's guest; changes nothing.
async fn proxmox_test(State(m): State<Arc<Manager>>, h: HeaderMap, body: Option<Json<Value>>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let given = body.map(|b| b.0).filter(|b| b.get("url").is_some());
    let cfg = match given {
        Some(p) => m.settings().patched(&json!({ "proxmox": p })).map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?.proxmox,
        None => m.fence().proxmox,
    }
    .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "no Proxmox provider is set".into()))?;
    let url = cfg.url();
    let px = crate::proxmox::Proxmox::new(cfg).map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    let version = px.version().await.map_err(|e| ApiError(StatusCode::BAD_GATEWAY, format!("the Proxmox API at {url}: {e:#}")))?;
    let nodes: Vec<crate::state::Node> = m.store.read(|s| s.nodes.values().filter(|n| crate::proxmox::provider_of(n) == "proxmox").cloned().collect());
    let mut guests = vec![];
    for n in nodes {
        let name = n.display_name();
        let r = match crate::proxmox::guest_of(&n) {
            Ok(g) => match px.status(&g).await {
                Ok(st) => json!({ "guest": g.to_string(), "status": st }),
                Err(e) => json!({ "guest": g.to_string(), "error": format!("{e:#}") }),
            },
            Err(e) => json!({ "error": format!("{e:#}") }),
        };
        let mut r = r;
        r["node"] = json!(n.id);
        r["name"] = json!(name);
        guests.push(r);
    }
    Ok(Json(json!({ "version": version, "guests": guests })))
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

/// Whether a workload wanted running has exited at its spec: it is said so, whether its restart policy runs it again
/// (soon, or not once its attempts are spent) or leaves it.
fn left_exited(run: bool, actual: Option<&freightdock_proto::v1::WorkloadState>, generation: u64) -> bool {
    run && actual.is_some_and(|a| matches!(a.state.as_str(), "exited" | "dead") && a.generation == generation)
}

/// A stack's options as the API shows them; its env file only in the detail (it may hold secrets, like the file).
fn options_json(o: &crate::state::StackOptions, detail: bool) -> Value {
    let mut v = json!({ "description": o.description, "selector": o.selector.iter().map(ToString::to_string).collect::<Vec<_>>(), "mode": o.mode, "names": o.names });
    if detail {
        v["env"] = json!(o.env);
    }
    v
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
                "container_name": w.container_name(),
                // Moved and not run yet: the node whose copy may still run (G5.5).
                "waits_for": s.waits_for_old(w).then_some(w.leaving.as_ref()).flatten(),
                "service": w.service,
                "slot": w.slot,
                "node_id": w.node_id,
                "hostname": w.node_id.as_ref().and_then(|n| s.nodes.get(n)).map(|n| n.hostname.clone()),
                "generation": w.generation,
                "address": s.address(&w.id),
                "desired": if run { "running" } else { "stopped" },
                "status": if fenced { "fenced" } else if left_exited(run, actual.as_ref(), w.generation) { "exited" } else { workload_status(run, w.node_id.is_some(), actual.as_ref(), w.generation, report.is_some(), lc) },
                "recovery": s.recovery_kind(w),
                "actual": actual.map(|a| json!({ "state": a.state, "generation": a.generation, "exit_code": a.exit_code, "container_id": a.container_id, "image": a.image })),
                "last_command": lc.map(|c| json!({ "id": c.id, "op": c.op, "status": c.status, "message": c.message, "at": c.updated_at })),
            })
        })
        .collect();
    let services: serde_json::Map<String, Value> = st.services.iter().map(|(k, v)| (k.clone(), json!({ "image": v.image, "replicas": v.replicas, "ports": v.ports, "ha": v.ha, "name": v.name, "restart": v.restart, "limits": v.limits, "placement": { "constraints": v.placement.constraints.iter().map(ToString::to_string).collect::<Vec<_>>(), "prefer": v.placement.prefer.iter().map(ToString::to_string).collect::<Vec<_>>(), "spread": v.placement.spread, "reserve": v.placement.reserve } }))).collect();
    let mut v = json!({ "name": st.name, "subnet": s.subnet(&st.name), "stopped": st.stopped, "created_at": st.created_at, "updated_at": st.updated_at, "services": services, "workloads": workloads, "options": options_json(&st.options, false), "description": st.options.description, "exposures": st.exposures, "nodes": st.nodes, "volumes": st.volumes.iter().map(|(k, v)| (k.clone(), json!({ "storage": v.storage, "access": v.access, "holder": s.leases.get(&format!("{}/{k}", st.name)).map(|l| &l.node_id) }))).collect::<serde_json::Map<String, Value>>() });
    if with_compose {
        v["compose"] = json!(st.compose);
        v["options"] = options_json(&st.options, true);
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
        None => json!({ "enabled": false }),
        Some(st) => json!({ "enabled": true, "address": st.exposures.first().map(|e| e.address.clone()), "port": crate::ui::PORT, "image": st.services.get(crate::ui::SERVICE).map(|sp| sp.image.clone()), "stack": stack_json(&m, s, st, false) }),
    })))
}

#[derive(Deserialize)]
struct UiBody {
    image: String,
    /// The pool address to reach it on; by default the one it has, else the first free one.
    #[serde(default)]
    address: Option<String>,
}

/// Runs the UI from `image`, or moves it to another image, reached on a pool address (`ui::address_for`): its API
/// token first (derived, so the same on every manager; only its hash in the state), then the managers' own stack.
async fn ui_deploy(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<UiBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let image = b.image.trim().to_string();
    if image.is_empty() || image.len() > 256 || image.chars().any(char::is_whitespace) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "an image, as docker pull takes it".into()));
    }
    let address = m.store.read(|s| crate::ui::address_for(s, b.address.as_deref())).map_err(|e| ApiError(StatusCode::CONFLICT, e))?;
    let hash = freightdock_proto::sha256_hex(crate::ui::token(&m.ca.key_pem()));
    if m.store.read(|s| s.api_token(&hash).is_none()) {
        m.store.propose(Entry::ApiTokenIssued { id: crate::ui::TOKEN_ID.into(), name: crate::ui::TOKEN_NAME.into(), role: ApiRole::Admin, secret_hash: hash, at: now_ms() }).await?;
    }
    let services: std::collections::BTreeMap<String, crate::state::ServiceSpec> = [(crate::ui::SERVICE.to_string(), crate::ui::spec(&image))].into();
    let compose = format!("# The managers' own stack: the Freightdock UI (PUT /v1/ui).\nservices:\n  {}:\n    image: {image}\n", crate::ui::SERVICE);
    let placements = m.store.read(|s| crate::stacks::place(s, crate::ui::STACK, &services, &m.eligible(s), &[])).map_err(|e| ApiError(StatusCode::CONFLICT, e))?;
    let existed = m.store.read(|s| s.stacks.contains_key(crate::ui::STACK));
    let options = crate::state::StackOptions { description: "The Freightdock UI, run by the managers".into(), ..Default::default() };
    m.store.propose(Entry::StackDeployed { name: crate::ui::STACK.into(), compose, services, placements, at: now_ms(), nodes: vec![], exposures: Some(vec![crate::ui::exposure(&address)]), volumes: Default::default(), options }).await?;
    m.events.push("ui", None, format!("the UI {} ({image}) at https://{address}", if existed { "updated" } else { "deployed" }));
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
    /// The nodes it may run on, by id or hostname; empty: any. Left out: as it was (any for a new stack).
    #[serde(default)]
    nodes: Option<Vec<String>>,
    /// Its exposures on the pool, all of them. Left out: as they were.
    #[serde(default)]
    exposures: Option<Vec<crate::state::Exposure>>,
    /// The options beside the Compose file (`StackOptions`); each left out is as it was.
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    env: Option<String>,
    /// Node labels its workloads need: `key=value`, `key!=value`, or a constraint as Compose writes it.
    #[serde(default)]
    selector: Option<Vec<String>>,
    /// `{"kind": "stateless" | "pinned" | "shared", "storage": ...}`, or null for as the file says.
    #[serde(default, deserialize_with = "present")]
    mode: Option<Value>,
    /// Container names by service.
    #[serde(default)]
    names: Option<std::collections::BTreeMap<String, String>>,
}

/// A field that is there, null included: `Some(Value::Null)`, where a plain `Option` reads null as left out.
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    use serde::Deserialize;
    Value::deserialize(d).map(Some)
}

/// A selector's item: `zone=a` and `zone!=a` are node labels; anything else is a constraint (`node.hostname==w1`).
fn selector_item(s: &str) -> Result<crate::state::Constraint, String> {
    let s = s.trim();
    if s.starts_with("node.") {
        return crate::compose::constraint(s);
    }
    let (k, equal, v) = match s.split_once("!=") {
        Some((k, v)) => (k, false, v),
        None => s.split_once('=').map(|(k, v)| (k, true, v.trim_start_matches('='))).ok_or_else(|| format!("selector: {s:?} is not label=value or label!=value"))?,
    };
    let (k, v) = (k.trim(), v.trim());
    if k.is_empty() || v.is_empty() {
        return Err(format!("selector: {s:?} is not label=value or label!=value"));
    }
    Ok(crate::state::Constraint { field: format!("node.labels.{k}"), equal, value: v.to_string() })
}

/// The stack's options as this deploy makes them: each given one, else what the stack had.
fn options_of(b: &DeployBody, had: Option<&crate::state::StackOptions>) -> Result<crate::state::StackOptions, String> {
    let mut o = had.cloned().unwrap_or_default();
    if let Some(d) = &b.description {
        let d = d.trim();
        if d.chars().count() > 500 {
            return Err("a description is at most 500 characters".into());
        }
        o.description = d.to_string();
    }
    if let Some(e) = &b.env {
        if e.len() > 64 * 1024 {
            return Err("an env file is at most 64 KiB".into());
        }
        o.env = e.clone();
    }
    if let Some(list) = &b.selector {
        o.selector = list.iter().filter(|s| !s.trim().is_empty()).map(|s| selector_item(s)).collect::<Result<_, _>>()?;
    }
    if let Some(m) = &b.mode {
        o.mode = if m.is_null() { None } else { Some(serde_json::from_value(m.clone()).map_err(|_| "mode is {\"kind\": \"stateless\"}, {\"kind\": \"pinned\"}, {\"kind\": \"shared\", \"storage\": <name>} or null".to_string())?) };
    }
    if let Some(n) = &b.names {
        o.names = n.iter().map(|(k, v)| (k.clone(), v.trim().to_string())).collect();
    }
    Ok(o)
}

/// A deploy made ready: the Compose file parsed, the nodes named resolved to ids, and the new workloads placed (or
/// why they cannot be).
struct Ready {
    parsed: crate::compose::Parsed,
    options: crate::state::StackOptions,
    nodes: Vec<String>,
    placements: Result<std::collections::BTreeMap<String, String>, String>,
}

fn ready(m: &Manager, b: &DeployBody) -> R<(String, Ready)> {
    let name = b.name.trim().to_string();
    if !crate::state::valid_stack_name(&name) {
        return Err(ApiError(StatusCode::BAD_REQUEST, Rejected::BadName.to_string()));
    }
    if name == crate::ui::STACK {
        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{} is the managers' own stack, for the UI: PUT /v1/ui", crate::ui::STACK)));
    }
    let bad = |e: String| ApiError(StatusCode::BAD_REQUEST, e);
    let options = m.store.read(|s| options_of(b, s.stacks.get(&name).map(|st| &st.options))).map_err(bad)?;
    let vars = crate::compose::env_file(&options.env).map_err(bad)?;
    let mut parsed = crate::compose::parse_with(&b.compose, &vars).map_err(bad)?;
    crate::compose::apply_options(&mut parsed, &options).map_err(bad)?;
    m.store.read(|s| {
        for (vol, v) in &parsed.volumes {
            if !s.storage.contains_key(&v.storage) {
                return Err(ApiError(StatusCode::BAD_REQUEST, format!("volume {vol}: the cluster has no storage named {:?} (storage add)", v.storage)));
            }
        }
        let nodes = match &b.nodes {
            None => s.stacks.get(&name).map(|st| st.nodes.clone()).unwrap_or_default(),
            Some(list) => {
                let mut ids = vec![];
                for n in list.iter().map(|n| n.trim()).filter(|n| !n.is_empty()) {
                    let Some(id) = node_by(s, n) else {
                        return Err(ApiError(StatusCode::BAD_REQUEST, format!("{n:?} is no node's id, nor one node's name label or hostname")));
                    };
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
                ids
            }
        };
        let placements = crate::stacks::place(s, &name, &parsed.services, &m.eligible(s), &nodes);
        Ok((name.clone(), Ready { parsed, options, nodes, placements }))
    })
}

/// What a deploy would do, changing nothing: the services with the ports their Compose file asks for (to be exposed
/// on the pool, never on a node's own address), the warnings, the nodes resolved, and where the new workloads would go
/// or why they could not.
async fn stack_check(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<DeployBody>) -> R<Json<Value>> {
    authed(&m, &h)?;
    let (name, r) = ready(&m, &b)?;
    let services: serde_json::Map<String, Value> = r.parsed.services.iter().map(|(k, v)| (k.clone(), json!({ "image": v.image, "replicas": v.replicas, "ports": v.ports, "name": v.name, "ha": v.ha }))).collect();
    let (placements, unplaced) = match r.placements {
        Ok(p) => (json!(p), Value::Null),
        Err(why) => (Value::Null, json!(why)),
    };
    let exists = m.store.read(|s| s.stacks.contains_key(&name));
    Ok(Json(json!({ "name": name, "exists": exists, "services": services, "warnings": r.parsed.warnings, "nodes": r.nodes, "placements": placements, "unplaced": unplaced, "volumes": r.parsed.volumes, "options": options_json(&r.options, true) })))
}

/// Makes or changes a stack from a Compose file: its new workloads are placed on connected nodes with Docker (the
/// ones chosen for it, if any), and the reconcile on each node's manager does the rest. Its Compose ports are
/// published nowhere: what leaves the cluster is its exposures on the pool, given here or later.
async fn deploy_stack(State(m): State<Arc<Manager>>, h: HeaderMap, Json(b): Json<DeployBody>) -> R<(StatusCode, Json<Value>)> {
    authed(&m, &h)?;
    let (name, r) = ready(&m, &b)?;
    let placements = r.placements.map_err(|e| ApiError(StatusCode::CONFLICT, e))?;
    let existed = m.store.read(|s| s.stacks.contains_key(&name));
    m.store.propose(Entry::StackDeployed { name: name.clone(), compose: b.compose, services: r.parsed.services, placements, at: now_ms(), nodes: r.nodes, exposures: b.exposures, volumes: r.parsed.volumes, options: r.options }).await?;
    m.events.push("stack_deployed", None, format!("stack {name} {}", if existed { "updated" } else { "deployed" }));
    let v = m.store.read(|s| s.stacks.get(&name).map(|st| stack_json(&m, s, st, false))).unwrap_or(Value::Null);
    Ok((if existed { StatusCode::OK } else { StatusCode::CREATED }, Json(json!({ "stack": v, "warnings": r.parsed.warnings }))))
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

/// The node `n` names: its id, else the one node whose `name` label it is (given by the managers or reported by the
/// worker), else the one node of that hostname.
fn node_by(s: &crate::state::ClusterState, n: &str) -> Option<String> {
    if s.nodes.contains_key(n) {
        return Some(n.to_string());
    }
    let one = |hits: Vec<&String>| if let [one] = hits[..] { Some(one.clone()) } else { None };
    one(s.nodes.values().filter(|x| x.all_labels().get("name").is_some_and(|l| l == n)).map(|x| &x.id).collect())
        .or_else(|| one(s.nodes.values().filter(|x| x.hostname == n).map(|x| &x.id).collect()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_node_is_found_by_id_by_a_name_label_it_was_given_or_reports_or_by_hostname() {
        let node = |id: &str, host: &str, set: &[(&str, &str)], reported: &[(&str, &str)]| crate::state::Node {
            id: id.into(), hostname: host.into(), cert_sha256: String::new(), prev_cert_sha256: None, enrolled_at: 0,
            report: Some(crate::state::NodeReport { labels: reported.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), ..Default::default() }),
            reported_at: None, labels: set.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), fencing: Default::default(), recovered: false, overlay: None,
        };
        let mut s = crate::state::ClusterState::default();
        for n in [node("a", "docker", &[("name", "w3")], &[]), node("b", "docker", &[], &[("name", "vm1")]), node("c", "vm02", &[], &[])] {
            s.nodes.insert(n.id.clone(), n);
        }
        assert_eq!(node_by(&s, "a").as_deref(), Some("a"));
        assert_eq!(node_by(&s, "w3").as_deref(), Some("a"));
        assert_eq!(node_by(&s, "vm1").as_deref(), Some("b"), "a name from the worker's own FREIGHTDOCK_LABELS");
        assert_eq!(node_by(&s, "vm02").as_deref(), Some("c"));
        assert_eq!(node_by(&s, "docker"), None, "two have that hostname");
    }

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
        s.workloads.insert("a.web.1".into(), crate::state::Workload { id: "a.web.1".into(), stack: "a".into(), service: "web".into(), slot: 1, node_id: Some("new".into()), spec: crate::state::tests::spec("nginx", 1), generation: 1, host: None, leaving: None, attempts: None });
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
        let done = freightdock_proto::v1::WorkloadState { state: "exited".into(), generation: 4, ..Default::default() };
        assert!(left_exited(true, Some(&done), 4), "exited at its spec");
        assert!(!left_exited(false, Some(&done), 4), "stopped as wanted");
        assert!(!left_exited(true, Some(&done), 5), "an old spec: the new one runs");
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
