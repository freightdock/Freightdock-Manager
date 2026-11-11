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
            ProposeError::Rejected(r @ (Rejected::NodeUnknown | Rejected::CommandUnknown)) => ApiError(StatusCode::NOT_FOUND, r.to_string()),
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
        .route("/v1/join-tokens", post(join_token))
        .route("/v1/manager-tokens", post(manager_token))
        .route("/v1/commands", get(commands))
        .route("/v1/events", get(events))
        .route("/v1/commands/{id}", get(command))
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

/// The Raft members for the API. Ids are 63-bit numbers, beyond what a JavaScript number holds exactly, so they go out
/// as strings.
fn raft_members(members: Vec<(u64, String)>, voters: &[u64], leader: Option<u64>, me: u64) -> Vec<Value> {
    members.into_iter().map(|(id, addr)| json!({ "raft_id": id.to_string(), "addr": addr, "voter": voters.contains(&id), "leader": leader == Some(id), "me": id == me })).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_ids_go_out_as_strings_since_javascript_rounds_big_numbers() {
        let v = raft_members(vec![(5271529257891035005, "a:7450".into()), (7, "b:7450".into())], &[5271529257891035005], Some(5271529257891035005), 7);
        assert_eq!(v[0]["raft_id"], "5271529257891035005");
        assert_eq!((v[0]["leader"].clone(), v[0]["voter"].clone(), v[0]["me"].clone()), (json!(true), json!(true), json!(false)));
        assert_eq!((v[1]["voter"].clone(), v[1]["me"].clone()), (json!(false), json!(true)));
    }
}
