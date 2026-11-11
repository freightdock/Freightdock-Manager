//! How this manager reaches the other managers' Raft: the `ManagerPeer.Raft` gRPC call (Contracts' `manager.proto`),
//! over mutual TLS with this manager's client certificate. Each message is the openraft type as JSON, addressed to the
//! target's Raft id, which the receiver checks.

use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::{AnyError, BasicNode, OptionalSend};
use serde::{Serialize, de::DeserializeOwned};

use freightdock_proto::v1::{RaftRequest, RaftRpc, manager_peer_client::ManagerPeerClient};

use super::{NodeId, Snapshot, TypeConfig, Vote, sm::SnapshotData};

#[derive(Clone)]
pub struct Network {
    /// None for a manager that never talks to others (tests).
    tls: Option<Arc<rustls::ClientConfig>>,
    me: NodeId,
}

impl Network {
    pub fn new(me: NodeId, tls: Arc<rustls::ClientConfig>) -> Self {
        Network { tls: Some(tls), me }
    }

    pub fn alone(me: NodeId) -> Self {
        Network { tls: None, me }
    }
}

pub struct Peer {
    target: NodeId,
    addr: String,
    client: Option<ManagerPeerClient<tonic::transport::Channel>>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Peer {
        let client = self.tls.clone().and_then(|tls| match crate::tls::peer_channel(&node.addr, tls) {
            Ok(ch) => Some(ManagerPeerClient::new(ch).max_decoding_message_size(64 << 20).max_encoding_message_size(64 << 20)),
            Err(e) => {
                tracing::warn!(raft_id = target, "the address {} is not usable: {e:#}", node.addr);
                None
            }
        });
        let _ = self.me;
        Peer { target, addr: node.addr.clone(), client }
    }
}

/// Why a call failed, as openraft wants it told: unreachable (back off), timed out, or a network error.
enum Fail {
    Unreachable(String),
    Timeout,
    Network(String),
    Remote(String),
}

impl Peer {
    async fn call<Req: Serialize, Resp: DeserializeOwned>(&mut self, rpc: RaftRpc, req: &Req, option: &RPCOption) -> Result<Resp, Fail> {
        let client = self.client.as_mut().ok_or_else(|| Fail::Unreachable(format!("no way to reach {}", self.addr)))?;
        let payload = serde_json::to_vec(req).map_err(|e| Fail::Network(e.to_string()))?;
        let request = RaftRequest { target: self.target, rpc: rpc as i32, payload };
        let reply = match tokio::time::timeout(option.hard_ttl(), client.raft(request)).await {
            Err(_) => return Err(Fail::Timeout),
            Ok(Err(s)) if matches!(s.code(), tonic::Code::Unavailable | tonic::Code::Unknown) => return Err(Fail::Unreachable(format!("{}: {}", self.addr, s.message()))),
            Ok(Err(s)) => return Err(Fail::Network(format!("{}: {}", self.addr, s.message()))),
            Ok(Ok(r)) => r.into_inner(),
        };
        if !reply.error.is_empty() {
            return Err(Fail::Remote(reply.error));
        }
        serde_json::from_slice(&reply.payload).map_err(|e| Fail::Network(format!("an unreadable answer from {}: {e}", self.addr)))
    }

    fn rpc_error(&self, f: Fail, option: &RPCOption) -> RPCError<TypeConfig> {
        match f {
            Fail::Unreachable(why) => RPCError::Unreachable(Unreachable::new(&AnyError::error(why))),
            Fail::Timeout => RPCError::Timeout(Timeout { action: openraft::network::RPCTypes::AppendEntries, id: 0, target: self.target, timeout: option.hard_ttl() }),
            // A Raft failure on the other side (it is shutting down, its storage failed) is told as a network error:
            // openraft retries it as it would a lost message.
            Fail::Network(why) | Fail::Remote(why) => RPCError::Network(NetworkError::new(&AnyError::error(why))),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for Peer {
    type SnapshotData = SnapshotData;

    async fn append_entries(&mut self, rpc: AppendEntriesRequest<TypeConfig>, option: RPCOption) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let prev = rpc.prev_log_id;
        let last = rpc.entries.last().map(|e| e.log_id).or(prev);
        let r: AppendEntriesResponse<TypeConfig> = self.call(RaftRpc::AppendEntries, &rpc, &option).await.map_err(|f| self.rpc_error(f, &option))?;
        Ok(r.into_append_result(prev, last))
    }

    async fn vote(&mut self, rpc: VoteRequest<TypeConfig>, option: RPCOption) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call(RaftRpc::Vote, &rpc, &option).await.map_err(|f| self.rpc_error(f, &option))
    }

    async fn pre_vote(&mut self, rpc: VoteRequest<TypeConfig>, option: RPCOption) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call(RaftRpc::PreVote, &rpc, &option).await.map_err(|f| self.rpc_error(f, &option))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let req = (vote, snapshot.meta, snapshot.snapshot);
        self.call(RaftRpc::FullSnapshot, &req, &option).await.map_err(|f| match f {
            Fail::Unreachable(why) | Fail::Remote(why) => StreamingError::Unreachable(Unreachable::new(&AnyError::error(why))),
            Fail::Timeout => StreamingError::Timeout(Timeout { action: openraft::network::RPCTypes::InstallSnapshot, id: 0, target: self.target, timeout: option.hard_ttl() }),
            Fail::Network(why) => StreamingError::Network(NetworkError::new(&AnyError::error(why))),
        })
    }
}
