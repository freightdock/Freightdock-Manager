//! The manager-to-manager service (`ManagerPeer`, Contracts' `manager.proto`). Only a caller with a manager's
//! certificate gets in; a worker's certificate, though from the same CA, is refused.

use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use tonic::{Request, Response, Status};

use freightdock_proto::token::JoinToken;
use freightdock_proto::v1::{HeardReply, HeardRequest, AddVoterReply, AddVoterRequest, JoinReply, JoinRequest, ProposeReply, ProposeRequest, RaftReply, RaftRequest, RaftRpc, manager_peer_server::ManagerPeer};

use crate::{Manager, now_ms};
use crate::raft::{Snapshot, SnapshotMeta, TypeConfig, Vote};
use crate::state::{Entry, Rejected};
use crate::store::ProposeError;

pub struct PeerSvc(pub Arc<Manager>);

/// The Raft id of the manager calling, or why it may not.
pub fn caller<T>(req: &Request<T>) -> Result<u64, Status> {
    let peer = req.extensions().get::<crate::tls::Peer>().ok_or_else(|| Status::internal("no peer"))?;
    let cert = peer.cert.as_ref().ok_or_else(|| Status::unauthenticated("a manager certificate is needed"))?;
    crate::ca::manager_of(cert).ok_or_else(|| Status::permission_denied("this certificate is not a manager's"))
}

fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(b).map_err(|e| Status::invalid_argument(format!("an unreadable Raft message: {e}")))
}

fn reply<T: Serialize, E: std::fmt::Display>(r: Result<T, E>) -> Result<Response<RaftReply>, Status> {
    Ok(Response::new(match r {
        Ok(v) => RaftReply { payload: serde_json::to_vec(&v).map_err(|e| Status::internal(e.to_string()))?, error: String::new() },
        Err(e) => RaftReply { payload: vec![], error: e.to_string() },
    }))
}

#[tonic::async_trait]
impl ManagerPeer for PeerSvc {
    async fn raft(&self, req: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        caller(&req)?;
        let r = req.into_inner();
        let me = self.0.store.id;
        if r.target != me {
            return Err(Status::failed_precondition(format!("this is Raft node {me}, not {}", r.target)));
        }
        let raft = &self.0.store.raft;
        match RaftRpc::try_from(r.rpc).unwrap_or(RaftRpc::Unspecified) {
            RaftRpc::AppendEntries => {
                let res = raft.append_entries(decode(&r.payload)?).await;
                reply(res.map(openraft::raft::AppendEntriesResponse::<TypeConfig>::from))
            }
            RaftRpc::Vote => reply(raft.vote(decode(&r.payload)?).await),
            RaftRpc::PreVote => reply(raft.pre_vote(decode(&r.payload)?).await),
            RaftRpc::FullSnapshot => {
                let (vote, meta, data): (Vote, SnapshotMeta, Vec<u8>) = decode(&r.payload)?;
                reply(raft.install_full_snapshot(vote, Snapshot { meta, snapshot: data }).await)
            }
            RaftRpc::Unspecified => Err(Status::invalid_argument("which Raft RPC?")),
        }
    }

    async fn propose(&self, req: Request<ProposeRequest>) -> Result<Response<ProposeReply>, Status> {
        caller(&req)?;
        let entry: Entry = decode(&req.into_inner().entry)?;
        Ok(Response::new(match self.0.store.propose_here(entry).await {
            Ok(index) => ProposeReply { index, ..Default::default() },
            Err(ProposeError::Rejected(r)) => ProposeReply { rejected: serde_json::to_vec(&r).unwrap_or_default(), ..Default::default() },
            Err(e) => ProposeReply { unavailable: e.to_string(), ..Default::default() },
        }))
    }

    /// A new manager asks to join with a manager join token. It has no certificate yet (it has no CA), so this is the
    /// one call open without one; the token is spent through consensus before the CA's key is handed over, and it
    /// checked this manager against the CA the token pins, so the key goes only to whoever holds the token.
    async fn join(&self, req: Request<JoinRequest>) -> Result<Response<JoinReply>, Status> {
        let r = req.into_inner();
        let token = JoinToken::parse_manager(&r.join_token).ok_or_else(|| Status::invalid_argument("not a manager join token (FDMT1-...)"))?;
        if token.ca_sha256 != self.0.ca.fingerprint() {
            return Err(Status::permission_denied("the join token is for another cluster"));
        }
        if r.raft_id == 0 || r.addr.trim().is_empty() {
            return Err(Status::invalid_argument("a raft id and an address are needed"));
        }
        if self.0.store.members().iter().any(|(id, _)| *id == r.raft_id) {
            return Err(Status::already_exists("a manager with this raft id is already in the cluster"));
        }
        let entry = Entry::ManagerJoined { raft_id: r.raft_id, addr: r.addr.clone(), secret_hash: token.secret_hash(), at: now_ms() };
        match self.0.store.propose(entry).await {
            Ok(_) => {}
            Err(ProposeError::Rejected(Rejected::TokenWrongKind)) => return Err(Status::permission_denied("that is a worker's join token")),
            Err(ProposeError::Rejected(why)) => return Err(Status::permission_denied(format!("the join token was refused: {why:?}"))),
            Err(e) => return Err(Status::unavailable(e.to_string())),
        }
        tracing::info!(raft_id = r.raft_id, addr = %r.addr, "a manager joined");
        self.0.events.push("manager_joined", None, format!("manager {} at {}", r.raft_id, r.addr));
        let cluster_id = self.0.store.read(|s| s.cluster_id.clone()).unwrap_or_default();
        Ok(Response::new(JoinReply { ca_pem: self.0.ca.cert_pem.clone(), ca_key_pem: self.0.ca.key_pem(), cluster_id, managers: self.0.managers() }))
    }

    /// A joined manager asks to become a voter, itself or through the manager it asked (which forwards to the leader
    /// with its own certificate). Every manager holds the CA key, so the caller's name proves little; what counts is
    /// that this raft id joined with a manager token, which the replicated state records.
    async fn add_voter(&self, req: Request<AddVoterRequest>) -> Result<Response<AddVoterReply>, Status> {
        caller(&req)?;
        let r = req.into_inner();
        if !self.0.store.read(|s| s.manager_joined(r.raft_id)) {
            return Err(Status::permission_denied("this manager did not join with a token"));
        }
        self.0.store.add_voter_anywhere(r.raft_id, &r.addr).await.map_err(|e| Status::unavailable(e.to_string()))?;
        tracing::info!(raft_id = r.raft_id, addr = %r.addr, "a manager became a voter");
        Ok(Response::new(AddVoterReply {}))
    }

    async fn heard(&self, req: Request<HeardRequest>) -> Result<Response<HeardReply>, Status> {
        caller(&req)?;
        Ok(Response::new(HeardReply { nodes: self.0.sessions.connected() }))
    }
}
