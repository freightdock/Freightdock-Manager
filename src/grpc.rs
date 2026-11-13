//! The worker API: Enrollment (a join token for a certificate) and WorkerSession (the long-lived stream).

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use freightdock_proto::sha256_hex;
use freightdock_proto::token::JoinToken;
use freightdock_proto::v1::{self, enrollment_server::Enrollment, manager_message, worker_message, worker_session_server::WorkerSession};

use crate::state::{CommandRecord, CommandStatus, Entry, NodeReport, Op, Rejected};
use crate::store::ProposeError;
use crate::{Manager, now_ms};

fn status_of(e: ProposeError) -> Status {
    match e {
        ProposeError::Rejected(r @ (Rejected::TokenUnknown | Rejected::TokenSpent | Rejected::TokenExpired)) => Status::permission_denied(r.to_string()),
        ProposeError::Rejected(r @ Rejected::NodeExists) => Status::already_exists(r.to_string()),
        ProposeError::Rejected(r) => Status::failed_precondition(r.to_string()),
        e @ (ProposeError::Unavailable(_) | ProposeError::NotLeader(_)) => Status::unavailable(e.to_string()),
        ProposeError::Io(e) => {
            tracing::error!("{e:#}");
            Status::unavailable("the manager could not commit the change")
        }
    }
}

fn peer_of<T>(req: &Request<T>) -> Option<crate::tls::Peer> {
    req.extensions().get::<crate::tls::Peer>().cloned()
}

pub fn op_to_proto(op: &Op) -> v1::command::Op {
    use v1::command::Op as P;
    match op.clone() {
        Op::Ping => P::Ping(v1::Ping {}),
        Op::DockerInfo => P::DockerInfo(v1::DockerInfo {}),
        Op::RunContainer { workload_id, image, name, command, env, labels, generation, ports, volumes, limits, net, storage } => P::RunContainer(v1::RunContainer {
            workload_id,
            image,
            name,
            command,
            env: env.into_iter().collect(),
            labels: labels.into_iter().collect(),
            generation,
            ports: ports.into_iter().map(|p| v1::PortMapping { host_port: p.host_port, container_port: p.container_port, protocol: p.protocol, host_ip: p.host_ip }).collect(),
            volumes,
            cpu_millis: limits.cpu_millis,
            memory_bytes: limits.memory_bytes,
            network_stack: net.as_ref().map(|n| n.stack.clone()).unwrap_or_default(),
            address: net.as_ref().map(|n| n.address.clone()).unwrap_or_default(),
            hosts: net.map(|n| n.hosts).unwrap_or_default(),
            storage: storage
                .into_iter()
                .map(|m| v1::StorageVolume { volume: m.volume, storage: m.storage, driver: match m.driver { crate::state::StorageDriver::Nfs => "nfs", crate::state::StorageDriver::Path => "path" }.into(), source: m.source, options: m.options, dir: m.dir })
                .collect(),
        }),
        Op::StopContainer { workload_id, timeout_s } => P::StopContainer(v1::StopContainer { workload_id, timeout_s }),
        Op::RemoveContainer { workload_id } => P::RemoveContainer(v1::RemoveContainer { workload_id }),
    }
}

pub fn command_to_proto(c: &CommandRecord) -> v1::ManagerMessage {
    v1::ManagerMessage {
        body: Some(manager_message::Body::Command(v1::Command { command_id: c.id.clone(), node_id: c.node_id.clone(), config_version: c.config_version, deadline_ms: c.deadline, op: Some(op_to_proto(&c.op)) })),
    }
}

fn status_from_proto(s: i32) -> Option<CommandStatus> {
    use v1::CommandStatus as P;
    Some(match P::try_from(s).ok()? {
        P::Received => CommandStatus::Received,
        P::Accepted => CommandStatus::Accepted,
        P::Succeeded => CommandStatus::Succeeded,
        P::Failed => CommandStatus::Failed,
        P::Rejected => CommandStatus::Rejected,
        P::Expired => CommandStatus::Expired,
        P::Unspecified | P::Pending => return None,
    })
}

fn report_of(h: &v1::Hello) -> NodeReport {
    let caps = h.capabilities.clone().unwrap_or_default();
    let res = h.resources.unwrap_or_default();
    NodeReport {
        hostname: h.hostname.clone(),
        agent_version: h.agent_version.clone(),
        boot_id: h.boot_id.clone(),
        fencing_state: v1::FencingState::try_from(h.fencing_state).map(|f| f.as_str_name().trim_start_matches("FENCING_STATE_").to_ascii_lowercase()).unwrap_or_default(),
        labels: h.labels.clone().into_iter().collect(),
        docker: caps.docker,
        docker_version: caps.docker_version,
        os: caps.os,
        arch: caps.arch,
        kernel: caps.kernel,
        cpu_count: res.cpu_count,
        memory_total_bytes: res.memory_total_bytes,
        wg_public_key: h.network.as_ref().map(|n| n.public_key.clone()).unwrap_or_default(),
        docker_requests: caps.docker_requests,
    }
}

pub struct EnrollmentSvc(pub Arc<Manager>);

#[tonic::async_trait]
impl Enrollment for EnrollmentSvc {
    async fn enroll(&self, req: Request<v1::EnrollRequest>) -> Result<Response<v1::EnrollResponse>, Status> {
        let peer = peer_of(&req).map(|p| p.addr.to_string()).unwrap_or_default();
        let r = req.into_inner();
        let token = JoinToken::parse(&r.join_token).ok_or_else(|| Status::invalid_argument("not a join token"))?;
        if token.ca_sha256 != self.0.ca.fingerprint() {
            return Err(Status::permission_denied("the join token is for another cluster"));
        }
        if uuid::Uuid::parse_str(&r.node_id).is_err() {
            return Err(Status::invalid_argument("the node id is not a UUID"));
        }
        let hostname: String = r.hostname.chars().filter(|c| !c.is_control()).take(253).collect();
        let (cert_pem, cert_sha256) = self.0.ca.sign_worker(&r.csr_pem, &r.node_id).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let entry = Entry::NodeEnrolled { node_id: r.node_id.clone(), hostname: hostname.clone(), secret_hash: token.secret_hash(), cert_sha256, at: now_ms() };
        self.0.store.propose(entry).await.map_err(status_of)?;
        tracing::info!(node = %r.node_id, %hostname, %peer, "a worker enrolled");
        self.0.events.push("enrolled", Some(&r.node_id), format!("{hostname} enrolled from {peer}"));
        let cluster_id = self.0.store.read(|s| s.cluster_id.clone()).unwrap_or_default();
        Ok(Response::new(v1::EnrollResponse { cert_pem, ca_pem: self.0.ca.cert_pem.clone(), cluster_id, managers: self.0.managers() }))
    }
}

/// A worker's certificate (five years) is renewed in its last year.
pub const RENEW_BEFORE: std::time::Duration = std::time::Duration::from_secs(365 * 86_400);

pub struct SessionSvc(pub Arc<Manager>);

#[tonic::async_trait]
impl WorkerSession for SessionSvc {
    type OpenStream = ReceiverStream<Result<v1::ManagerMessage, Status>>;

    /// A new certificate for the node whose certificate this call came with (G2.7).
    async fn renew(&self, req: Request<v1::RenewRequest>) -> Result<Response<v1::RenewResponse>, Status> {
        let m = &self.0;
        let peer = peer_of(&req).ok_or_else(|| Status::internal("no peer"))?;
        let cert = peer.cert.as_ref().ok_or_else(|| Status::unauthenticated("a client certificate is needed"))?;
        let fp = sha256_hex(cert);
        let node_id = m.store.read(|s| s.node_by_cert(&fp).map(|n| n.id.clone())).ok_or_else(|| Status::permission_denied("this certificate is not an enrolled node's"))?;
        let (cert_pem, cert_sha256) = m.ca.sign_worker(&req.into_inner().csr_pem, &node_id).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        m.store.propose(Entry::NodeCertRenewed { node_id: node_id.clone(), cert_sha256, at: now_ms() }).await.map_err(status_of)?;
        tracing::info!(node = %node_id, peer = %peer.addr, "a worker's certificate renewed");
        m.events.push("renewed", Some(&node_id), "its certificate was renewed");
        Ok(Response::new(v1::RenewResponse { cert_pem, ca_pem: m.ca.cert_pem.clone() }))
    }

    async fn open(&self, req: Request<Streaming<v1::WorkerMessage>>) -> Result<Response<Self::OpenStream>, Status> {
        let m = self.0.clone();
        if m.cut_off() {
            return Err(Status::unavailable("this manager is cut off from a majority of managers; try another"));
        }
        let peer = peer_of(&req).ok_or_else(|| Status::internal("no peer"))?;
        let cert = peer.cert.as_ref().ok_or_else(|| Status::unauthenticated("a client certificate is needed"))?;
        let fp = sha256_hex(cert);
        let node_id = m.store.read(|s| s.node_by_cert(&fp).map(|n| n.id.clone())).ok_or_else(|| Status::permission_denied("this certificate is not an enrolled node's"))?;

        let mut inbound = req.into_inner();
        let hello = match tokio::time::timeout(std::time::Duration::from_secs(10), inbound.message()).await {
            Ok(Ok(Some(v1::WorkerMessage { body: Some(worker_message::Body::Hello(h)) }))) => h,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(Status::deadline_exceeded("no hello within 10s")),
            _ => return Err(Status::invalid_argument("the first message must be a hello")),
        };
        if hello.node_id != node_id {
            return Err(Status::permission_denied("the hello names another node than the certificate"));
        }
        let report = report_of(&hello);
        if m.store.read(|s| s.nodes.get(&node_id).and_then(|n| n.report.clone())) != Some(report.clone()) {
            m.store.propose(Entry::NodeReported { node_id: node_id.clone(), report, at: now_ms() }).await.map_err(status_of)?;
        }

        let (tx, rx) = mpsc::channel(256);
        let welcome = m.welcome(crate::ca::expires_within(cert, RENEW_BEFORE));
        tx.send(Ok(welcome)).await.map_err(|_| Status::cancelled("gone"))?;
        let fenced = hello.fencing_state == v1::FencingState::AwaitingAuthorization as i32;
        let generation = m.sessions.open_fenced(&node_id, tx.clone(), peer.addr.to_string(), hello.workloads.clone(), fenced);
        m.sessions.heard_network(&node_id, generation, hello.network.clone());
        // A node enrolled before G10 is given its overlay address now.
        if m.store.read(|s| s.nodes.get(&node_id).is_some_and(|n| n.overlay.is_none()))
            && let Err(e) = m.store.propose(Entry::NodeAddressed { node_id: node_id.clone() }).await
        {
            tracing::warn!(node = %node_id, "giving it its overlay address: {e}");
        }
        tracing::info!(node = %node_id, hostname = %hello.hostname, peer = %peer.addr, in_flight = hello.in_flight.len(), "worker connected");
        m.events.push("connected", Some(&node_id), format!("connected from {}", peer.addr));

        // What it has not finished, again: it knows each command by id and runs none twice.
        for c in m.store.read(|s| s.open_commands(&node_id).into_iter().cloned().collect::<Vec<_>>()) {
            let _ = tx.send(Ok(m.wire(&c))).await;
        }

        tokio::spawn(async move {
            loop {
                let msg = match inbound.message().await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::debug!(node = %node_id, "stream: {e}");
                        break;
                    }
                };
                match msg.body {
                    Some(worker_message::Body::Heartbeat(hb)) => {
                        m.sessions.fenced(&node_id, generation, hb.fencing_state == v1::FencingState::AwaitingAuthorization as i32);
                        m.sessions.heard_network(&node_id, generation, hb.network);
                        m.sessions.heard_traffic(&node_id, generation, hb.traffic);
                        m.sessions.heard(&node_id, generation, hb.resources, hb.workloads);
                        let ack = v1::ManagerMessage { body: Some(manager_message::Body::HeartbeatAck(v1::HeartbeatAck { seq: hb.seq })) };
                        if tx.send(Ok(ack)).await.is_err() {
                            break;
                        }
                    }
                    Some(worker_message::Body::CommandUpdate(u)) => {
                        m.sessions.heard(&node_id, generation, None, None);
                        let Some(status) = status_from_proto(u.status) else { continue };
                        if status.is_terminal() {
                            m.sessions.command_ended(&node_id, generation);
                        }
                        if m.store.read(|s| s.commands.get(&u.command_id).map(|c| c.node_id != node_id)).unwrap_or(true) {
                            tracing::warn!(node = %node_id, command = %u.command_id, "a report on a command that is not this node's");
                            continue;
                        }
                        let message: String = u.message.chars().take(4096).collect();
                        let e = Entry::CommandStatus { command_id: u.command_id.clone(), status, message, result_json: u.result_json, at: now_ms() };
                        if let Err(e) = m.store.propose(e).await {
                            tracing::warn!(node = %node_id, command = %u.command_id, "{e}");
                        }
                    }
                    Some(worker_message::Body::DockerReply(r)) => {
                        m.sessions.heard(&node_id, generation, None, None);
                        m.docker_replied(r);
                    }
                    Some(worker_message::Body::Hello(_)) | None => {}
                }
            }
            m.sessions.close(&node_id, generation);
            tracing::info!(node = %node_id, "worker disconnected");
            m.events.push("disconnected", Some(&node_id), "the stream ended");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
