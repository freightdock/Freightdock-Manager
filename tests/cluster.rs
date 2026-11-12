//! Real managers in one process, each with its own data folder and port, talking over the ManagerPeer gRPC service
//! with mutual TLS: they join one Raft cluster by token, every change reaches all of them from any of them, and when
//! the leader goes the others elect a new one and carry on.

use std::sync::Arc;
use std::time::Duration;

use freightdock_manager::state::Entry;
use freightdock_manager::{Config, Join, Manager, now_ms};
use freightdock_proto::token::JoinToken;

struct Running {
    m: Arc<Manager>,
    server: tokio::task::JoinHandle<()>,
    addr: String,
    _dir: tempfile::TempDir,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A manager: a new cluster's first, or joining through another manager with a manager join token.
async fn start(join: Option<(&str, String)>) -> Running {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let join = join.map(|(a, token)| Join { addr: a.to_string(), token });
    let config = Config { data_dir: dir.path().into(), bind: addr.parse().unwrap(), api_bind: None, advertise: vec![addr.clone()], thresholds: Default::default(), bootstrap: join.is_none(), join: join.clone(), local_worker_token: None, fencing: Default::default() };
    let m = Manager::open(&config).await.unwrap();
    let l = tokio::net::TcpListener::bind(config.bind).await.unwrap();
    let server = tokio::spawn({
        let m = m.clone();
        async move {
            let _ = freightdock_manager::serve_workers(m, l).await;
        }
    });
    if let Some(j) = join {
        tokio::spawn(m.clone().become_voter(j.addr));
    }
    Running { m, server, addr, _dir: dir }
}

/// A join token for a manager (`for_manager`) or a worker, issued through `m`.
async fn issue(m: &Manager, for_manager: bool) -> String {
    let t = JoinToken { ca_sha256: m.ca.fingerprint(), secret: hex::encode(rand::random::<[u8; 32]>()) };
    m.store.propose(Entry::JoinTokenIssued { secret_hash: t.secret_hash(), expires_at: now_ms() + 60_000, at: now_ms(), for_manager }).await.unwrap();
    if for_manager { t.to_manager_string() } else { t.to_string() }
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..150 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

fn token(hash: &str) -> Entry {
    Entry::JoinTokenIssued { secret_hash: hash.into(), expires_at: now_ms() + 60_000, at: now_ms(), for_manager: false }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_managers_join_replicate_and_survive_the_leader() {
    let a = start(None).await;
    // b joins through the leader; c through b, a follower, so its join and its voter request are forwarded.
    let b = start(Some((&a.addr, issue(&a.m, true).await))).await;
    until("b a voter", || a.m.store.voters().len() == 2).await;
    let c = start(Some((&b.addr, issue(&a.m, true).await))).await;
    let all = [&a, &b, &c];
    let ids: Vec<u64> = all.iter().map(|x| x.m.store.id).collect();
    until("three voters on every manager", || all.iter().all(|x| {
        let mut v = x.m.store.voters();
        v.sort();
        let mut want = ids.clone();
        want.sort();
        v == want
    }))
    .await;
    assert_eq!(c.m.ca.fingerprint(), a.m.ca.fingerprint(), "the joiner got the cluster's CA");
    assert!(all.iter().all(|x| x.m.store.read(|s| s.cluster_id.is_some())), "the cluster id reached the joiners");
    assert_eq!(c.m.managers().len(), 3, "workers are told of every manager: {:?}", c.m.managers());
    assert_eq!(c.m.managers()[0], c.addr, "this manager first");

    // A change proposed on a follower is forwarded to the leader and reaches all three.
    let follower = all.iter().find(|x| !x.m.store.is_leader()).unwrap();
    follower.m.store.propose(token("one")).await.unwrap();
    until("the change everywhere", || all.iter().all(|x| x.m.store.read(|s| s.join_tokens.contains_key("one")))).await;

    // The leader goes away.
    let gone = all.iter().position(|x| x.m.store.is_leader()).unwrap();
    all[gone].m.store.shutdown().await;
    all[gone].server.abort();
    let rest: Vec<&&Running> = all.iter().enumerate().filter(|(i, _)| *i != gone).map(|(_, x)| x).collect();
    until("a new leader", || rest.iter().any(|x| x.m.store.is_leader())).await;
    // Through the one that does not lead, so this goes through forwarding to the new leader.
    let other = rest.iter().find(|x| !x.m.store.is_leader()).unwrap();
    other.m.store.propose(token("two")).await.unwrap();
    until("the second change on both", || rest.iter().all(|x| x.m.store.read(|s| s.join_tokens.contains_key("two")))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_or_worker_token_makes_no_manager() {
    use freightdock_proto::v1::{JoinRequest, manager_peer_client::ManagerPeerClient};
    let a = start(None).await;
    let join = |token: String| {
        let addr = a.addr.clone();
        let pin = a.m.ca.fingerprint();
        async move {
            let ch = freightdock_manager::tls::peer_channel(&addr, freightdock_manager::tls::pinned_client_config(&pin).unwrap()).unwrap();
            ManagerPeerClient::new(ch).join(JoinRequest { join_token: token, raft_id: 77, addr: "127.0.0.1:1".into() }).await
        }
    };
    // A worker's token, even turned into a manager token's shape, is refused.
    let worker = JoinToken::parse(&issue(&a.m, false).await).unwrap();
    let err = join(worker.to_manager_string()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");

    let token = issue(&a.m, true).await;
    let ok = join(token.clone()).await.unwrap().into_inner();
    assert_eq!(ok.ca_pem, a.m.ca.cert_pem);
    assert!(ok.ca_key_pem.contains("PRIVATE KEY"));
    let err = join(token).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "a token is good once: {err:?}");
}

#[tokio::test]
async fn a_worker_certificate_opens_nothing_between_managers() {
    use freightdock_manager::ca::ServerIdentity;
    use freightdock_proto::v1::{RaftRequest, RaftRpc, manager_peer_client::ManagerPeerClient};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject};

    let a = start(None).await;
    let key = rcgen::KeyPair::generate().unwrap();
    let csr = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap().serialize_request(&key).unwrap().pem().unwrap();
    let (pem, _) = a.m.ca.sign_worker(&csr, "6f1c7c2e-0000-4000-8000-000000000000").unwrap();
    let worker = ServerIdentity { chain: vec![CertificateDer::from_pem_slice(pem.as_bytes()).unwrap()], key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())) };
    let tls = freightdock_manager::tls::peer_client_config(&a.m.ca, worker).unwrap();
    let mut client = ManagerPeerClient::new(freightdock_manager::tls::peer_channel(&a.addr, tls).unwrap());
    let err = client.raft(RaftRequest { target: a.m.store.id, rpc: RaftRpc::Vote as i32, payload: b"{}".to_vec() }).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_quorum_changes_fail_fast_and_reads_still_answer() {
    let a = start(None).await;
    let b = start(Some((&a.addr, issue(&a.m, true).await))).await;
    until("b a voter", || a.m.store.voters().len() == 2).await;
    until("both with the majority", || a.m.store.in_quorum() && b.m.store.in_quorum()).await;
    a.m.store.propose(token("before")).await.unwrap();
    until("the change on b", || b.m.store.read(|s| s.join_tokens.contains_key("before"))).await;

    // Two voters, one gone: no quorum left, whichever of them led.
    b.m.store.shutdown().await;
    b.server.abort();
    let started = std::time::Instant::now();
    let r = a.m.store.propose(token("after")).await;
    assert!(matches!(r, Err(freightdock_manager::store::ProposeError::Unavailable(_))), "{r:?}");
    assert!(started.elapsed() < Duration::from_secs(12), "failed in {:?}", started.elapsed());
    assert!(a.m.store.read(|s| s.join_tokens.contains_key("before")), "reads still answer");
    until("a out of the majority", || !a.m.store.in_quorum()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_made_on_one_manager_reaches_its_worker_on_another() {
    use freightdock_manager::state::{CommandRecord, CommandStatus, Op};
    use freightdock_proto::v1::manager_message::Body;
    let a = start(None).await;
    let b = start(Some((&a.addr, issue(&a.m, true).await))).await;
    until("b a voter", || a.m.store.voters().len() == 2).await;
    let t = JoinToken::parse(&issue(&a.m, false).await).unwrap();
    a.m.store.propose(Entry::NodeEnrolled { node_id: "n1".into(), hostname: "h".into(), secret_hash: t.secret_hash(), cert_sha256: "c".into(), at: now_ms() }).await.unwrap();

    // The worker is connected to b; the command is made through a.
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    b.m.sessions.open("n1", tx, "test".into(), None);
    let now = now_ms();
    let c = CommandRecord { id: "c1".into(), node_id: "n1".into(), op: Op::Ping, config_version: 0, created_at: now, deadline: now + 60_000, status: CommandStatus::Pending, message: String::new(), result_json: String::new(), updated_at: now };
    a.m.store.propose(Entry::CommandCreated { command: c }).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("delivered by b").unwrap().unwrap();
    assert!(matches!(got.body, Some(Body::Command(ref c)) if c.command_id == "c1"), "{got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_heard_by_one_manager_is_healthy_on_all() {
    use freightdock_manager::sessions::Health;
    let a = start(None).await;
    let b = start(Some((&a.addr, issue(&a.m, true).await))).await;
    until("b a voter", || a.m.store.voters().len() == 2).await;
    assert_eq!(a.m.sessions.liveness("n1").health, Health::Unreachable);

    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    b.m.sessions.open("n1", tx, "10.0.0.5:4000".into(), None);
    a.m.gather_heard().await;
    let l = a.m.sessions.liveness("n1");
    assert_eq!(l.health, Health::Healthy, "{l:?}");
    assert_eq!(l.via.as_deref(), Some(b.addr.as_str()), "and a says who hears it");
    assert_eq!(l.peer.as_deref(), Some("10.0.0.5:4000"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manager_leaves_a_token_for_the_worker_on_its_own_node() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("local-worker.token");
    let addr = format!("127.0.0.1:{}", free_port());
    let config = Config { data_dir: dir.path().join("data"), bind: addr.parse().unwrap(), api_bind: None, advertise: vec![addr], thresholds: Default::default(), bootstrap: true, join: None, local_worker_token: Some(path.clone()), fencing: Default::default() };
    let m = Manager::open(&config).await.unwrap();
    m.local_worker_token().await;
    let t = JoinToken::parse(&std::fs::read_to_string(&path).unwrap()).expect("a worker's join token");
    assert_eq!(t.ca_sha256, m.ca.fingerprint());
    let kept = m.store.read(|s| s.join_tokens.get(&t.secret_hash()).cloned()).expect("the cluster knows it");
    assert!(!kept.for_manager);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    }
    // Once there, it is left alone.
    m.local_worker_token().await;
    assert_eq!(JoinToken::parse(&std::fs::read_to_string(&path).unwrap()), Some(t));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stack_deployed_on_one_manager_is_reconciled_by_the_worker_s_manager() {
    use freightdock_manager::state::CommandStatus;
    use freightdock_proto::v1::manager_message::Body;
    use freightdock_proto::v1::{WorkloadReport, WorkloadState};
    let a = start(None).await;
    let b = start(Some((&a.addr, issue(&a.m, true).await))).await;
    until("b a voter", || a.m.store.voters().len() == 2).await;
    let t = JoinToken::parse(&issue(&a.m, false).await).unwrap();
    a.m.store.propose(Entry::NodeEnrolled { node_id: "n1".into(), hostname: "h".into(), secret_hash: t.secret_hash(), cert_sha256: "c".into(), at: now_ms() }).await.unwrap();

    // n1 is connected to b and runs nothing yet.
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let g = b.m.sessions.open("n1", tx, "test".into(), Some(WorkloadReport::default()));
    let parsed = freightdock_manager::compose::parse("services:\n  web:\n    image: nginx:alpine\n").unwrap();
    let placements = [("app.web.1".to_string(), "n1".to_string())].into();
    a.m.store.propose(Entry::StackDeployed { name: "app".into(), compose: String::new(), services: parsed.services, placements, at: now_ms() }).await.unwrap();
    until("the stack on b", || b.m.store.read(|s| s.workloads.contains_key("app.web.1"))).await;

    a.m.reconcile().await;
    assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(), "a does not reconcile a worker it does not hold");
    b.m.reconcile().await;
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("a run").unwrap().unwrap();
    let Some(Body::Command(c)) = got.body else { panic!("{got:?}") };
    let Some(freightdock_proto::v1::command::Op::RunContainer(r)) = &c.op else { panic!("{c:?}") };
    assert_eq!((r.workload_id.as_str(), r.image.as_str()), ("app.web.1", "nginx:alpine"));

    // While it runs, nothing more; once done and reported running, still nothing.
    b.m.reconcile().await;
    assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(), "one command at a time");
    let done = Entry::CommandStatus { command_id: c.command_id.clone(), status: CommandStatus::Succeeded, message: "running".into(), result_json: String::new(), at: now_ms() };
    b.m.store.propose(done).await.unwrap();
    until("the end on b", || b.m.store.read(|s| s.busy_workloads().is_empty())).await;
    b.m.sessions.command_ended("n1", g);
    let up = WorkloadReport { workloads: vec![WorkloadState { workload_id: "app.web.1".into(), state: "running".into(), generation: r.generation, ..Default::default() }] };
    b.m.sessions.heard("n1", g, None, Some(up.clone()));
    b.m.reconcile().await;
    assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(), "as wanted");

    // The stack goes: its container is removed.
    a.m.store.propose(Entry::StackRemoved { name: "app".into(), at: now_ms() }).await.unwrap();
    until("gone on b", || b.m.store.read(|s| s.stacks.is_empty())).await;
    b.m.reconcile().await;
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("a remove").unwrap().unwrap();
    assert!(matches!(got.body, Some(Body::Command(ref c)) if matches!(c.op, Some(freightdock_proto::v1::command::Op::RemoveContainer(_)))), "{got:?}");
}
