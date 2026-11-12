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
    let config = Config { data_dir: dir.path().into(), bind: addr.parse().unwrap(), api_bind: None, api_tls_bind: None, ui_bind: None, advertise: vec![addr.clone()], thresholds: Default::default(), bootstrap: join.is_none(), join: join.clone(), local_worker_token: None, fencing: Default::default() };
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
    let config = Config { data_dir: dir.path().join("data"), bind: addr.parse().unwrap(), api_bind: None, api_tls_bind: None, ui_bind: None, advertise: vec![addr], thresholds: Default::default(), bootstrap: true, join: None, local_worker_token: Some(path.clone()), fencing: Default::default() };
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

/// One HTTP/1.1 request over TLS, checked against the cluster CA for the managers' name: (status, body).
async fn https(m: &Manager, addr: &str, method: &str, path: &str, token: Option<&str>) -> (u16, String) {
    https_with(m, addr, method, path, token, "{}").await
}

async fn https_with(m: &Manager, addr: &str, method: &str, path: &str, token: Option<&str>, body: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut roots = rustls::RootCertStore::empty();
    roots.add(m.ca.cert_der.clone()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider).with_safe_default_protocol_versions().unwrap().with_root_certificates(roots).with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from(freightdock_proto::MANAGER_SERVER_NAME).unwrap();
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(cfg)).connect(name, tcp).await.unwrap();
    let auth = token.map(|t| format!("Authorization: Bearer {t}\r\n")).unwrap_or_default();
    tls.write_all(format!("{method} {path} HTTP/1.1\r\nHost: m\r\n{auth}Content-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut out = String::new();
    let _ = tls.read_to_string(&mut out).await;
    let status = out.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    (status, out.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}

#[tokio::test]
async fn the_api_over_tls_takes_api_tokens_by_role() {
    let a = start(None).await;
    until("a leader", || a.m.store.leader().is_some()).await;
    let read = format!("{}{}", freightdock_manager::admin::API_TOKEN_PREFIX, "ab".repeat(32));
    let e = Entry::ApiTokenIssued { id: "t1".into(), name: "ui".into(), role: freightdock_manager::state::ApiRole::Read, secret_hash: freightdock_proto::sha256_hex(&read), at: now_ms() };
    a.m.store.propose(e).await.unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(freightdock_manager::serve_admin_tls(a.m.clone(), l));
    assert_eq!(https(&a.m, &addr, "GET", "/v1/health", None).await.0, 200);
    let (s, ca) = https(&a.m, &addr, "GET", "/v1/ca", None).await;
    assert!(s == 200 && ca.contains("BEGIN CERTIFICATE"), "the CA's certificate is public");
    assert_eq!(https(&a.m, &addr, "GET", "/v1/nodes", None).await.0, 401);
    assert_eq!(https(&a.m, &addr, "GET", "/v1/nodes", Some("FDAT1-nope")).await.0, 401);
    assert_eq!(https(&a.m, &addr, "GET", "/v1/nodes", Some(&read)).await.0, 200);
    assert_eq!(https(&a.m, &addr, "POST", "/v1/join-tokens", Some(&read)).await.0, 403, "a read token only reads");
    let (s, b) = https(&a.m, &addr, "POST", "/v1/api-tokens", Some(&a.m.admin_token)).await;
    assert_eq!(s, 422, "a name is needed: {b}");

    // The UI's database: admin only, one writer, a big snapshot taken.
    let admin = Some(a.m.admin_token.as_str());
    assert_eq!(https(&a.m, &addr, "GET", "/v1/ui-db", Some(&read)).await.0, 403, "it holds the UI's users and secrets");
    let big = "x".repeat(5 << 20);
    assert_eq!(https_with(&a.m, &addr, "PUT", "/v1/ui-db/snapshot", admin, &format!(r#"{{"head":0,"data":"{big}"}}"#)).await.0, 204);
    assert_eq!(https_with(&a.m, &addr, "POST", "/v1/ui-db/changes", admin, r#"{"seq":1,"data":"c1"}"#).await.0, 204);
    assert_eq!(https_with(&a.m, &addr, "POST", "/v1/ui-db/changes", admin, r#"{"seq":1,"data":"other"}"#).await.0, 409);
    let (s, b) = https(&a.m, &addr, "GET", "/v1/ui-db?after=0", admin).await;
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!((s, v["head"].as_u64(), v["changes"][0]["data"].as_str(), v.get("snapshot")), (200, Some(1), Some("c1"), None));
    let (_, b) = https(&a.m, &addr, "GET", "/v1/ui-db", admin).await;
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!(v["snapshot"].as_str().map(str::len), Some(5 << 20));
    a.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ui_is_the_managers_own_stack_and_only_its_command_carries_the_token() {
    use freightdock_manager::state::NodeReport;
    use freightdock_manager::ui;
    use freightdock_proto::v1::manager_message::Body;
    let a = start(None).await;
    until("a leader", || a.m.store.leader().is_some()).await;
    let t = JoinToken::parse(&issue(&a.m, false).await).unwrap();
    a.m.store.propose(Entry::NodeEnrolled { node_id: "n1".into(), hostname: "h".into(), secret_hash: t.secret_hash(), cert_sha256: "c".into(), at: now_ms() }).await.unwrap();
    a.m.store.propose(Entry::NodeReported { node_id: "n1".into(), report: NodeReport { docker: true, ..Default::default() }, at: now_ms() }).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    a.m.sessions.open("n1", tx, "192.0.2.7:50000".into(), Some(Default::default()));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(freightdock_manager::serve_admin_tls(a.m.clone(), l));
    let admin = Some(a.m.admin_token.as_str());

    let (s, b) = https(&a.m, &addr, "GET", "/v1/ui", admin).await;
    assert_eq!(s, 200, "{b}");
    assert!(b.contains(r#""enabled":false"#), "{b}");
    let compose = r#"{"name":"freightdock","compose":"services:\n  web:\n    image: nginx\n"}"#;
    assert_eq!(https_with(&a.m, &addr, "POST", "/v1/stacks", admin, compose).await.0, 400, "the name is the managers'");
    assert_eq!(https_with(&a.m, &addr, "PUT", "/v1/ui", admin, r#"{"image":""}"#).await.0, 400);

    let (s, b) = https_with(&a.m, &addr, "PUT", "/v1/ui", admin, r#"{"image":"freightdock:test"}"#).await;
    assert_eq!(s, 201, "{b}");
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
    assert_eq!((v["enabled"].as_bool(), v["image"].as_str()), (Some(true), Some("freightdock:test")));
    let (w, kept) = a.m.store.read(|s| (s.workloads.get(ui::WORKLOAD).cloned(), s.api_tokens.get(ui::TOKEN_ID).cloned()));
    let w = w.expect("its workload");
    assert_eq!(w.node_id.as_deref(), Some("n1"));
    assert!(w.spec.env.values().all(|v| v.starts_with("@freightdock:")), "no secret in the state");
    let token = ui::token(&a.m.ca.key_pem());
    assert_eq!(kept.expect("its API token").secret_hash, freightdock_proto::sha256_hex(&token));
    assert_eq!(https(&a.m, &addr, "GET", "/v1/nodes", Some(&token)).await.0, 200, "the token works");
    assert_eq!(https_with(&a.m, &addr, "PUT", "/v1/ui", admin, r#"{"image":"freightdock:test2"}"#).await.0, 200, "again: a new image");
    assert_eq!(https(&a.m, &addr, "DELETE", "/v1/stacks/freightdock", admin).await.0, 400);

    // The run that reaches the worker has them filled.
    a.m.reconcile().await;
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("a run").unwrap().unwrap();
    let Some(Body::Command(c)) = got.body else { panic!("{got:?}") };
    let Some(freightdock_proto::v1::command::Op::RunContainer(r)) = &c.op else { panic!("{c:?}") };
    assert_eq!(r.image, "freightdock:test2");
    assert_eq!(r.env.get("FREIGHTDOCK_CLUSTER_TOKEN"), Some(&token));
    assert!(r.env["FREIGHTDOCK_CLUSTER_CA"].contains("BEGIN CERTIFICATE"));
    assert_eq!(r.env["FREIGHTDOCK_CLUSTER_API"], "https://127.0.0.1:7452");

    // Off: the stack and the token go.
    assert_eq!(https(&a.m, &addr, "DELETE", "/v1/ui", admin).await.0, 204);
    assert!(a.m.store.read(|s| !s.stacks.contains_key(ui::STACK) && !s.api_tokens.contains_key(ui::TOKEN_ID)));
    assert_eq!(https(&a.m, &addr, "GET", "/v1/nodes", Some(&token)).await.0, 401);
    a.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_manager_forwards_the_ui_s_port_to_the_node_it_runs_on() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let a = start(None).await;
    until("a leader", || a.m.store.leader().is_some()).await;
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paddr = proxy.local_addr().unwrap();
    // The UI on its node: an echo server.
    let ui = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = ui.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut c, _) = ui.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut r, mut w) = c.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    tokio::spawn(freightdock_manager::serve_ui(a.m.clone(), proxy, port as u32));

    // No UI deployed: the connection is closed at once.
    let mut c = tokio::net::TcpStream::connect(paddr).await.unwrap();
    let mut buf = [0u8; 8];
    assert_eq!(tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf)).await.unwrap().unwrap(), 0);

    // Deployed on n1, which connected over loopback to this manager: its host.
    let t = JoinToken::parse(&issue(&a.m, false).await).unwrap();
    a.m.store.propose(Entry::NodeEnrolled { node_id: "n1".into(), hostname: "h".into(), secret_hash: t.secret_hash(), cert_sha256: "c".into(), at: now_ms() }).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    a.m.sessions.open("n1", tx, "127.0.0.1:50000".into(), None);
    let services: std::collections::BTreeMap<_, _> = [(freightdock_manager::ui::SERVICE.to_string(), freightdock_manager::ui::spec("i"))].into();
    let placements = [(freightdock_manager::ui::WORKLOAD.to_string(), "n1".to_string())].into();
    a.m.store.propose(Entry::StackDeployed { name: freightdock_manager::ui::STACK.into(), compose: String::new(), services, placements, at: now_ms() }).await.unwrap();
    let mut c = tokio::net::TcpStream::connect(paddr).await.unwrap();
    c.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    tokio::time::timeout(Duration::from_secs(5), c.read_exact(&mut buf)).await.unwrap().unwrap();
    assert_eq!(&buf, b"hello");
    a.server.abort();
}
