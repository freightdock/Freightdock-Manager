//! The UI as a system workload (G8.5): the managers keep a stack of their own, [`STACK`], with one service, the
//! Freightdock UI, placed and recovered like any other workload. It needs no volume: its database is the managers'
//! (G8.1), so it moves freely when its node fails.
//!
//! What the UI needs to reach the managers (an admin API token, the cluster CA, the managers' addresses) is never in
//! the replicated state: the spec carries placeholders, and the manager that sends the command fills them in
//! ([`fill`]). The token is derived from the cluster CA's key, which every manager holds ([`token`]); the state keeps
//! only its hash, as an API token named [`TOKEN_NAME`]. A user's stack cannot ask for them: the name [`STACK`] is
//! reserved, and only its workloads are filled.
//!
//! Every manager forwards [`UI_PORT`] to the node the UI runs on, at [`HOST_PORT`], as plain TCP: the TLS is the UI's
//! own, end to end ([`target`]).

use std::collections::BTreeMap;

use crate::state::{Op, Port, ServiceSpec};

/// The system stack's name, reserved: a deploy through the API may not use it.
pub const STACK: &str = "freightdock";
pub const SERVICE: &str = "ui";
/// The UI's one workload.
pub const WORKLOAD: &str = "freightdock.ui.1";
/// Where the UI listens on its node, published from the container's 7443.
pub const HOST_PORT: u32 = 7453;
/// Where every manager answers for the UI.
pub const UI_PORT: u16 = 7443;
/// The API token's id and name in the state.
pub const TOKEN_ID: &str = "freightdock-ui";
pub const TOKEN_NAME: &str = "freightdock-ui";

const TOKEN: &str = "@freightdock:token";
const CA: &str = "@freightdock:ca";
const MANAGERS: &str = "@freightdock:managers";
const NAMES: &str = "@freightdock:names";

/// The UI's service: `image`, its port published at [`HOST_PORT`], and the placeholders [`fill`] fills.
pub fn spec(image: &str) -> ServiceSpec {
    let env: BTreeMap<String, String> = [
        ("FREIGHTDOCK_CLUSTER_API", MANAGERS),
        ("FREIGHTDOCK_CLUSTER_TOKEN", TOKEN),
        ("FREIGHTDOCK_CLUSTER_CA", CA),
        ("FREIGHTDOCK_TLS_NAMES", NAMES),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    ServiceSpec {
        image: image.trim().to_string(),
        command: vec![],
        env,
        labels: [("freightdock.system".to_string(), "ui".to_string())].into(),
        ports: vec![Port { host_port: HOST_PORT, container_port: 7443, protocol: "tcp".into(), host_ip: String::new() }],
        volumes: vec![],
        replicas: 1,
        limits: Default::default(),
        placement: Default::default(),
        ha: true,
    }
}

/// The UI's API token: the same on every manager, from the CA's key, never written down.
pub fn token(ca_key_pem: &str) -> String {
    format!("{}{}", crate::admin::API_TOKEN_PREFIX, freightdock_proto::sha256_hex(format!("freightdock-ui-token-v1\n{ca_key_pem}").as_bytes()))
}

/// What [`fill`] puts in.
pub struct Fill {
    pub token: String,
    pub ca_pem: String,
    /// The managers' addresses as they advertise them (`host:7450`).
    pub managers: Vec<String>,
}

/// A manager's host, from `host:port` (`[v6]:port` too).
fn host(addr: &str) -> &str {
    let h = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    h.trim_start_matches('[').trim_end_matches(']')
}

/// Fills the placeholders of the UI's workload; anything else is left as it is.
pub fn fill(op: &mut Op, f: &Fill) {
    let Op::RunContainer { workload_id, env, .. } = op else { return };
    if workload_id.split('.').next() != Some(STACK) {
        return;
    }
    let hosts: Vec<&str> = f.managers.iter().map(|m| host(m)).collect();
    for v in env.values_mut() {
        *v = match v.as_str() {
            TOKEN => f.token.clone(),
            CA => f.ca_pem.clone(),
            MANAGERS => hosts.iter().map(|h| if h.contains(':') { format!("https://[{h}]:7452") } else { format!("https://{h}:7452") }).collect::<Vec<_>>().join(","),
            NAMES => hosts.join(","),
            _ => continue,
        };
    }
}

/// Where a manager forwards the UI's connections: the node's address as its worker connected from (`peer`), at
/// [`HOST_PORT`]. A worker that connected over loopback is on the same host as the manager that heard it: this one
/// (`via` none), else the manager `via` names. `port` is [`HOST_PORT`] but in tests.
pub fn target(peer: &str, via: Option<&str>, port: u32) -> Option<String> {
    let h = host(peer);
    if h.is_empty() {
        return None;
    }
    let loopback = h == "localhost" || h.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    let h = match (loopback, via) {
        (true, Some(m)) => host(m),
        (true, None) => "127.0.0.1",
        (false, _) => h,
    };
    Some(if h.contains(':') { format!("[{h}]:{port}") } else { format!("{h}:{port}") })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: &str, env: BTreeMap<String, String>) -> Op {
        Op::RunContainer { workload_id: id.into(), image: "i".into(), name: String::new(), command: vec![], env, labels: Default::default(), generation: 1, ports: vec![], volumes: vec![], limits: Default::default(), net: None, storage: vec![] }
    }

    fn filler() -> Fill {
        Fill { token: "FDAT1-t".into(), ca_pem: "PEM".into(), managers: vec!["192.0.2.1:7450".into(), "[2001:db8::2]:7450".into()] }
    }

    #[test]
    fn the_ui_s_spec_has_placeholders_a_published_port_and_no_volume() {
        let s = spec(" freightdock:cluster-ui ");
        assert_eq!(s.image, "freightdock:cluster-ui");
        assert_eq!((s.ports[0].host_port, s.ports[0].container_port), (HOST_PORT, 7443));
        assert!(s.volumes.is_empty() && s.ha, "stateless: it moves when its node fails");
        assert!(s.env.values().all(|v| v.starts_with("@freightdock:")), "no secret in the state: {:?}", s.env);
        assert_eq!(crate::state::workload_id(STACK, SERVICE, 1), WORKLOAD);
    }

    #[test]
    fn the_token_is_the_same_on_every_manager_and_another_for_another_ca() {
        let t = token("KEY A");
        assert!(t.starts_with(crate::admin::API_TOKEN_PREFIX));
        assert_eq!(t, token("KEY A"));
        assert_ne!(t, token("KEY B"));
    }

    #[test]
    fn only_the_ui_s_workload_is_filled() {
        let mut op = run(WORKLOAD, spec("i").env);
        fill(&mut op, &filler());
        let Op::RunContainer { env, .. } = &op else { unreachable!() };
        assert_eq!(env["FREIGHTDOCK_CLUSTER_TOKEN"], "FDAT1-t");
        assert_eq!(env["FREIGHTDOCK_CLUSTER_CA"], "PEM");
        assert_eq!(env["FREIGHTDOCK_CLUSTER_API"], "https://192.0.2.1:7452,https://[2001:db8::2]:7452");
        assert_eq!(env["FREIGHTDOCK_TLS_NAMES"], "192.0.2.1,2001:db8::2");

        let mut other = run("app.web.1", [("X".to_string(), TOKEN.to_string())].into());
        fill(&mut other, &filler());
        let Op::RunContainer { env, .. } = &other else { unreachable!() };
        assert_eq!(env["X"], TOKEN, "a user's stack never gets the token");
    }

    #[test]
    fn the_ui_is_reached_at_its_node_and_loopback_means_the_manager_that_heard_it() {
        assert_eq!(target("192.0.2.7:51000", None, HOST_PORT).as_deref(), Some("192.0.2.7:7453"));
        assert_eq!(target("127.0.0.1:51000", None, HOST_PORT).as_deref(), Some("127.0.0.1:7453"), "this manager's own node");
        assert_eq!(target("127.0.0.1:51000", Some("192.0.2.3:7450"), HOST_PORT).as_deref(), Some("192.0.2.3:7453"));
        assert_eq!(target("[2001:db8::9]:5", None, HOST_PORT).as_deref(), Some("[2001:db8::9]:7453"));
        assert_eq!(target("", None, HOST_PORT), None);
    }
}
