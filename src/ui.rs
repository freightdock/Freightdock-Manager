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
//! It is reached as every stack is, on an address of the pool at [`PORT`] ([`exposure`]), wherever it runs: never on a
//! node's or a manager's own address. The address stays the UI's when it moves, so nobody has to change where they
//! open it. Its certificate names that address too.

use std::collections::BTreeMap;

use crate::state::{ClusterState, Exposure, Op, ServiceSpec};

/// The system stack's name, reserved: a deploy through the API may not use it.
pub const STACK: &str = "freightdock";
pub const SERVICE: &str = "ui";
/// The UI's one workload.
pub const WORKLOAD: &str = "freightdock.ui.1";
/// The port the UI's container listens on.
pub const CONTAINER_PORT: u16 = 7443;
/// The port it is reached at on its pool address.
pub const PORT: u16 = 443;
/// The API token's id and name in the state.
pub const TOKEN_ID: &str = "freightdock-ui";
pub const TOKEN_NAME: &str = "freightdock-ui";

const TOKEN: &str = "@freightdock:token";
const CA: &str = "@freightdock:ca";
const MANAGERS: &str = "@freightdock:managers";
const NAMES: &str = "@freightdock:names";

/// The UI's service: `image`, no published port (it is reached on the pool), and the placeholders [`fill`] fills.
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
        ports: vec![],
        volumes: vec![],
        replicas: 1,
        limits: Default::default(),
        placement: Default::default(),
        ha: true,
        restart: Default::default(),
        name: String::new(),
        start_first: false,
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
    /// The pool addresses the UI is exposed on, for its certificate.
    pub addresses: Vec<String>,
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
            NAMES => hosts.iter().copied().chain(f.addresses.iter().map(String::as_str)).collect::<Vec<_>>().join(","),
            _ => continue,
        };
    }
}

/// The UI's exposure on `address`: [`PORT`] there to its container.
pub fn exposure(address: &str) -> Exposure {
    Exposure { service: SERVICE.into(), container_port: CONTAINER_PORT, protocol: "tcp".into(), address: address.into(), port: PORT }
}

/// The pool address the UI is to be reached on: `wanted` if given, else the one it has, else the first of the pool
/// whose [`PORT`] no other stack exposes. None in the pool is an error: the UI is reached on the pool or not at all.
pub fn address_for(s: &ClusterState, wanted: Option<&str>) -> Result<String, String> {
    let taken = |a: &str| s.stacks.values().filter(|st| st.name != STACK).flat_map(|st| &st.exposures).any(|e| e.address == a && e.port == PORT && e.protocol == "tcp");
    if let Some(a) = wanted.map(str::trim).filter(|a| !a.is_empty()) {
        if !s.floating.contains_key(a) {
            return Err(format!("{a} is not in the pool (freightdock-manager floating add {a})"));
        }
        if taken(a) {
            return Err(format!("another stack exposes port {PORT} on {a}"));
        }
        return Ok(a.to_string());
    }
    if let Some(e) = s.stacks.get(STACK).and_then(|st| st.exposures.first()) {
        return Ok(e.address.clone());
    }
    s.floating.keys().find(|a| !taken(a)).cloned().ok_or_else(|| format!("no address of the pool is free at port {PORT}: add one first (freightdock-manager floating add <address>)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: &str, env: BTreeMap<String, String>) -> Op {
        Op::RunContainer { workload_id: id.into(), image: "i".into(), name: String::new(), command: vec![], env, labels: Default::default(), generation: 1, ports: vec![], volumes: vec![], limits: Default::default(), net: None, storage: vec![] }
    }

    fn filler() -> Fill {
        Fill { token: "FDAT1-t".into(), ca_pem: "PEM".into(), managers: vec!["192.0.2.1:7450".into(), "[2001:db8::2]:7450".into()], addresses: vec!["198.51.100.20".into()] }
    }

    #[test]
    fn the_ui_s_spec_has_placeholders_and_neither_a_published_port_nor_a_volume() {
        let s = spec(" freightdock:cluster-ui ");
        assert_eq!(s.image, "freightdock:cluster-ui");
        assert!(s.ports.is_empty(), "reached on the pool, never on a node's own address");
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
        assert_eq!(env["FREIGHTDOCK_TLS_NAMES"], "192.0.2.1,2001:db8::2,198.51.100.20", "and its pool address");

        let mut other = run("app.web.1", [("X".to_string(), TOKEN.to_string())].into());
        fill(&mut other, &filler());
        let Op::RunContainer { env, .. } = &other else { unreachable!() };
        assert_eq!(env["X"], TOKEN, "a user's stack never gets the token");
    }

    #[test]
    fn the_ui_gets_the_address_asked_for_else_the_one_it_has_else_the_first_free_one() {
        use crate::state::Entry;
        use crate::state::tests::{deploy, next, with_nodes};
        let mut s = with_nodes(&["n1"]);
        assert!(address_for(&s, None).unwrap_err().contains("floating add"), "no pool: not reachable at all, so refused");
        for a in ["192.0.2.10", "192.0.2.11"] {
            next(&mut s, &Entry::FloatingAdded { address: a.into(), at: 1 }).unwrap();
        }
        next(&mut s, &deploy("a", &[("web", crate::state::tests::spec("nginx", 1))], &[("a.web.1", "n1")])).unwrap();
        next(&mut s, &Entry::StackExposed { name: "a".into(), exposures: vec![Exposure { service: "web".into(), ..exposure("192.0.2.10") }], at: 2 }).unwrap();
        assert_eq!(address_for(&s, None).unwrap(), "192.0.2.11", "443 on .10 is a's");
        assert!(address_for(&s, Some("192.0.2.10")).is_err());
        assert!(address_for(&s, Some("192.0.2.99")).unwrap_err().contains("not in the pool"));
        let mut d = deploy(STACK, &[(SERVICE, spec("freightdock:x"))], &[(WORKLOAD, "n1")]);
        if let Entry::StackDeployed { exposures, .. } = &mut d {
            *exposures = Some(vec![exposure("192.0.2.11")]);
        }
        next(&mut s, &d).unwrap();
        next(&mut s, &Entry::FloatingAdded { address: "192.0.2.9".into(), at: 3 }).unwrap();
        assert_eq!(address_for(&s, None).unwrap(), "192.0.2.11", "it keeps the one it has");
    }
}
