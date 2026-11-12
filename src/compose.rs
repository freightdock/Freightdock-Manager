//! Compose files, read on the manager into the services of a stack (G4.3). Only the part of Compose that maps onto
//! what a worker runs (`RunContainer`) is taken: image, command, environment, labels, ports, volumes and
//! `deploy.replicas`. A key that would change what runs and is not supported yet is refused by name, so a file never
//! deploys as something other than it says; a key that only concerns how Compose itself behaves (`restart`,
//! `depends_on`) is set aside with a warning.

use std::collections::BTreeMap;

use yaml_serde::Value;

use crate::state::{Constraint, Port, Resources, ServiceSpec, valid_service_name};

/// A Compose file is at most this long.
pub const MAX_LEN: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    pub services: BTreeMap<String, ServiceSpec>,
    /// What was set aside, for the person deploying to read.
    pub warnings: Vec<String>,
}

const TOP_IGNORED: &[&str] = &["version", "name"];
const SERVICE_IGNORED: &[&str] = &["restart", "depends_on", "healthcheck"];

pub fn parse(text: &str) -> Result<Parsed, String> {
    if text.len() > MAX_LEN {
        return Err(format!("the Compose file is over {} KiB", MAX_LEN / 1024));
    }
    let doc: Value = yaml_serde::from_str(text).map_err(|e| format!("not YAML: {e}"))?;
    let top = doc.as_mapping().ok_or("a Compose file is a mapping with services in it")?;
    let mut warnings = vec![];
    let mut services = BTreeMap::new();
    for (k, v) in top {
        let k = key(k)?;
        match k.as_str() {
            "services" => {
                let m = v.as_mapping().ok_or("services must be a mapping of names to services")?;
                for (name, svc) in m {
                    let name = key(name)?;
                    if !valid_service_name(&name) {
                        return Err(format!("service {name:?}: a name is 1 to 40 of a-z, 0-9, - and _, starting with a letter or digit"));
                    }
                    let spec = service(&name, svc, &mut warnings).map_err(|e| format!("service {name}: {e}"))?;
                    services.insert(name, spec);
                }
            }
            // Named volumes are made by Docker on first use on each node; their declaration needs nothing more until
            // a driver is set.
            "volumes" => {
                if let Some(m) = v.as_mapping() {
                    for (name, decl) in m {
                        if decl.as_mapping().is_some_and(|d| !d.is_empty()) {
                            warnings.push(format!("volume {}: its options are not used; it is a local volume on each node", key(name)?));
                        }
                    }
                }
            }
            k if k.starts_with("x-") => {}
            k if TOP_IGNORED.contains(&k) => {}
            k => return Err(format!("{k} is not supported yet")),
        }
    }
    if services.is_empty() {
        return Err("the Compose file has no services".into());
    }
    if text.contains("${") {
        warnings.push("variables (${...}) are not filled in; they are used as written".into());
    }
    Ok(Parsed { services, warnings })
}

fn key(v: &Value) -> Result<String, String> {
    v.as_str().map(str::to_string).ok_or_else(|| "a key is not a string".to_string())
}

/// A scalar as Compose reads it into a string.
fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

fn service(name: &str, v: &Value, warnings: &mut Vec<String>) -> Result<ServiceSpec, String> {
    let m = v.as_mapping().ok_or("a service is a mapping")?;
    let mut spec = ServiceSpec { image: String::new(), command: vec![], env: BTreeMap::new(), labels: BTreeMap::new(), ports: vec![], volumes: vec![], replicas: 1, limits: Resources::default(), placement: Default::default(), ha: true };
    for (k, v) in m {
        let k = key(k)?;
        match k.as_str() {
            "image" => spec.image = v.as_str().ok_or("image is a string")?.trim().to_string(),
            "command" => spec.command = command(v)?,
            "environment" => spec.env = pairs(v, "environment", warnings, name)?,
            "labels" => spec.labels = pairs(v, "labels", warnings, name)?,
            "ports" => spec.ports = v.as_sequence().ok_or("ports is a list")?.iter().map(port).collect::<Result<_, _>>()?,
            "volumes" => spec.volumes = v.as_sequence().ok_or("volumes is a list")?.iter().map(volume).collect::<Result<_, _>>()?,
            "deploy" => deploy(v, &mut spec)?,
            "x-freightdock" => freightdock(v, &mut spec)?,
            k if k.starts_with("x-") => {}
            k if SERVICE_IGNORED.contains(&k) => warnings.push(format!("service {name}: {k} is not used (the cluster starts and restarts workloads itself)")),
            "build" => return Err("build is not supported: give an image".into()),
            k => return Err(format!("{k} is not supported yet")),
        }
    }
    if spec.image.is_empty() {
        return Err("an image is needed".into());
    }
    let published: Vec<&Port> = spec.ports.iter().filter(|p| p.host_port != 0).collect();
    for (i, p) in published.iter().enumerate() {
        if published[..i].iter().any(|q| q.host_port == p.host_port && q.protocol == p.protocol) {
            return Err(format!("host port {}/{} is published twice", p.host_port, p.protocol));
        }
    }
    Ok(spec)
}

fn command(v: &Value) -> Result<Vec<String>, String> {
    match v {
        Value::String(s) => split_words(s),
        Value::Sequence(l) => l.iter().map(|x| scalar(x).ok_or_else(|| "command is a string or a list of strings".to_string())).collect(),
        Value::Null => Ok(vec![]),
        _ => Err("command is a string or a list of strings".into()),
    }
}

/// A command line split as a shell would split it into words, quotes and backslashes included; nothing is expanded.
pub fn split_words(s: &str) -> Result<Vec<String>, String> {
    let mut words = vec![];
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => cur.push(c),
                        None => return Err("command: a ' is not closed".into()),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(c @ ('"' | '\\' | '$' | '`')) => cur.push(c),
                            Some(c) => {
                                cur.push('\\');
                                cur.push(c);
                            }
                            None => return Err("command: a \" is not closed".into()),
                        },
                        Some(c) => cur.push(c),
                        None => return Err("command: a \" is not closed".into()),
                    }
                }
            }
            '\\' => {
                in_word = true;
                if let Some(c) = chars.next() {
                    cur.push(c);
                }
            }
            c if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            c => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    if in_word {
        words.push(cur);
    }
    Ok(words)
}

/// environment or labels: a mapping, or a list of KEY=VALUE.
fn pairs(v: &Value, what: &str, warnings: &mut Vec<String>, svc: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    match v {
        Value::Mapping(m) => {
            for (k, v) in m {
                let k = key(k)?;
                if v.is_null() && what == "environment" {
                    warnings.push(format!("service {svc}: {k} has no value and is left out (values are not taken from anywhere else)"));
                    continue;
                }
                out.insert(k, scalar(v).ok_or_else(|| format!("{what}: a value is not a string"))?);
            }
        }
        Value::Sequence(l) => {
            for item in l {
                let s = item.as_str().ok_or_else(|| format!("{what}: an item is not a string"))?;
                match s.split_once('=') {
                    Some((k, v)) => {
                        out.insert(k.to_string(), v.to_string());
                    }
                    None if what == "environment" => warnings.push(format!("service {svc}: {s} has no value and is left out (values are not taken from anywhere else)")),
                    None => {
                        out.insert(s.to_string(), String::new());
                    }
                }
            }
        }
        Value::Null => {}
        _ => return Err(format!("{what} is a mapping or a list")),
    }
    Ok(out)
}

fn port_number(s: &str) -> Result<u32, String> {
    match s.parse::<u32>() {
        Ok(n) if (1..=65535).contains(&n) => Ok(n),
        _ => Err(format!("{s:?} is not a port")),
    }
}

/// `"80"`, `"8080:80"`, `"127.0.0.1:8080:80"`, any with `/udp`; or the long form with target and published.
fn port(v: &Value) -> Result<Port, String> {
    if let Some(m) = v.as_mapping() {
        let get = |k: &str| m.get(Value::String(k.into())).and_then(scalar);
        let target = get("target").ok_or("a port needs a target")?;
        return Ok(Port {
            container_port: port_number(&target)?,
            host_port: get("published").filter(|p| !p.is_empty()).map(|p| port_number(&p)).transpose()?.unwrap_or(0),
            protocol: protocol(&get("protocol").unwrap_or_default())?,
            host_ip: get("host_ip").unwrap_or_default(),
        });
    }
    let s = scalar(v).ok_or("a port is a string or a mapping")?;
    let (spec, proto) = s.split_once('/').unwrap_or((&s, ""));
    if spec.contains('-') {
        return Err(format!("{s}: port ranges are not supported yet"));
    }
    let parts: Vec<&str> = spec.rsplitn(3, ':').collect();
    let (container, host, ip) = match parts.as_slice() {
        [c] => (*c, "", ""),
        [c, h] => (*c, *h, ""),
        [c, h, ip] => (*c, *h, *ip),
        _ => unreachable!("rsplitn(3) gives one to three"),
    };
    Ok(Port {
        container_port: port_number(container)?,
        host_port: if host.is_empty() { 0 } else { port_number(host)? },
        protocol: protocol(proto)?,
        host_ip: ip.trim_start_matches('[').trim_end_matches(']').to_string(),
    })
}

fn protocol(p: &str) -> Result<String, String> {
    match p {
        "" | "tcp" => Ok("tcp".into()),
        "udp" => Ok("udp".into()),
        p => Err(format!("{p}: only tcp and udp ports")),
    }
}

/// `name:/target[:ro]` or `/host:/target[:ro]`; or the long form with type, source, target and read_only.
fn volume(v: &Value) -> Result<String, String> {
    let (src, dst, ro) = if let Some(m) = v.as_mapping() {
        let get = |k: &str| m.get(Value::String(k.into())).and_then(scalar).unwrap_or_default();
        let kind = get("type");
        if !matches!(kind.as_str(), "" | "volume" | "bind") {
            return Err(format!("volumes of type {kind} are not supported yet"));
        }
        (get("source"), get("target"), get("read_only") == "true")
    } else {
        let s = v.as_str().ok_or("a volume is a string or a mapping")?;
        let parts: Vec<&str> = s.split(':').collect();
        match parts.as_slice() {
            [d] => return Err(format!("{d}: an anonymous volume would not follow its workload; name it (name:{d})")),
            [s, d] => (s.to_string(), d.to_string(), false),
            [s, d, "ro"] => (s.to_string(), d.to_string(), true),
            [s, d, "rw"] => (s.to_string(), d.to_string(), false),
            _ => return Err(format!("{s}: a volume is source:target or source:target:ro")),
        }
    };
    if src.starts_with('.') || src.starts_with('~') {
        return Err(format!("{src}: a relative path means nothing on a worker; use an absolute path or a named volume"));
    }
    if src.is_empty() || !dst.starts_with('/') {
        return Err("a volume needs a source and an absolute target".into());
    }
    if !src.starts_with('/') && !src.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)) {
        return Err(format!("{src}: a volume name is letters, digits and . _ -"));
    }
    Ok(if ro { format!("{src}:{dst}:ro") } else { format!("{src}:{dst}") })
}

fn mapping<'a>(v: &'a Value, what: &str) -> Result<&'a yaml_serde::Mapping, String> {
    v.as_mapping().ok_or_else(|| format!("{what} is a mapping"))
}

/// `deploy`: replicas, placement (constraints and preferences) and resources (limits and reservations).
fn deploy(v: &Value, spec: &mut ServiceSpec) -> Result<(), String> {
    for (k, v) in mapping(v, "deploy")? {
        match key(k)?.as_str() {
            "replicas" => spec.replicas = v.as_u64().filter(|n| (0..=100).contains(n)).ok_or("deploy.replicas is 0 to 100")? as u32,
            "placement" => {
                for (k, v) in mapping(v, "deploy.placement")? {
                    match key(k)?.as_str() {
                        "constraints" => spec.placement.constraints = v.as_sequence().ok_or("deploy.placement.constraints is a list")?.iter().map(|c| constraint(c.as_str().unwrap_or(""))).collect::<Result<_, _>>()?,
                        "preferences" => {
                            for p in v.as_sequence().ok_or("deploy.placement.preferences is a list")? {
                                let m = mapping(p, "a placement preference")?;
                                for (k, v) in m {
                                    let v = v.as_str().ok_or("a placement preference is a string")?;
                                    match key(k)?.as_str() {
                                        "spread" => spec.placement.spread.push(v.strip_prefix("node.labels.").filter(|k| !k.is_empty()).ok_or_else(|| format!("spread: {v}: only node.labels.<key>"))?.to_string()),
                                        "prefer" => spec.placement.prefer.push(constraint(v)?),
                                        k => return Err(format!("deploy.placement.preferences: {k} is not supported (spread and prefer are)")),
                                    }
                                }
                            }
                        }
                        k => return Err(format!("deploy.placement.{k} is not supported yet")),
                    }
                }
            }
            "resources" => {
                for (k, v) in mapping(v, "deploy.resources")? {
                    match key(k)?.as_str() {
                        "limits" => spec.limits = resources(v, "limits")?,
                        "reservations" => spec.placement.reserve = resources(v, "reservations")?,
                        k => return Err(format!("deploy.resources.{k} is not supported yet")),
                    }
                }
            }
            k => return Err(format!("deploy.{k} is not supported yet")),
        }
    }
    Ok(())
}

/// `x-freightdock`: what Compose has no word for. `ha: false` pins a service's workloads to their nodes (plan 5.3).
fn freightdock(v: &Value, spec: &mut ServiceSpec) -> Result<(), String> {
    for (k, v) in mapping(v, "x-freightdock")? {
        match key(k)?.as_str() {
            "ha" => spec.ha = v.as_bool().ok_or("x-freightdock.ha is true or false")?,
            k => return Err(format!("x-freightdock.{k} is not known")),
        }
    }
    Ok(())
}

/// `node.labels.zone==a`, `node.hostname != b`, as Swarm writes them.
pub fn constraint(s: &str) -> Result<Constraint, String> {
    let (field, equal, value) = if let Some((f, v)) = s.split_once("==") {
        (f, true, v)
    } else if let Some((f, v)) = s.split_once("!=") {
        (f, false, v)
    } else {
        return Err(format!("{s:?}: a constraint is field==value or field!=value"));
    };
    let field = field.trim();
    let known = matches!(field, "node.id" | "node.hostname" | "node.platform.os" | "node.platform.arch") || field.strip_prefix("node.labels.").is_some_and(|k| !k.is_empty());
    if !known {
        return Err(format!("{field}: constraints are on node.id, node.hostname, node.platform.os, node.platform.arch and node.labels.<key>"));
    }
    Ok(Constraint { field: field.to_string(), equal, value: value.trim().to_string() })
}

fn resources(v: &Value, what: &str) -> Result<Resources, String> {
    let mut r = Resources::default();
    for (k, v) in mapping(v, what)? {
        let text = scalar(v).ok_or_else(|| format!("{what}: a value is not a string or a number"))?;
        match key(k)?.as_str() {
            "cpus" => r.cpu_millis = text.trim().parse::<f64>().ok().filter(|c| *c > 0.0 && *c <= 1024.0).map(|c| (c * 1000.0).round() as u64).ok_or_else(|| format!("{what}.cpus: {text:?} is not a number of CPUs"))?,
            "memory" => r.memory_bytes = bytes(&text).ok_or_else(|| format!("{what}.memory: {text:?} is not a size like 512M or 2G"))?,
            "pids" | "devices" | "generic_resources" => return Err(format!("{what}.{}: not supported yet", key(k)?)),
            k => return Err(format!("{what}.{k} is not known")),
        }
    }
    Ok(r)
}

/// `512m`, `2G`, `1.5g`, `100k`, `4096b`, `4096`: bytes, as Compose reads them (powers of 1024).
pub fn bytes(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    let s = s.strip_suffix('b').filter(|r| r.ends_with(|c: char| "kmg".contains(c))).unwrap_or(s.as_str());
    let (num, mul) = match s.chars().last()? {
        'k' => (&s[..s.len() - 1], 1u64 << 10),
        'm' => (&s[..s.len() - 1], 1 << 20),
        'g' => (&s[..s.len() - 1], 1 << 30),
        'b' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    let n: f64 = num.trim().parse().ok()?;
    (n > 0.0).then_some((n * mul as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_service_reads_with_one_replica() {
        let p = parse("services:\n  web:\n    image: nginx:alpine\n").unwrap();
        assert_eq!(p.services["web"].image, "nginx:alpine");
        assert_eq!(p.services["web"].replicas, 1);
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn everything_supported_reads_in_both_forms() {
        let p = parse(
            r#"
version: "3.8"
x-common: &c {}
services:
  api:
    image: example/api:2
    command: ./serve --port 8080 "a b" 'c d'
    environment:
      MODE: prod
      WORKERS: 4
      DEBUG: false
    labels: ["team=web", "tier"]
    ports: ["8080:80", "127.0.0.1:9000:9000/udp", "443", {target: 81, published: 8081}]
    volumes: ["data:/var/lib/data", "/srv/conf:/etc/conf:ro", {type: bind, source: /x, target: /y, read_only: true}]
    deploy: {replicas: 3}
    restart: always
volumes:
  data:
"#,
        )
        .unwrap();
        let a = &p.services["api"];
        assert_eq!(a.command, vec!["./serve", "--port", "8080", "a b", "c d"]);
        assert_eq!(a.env["WORKERS"], "4");
        assert_eq!(a.env["DEBUG"], "false");
        assert_eq!(a.labels["tier"], "");
        assert_eq!(a.ports[0], Port { host_port: 8080, container_port: 80, protocol: "tcp".into(), host_ip: String::new() });
        assert_eq!(a.ports[1], Port { host_port: 9000, container_port: 9000, protocol: "udp".into(), host_ip: "127.0.0.1".into() });
        assert_eq!((a.ports[2].host_port, a.ports[2].container_port), (0, 443));
        assert_eq!((a.ports[3].host_port, a.ports[3].container_port), (8081, 81));
        assert_eq!(a.volumes, vec!["data:/var/lib/data", "/srv/conf:/etc/conf:ro", "/x:/y:ro"]);
        assert_eq!(a.replicas, 3);
        assert_eq!(p.warnings.len(), 1, "restart is set aside: {:?}", p.warnings);
    }

    #[test]
    fn what_would_change_what_runs_is_refused_by_name() {
        let refused = |y: &str, word: &str| {
            let e = parse(y).unwrap_err();
            assert!(e.contains(word), "{e}");
        };
        refused("services:\n  a:\n    build: .\n", "build");
        refused("services:\n  a:\n    image: x\n    network_mode: host\n", "network_mode");
        refused("services:\n  a:\n    image: x\nnetworks:\n  n: {}\n", "networks");
        refused("services:\n  a:\n    image: x\n    volumes: [./here:/x]\n", "relative");
        refused("services:\n  a:\n    image: x\n    volumes: [/anon]\n", "anonymous");
        refused("services:\n  a:\n    image: x\n    ports: [\"8000-8010:80\"]\n", "ranges");
        refused("services:\n  a:\n    image: x\n    ports: [\"80:80\", \"80:81\"]\n", "twice");
        refused("services:\n  a:\n    image: x\n    deploy: {mode: global}\n", "deploy.mode");
        refused("services:\n  a: {}\n", "image");
        refused("services:\n  A_b:\n    image: x\n", "name");
        refused("services: {}\n", "no services");
        refused("- a\n", "mapping");
    }

    #[test]
    fn an_environment_value_from_nowhere_is_left_out_and_said() {
        let p = parse("services:\n  a:\n    image: x\n    environment: [TOKEN, A=1]\n").unwrap();
        assert_eq!(p.services["a"].env.len(), 1);
        assert!(p.warnings[0].contains("TOKEN"));
    }

    #[test]
    fn placement_resources_and_ha_read_from_deploy_and_x_freightdock() {
        let p = parse(
            r#"
services:
  db:
    image: postgres:17
    deploy:
      placement:
        constraints: ["node.labels.ssd==true", "node.hostname != old"]
        preferences: [{spread: node.labels.zone}, {prefer: "node.labels.fast==yes"}]
      resources:
        limits: {cpus: "1.5", memory: 1G}
        reservations: {cpus: 0.25, memory: 256M}
    x-freightdock: {ha: false}
"#,
        )
        .unwrap();
        let d = &p.services["db"];
        assert_eq!(d.placement.constraints.iter().map(ToString::to_string).collect::<Vec<_>>(), vec!["node.labels.ssd==true", "node.hostname!=old"]);
        assert_eq!(d.placement.spread, vec!["zone"]);
        assert_eq!(d.placement.prefer[0].to_string(), "node.labels.fast==yes");
        assert_eq!(d.limits, Resources { cpu_millis: 1500, memory_bytes: 1 << 30 });
        assert_eq!(d.placement.reserve, Resources { cpu_millis: 250, memory_bytes: 256 << 20 });
        assert!(!d.ha);
        assert!(p.services.values().all(|s| s.replicas == 1));
    }

    #[test]
    fn a_bad_constraint_or_size_is_refused() {
        assert!(constraint("node.role==manager").is_err());
        assert!(constraint("node.labels.zone").is_err());
        assert!(constraint("node.labels.==a").is_err());
        assert_eq!(bytes("512M"), Some(512 << 20));
        assert_eq!(bytes("1.5gb"), Some(3 << 29));
        assert_eq!(bytes("4096"), Some(4096));
        assert_eq!(bytes("lots"), None);
        assert!(parse("services:\n  a:\n    image: x\n    x-freightdock: {fast: true}\n").is_err());
    }

    #[test]
    fn words_split_as_a_shell_would() {
        assert_eq!(split_words(r#"sh -c "echo \"hi\" \$HOME" it\'s"#).unwrap(), vec!["sh", "-c", "echo \"hi\" $HOME", "it's"]);
        assert_eq!(split_words("  a   b ").unwrap(), vec!["a", "b"]);
        assert_eq!(split_words("''").unwrap(), vec![""]);
        assert!(split_words("'open").is_err());
    }
}
