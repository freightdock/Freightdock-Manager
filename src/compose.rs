//! Compose files, read on the manager into the services of a stack (G4.3). Only the part of Compose that maps onto
//! what a worker runs (`RunContainer`) is taken: image, command, environment, labels, ports, volumes and
//! `deploy.replicas`. A key that would change what runs and is not supported yet is refused by name, so a file never
//! deploys as something other than it says; a key that only concerns how Compose itself behaves (`depends_on`,
//! `healthcheck`) is set aside with a warning. `restart` and `deploy.restart_policy` are the cluster's restart policy.

use std::collections::BTreeMap;

use yaml_serde::Value;

use crate::state::{Access, Constraint, Port, Resources, Restart, RestartWhen, ServiceSpec, StackVolume, valid_service_name};

/// A Compose file is at most this long.
pub const MAX_LEN: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    pub services: BTreeMap<String, ServiceSpec>,
    /// What was set aside, for the person deploying to read.
    pub warnings: Vec<String>,
    /// Named volumes put on the cluster's storage (`x-freightdock: {storage, access}`); the rest are local.
    pub volumes: BTreeMap<String, StackVolume>,
}

const TOP_IGNORED: &[&str] = &["version", "name"];
const SERVICE_IGNORED: &[&str] = &["depends_on", "healthcheck"];

pub fn parse(text: &str) -> Result<Parsed, String> {
    parse_with(text, &BTreeMap::new())
}

/// Reads a Compose file with its variables (`${VAR}` and the rest of Compose's forms) filled from `env`, the stack's
/// env file; an environment name with no value takes its value from there too.
pub fn parse_with(text: &str, env: &BTreeMap<String, String>) -> Result<Parsed, String> {
    if text.len() > MAX_LEN {
        return Err(format!("the Compose file is over {} KiB", MAX_LEN / 1024));
    }
    let mut doc: Value = yaml_serde::from_str(text).map_err(|e| format!("not YAML: {e}"))?;
    let mut unset = std::collections::BTreeSet::new();
    interpolate_all(&mut doc, env, &mut unset)?;
    let top = doc.as_mapping().ok_or("a Compose file is a mapping with services in it")?;
    let mut warnings = vec![];
    if !unset.is_empty() {
        warnings.push(format!("{} not set (in the env file or with a default): empty", unset.into_iter().collect::<Vec<_>>().join(", ")));
    }
    let mut services = BTreeMap::new();
    let mut volumes = BTreeMap::new();
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
                    let spec = service(&name, svc, env, &mut warnings).map_err(|e| format!("service {name}: {e}"))?;
                    services.insert(name, spec);
                }
            }
            // Named volumes are made by Docker on first use on each node, unless one is put on the cluster's storage
            // (G11); Compose's own options for them are not used.
            "volumes" => {
                if let Some(m) = v.as_mapping() {
                    for (name, decl) in m {
                        let name = key(name)?;
                        let Some(d) = decl.as_mapping() else { continue };
                        let ours = d.get("x-freightdock");
                        if let Some(x) = ours {
                            volumes.insert(name.clone(), on_storage(x).map_err(|e| format!("volume {name}: {e}"))?);
                        }
                        let other: Vec<String> = d.keys().filter_map(|k| k.as_str()).filter(|k| *k != "x-freightdock").map(str::to_string).collect();
                        if !other.is_empty() {
                            let kept = if ours.is_some() { "it is on the cluster's storage" } else { "it is a local volume on each node" };
                            warnings.push(format!("volume {name}: {} not used; {kept}", other.join(", ")));
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
    for (name, spec) in &services {
        for other in &spec.placement.apart {
            if other == name {
                return Err(format!("service {name}: x-freightdock.apart: a service is not kept apart from itself"));
            }
            if !other.contains('/') && !services.contains_key(other) {
                return Err(format!("service {name}: x-freightdock.apart: the stack has no service {other} (another stack's is <stack>/<service>)"));
            }
        }
    }
    Ok(Parsed { services, warnings, volumes })
}

/// Fills every string of the document (not its keys) in place.
fn interpolate_all(v: &mut Value, env: &BTreeMap<String, String>, unset: &mut std::collections::BTreeSet<String>) -> Result<(), String> {
    match v {
        Value::String(s) if s.contains('$') => *s = interpolate(s, env, unset)?,
        Value::Sequence(l) => l.iter_mut().try_for_each(|x| interpolate_all(x, env, unset))?,
        Value::Mapping(m) => m.iter_mut().try_for_each(|(_, x)| interpolate_all(x, env, unset))?,
        Value::Tagged(t) => interpolate_all(&mut t.value, env, unset)?,
        _ => {}
    }
    Ok(())
}

fn var_name(s: &str) -> usize {
    s.char_indices().find(|&(i, c)| !(c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))).map_or(s.len(), |(i, _)| i)
}

/// One string with Compose's variables filled: `$$` is `$`; `$VAR` and `${VAR}`; `${VAR:-x}` (x when unset or empty),
/// `${VAR-x}` (when unset), `${VAR:?msg}` and `${VAR?msg}` (refused), `${VAR:+x}` and `${VAR+x}` (x when set). An
/// unset variable with none of them is empty, and said in `unset`.
pub fn interpolate(s: &str, env: &BTreeMap<String, String>, unset: &mut std::collections::BTreeSet<String>) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        rest = &rest[i + 1..];
        if let Some(r) = rest.strip_prefix('$') {
            out.push('$');
            rest = r;
        } else if let Some(r) = rest.strip_prefix('{') {
            let end = r.find('}').ok_or_else(|| format!("{s:?}: a ${{ with no }}"))?;
            let (inner, after) = (&r[..end], &r[end + 1..]);
            let n = var_name(inner);
            let (name, op) = inner.split_at(n);
            if name.is_empty() {
                return Err(format!("{s:?}: ${{{inner}}} is not a variable"));
            }
            let val = env.get(name);
            let set_nonempty = val.is_some_and(|v| !v.is_empty());
            let piece = |p: &str| op[p.len()..].to_string();
            let filled = match op {
                "" => match val {
                    Some(v) => v.clone(),
                    None => {
                        unset.insert(name.to_string());
                        String::new()
                    }
                },
                o if o.starts_with(":-") => if set_nonempty { val.cloned().unwrap_or_default() } else { piece(":-") },
                o if o.starts_with('-') => val.cloned().unwrap_or_else(|| piece("-")),
                o if o.starts_with(":?") => if set_nonempty { val.cloned().unwrap_or_default() } else { return Err(format!("{name}: {}", piece(":?"))) },
                o if o.starts_with('?') => val.cloned().ok_or_else(|| format!("{name}: {}", piece("?")))?,
                o if o.starts_with(":+") => if set_nonempty { piece(":+") } else { String::new() },
                o if o.starts_with('+') => if val.is_some() { piece("+") } else { String::new() },
                o => return Err(format!("{s:?}: ${{{name}{o}}} is not a form Compose has")),
            };
            out.push_str(&filled);
            rest = after;
        } else {
            let n = var_name(rest);
            if n == 0 {
                out.push('$');
                continue;
            }
            let name = &rest[..n];
            match env.get(name) {
                Some(v) => out.push_str(v),
                None => {
                    unset.insert(name.to_string());
                }
            }
            rest = &rest[n..];
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// An env file: `KEY=value` lines (`export ` before is let be, quotes around the value taken off), blank lines and
/// `#` comments.
pub fn env_file(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (i, line) in text.lines().enumerate() {
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        let l = l.strip_prefix("export ").unwrap_or(l);
        let (k, v) = l.split_once('=').ok_or_else(|| format!("env file, line {}: not KEY=value", i + 1))?;
        let k = k.trim();
        if k.is_empty() || var_name(k) != k.len() {
            return Err(format!("env file, line {}: {k:?} is not a variable's name", i + 1));
        }
        let v = v.trim();
        let v = [('"', '"'), ('\'', '\'')].iter().find_map(|(a, b)| v.strip_prefix(*a).and_then(|r| r.strip_suffix(*b))).unwrap_or(v);
        out.insert(k.to_string(), v.to_string());
    }
    Ok(out)
}

/// The deploy's options over the file: names over `container_name`, the selector added to every service's
/// constraints, and the mode (stateless: every service moves, named volumes local; pinned: every service waits for
/// its node; shared: every service moves, every named volume not on storage already put on `storage`, one writer).
pub fn apply_options(p: &mut Parsed, o: &crate::state::StackOptions) -> Result<(), String> {
    use crate::state::Mode;
    for (svc, name) in &o.names {
        let spec = p.services.get_mut(svc).ok_or_else(|| format!("names: the stack has no service {svc}"))?;
        if name.is_empty() {
            continue;
        }
        if !crate::state::valid_container_name(name) {
            return Err(format!("names: {name:?} is not 1 to 63 of a-z, 0-9 and -"));
        }
        spec.name = name.clone();
    }
    for spec in p.services.values_mut() {
        for c in &o.selector {
            if !spec.placement.constraints.contains(c) {
                spec.placement.constraints.push(c.clone());
            }
        }
        match &o.mode {
            Some(Mode::Pinned) => spec.ha = false,
            Some(_) => spec.ha = true,
            None => {}
        }
    }
    match &o.mode {
        Some(Mode::Stateless) => p.volumes.clear(),
        Some(Mode::Shared { storage }) => {
            for spec in p.services.values() {
                for v in &spec.volumes {
                    let src = v.split(':').next().unwrap_or("");
                    if v.contains(':') && !src.starts_with('/') && !p.volumes.contains_key(src) {
                        p.volumes.insert(src.to_string(), StackVolume { storage: storage.clone(), access: Access::One });
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// A volume's `x-freightdock`: `storage` (a name) and `access` (`one`, the default, or `many`).
fn on_storage(x: &Value) -> Result<StackVolume, String> {
    let m = x.as_mapping().ok_or("x-freightdock is a mapping")?;
    let mut storage = None;
    let mut access = Access::One;
    for (k, v) in m {
        match key(k)?.as_str() {
            "storage" => storage = Some(v.as_str().filter(|s| !s.is_empty()).ok_or("storage is a name")?.to_string()),
            "access" => {
                access = match v.as_str() {
                    Some("one") => Access::One,
                    Some("many") => Access::Many,
                    _ => return Err("access is one or many".into()),
                }
            }
            k => return Err(format!("x-freightdock.{k} is not supported (storage, access)")),
        }
    }
    Ok(StackVolume { storage: storage.ok_or("x-freightdock needs the storage it is on")?, access })
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

fn service(name: &str, v: &Value, vars: &BTreeMap<String, String>, warnings: &mut Vec<String>) -> Result<ServiceSpec, String> {
    let m = v.as_mapping().ok_or("a service is a mapping")?;
    let mut spec = ServiceSpec { image: String::new(), command: vec![], env: BTreeMap::new(), labels: BTreeMap::new(), ports: vec![], volumes: vec![], replicas: 1, limits: Resources::default(), placement: Default::default(), ha: true, restart: Default::default(), name: String::new(), start_first: false };
    for (k, v) in m {
        let k = key(k)?;
        match k.as_str() {
            "image" => spec.image = v.as_str().ok_or("image is a string")?.trim().to_string(),
            "command" => spec.command = command(v)?,
            "environment" => spec.env = pairs(v, "environment", vars, warnings, name)?,
            "labels" => spec.labels = pairs(v, "labels", &BTreeMap::new(), warnings, name)?,
            "container_name" => {
                spec.name = v.as_str().filter(|n| crate::state::valid_container_name(n)).ok_or("container_name is 1 to 63 of a-z, 0-9 and -, starting with a letter or digit")?.to_string();
            }
            "ports" => spec.ports = v.as_sequence().ok_or("ports is a list")?.iter().map(port).collect::<Result<Vec<_>, _>>()?.into_iter().flatten().collect(),
            "volumes" => spec.volumes = v.as_sequence().ok_or("volumes is a list")?.iter().map(volume).collect::<Result<_, _>>()?,
            "restart" => spec.restart = restart(v)?,
            "deploy" => deploy(v, &mut spec, warnings, name)?,
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
fn pairs(v: &Value, what: &str, vars: &BTreeMap<String, String>, warnings: &mut Vec<String>, svc: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    match v {
        Value::Mapping(m) => {
            for (k, v) in m {
                let k = key(k)?;
                if v.is_null() && what == "environment" {
                    match vars.get(&k) {
                        Some(x) => {
                            out.insert(k, x.clone());
                        }
                        None => warnings.push(format!("service {svc}: {k} has no value and is not in the env file: left out")),
                    }
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
                    None if what == "environment" => match vars.get(s) {
                        Some(x) => {
                            out.insert(s.to_string(), x.clone());
                        }
                        None => warnings.push(format!("service {svc}: {s} has no value and is not in the env file: left out")),
                    },
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

/// A port range is at most this many ports.
const MAX_RANGE: u32 = 1000;

/// `"8000"` or `"8000-8010"`: the first and last port.
fn port_range(s: &str) -> Result<(u32, u32), String> {
    match s.split_once('-') {
        None => port_number(s).map(|p| (p, p)),
        Some((a, b)) => {
            let (a, b) = (port_number(a)?, port_number(b)?);
            if b < a {
                return Err(format!("{s}: a range goes up"));
            }
            if b - a >= MAX_RANGE {
                return Err(format!("{s}: a range is at most {MAX_RANGE} ports"));
            }
            Ok((a, b))
        }
    }
}

/// `"80"`, `"8080:80"`, `"127.0.0.1:8080:80"`, any with `/udp`, any of them a range (`"8000-8010:9000-9010"`, a host
/// range to one container port, a container range alone); or the long form with target and published. A range is
/// each port of it.
fn port(v: &Value) -> Result<Vec<Port>, String> {
    if let Some(m) = v.as_mapping() {
        let get = |k: &str| m.get(Value::String(k.into())).and_then(scalar);
        let target = get("target").ok_or("a port needs a target")?;
        return Ok(vec![Port {
            container_port: port_number(&target)?,
            host_port: get("published").filter(|p| !p.is_empty()).map(|p| port_number(&p)).transpose()?.unwrap_or(0),
            protocol: protocol(&get("protocol").unwrap_or_default())?,
            host_ip: get("host_ip").unwrap_or_default(),
        }]);
    }
    let s = scalar(v).ok_or("a port is a string or a mapping")?;
    let (spec, proto) = s.split_once('/').unwrap_or((&s, ""));
    let parts: Vec<&str> = spec.rsplitn(3, ':').collect();
    let (container, host, ip) = match parts.as_slice() {
        [c] => (*c, "", ""),
        [c, h] => (*c, *h, ""),
        [c, h, ip] => (*c, *h, *ip),
        _ => unreachable!("rsplitn(3) gives one to three"),
    };
    let (c0, c1) = port_range(container)?;
    let hosts = if host.is_empty() { None } else { Some(port_range(host)?) };
    let pairs: Vec<(u32, u32)> = match hosts {
        None => (c0..=c1).map(|c| (0, c)).collect(),
        Some((h0, h1)) if c0 == c1 => (h0..=h1).map(|h| (h, c0)).collect(),
        Some((h0, h1)) if h1 - h0 == c1 - c0 => (h0..=h1).zip(c0..=c1).collect(),
        Some(_) => return Err(format!("{s}: the host range has as many ports as the container's, or the container one")),
    };
    let protocol = protocol(proto)?;
    let host_ip = ip.trim_start_matches('[').trim_end_matches(']').to_string();
    Ok(pairs.into_iter().map(|(host_port, container_port)| Port { container_port, host_port, protocol: protocol.clone(), host_ip: host_ip.clone() }).collect())
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
fn deploy(v: &Value, spec: &mut ServiceSpec, warnings: &mut Vec<String>, svc: &str) -> Result<(), String> {
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
            "restart_policy" => {
                for (k, v) in mapping(v, "deploy.restart_policy")? {
                    let text = scalar(v).unwrap_or_default();
                    let ms = |what: &str| duration_ms(&text).ok_or_else(|| format!("deploy.restart_policy.{what}: {text:?} is not a duration (5s, 1m30s, 500ms)"));
                    match key(k)?.as_str() {
                        "condition" => {
                            spec.restart.condition = match text.as_str() {
                                "any" => RestartWhen::Any,
                                "on-failure" => RestartWhen::OnFailure,
                                "none" => RestartWhen::Never,
                                c => return Err(format!("deploy.restart_policy.condition: {c:?} is none, on-failure or any")),
                            }
                        }
                        "delay" => spec.restart.delay_ms = ms("delay")?,
                        "window" => spec.restart.window_ms = ms("window")?,
                        "max_attempts" => spec.restart.max_attempts = v.as_u64().and_then(|n| u32::try_from(n).ok()).ok_or("deploy.restart_policy.max_attempts is a number")?,
                        k => return Err(format!("deploy.restart_policy.{k} is not known")),
                    }
                }
            }
            "update_config" => {
                for (k, v) in mapping(v, "deploy.update_config")? {
                    match key(k)?.as_str() {
                        "order" => {
                            spec.start_first = match scalar(v).unwrap_or_default().as_str() {
                                "start-first" => true,
                                "stop-first" => false,
                                o => return Err(format!("deploy.update_config.order: {o:?} is stop-first or start-first")),
                            }
                        }
                        k => warnings.push(format!("service {svc}: deploy.update_config.{k} is not used (a workload is moved or made again one at a time)")),
                    }
                }
            }
            k => return Err(format!("deploy.{k} is not supported yet")),
        }
    }
    Ok(())
}

/// `restart`: `no`, `always`, `unless-stopped` (the same here: a stack stopped stays stopped), `on-failure[:N]`.
fn restart(v: &Value) -> Result<Restart, String> {
    let text = match v {
        Value::Bool(false) => "no".to_string(),
        v => scalar(v).unwrap_or_default(),
    };
    let condition = match text.split_once(':').map_or(text.as_str(), |(c, _)| c) {
        "no" => RestartWhen::Never,
        "always" | "unless-stopped" => RestartWhen::Any,
        "on-failure" => RestartWhen::OnFailure,
        _ => return Err(format!("restart: {text:?} is no, always, unless-stopped or on-failure[:N]")),
    };
    let max_attempts = match text.split_once(':') {
        Some(("on-failure", n)) => n.parse().map_err(|_| format!("restart: {n:?} is not a number of attempts"))?,
        Some(_) => return Err(format!("restart: {text:?}: only on-failure takes a number")),
        None => 0,
    };
    Ok(Restart { condition, max_attempts, ..Restart::default() })
}

/// `1h2m3s`, `90s`, `500ms`, `1.5s`: milliseconds, as Compose writes durations. A number needs its unit.
pub fn duration_ms(s: &str) -> Option<u64> {
    let mut rest = s.trim();
    if rest.is_empty() {
        return None;
    }
    let mut total = 0f64;
    while !rest.is_empty() {
        let n = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).filter(|&i| i > 0)?;
        let num: f64 = rest[..n].parse().ok()?;
        rest = &rest[n..];
        let u = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let mul = match &rest[..u] {
            "ms" => 1.0,
            "s" => 1000.0,
            "m" => 60_000.0,
            "h" => 3_600_000.0,
            _ => return None,
        };
        total += num * mul;
        rest = &rest[u..];
    }
    Some(total as u64)
}

/// `x-freightdock`: what Compose has no word for. `ha: false` pins a service's workloads to their nodes (plan 5.3).
fn freightdock(v: &Value, spec: &mut ServiceSpec) -> Result<(), String> {
    for (k, v) in mapping(v, "x-freightdock")? {
        match key(k)?.as_str() {
            "ha" => spec.ha = v.as_bool().ok_or("x-freightdock.ha is true or false")?,
            "apart" => {
                spec.placement.apart = v.as_sequence().ok_or("x-freightdock.apart is a list of services")?.iter().map(|x| x.as_str().map(str::to_string).ok_or("x-freightdock.apart is a list of services")).collect::<Result<_, _>>()?;
            }
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
    healthcheck: {test: [CMD, "true"]}
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
        assert_eq!(p.warnings.len(), 1, "healthcheck is set aside: {:?}", p.warnings);
    }

    #[test]
    fn a_move_stops_the_old_copy_first_unless_update_config_says_start_first() {
        let p = |y: &str| parse(&format!("services:\n  a:\n    image: x\n{y}"));
        assert!(!p("").unwrap().services["a"].start_first, "Docker's default: stop-first");
        assert!(p("    deploy:\n      update_config: {order: start-first}\n").unwrap().services["a"].start_first);
        assert!(!p("    deploy:\n      update_config: {order: stop-first}\n").unwrap().services["a"].start_first);
        assert!(p("    deploy:\n      update_config: {order: sideways}\n").unwrap_err().contains("order"));
        let w = p("    deploy:\n      update_config: {order: start-first, parallelism: 2}\n").unwrap();
        assert!(w.warnings.iter().any(|w| w.contains("parallelism")), "{:?}", w.warnings);
    }

    #[test]
    fn the_restart_policy_reads_from_restart_and_from_deploy_restart_policy() {
        use crate::state::{Restart, RestartWhen};
        let r = |y: &str| parse(&format!("services:\n  a:\n    image: x\n{y}")).map(|p| p.services["a"].restart);
        assert_eq!(r("").unwrap(), Restart::default());
        assert_eq!(Restart::default(), Restart { condition: RestartWhen::Any, delay_ms: 5000, max_attempts: 0, window_ms: 10_000 });
        assert_eq!(r("    restart: \"no\"\n").unwrap().condition, RestartWhen::Never);
        assert_eq!(r("    restart: no\n").unwrap().condition, RestartWhen::Never, "YAML 1.1's no is false");
        assert_eq!(r("    restart: unless-stopped\n").unwrap().condition, RestartWhen::Any);
        assert_eq!(r("    restart: on-failure:3\n").unwrap(), Restart { condition: RestartWhen::OnFailure, max_attempts: 3, ..Restart::default() });
        let d = r("    deploy:\n      restart_policy: {condition: on-failure, delay: 1m30s, max_attempts: 5, window: 500ms}\n").unwrap();
        assert_eq!(d, Restart { condition: RestartWhen::OnFailure, delay_ms: 90_000, max_attempts: 5, window_ms: 500 });
        assert!(r("    restart: sometimes\n").unwrap_err().contains("restart"));
        assert!(r("    deploy:\n      restart_policy: {delay: soon}\n").unwrap_err().contains("delay"));
    }

    #[test]
    fn variables_are_filled_from_the_env_file_as_compose_fills_them() {
        let env = env_file("# a comment\n\nTAG=1.2\nexport PORT=8080\nQUOTED=\"a b\"\nSINGLE='c d'\nEMPTY=\n").unwrap();
        assert_eq!((env["QUOTED"].as_str(), env["SINGLE"].as_str(), env["PORT"].as_str(), env["EMPTY"].as_str()), ("a b", "c d", "8080", ""));
        let y = "services:\n  a:\n    image: \"x:${TAG}\"\n    environment:\n      P: $PORT\n      D: ${MISSING:-dflt}\n      E: ${EMPTY:-was-empty}\n      F: ${EMPTY-kept}\n      G: $$HOME\n      H: ${TAG:+set}\n      TOKEN:\n    ports: [\"${PORT}:80\"]\n";
        let p = parse_with(y, &env).unwrap();
        let a = &p.services["a"];
        assert_eq!(a.image, "x:1.2");
        assert_eq!((a.env["P"].as_str(), a.env["D"].as_str(), a.env["E"].as_str(), a.env["F"].as_str(), a.env["G"].as_str(), a.env["H"].as_str()), ("8080", "dflt", "was-empty", "", "$HOME", "set"));
        assert!(!a.env.contains_key("TOKEN") && p.warnings.iter().any(|w| w.contains("TOKEN")), "{:?}", p.warnings);
        assert_eq!(a.ports[0].host_port, 8080);
        let mut with_token = env.clone();
        with_token.insert("TOKEN".into(), "t".into());
        assert_eq!(parse_with(y, &with_token).unwrap().services["a"].env["TOKEN"], "t", "a name with no value takes the env file's");
        let p = parse_with("services:\n  a:\n    image: x\n    environment: {A: \"${NOPE}\"}\n", &BTreeMap::new()).unwrap();
        assert_eq!(p.services["a"].env["A"], "");
        assert!(p.warnings.iter().any(|w| w.contains("NOPE")), "unset: empty, and said: {:?}", p.warnings);
        assert!(parse_with("services:\n  a:\n    image: \"${NOPE:?give NOPE}\"\n", &BTreeMap::new()).unwrap_err().contains("give NOPE"));
        assert!(env_file("not a line").unwrap_err().contains("line 1"));
    }

    #[test]
    fn container_name_is_the_service_s_own_name() {
        assert_eq!(parse("services:\n  a:\n    image: x\n    container_name: main-db\n").unwrap().services["a"].name, "main-db");
        assert!(parse("services:\n  a:\n    image: x\n    container_name: Main_DB\n").unwrap_err().contains("container_name"));
    }

    #[test]
    fn the_deploy_s_options_go_over_the_file() {
        use crate::state::{Mode, StackOptions};
        let y = "services:\n  db:\n    image: pg\n    volumes: [data:/d, /srv/x:/x]\n  web:\n    image: nginx\n    container_name: site\n    x-freightdock: {ha: false}\nvolumes:\n  data:\n";
        let mut p = parse(y).unwrap();
        let o = StackOptions { names: [("db".to_string(), "main-db".to_string()), ("web".to_string(), String::new())].into(), selector: vec![constraint("node.labels.zone==a").unwrap()], mode: Some(Mode::Shared { storage: "nas".into() }), ..Default::default() };
        apply_options(&mut p, &o).unwrap();
        assert_eq!((p.services["db"].name.as_str(), p.services["web"].name.as_str()), ("main-db", "site"), "an empty name keeps the file's");
        assert!(p.services.values().all(|s| s.placement.constraints.contains(&constraint("node.labels.zone==a").unwrap()) && s.ha));
        assert_eq!(p.volumes["data"], StackVolume { storage: "nas".into(), access: Access::One }, "named volumes on the storage, a bind mount left");
        assert_eq!(p.volumes.len(), 1);
        let mut p = parse(y).unwrap();
        apply_options(&mut p, &StackOptions { mode: Some(Mode::Pinned), ..Default::default() }).unwrap();
        assert!(p.services.values().all(|s| !s.ha));
        let mut p = parse(y).unwrap();
        apply_options(&mut p, &StackOptions { mode: Some(Mode::Stateless), ..Default::default() }).unwrap();
        assert!(p.services.values().all(|s| s.ha) && p.volumes.is_empty());
        for bad in [("nope", "x"), ("db", "Bad Name")] {
            let mut p = parse(y).unwrap();
            assert!(apply_options(&mut p, &StackOptions { names: [(bad.0.to_string(), bad.1.to_string())].into(), ..Default::default() }).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn services_are_kept_apart_with_x_freightdock_apart() {
        let p = parse("services:\n  a:\n    image: x\n    x-freightdock: {apart: [b, shop/db]}\n  b:\n    image: y\n").unwrap();
        assert_eq!(p.services["a"].placement.apart, vec!["b", "shop/db"]);
        assert!(parse("services:\n  a:\n    image: x\n    x-freightdock: {apart: [a]}\n").unwrap_err().contains("itself"));
        assert!(parse("services:\n  a:\n    image: x\n    x-freightdock: {apart: [nope]}\n").unwrap_err().contains("nope"));
    }

    #[test]
    fn a_port_range_is_each_port_of_it() {
        let p = parse("services:\n  a:\n    image: x\n    ports: [\"8000-8002:9000-9002/udp\", \"7000-7001\", \"127.0.0.1:6000-6001:80\"]\n").unwrap();
        let got: Vec<(u32, u32, &str)> = p.services["a"].ports.iter().map(|p| (p.host_port, p.container_port, p.protocol.as_str())).collect();
        assert_eq!(got, vec![(8000, 9000, "udp"), (8001, 9001, "udp"), (8002, 9002, "udp"), (0, 7000, "tcp"), (0, 7001, "tcp"), (6000, 80, "tcp"), (6001, 80, "tcp")], "a host range to one container port: each to it");
    }

    #[test]
    fn durations_read_as_compose_writes_them() {
        assert_eq!(duration_ms("5s"), Some(5000));
        assert_eq!(duration_ms("1h2m3s"), Some(3_723_000));
        assert_eq!(duration_ms("250ms"), Some(250));
        assert_eq!(duration_ms("1.5s"), Some(1500));
        assert_eq!(duration_ms("10"), None, "a unit is needed");
        assert_eq!(duration_ms(""), None);
    }

    #[test]
    fn a_named_volume_is_put_on_the_cluster_s_storage_for_one_writer_or_many() {
        let p = parse(
            r#"
services:
  db: {image: postgres, volumes: ["pg:/var/lib/postgresql/data"]}
  web: {image: nginx, volumes: ["site:/usr/share/nginx/html:ro", "cache:/cache"]}
volumes:
  pg:
    x-freightdock: {storage: nas}
  site:
    driver: local
    x-freightdock: {storage: nas, access: many}
  cache:
"#,
        )
        .unwrap();
        assert_eq!(p.volumes["pg"], StackVolume { storage: "nas".into(), access: Access::One }, "one writer unless it says");
        assert_eq!(p.volumes["site"].access, Access::Many);
        assert!(!p.volumes.contains_key("cache"), "a plain volume stays local to each node");
        assert!(p.warnings.iter().any(|w| w.contains("site") && w.contains("driver")), "{:?}", p.warnings);
        for (y, word) in [
            ("services: {a: {image: x}}\nvolumes: {v: {x-freightdock: {access: one}}}\n", "storage"),
            ("services: {a: {image: x}}\nvolumes: {v: {x-freightdock: {storage: nas, access: some}}}\n", "access"),
            ("services: {a: {image: x}}\nvolumes: {v: {x-freightdock: {storage: nas, size: 5}}}\n", "size"),
        ] {
            let e = parse(y).unwrap_err();
            assert!(e.contains(word), "{e}");
        }
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
        refused("services:\n  a:\n    image: x\n    ports: [\"8000-8010:80-81\"]\n", "as many");
        refused("services:\n  a:\n    image: x\n    ports: [\"90-80\"]\n", "range");
        refused("services:\n  a:\n    image: x\n    ports: [\"1-2000\"]\n", "at most");
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
