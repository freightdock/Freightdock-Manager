//! Notifications: channels an administrator sets up (a webhook, e-mail over SMTP, an ntfy topic, a Gotify server), kept
//! in the replicated state, each subscribed to some of the events the cluster tells about. The Raft leader watches the
//! cluster (`Watch::look`: node health, fencing, recoveries, workload containers, failed runs, managers) and tells each
//! change once; a manager whose scheduled backup fails tells that itself. Each channel's one secret (a webhook's signing
//! key, an ntfy or Gotify token, an SMTP password) is set once and never shown, like a registry's password.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::sessions::Health;
use crate::settings::Secret;
use crate::state::{FenceState, Ms, Recovery};

/// What the cluster tells about, and how the UI says it.
pub const EVENTS: [(&str, &str); 14] = [
    ("node.down", "A node stops answering the managers"),
    ("node.back", "A node that was down answers again"),
    ("node.fenced", "A node was fenced, so its workloads may run elsewhere"),
    ("fence.failed", "Fencing a node failed: its workloads stay where they are"),
    ("node.awaiting", "A node that came back waits for a person to let it run workloads again"),
    ("workloads.moved", "Workloads were started again on other nodes"),
    ("workloads.stranded", "Workloads that could not be started anywhere else"),
    ("container.died", "A workload's container stopped on its own: it crashed, or was killed"),
    ("deploy.failed", "A workload could not be started: its image could not be pulled, or Docker refused it"),
    ("storage.unhealthy", "A node's shared storage stops answering, is not mounted, or cannot be written"),
    ("storage.back", "A node's shared storage answers again"),
    ("manager.down", "A manager stops answering the others"),
    ("manager.back", "A manager that was down answers again"),
    ("backup.failed", "A manager's scheduled backup could not be written"),
];

pub const KINDS: [&str; 4] = ["webhook", "email", "ntfy", "gotify"];

/// The same event about the same thing is told at most once in this long: a container that crashes in a loop is one
/// message, not one a minute.
pub const QUIET_MS: Ms = 600_000;
/// How long one delivery may take.
const SEND_WITHIN: Duration = Duration::from_secs(20);

/// A channel's settings. One struct for every kind; each kind reads its own fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// webhook: where to POST.
    pub url: String,
    /// ntfy and Gotify: the server (`https://ntfy.sh` for ntfy when left empty).
    pub server: String,
    /// ntfy: the topic.
    pub topic: String,
    /// ntfy (1 to 5) and Gotify (0 to 10): how loud.
    pub priority: Option<u8>,
    /// email: the SMTP server, its port, `starttls` (the default), `tls` or `none`, who signs in, from and to whom.
    pub host: String,
    pub port: Option<u16>,
    pub security: String,
    pub username: String,
    pub from: String,
    pub to: Vec<String>,
}

/// A channel as the replicated state keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Channel {
    pub name: String,
    pub kind: String,
    pub enabled: bool,
    pub events: Vec<String>,
    pub config: Config,
    /// The webhook's signing key, the ntfy or Gotify token, the SMTP password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<Secret>,
    pub at: Ms,
    /// The last delivery that went through, and why the last one did not (cleared by the next that does).
    #[serde(default)]
    pub last_sent_at: Option<Ms>,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl Channel {
    /// Whether it tells `event` now.
    pub fn wants(&self, event: &str) -> bool {
        self.enabled && (event == "test" || self.events.iter().any(|e| e == event))
    }

    /// As the API shows it: everything but the secret, and whether there is one.
    pub fn shown(&self) -> Value {
        json!({
            "name": self.name, "kind": self.kind, "enabled": self.enabled, "events": self.events, "config": self.config,
            "has_secret": self.secret.is_some(), "at": self.at, "last_sent_at": self.last_sent_at, "last_error": self.last_error,
        })
    }
}

/// A channel's name: 1 to 64 lower-case letters, digits, `-` or `_` (it is a path in the API).
pub fn valid_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

/// What the replicated state checks of a channel: its name, its kind and its events. The rest `Config::checked` does.
pub fn valid_channel(c: &Channel) -> bool {
    valid_name(&c.name) && KINDS.contains(&c.kind.as_str()) && c.events.iter().all(|e| EVENTS.iter().any(|(k, _)| k == e))
}

fn http_url(u: &str, field: &str) -> Result<String, String> {
    match reqwest::Url::parse(u.trim()) {
        Ok(p) if matches!(p.scheme(), "http" | "https") && p.host_str().is_some() => Ok(p.to_string()),
        _ => Err(format!("{field}: an address that starts http:// or https://")),
    }
}

fn mailbox(m: &str, field: &str) -> Result<lettre::message::Mailbox, String> {
    m.trim().parse().map_err(|_| format!("{field}: {:?} is not an e-mail address", m.trim()))
}

impl Config {
    /// The settings of a channel of `kind`, checked, trimmed and with their defaults; the error names the field.
    /// `has_secret`: the channel will have a secret (given now or kept).
    pub fn checked(mut self, kind: &str, has_secret: bool) -> Result<Config, String> {
        match kind {
            "webhook" => self.url = http_url(&self.url, "url")?,
            "ntfy" => {
                if self.server.trim().is_empty() {
                    self.server = "https://ntfy.sh".into();
                }
                self.server = http_url(&self.server, "server")?.trim_end_matches('/').to_string();
                self.topic = self.topic.trim().to_string();
                if self.topic.is_empty() || self.topic.len() > 64 || !self.topic.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                    return Err("topic: 1 to 64 letters, digits, - or _".into());
                }
                if self.priority.is_some_and(|p| !(1..=5).contains(&p)) {
                    return Err("priority: ntfy's is 1 to 5".into());
                }
            }
            "gotify" => {
                self.server = http_url(&self.server, "server")?.trim_end_matches('/').to_string();
                if !has_secret {
                    return Err("secret: Gotify needs an application's token".into());
                }
                if self.priority.is_some_and(|p| p > 10) {
                    return Err("priority: Gotify's is 0 to 10".into());
                }
            }
            "email" => {
                self.host = self.host.trim().to_string();
                if self.host.is_empty() || self.host.contains(['/', ' ']) {
                    return Err("host: name the SMTP server".into());
                }
                self.security = match self.security.trim() {
                    "" | "starttls" => "starttls".into(),
                    s @ ("tls" | "none") => s.into(),
                    _ => return Err("security: starttls, tls or none".into()),
                };
                mailbox(&self.from, "from")?;
                self.to = self.to.iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
                if self.to.is_empty() {
                    return Err("to: say who gets the mail".into());
                }
                for t in &self.to {
                    mailbox(t, "to")?;
                }
                self.username = self.username.trim().to_string();
            }
            _ => return Err("kind: a channel is a webhook, email, ntfy or gotify".into()),
        }
        Ok(self)
    }

    fn smtp_port(&self) -> u16 {
        self.port.unwrap_or(match self.security.as_str() {
            "tls" => 465,
            "none" => 25,
            _ => 587,
        })
    }
}

/// Something to tell.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub kind: &'static str,
    pub title: String,
    pub message: String,
    /// The node it happened on, by name.
    pub node: Option<String>,
    /// What it is about: a node's id, a workload, a manager's address. With `kind` the key of the quiet window.
    pub target: String,
}

impl Event {
    pub fn payload(&self, at: Ms) -> Value {
        json!({
            "event": self.kind, "title": self.title, "message": self.message, "node": self.node,
            "target": self.target, "at": at, "freightdock": env!("CARGO_PKG_VERSION"),
        })
    }

    /// What a test sends.
    pub fn test() -> Event {
        Event { kind: "test", title: "A test from the Freightdock cluster".into(), message: "This channel works.".into(), node: None, target: String::new() }
    }
}

/// An HTTP delivery: where, with which headers, what.
#[derive(Debug, PartialEq)]
pub struct Request {
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

/// A webhook's signature: HMAC-SHA256 of the body under the secret, in hex.
pub fn sign(secret: &str, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    hex::encode(ring::hmac::sign(&key, body).as_ref())
}

/// What to send for an HTTP kind.
pub fn http_request(kind: &str, c: &Config, secret: &str, e: &Event, at: Ms) -> Option<Request> {
    let mut headers = vec![("content-type", "application/json".to_string())];
    match kind {
        "webhook" => {
            let body = e.payload(at).to_string();
            if !secret.is_empty() {
                headers.push(("x-freightdock-signature", format!("sha256={}", sign(secret, body.as_bytes()))));
            }
            headers.push(("x-freightdock-event", e.kind.to_string()));
            Some(Request { url: c.url.clone(), headers, body })
        }
        "ntfy" => {
            // Published as JSON to the server's root, so a title in any language needs no header encoding.
            if !secret.is_empty() {
                headers.push(("authorization", format!("Bearer {secret}")));
            }
            let tag = if e.kind.ends_with(".back") || e.kind == "test" { "white_check_mark" } else { "warning" };
            let body = json!({ "topic": c.topic, "title": e.title, "message": e.message, "priority": c.priority.unwrap_or(4), "tags": [tag] }).to_string();
            Some(Request { url: format!("{}/", c.server.trim_end_matches('/')), headers, body })
        }
        "gotify" => {
            headers.push(("x-gotify-key", secret.to_string()));
            let body = json!({ "title": e.title, "message": e.message, "priority": c.priority.unwrap_or(5) }).to_string();
            Some(Request { url: format!("{}/message", c.server.trim_end_matches('/')), headers, body })
        }
        _ => None,
    }
}

/// A time in milliseconds as UTC, `2026-11-09 21:14:09 UTC`.
pub fn utc_text(at: Ms) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(at.div_euclid(1000)).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC", t.year(), u8::from(t.month()), t.day(), t.hour(), t.minute(), t.second())
}

/// The mail for an event.
pub fn mail(c: &Config, e: &Event, at: Ms) -> anyhow::Result<lettre::Message> {
    let mut b = lettre::Message::builder().from(c.from.trim().parse()?).subject(format!("Freightdock: {}", e.title));
    for t in &c.to {
        b = b.to(t.parse()?);
    }
    let mut text = format!("{}\n\n", e.message);
    if let Some(n) = &e.node {
        text.push_str(&format!("Node: {n}\n"));
    }
    if !e.target.is_empty() {
        text.push_str(&format!("About: {}\n", e.target));
    }
    text.push_str(&format!("Event: {}\nAt: {}\n", e.kind, utc_text(at)));
    Ok(b.header(lettre::message::header::ContentType::TEXT_PLAIN).body(text)?)
}

/// reqwest's error without the address it repeats, which may carry a token in its query.
fn without_url(e: &reqwest::Error) -> String {
    let mut s = format!("{e:#}");
    if let Some(u) = e.url() {
        s = s.replace(&format!(" ({u})"), "").replace(u.as_str(), "");
    }
    s
}

/// An address without its query, which may hold a token.
fn plain(u: &str) -> String {
    u.split(['?', '#']).next().unwrap_or(u).to_string()
}

/// Sends `e` through one channel; the error in words, without a token.
pub async fn deliver(ch: &Channel, e: &Event, at: Ms) -> Result<(), String> {
    let secret = ch.secret.as_ref().map(|s| s.0.as_str()).unwrap_or("");
    let c = &ch.config;
    if ch.kind == "email" {
        use lettre::AsyncTransport;
        type Smtp = lettre::AsyncSmtpTransport<lettre::Tokio1Executor>;
        let msg = mail(c, e, at).map_err(|x| format!("the mail could not be written: {x}"))?;
        let builder = match c.security.as_str() {
            "tls" => Smtp::relay(&c.host),
            "none" => Ok(Smtp::builder_dangerous(&c.host)),
            _ => Smtp::starttls_relay(&c.host),
        }
        .map_err(|x| format!("{}: {x}", c.host))?;
        let mut builder = builder.port(c.smtp_port()).timeout(Some(SEND_WITHIN));
        if !c.username.is_empty() {
            builder = builder.credentials(lettre::transport::smtp::authentication::Credentials::new(c.username.clone(), secret.to_string()));
        }
        return builder.build().send(msg).await.map(|_| ()).map_err(|x| format!("{}: {x}", c.host));
    }
    let Some(r) = http_request(&ch.kind, c, secret, e, at) else { return Err(format!("{:?} is not a kind of channel", ch.kind)) };
    let client = reqwest::Client::builder().timeout(SEND_WITHIN).build().map_err(|x| x.to_string())?;
    let mut req = client.post(&r.url).body(r.body);
    for (k, v) in r.headers {
        req = req.header(k, v);
    }
    let shown = plain(&r.url);
    let answer = req.send().await.map_err(|x| format!("{shown}: {}", without_url(&x)))?;
    if answer.status().is_success() {
        return Ok(());
    }
    let status = answer.status();
    let text: String = answer.text().await.unwrap_or_default().chars().take(200).collect();
    Err(format!("{shown} answered {status}{}", if text.trim().is_empty() { String::new() } else { format!(": {}", text.trim()) }))
}

/// Who was told what lately, so the same news is not told twice within `QUIET_MS`. In memory: a new leader may tell
/// once more.
#[derive(Default)]
pub struct Quiet {
    told: Mutex<HashMap<String, Ms>>,
}

impl Quiet {
    /// Whether `e` may be told at `at`; if so, it is counted as told.
    pub fn allow(&self, e: &Event, at: Ms) -> bool {
        let key = format!("{}|{}", e.kind, e.target);
        let mut told = self.told.lock().unwrap_or_else(|p| p.into_inner());
        told.retain(|_, t| at - *t < QUIET_MS);
        if told.contains_key(&key) {
            return false;
        }
        told.insert(key, at);
        true
    }
}

/// A node as the watch sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeView {
    pub id: String,
    pub name: String,
    pub health: Health,
    pub fence: FenceState,
    pub fence_message: String,
}

/// A container a worker reports for a workload.
#[derive(Debug, Clone, PartialEq)]
pub struct ContainerView {
    pub node_id: String,
    pub workload_id: String,
    pub container_id: String,
    pub state: String,
    pub generation: u64,
    pub exit_code: i32,
}

/// A run that failed: a `RunContainer` command a worker could not carry out.
#[derive(Debug, Clone, PartialEq)]
pub struct FailedRun {
    pub command_id: String,
    pub node_id: String,
    pub workload_id: String,
    pub image: String,
    pub message: String,
}

/// What the watch looks at, taken from the state and the leader's view of the nodes and managers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct View {
    pub nodes: Vec<NodeView>,
    /// Every manager but this one: its address and whether it answers.
    pub managers: Vec<(String, bool)>,
    pub recoveries: Vec<Recovery>,
    pub containers: Vec<ContainerView>,
    /// The workloads that should run: id to (node, generation), none of a stopped stack.
    pub wanted: HashMap<String, (String, u64)>,
    pub failed: Vec<FailedRun>,
    /// Each node's shared storage as it last said: node, storage, whether it answers, why not.
    pub storage: Vec<(String, String, bool, String)>,
}

/// What the leader saw last, to tell what changed since. The first look only records (a manager that just became
/// leader knows nothing of before).
#[derive(Default)]
pub struct Watch {
    seen: bool,
    health: HashMap<String, Health>,
    fence: HashMap<String, FenceState>,
    recovered_to: Ms,
    containers: HashMap<String, String>,
    failed: HashSet<String>,
    managers: HashMap<String, bool>,
    storage: HashMap<(String, String), bool>,
}

fn stopped(state: &str) -> bool {
    matches!(state, "exited" | "dead")
}

impl Watch {
    /// Forgets everything: this manager is no longer the leader.
    pub fn reset(&mut self) {
        *self = Watch::default();
    }

    /// What changed since the last look, to tell.
    pub fn look(&mut self, v: &View) -> Vec<Event> {
        let first = !self.seen;
        self.seen = true;
        let mut out = vec![];
        let name_of = |id: &str| v.nodes.iter().find(|n| n.id == id).map_or_else(|| id.to_string(), |n| n.name.clone());

        for n in &v.nodes {
            let before = self.health.insert(n.id.clone(), n.health);
            match (before, n.health) {
                (Some(Health::Healthy | Health::Suspect), Health::Unreachable) => out.push(Event { kind: "node.down", title: format!("{} is down", n.name), message: format!("{} stopped answering the managers.", n.name), node: Some(n.name.clone()), target: n.id.clone() }),
                (Some(Health::Unreachable), Health::Healthy) => out.push(Event { kind: "node.back", title: format!("{} is back", n.name), message: format!("{} answers the managers again.", n.name), node: Some(n.name.clone()), target: n.id.clone() }),
                _ => {}
            }
            let before = self.fence.insert(n.id.clone(), n.fence);
            if before.is_some_and(|b| b != n.fence) {
                let why = if n.fence_message.is_empty() { String::new() } else { format!(" {}", n.fence_message) };
                let told = match n.fence {
                    FenceState::Fenced => Some(("node.fenced", format!("{} was fenced", n.name), format!("{} is off, so its workloads may run elsewhere.{why}", n.name))),
                    FenceState::Failed => Some(("fence.failed", format!("Fencing {} failed", n.name), format!("Its workloads stay where they are until it is fenced or back.{why}"))),
                    FenceState::AwaitingAuthorization => Some(("node.awaiting", format!("{} waits to be let back", n.name), format!("{} came back after it was fenced; it runs no workloads until a person lets it.", n.name))),
                    _ => None,
                };
                if let Some((kind, title, message)) = told {
                    out.push(Event { kind, title, message, node: Some(n.name.clone()), target: n.id.clone() });
                }
            }
        }
        self.health.retain(|id, _| v.nodes.iter().any(|n| &n.id == id));
        self.fence.retain(|id, _| v.nodes.iter().any(|n| &n.id == id));

        let newest = v.recoveries.iter().map(|r| r.at).max().unwrap_or(0);
        if !first {
            let new: Vec<&Recovery> = v.recoveries.iter().filter(|r| r.at > self.recovered_to).collect();
            let mut froms: Vec<Option<&String>> = new.iter().map(|r| r.from.as_ref()).collect();
            froms.sort();
            froms.dedup();
            for from in froms {
                let node = from.map(|f| name_of(f));
                let place = node.clone().unwrap_or_else(|| "a removed node".into());
                let of: Vec<&&Recovery> = new.iter().filter(|r| r.from.as_ref() == from).collect();
                let moved: Vec<String> = of.iter().filter_map(|r| Some(format!("{} to {}", r.workload_id, name_of(r.to.as_ref()?)))).collect();
                let left: Vec<String> = of.iter().filter(|r| r.to.is_none()).map(|r| format!("{}: {}", r.workload_id, r.reason)).collect();
                let target = from.cloned().unwrap_or_default();
                if !moved.is_empty() {
                    out.push(Event { kind: "workloads.moved", title: format!("{} workload(s) moved from {place}", moved.len()), message: moved.join("\n"), node: node.clone(), target: target.clone() });
                }
                if !left.is_empty() {
                    out.push(Event { kind: "workloads.stranded", title: format!("{} workload(s) of {place} could not move", left.len()), message: left.join("\n"), node: node.clone(), target });
                }
            }
        }
        self.recovered_to = self.recovered_to.max(newest);

        let mut now = HashMap::new();
        for c in &v.containers {
            let before = self.containers.get(&c.container_id);
            let wanted = v.wanted.get(&c.workload_id).is_some_and(|(node, g)| *node == c.node_id && *g == c.generation);
            if before.is_some_and(|b| !stopped(b)) && stopped(&c.state) && c.exit_code != 0 && wanted {
                let node = name_of(&c.node_id);
                let why = if c.exit_code == 137 { "It was killed (exit code 137): out of memory, or a kill.".to_string() } else { format!("It exited with code {}.", c.exit_code) };
                out.push(Event { kind: "container.died", title: format!("{} died on {node}", c.workload_id), message: why, node: Some(node), target: c.workload_id.clone() });
            }
            now.insert(c.container_id.clone(), c.state.clone());
        }
        self.containers = now;

        for f in &v.failed {
            if self.failed.insert(f.command_id.clone()) && !first {
                let node = name_of(&f.node_id);
                out.push(Event { kind: "deploy.failed", title: format!("{} could not start on {node}", f.workload_id), message: format!("{} ({})", f.message, f.image), node: Some(node), target: f.workload_id.clone() });
            }
        }
        self.failed.retain(|id| v.failed.iter().any(|f| &f.command_id == id));

        for (node, name, ok, why) in &v.storage {
            let before = self.storage.insert((node.clone(), name.clone()), *ok);
            let place = name_of(node);
            let target = format!("{node}/{name}");
            match (before, ok) {
                (None | Some(true), false) => out.push(Event { kind: "storage.unhealthy", title: format!("Storage {name} fails on {place}"), message: format!("{why}. A workload with volumes on it may not run there."), node: Some(place), target }),
                (Some(false), true) => out.push(Event { kind: "storage.back", title: format!("Storage {name} answers again on {place}"), message: format!("{place} reads and writes {name} again."), node: Some(place), target }),
                _ => {}
            }
        }
        self.storage.retain(|(n, s), _| v.storage.iter().any(|(vn, vs, _, _)| vn == n && vs == s));

        for (addr, online) in &v.managers {
            match (self.managers.insert(addr.clone(), *online), online) {
                (Some(true), false) => out.push(Event { kind: "manager.down", title: format!("The manager at {addr} is down"), message: format!("The manager at {addr} stopped answering the others."), node: None, target: addr.clone() }),
                (Some(false), true) => out.push(Event { kind: "manager.back", title: format!("The manager at {addr} is back"), message: format!("The manager at {addr} answers the others again."), node: None, target: addr.clone() }),
                _ => {}
            }
        }
        self.managers.retain(|a, _| v.managers.iter().any(|(m, _)| m == a));

        if first { vec![] } else { out }
    }
}

/// The leader's watch and the quiet window, kept by each manager.
#[derive(Default)]
pub struct Notifier {
    pub quiet: Quiet,
    watch: Mutex<Watch>,
}

impl crate::Manager {
    /// What the watch looks at now.
    pub fn notify_view(&self) -> View {
        let mut v = self.store.read(|s| {
            let nodes = s.nodes.values().map(|n| NodeView { id: n.id.clone(), name: n.display_name(), health: Health::Unreachable, fence: n.fencing.state, fence_message: n.fencing.message.clone() }).collect();
            let wanted = s
                .workloads
                .values()
                .filter(|w| s.stacks.get(&w.stack).is_some_and(|st| !st.stopped))
                .filter_map(|w| Some((w.id.clone(), (w.node_id.clone()?, w.generation))))
                .collect();
            let failed = s
                .commands
                .values()
                .filter(|c| matches!(c.status, crate::state::CommandStatus::Failed | crate::state::CommandStatus::Rejected))
                .filter_map(|c| match &c.op {
                    crate::state::Op::RunContainer { workload_id, image, .. } => Some(FailedRun { command_id: c.id.clone(), node_id: c.node_id.clone(), workload_id: workload_id.clone(), image: image.clone(), message: c.message.clone() }),
                    _ => None,
                })
                .collect();
            View { nodes, recoveries: s.recoveries.iter().cloned().collect(), wanted, failed, ..Default::default() }
        });
        for n in &mut v.nodes {
            n.health = self.sessions.liveness(&n.id).health;
            for h in self.sessions.storage(&n.id).map(|r| r.storages).unwrap_or_default() {
                v.storage.push((n.id.clone(), h.name, h.ok, h.error));
            }
            for w in self.sessions.reported(&n.id).unwrap_or_default() {
                v.containers.push(ContainerView { node_id: n.id.clone(), workload_id: w.workload_id, container_id: w.container_id, state: w.state, generation: w.generation, exit_code: w.exit_code });
            }
        }
        v.managers = self.store.members().into_iter().filter(|(id, _)| *id != self.store.id).map(|(_, a)| {
            let up = self.sessions.manager_online(&a);
            (a, up)
        }).collect();
        v
    }

    /// The leader tells what changed since its last look; another manager forgets what it saw.
    pub fn notify_round(self: &std::sync::Arc<Self>) {
        if !self.store.is_leader() {
            self.notifier.watch.lock().unwrap_or_else(|p| p.into_inner()).reset();
            return;
        }
        let view = self.notify_view();
        let events = self.notifier.watch.lock().unwrap_or_else(|p| p.into_inner()).look(&view);
        for e in events {
            self.tell(e);
        }
    }

    /// Tells `e` through every enabled channel that wants it, unless it was told within the quiet window. Returns at
    /// once; the deliveries go on in the background.
    pub fn tell(self: &std::sync::Arc<Self>, e: Event) {
        let at = crate::now_ms();
        if !self.notifier.quiet.allow(&e, at) {
            return;
        }
        self.events.push("notify", None, format!("{}: {}", e.kind, e.title));
        let channels: Vec<Channel> = self.store.read(|s| s.notifications.values().filter(|c| c.wants(e.kind)).cloned().collect());
        for ch in channels {
            let (m, e) = (self.clone(), e.clone());
            tokio::spawn(async move {
                let _ = m.send_through(&ch, &e).await;
            });
        }
    }

    /// One channel's delivery, its outcome written to the state.
    pub async fn send_through(&self, ch: &Channel, e: &Event) -> Result<(), String> {
        let at = crate::now_ms();
        let r = deliver(ch, e, at).await;
        if let Err(why) = &r {
            tracing::warn!(channel = %ch.name, event = e.kind, "a notification could not be sent: {why}");
        }
        let entry = crate::state::Entry::ChannelOutcome { name: ch.name.clone(), error: r.as_ref().err().cloned(), at };
        if let Err(x) = self.store.propose(entry).await {
            tracing::debug!("the channel's outcome was not written: {x}");
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, h: Health, f: FenceState) -> NodeView {
        NodeView { id: id.into(), name: format!("{id}-host"), health: h, fence: f, fence_message: String::new() }
    }

    fn kinds(e: &[Event]) -> Vec<&str> {
        e.iter().map(|e| e.kind).collect()
    }

    #[test]
    fn the_first_look_only_records() {
        let mut w = Watch::default();
        let v = View { nodes: vec![node("a", Health::Unreachable, FenceState::Fenced)], ..Default::default() };
        assert!(w.look(&v).is_empty(), "a new leader knows nothing of before");
        assert!(w.look(&v).is_empty(), "and nothing changed");
    }

    #[test]
    fn a_node_going_and_coming_back_is_told() {
        let mut w = Watch::default();
        let mut v = View { nodes: vec![node("a", Health::Healthy, FenceState::None)], ..Default::default() };
        w.look(&v);
        v.nodes[0].health = Health::Suspect;
        assert!(w.look(&v).is_empty(), "a blip is no news");
        v.nodes[0].health = Health::Unreachable;
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["node.down"]);
        assert_eq!((e[0].node.as_deref(), e[0].target.as_str()), (Some("a-host"), "a"));
        v.nodes[0].health = Health::Healthy;
        assert_eq!(kinds(&w.look(&v)), ["node.back"]);
    }

    #[test]
    fn fencing_outcomes_are_told() {
        let mut w = Watch::default();
        let mut v = View { nodes: vec![node("a", Health::Unreachable, FenceState::InProgress)], ..Default::default() };
        w.look(&v);
        v.nodes[0].fence = FenceState::Fenced;
        assert_eq!(kinds(&w.look(&v)), ["node.fenced"]);
        v.nodes[0].fence = FenceState::AwaitingAuthorization;
        assert_eq!(kinds(&w.look(&v)), ["node.awaiting"]);
        v.nodes[0].fence = FenceState::Failed;
        v.nodes[0].fence_message = "the API refused".into();
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["fence.failed"]);
        assert!(e[0].message.contains("the API refused"));
    }

    #[test]
    fn new_recoveries_are_told_once_per_node_moved_and_stranded_apart() {
        let r = |at, w: &str, to: Option<&str>| Recovery { at, workload_id: w.into(), from: Some("a".into()), to: to.map(Into::into), reason: "pinned".into() };
        let mut w = Watch::default();
        let mut v = View { nodes: vec![node("a", Health::Unreachable, FenceState::Fenced), node("b", Health::Healthy, FenceState::None)], recoveries: vec![r(1, "old.web.1", Some("b"))], ..Default::default() };
        w.look(&v);
        v.recoveries.extend([r(5, "app.web.1", Some("b")), r(5, "app.db.1", None)]);
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["workloads.moved", "workloads.stranded"]);
        assert_eq!(e[0].message, "app.web.1 to b-host", "the old one is not told again");
        assert!(e[1].message.contains("app.db.1: pinned"));
        assert!(w.look(&v).is_empty());
    }

    #[test]
    fn a_container_that_dies_on_its_own_is_told_a_stop_or_a_job_done_is_not() {
        let c = |id: &str, state: &str, code: i32, generation: u64| ContainerView { node_id: "b".into(), workload_id: "app.web.1".into(), container_id: id.into(), state: state.into(), generation, exit_code: code };
        let mut w = Watch::default();
        let mut v = View { nodes: vec![node("b", Health::Healthy, FenceState::None)], wanted: HashMap::from([("app.web.1".to_string(), ("b".to_string(), 3))]), containers: vec![c("x", "running", 0, 3)], ..Default::default() };
        w.look(&v);
        v.containers = vec![c("x", "exited", 1, 3)];
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["container.died"]);
        assert_eq!(e[0].message, "It exited with code 1.");
        assert!(w.look(&v).is_empty(), "told once");

        v.containers = vec![c("y", "running", 0, 3)];
        w.look(&v);
        v.containers = vec![c("y", "exited", 0, 3)];
        assert!(w.look(&v).is_empty(), "exit 0 is a job done");

        v.containers = vec![c("z", "running", 0, 2)];
        w.look(&v);
        v.containers = vec![c("z", "exited", 137, 2)];
        assert!(w.look(&v).is_empty(), "an older generation's container is being replaced");

        v.containers = vec![c("q", "running", 0, 3)];
        w.look(&v);
        v.wanted.clear();
        v.containers = vec![c("q", "exited", 143, 3)];
        assert!(w.look(&v).is_empty(), "a stopped stack's container was stopped on purpose");

        v.containers = vec![c("n", "exited", 1, 3)];
        v.wanted.insert("app.web.1".into(), ("b".into(), 3));
        assert!(w.look(&v).is_empty(), "a container first seen dead is not news");
    }

    #[test]
    fn a_failed_run_is_told_once_and_a_manager_going_away() {
        let f = |id: &str| FailedRun { command_id: id.into(), node_id: "b".into(), workload_id: "app.web.1".into(), image: "ghcr.io/x/y:1".into(), message: "pull access denied".into() };
        let mut w = Watch::default();
        let mut v = View { failed: vec![f("c1")], managers: vec![("m2:7450".into(), true)], ..Default::default() };
        w.look(&v);
        v.failed.push(f("c2"));
        v.managers[0].1 = false;
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["deploy.failed", "manager.down"]);
        assert_eq!(e[0].message, "pull access denied (ghcr.io/x/y:1)");
        v.managers[0].1 = true;
        assert_eq!(kinds(&w.look(&v)), ["manager.back"]);
    }

    #[test]
    fn a_node_s_storage_failing_and_answering_again_is_told() {
        let mut w = Watch::default();
        let mut v = View { nodes: vec![node("a", Health::Healthy, FenceState::None)], storage: vec![("a".into(), "nas".into(), true, String::new())], ..Default::default() };
        w.look(&v);
        assert!(w.look(&v).is_empty());
        v.storage[0] = ("a".into(), "nas".into(), false, "nas is not mounted at /x".into());
        let e = w.look(&v);
        assert_eq!(kinds(&e), ["storage.unhealthy"]);
        assert_eq!((e[0].title.as_str(), e[0].target.as_str()), ("Storage nas fails on a-host", "a/nas"));
        assert!(e[0].message.starts_with("nas is not mounted"));
        v.storage[0].2 = true;
        assert_eq!(kinds(&w.look(&v)), ["storage.back"]);
        v.storage.push(("a".into(), "nfs2".into(), false, "write failed".into()));
        assert_eq!(kinds(&w.look(&v)), ["storage.unhealthy"], "a storage first heard failing is news after the first look");
    }

    #[test]
    fn the_same_news_is_told_once_in_the_quiet_window() {
        let q = Quiet::default();
        let e = Event { kind: "container.died", title: "t".into(), message: "m".into(), node: None, target: "app.web.1".into() };
        assert!(q.allow(&e, 0));
        assert!(!q.allow(&e, QUIET_MS - 1));
        assert!(q.allow(&Event { target: "app.web.2".into(), ..e.clone() }, 1), "another workload is other news");
        assert!(q.allow(&e, QUIET_MS));
    }

    #[test]
    fn a_channel_s_settings_are_checked_per_kind() {
        let c = |f: fn(&mut Config)| {
            let mut c = Config::default();
            f(&mut c);
            c
        };
        assert_eq!(c(|c| c.url = " https://hooks.example/x ".into()).checked("webhook", false).unwrap().url, "https://hooks.example/x");
        assert!(c(|c| c.url = "ftp://x".into()).checked("webhook", false).unwrap_err().starts_with("url:"));
        let n = c(|c| c.topic = "fd-alerts".into()).checked("ntfy", false).unwrap();
        assert_eq!(n.server, "https://ntfy.sh", "ntfy's own server by default");
        assert!(c(|c| c.topic = "a b".into()).checked("ntfy", false).is_err());
        assert!(c(|c| c.server = "https://gotify.example".into()).checked("gotify", false).unwrap_err().starts_with("secret:"));
        assert!(c(|c| c.server = "https://gotify.example".into()).checked("gotify", true).is_ok());
        let m = c(|c| {
            c.host = "smtp.example".into();
            c.from = "Cluster <cluster@example.org>".into();
            c.to = vec![" ops@example.org ".into(), "".into()];
        })
        .checked("email", false)
        .unwrap();
        assert_eq!((m.security.as_str(), m.to.len(), m.smtp_port()), ("starttls", 1, 587));
        assert!(c(|c| {
            c.host = "smtp.example".into();
            c.from = "nobody".into();
            c.to = vec!["ops@example.org".into()];
        })
        .checked("email", false)
        .unwrap_err()
        .starts_with("from:"));
        assert!(Config::default().checked("pager", false).is_err());
    }

    #[test]
    fn a_channel_s_name_and_events_are_its_own() {
        let ch = Channel { name: "ops".into(), kind: "ntfy".into(), enabled: true, events: vec!["node.down".into()], config: Config::default(), secret: Some(Secret("tok".into())), at: 1, last_sent_at: None, last_error: None };
        assert!(valid_channel(&ch));
        assert!(!valid_channel(&Channel { name: "Ops x".into(), ..ch.clone() }));
        assert!(!valid_channel(&Channel { events: vec!["node.melted".into()], ..ch.clone() }));
        assert!(ch.wants("node.down") && ch.wants("test") && !ch.wants("node.back"));
        assert!(!Channel { enabled: false, ..ch.clone() }.wants("node.down"));
        let shown = ch.shown().to_string();
        assert!(shown.contains("\"has_secret\":true") && !shown.contains("tok"), "the secret is never shown: {shown}");
    }

    #[test]
    fn a_webhook_is_signed_as_github_signs_its_hooks() {
        // RFC 4231 test case 2.
        assert_eq!(sign("Jefe", b"what do ya want for nothing?"), "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
        let e = Event { kind: "node.down", title: "w1 is down".into(), message: "m".into(), node: Some("w1".into()), target: "n1".into() };
        let c = Config { url: "https://hooks.example/x".into(), ..Default::default() };
        let r = http_request("webhook", &c, "k", &e, 7).unwrap();
        let sig = r.headers.iter().find(|(h, _)| *h == "x-freightdock-signature").unwrap().1.clone();
        assert_eq!(sig, format!("sha256={}", sign("k", r.body.as_bytes())));
        let body: Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!((body["event"].as_str(), body["node"].as_str(), body["at"].as_i64()), (Some("node.down"), Some("w1"), Some(7)));
        assert!(http_request("webhook", &c, "", &e, 7).unwrap().headers.iter().all(|(h, _)| *h != "x-freightdock-signature"), "no key, no signature");
    }

    #[test]
    fn ntfy_and_gotify_are_asked_their_own_way() {
        let e = Event::test();
        let n = http_request("ntfy", &Config { server: "https://ntfy.example".into(), topic: "t".into(), ..Default::default() }, "tok", &e, 0).unwrap();
        assert_eq!(n.url, "https://ntfy.example/");
        assert!(n.headers.contains(&("authorization", "Bearer tok".into())));
        assert_eq!(serde_json::from_str::<Value>(&n.body).unwrap()["topic"], "t");
        let g = http_request("gotify", &Config { server: "https://gotify.example".into(), priority: Some(8), ..Default::default() }, "tok", &e, 0).unwrap();
        assert_eq!(g.url, "https://gotify.example/message");
        assert!(g.headers.contains(&("x-gotify-key", "tok".into())));
        assert_eq!(serde_json::from_str::<Value>(&g.body).unwrap()["priority"], 8);
    }

    #[test]
    fn a_mail_says_what_where_and_when() {
        let c = Config { from: "cluster@example.org".into(), to: vec!["ops@example.org".into()], ..Default::default() };
        let e = Event { kind: "node.down", title: "w1 is down".into(), message: "w1 stopped answering.".into(), node: Some("w1".into()), target: "n1".into() };
        let text = String::from_utf8(mail(&c, &e, 1_762_722_849_000).unwrap().formatted()).unwrap();
        assert!(text.contains("Subject: Freightdock: w1 is down"));
        assert!(text.contains("Node: w1") && text.contains("Event: node.down") && text.contains("2025-11-09 21:14:09 UTC"), "{text}");
    }

    #[test]
    fn an_address_is_said_without_its_query() {
        assert_eq!(plain("https://hooks.example/x?token=s3cret"), "https://hooks.example/x");
    }

    #[tokio::test]
    async fn a_delivery_that_fails_says_why_without_the_token() {
        let ch = Channel { name: "h".into(), kind: "webhook".into(), enabled: true, events: vec![], config: Config { url: "http://127.0.0.1:1/x?token=s3cret".into(), ..Default::default() }, secret: None, at: 0, last_sent_at: None, last_error: None };
        let why = deliver(&ch, &Event::test(), 0).await.unwrap_err();
        assert!(why.starts_with("http://127.0.0.1:1/x") && !why.contains("s3cret"), "{why}");
    }
}
