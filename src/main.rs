use anyhow::{Context, Result, bail};
use serde_json::Value;

use freightdock_manager::{Config, Manager, housekeeping, serve_admin, serve_workers};

const USAGE: &str = "freightdock-manager [serve]                 run the manager
freightdock-manager join-token [ttl_s]      make a join token for a worker
freightdock-manager manager-token [ttl_s]   make a join token for another manager
freightdock-manager status                  the cluster, its nodes and the latest events
freightdock-manager nodes                   list the nodes and their health
freightdock-manager events [-f]             recent events; -f keeps following
freightdock-manager command <node> <json>   send a command, e.g. '{\"op\":\"ping\"}', and wait for its end
freightdock-manager remove <node>           forget a node; its certificate opens nothing more

A manager joins a cluster on its first start with FREIGHTDOCK_MANAGER_JOIN=<a manager's host:port> and
FREIGHTDOCK_MANAGER_JOIN_TOKEN=<a manager-token>.

The other commands talk to a running manager's API (FREIGHTDOCK_MANAGER_API_BIND) with the token in its data folder.";

#[tokio::main]
async fn main() -> Result<()> {
    // `freightdock-manager nodes | head` ends quietly when `head` has read enough, as a command-line tool should.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "freightdock_manager=info,warn".into())).init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = Config::from_env()?;
    match args.first().map(String::as_str) {
        None | Some("serve") => serve(config).await,
        Some("join-token") => {
            let body = serde_json::json!({ "ttl_s": args.get(1).map(|s| s.parse::<i64>()).transpose().context("ttl_s")? });
            let v = call(&config, reqwest::Method::POST, "/v1/join-tokens", Some(body)).await?;
            println!("{}", v["token"].as_str().unwrap_or_default());
            eprintln!("good until {} (unix ms); workers dial {}", v["expires_at"], v["managers"]);
            Ok(())
        }
        Some("manager-token") => {
            let body = serde_json::json!({ "ttl_s": args.get(1).map(|s| s.parse::<i64>()).transpose().context("ttl_s")? });
            let v = call(&config, reqwest::Method::POST, "/v1/manager-tokens", Some(body)).await?;
            println!("{}", v["token"].as_str().unwrap_or_default());
            eprintln!("good until {} (unix ms); the new manager joins through {}", v["expires_at"], v["join"]);
            Ok(())
        }
        Some("nodes") => {
            print_nodes(&call(&config, reqwest::Method::GET, "/v1/nodes", None).await?);
            Ok(())
        }
        Some("status") => {
            let c = call(&config, reqwest::Method::GET, "/v1/cluster", None).await?;
            println!("cluster  {}", c["cluster_id"].as_str().unwrap_or(""));
            println!("manager  {} (v{}), log at {}", c["manager_id"].as_str().unwrap_or(""), c["version"].as_str().unwrap_or(""), c["applied_index"]);
            for r in c["raft"].as_array().into_iter().flatten() {
                let role = if r["leader"] == true { "leader" } else if r["voter"] == true { "voter" } else { "learner" };
                println!("manager  {:<22} {:<8} raft {}{}", r["addr"].as_str().unwrap_or(""), role, r["raft_id"].as_str().unwrap_or(""), if r["me"] == true { "  (this one)" } else { "" });
            }
            println!();
            print_nodes(&call(&config, reqwest::Method::GET, "/v1/nodes", None).await?);
            println!();
            let ev = call(&config, reqwest::Method::GET, "/v1/events", None).await?;
            let ev = ev.as_array().cloned().unwrap_or_default();
            for e in &ev[ev.len().saturating_sub(10)..] {
                print_event(e);
            }
            Ok(())
        }
        Some("events") => {
            let follow = args.get(1).is_some_and(|a| a == "-f");
            let mut after: Option<u64> = None;
            loop {
                let path = match after {
                    Some(a) => format!("/v1/events?after={a}"),
                    None => "/v1/events".into(),
                };
                let ev = call(&config, reqwest::Method::GET, &path, None).await?;
                for e in ev.as_array().into_iter().flatten() {
                    print_event(e);
                    after = e["seq"].as_u64().or(after);
                }
                if !follow {
                    return Ok(());
                }
                after.get_or_insert(0);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
        Some("command") => {
            let (Some(node), Some(op)) = (args.get(1), args.get(2)) else { bail!("{USAGE}") };
            let body: Value = serde_json::from_str(op).context("the command is not JSON")?;
            let c = call(&config, reqwest::Method::POST, &format!("/v1/nodes/{node}/commands"), Some(body)).await?;
            let id = c["id"].as_str().unwrap_or_default().to_string();
            eprintln!("command {id}: pending");
            let mut last = String::from("pending");
            for _ in 0..600 {
                let c = call(&config, reqwest::Method::GET, &format!("/v1/commands/{id}"), None).await?;
                let st = c["status"].as_str().unwrap_or_default().to_string();
                if st != last {
                    eprintln!("command {id}: {st} {}", c["message"].as_str().unwrap_or_default());
                    last = st.clone();
                }
                if matches!(st.as_str(), "succeeded" | "failed" | "rejected" | "expired") {
                    if let Some(r) = c["result_json"].as_str().filter(|s| !s.is_empty()) {
                        println!("{r}");
                    }
                    return if st == "succeeded" { Ok(()) } else { bail!("{st}") };
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            bail!("still {last} after two minutes")
        }
        Some("remove") => {
            let Some(node) = args.get(1) else { bail!("{USAGE}") };
            call(&config, reqwest::Method::DELETE, &format!("/v1/nodes/{node}"), None).await?;
            Ok(())
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2)
        }
    }
}

fn print_nodes(v: &Value) {
    println!("{:<38} {:<16} {:<12} {:>8}  {:<22} {:<8} LABELS", "NODE", "HOSTNAME", "HEALTH", "HEARD", "VIA", "DOCKER");
    for n in v.as_array().into_iter().flatten() {
        let heard = n["liveness"]["last_heard_ms_ago"].as_u64().map(|ms| format!("{:.1}s", ms as f64 / 1000.0)).unwrap_or_else(|| "-".into());
        let labels = n["report"]["labels"].as_object().map(|m| m.iter().map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or(""))).collect::<Vec<_>>().join(",")).unwrap_or_default();
        println!(
            "{:<38} {:<16} {:<12} {:>8}  {:<22} {:<8} {labels}",
            n["id"].as_str().unwrap_or(""),
            n["hostname"].as_str().unwrap_or(""),
            n["liveness"]["health"].as_str().unwrap_or(""),
            heard,
            // Which manager hears it: this one, another, or none.
            match (n["liveness"]["via"].as_str(), n["liveness"]["last_heard_ms_ago"].is_null()) {
                (Some(via), _) => via,
                (None, false) => "this manager",
                (None, true) => "-",
            },
            n["report"]["docker_version"].as_str().unwrap_or("-")
        );
    }
}

fn print_event(e: &Value) {
    let at = e["at"].as_i64().unwrap_or(0);
    let t = time::OffsetDateTime::from_unix_timestamp_nanos(at as i128 * 1_000_000).map(|t| format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second())).unwrap_or_default();
    let node = e["node_id"].as_str().map(|n| &n[..n.len().min(8)]).unwrap_or("-");
    println!("{t} UTC  {:<16} {node:<8}  {}", e["kind"].as_str().unwrap_or(""), e["message"].as_str().unwrap_or(""));
}

async fn serve(config: Config) -> Result<()> {
    let m = Manager::open(&config).await?;
    let workers = tokio::net::TcpListener::bind(config.bind).await.with_context(|| format!("binding {}", config.bind))?;
    tracing::info!(manager = %m.manager_id, ca = %m.ca.fingerprint(), "worker API on {} (advertised as {})", config.bind, config.advertise.join(", "));
    let admin = match config.api_bind {
        Some(a) => {
            let l = tokio::net::TcpListener::bind(a).await.with_context(|| format!("binding {a}"))?;
            tracing::info!("admin API on http://{a} (token in {})", config.data_dir.join("admin.token").display());
            Some(tokio::spawn(serve_admin(m.clone(), l)))
        }
        None => None,
    };
    tokio::spawn(housekeeping(m.clone()));
    if let Some(j) = &config.join {
        tokio::spawn(m.clone().become_voter(j.addr.clone()));
    }
    tokio::select! {
        r = serve_workers(m.clone(), workers) => r,
        r = async { match admin { Some(h) => h.await?, None => std::future::pending().await } } => r,
        _ = shutdown() => {
            tracing::info!("stopping");
            m.sessions.kick_all();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            m.store.shutdown().await;
            Ok(())
        }
    }
}

/// Ctrl-C, or SIGTERM (systemd, docker stop).
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("a SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn call(config: &Config, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value> {
    let addr = config.api_bind.context("the admin API is off (FREIGHTDOCK_MANAGER_API_BIND=off)")?;
    let host = if addr.ip().is_unspecified() { format!("127.0.0.1:{}", addr.port()) } else { addr.to_string() };
    let token = std::fs::read_to_string(config.data_dir.join("admin.token")).with_context(|| format!("reading the admin token in {}", config.data_dir.display()))?;
    let mut req = reqwest::Client::new().request(method, format!("http://{host}{path}")).bearer_auth(token.trim());
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req.send().await.with_context(|| format!("is the manager running? (http://{host})"))?;
    let status = r.status();
    let text = r.text().await?;
    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    if !status.is_success() {
        bail!("{status}: {}", v["error"].as_str().unwrap_or(&text));
    }
    Ok(v)
}
