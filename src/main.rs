use anyhow::{Context, Result, bail};
use serde_json::Value;

use freightdock_manager::{Config, Manager, housekeeping, serve_admin, serve_admin_tls, serve_workers};

const USAGE: &str = "freightdock-manager [serve]                 run the manager
freightdock-manager join-token [ttl_s]      make a join token for a worker
freightdock-manager manager-token [ttl_s]   make a join token for another manager
freightdock-manager status                  the cluster, its nodes and the latest events
freightdock-manager nodes                   list the nodes and their health
freightdock-manager events [-f]             recent events; -f keeps following
freightdock-manager command <node> <json>   send a command, e.g. '{\"op\":\"ping\"}', and wait for its end
freightdock-manager remove <node>           forget a node; its certificate opens nothing more
freightdock-manager authorize <node>        let a node that came back fenced run workloads again
freightdock-manager label <node> [k=v ...]  set the node's labels (all of them; none clears them)
freightdock-manager stack deploy <name> <compose file> [options]   make or change a stack (- reads the file
                                            from stdin). Its Compose ports open nothing: expose them on the pool with stack expose.
                                            Each option left out stays as it was:
                                              --on node,...          the nodes it runs on (id, name label or hostname; any: any node)
                                              --env-file <file>      fills its ${VAR}s and its environment names with no value
                                              --description <text>   what it is for
                                              --selector k=v,k!=v    node labels its workloads need (none: no selector)
                                              --mode stateless|pinned|shared:<storage>|file   how it survives a node
                                              --name service=name    a container name of its own (again for more)
freightdock-manager stack ls                list the stacks
freightdock-manager stack ps <name>         a stack's workloads: where they run and how they are
freightdock-manager stack start|stop|restart|rm <name>
freightdock-manager stack expose <name> [service:port=address:port[/udp] ...]   what the stack exposes on the pool's
                                            floating addresses, all of it (none: nothing); the Compose file is untouched
freightdock-manager recoveries [stack]      workloads moved off fenced nodes, or left there and why
freightdock-manager api-token create <name> [read|admin]   a token for the API over TLS (shown once)
freightdock-manager api-token ls|rm <id>    list or revoke the API tokens
freightdock-manager floating ls|add|rm [address]   the address pool; add takes a range too (192.0.2.10-20, 192.0.2.8/29)
freightdock-manager storage ls                  the cluster's shared storage, the volumes on it and who holds each
freightdock-manager storage add <name> nfs <host:/export> [options]   an NFS export the workers mount themselves
freightdock-manager storage add <name> path <dir>   a directory that is the same shared storage on every node
freightdock-manager storage add <name> cephfs <mons:/path> <name=client,fs=fs> <key-file> [fence-client fence-key-file]
                                                a CephFS the workers' kernels mount; the fencing client evicts a fenced node
freightdock-manager storage keys <name> <key-file> [fence-client fence-key-file]   a CephFS's keys set anew
freightdock-manager storage rm <name>           take it away (no stack may keep a volume on it)
freightdock-manager settings                    the cluster's settings, each with where its value comes from
freightdock-manager settings set <k=v> ...      set some for the whole cluster (v default: back to each manager's own)
freightdock-manager settings proxmox <https://host:port> <fingerprint> [token-file]   the Proxmox fencing provider
freightdock-manager settings proxmox off        take it away
freightdock-manager settings redfish <user>     the Redfish fencing provider's BMC account (password on stdin or FREIGHTDOCK_REDFISH_PASSWORD)
freightdock-manager settings redfish off        take it away
freightdock-manager settings redfish test       each redfish node's BMC and power state
freightdock-manager settings redfish fingerprint <host[:port]>   the certificate a BMC shows
freightdock-manager settings backup-passphrase [off]   the passphrase the scheduled backups are sealed with
                                       (read from FREIGHTDOCK_BACKUP_PASSPHRASE or the first line of stdin)
freightdock-manager backup <file>           a backup of the cluster (its state and CA), sealed with the passphrase
freightdock-manager restore <file>          on a new manager, before its first start: bring the backed up cluster
                                       back (its CA and state); then start it as a new cluster's first manager
freightdock-manager docker <node> <action> [target] [k=v ...]   a node's Docker through its worker: info,
                                       containers, container <c>, logs <c> [tail=N], images, volumes, networks;
                                       container.start|stop|restart|remove <c>, image.remove <i>, image.pull image=<i>,
                                       images.prune, volume.remove <v>, network.remove <n> (never the cluster's own)
freightdock-manager registry ls             the private registries the cluster has a login for
freightdock-manager registry add <host> <user>   a registry's login (ghcr.io, registry.example:5000, docker.io); the
                                       password or token from FREIGHTDOCK_REGISTRY_PASSWORD or the first line of stdin
freightdock-manager registry test|rm <host> try the login set (nothing is pulled), or take it away
freightdock-manager notify ls               the notification channels, and the events the cluster tells
freightdock-manager notify set <name> <webhook|email|ntfy|gotify> [k=v ...]   a channel, set whole: url= (webhook);
                                       server= topic= priority= (ntfy, gotify); host= port= security=starttls|tls|none
                                       username= from= to=a,b (email); events=a,b or all; enabled=false. Its secret
                                       (signing key, token, SMTP password) from FREIGHTDOCK_NOTIFY_SECRET, kept if unset
freightdock-manager notify test|rm <name>   send a test through it, or take it away
freightdock-manager ca                      the cluster CA's certificate (PEM), for a client of the API over TLS
freightdock-manager ui <image> [address]|off|status   run the Freightdock UI on the cluster, at https://<address>, an
                                       address of the pool (by default the one it has, else the first free one)

A manager joins a cluster on its first start with FREIGHTDOCK_MANAGER_JOIN=<a manager's host:port> and
FREIGHTDOCK_MANAGER_JOIN_TOKEN=<a manager-token>.

The other commands talk to a running manager's API (FREIGHTDOCK_MANAGER_API_BIND) with the token in its data folder.";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // `freightdock-manager nodes | head` ends quietly when `head` has read enough, as a command-line tool should; the
    // server keeps Rust's default (ignored), so a peer that hangs up mid-write is an error, not the manager's end.
    #[cfg(unix)]
    if quiet_on_closed_pipe(args.first().map(String::as_str)) {
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        }
    }
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "freightdock_manager=info,warn".into())).init();
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
        Some("api-token") => match (args.get(1).map(String::as_str), args.get(2)) {
            (Some("create"), Some(name)) => {
                let role = args.get(3).map(String::as_str).unwrap_or("read");
                anyhow::ensure!(matches!(role, "read" | "admin"), "the role is read or admin");
                let t = call(&config, reqwest::Method::POST, "/v1/api-tokens", Some(serde_json::json!({ "name": name, "role": role }))).await?;
                eprintln!("API token {} ({role}), id {}. It is shown this once; the cluster CA is sha256 {}.", name, t["id"].as_str().unwrap_or(""), t["ca_sha256"].as_str().unwrap_or(""));
                println!("{}", t["token"].as_str().unwrap_or(""));
                Ok(())
            }
            (Some("ls"), _) => {
                let v = call(&config, reqwest::Method::GET, "/v1/api-tokens", None).await?;
                println!("{:<38} {:<24} {:<6}", "ID", "NAME", "ROLE");
                for t in v.as_array().into_iter().flatten() {
                    println!("{:<38} {:<24} {:<6}", t["id"].as_str().unwrap_or(""), t["name"].as_str().unwrap_or(""), t["role"].as_str().unwrap_or(""));
                }
                Ok(())
            }
            (Some("rm"), Some(id)) => {
                call(&config, reqwest::Method::DELETE, &format!("/v1/api-tokens/{id}"), None).await?;
                eprintln!("revoked");
                Ok(())
            }
            _ => bail!("{USAGE}"),
        },
        Some("ca") => {
            // Only read: a host with no CA gets none made by asking.
            let path = config.ca_dir().join("ca.pem");
            let pem = std::fs::read_to_string(&path).with_context(|| format!("reading {}: is this a manager's host?", path.display()))?;
            let der: rustls::pki_types::CertificateDer = rustls::pki_types::pem::PemObject::from_pem_slice(pem.as_bytes()).context("the CA's certificate")?;
            print!("{pem}");
            eprintln!("sha256 {}", freightdock_proto::sha256_hex(der.as_ref()));
            Ok(())
        }
        Some("recoveries") => {
            let path = match args.get(1) {
                Some(st) => format!("/v1/recoveries?stack={st}"),
                None => "/v1/recoveries".into(),
            };
            let v = call(&config, reqwest::Method::GET, &path, None).await?;
            let host = |h: &Value, id: &Value| match (h.as_str(), id.as_str()) {
                (Some(h), Some(n)) => format!("{h}/{}", &n[..n.len().min(8)]),
                (_, Some(n)) => n[..n.len().min(8)].to_string(),
                _ => "-".into(),
            };
            for r in v.as_array().into_iter().flatten().rev() {
                let at = r["at"].as_i64().unwrap_or(0);
                let t = time::OffsetDateTime::from_unix_timestamp_nanos(at as i128 * 1_000_000).map(|t| format!("{} {:02}:{:02}:{:02}", t.date(), t.hour(), t.minute(), t.second())).unwrap_or_default();
                let what = if r["to"].is_null() { format!("stays: {}", r["reason"].as_str().unwrap_or("")) } else { format!("to {}", host(&r["to_hostname"], &r["to"])) };
                println!("{t} UTC  {:<32} from {:<17} {what}", r["workload_id"].as_str().unwrap_or(""), host(&r["from_hostname"], &r["from"]));
            }
            Ok(())
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
        Some("authorize") => {
            let Some(node) = args.get(1) else { bail!("{USAGE}") };
            let n = call(&config, reqwest::Method::POST, &format!("/v1/nodes/{node}/authorize"), None).await?;
            println!("{}", n["fencing"]["message"].as_str().unwrap_or(""));
            Ok(())
        }
        Some("label") => {
            let Some(node) = args.get(1) else { bail!("{USAGE}") };
            let mut labels = serde_json::Map::new();
            for kv in &args[2..] {
                let (k, v) = kv.split_once('=').with_context(|| format!("{kv}: a label is key=value"))?;
                labels.insert(k.to_string(), Value::String(v.to_string()));
            }
            let n = call(&config, reqwest::Method::PUT, &format!("/v1/nodes/{node}/labels"), Some(serde_json::json!({ "labels": labels }))).await?;
            println!("{}", n["all_labels"]);
            Ok(())
        }
        Some("stack") => stack(&config, &args[1..]).await,
        Some("floating") => {
            use reqwest::Method;
            match (args.get(1).map(String::as_str), args.get(2)) {
                (Some("ls") | None, _) => {
                    let v = call(&config, Method::GET, "/v1/floating", None).await?;
                    println!("{:<18} {:<38} SINCE", "ADDRESS", "HOLDER");
                    for f in v.as_array().into_iter().flatten() {
                        println!("{:<18} {:<38} {}", f["address"].as_str().unwrap_or(""), f["holder"].as_str().unwrap_or("(waiting for a worker)"), f["since"]);
                    }
                }
                (Some("add"), Some(a)) => {
                    call(&config, Method::POST, "/v1/floating", Some(serde_json::json!({ "address": a }))).await?;
                }
                (Some("rm"), Some(a)) => {
                    call(&config, Method::DELETE, &format!("/v1/floating/{a}"), None).await?;
                }
                _ => bail!("{USAGE}"),
            }
            Ok(())
        }
        Some("settings") => {
            use reqwest::Method;
            match args.get(1).map(String::as_str) {
                None | Some("ls") => {}
                Some("set") if args.len() > 2 => {
                    let mut patch = serde_json::Map::new();
                    for kv in &args[2..] {
                        let (k, v) = kv.split_once('=').with_context(|| format!("{kv}: a setting is key=value"))?;
                        let v = match v {
                            "default" => Value::Null,
                            "on" | "true" => Value::Bool(true),
                            "off" | "false" => Value::Bool(false),
                            n => Value::from(n.parse::<u64>().with_context(|| format!("{k}: a number, on, off or default"))?),
                        };
                        patch.insert(k.to_string(), v);
                    }
                    call(&config, Method::PUT, "/v1/settings", Some(Value::Object(patch))).await?;
                }
                Some("proxmox") => {
                    let p = match (args.get(2).map(String::as_str), args.get(3), args.get(4)) {
                        (Some("off"), ..) => Value::Null,
                        (Some(url), Some(fp), file) => {
                            let mut p = serde_json::json!({ "url": url, "fingerprint": fp });
                            if let Some(f) = file {
                                p["token"] = Value::String(std::fs::read_to_string(f).with_context(|| format!("reading {f}"))?.trim().to_string());
                            }
                            p
                        }
                        _ => bail!("{USAGE}"),
                    };
                    call(&config, Method::PUT, "/v1/settings", Some(serde_json::json!({ "proxmox": p }))).await?;
                }
                Some("redfish") => match args.get(2).map(String::as_str) {
                    Some("off") => {
                        call(&config, Method::PUT, "/v1/settings", Some(serde_json::json!({ "redfish": null }))).await?;
                    }
                    Some("test") => {
                        let v = call(&config, Method::POST, "/v1/settings/redfish/test", None).await?;
                        for s in v["systems"].as_array().into_iter().flatten() {
                            let what = match s["error"].as_str() {
                                Some(e) => format!("error: {e}"),
                                None => format!("{} power {}", s["system"].as_str().unwrap_or(""), s["power"].as_str().unwrap_or("")),
                            };
                            println!("{:<16} {:<28} {what}", s["name"].as_str().unwrap_or(""), s["bmc"].as_str().unwrap_or(""));
                        }
                        return Ok(());
                    }
                    Some("fingerprint") => {
                        let host = args.get(3).with_context(|| USAGE.to_string())?;
                        let v = call(&config, Method::POST, "/v1/settings/redfish/fingerprint", Some(serde_json::json!({ "host": host }))).await?;
                        println!("{}", v["fingerprint"].as_str().unwrap_or(""));
                        return Ok(());
                    }
                    Some(user) => {
                        let password = match std::env::var("FREIGHTDOCK_REDFISH_PASSWORD") {
                            Ok(p) => p,
                            Err(_) => {
                                eprintln!("the BMC account's password (one line on stdin):");
                                let mut line = String::new();
                                std::io::stdin().read_line(&mut line)?;
                                line.trim_end_matches(['\r', '\n']).to_string()
                            }
                        };
                        call(&config, Method::PUT, "/v1/settings", Some(serde_json::json!({ "redfish": { "username": user, "password": password } }))).await?;
                    }
                    None => bail!("{USAGE}"),
                },
                Some("backup-passphrase") => {
                    let p = match args.get(2).map(String::as_str) {
                        Some("off") => Value::Null,
                        None => Value::String(passphrase()?),
                        _ => bail!("{USAGE}"),
                    };
                    call(&config, Method::PUT, "/v1/settings", Some(serde_json::json!({ "backup_passphrase": p }))).await?;
                }
                _ => bail!("{USAGE}"),
            }
            let v = call(&config, Method::GET, "/v1/settings", None).await?;
            println!("{:<20} {:<10} {:<9} DEFAULT", "SETTING", "VALUE", "FROM");
            for (k, x) in v["settings"].as_object().into_iter().flatten().filter(|(k, x)| *k != "proxmox" && *k != "redfish" && x.is_object()) {
                println!("{:<20} {:<10} {:<9} {}", k, x["value"].to_string(), x["source"].as_str().unwrap_or(""), x["default"]);
            }
            println!("backup_passphrase    {}", if v["settings"]["backup_passphrase_set"] == true { "set" } else { "not set (no scheduled backups)" });
            let r = &v["settings"]["redfish"];
            match r.is_null() {
                true => println!("redfish              not set"),
                false => println!("redfish              user {} ({}), password set", r["username"].as_str().unwrap_or(""), r["source"].as_str().unwrap_or("")),
            }
            let p = &v["settings"]["proxmox"];
            match p.is_null() {
                true => println!("proxmox              not set"),
                false => println!("proxmox              {} token {} ({}), certificate {}", p["url"].as_str().unwrap_or(""), p["token_id"].as_str().unwrap_or(""), p["source"].as_str().unwrap_or(""), p["fingerprint"].as_str().unwrap_or("")),
            }
            Ok(())
        }
        Some("docker") => {
            let (Some(node), Some(action)) = (args.get(1), args.get(2)) else { bail!("{USAGE}") };
            let mut target = String::new();
            let mut params = serde_json::Map::new();
            for a in &args[3..] {
                match a.split_once('=') {
                    Some((k, v)) => {
                        params.insert(k.to_string(), Value::String(v.to_string()));
                    }
                    None => target = a.clone(),
                }
            }
            let v = if freightdock_manager::docker::READS.contains(&action.as_str()) {
                let mut q = format!("action={}&target={}", encode(action), encode(&target));
                for (k, v) in &params {
                    q.push_str(&format!("&{}={}", encode(k), encode(v.as_str().unwrap_or(""))));
                }
                call(&config, reqwest::Method::GET, &format!("/v1/nodes/{node}/docker?{q}"), None).await?
            } else {
                call(&config, reqwest::Method::POST, &format!("/v1/nodes/{node}/docker"), Some(serde_json::json!({ "action": action, "target": target, "params": params }))).await?
            };
            if action == "logs" {
                for l in v["lines"].as_array().into_iter().flatten() {
                    println!("{}", l.as_str().unwrap_or(""));
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
            Ok(())
        }
        Some("registry") => {
            use reqwest::Method;
            match (args.get(1).map(String::as_str), args.get(2), args.get(3)) {
                (Some("ls") | None, ..) => {}
                (Some("add"), Some(host), Some(user)) => {
                    let password = match std::env::var("FREIGHTDOCK_REGISTRY_PASSWORD") {
                        Ok(p) => p,
                        Err(_) => {
                            eprintln!("the password or token for {user} at {host} (one line on stdin):");
                            let mut line = String::new();
                            std::io::stdin().read_line(&mut line)?;
                            line.trim_end_matches(['\r', '\n']).to_string()
                        }
                    };
                    call(&config, Method::PUT, &format!("/v1/registries/{host}"), Some(serde_json::json!({ "username": user, "password": password }))).await?;
                }
                (Some("test"), Some(host), _) => {
                    let v = call(&config, Method::POST, &format!("/v1/registries/{host}/test"), Some(serde_json::json!({}))).await?;
                    println!("{}", v["message"].as_str().unwrap_or(""));
                    return Ok(());
                }
                (Some("rm"), Some(host), _) => {
                    call(&config, Method::DELETE, &format!("/v1/registries/{host}"), None).await?;
                }
                _ => bail!("{USAGE}"),
            }
            let v = call(&config, Method::GET, "/v1/registries", None).await?;
            println!("{:<32} USER", "REGISTRY");
            for r in v.as_array().into_iter().flatten() {
                println!("{:<32} {}", r["host"].as_str().unwrap_or(""), r["username"].as_str().unwrap_or(""));
            }
            Ok(())
        }
        Some("notify") => {
            use reqwest::Method;
            match (args.get(1).map(String::as_str), args.get(2), args.get(3)) {
                (Some("ls") | None, ..) => {}
                (Some("set"), Some(name), Some(kind)) => {
                    let mut settings = serde_json::Map::new();
                    let mut body = serde_json::json!({ "kind": kind });
                    for kv in &args[4..] {
                        let (k, v) = kv.split_once('=').with_context(|| format!("{kv}: a setting is key=value"))?;
                        match k {
                            "events" if v == "all" => {
                                let known = call(&config, Method::GET, "/v1/notifications", None).await?;
                                body["events"] = serde_json::json!(known["events"].as_array().into_iter().flatten().filter_map(|e| e["event"].as_str()).collect::<Vec<_>>());
                            }
                            "events" => body["events"] = serde_json::json!(v.split(',').filter(|e| !e.is_empty()).collect::<Vec<_>>()),
                            "enabled" => body["enabled"] = serde_json::json!(v != "false"),
                            "to" => {
                                settings.insert(k.into(), serde_json::json!(v.split(',').collect::<Vec<_>>()));
                            }
                            "priority" | "port" => {
                                settings.insert(k.into(), serde_json::json!(v.parse::<u32>().with_context(|| format!("{k} is a number"))?));
                            }
                            _ => {
                                settings.insert(k.into(), serde_json::json!(v));
                            }
                        }
                    }
                    body["config"] = serde_json::Value::Object(settings);
                    if let Ok(secret) = std::env::var("FREIGHTDOCK_NOTIFY_SECRET") {
                        body["secret"] = serde_json::json!(secret);
                    }
                    call(&config, Method::PUT, &format!("/v1/notifications/{name}"), Some(body)).await?;
                }
                (Some("test"), Some(name), _) => {
                    let v = call(&config, Method::POST, &format!("/v1/notifications/{name}/test"), None).await?;
                    println!("{}", v["message"].as_str().unwrap_or(""));
                    return Ok(());
                }
                (Some("rm"), Some(name), _) => {
                    call(&config, Method::DELETE, &format!("/v1/notifications/{name}"), None).await?;
                }
                _ => bail!("{USAGE}"),
            }
            let v = call(&config, Method::GET, "/v1/notifications", None).await?;
            println!("{:<20} {:<8} {:<8} {:<40} LAST", "CHANNEL", "KIND", "ENABLED", "EVENTS");
            for c in v["channels"].as_array().into_iter().flatten() {
                let events: Vec<&str> = c["events"].as_array().into_iter().flatten().filter_map(|e| e.as_str()).collect();
                let last = match (c["last_error"].as_str(), c["last_sent_at"].as_i64()) {
                    (Some(e), _) => format!("failed: {e}"),
                    (None, Some(_)) => "sent".into(),
                    _ => "-".into(),
                };
                println!("{:<20} {:<8} {:<8} {:<40} {last}", c["name"].as_str().unwrap_or(""), c["kind"].as_str().unwrap_or(""), if c["enabled"] == true { "yes" } else { "no" }, events.join(","));
            }
            Ok(())
        }
        Some("backup") => {
            let file = args.get(1).context(USAGE)?;
            let bytes = call_bytes(&config, reqwest::Method::POST, "/v1/backup", Some(serde_json::json!({ "passphrase": passphrase()? }))).await?;
            freightdock_manager::ca::write_private(std::path::Path::new(file), &bytes)?;
            println!("wrote {file} ({} bytes): keep it and its passphrase apart", bytes.len());
            Ok(())
        }
        Some("restore") => {
            let file = args.get(1).context(USAGE)?;
            let bytes = std::fs::read(file).with_context(|| format!("reading {file}"))?;
            let cluster = freightdock_manager::backup::stage_restore(&config.data_dir, &bytes, &passphrase()?)?;
            println!("cluster {cluster} is ready in {}: start this manager now, without FREIGHTDOCK_MANAGER_JOIN, and point the workers' FREIGHTDOCK_MANAGERS at it", config.data_dir.display());
            Ok(())
        }
        Some("storage") => {
            use reqwest::Method;
            match (args.get(1).map(String::as_str), args.get(2), args.get(3).map(String::as_str), args.get(4)) {
                (Some("ls") | None, ..) => {
                    let v = call(&config, Method::GET, "/v1/storage", None).await?;
                    println!("{:<16} {:<6} {:<40} VOLUMES", "NAME", "DRIVER", "SOURCE");
                    for st in v.as_array().into_iter().flatten() {
                        let vols: Vec<String> = st["volumes"].as_array().into_iter().flatten().map(|v| format!("{}/{} ({}{})", v["stack"].as_str().unwrap_or(""), v["volume"].as_str().unwrap_or(""), v["access"].as_str().unwrap_or(""), v["holder"].as_str().map(|h| format!(", on {h}")).unwrap_or_default())).collect();
                        println!("{:<16} {:<6} {:<40} {}", st["name"].as_str().unwrap_or(""), st["driver"].as_str().unwrap_or(""), st["source"].as_str().unwrap_or(""), if vols.is_empty() { "-".to_string() } else { vols.join(", ") });
                        for h in st["health"].as_array().into_iter().flatten() {
                            let gib = |k: &str| h[k].as_u64().unwrap_or(0) as f64 / (1u64 << 30) as f64;
                            let said = if h["ok"] == true { format!("ok, {:.1} of {:.1} GiB free, {} ms", gib("free_bytes"), gib("total_bytes"), h["probe_ms"]) } else { format!("FAILING: {}", h["error"].as_str().unwrap_or("")) };
                            println!("{:<16} on {}: {said}", "", h["node"].as_str().unwrap_or(""));
                        }
                    }
                }
                (Some("add"), Some(name), Some(driver @ ("nfs" | "path")), Some(source)) => {
                    let options = args.get(5).cloned().unwrap_or_default();
                    call(&config, Method::POST, "/v1/storage", Some(serde_json::json!({ "name": name, "driver": driver, "source": source, "options": options }))).await?;
                }
                (Some("add"), Some(name), Some("cephfs"), Some(source)) => {
                    let (Some(options), Some(key)) = (args.get(5), args.get(6)) else { bail!("{USAGE}") };
                    let mut b = serde_json::json!({ "name": name, "driver": "cephfs", "source": source, "options": options, "secret": read_key(key)? });
                    if let (Some(client), Some(file)) = (args.get(7), args.get(8)) {
                        b["fence_client"] = client.as_str().into();
                        b["fence_secret"] = read_key(file)?.into();
                    }
                    call(&config, Method::POST, "/v1/storage", Some(b)).await?;
                }
                (Some("keys"), Some(name), Some(key), _) => {
                    let mut b = serde_json::json!({ "secret": read_key(key)? });
                    if let (Some(client), Some(file)) = (args.get(4), args.get(5)) {
                        b["fence_client"] = client.as_str().into();
                        b["fence_secret"] = read_key(file)?.into();
                    }
                    call(&config, Method::PUT, &format!("/v1/storage/{name}/keys"), Some(b)).await?;
                }
                (Some("rm"), Some(name), ..) => {
                    call(&config, Method::DELETE, &format!("/v1/storage/{name}"), None).await?;
                }
                _ => bail!("{USAGE}"),
            }
            Ok(())
        }
        Some("ui") => {
            use reqwest::Method;
            match args.get(1).map(String::as_str) {
                None => bail!("{USAGE}"),
                Some("off") => {
                    call(&config, Method::DELETE, "/v1/ui", None).await?;
                }
                Some("status") => {}
                Some(image) => {
                    call(&config, Method::PUT, "/v1/ui", Some(serde_json::json!({ "image": image, "address": args.get(2) }))).await?;
                }
            }
            let v = call(&config, Method::GET, "/v1/ui", None).await?;
            if v["enabled"] != true {
                println!("the UI is off");
            } else {
                println!("the UI runs {} at https://{}", v["image"].as_str().unwrap_or(""), v["address"].as_str().unwrap_or("?"));
                print_workloads(&v["stack"]);
            }
            Ok(())
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2)
        }
    }
}

async fn stack(config: &Config, args: &[String]) -> Result<()> {
    use reqwest::Method;
    let name = args.get(1).map(String::as_str);
    match (args.first().map(String::as_str), name) {
        (Some("deploy"), Some(name)) => {
            let Some(file) = args.get(2) else { bail!("{USAGE}") };
            let mut body = serde_json::json!({ "name": name });
            let mut names = serde_json::Map::new();
            let mut rest = args[3..].iter();
            while let Some(flag) = rest.next() {
                let Some(v) = rest.next() else { bail!("{flag} needs a value\n{USAGE}") };
                match flag.as_str() {
                    "--on" if v == "any" => body["nodes"] = serde_json::json!([]),
                    "--on" => body["nodes"] = serde_json::json!(v.split(',').collect::<Vec<_>>()),
                    "--env-file" => body["env"] = serde_json::json!(std::fs::read_to_string(v).with_context(|| format!("reading {v}"))?),
                    "--description" => body["description"] = serde_json::json!(v),
                    "--selector" if v == "none" => body["selector"] = serde_json::json!([]),
                    "--selector" => body["selector"] = serde_json::json!(v.split(',').collect::<Vec<_>>()),
                    "--mode" => {
                        body["mode"] = match v.split_once(':') {
                            _ if v == "file" => serde_json::Value::Null,
                            Some(("shared", storage)) => serde_json::json!({ "kind": "shared", "storage": storage }),
                            None if v == "stateless" || v == "pinned" => serde_json::json!({ "kind": v }),
                            _ => bail!("--mode is stateless, pinned, shared:<storage> or file"),
                        }
                    }
                    "--name" => {
                        let (svc, n) = v.split_once('=').context("--name is service=name")?;
                        names.insert(svc.to_string(), serde_json::json!(n));
                    }
                    _ => bail!("{flag}?\n{USAGE}"),
                }
            }
            if !names.is_empty() {
                body["names"] = serde_json::Value::Object(names);
            }
            let compose = if file == "-" {
                std::io::read_to_string(std::io::stdin())?
            } else {
                std::fs::read_to_string(file).with_context(|| format!("reading {file}"))?
            };
            body["compose"] = serde_json::json!(compose);
            let v = call(config, Method::POST, "/v1/stacks", Some(body)).await?;
            for w in v["warnings"].as_array().into_iter().flatten() {
                eprintln!("warning: {}", w.as_str().unwrap_or(""));
            }
            print_workloads(&v["stack"]);
            Ok(())
        }
        (Some("ls"), _) => {
            let v = call(config, Method::GET, "/v1/stacks", None).await?;
            println!("{:<24} {:<8} {:>9}  SERVICES", "STACK", "STATE", "RUNNING");
            for s in v.as_array().into_iter().flatten() {
                let w = s["workloads"].as_array().cloned().unwrap_or_default();
                let up = w.iter().filter(|w| w["status"] == "running").count();
                let services = s["services"].as_object().map(|m| m.keys().cloned().collect::<Vec<_>>().join(",")).unwrap_or_default();
                println!("{:<24} {:<8} {:>9}  {services}", s["name"].as_str().unwrap_or(""), if s["stopped"] == true { "stopped" } else { "running" }, format!("{up}/{}", w.len()));
            }
            Ok(())
        }
        (Some("ps"), Some(name)) => {
            let v = call(config, Method::GET, &format!("/v1/stacks/{name}"), None).await?;
            let on: Vec<&str> = v["nodes"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            println!("runs on: {}\n", if on.is_empty() { "any healthy node".to_string() } else { on.join(", ") });
            print_workloads(&v);
            print_reach(&v);
            let vols: Vec<String> = v["volumes"].as_object().into_iter().flatten().map(|(k, x)| format!("{k} on {} ({}{})", x["storage"].as_str().unwrap_or(""), x["access"].as_str().unwrap_or(""), x["holder"].as_str().map(|h| format!(", held by {h}")).unwrap_or_default())).collect();
            if !vols.is_empty() {
                println!("\nvolumes on shared storage: {}", vols.join(", "));
            }
            Ok(())
        }
        (Some("expose"), Some(name)) => {
            let exposures = args[2..].iter().map(|a| freightdock_manager::stacks::parse_exposure(a).with_context(|| format!("{a}: an exposure is service:port=address:port, /udp at the end for udp"))).collect::<Result<Vec<_>>>()?;
            let v = call(config, Method::PUT, &format!("/v1/stacks/{name}/exposures"), Some(serde_json::json!({ "exposures": exposures }))).await?;
            print_reach(&v);
            Ok(())
        }
        (Some(a @ ("start" | "stop" | "restart")), Some(name)) => {
            let v = call(config, Method::POST, &format!("/v1/stacks/{name}/{a}"), None).await?;
            if a == "restart" {
                eprintln!("restarting {} workload(s); {} busy, left as they are", v["restarting"], v["busy"]);
            }
            Ok(())
        }
        (Some("rm"), Some(name)) => {
            call(config, Method::DELETE, &format!("/v1/stacks/{name}"), None).await?;
            Ok(())
        }
        _ => bail!("{USAGE}"),
    }
}

fn print_workloads(s: &Value) {
    let o = &s["options"];
    if let Some(d) = o["description"].as_str().filter(|d| !d.is_empty()) {
        println!("{d}");
    }
    let mode = o["mode"]["kind"].as_str().map_or("as its file says".to_string(), |k| match o["mode"]["storage"].as_str() {
        Some(st) => format!("{k} on {st}"),
        None => k.to_string(),
    });
    let selector = o["selector"].as_array().map(|l| l.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")).filter(|l| !l.is_empty());
    if !o.is_null() {
        println!("mode: {mode}{}", selector.map(|l| format!("; only on nodes with {l}")).unwrap_or_default());
    }
    println!("{:<32} {:<16} {:<10} {:<10} LAST", "WORKLOAD", "NODE", "WANTED", "STATUS");
    for w in s["workloads"].as_array().into_iter().flatten() {
        let last = &w["last_command"];
        let last = if last.is_null() { String::new() } else { format!("{} {} {}", last["op"]["op"].as_str().unwrap_or(""), last["status"].as_str().unwrap_or(""), last["message"].as_str().unwrap_or("")) };
        let last = match w["waits_for"].as_str() {
            Some(n) => format!("waits for its old copy on {} to stop", &n[..n.len().min(8)]),
            None => last,
        };
        println!(
            "{:<32} {:<16} {:<10} {:<10} {}",
            w["id"].as_str().unwrap_or(""),
            w["hostname"].as_str().unwrap_or("-"),
            w["desired"].as_str().unwrap_or(""),
            w["status"].as_str().unwrap_or(""),
            last.trim()
        );
    }
}

fn print_nodes(v: &Value) {
    println!("{:<38} {:<16} {:<12} {:>8}  {:<22} {:<8} {:<24} LABELS", "NODE", "HOSTNAME", "HEALTH", "HEARD", "VIA", "DOCKER", "FENCING");
    for n in v.as_array().into_iter().flatten() {
        let heard = n["liveness"]["last_heard_ms_ago"].as_u64().map(|ms| format!("{:.1}s", ms as f64 / 1000.0)).unwrap_or_else(|| "-".into());
        let labels = n["all_labels"].as_object().map(|m| m.iter().map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or(""))).collect::<Vec<_>>().join(",")).unwrap_or_default();
        println!(
            "{:<38} {:<16} {:<12} {:>8}  {:<22} {:<8} {:<24} {labels}",
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
            n["report"]["docker_version"].as_str().unwrap_or("-"),
            n["fencing"]["state"].as_str().unwrap_or("-")
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
    if let Some(a) = config.api_tls_bind {
        let l = tokio::net::TcpListener::bind(a).await.with_context(|| format!("binding {a}"))?;
        tracing::info!("admin API over TLS on {a}, for API tokens (freightdock-manager api-token)");
        let m = m.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_admin_tls(m, l).await {
                tracing::error!("admin API over TLS: {e:#}");
            }
        });
    }
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

/// A query string's value: everything but letters, digits and `-._~` as %XX.
fn encode(s: &str) -> String {
    s.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") }).collect()
}

/// A backup's passphrase: FREIGHTDOCK_BACKUP_PASSPHRASE, else the first line of stdin. Never an argument (it would
/// be in the shell's history and the process list).
/// A Ceph key from a file: the bare key, or a keyring's `key = ...` line. From a file, so it is on no command line.
fn read_key(path: &str) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading the key in {path}"))?;
    let key = text.lines().find_map(|l| l.trim().strip_prefix("key").map(|r| r.trim_start().trim_start_matches('=').trim().to_string())).unwrap_or_else(|| text.trim().to_string());
    if key.is_empty() {
        bail!("{path} holds no key");
    }
    Ok(key)
}

fn passphrase() -> Result<String> {
    let p = match std::env::var("FREIGHTDOCK_BACKUP_PASSPHRASE") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("the backup passphrase (one line on stdin):");
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            line
        }
    };
    let p = p.trim_end_matches(['\r', '\n']).to_string();
    if p.chars().count() < freightdock_manager::backup::MIN_PASSPHRASE {
        bail!("a backup's passphrase is at least {} characters", freightdock_manager::backup::MIN_PASSPHRASE);
    }
    Ok(p)
}

async fn call(config: &Config, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value> {
    let bytes = call_bytes(config, method, path, body).await?;
    Ok(serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn call_bytes(config: &Config, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Vec<u8>> {
    let addr = config.api_bind.context("the admin API is off (FREIGHTDOCK_MANAGER_API_BIND=off)")?;
    let host = if addr.ip().is_unspecified() { format!("127.0.0.1:{}", addr.port()) } else { addr.to_string() };
    let token = std::fs::read_to_string(config.data_dir.join("admin.token")).with_context(|| format!("reading the admin token in {}", config.data_dir.display()))?;
    let mut req = reqwest::Client::new().request(method, format!("http://{host}{path}")).bearer_auth(token.trim());
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req.send().await.with_context(|| format!("is the manager running? (http://{host})"))?;
    let status = r.status();
    let bytes = r.bytes().await?.to_vec();
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes);
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        bail!("{status}: {}", v["error"].as_str().unwrap_or(&text));
    }
    Ok(bytes)
}

/// How each service of a stack is reached from outside the cluster: on its pool addresses, on every node's address
/// (its Compose file's ports), or not at all.
fn print_reach(stack: &Value) {
    println!("\n{:<20} REACHED FROM OUTSIDE", "SERVICE");
    for (name, svc) in stack["services"].as_object().into_iter().flatten() {
        let ways: Vec<String> = stack["exposures"].as_array().into_iter().flatten().filter(|e| e["service"] == name.as_str()).map(|e| format!("{}:{} -> {}/{}", e["address"].as_str().unwrap_or(""), e["port"], e["container_port"], e["protocol"].as_str().unwrap_or(""))).collect();
        let asked: Vec<String> = svc["ports"].as_array().into_iter().flatten().filter(|p| p["host_port"].as_u64().unwrap_or(0) != 0).map(|p| format!("{}", p["host_port"])).collect();
        let line = if !ways.is_empty() {
            ways.join(", ")
        } else if !asked.is_empty() {
            format!("no: internal only; its Compose file asks for {} (stack expose puts it on the pool)", asked.join(", "))
        } else {
            "no: internal only (reached by name inside the stack)".to_string()
        };
        println!("{:<20} {line}", name);
    }
}

/// Whether a command dies quietly on a closed pipe: every command-line one, never the server (`serve`, or none).
fn quiet_on_closed_pipe(command: Option<&str>) -> bool {
    !matches!(command, None | Some("serve"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_command_line_dies_on_a_closed_pipe_never_the_server() {
        assert!(!quiet_on_closed_pipe(None), "no command is the server");
        assert!(!quiet_on_closed_pipe(Some("serve")));
        assert!(quiet_on_closed_pipe(Some("nodes")));
    }
}
