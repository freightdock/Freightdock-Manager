//! Private registries: a login per registry, kept in the replicated state (the password set once and never shown,
//! like the Proxmox token) and filled into a pull by the manager that sends it (`RunContainer.auth`,
//! `DockerRequest.auth`), never into a command record. Which registry an image is from follows Docker's own rule.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::settings::Secret;

/// Docker Hub, as an image without a registry is from it.
pub const HUB: &str = "docker.io";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Registry {
    /// The registry as images name it: `ghcr.io`, `registry.example:5000`, `docker.io`.
    pub host: String,
    pub username: String,
    pub password: Secret,
    pub at: crate::state::Ms,
}

/// The registry an image is pulled from: the first part of its name when that has a `.` or a `:` or is `localhost`
/// (Docker's rule), else Docker Hub. `index.docker.io` and `registry-1.docker.io` are Docker Hub too.
pub fn host_of(image: &str) -> String {
    let first = image.split('/').next().unwrap_or("");
    let is_host = image.contains('/') && (first.contains('.') || first.contains(':') || first == "localhost");
    match first {
        "index.docker.io" | "registry-1.docker.io" if is_host => HUB.to_string(),
        h if is_host => h.to_ascii_lowercase(),
        _ => HUB.to_string(),
    }
}

/// A registry's name as it is kept: lower case, a host and maybe a port, nothing else.
pub fn valid_host(h: &str) -> bool {
    let (name, port) = match h.split_once(':') {
        Some((_, "")) => return false,
        Some(np) => np,
        None => (h, ""),
    };
    !name.is_empty()
        && name.len() <= 253
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        && !name.starts_with(['.', '-'])
        && (port.is_empty() || (port.len() <= 5 && port.parse::<u16>().is_ok_and(|p| p > 0)))
}

/// The login for an image's registry, as the worker takes it.
pub fn auth_for(registries: &std::collections::BTreeMap<String, Registry>, image: &str) -> Option<freightdock_proto::v1::RegistryAuth> {
    let r = registries.get(&host_of(image))?;
    Some(freightdock_proto::v1::RegistryAuth { server: r.host.clone(), username: r.username.clone(), password: r.password.0.clone() })
}

/// Where a registry's API is: Docker Hub's is not at its name.
fn api(host: &str) -> String {
    if host == HUB { "https://registry-1.docker.io".into() } else { format!("https://{host}") }
}

/// Signs in to the registry as Docker would (the v2 API: Basic, or a Bearer token from its realm) and answers how it
/// went in words. Nothing is pulled.
pub async fn test(host: &str, username: &str, password: &str) -> Result<String> {
    let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?;
    let base = api(host);
    let first = http.get(format!("{base}/v2/")).send().await.with_context(|| format!("the registry at {base}"))?;
    if first.status().is_success() {
        return Ok("the registry asks no login".into());
    }
    if first.status() != reqwest::StatusCode::UNAUTHORIZED {
        bail!("the registry at {base} answered {}", first.status());
    }
    let challenge = first.headers().get("www-authenticate").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let ok = match parse_challenge(&challenge) {
        Challenge::Basic => http.get(format!("{base}/v2/")).basic_auth(username, Some(password)).send().await?.status().is_success(),
        Challenge::Bearer { realm, service } => {
            let mut q = vec![("account", username.to_string())];
            if let Some(s) = service {
                q.push(("service", s));
            }
            http.get(&realm).query(&q).basic_auth(username, Some(password)).send().await.with_context(|| format!("the registry's sign-in at {realm}"))?.status().is_success()
        }
        Challenge::Unknown => bail!("the registry asks for a login Freightdock does not know: {challenge:?}"),
    };
    if ok { Ok(format!("signed in to {host} as {username}")) } else { bail!("{host} refused the login for {username}") }
}

#[derive(Debug, PartialEq)]
enum Challenge {
    Basic,
    Bearer { realm: String, service: Option<String> },
    Unknown,
}

/// `Bearer realm="https://auth.example/token",service="registry.example"` or `Basic realm="..."`.
fn parse_challenge(h: &str) -> Challenge {
    let (scheme, rest) = h.split_once(' ').unwrap_or((h, ""));
    let field = |k: &str| {
        rest.split(',').filter_map(|p| p.trim().split_once('=')).find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.trim_matches('"').to_string())
    };
    match scheme.to_ascii_lowercase().as_str() {
        "basic" => Challenge::Basic,
        "bearer" => match field("realm") {
            Some(realm) if realm.starts_with("https://") => Challenge::Bearer { realm, service: field("service") },
            _ => Challenge::Unknown,
        },
        _ => Challenge::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_image_s_registry_is_docker_s_rule() {
        assert_eq!(host_of("nginx"), "docker.io");
        assert_eq!(host_of("nginx:1.27"), "docker.io");
        assert_eq!(host_of("library/nginx"), "docker.io");
        assert_eq!(host_of("someone/app:1"), "docker.io", "a Hub user, not a host");
        assert_eq!(host_of("ghcr.io/a/b:2"), "ghcr.io");
        assert_eq!(host_of("Registry.Example:5000/team/app"), "registry.example:5000");
        assert_eq!(host_of("localhost/app"), "localhost");
        assert_eq!(host_of("index.docker.io/library/nginx"), "docker.io");
    }

    #[test]
    fn a_registry_s_name_is_a_host_and_maybe_a_port() {
        for ok in ["ghcr.io", "registry.example:5000", "docker.io", "localhost", "10.0.0.5:5000"] {
            assert!(valid_host(ok), "{ok}");
        }
        for bad in ["", "GHCR.io", "https://ghcr.io", "ghcr.io/x", "a b", "h:0", "h:99999", "-x.io", "h:"] {
            assert!(!valid_host(bad), "{bad}");
        }
    }

    #[test]
    fn the_login_goes_with_the_image_s_registry_only() {
        let r = Registry { host: "ghcr.io".into(), username: "u".into(), password: Secret("p".into()), at: 0 };
        let all = std::collections::BTreeMap::from([("ghcr.io".to_string(), r)]);
        let a = auth_for(&all, "ghcr.io/team/app:1").unwrap();
        assert_eq!((a.server.as_str(), a.username.as_str(), a.password.as_str()), ("ghcr.io", "u", "p"));
        assert!(auth_for(&all, "nginx").is_none());
        assert!(!format!("{all:?}").contains("\"p\""), "the password is never in a debug line");
    }

    #[test]
    fn a_registry_s_challenge_says_how_to_sign_in() {
        assert_eq!(parse_challenge(r#"Basic realm="Registry""#), Challenge::Basic);
        assert_eq!(parse_challenge(r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:x:pull""#), Challenge::Bearer { realm: "https://ghcr.io/token".into(), service: Some("ghcr.io".into()) });
        assert_eq!(parse_challenge(r#"Bearer realm="http://plain/token""#), Challenge::Unknown, "a login is never sent in the clear");
        assert_eq!(parse_challenge("Negotiate"), Challenge::Unknown);
    }
}
