//! AZ-5 — `dtfs-cli update`: pull the latest GitHub release, verify it
//! against the in-tarball MANIFEST (sha256), atomically install the
//! target binary, and restart the matching systemd service on the local
//! host. Local-node only; cluster-wide rolling stays in
//! `scripts/dtfs-update.sh`.
//!
//! Network/IO is delegated to the same tools the deploy layer already
//! uses (`curl`, `tar`, `install`, `systemctl`) so there are no new Rust
//! deps beyond `sha2` and the download streams to disk (bounded memory,
//! AGENTS.md § A.3.2). Integrity gate is § A.3.3 / A.6: a single
//! sha256 mismatch aborts the whole update before anything is installed.

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Release asset name (fixed; produced by `scripts/dtfs-release.sh`).
const ASSET: &str = "dtfs-linux-amd64.tar.gz";

/// What to update on this host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum UpdateTarget {
    Manager,
    Data,
    /// FUSE client (`client` is accepted as an alias).
    #[value(alias = "client")]
    Fuse,
    Cli,
    All,
}

// ---- pure helpers (unit-tested) -----------------------------------------

/// Binaries installed for a target.
pub fn target_binaries(t: UpdateTarget) -> &'static [&'static str] {
    match t {
        UpdateTarget::Manager => &["dtfs-manager"],
        UpdateTarget::Data => &["dtfs-data"],
        UpdateTarget::Fuse => &["dtfs-fuse"],
        UpdateTarget::Cli => &["dtfs-cli"],
        UpdateTarget::All => &["dtfs-manager", "dtfs-data", "dtfs-fuse", "dtfs-cli"],
    }
}

/// systemd unit to restart for a binary. `dtfs-fuse@*` is a glob the
/// runtime expands to the running instances; `dtfs-cli` has no service.
pub fn service_for_binary(bin: &str) -> Option<&'static str> {
    match bin {
        "dtfs-manager" => Some("dtfs-manager.service"),
        "dtfs-data" => Some("dtfs-data.service"),
        "dtfs-fuse" => Some("dtfs-fuse@*"),
        _ => None,
    }
}

/// Release tarball URL. `tag = None` → the `latest` release.
pub fn tarball_url(repo_url: &str, tag: Option<&str>) -> String {
    let base = repo_url.trim_end_matches('/');
    match tag {
        None => format!("{base}/releases/latest/download/{ASSET}"),
        Some(t) => format!("{base}/releases/download/{t}/{ASSET}"),
    }
}

/// Parse `(owner, repo)` from a github.com URL; `None` if not github.com.
pub fn parse_owner_repo(repo_url: &str) -> Option<(String, String)> {
    let s = repo_url.trim();
    let rest = s
        .strip_prefix("https://github.com/")
        .or_else(|| s.strip_prefix("http://github.com/"))
        .or_else(|| s.strip_prefix("github.com/"))?;
    let rest = rest.trim_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let mut parts = rest.split('/').filter(|p| !p.is_empty());
    let owner = parts.next()?.to_string();
    let repo = parts.next()?.to_string();
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some((owner, repo))
}

/// GitHub REST URL for the latest release of `owner/repo`.
pub fn api_latest_url(owner: &str, repo: &str) -> String {
    format!("https://api.github.com/repos/{owner}/{repo}/releases/latest")
}

/// Extract the sha256 hex for `bin_name` from a MANIFEST body. Lines are
/// `<64-hex>  <basename>`; header lines (`rev:`, `built:`, …) are skipped
/// because their first token is not 64 hex chars.
pub fn manifest_sha(manifest: &str, bin_name: &str) -> Option<String> {
    for line in manifest.lines() {
        let mut it = line.split_whitespace();
        let sha = it.next();
        let name = it.next();
        if let (Some(sha), Some(name)) = (sha, name) {
            if name == bin_name
                && sha.len() == 64
                && sha.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Some(sha.to_ascii_lowercase());
            }
        }
    }
    None
}

/// repo_url precedence: `--repo-url` > `[updater].repo_url` (when the
/// config file set it) > `DTFS_REPO_URL` env > built-in default.
pub fn resolve_repo_url(
    flag: Option<&str>,
    cfg: Option<&str>,
    env: Option<&str>,
    default: &str,
) -> String {
    flag.filter(|s| !s.is_empty())
        .or(cfg.filter(|s| !s.is_empty()))
        .or(env.filter(|s| !s.is_empty()))
        .unwrap_or(default)
        .to_string()
}

// ---- runtime ------------------------------------------------------------

/// Entry point for the `update` subcommand.
#[allow(clippy::too_many_arguments)]
pub fn run(
    target: UpdateTarget,
    check: bool,
    tag: Option<String>,
    repo_url_flag: Option<String>,
    install_dir_flag: Option<String>,
    config_path: Option<String>,
    no_restart: bool,
    yes: bool,
) -> Result<()> {
    let defaults = dtfs_common::config::UpdaterConfig::default();

    // Pull [updater] from --config, else the default role config path.
    let cfg_path = config_path
        .map(PathBuf::from)
        .or_else(|| default_config_path(target));
    let (cfg_repo, cfg_install) = match cfg_path {
        Some(ref p) if p.exists() => read_updater_section(p)?,
        _ => (None, None),
    };

    let env_repo = std::env::var("DTFS_REPO_URL").ok();
    let repo_url = resolve_repo_url(
        repo_url_flag.as_deref(),
        cfg_repo.as_deref(),
        env_repo.as_deref(),
        &defaults.repo_url,
    );
    let install_dir = install_dir_flag
        .or(cfg_install)
        .unwrap_or(defaults.install_dir);

    if check {
        return run_check(target, &repo_url, &install_dir);
    }
    run_update(target, &repo_url, tag.as_deref(), &install_dir, no_restart, yes)
}

fn default_config_path(t: UpdateTarget) -> Option<PathBuf> {
    let role = match t {
        UpdateTarget::Manager => "manager",
        UpdateTarget::Data => "data",
        UpdateTarget::Fuse => "fuse",
        UpdateTarget::Cli | UpdateTarget::All => return None,
    };
    Some(PathBuf::from(format!("/etc/dtfs/{role}.toml")))
}

/// Read just the `[updater]` block from any dtfs `.toml` (serde ignores
/// the rest). `Option` fields so we can tell "set" from "defaulted".
fn read_updater_section(path: &Path) -> Result<(Option<String>, Option<String>)> {
    #[derive(serde::Deserialize, Default)]
    struct Section {
        repo_url: Option<String>,
        install_dir: Option<String>,
    }
    #[derive(serde::Deserialize, Default)]
    struct File {
        #[serde(default)]
        updater: Section,
    }
    let f: File = dtfs_common::config::load_config(path)
        .with_context(|| format!("reading [updater] from {}", path.display()))?;
    Ok((f.updater.repo_url, f.updater.install_dir))
}

fn run_update(
    target: UpdateTarget,
    repo_url: &str,
    tag: Option<&str>,
    install_dir: &str,
    no_restart: bool,
    yes: bool,
) -> Result<()> {
    let bins = target_binaries(target);
    let url = tarball_url(repo_url, tag);

    let tmp = make_tmp_dir()?;
    let _guard = TmpGuard(tmp.clone());

    let tarball = tmp.join(ASSET);
    eprintln!("Downloading {url}");
    curl_download(&url, &tarball)?;

    eprintln!("Extracting {ASSET}");
    extract(&tarball, &tmp)?;

    // Integrity gate: verify every target binary before installing any.
    let manifest_path = tmp.join("dtfs/MANIFEST");
    let manifest = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("release tarball missing {}", manifest_path.display()))?;
    let staged: Vec<PathBuf> = bins.iter().map(|b| tmp.join("dtfs/bin").join(b)).collect();
    for (bin, src) in bins.iter().zip(&staged) {
        if !src.exists() {
            bail!("release tarball missing dtfs/bin/{bin}");
        }
        let want = manifest_sha(&manifest, bin)
            .with_context(|| format!("MANIFEST has no sha256 for {bin}"))?;
        let got = sha256_file(src).with_context(|| format!("hashing {}", src.display()))?;
        if got != want {
            bail!("integrity check failed for {bin}: manifest {want}, downloaded {got} (installing nothing)");
        }
        eprintln!("verified {bin} (sha256 {}…)", &got[..12]);
    }

    // Plan + single confirmation gate (install is itself disruptive).
    eprintln!("\nUpdate plan:");
    eprintln!("  source : {url}");
    eprintln!("  install: {install_dir}");
    for bin in bins {
        match service_for_binary(bin) {
            Some(svc) if !no_restart => eprintln!("  {bin}: install + restart {svc}"),
            Some(svc) => eprintln!("  {bin}: install (skip restart {svc})"),
            None => eprintln!("  {bin}: install (no service)"),
        }
    }
    if !yes && !confirm("Proceed? [y/N] ")? {
        eprintln!("aborted; nothing installed");
        return Ok(());
    }

    for (bin, src) in bins.iter().zip(&staged) {
        let dst = format!("{install_dir}/{bin}");
        install_binary(src, &dst)?;
        eprintln!("installed {bin} -> {dst}");
    }

    if no_restart {
        eprintln!("--no-restart: services keep the old binary until the next restart");
    } else {
        for bin in bins {
            if let Some(svc) = service_for_binary(bin) {
                restart_service(svc)?;
            }
        }
    }
    eprintln!("update complete");
    Ok(())
}

/// CO-5-15 — parse the numeric `(major, minor, patch)` tuple out of a
/// version string. Tolerates a leading `v`, surrounding text (the binary
/// `--version` prints `v0.1.9 (built ...)`), and missing components
/// (`v1` -> `(1, 0, 0)`). Returns `None` if no leading-numeric version
/// token is found. A trailing pre-release/build suffix on the patch
/// component (`v1.2.3-rc1`) is ignored for ordering.
fn parse_semver(s: &str) -> Option<(u64, u64, u64)> {
    // Take the first whitespace-delimited token, drop an optional `v`.
    let tok = s.trim().split_whitespace().next()?;
    let tok = tok.strip_prefix('v').or_else(|| tok.strip_prefix('V')).unwrap_or(tok);
    let mut it = tok.split('.');
    let parse_part = |p: Option<&str>| -> u64 {
        p.map(|p| {
            // Stop at the first non-digit (pre-release/build suffix).
            let digits: String = p.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse().unwrap_or(0)
        })
        .unwrap_or(0)
    };
    let first = it.next()?;
    // The major component must start with a digit, else this is not a
    // version token (avoids treating "dtfs-cli" as 0.0.0).
    if !first.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let major = parse_part(Some(first));
    let minor = parse_part(it.next());
    let patch = parse_part(it.next());
    Some((major, minor, patch))
}

/// CO-5-15 — semver-aware "is `latest` strictly newer than `installed`?".
/// A naive string compare orders `v0.1.10 < v0.1.9`; this compares the
/// numeric `(major, minor, patch)` tuples. Unparseable versions
/// (e.g. `not installed`, `v0.0.0-dev`) make the answer conservative:
/// if `installed` can't be parsed but `latest` can, an update is
/// available; otherwise (latest unparseable, or both unparseable) false.
fn version_is_newer(installed: &str, latest: &str) -> bool {
    match (parse_semver(installed), parse_semver(latest)) {
        (Some(i), Some(l)) => l > i,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn run_check(target: UpdateTarget, repo_url: &str, install_dir: &str) -> Result<()> {
    let (owner, repo) = parse_owner_repo(repo_url)
        .with_context(|| format!("--check needs a github.com repo_url, got {repo_url}"))?;
    let body = curl_get(&api_latest_url(&owner, &repo))?;
    let v: serde_json::Value =
        serde_json::from_str(&body).context("parsing GitHub releases/latest JSON")?;
    let tag = v.get("tag_name").and_then(|t| t.as_str()).unwrap_or("<unknown>");
    let published = v.get("published_at").and_then(|t| t.as_str()).unwrap_or("");
    if published.is_empty() {
        println!("latest release: {tag}");
    } else {
        println!("latest release: {tag} ({published})");
    }
    for bin in target_binaries(target) {
        let installed = installed_version(install_dir, bin);
        if version_is_newer(&installed, tag) {
            println!("  {bin}: installed {installed} -> update available ({tag})");
        } else {
            println!("  {bin}: installed {installed} (up to date)");
        }
    }
    Ok(())
}

fn installed_version(install_dir: &str, bin: &str) -> String {
    let path = format!("{install_dir}/{bin}");
    match Command::new(&path).arg("--version").output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => "not installed".to_string(),
    }
}

// ---- external tools -----------------------------------------------------

fn curl_download(url: &str, out: &Path) -> Result<()> {
    // `=https,file`: https for real releases, file:// for a local/offline
    // --repo-url the operator explicitly points at; nothing else.
    let status = Command::new("curl")
        .args(["-fL", "--proto", "=https,file", "--tlsv1.2", "-o"])
        .arg(out)
        .arg(url)
        .status()
        .context("running curl (is curl installed?)")?;
    if !status.success() {
        bail!("curl failed downloading {url}");
    }
    Ok(())
}

fn curl_get(url: &str) -> Result<String> {
    let out = Command::new("curl")
        .args(["-fsSL", "--proto", "=https", "-H", "User-Agent: dtfs-cli"])
        .arg(url)
        .output()
        .context("running curl (is curl installed?)")?;
    if !out.status.success() {
        bail!("curl failed GET {url}: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn extract(tarball: &Path, dir: &Path) -> Result<()> {
    let status = Command::new("tar")
        .arg("-C")
        .arg(dir)
        .arg("-xzf")
        .arg(tarball)
        .status()
        .context("running tar")?;
    if !status.success() {
        bail!("tar failed extracting {}", tarball.display());
    }
    Ok(())
}

fn install_binary(src: &Path, dst: &str) -> Result<()> {
    let status = Command::new("install")
        .args(["-m", "0755"])
        .arg(src)
        .arg(dst)
        .status()
        .context("running install")?;
    if !status.success() {
        bail!("install failed for {dst} (run as root?)");
    }
    Ok(())
}

fn restart_service(svc: &str) -> Result<()> {
    if svc.contains('*') {
        let units = list_units(svc)?;
        if units.is_empty() {
            eprintln!("no running {svc} instances to restart");
        }
        for u in units {
            systemctl(&["restart", &u])?;
            assert_active(&u)?;
            eprintln!("restarted {u}");
        }
    } else {
        systemctl(&["restart", svc])?;
        assert_active(svc)?;
        eprintln!("restarted {svc}");
    }
    Ok(())
}

fn list_units(pattern: &str) -> Result<Vec<String>> {
    let out = Command::new("systemctl")
        .args(["list-units", "--type=service", "--no-legend", pattern])
        .output()
        .context("running systemctl list-units")?;
    if !out.status.success() {
        bail!("systemctl list-units {pattern} failed");
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .map(|s| s.trim_start_matches('●').to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .args(args)
        .status()
        .context("running systemctl (run as root?)")?;
    if !status.success() {
        bail!("systemctl {} failed", args.join(" "));
    }
    Ok(())
}

fn assert_active(unit: &str) -> Result<()> {
    let out = Command::new("systemctl")
        .args(["is-active", unit])
        .output()
        .context("running systemctl is-active")?;
    if !out.status.success() {
        bail!(
            "{unit} did not come back active ({})",
            String::from_utf8_lossy(&out.stdout).trim()
        );
    }
    Ok(())
}

// ---- small utilities ----------------------------------------------------

fn make_tmp_dir() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("dtfs-update-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

struct TmpGuard(PathBuf);
impl Drop for TmpGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    Ok(hex)
}

fn confirm(prompt: &str) -> Result<bool> {
    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "Yes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn co_5_15_version_is_newer_is_semver_aware() {
        // The motivating bug: a naive string compare orders
        // "v0.1.10" < "v0.1.9"; the numeric compare must not.
        assert!(version_is_newer("v0.1.9", "v0.1.10"));
        assert!(!version_is_newer("v0.1.10", "v0.1.9"));
        // Equal -> not newer.
        assert!(!version_is_newer("v0.1.9", "v0.1.9"));
        // Major/minor ordering.
        assert!(version_is_newer("v0.9.9", "v1.0.0"));
        assert!(!version_is_newer("v2.0.0", "v1.9.9"));
        // Tolerates the `v` prefix being absent and trailing build text
        // (the binary `--version` prints "v0.1.9 (built 20260101-000000)").
        assert!(version_is_newer("v0.1.9 (built 20260101-000000)", "0.1.10"));
        // Missing components default to 0: v1 == v1.0.0.
        assert!(!version_is_newer("v1", "v1.0.0"));
        assert!(version_is_newer("v1", "v1.0.1"));
        // Unparseable installed (not yet installed) but a real latest ->
        // update available.
        assert!(version_is_newer("not installed", "v0.1.0"));
        // Unparseable latest -> conservatively not newer.
        assert!(!version_is_newer("v0.1.0", "<unknown>"));
    }

    #[test]
    fn target_binaries_map() {
        assert_eq!(target_binaries(UpdateTarget::Manager), &["dtfs-manager"]);
        assert_eq!(target_binaries(UpdateTarget::Data), &["dtfs-data"]);
        assert_eq!(target_binaries(UpdateTarget::Fuse), &["dtfs-fuse"]);
        assert_eq!(target_binaries(UpdateTarget::Cli), &["dtfs-cli"]);
        assert_eq!(
            target_binaries(UpdateTarget::All),
            &["dtfs-manager", "dtfs-data", "dtfs-fuse", "dtfs-cli"]
        );
    }

    #[test]
    fn service_map() {
        assert_eq!(service_for_binary("dtfs-manager"), Some("dtfs-manager.service"));
        assert_eq!(service_for_binary("dtfs-data"), Some("dtfs-data.service"));
        assert_eq!(service_for_binary("dtfs-fuse"), Some("dtfs-fuse@*"));
        assert_eq!(service_for_binary("dtfs-cli"), None);
    }

    #[test]
    fn tarball_url_latest_and_tagged() {
        assert_eq!(
            tarball_url("https://github.com/lischetzke/dtfs", None),
            "https://github.com/lischetzke/dtfs/releases/latest/download/dtfs-linux-amd64.tar.gz"
        );
        assert_eq!(
            tarball_url("https://github.com/lischetzke/dtfs/", Some("v1.2.3")),
            "https://github.com/lischetzke/dtfs/releases/download/v1.2.3/dtfs-linux-amd64.tar.gz"
        );
    }

    #[test]
    fn parse_owner_repo_cases() {
        assert_eq!(
            parse_owner_repo("https://github.com/lischetzke/dtfs"),
            Some(("lischetzke".into(), "dtfs".into()))
        );
        assert_eq!(
            parse_owner_repo("https://github.com/lischetzke/dtfs.git/"),
            Some(("lischetzke".into(), "dtfs".into()))
        );
        assert_eq!(
            parse_owner_repo("github.com/acme/widget"),
            Some(("acme".into(), "widget".into()))
        );
        assert_eq!(parse_owner_repo("https://gitlab.com/a/b"), None);
        assert_eq!(parse_owner_repo("https://github.com/onlyowner"), None);
        assert_eq!(parse_owner_repo("file:///tmp/repo"), None);
    }

    #[test]
    fn api_latest_url_fmt() {
        assert_eq!(
            api_latest_url("lischetzke", "dtfs"),
            "https://api.github.com/repos/lischetzke/dtfs/releases/latest"
        );
    }

    #[test]
    fn manifest_sha_parsing() {
        let sha = "a".repeat(64);
        let manifest = format!(
            "DTFS build manifest\nrev:     abc1234\nbuilt:   2026-06-04T00:00:00Z\n{sha}  dtfs-cli\n{}  dtfs-data\n",
            "b".repeat(64)
        );
        assert_eq!(manifest_sha(&manifest, "dtfs-cli"), Some(sha.clone()));
        assert_eq!(manifest_sha(&manifest, "dtfs-data"), Some("b".repeat(64)));
        assert_eq!(manifest_sha(&manifest, "dtfs-manager"), None);
        // Header lines never match (first token isn't 64 hex chars).
        assert_eq!(manifest_sha(&manifest, "abc1234"), None);
    }

    #[test]
    fn resolve_repo_url_precedence() {
        let dflt = "https://github.com/lischetzke/dtfs";
        // flag wins over everything.
        assert_eq!(
            resolve_repo_url(Some("https://f/flag"), Some("https://c/cfg"), Some("https://e/env"), dflt),
            "https://f/flag"
        );
        // config beats env + default.
        assert_eq!(
            resolve_repo_url(None, Some("https://c/cfg"), Some("https://e/env"), dflt),
            "https://c/cfg"
        );
        // env beats default.
        assert_eq!(resolve_repo_url(None, None, Some("https://e/env"), dflt), "https://e/env");
        // empty strings are skipped.
        assert_eq!(resolve_repo_url(Some(""), None, None, dflt), dflt);
        // nothing set → default.
        assert_eq!(resolve_repo_url(None, None, None, dflt), dflt);
    }
}
