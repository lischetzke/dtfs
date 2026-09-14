//! AZ-5 — `dtfs-cli update`: pull a release tarball, verify it against
//! the in-tarball MANIFEST (sha256), atomically install the target
//! binary, and restart the matching systemd service on the local host.
//! Local-node only; the cluster-wide rolling update is the GitLab
//! pipeline (`deploy/ansible/update-cluster.yml`).
//!
//! Sources (`release_source`): a GitLab project URL (the default,
//! `dtfs_common::REPO_URL`) reads the Generic Package Registry and the
//! Releases API, optionally with a deploy/access token; a github.com URL
//! keeps the legacy releases/download path.
//!
//! Network/IO is delegated to the same tools the deploy layer already
//! uses (`curl`, `tar`, `install`, `systemctl`); the only Rust deps are
//! `sha2` (integrity) and `tempfile` (CO-2-4 unpredictable staging dir),
//! and the download streams to disk (bounded memory, CLAUDE.md § A.3.2).
//! Integrity gate is § A.3.3 / A.6: a single sha256 mismatch aborts the
//! whole update before anything is installed.

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

/// Where a `repo_url` points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseSource {
    /// github.com `owner/repo` (legacy releases/download URLs).
    GitHub { owner: String, repo: String },
    /// A GitLab project; `api` is `https://<host>/api/v4/projects/<group>%2F<project>`.
    GitLab { api: String },
}

/// Classify `repo_url`. GitLab is the default for any non-github.com
/// host: the project path is URL-encoded into the `/projects/:id` form
/// so no numeric project id is needed. `None` when no project path can
/// be found.
pub fn release_source(repo_url: &str) -> Option<ReleaseSource> {
    if let Some((owner, repo)) = parse_owner_repo(repo_url) {
        return Some(ReleaseSource::GitHub { owner, repo });
    }
    let s = repo_url.trim().trim_end_matches('/');
    let (scheme, rest) = s.split_once("://")?;
    let (host, path) = rest.split_once('/')?;
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    if host.is_empty() || path.is_empty() {
        return None;
    }
    let encoded = path.replace('/', "%2F");
    Some(ReleaseSource::GitLab {
        api: format!("{scheme}://{host}/api/v4/projects/{encoded}"),
    })
}

/// GitLab Generic Package Registry URL of the tarball for `tag`
/// (package `dtfs`, the layout `scripts/ci/gitlab-release.sh` uploads).
pub fn gitlab_tarball_url(api: &str, tag: &str) -> String {
    format!("{api}/packages/generic/dtfs/{tag}/{ASSET}")
}

/// GitLab Releases API listing (newest first).
pub fn gitlab_releases_url(api: &str) -> String {
    format!("{api}/releases?per_page=50")
}

/// Header line for a GitLab token: deploy tokens (`gldt-...`) go in
/// `DEPLOY-TOKEN`, project/personal access tokens (`glpat-...` or
/// anything else) in `PRIVATE-TOKEN`.
pub fn token_header(token: &str) -> String {
    if token.starts_with("gldt-") {
        format!("DEPLOY-TOKEN: {token}")
    } else {
        format!("PRIVATE-TOKEN: {token}")
    }
}

/// Is `tag` a full release (`vX.Y.Z`) rather than an alpha
/// (`vX.Y.Z_YYYYMMDDHHmm`, the pipeline's `deploy` builds)?
pub fn is_full_release_tag(tag: &str) -> bool {
    let Some(rest) = tag.strip_prefix('v') else { return false };
    let parts: Vec<&str> = rest.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

/// Pick the newest release tag from a GitLab `/releases` listing
/// (already newest-first). Alphas are skipped unless `include_alpha`.
/// Returns `(tag, released_at)`.
pub fn pick_latest_release(json: &str, include_alpha: bool) -> Option<(String, String)> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let arr = v.as_array()?;
    for rel in arr {
        let tag = rel.get("tag_name")?.as_str()?;
        if !include_alpha && !is_full_release_tag(tag) {
            continue;
        }
        let at = rel.get("released_at").and_then(|t| t.as_str()).unwrap_or("").to_string();
        return Some((tag.to_string(), at));
    }
    None
}

/// Token precedence: `--token` > `DTFS_REPO_TOKEN` env > `[updater].token`.
pub fn resolve_token(flag: Option<&str>, env: Option<&str>, cfg: Option<&str>) -> Option<String> {
    flag.filter(|s| !s.is_empty())
        .or(env.filter(|s| !s.is_empty()))
        .or(cfg.filter(|s| !s.is_empty()))
        .map(|s| s.to_string())
}

/// GitHub release tarball URL. `tag = None` → the `latest` release.
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
    token_flag: Option<String>,
    no_restart: bool,
    yes: bool,
) -> Result<()> {
    let defaults = dtfs_common::config::UpdaterConfig::default();

    // Pull [updater] from --config, else the default role config path.
    let cfg_path = config_path
        .map(PathBuf::from)
        .or_else(|| default_config_path(target));
    let (cfg_repo, cfg_install, cfg_token) = match cfg_path {
        Some(ref p) if p.exists() => read_updater_section(p)?,
        _ => (None, None, None),
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
    let env_token = std::env::var("DTFS_REPO_TOKEN").ok();
    let token = resolve_token(token_flag.as_deref(), env_token.as_deref(), cfg_token.as_deref());
    let headers: Vec<String> = token.iter().map(|t| token_header(t)).collect();
    let source = release_source(&repo_url)
        .with_context(|| format!("repo_url '{repo_url}' is neither a GitLab project URL nor a github.com repo"))?;

    if check {
        return run_check(target, &source, &headers, &install_dir);
    }
    run_update(target, &source, &headers, tag.as_deref(), &install_dir, no_restart, yes)
}

/// Resolve the tarball URL for `tag` (or the latest full release).
fn resolve_tarball_url(source: &ReleaseSource, headers: &[String], tag: Option<&str>) -> Result<String> {
    match source {
        ReleaseSource::GitHub { owner, repo } => {
            Ok(tarball_url(&format!("https://github.com/{owner}/{repo}"), tag))
        }
        ReleaseSource::GitLab { api } => {
            let tag = match tag {
                Some(t) => t.to_string(),
                None => {
                    let body = curl_get(&gitlab_releases_url(api), headers)?;
                    pick_latest_release(&body, false)
                        .map(|(t, _)| t)
                        .context("no full release (vX.Y.Z) found in the GitLab release list")?
                }
            };
            Ok(gitlab_tarball_url(api, &tag))
        }
    }
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
fn read_updater_section(path: &Path) -> Result<(Option<String>, Option<String>, Option<String>)> {
    #[derive(serde::Deserialize, Default)]
    struct Section {
        repo_url: Option<String>,
        install_dir: Option<String>,
        token: Option<String>,
    }
    #[derive(serde::Deserialize, Default)]
    struct File {
        #[serde(default)]
        updater: Section,
    }
    let f: File = dtfs_common::config::load_config(path)
        .with_context(|| format!("reading [updater] from {}", path.display()))?;
    Ok((f.updater.repo_url, f.updater.install_dir, f.updater.token))
}

fn run_update(
    target: UpdateTarget,
    source: &ReleaseSource,
    headers: &[String],
    tag: Option<&str>,
    install_dir: &str,
    no_restart: bool,
    yes: bool,
) -> Result<()> {
    let bins = target_binaries(target);
    let url = resolve_tarball_url(source, headers, tag)?;

    // Stage into a fresh, unpredictable directory (CO-2-4): a fixed path
    // under temp_dir() is a symlink/race/clobber target. tempfile creates
    // it with 0700 and removes it on drop.
    let staging = tempfile::tempdir().context("creating update staging dir")?;
    let tmp = staging.path();

    let tarball = tmp.join(ASSET);
    eprintln!("Downloading {url}");
    curl_download(&url, headers, &tarball)?;

    eprintln!("Extracting {ASSET}");
    extract(&tarball, tmp)?;

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

fn run_check(
    target: UpdateTarget,
    source: &ReleaseSource,
    headers: &[String],
    install_dir: &str,
) -> Result<()> {
    let (tag, published) = match source {
        ReleaseSource::GitHub { owner, repo } => {
            let body = curl_get(&api_latest_url(owner, repo), headers)?;
            let v: serde_json::Value =
                serde_json::from_str(&body).context("parsing GitHub releases/latest JSON")?;
            (
                v.get("tag_name").and_then(|t| t.as_str()).unwrap_or("<unknown>").to_string(),
                v.get("published_at").and_then(|t| t.as_str()).unwrap_or("").to_string(),
            )
        }
        ReleaseSource::GitLab { api } => {
            let body = curl_get(&gitlab_releases_url(api), headers)?;
            pick_latest_release(&body, false)
                .context("no full release (vX.Y.Z) found in the GitLab release list")?
        }
    };
    let tag = tag.as_str();
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

fn curl_download(url: &str, headers: &[String], out: &Path) -> Result<()> {
    // `=https,file`: https for real releases, file:// for a local/offline
    // --repo-url the operator explicitly points at; nothing else.
    let mut cmd = Command::new("curl");
    cmd.args(["-fL", "--proto", "=https,file", "--tlsv1.2"]);
    for h in headers {
        cmd.arg("-H").arg(h);
    }
    let status = cmd
        .arg("-o")
        .arg(out)
        .arg(url)
        .status()
        .context("running curl (is curl installed?)")?;
    if !status.success() {
        bail!("curl failed downloading {url}");
    }
    Ok(())
}

fn curl_get(url: &str, headers: &[String]) -> Result<String> {
    let mut cmd = Command::new("curl");
    cmd.args(["-fsSL", "--proto", "=https", "-H", "User-Agent: dtfs-cli"]);
    for h in headers {
        cmd.arg("-H").arg(h);
    }
    let out = cmd
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

/// Install `src` over `dst` without ever leaving a truncated destination
/// (CO-2-4): install to a sibling `<dst>.new` first, then atomically
/// rename it over `dst`. A crash/failed copy leaves the old `dst` intact
/// (only the throwaway `.new` is partial). `rename(2)` within one
/// directory is atomic, so a reader never sees a half-written binary.
fn install_binary(src: &Path, dst: &str) -> Result<()> {
    let staged = format!("{dst}.new");
    let status = Command::new("install")
        .args(["-m", "0755"])
        .arg(src)
        .arg(&staged)
        .status()
        .context("running install")?;
    if !status.success() {
        let _ = std::fs::remove_file(&staged);
        bail!("install failed for {dst} (run as root?)");
    }
    if let Err(e) = std::fs::rename(&staged, dst) {
        let _ = std::fs::remove_file(&staged);
        return Err(e).with_context(|| format!("atomically installing {dst}"));
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
    fn release_source_gitlab_encodes_the_project_path() {
        assert_eq!(
            release_source("https://github.com/lischetzke/dtfs"),
            Some(ReleaseSource::GitLab {
                api: "https://gitlab.example.invalid/api/v4/projects/example%2Fdtfs".into()
            })
        );
        // Trailing slash / .git are tolerated; nested groups encode every slash.
        assert_eq!(
            release_source("https://gl.example/a/b/c.git/"),
            Some(ReleaseSource::GitLab { api: "https://gl.example/api/v4/projects/a%2Fb%2Fc".into() })
        );
        assert_eq!(
            release_source("https://github.com/lischetzke/dtfs"),
            Some(ReleaseSource::GitHub { owner: "lischetzke".into(), repo: "dtfs".into() })
        );
        assert_eq!(release_source("https://gl.example"), None);
        assert_eq!(release_source("not a url"), None);
    }

    #[test]
    fn gitlab_urls_follow_the_pipeline_layout() {
        let api = "https://gl.example/api/v4/projects/example%2Fdtfs";
        assert_eq!(
            gitlab_tarball_url(api, "v0.2.0"),
            "https://gl.example/api/v4/projects/example%2Fdtfs/packages/generic/dtfs/v0.2.0/dtfs-linux-amd64.tar.gz"
        );
        assert_eq!(gitlab_releases_url(api), format!("{api}/releases?per_page=50"));
    }

    #[test]
    fn token_header_picks_deploy_vs_private() {
        assert_eq!(token_header("gldt-abc"), "DEPLOY-TOKEN: gldt-abc");
        assert_eq!(token_header("glpat-abc"), "PRIVATE-TOKEN: glpat-abc");
        assert_eq!(token_header("legacy"), "PRIVATE-TOKEN: legacy");
    }

    #[test]
    fn latest_release_skips_alphas_unless_asked() {
        let json = r#"[
            {"tag_name": "v0.2.0_202609141200", "released_at": "2026-09-14T12:00:00Z"},
            {"tag_name": "v0.2.0", "released_at": "2026-09-13T10:00:00Z"},
            {"tag_name": "v0.1.9", "released_at": "2026-09-01T10:00:00Z"}
        ]"#;
        assert_eq!(
            pick_latest_release(json, false),
            Some(("v0.2.0".into(), "2026-09-13T10:00:00Z".into()))
        );
        assert_eq!(
            pick_latest_release(json, true),
            Some(("v0.2.0_202609141200".into(), "2026-09-14T12:00:00Z".into()))
        );
        assert_eq!(pick_latest_release("[]", false), None);
        assert!(is_full_release_tag("v1.2.3"));
        assert!(!is_full_release_tag("v1.2.3_202609141200"));
        assert!(!is_full_release_tag("1.2.3"));
    }

    #[test]
    fn token_precedence_flag_env_config() {
        assert_eq!(resolve_token(Some("f"), Some("e"), Some("c")).as_deref(), Some("f"));
        assert_eq!(resolve_token(None, Some("e"), Some("c")).as_deref(), Some("e"));
        assert_eq!(resolve_token(None, None, Some("c")).as_deref(), Some("c"));
        assert_eq!(resolve_token(Some(""), Some(""), Some("")), None);
        assert_eq!(resolve_token(None, None, None), None);
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
    fn co_2_4_install_is_atomic_and_leaves_no_new_residue() {
        // install_binary must replace dst via a sibling <dst>.new + rename,
        // never truncate dst in place. On success the .new file is gone and
        // dst has the new contents; an existing dst is fully overwritten,
        // never observed half-written.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src-bin");
        std::fs::write(&src, b"#!/bin/sh\necho new\n").unwrap();
        let dst = dir.path().join("dtfs-cli");
        // Pre-existing (old) dst that must be replaced wholesale.
        std::fs::write(&dst, b"OLD-CONTENT-LONGER-THAN-NEW").unwrap();

        install_binary(&src, dst.to_str().unwrap()).unwrap();

        assert_eq!(std::fs::read(&dst).unwrap(), b"#!/bin/sh\necho new\n");
        // No leftover staging file -> no truncated-dest window artifact.
        assert!(!dir.path().join("dtfs-cli.new").exists(), "<dst>.new must not linger");
    }

    #[test]
    fn co_2_4_failed_install_leaves_old_dst_intact() {
        // If the copy can't happen (missing source), the pre-existing dst
        // must be untouched (no truncation) and no .new residue remains.
        let dir = tempfile::tempdir().unwrap();
        let missing_src = dir.path().join("does-not-exist");
        let dst = dir.path().join("dtfs-cli");
        std::fs::write(&dst, b"OLD-CONTENT").unwrap();

        let err = install_binary(&missing_src, dst.to_str().unwrap());
        assert!(err.is_err(), "install of a missing source must error");
        assert_eq!(std::fs::read(&dst).unwrap(), b"OLD-CONTENT", "dst must be untouched");
        assert!(!dir.path().join("dtfs-cli.new").exists(), "<dst>.new must not linger");
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
