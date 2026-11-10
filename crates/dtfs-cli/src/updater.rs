//! AZ-5 — `dtfs-cli update`: pull a release tarball, verify it against
//! the in-tarball MANIFEST (sha256), atomically install the target
//! binary, and restart the matching systemd service on the local host.
//! Local-node only; the cluster-wide rolling update is
//! `deploy/ansible/update-cluster.yml`.
//!
//! Source (`release_source`): GitHub releases of a github.com repo (the
//! default, `dtfs_common::REPO_URL`), optionally with a GitHub token.
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
use std::process::{Command, Stdio};

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

/// Where a `repo_url` points: a github.com `owner/repo` (the only source).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseSource {
    GitHub { owner: String, repo: String },
}

/// Classify `repo_url`; `None` unless it is a github.com `owner/repo`.
pub fn release_source(repo_url: &str) -> Option<ReleaseSource> {
    parse_owner_repo(repo_url).map(|(owner, repo)| ReleaseSource::GitHub { owner, repo })
}

/// Header line for the optional GitHub token (rate limits / private fork).
pub fn token_header(token: &str) -> String {
    format!("Authorization: Bearer {token}")
}

/// Token precedence: `--token` > `DTFS_REPO_TOKEN` env > `[updater].token`.
pub fn resolve_token(flag: Option<&str>, env: Option<&str>, cfg: Option<&str>) -> Option<String> {
    flag.filter(|s| !s.is_empty())
        .or(env.filter(|s| !s.is_empty()))
        .or(cfg.filter(|s| !s.is_empty()))
        .map(|s| s.to_string())
}

/// Minisign key precedence: `--minisign-key` > `DTFS_MINISIGN_PUBKEY` env >
/// `[updater].minisign_public_key`. Blank values are skipped.
pub fn resolve_minisign_key(flag: Option<&str>, env: Option<&str>, cfg: Option<&str>) -> Option<String> {
    [flag, env, cfg]
        .into_iter()
        .flatten()
        .find(|s| !s.trim().is_empty())
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
    minisign_key_flag: Option<String>,
    no_restart: bool,
    yes: bool,
    force: bool,
) -> Result<()> {
    let defaults = dtfs_common::config::UpdaterConfig::default();

    // Pull [updater] from --config, else the default role config path.
    let cfg_paths = match config_path {
        Some(p) => vec![PathBuf::from(p)],
        None => default_config_paths(target),
    };
    let (mut cfg_repo, mut cfg_install, mut cfg_token, mut cfg_key) = (None, None, None, None);
    for p in cfg_paths.iter().filter(|p| p.exists()) {
        let (r, i, t, k) = read_updater_section(p)?;
        cfg_repo = cfg_repo.or(r);
        cfg_install = cfg_install.or(i);
        cfg_token = cfg_token.or(t);
        cfg_key = cfg_key.or(k);
    }
    let env_key = std::env::var("DTFS_MINISIGN_PUBKEY").ok();
    let minisign_key = resolve_minisign_key(minisign_key_flag.as_deref(), env_key.as_deref(), cfg_key.as_deref());

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
        .with_context(|| format!("repo_url '{repo_url}' is not a github.com repo (https://github.com/<owner>/<repo>); GitHub is the only supported release source"))?;

    if check {
        return run_check(target, &source, &headers, &install_dir);
    }
    run_update(
        target,
        &source,
        &headers,
        tag.as_deref(),
        &install_dir,
        minisign_key.as_deref(),
        no_restart,
        yes,
        force,
    )
}

/// Resolve the tarball URL for `tag` (or the latest full release).
fn resolve_tarball_url(source: &ReleaseSource, tag: Option<&str>) -> Result<String> {
    match source {
        ReleaseSource::GitHub { owner, repo } => {
            Ok(tarball_url(&format!("https://github.com/{owner}/{repo}"), tag))
        }
    }
}

/// Role configs that may carry `[updater]` for `t`. `cli`/`all` have no role
/// of their own, so any present role config on the host may supply it.
fn default_config_paths(t: UpdateTarget) -> Vec<PathBuf> {
    let roles: &[&str] = match t {
        UpdateTarget::Manager => &["manager"],
        UpdateTarget::Data => &["data"],
        UpdateTarget::Fuse => &["fuse"],
        UpdateTarget::Cli | UpdateTarget::All => &["manager", "data", "fuse"],
    };
    roles.iter().map(|r| PathBuf::from(format!("/etc/dtfs/{r}.toml"))).collect()
}

/// Read just the `[updater]` block from any dtfs `.toml` (serde ignores
/// the rest). `Option` fields so we can tell "set" from "defaulted".
fn read_updater_section(
    path: &Path,
) -> Result<(Option<String>, Option<String>, Option<String>, Option<String>)> {
    #[derive(serde::Deserialize, Default)]
    struct Section {
        repo_url: Option<String>,
        install_dir: Option<String>,
        token: Option<String>,
        minisign_public_key: Option<String>,
    }
    #[derive(serde::Deserialize, Default)]
    struct File {
        #[serde(default)]
        updater: Section,
    }
    let f: File = dtfs_common::config::load_config(path)
        .with_context(|| format!("reading [updater] from {}", path.display()))?;
    let u = f.updater;
    Ok((u.repo_url, u.install_dir, u.token, u.minisign_public_key))
}

#[allow(clippy::too_many_arguments)]
fn run_update(
    target: UpdateTarget,
    source: &ReleaseSource,
    headers: &[String],
    tag: Option<&str>,
    install_dir: &str,
    minisign_key: Option<&str>,
    no_restart: bool,
    yes: bool,
    force: bool,
) -> Result<()> {
    let bins = target_binaries(target);
    let url = resolve_tarball_url(source, tag)?;

    // Stage into a fresh, unpredictable directory (CO-2-4): a fixed path
    // under temp_dir() is a symlink/race/clobber target. tempfile creates
    // it with 0700 and removes it on drop.
    let staging = tempfile::tempdir().context("creating update staging dir")?;
    let tmp = staging.path();

    let tarball = tmp.join(ASSET);
    eprintln!("Downloading {url}");
    curl_download(&url, headers, &tarball)?;

    match minisign_key {
        Some(key) => {
            let sums_url = sibling_url(&url, SUMS_ASSET);
            let sums_path = tmp.join(SUMS_ASSET);
            let sig_path = tmp.join(format!("{SUMS_ASSET}.minisig"));
            eprintln!("Downloading {sums_url} (+ .minisig)");
            curl_download(&sums_url, headers, &sums_path)?;
            curl_download(&format!("{sums_url}.minisig"), headers, &sig_path)?;
            let sums = std::fs::read(&sums_path).context("reading SHA256SUMS")?;
            let sig = std::fs::read_to_string(&sig_path).context("reading SHA256SUMS.minisig")?;
            verify_signed_sums(key, &sums, &sig, &sha256_file(&tarball)?)?;
            eprintln!("verified {SUMS_ASSET} signature and {ASSET} checksum");
        }
        None => eprintln!(
            "warning: no minisign public key configured ([updater].minisign_public_key / \
             DTFS_MINISIGN_PUBKEY); release authenticity is NOT verified"
        ),
    }

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

    if confirm_before_probe(minisign_key.is_some(), yes)
        && !confirm(
            "No minisign key: the downloaded binaries are only TLS-authenticated and will be              run with --version to probe them. Continue? [y/N] ",
        )?
    {
        bail!("aborted before running unverified binaries; nothing installed");
    }

    // Skip binaries already at the staged version (#153).
    let mut todo: Vec<(&str, &PathBuf)> = Vec::new();
    for (bin, src) in bins.iter().zip(&staged) {
        let staged_v = binary_version(src);
        let installed = installed_version(install_dir, bin);
        match plan_install(&staged_v, &installed, tag, force)? {
            Plan::Install => todo.push((bin, src)),
            Plan::UpToDate => eprintln!("{bin}: already at {installed}; skipping (--force reinstalls)"),
        }
    }
    if todo.is_empty() {
        eprintln!("up to date; nothing installed or restarted");
        return Ok(());
    }
    let bins: Vec<&str> = todo.iter().map(|(b, _)| *b).collect();

    // Plan + single confirmation gate (install is itself disruptive).
    eprintln!("\nUpdate plan:");
    eprintln!("  source : {url}");
    eprintln!("  install: {install_dir}");
    for bin in &bins {
        match service_for_binary(bin) {
            Some(svc) if !no_restart => eprintln!("  {bin}: install + restart {svc}"),
            Some(svc) => eprintln!("  {bin}: install (skip restart {svc})"),
            None => eprintln!("  {bin}: install (no service)"),
        }
    }
    if !yes && !confirm("Proceed? [y/N] ")? {
        // Non-zero so scripts running without -y do not read "aborted" as success.
        bail!("aborted; nothing installed (pass -y to proceed non-interactively)");
    }

    for (bin, src) in &todo {
        let dst = format!("{install_dir}/{bin}");
        install_binary(src, &dst)?;
        eprintln!("installed {bin} -> {dst}");
    }

    if no_restart {
        eprintln!("--no-restart: services keep the old binary until the next restart");
    } else {
        let mut failures = Vec::new();
        for bin in &bins {
            if let Some(svc) = service_for_binary(bin) {
                restart_service(svc, &mut failures)?;
            }
        }
        finish_restarts(failures)?;
    }
    eprintln!("update complete");
    Ok(())
}

use dtfs_common::version::{version_is_newer, version_token};

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

/// Whether a staged binary needs installing (#153).
#[derive(Debug, PartialEq)]
enum Plan {
    Install,
    UpToDate,
}

/// Decide from the `--version` outputs of the staged and the installed
/// binary. Same version token: nothing to do unless `force`.
/// Without a signature key the staged binaries are unauthenticated beyond TLS:
/// ask before the `--version` probe executes them (#153/#154).
fn confirm_before_probe(has_key: bool, yes: bool) -> bool {
    !has_key && !yes
}

fn plan_install(staged: &str, installed: &str, tag: Option<&str>, force: bool) -> Result<Plan> {
    let s = version_token(staged)
        .with_context(|| format!("cannot read the version of the staged binary ({staged:?})"))?;
    // #154: the signature covers the binaries, and the binaries carry the
    // version stamp, so this binds the signed asset set to a version.
    let bare = |v: &str| v.strip_prefix('v').unwrap_or(v).to_string();
    match tag {
        Some(t) if bare(s) != bare(t) => {
            bail!("release asset reports {s} but tag {t} was requested; refusing to install assets of another release")
        }
        None if version_is_newer(s, installed) => {
            bail!("refusing to downgrade {installed} to {s} from the latest release; pass --tag {s} to downgrade")
        }
        _ => {}
    }
    if !force && version_token(installed) == Some(s) {
        return Ok(Plan::UpToDate);
    }
    Ok(Plan::Install)
}

/// Seconds a restarted unit must stay active, longer than the units'
/// `RestartSec=5` so a crash loop shows up as a state or `NRestarts` change.
const RESTART_SETTLE_SECS: u64 = 10;

/// Verdict on a unit `RESTART_SETTLE_SECS` after its restart: still
/// `active` and no automatic restart in between.
fn check_settled(unit: &str, state: &str, restarts_before: Option<u64>, restarts_after: Option<u64>) -> Result<()> {
    if state != "active" {
        bail!("{unit} is {state} {RESTART_SETTLE_SECS}s after restart");
    }
    if let (Some(b), Some(a)) = (restarts_before, restarts_after) {
        if a > b {
            bail!("{unit} restarted {} time(s) within {RESTART_SETTLE_SECS}s (crash loop?)", a - b);
        }
    }
    Ok(())
}

fn installed_version(install_dir: &str, bin: &str) -> String {
    binary_version(Path::new(&format!("{install_dir}/{bin}")))
}

/// Trimmed `--version` output of `path`, or `not installed`.
fn binary_version(path: &Path) -> String {
    match Command::new(path).arg("--version").output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => "not installed".to_string(),
    }
}

// ---- external tools -----------------------------------------------------

const MAX_REDIRECTS: usize = 5;
const SUMS_ASSET: &str = "SHA256SUMS";

/// Same directory as `url`, different file name.
fn sibling_url(url: &str, name: &str) -> String {
    match url.rsplit_once('/') {
        Some((dir, _)) => format!("{dir}/{name}"),
        None => name.to_string(),
    }
}

/// curl argv for ONE hop. No `-L`: curl re-sends custom headers (the
/// token) to redirect targets on other hosts, so redirects are
/// followed by hand and only the first hop carries `headers`. https only on
/// every hop so a hostile redirect cannot point at a local file.
fn curl_hop_args(url: &str, headers: &[String], out: &Path, first_hop: bool) -> Vec<String> {
    let mut a: Vec<String> = ["-fsS", "--proto", "=https", "--tlsv1.2", "-w", "%{redirect_url}"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    // Headers (the token) are read from stdin, never argv: argv is
    // world-readable via /proc/<pid>/cmdline.
    if curl_hop_stdin(headers, first_hop).is_some() {
        a.push("-H".into());
        a.push("@-".into());
    }
    a.push("-o".into());
    a.push(out.display().to_string());
    a.push(url.to_string());
    a
}

/// Header lines curl reads from stdin (`-H @-`) for one hop; `None` when
/// the hop sends no headers (later hops, or no token).
fn curl_hop_stdin(headers: &[String], first_hop: bool) -> Option<String> {
    (first_hop && !headers.is_empty()).then(|| headers.iter().map(|h| format!("{h}\n")).collect())
}

/// Redirect target from curl's `-w %{redirect_url}` output, if any.
fn redirect_target(stdout: &str) -> Option<String> {
    let t = stdout.trim();
    (!t.is_empty()).then(|| t.to_string())
}

fn curl_download(url: &str, headers: &[String], out: &Path) -> Result<()> {
    let mut cur = url.to_string();
    for hop in 0..=MAX_REDIRECTS {
        let stdin = curl_hop_stdin(headers, hop == 0);
        let mut child = Command::new("curl")
            .args(curl_hop_args(&cur, headers, out, hop == 0))
            .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("running curl (is curl installed?)")?;
        if let (Some(s), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(s.as_bytes()).context("writing curl headers to stdin")?;
        }
        let o = child.wait_with_output().context("waiting for curl")?;
        if !o.status.success() {
            bail!(
                "curl failed downloading {cur}: {}",
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        match redirect_target(&String::from_utf8_lossy(&o.stdout)) {
            None => return Ok(()),
            Some(next) => cur = next,
        }
    }
    bail!("too many redirects downloading {url}")
}

/// Verify `sums` (the release SHA256SUMS) against its minisign signature
/// under `key` (base64 line or full .pub text), then require the line for
/// the tarball to equal `tarball_sha`. Pure so it is testable.
fn verify_signed_sums(key: &str, sums: &[u8], sig: &str, tarball_sha: &str) -> Result<()> {
    let pk = if key.trim().contains('\n') {
        minisign_verify::PublicKey::decode(key.trim())
    } else {
        minisign_verify::PublicKey::from_base64(key.trim())
    }
    .map_err(|e| anyhow::anyhow!("invalid minisign public key: {e}"))?;
    let sig = minisign_verify::Signature::decode(sig)
        .map_err(|e| anyhow::anyhow!("invalid SHA256SUMS.minisig: {e}"))?;
    pk.verify(sums, &sig, false)
        .map_err(|e| anyhow::anyhow!("SHA256SUMS signature verification failed: {e}"))?;
    check_sums_entry(std::str::from_utf8(sums).context("SHA256SUMS is not UTF-8")?, tarball_sha)
}

fn check_sums_entry(sums: &str, tarball_sha: &str) -> Result<()> {
    let want = manifest_sha(sums, ASSET)
        .with_context(|| format!("signed SHA256SUMS has no entry for {ASSET}"))?;
    if want != tarball_sha {
        bail!("{ASSET} sha256 {tarball_sha} does not match signed SHA256SUMS {want}");
    }
    Ok(())
}

fn curl_get(url: &str, headers: &[String]) -> Result<String> {
    // Same hop-by-hop path as downloads so the token never follows a redirect.
    let dir = tempfile::tempdir().context("creating temp dir")?;
    let out = dir.path().join("body");
    curl_download(url, headers, &out)?;
    std::fs::read_to_string(&out).with_context(|| format!("reading response of GET {url}"))
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

/// Restart `svc` if it exists on this host (a host without e.g. the data
/// role must not abort the rest of an `update all`). Per-unit failures are
/// pushed to `failures` so remaining units still restart.
fn restart_service(svc: &str, failures: &mut Vec<String>) -> Result<()> {
    let units = if svc.contains('*') {
        list_units(svc)?
    } else if unit_exists(svc)? {
        vec![svc.to_string()]
    } else {
        Vec::new()
    };
    if units.is_empty() {
        eprintln!("no {svc} unit on this host; nothing to restart");
    }
    for u in units {
        match systemctl(&["restart", &u]).and_then(|_| assert_active(&u)) {
            Ok(()) => eprintln!("restarted {u}"),
            Err(e) => {
                eprintln!("FAILED {u}: {e:#}");
                failures.push(u);
            }
        }
    }
    Ok(())
}

/// Report every unit that failed to restart, as one non-zero exit.
fn finish_restarts(failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    bail!(
        "binaries installed but {} service(s) failed to restart: {}",
        failures.len(),
        failures.join(", ")
    )
}

fn unit_exists(unit: &str) -> Result<bool> {
    let out = Command::new("systemctl")
        .args(["list-unit-files", "--no-legend", "--plain", unit])
        .output()
        .context("running systemctl list-unit-files")?;
    Ok(out.status.success() && !String::from_utf8_lossy(&out.stdout).trim().is_empty())
}

fn list_units(pattern: &str) -> Result<Vec<String>> {
    let out = Command::new("systemctl")
        .args(["list-units", "--plain", "--no-legend", "--type=service", pattern])
        .output()
        .context("running systemctl list-units")?;
    if !out.status.success() {
        bail!("systemctl list-units {pattern} failed");
    }
    Ok(parse_unit_names(&String::from_utf8_lossy(&out.stdout)))
}

/// First column of `systemctl list-units --plain --no-legend`; failed
/// instances are listed too (no marker with --plain, tolerated anyway).
fn parse_unit_names(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .filter_map(|l| l.split_whitespace().find(|t| *t != "●"))
        .map(str::to_string)
        .collect()
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
    let state = unit_state(unit)?;
    if state != "active" {
        bail!("{unit} did not come back active ({state})");
    }
    let before = n_restarts(unit);
    eprintln!("waiting {RESTART_SETTLE_SECS}s for {unit} to stay up");
    std::thread::sleep(std::time::Duration::from_secs(RESTART_SETTLE_SECS));
    check_settled(unit, &unit_state(unit)?, before, n_restarts(unit))
}

/// `systemctl is-active` state word (`active`, `activating`, `failed`, ...).
fn unit_state(unit: &str) -> Result<String> {
    let out = Command::new("systemctl")
        .args(["is-active", unit])
        .output()
        .context("running systemctl is-active")?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Automatic restarts systemd counted for `unit`; `None` when unreadable.
fn n_restarts(unit: &str) -> Option<u64> {
    let out = Command::new("systemctl")
        .args(["show", "-p", "NRestarts", "--value", unit])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
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
    fn probe_prompt_only_without_key_and_without_yes() {
        assert!(confirm_before_probe(false, false));
        assert!(!confirm_before_probe(false, true));
        assert!(!confirm_before_probe(true, false));
        assert!(!confirm_before_probe(true, true));
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
    fn release_source_is_github_only() {
        assert_eq!(
            release_source("https://github.com/lischetzke/dtfs"),
            Some(ReleaseSource::GitHub { owner: "lischetzke".into(), repo: "dtfs".into() })
        );
        for bad in ["https://git.example/a/b", "https://gl.example", "not a url"] {
            assert_eq!(release_source(bad), None, "{bad}");
        }
    }

    #[test]
    fn token_header_is_bearer() {
        assert_eq!(token_header("ghp_abc"), "Authorization: Bearer ghp_abc");
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
    fn minisign_key_precedence_flag_env_config() {
        assert_eq!(resolve_minisign_key(Some("f"), Some("e"), Some("c")).as_deref(), Some("f"));
        assert_eq!(resolve_minisign_key(None, Some("e"), Some("c")).as_deref(), Some("e"));
        assert_eq!(resolve_minisign_key(Some(" "), None, Some("c")).as_deref(), Some("c"));
        assert_eq!(resolve_minisign_key(Some(""), Some("\n"), Some(" ")), None);
        assert_eq!(resolve_minisign_key(None, None, None), None);
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
        assert_eq!(parse_owner_repo("https://example.com/a/b"), None);
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

    const MS_KEY: &str = "RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3";
    const MS_SIG: &str = "untrusted comment: signature from minisign secret key\nRUQf6LRCGA9i559r3g7V1qNyJDApGip8MfqcadIgT9CuhV3EMhHoN1mGTkUidF/z7SrlQgXdy8ofjb7bNJJylDOocrCo8KLzZwo=\ntrusted comment: timestamp:1633700835\tfile:test\tprehashed\nwLMDjy9FLAuxZ3q4NlEvkgtyhrr0gtTu6KC4KBJdITbbOeAi1zBIYo0v4iTgt8jJpIidRJnp94ABQkJAgAooBQ==\n";

    #[test]
    fn minisign_verification_gate() {
        // Valid signature over "test" passes the signature step and then
        // fails on the missing tarball entry (proves the sig was accepted).
        let e = verify_signed_sums(MS_KEY, b"test", MS_SIG, "x").unwrap_err().to_string();
        assert!(e.contains("no entry"), "{e}");
        // Tampered content is refused at the signature step.
        let e = verify_signed_sums(MS_KEY, b"Test", MS_SIG, "x").unwrap_err().to_string();
        assert!(e.contains("signature verification failed"), "{e}");
        // Garbage key / signature.
        assert!(verify_signed_sums("nope", b"test", MS_SIG, "x").unwrap_err().to_string().contains("invalid minisign public key"));
        assert!(verify_signed_sums(MS_KEY, b"test", "junk", "x").unwrap_err().to_string().contains("invalid SHA256SUMS.minisig"));
    }

    #[test]
    fn signed_sums_entry_must_match_tarball() {
        let sha = "a".repeat(64);
        let sums = format!("{sha}  {ASSET}\n");
        assert!(check_sums_entry(&sums, &sha).is_ok());
        assert!(check_sums_entry(&sums, &"b".repeat(64)).unwrap_err().to_string().contains("does not match"));
        assert!(check_sums_entry(&format!("{sha}  other.tar.gz\n"), &sha).is_err());
    }

    #[test]
    fn curl_hops_never_follow_redirects_with_token() {
        let h = vec!["Authorization: Bearer t".to_string()];
        let out = Path::new("/tmp/o");
        let first = curl_hop_args("https://gl/x", &h, out, true);
        assert!(!first.contains(&"-L".to_string()) && !first.contains(&"-fL".to_string()));
        assert!(first.contains(&"=https".to_string()) && !first.iter().any(|a| a.contains("file")));
        assert_eq!(first.last().unwrap(), "https://gl/x");
        let next = curl_hop_args("https://s3/y", &h, out, false);
        assert!(!next.contains(&"Authorization: Bearer t".to_string()) && !next.contains(&"-H".to_string()));
        assert!(next.contains(&"=https".to_string()));
        assert_eq!(redirect_target("https://s3/y\n").as_deref(), Some("https://s3/y"));
        assert_eq!(redirect_target(""), None);
        assert_eq!(sibling_url("https://h/p/dtfs/v1/dtfs-linux-amd64.tar.gz", "SHA256SUMS"), "https://h/p/dtfs/v1/SHA256SUMS");
    }

    /// argv is world-readable via /proc/<pid>/cmdline: the token must go
    /// through curl's stdin (`-H @-`), never the command line.
    #[test]
    fn curl_token_never_in_argv() {
        let h = vec![token_header("ghp_SECRET")];
        let out = Path::new("/tmp/o");
        let first = curl_hop_args("https://gl/x", &h, out, true);
        assert!(!first.iter().any(|a| a.contains("ghp_SECRET")), "{first:?}");
        assert!(first.windows(2).any(|w| w[0] == "-H" && w[1] == "@-"), "{first:?}");
        assert_eq!(curl_hop_stdin(&h, true).as_deref(), Some("Authorization: Bearer ghp_SECRET\n"));
        // Later hops send no header at all, and no token-less run reads stdin.
        assert_eq!(curl_hop_stdin(&h, false), None);
        assert_eq!(curl_hop_stdin(&[], true), None);
        assert!(!curl_hop_args("https://gl/x", &[], out, true).contains(&"-H".to_string()));
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
    fn unit_names_include_failed_instances() {
        let out = "dtfs-fuse@mnt-a.service loaded active running DTFS\n\
                   dtfs-fuse@mnt-b.service loaded failed failed DTFS\n\
                   ● dtfs-fuse@mnt-c.service loaded failed failed DTFS\n";
        assert_eq!(
            parse_unit_names(out),
            ["dtfs-fuse@mnt-a.service", "dtfs-fuse@mnt-b.service", "dtfs-fuse@mnt-c.service"]
        );
        assert!(parse_unit_names("").is_empty());
    }

    #[test]
    fn restart_failures_are_all_reported() {
        assert!(finish_restarts(vec![]).is_ok());
        let e = finish_restarts(vec!["a.service".into(), "b.service".into()]).unwrap_err().to_string();
        assert!(e.contains("2 service(s)") && e.contains("a.service, b.service"), "{e}");
    }

    #[test]
    fn same_version_is_skipped_unless_forced() {
        let staged = "dtfs-manager v0.1.14 (built 20261110-000000)";
        // Same version, different build date: nothing to do.
        assert_eq!(plan_install(staged, "dtfs-manager v0.1.14 (built 20261101-000000)", None, false).unwrap(), Plan::UpToDate);
        assert_eq!(plan_install(staged, "dtfs-manager v0.1.14 (built 20261101-000000)", None, true).unwrap(), Plan::Install);
        assert_eq!(plan_install(staged, "dtfs-manager v0.1.13 (built x)", None, false).unwrap(), Plan::Install);
        // Suffix differs from the release: not the same version.
        assert_eq!(plan_install(staged, "dtfs-manager v0.1.14-rc1 (built x)", None, false).unwrap(), Plan::Install);
        assert_eq!(plan_install(staged, "not installed", None, false).unwrap(), Plan::Install);
    }

    #[test]
    fn staged_binary_without_version_is_refused() {
        let e = plan_install("dtfs-manager (built x)", "not installed", None, true).unwrap_err().to_string();
        assert!(e.contains("cannot read the version of the staged binary"), "{e}");
    }

    #[test]
    fn signed_assets_of_another_version_are_refused_for_a_tag() {
        let staged = "dtfs-manager v0.1.13 (built x)";
        // #154: a validly signed older set served under v0.1.14.
        let e = plan_install(staged, "dtfs-manager v0.1.12 (built x)", Some("v0.1.14"), true).unwrap_err().to_string();
        assert!(e.contains("reports v0.1.13 but tag v0.1.14 was requested"), "{e}");
        // Matching tag installs, with or without the leading v; explicit downgrade is allowed.
        assert_eq!(plan_install(staged, "dtfs-manager v0.1.14 (built x)", Some("v0.1.13"), false).unwrap(), Plan::Install);
        assert_eq!(plan_install(staged, "not installed", Some("0.1.13"), false).unwrap(), Plan::Install);
    }

    #[test]
    fn latest_never_downgrades() {
        let staged = "dtfs-manager v0.1.13 (built x)";
        let e = plan_install(staged, "dtfs-manager v0.1.14 (built x)", None, true).unwrap_err().to_string();
        assert!(e.contains("refusing to downgrade dtfs-manager v0.1.14 (built x) to v0.1.13"), "{e}");
        // Boundary: one patch newer installs.
        assert_eq!(plan_install("dtfs-manager v0.1.15 (built x)", "dtfs-manager v0.1.14 (built x)", None, false).unwrap(), Plan::Install);
    }

    #[test]
    fn restart_settle_requires_active_and_no_auto_restart() {
        assert!(check_settled("m.service", "active", Some(0), Some(0)).is_ok());
        // NRestarts unreadable on an old systemd: state alone decides.
        assert!(check_settled("m.service", "active", None, None).is_ok());
        let e = check_settled("m.service", "activating", Some(0), Some(0)).unwrap_err().to_string();
        assert!(e.contains("m.service is activating 10s after restart"), "{e}");
        // Boundary: one automatic restart inside the window is a crash loop.
        let e = check_settled("m.service", "active", Some(3), Some(4)).unwrap_err().to_string();
        assert!(e.contains("m.service restarted 1 time(s) within 10s"), "{e}");
    }

    #[test]
    fn config_paths_cover_all_roles_for_cli_and_all() {
        assert_eq!(default_config_paths(UpdateTarget::Manager), [PathBuf::from("/etc/dtfs/manager.toml")]);
        for t in [UpdateTarget::Cli, UpdateTarget::All] {
            assert_eq!(default_config_paths(t).len(), 3);
        }
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
