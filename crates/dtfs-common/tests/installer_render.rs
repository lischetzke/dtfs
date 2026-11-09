//! Renders each role's config with the real functions from
//! scripts/dtfs-install.sh and parses it with the real config structs, so the
//! installer can never again emit a file the daemons refuse to load.
use dtfs_common::config::{load_config, DataNodeConfig, FuseConfig, ManagerConfig};
use std::path::PathBuf;
use std::process::Command;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/dtfs-install.sh")
}

/// Runs parse_managers + render_<role> in bash; returns (exit ok, stderr+stdout).
fn render(role: &str, cfg: &std::path::Path, env: &[(&str, &str)]) -> (bool, String) {
    let driver = r#"
error() { echo "ERR: $*"; }
chown() { :; }
eval "$(sed -n '/^parse_managers()/,/^}/p;/^render_manager()/,/^}/p;/^render_data()/,/^}/p;/^render_fuse()/,/^}/p' "$SCRIPT")"
set -e
parse_managers
"render_$FN"
"#;
    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(driver).env("SCRIPT", script()).env("CFG", cfg).env("FN", role);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("bash available");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

const BASE: &[(&str, &str)] = &[
    ("HOSTNAME_FQ", "mgr01"),
    ("CLUSTER_NAME", "c"),
    ("SHARED_SECRET", "abcd"),
    ("MOUNTPOINT", "/mnt/dtfs"),
    ("DISKS", "/mnt/a,/mnt/b"),
];

fn with(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<_> = BASE.to_vec();
    v.extend_from_slice(extra);
    v
}

#[test]
fn rendered_manager_config_loads_with_matching_voter_hostnames() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let env = with(&[
        ("ROLE", "manager"),
        ("BOOTSTRAP", "true"),
        ("MGR_LIST", "mgr01=10.0.0.1:9401,mgr02=10.0.0.2:9401"),
    ]);
    let (ok, out) = render("manager", &p, &env);
    assert!(ok, "{out}");
    let cfg: ManagerConfig = load_config(&p).expect("rendered manager.toml must load");
    assert_eq!(cfg.cluster.initial_voters.len(), 2);
    assert!(cfg.cluster.initial_voters.iter().any(|v| v.hostname == "mgr01" && v.addr == "10.0.0.1:9401"));
    assert!(cfg.cluster.initial_voters.iter().any(|v| v.hostname == "mgr02"));
}

#[test]
fn rendered_data_and_fuse_configs_load_and_strip_hostnames() {
    let d = tempfile::tempdir().unwrap();
    let env = with(&[("ROLE", "data"), ("MGR_LIST", "mgr01=10.0.0.1:9401,10.0.0.2:9401")]);
    let p = d.path().join("data.toml");
    let (ok, out) = render("data", &p, &env);
    assert!(ok, "{out}");
    let cfg: DataNodeConfig = load_config(&p).expect("rendered data.toml must load");
    assert_eq!(cfg.cluster.manager_addr, "10.0.0.1:9401");

    let env = with(&[("ROLE", "fuse-client"), ("MGR_LIST", "mgr01=10.0.0.1:9401")]);
    let p = d.path().join("fuse.toml");
    let (ok, out) = render("fuse", &p, &env);
    assert!(ok, "{out}");
    let _: FuseConfig = load_config(&p).expect("rendered fuse.toml must load");
}

#[test]
fn bare_manager_addresses_are_refused_for_manager_role() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let env = with(&[("ROLE", "manager"), ("BOOTSTRAP", "false"), ("MGR_LIST", "10.0.0.1:9401,10.0.0.2:9401")]);
    let (ok, out) = render("manager", &p, &env);
    assert!(!ok && out.contains("has no hostname"), "{out}");
    assert!(!p.exists());
}

#[test]
fn bootstrap_must_be_named_in_manager_list() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let env = with(&[("ROLE", "manager"), ("BOOTSTRAP", "true"), ("MGR_LIST", "other=10.0.0.2:9401,x=10.0.0.3:9401")]);
    let (ok, out) = render("manager", &p, &env);
    assert!(!ok && out.contains("is not named in --managers"), "{out}");
}

/// Runs `body` in bash after loading the named installer functions (top-level
/// `name()` up to `}`, or 4-space-indented nested ones). `curl` / `whiptail`
/// are stubbed by `body`, recording argv to $ARGV (argv is world-readable via
/// /proc/<pid>/cmdline) and stdin to $STDIN.
fn run_fns(fns: &[&str], body: &str, dir: &std::path::Path) -> (String, String, String) {
    let ranges: String = fns
        .iter()
        .map(|f| format!("/^{f}()/,/^}}/p;/^    {f}()/,/^    }}/p;"))
        .collect();
    let driver = format!(
        "error() {{ echo \"ERR: $*\"; }}\neval \"$(sed -n '{ranges}' \"$SCRIPT\")\"\nset -euo pipefail\n{body}"
    );
    let argv = dir.join("argv");
    let stdin = dir.join("stdin");
    let out = Command::new("bash")
        .arg("-c")
        .arg(driver)
        .env("SCRIPT", script())
        .env("ARGV", &argv)
        .env("STDIN", &stdin)
        .output()
        .expect("bash available");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "{text}");
    let read = |p: &std::path::Path| std::fs::read_to_string(p).unwrap_or_default();
    (text, read(&argv), read(&stdin))
}

#[test]
fn repo_token_goes_to_curl_stdin_not_argv() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
curl() { printf '%s\n' "$@" >> "$ARGV"; cat >> "$STDIN"; }
REPO_TOKEN=ghp_SECRET
fetch_hops https://example.invalid/x "$ARGV.out"
"#;
    let (out, argv, stdin) = run_fns(&["repo_auth_header", "fetch_hops"], body, d.path());
    assert!(!argv.contains("ghp_SECRET"), "token in curl argv: {argv}");
    assert!(argv.lines().any(|l| l == "@-"), "{argv}{out}");
    assert_eq!(stdin, "Authorization: Bearer ghp_SECRET\n");
}

#[test]
fn no_repo_token_sends_no_header() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
curl() { printf '%s\n' "$@" >> "$ARGV"; cat >> "$STDIN"; }
REPO_TOKEN=
fetch_hops https://example.invalid/x "$ARGV.out"
"#;
    let (out, argv, stdin) = run_fns(&["repo_auth_header", "fetch_hops"], body, d.path());
    assert!(!argv.lines().any(|l| l == "-H"), "{argv}{out}");
    assert_eq!(stdin, "");
}

#[test]
fn secret_prompt_never_puts_secret_in_whiptail_argv() {
    let d = tempfile::tempdir().unwrap();
    // Operator leaves the field blank: the existing/generated secret is kept.
    let body = r#"
whiptail() { printf '%s\n' "$@" >> "$ARGV"; printf '' >&2; }
NONINT=0
SHARED_SECRET=deadbeefsecret
ask_secret SHARED_SECRET "Auth" "Cluster shared secret:"
echo "result=$SHARED_SECRET"
"#;
    let (out, argv, _) = run_fns(&["ask_secret"], body, d.path());
    assert!(!argv.contains("deadbeefsecret"), "secret in whiptail argv: {argv}");
    assert!(argv.lines().any(|l| l == "--passwordbox"), "{argv}");
    assert!(out.contains("result=deadbeefsecret"), "{out}");
}

#[test]
fn secret_prompt_takes_typed_value() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
whiptail() { printf 'typedsecret' >&2; }
NONINT=0
SHARED_SECRET=old
ask_secret SHARED_SECRET "Auth" "Cluster shared secret:"
echo "result=$SHARED_SECRET"
"#;
    let (out, _, _) = run_fns(&["ask_secret"], body, d.path());
    assert!(out.contains("result=typedsecret"), "{out}");
}

#[test]
fn single_bare_entry_is_self_for_bootstrap() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let env = with(&[("ROLE", "manager"), ("BOOTSTRAP", "true"), ("MGR_LIST", "10.0.0.1:9401")]);
    let (ok, out) = render("manager", &p, &env);
    assert!(ok, "{out}");
    let cfg: ManagerConfig = load_config(&p).unwrap();
    assert_eq!(cfg.cluster.initial_voters[0].hostname, "mgr01");
}
