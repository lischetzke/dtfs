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

/// #152 — `--allow-non-mountpoint` renders the opt-out on every disk; the
/// default renders none, so a fresh unmounted disk still refuses to start.
#[test]
fn data_disks_allow_non_mountpoint_only_when_opted_in() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("data.toml");
    for (flag, want) in [("0", false), ("1", true)] {
        let env = with(&[("ROLE", "data"), ("MGR_LIST", "10.0.0.1:9401"), ("ALLOW_NON_MOUNTPOINT", flag)]);
        let (ok, out) = render("data", &p, &env);
        assert!(ok, "{out}");
        let cfg: DataNodeConfig = load_config(&p).expect("rendered data.toml must load");
        assert_eq!(cfg.disks.len(), 2);
        assert!(cfg.disks.iter().all(|disk| disk.allow_non_mountpoint == want), "flag {flag}");
    }
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

/// #154: the staged binaries carry the version stamp, so the requested (or
/// resolved latest) tag must equal it.
#[test]
fn release_version_gate_binds_signed_assets_to_the_tag() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
gate() { if release_version_gate "$1" "$2"; then echo "ok:$STAGED_VERSION"; else echo refused; fi; }
gate v0.1.14 "dtfs-cli v0.1.14 (built 20261110-000000)"
gate 0.1.14 "dtfs-cli v0.1.14 (built 20261110-000000)"
gate "" "dtfs-cli v0.1.13 (built x)"
"#;
    let (out, _, _) = run_fns(&["version_token", "release_version_gate"], body, d.path());
    assert_eq!(out.lines().filter(|l| l.starts_with("ok:")).collect::<Vec<_>>(), ["ok:v0.1.14", "ok:v0.1.14", "ok:v0.1.13"], "{out}");
}

#[test]
fn release_version_gate_refuses_another_release_and_unreadable_version() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
gate() { if release_version_gate "$1" "$2"; then echo "ok:$STAGED_VERSION"; else echo refused; fi; }
gate v0.1.14 "dtfs-cli v0.1.13 (built x)"
gate v0.1.14 ""
"#;
    let (out, _, _) = run_fns(&["version_token", "release_version_gate"], body, d.path());
    assert!(out.contains("ERR: release asset reports v0.1.13 but tag v0.1.14 was requested"), "{out}");
    assert!(out.contains("ERR: cannot read the version of the staged dtfs-cli"), "{out}");
    assert_eq!(out.matches("refused").count(), 2, "{out}");
}

/// Fake staging root with tarballs for `versions`; returns its path.
fn fake_staging(dir: &std::path::Path, versions: &[&str]) -> PathBuf {
    let root = dir.join("staging");
    for v in versions {
        let d = root.join(v);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(format!("dtfs-{v}-linux-amd64.tar.gz")), b"x").unwrap();
    }
    root
}

const SELECT_FNS: &[&str] = &["select_tarball_local", "select_auto"];

/// #155: `auto` resolves the latest tag first and uses a local file only
/// when it is that release.
#[test]
fn auto_source_prefers_the_remote_latest_over_an_old_staged_tarball() {
    let d = tempfile::tempdir().unwrap();
    let root = fake_staging(d.path(), &["v0.1.9", "v0.1.10"]);
    let body = format!(
        r#"
info() {{ :; }}; warn() {{ echo "WARN: $*"; }}
STAGING_ROOT='{}'; DOWNLOAD_CACHE=/nonexistent; VERSION=""; EXPECT_TAG=""; TARBALL=""
resolve_latest_tag() {{ echo v0.1.14; }}
download_tarball_remote() {{ echo "download:$VERSION"; TARBALL=/dl/dtfs-$VERSION-linux-amd64.tar.gz; }}
select_auto
echo "tarball=$TARBALL expect=$EXPECT_TAG local=$LOCAL_UNTAGGED"
"#,
        root.display()
    );
    let (out, _, _) = run_fns(SELECT_FNS, &body, d.path());
    assert!(out.contains("download:v0.1.14"), "{out}");
    assert!(out.contains("tarball=/dl/dtfs-v0.1.14-linux-amd64.tar.gz expect=v0.1.14 local=0"), "{out}");

    // The latest release already staged: reused, no download.
    let root = fake_staging(d.path(), &["v0.1.14"]);
    let body = body.replace("download:$VERSION", "UNEXPECTED download");
    let (out, _, _) = run_fns(SELECT_FNS, &body, d.path());
    assert!(!out.contains("UNEXPECTED"), "{out}");
    assert!(out.contains(&format!("tarball={}/v0.1.14/dtfs-v0.1.14-linux-amd64.tar.gz expect=v0.1.14", root.display())), "{out}");
}

#[test]
fn auto_source_without_remote_takes_the_newest_local_by_version() {
    let d = tempfile::tempdir().unwrap();
    // Lexical order would pick v0.1.9 last and v0.1.10 first; version order picks v0.1.14.
    let root = fake_staging(d.path(), &["v0.1.10", "v0.1.14", "v0.1.9"]);
    let body = format!(
        r#"
info() {{ :; }}; warn() {{ echo "WARN: $*"; }}
STAGING_ROOT='{}'; DOWNLOAD_CACHE=/nonexistent; VERSION=""; EXPECT_TAG=""; TARBALL=""
resolve_latest_tag() {{ return 1; }}
download_tarball_remote() {{ echo UNEXPECTED; }}
select_auto
echo "tarball=$TARBALL expect=$EXPECT_TAG local=$LOCAL_UNTAGGED"
"#,
        root.display()
    );
    let (out, _, _) = run_fns(SELECT_FNS, &body, d.path());
    assert!(out.contains("WARN: AZ-2: cannot resolve the latest release"), "{out}");
    assert!(out.contains(&format!("tarball={}/v0.1.14/dtfs-v0.1.14-linux-amd64.tar.gz expect= local=1", root.display())), "{out}");

    // Nothing local and no remote: refused.
    let body = body
        .replace(&root.display().to_string(), "/nonexistent")
        .replace("select_auto\n", "if select_auto; then echo picked; else echo none; fi\n");
    let (out, _, _) = run_fns(SELECT_FNS, &body, d.path());
    assert!(out.contains("ERR: AZ-2: no local source found and GitHub fallback failed"), "{out}");
    assert!(out.contains("none") && !out.contains("picked"), "{out}");
}

/// A locally found tarball with no tag to check against must not downgrade.
#[test]
fn untagged_local_tarball_never_downgrades() {
    let d = tempfile::tempdir().unwrap();
    let body = r#"
gate() { if release_version_gate "$1" "$2" "$3"; then echo "ok:$STAGED_VERSION"; else echo refused; fi; }
gate "" "dtfs-cli v0.1.13 (built x)" "dtfs-cli v0.1.14 (built y)"
gate "" "dtfs-cli v0.1.14 (built x)" "dtfs-cli v0.1.14 (built y)"
gate "" "dtfs-cli v0.1.15 (built x)" "dtfs-cli v0.1.14 (built y)"
gate "" "dtfs-cli v0.1.13 (built x)" ""
"#;
    let (out, _, _) = run_fns(&["version_token", "release_version_gate"], body, d.path());
    assert!(out.contains("ERR: refusing to downgrade v0.1.14 to v0.1.13"), "{out}");
    let res: Vec<&str> = out.lines().filter(|l| l.starts_with("ok:") || *l == "refused").collect();
    assert_eq!(res, ["refused", "ok:v0.1.14", "ok:v0.1.15", "ok:v0.1.13"], "{out}");
}

/// #126: the installer leaves Raft timing to the code defaults.
#[test]
fn rendered_manager_config_uses_default_raft_settings() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("manager.toml");
    let env = with(&[("ROLE", "manager"), ("BOOTSTRAP", "true"), ("MGR_LIST", "mgr01=10.0.0.1:9401")]);
    let (ok, out) = render("manager", &p, &env);
    assert!(ok, "{out}");
    let cfg: ManagerConfig = load_config(&p).unwrap();
    assert_eq!(
        (cfg.raft.heartbeat_interval_ms, cfg.raft.election_timeout_ms, cfg.raft.snapshot_threshold),
        (150, 400, 10000)
    );
}

/// Key paths in `rendered` that `loaded` (the parsed struct serialized back)
/// does not have: keys no config struct reads.
fn unread_keys(rendered: &toml::Value, loaded: &toml::Value, path: &str, out: &mut Vec<String>) {
    match (rendered, loaded) {
        (toml::Value::Table(r), toml::Value::Table(l)) => {
            for (k, v) in r {
                let p = format!("{path}.{k}");
                match l.get(k) {
                    Some(lv) => unread_keys(v, lv, &p, out),
                    None => out.push(p),
                }
            }
        }
        (toml::Value::Array(r), toml::Value::Array(l)) => {
            for (i, (rv, lv)) in r.iter().zip(l).enumerate() {
                unread_keys(rv, lv, &format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

/// #132: every key the installer renders is read by the role's config struct.
#[test]
fn installer_renders_no_key_the_config_structs_ignore() {
    fn check<T: serde::de::DeserializeOwned + serde::Serialize>(role: &'static str, fn_name: &str, extra: &[(&'static str, &'static str)]) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join(format!("{fn_name}.toml"));
        let mut env = with(&[("ROLE", role)]);
        env.extend_from_slice(extra);
        let (ok, out) = render(fn_name, &p, &env);
        assert!(ok, "{out}");
        let text = std::fs::read_to_string(&p).unwrap();
        let rendered: toml::Value = toml::from_str(&text).unwrap();
        let cfg: T = load_config(&p).unwrap();
        let loaded = toml::Value::try_from(&cfg).unwrap();
        let mut unread = Vec::new();
        unread_keys(&rendered, &loaded, "", &mut unread);
        assert!(unread.is_empty(), "{role}: rendered keys no struct reads: {unread:?}");
    }
    let mgr = [("BOOTSTRAP", "true"), ("MGR_LIST", "mgr01=10.0.0.1:9401")];
    check::<ManagerConfig>("manager", "manager", &mgr);
    check::<DataNodeConfig>("data", "data", &[("MGR_LIST", "mgr01=10.0.0.1:9401")]);
    check::<FuseConfig>("fuse-client", "fuse", &[("MGR_LIST", "mgr01=10.0.0.1:9401")]);
}
