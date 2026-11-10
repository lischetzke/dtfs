//! DTFS Administration CLI.
//!
//! Connects to a manager node's REST API to perform cluster operations.

use clap::{Parser, Subcommand};
use serde_json::{json, Value};

mod updater;

#[derive(Parser, Debug)]
#[command(
    name = "dtfs-cli",
    about = "DTFS Administration CLI",
    version = dtfs_common::version::VERSION_STRING,
    long_about = "Command-line tool for administering a DTFS distributed storage cluster."
)]
struct Args {
    /// Manager REST API address (host:port)
    #[arg(short, long, default_value = "localhost:9402", env = "DTFS_MANAGER")]
    manager: String,

    /// Cluster shared secret for mutating REST (`X-Dtfs-Shared-Secret`).
    /// Required when the manager has `[cluster].shared_secret` set (LIS-18).
    #[arg(long, global = true, default_value = "", env = "DTFS_SHARED_SECRET", hide_env_values = true)]
    secret: String,

    /// Output format
    #[arg(long, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum PrecacheCmd {
    /// Show the leader's pre-cache plan (use the leader's --manager; a follower answers 409)
    Status,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show cluster status
    Status,

    /// List all data nodes (`nodes remove-dead <id>` clears a dead node)
    #[command(args_conflicts_with_subcommands = true)]
    Nodes {
        /// Show detailed info for a specific node
        #[arg(long)]
        id: Option<String>,
        #[command(subcommand)]
        cmd: Option<NodesCmd>,
    },

    /// Manage volumes
    #[command(subcommand)]
    Volumes(VolumesCmd),

    /// File operations (list, flush cache)
    #[command(subcommand)]
    Files(FilesCmd),

    /// UIP-4-3 (D3) — manual data migration (operator-triggered
    /// tier/DC moves of a path prefix).
    #[command(subcommand)]
    Migrations(MigrationsCmd),

    /// Predictive pre-caching observability
    #[command(subcommand)]
    Precache(PrecacheCmd),

    /// Trigger a full scrub of all disks (the server has no per-target scrub)
    Scrub {
        /// Scrub level: quick or deep
        #[arg(long, default_value = "deep")]
        level: String,
        #[command(subcommand)]
        action: Option<ScrubCmd>,
    },

    /// Maintenance mode operations
    #[command(group(clap::ArgGroup::new("maintenance_mode").required(true).args(["enable", "disable"])))]
    Maintenance {
        /// Node ID
        node_id: String,
        /// Enable maintenance mode
        #[arg(long)]
        enable: bool,
        /// Disable maintenance mode
        #[arg(long)]
        disable: bool,
    },

    /// UIP-4-1 (D1) — Bug BC-12 operator unquarantine. Flat top-level
    /// command mirroring `Maintenance`'s shape.
    Unquarantine {
        /// Node ID
        node_id: String,
        /// Free-form actor label (initials/ticket) for the audit row.
        #[arg(long)]
        operator_id: String,
        /// Written justification for clearing the integrity-fault
        /// state (server requires >= 16 chars).
        #[arg(long)]
        reason_acknowledged: String,
    },

    /// UIP-4-2 (D1) — remove a drained/dead node from the cluster
    /// registry. Flat top-level command mirroring `Maintenance`'s
    /// shape. Rejected (409) while the node still holds chunk
    /// replicas.
    Decommission {
        /// Node ID
        node_id: String,
        /// Free-form actor label (initials/ticket) for the audit row.
        #[arg(long)]
        operator_id: String,
        /// Written justification for removing the node.
        #[arg(long)]
        reason: String,
    },

    /// View audit log
    Audit {
        /// Maximum entries to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by action prefix
        #[arg(long)]
        action: Option<String>,
    },

    /// UIP-5-3 — integrity-fault views (scrub/read checksum faults,
    /// quarantines) filtered out of the audit log by the manager.
    #[command(subcommand)]
    Integrity(IntegrityCmd),

    /// View or update cluster configuration
    Config {
        /// Show current config
        #[arg(long)]
        show: bool,
        /// UIP-5-1/5-2 (D2) — optional nested subcommand. Bare
        /// `dtfs-cli config` (action = None) keeps today's GET-and-
        /// render behaviour byte-for-byte.
        #[command(subcommand)]
        action: Option<ConfigAction>,
    },

    /// Check manager health
    Health,

    /// UIP-5-5 — aggregate cluster stats (`GET /api/v1/stats`): node
    /// counts, capacity, per-tier breakdown, last scrub.
    Stats,

    /// CF-2 — raft membership operations. Membership lives in the raft
    /// log (`[[cluster.initial_voters]]` is consulted at bootstrap
    /// only); growing/shrinking a live cluster goes through these.
    #[command(subcommand)]
    Raft(RaftCmd),

    /// Update DTFS binaries from the configured GitHub release and
    /// restart the matching local service. Run as root (writes the
    /// install dir and calls systemctl).
    Update {
        /// What to update on this host.
        #[arg(value_enum)]
        target: updater::UpdateTarget,
        /// Only report installed vs latest available; make no changes.
        #[arg(long)]
        check: bool,
        /// Release tag to install (default: the latest release).
        #[arg(long)]
        tag: Option<String>,
        /// Override the release repo URL (default: [updater].repo_url or built-in).
        #[arg(long)]
        repo_url: Option<String>,
        /// Override the install directory (default: [updater].install_dir or /usr/local/bin).
        #[arg(long)]
        install_dir: Option<String>,
        /// Read [updater] from this config file (default: /etc/dtfs/<role>.toml).
        #[arg(long)]
        config: Option<String>,
        /// GitHub token for rate limits or a private fork (default: DTFS_REPO_TOKEN, then [updater].token).
        #[arg(long)]
        token: Option<String>,
        /// Minisign public key (base64 line or .pub text) used to verify SHA256SUMS
        /// (default: DTFS_MINISIGN_PUBKEY, then [updater].minisign_public_key).
        #[arg(long)]
        minisign_key: Option<String>,
        /// Install the new binary but do not restart the service.
        #[arg(long)]
        no_restart: bool,
        /// Skip the confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
        /// Reinstall and restart even when the installed binary is already at the release version.
        #[arg(long)]
        force: bool,
    },
}

#[derive(clap::Subcommand, Debug)]
enum VolumesCmd {
    /// List all volumes
    List,
    /// Create a new volume
    Create {
        /// Volume name (used as both name and id)
        name: String,
        /// Replication factor for the hot tier (sent as replication_hot)
        #[arg(long)]
        replication_factor: Option<u32>,
        /// Skip the server's check that some node reports the datacenter.
        #[arg(long)]
        force: bool,
    },
    /// Bug AV — migrate a legacy volume to the mandatory/cache split
    /// schema. Clones the volume's `replication` field into
    /// `mandatory_replicas`; legacy field preserved. Idempotent.
    /// Operators typically follow this with `set-cache` (AV-3-5).
    MigratePolicy {
        /// Volume id to migrate.
        volume_id: String,
    },
    /// Bug AV (AV-3-5) — show a volume's full configuration
    /// including the AV-introduced mandatory_replicas /
    /// cache_replicas / anchor fields.
    Show {
        /// Volume id to show.
        volume_id: String,
    },
    /// Delete a volume. A volume that still holds files needs two
    /// confirmations and deletes those files too (bytes are reclaimed by
    /// the orphan reaper). `default` cannot be deleted.
    Delete {
        /// Volume id to delete.
        volume_id: String,
        /// Answer the yes/no question (first confirmation) non-interactively.
        #[arg(long)]
        yes: bool,
        /// Second confirmation for a volume with files: must equal the volume id.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// Bug AV (AV-3-5) — set or update the volume's
    /// `cache_replicas` policy. Every flag is optional; omitted
    /// flags keep the current value.
    SetCache {
        /// Volume id to update.
        volume_id: String,
        /// Minimum warm cache replicas per scope.
        #[arg(long)]
        warm_min: Option<u32>,
        /// Maximum warm cache replicas per scope (must be >= min).
        #[arg(long)]
        warm_max: Option<u32>,
        /// Minimum hot cache replicas per scope.
        #[arg(long)]
        hot_min: Option<u32>,
        /// Maximum hot cache replicas per scope.
        #[arg(long)]
        hot_max: Option<u32>,
        /// Per-DC scope when true (default); global pool when false.
        #[arg(long)]
        per_dc: Option<bool>,
        /// Read count threshold for promotion within
        /// `promote_window_secs`.
        #[arg(long)]
        promote_reads: Option<u32>,
        /// Sliding-window length (seconds) over which
        /// `promote_reads` is counted.
        #[arg(long)]
        promote_window_secs: Option<u64>,
        /// Cooldown applied to a chunk after operator flush
        /// (seconds). Engine-driven idle demote clears this.
        #[arg(long)]
        cooldown_secs: Option<u64>,
    },
    /// Set the volume's predictive pre-caching policy (needs every manager
    /// on v0.1.16 or later). Omitted flags keep the current value.
    SetPrecache {
        /// Volume id to update.
        volume_id: String,
        /// Pre-caching mode.
        #[arg(long, value_parser = ["off", "files", "plex"])]
        mode: Option<String>,
        /// Whole files to pre-cache after the file being watched.
        #[arg(long)]
        next_files: Option<u32>,
        /// GiB cap on the next-files selection (0 = no cap).
        #[arg(long)]
        next_max_gb: Option<u32>,
        /// GiB budget per series (plex mode).
        #[arg(long)]
        series_max_gb: Option<u32>,
        /// Series pre-cached per volume per DC (plex mode).
        #[arg(long)]
        max_series: Option<u32>,
    },
    /// Bug AV (AV-4-1a) — set or update the volume's
    /// `mandatory_replicas` (durability floor enforced by the
    /// under-replication scheduler). Every flag is optional;
    /// omitted flags keep the current value. Rejects all-zero
    /// results and anchor-violating updates.
    SetMandatory {
        /// Volume id to update.
        volume_id: String,
        /// Required hot-tier replicas (default keep current).
        #[arg(long)]
        hot_replicas: Option<u32>,
        /// Required warm-tier replicas (default keep current).
        #[arg(long)]
        warm_replicas: Option<u32>,
        /// Required cold-tier replicas (default keep current).
        #[arg(long)]
        cold_replicas: Option<u32>,
        /// Required tape-tier replicas (default keep current).
        #[arg(long)]
        tape_replicas: Option<u32>,
    },
    /// CF-3a — set or update the volume's `write_safety` policy
    /// (durability-ack threshold + write timeout). Every flag is
    /// optional; omitted flags keep the current value. The runtime
    /// reconciliation tool for the ack threshold (otherwise only a
    /// full volume upsert changes it).
    SetWriteSafety {
        /// Volume id to update.
        volume_id: String,
        /// Replicas that must be durable before acking a write
        /// (must be >= 1). Default keep current.
        #[arg(long)]
        min_replicas_before_ack: Option<u32>,
        /// Per-write timeout in seconds (must be >= 1). Default keep
        /// current.
        #[arg(long)]
        write_timeout_secs: Option<u32>,
    },
    /// UIP-3-1 — set or update the volume's anchor policy (Bug AV
    /// anchor invariant: every chunk keeps >=1 replica on the
    /// anchor datacenter). Every flag is optional; omitted flags
    /// keep the current value.
    SetAnchor {
        /// Volume id to update.
        volume_id: String,
        /// Anchor datacenter (matched case-insensitively against a
        /// node's registered datacenter). Default keep current.
        #[arg(long)]
        datacenter: Option<String>,
        /// Required tier for the anchor replica. Default keep
        /// current.
        #[arg(long)]
        required_tier: Option<String>,
        /// Skip the server's check that some node reports the datacenter.
        #[arg(long)]
        force: bool,
    },
    /// UIP-3-2 — set or update the volume's spill policy (Bug BZ:
    /// which tiers/datacenters a chunk may spill onto beyond its
    /// mandatory placement). Every flag is optional; omitted flags
    /// keep the current value.
    SetSpill {
        /// Volume id to update.
        volume_id: String,
        /// Whether spill is allowed at all. Default keep current.
        #[arg(long)]
        allow: Option<bool>,
        /// Tiers spill must never land on. Comma-separated;
        /// an empty list clears the exclusion. Default keep current.
        #[arg(long, value_delimiter = ',')]
        forbidden_tiers: Option<Vec<String>>,
        /// Datacenters spill must never land on. Comma-separated;
        /// an empty list clears the exclusion. Default keep current.
        #[arg(long, value_delimiter = ',')]
        forbidden_datacenters: Option<Vec<String>>,
    },
}

/// Bug AV (AV-3-5) — file-level commands. The legacy
/// `dtfs-cli files --limit N` is now `dtfs-cli files list
/// --limit N`; the breaking shape change is intentional so the
/// new `dtfs-cli files flush-cache` shares the same noun
/// hierarchy as the other `<noun> <verb>` subcommands.
#[derive(clap::Subcommand, Debug)]
enum FilesCmd {
    /// List files in the cluster (legacy `dtfs-cli files`
    /// behaviour preserved here).
    List {
        /// Maximum number of files to list.
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// UIP-5-4 — show a file's chunk layout and per-replica
    /// placement (`GET /api/v1/files/inspect?path=...`).
    Inspect {
        /// Full file path inside the cluster namespace.
        path: String,
    },
    /// Drop cache replicas under a path prefix. Mandatory
    /// (anchor) replicas are always preserved.
    FlushCache {
        /// Path prefix to flush. Required.
        path: String,
        /// Cache tiers to flush. Comma-separated.
        #[arg(long, default_value = "warm")]
        cache_tiers: String,
        /// Cooldown (seconds) before the tiering engine may
        /// re-promote a flushed chunk.
        #[arg(long, default_value = "300")]
        cooldown_secs: u64,
        /// Maximum delete tasks to enqueue in one call.
        #[arg(long, default_value = "10000")]
        max_tasks: u32,
        /// Safety gate: must be set to flush an empty prefix.
        #[arg(long)]
        allow_empty_prefix: bool,
    },
}

/// UIP-5-1/5-2 (D2) — `dtfs-cli config`'s optional nested subcommand.
/// `None` (bare `dtfs-cli config`) keeps the existing GET-and-render
/// behaviour; these are new mutating/read leaves.
#[derive(clap::Subcommand, Debug)]
enum ConfigAction {
    /// UIP-5-1 — apply a config override via `POST /api/v1/config`.
    /// Mirrors the WebUI Cluster tab's save action.
    Set {
        /// Config override as a JSON object string (e.g. the sparse
        /// `{"defaults":{"chunk_size_mb":64}}` shape `update_config`
        /// deep-merges onto the startup config).
        #[arg(long)]
        config_json: String,
        /// AZ-5 — also persist the merged config to
        /// `/etc/dtfs/manager.toml` so the change survives a restart.
        #[arg(long)]
        write_to_disk: bool,
        /// Bug BD-3 — required when `write_to_disk` is set, to
        /// confirm the operator understands a cluster-wide key
        /// propagates to every manager.
        #[arg(long)]
        acknowledge_cluster_wide: bool,
    },
    /// UIP-5-2 — list every effective config key via
    /// `GET /config/effective`.
    Effective,
}

/// UIP-5-3 — integrity subcommands.
#[derive(clap::Subcommand, Debug)]
enum IntegrityCmd {
    /// List integrity faults (`GET /api/v1/integrity/faults`). Same
    /// query shape as `audit`.
    Faults {
        /// Maximum entries to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by action prefix
        #[arg(long)]
        action: Option<String>,
    },
}

#[derive(clap::Subcommand, Debug)]
enum NodesCmd {
    /// Remove a permanently dead node: drop its chunk location rows (guarded
    /// by live replicas and the anchor), then deregister it. Leader only.
    RemoveDead {
        /// Node ID
        node_id: String,
        /// Free-form actor label (initials/ticket) for the audit row.
        #[arg(long, default_value = "dtfs-cli")]
        operator_id: String,
        /// Written justification, recorded in the audit row.
        #[arg(long, default_value = "")]
        reason: String,
        /// Also drop rows with no other live replica (those chunks are lost).
        #[arg(long)]
        accept_data_loss: bool,
        /// Print the plan without changing anything.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(clap::Subcommand, Debug)]
enum ScrubCmd {
    /// Run the orphan reaper once now on the leader (POST /api/v1/scrub/orphans)
    Orphans {
        /// One-time grace in seconds (default: the configured
        /// scrub.orphan_grace_secs; minimum 3600)
        #[arg(long)]
        grace_secs: Option<u64>,
        /// Only report what would be reaped
        #[arg(long)]
        dry_run: bool,
        /// Wait for the run to finish and print its result
        #[arg(long)]
        wait: bool,
    },
    /// Show the last (or running) orphan reap (GET /api/v1/scrub/orphans)
    OrphansStatus,
}

/// UIP-4-3 (D3) — manual data migration subcommands.
#[derive(clap::Subcommand, Debug)]
enum MigrationsCmd {
    /// Start a migration of every chunk under `path_prefix` to
    /// `target_tier` (optionally constrained to `target_dc`).
    Create {
        /// Path prefix to match. Empty string means "every file" —
        /// requires `--allow-empty-prefix` to confirm.
        path_prefix: String,
        /// Target tier: one of hot, warm, cold, tape. Validated
        /// client-side before the POST.
        #[arg(long)]
        target_tier: String,
        /// Optional target datacenter; the chosen target node must
        /// be in this DC.
        #[arg(long)]
        target_dc: Option<String>,
        /// Required when `path_prefix` is empty, to avoid an
        /// accidental whole-volume migration.
        #[arg(long)]
        allow_empty_prefix: bool,
        /// Soft cap on tasks scheduled in this call.
        #[arg(long)]
        max_tasks: Option<usize>,
    },
}

/// UIP-4-3 — the four valid `target_tier` values for `migrations
/// create`, matching `StartMigrationRequest`'s server-side
/// `parse_tier` (api.rs). Checked client-side so a typo fails fast
/// without a network round-trip.
const VALID_TARGET_TIERS: &[&str] = &["hot", "warm", "cold", "tape"];

fn is_valid_target_tier(tier: &str) -> bool {
    VALID_TARGET_TIERS.contains(&tier.to_ascii_lowercase().as_str())
}

/// CF-2 — raft membership subcommands. The mutating ones must be sent
/// to the LEADER's API (`--manager <leader>:9402`); a follower answers
/// 409 with a leader hint.
#[derive(clap::Subcommand, Debug)]
enum RaftCmd {
    /// Show raft membership (voters, learners, leader) as seen by the
    /// queried manager.
    Members,
    /// Add a manager as a non-voting LEARNER (full state replication).
    AddLearner {
        /// Manager hostname (`[node].hostname` in its manager.toml).
        hostname: String,
        /// Raft/gRPC addr host:port peers dial for replication
        /// (e.g. 100.64.0.10:9401). Never a wildcard.
        addr: String,
        /// Cluster shared secret (`[cluster].shared_secret`).
        #[arg(long, env = "DTFS_SHARED_SECRET", hide_env_values = true)]
        secret: String,
    },
    /// Promote an up-to-date learner to voter.
    Promote {
        /// Manager hostname of the learner.
        hostname: String,
        #[arg(long, env = "DTFS_SHARED_SECRET", hide_env_values = true)]
        secret: String,
    },
    /// Remove a learner or voter from the membership entirely.
    Remove {
        /// Manager hostname of the member.
        hostname: String,
        #[arg(long, env = "DTFS_SHARED_SECRET", hide_env_values = true)]
        secret: String,
    },
}

/// A bare `host:port` gets `http://`; only a real scheme is kept (a host
/// named `httpd01:9402` must not be mistaken for a URL).
fn base_url_for(manager_addr: &str) -> String {
    if manager_addr.starts_with("http://") || manager_addr.starts_with("https://") {
        manager_addr.to_string()
    } else {
        format!("http://{}", manager_addr)
    }
}

/// `/api/v1/files` page request.
fn files_page_path(limit: usize, offset: usize) -> String {
    format!("/api/v1/files?limit={}&offset={}", limit, offset)
}

/// "showing N of M" line, only when the list is cut short.
fn files_truncation_note(shown: usize, total: Option<u64>, truncated: bool) -> Option<String> {
    let total = total?;
    (truncated || (shown as u64) < total)
        .then(|| format!("showing {} of {} (raise --limit to see more)", shown, total))
}

/// Failure summary for a flush-cache response; `None` when clean.
fn flush_failure_summary(resp: &Value) -> Option<String> {
    let rpc = resp["rpc_failures"].as_u64().unwrap_or(0);
    let raft = resp["raft_failures"].as_u64().unwrap_or(0);
    (rpc + raft > 0).then(|| {
        format!("flush-cache incomplete: rpc_failures={} raft_failures={}", rpc, raft)
    })
}

/// Audit list path with the action filter percent-encoded.
fn audit_path(base: &str, limit: usize, action: Option<&str>) -> String {
    let mut path = format!("{}?limit={}", base, limit);
    if let Some(a) = action {
        path.push_str(&format!("&action={}", percent_encode_query(a)));
    }
    path
}

struct ApiClient {
    base_url: String,
    client: reqwest::Client,
    /// LIS-18 — sent as `X-Dtfs-Shared-Secret` on every POST when non-empty.
    shared_secret: String,
}

impl ApiClient {
    fn new(manager_addr: &str, shared_secret: String) -> Self {
        let base_url = base_url_for(manager_addr);

        Self {
            base_url,
            client: reqwest::Client::new(),
            shared_secret,
        }
    }

    fn apply_mutation_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.shared_secret.is_empty() {
            req
        } else {
            req.header("X-Dtfs-Shared-Secret", &self.shared_secret)
        }
    }

    /// Clone with an explicit secret (CF-2 raft subcommands take `--secret`
    /// per-command; that value must also ride the LIS-18 header).
    fn with_secret(&self, secret: &str) -> Self {
        Self {
            base_url: self.base_url.clone(),
            client: self.client.clone(),
            shared_secret: secret.to_string(),
        }
    }

    async fn get(&self, path: &str) -> anyhow::Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// GET that preserves the server's JSON error body on non-2xx (409 leader hint).
    async fn get_with_status(&self, path: &str) -> anyhow::Result<(reqwest::StatusCode, Value)> {
        let resp = self.client.get(format!("{}{}", self.base_url, path)).send().await?;
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or_else(|_| json!({}));
        Ok((status, value))
    }

    async fn post(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .apply_mutation_auth(self.client.post(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// POST that preserves the server's JSON error body on non-2xx
    /// (CF-2: the membership endpoints return structured errors — e.g.
    /// a leader hint on 409 — that `error_for_status()` would discard).
    async fn post_with_status(
        &self,
        path: &str,
        body: &Value,
    ) -> anyhow::Result<(reqwest::StatusCode, Value)> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .apply_mutation_auth(self.client.post(&url))
            .json(body)
            .send()
            .await?;
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or_else(|_| json!({}));
        Ok((status, value))
    }

    /// DELETE that preserves the server's JSON error body on non-2xx.
    async fn delete_with_status(&self, path: &str) -> anyhow::Result<(reqwest::StatusCode, Value)> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.apply_mutation_auth(self.client.delete(&url)).send().await?;
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or_else(|_| json!({}));
        Ok((status, value))
    }

    /// POST that returns the JSON body on 2xx and otherwise fails with
    /// the server's `error` message (UIP audit follow-up: `post()`'s
    /// `error_for_status()` discarded validation 400s such as "interval
    /// below 60s" or "anchor unsatisfiable", leaving the operator with
    /// only the status line).
    async fn post_checked(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        let (status, value) = self.post_with_status(path, body).await?;
        if !status.is_success() {
            let msg = value["error"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            anyhow::bail!("HTTP {}: {}", status, msg);
        }
        Ok(value)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let api = ApiClient::new(&args.manager, args.secret.clone());

    match args.command {
        Commands::Status => cmd_status(&api, &args.format).await?,
        Commands::Nodes { id, cmd: None } => cmd_nodes(&api, &args.format, id).await?,
        Commands::Nodes {
            cmd: Some(NodesCmd::RemoveDead { node_id, operator_id, reason, accept_data_loss, dry_run }),
            ..
        } => {
            let body = remove_dead_body(&operator_id, &reason, accept_data_loss, dry_run);
            let (status, value) = api
                .post_with_status(&format!("/api/v1/nodes/{}/remove-dead", node_id), &body)
                .await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
            if !status.is_success() {
                anyhow::bail!("remove-dead node {} failed: HTTP {}", node_id, status);
            }
            // The run continues on the manager after the POST returns; wait for it.
            if value["running"] == true {
                let path = format!("/api/v1/nodes/{}/remove-dead", node_id);
                let done = loop {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    let v = api.get(&path).await?;
                    if v["running"] != true {
                        break v;
                    }
                };
                println!("{}", serde_json::to_string_pretty(&done)?);
            }
        }
        Commands::Volumes(sub) => match sub {
            VolumesCmd::List => cmd_volumes(&api, &args.format).await?,
            VolumesCmd::Create {
                name,
                replication_factor,
                force,
            } => cmd_volumes_create(&api, &args.format, &name, replication_factor, force).await?,
            VolumesCmd::MigratePolicy { volume_id } => {
                cmd_volumes_migrate_policy(&api, &args.format, &volume_id).await?
            }
            VolumesCmd::Show { volume_id } => {
                cmd_volumes_show(&api, &args.format, &volume_id).await?
            }
            VolumesCmd::Delete { volume_id, yes, confirm } => {
                cmd_volumes_delete(&api, &args.format, &volume_id, yes, confirm.as_deref()).await?
            }
            VolumesCmd::SetCache {
                volume_id,
                warm_min, warm_max, hot_min, hot_max,
                per_dc, promote_reads, promote_window_secs, cooldown_secs,
            } => {
                cmd_volumes_set_cache(
                    &api, &args.format, &volume_id,
                    warm_min, warm_max, hot_min, hot_max,
                    per_dc, promote_reads, promote_window_secs, cooldown_secs,
                ).await?
            }
            VolumesCmd::SetPrecache {
                volume_id, mode, next_files, next_max_gb, series_max_gb, max_series,
            } => {
                cmd_volumes_set_precache(
                    &api, &args.format, &volume_id,
                    set_precache_body(mode.as_deref(), next_files, next_max_gb, series_max_gb, max_series),
                ).await?
            }
            VolumesCmd::SetMandatory {
                volume_id,
                hot_replicas, warm_replicas, cold_replicas, tape_replicas,
            } => {
                cmd_volumes_set_mandatory(
                    &api, &args.format, &volume_id,
                    hot_replicas, warm_replicas, cold_replicas, tape_replicas,
                ).await?
            }
            VolumesCmd::SetWriteSafety {
                volume_id,
                min_replicas_before_ack, write_timeout_secs,
            } => {
                cmd_volumes_set_write_safety(
                    &api, &args.format, &volume_id,
                    min_replicas_before_ack, write_timeout_secs,
                ).await?
            }
            VolumesCmd::SetAnchor {
                volume_id,
                datacenter, required_tier, force,
            } => {
                cmd_volumes_set_anchor(
                    &api, &args.format, &volume_id,
                    datacenter, required_tier, force,
                ).await?
            }
            VolumesCmd::SetSpill {
                volume_id,
                allow, forbidden_tiers, forbidden_datacenters,
            } => {
                cmd_volumes_set_spill(
                    &api, &args.format, &volume_id,
                    allow, forbidden_tiers, forbidden_datacenters,
                ).await?
            }
        },
        Commands::Files(sub) => match sub {
            FilesCmd::List { limit } => cmd_files(&api, &args.format, limit).await?,
            FilesCmd::Inspect { path } => cmd_files_inspect(&api, &args.format, &path).await?,
            FilesCmd::FlushCache {
                path, cache_tiers, cooldown_secs, max_tasks, allow_empty_prefix,
            } => cmd_files_flush_cache(
                &api, &args.format, &path, &cache_tiers,
                cooldown_secs, max_tasks, allow_empty_prefix,
            ).await?,
        },
        Commands::Precache(PrecacheCmd::Status) => cmd_precache_status(&api, &args.format).await?,
        Commands::Migrations(sub) => match sub {
            MigrationsCmd::Create {
                path_prefix, target_tier, target_dc, allow_empty_prefix, max_tasks,
            } => cmd_migrations_create(
                &api, &args.format, &path_prefix, &target_tier, target_dc,
                allow_empty_prefix, max_tasks,
            ).await?,
        },
        Commands::Scrub { level, action: None } => cmd_scrub(&api, &level).await?,
        Commands::Scrub { action: Some(ScrubCmd::Orphans { grace_secs, dry_run, wait }), .. } => {
            cmd_scrub_orphans(&api, &args.format, grace_secs, dry_run, wait).await?
        }
        Commands::Scrub { action: Some(ScrubCmd::OrphansStatus), .. } => {
            let v = api.get("/api/v1/scrub/orphans").await?;
            print_orphan_status(&args.format, &v)?;
        }
        Commands::Maintenance {
            node_id,
            enable,
            disable,
        } => cmd_maintenance(&api, &node_id, enable, disable).await?,
        Commands::Unquarantine {
            node_id,
            operator_id,
            reason_acknowledged,
        } => cmd_unquarantine(&api, &node_id, &operator_id, &reason_acknowledged).await?,
        Commands::Decommission {
            node_id,
            operator_id,
            reason,
        } => cmd_decommission(&api, &node_id, &operator_id, &reason).await?,
        Commands::Audit { limit, action } => cmd_audit(&api, &args.format, limit, action).await?,
        Commands::Integrity(sub) => match sub {
            IntegrityCmd::Faults { limit, action } => {
                cmd_integrity_faults(&api, &args.format, limit, action).await?
            }
        },
        Commands::Config { show: _, action } => match action {
            None => cmd_config(&api, &args.format).await?,
            Some(ConfigAction::Set {
                config_json,
                write_to_disk,
                acknowledge_cluster_wide,
            }) => {
                cmd_config_set(&api, &config_json, write_to_disk, acknowledge_cluster_wide).await?
            }
            Some(ConfigAction::Effective) => cmd_config_effective(&api, &args.format).await?,
        },
        Commands::Health => cmd_health(&api).await?,
        Commands::Stats => cmd_stats(&api, &args.format).await?,
        Commands::Raft(sub) => match sub {
            RaftCmd::Members => cmd_raft_members(&api, &args.format).await?,
            RaftCmd::AddLearner {
                hostname,
                addr,
                secret,
            } => {
                let api = api.with_secret(&secret);
                cmd_raft_change(
                    &api,
                    "/api/v1/raft/add-learner",
                    json!({"hostname": hostname, "addr": addr, "shared_secret": secret}),
                )
                .await?
            }
            RaftCmd::Promote { hostname, secret } => {
                let api = api.with_secret(&secret);
                cmd_raft_change(
                    &api,
                    "/api/v1/raft/promote",
                    json!({"hostname": hostname, "shared_secret": secret}),
                )
                .await?
            }
            RaftCmd::Remove { hostname, secret } => {
                let api = api.with_secret(&secret);
                cmd_raft_change(
                    &api,
                    "/api/v1/raft/remove",
                    json!({"hostname": hostname, "shared_secret": secret}),
                )
                .await?
            }
        },
        Commands::Update {
            target,
            check,
            tag,
            repo_url,
            install_dir,
            config,
            token,
            minisign_key,
            no_restart,
            yes,
            force,
        } => updater::run(target, check, tag, repo_url, install_dir, config, token, minisign_key, no_restart, yes, force)?,
    }

    Ok(())
}

async fn cmd_status(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let status = api.get("/api/v1/cluster/status").await?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        OutputFormat::Table => {
            println!("DTFS Cluster Status");
            println!("===================");
            println!(
                "Cluster:        {}",
                status["cluster_name"].as_str().unwrap_or("—")
            );
            println!(
                "Status:         {}",
                status["status"].as_str().unwrap_or("—")
            );
            println!(
                "Manager:        {}",
                status["node"].as_str().unwrap_or("—")
            );
            println!(
                "Data Nodes:     {} ({} healthy)",
                status["data_nodes"].as_u64().unwrap_or(0),
                status["data_nodes_healthy"].as_u64().unwrap_or(0),
            );
            println!(
                "Files:          {}",
                status["files"].as_u64().unwrap_or(0)
            );
            println!(
                "Total Capacity: {}",
                format_bytes(status["total_capacity"].as_u64().unwrap_or(0))
            );
            println!(
                "Used Capacity:  {}",
                format_bytes(status["used_capacity"].as_u64().unwrap_or(0))
            );
            println!(
                "Config Version: {}",
                status["config_version"].as_u64().unwrap_or(0)
            );
        }
    }
    Ok(())
}

async fn cmd_nodes(
    api: &ApiClient,
    format: &OutputFormat,
    id: Option<String>,
) -> anyhow::Result<()> {
    if let Some(node_id) = id {
        let node = api.get(&format!("/api/v1/nodes/{}", node_id)).await?;
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&node)?),
            OutputFormat::Table => {
                println!("Node Details");
                println!("============");
                println!("ID:       {}", node["node_id"].as_str().unwrap_or("—"));
                println!("Hostname: {}", node["hostname"].as_str().unwrap_or("—"));
                println!("Address:  {}", node["grpc_addr"].as_str().unwrap_or("—"));
                println!("Health:   {}", node["health"].as_str().unwrap_or("—"));
                println!("Role:     {}", node["role"].as_str().unwrap_or("—"));

                if let Some(loc) = node.get("location") {
                    println!(
                        "Location: {}/{}/{}/{}",
                        loc["region"].as_str().unwrap_or("—"),
                        loc["datacenter"].as_str().unwrap_or("—"),
                        loc["rack"].as_str().unwrap_or("—"),
                        loc["host"].as_str().unwrap_or("—"),
                    );
                }

                if let Some(disks) = node["disks"].as_array() {
                    println!("\nDisks ({}):", disks.len());
                    println!(
                        "  {:<8} {:<10} {:<24} {:<12} {:<12} {}",
                        "ID", "Tier", "Path", "Total", "Used", "Status"
                    );
                    for d in disks {
                        println!(
                            "  {:<8} {:<10} {:<24} {:<12} {:<12} {}",
                            d["disk_id"].as_u64().unwrap_or(0),
                            d["tier"].as_str().unwrap_or("—"),
                            d["mount_path"].as_str().unwrap_or("—"),
                            format_bytes(d["total_bytes"].as_u64().unwrap_or(0)),
                            format_bytes(d["used_bytes"].as_u64().unwrap_or(0)),
                            d["status"].as_str().unwrap_or("—"),
                        );
                    }
                }
            }
        }
    } else {
        let nodes = api.get("/api/v1/nodes").await?;
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&nodes)?),
            OutputFormat::Table => {
                let nodes = nodes.as_array().cloned().unwrap_or_default();
                if nodes.is_empty() {
                    println!("No data nodes registered.");
                    return Ok(());
                }
                println!(
                    "{:<20} {:<20} {:<30} {:<6} {:<12} {:<12} {}",
                    "NODE ID", "HOSTNAME", "LOCATION", "DISKS", "CAPACITY", "USED", "HEALTH"
                );
                println!("{}", "-".repeat(110));
                for n in &nodes {
                    println!(
                        "{:<20} {:<20} {:<30} {:<6} {:<12} {:<12} {}",
                        truncate(n["node_id"].as_str().unwrap_or("—"), 18),
                        n["hostname"].as_str().unwrap_or("—"),
                        n["location"].as_str().unwrap_or("—"),
                        n["disk_count"].as_u64().unwrap_or(0),
                        format_bytes(n["total_capacity"].as_u64().unwrap_or(0)),
                        format_bytes(n["used_capacity"].as_u64().unwrap_or(0)),
                        n["health"].as_str().unwrap_or("—"),
                    );
                }
                println!("\nTotal: {} nodes", nodes.len());
            }
        }
    }
    Ok(())
}

fn volumes_create_body(
    name: &str,
    replication_factor: Option<u32>,
    force: bool,
) -> serde_json::Value {
    let mut body = serde_json::json!({ "name": name });
    if let Some(rf) = replication_factor {
        body["replication_hot"] = serde_json::json!(rf);
    }
    if force {
        body["force"] = serde_json::json!(true);
    }
    body
}

fn set_anchor_body(
    datacenter: Option<String>,
    required_tier: Option<String>,
    force: bool,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    if let Some(v) = datacenter { body.insert("datacenter".into(), v.into()); }
    if let Some(v) = required_tier { body.insert("required_tier".into(), v.into()); }
    if force { body.insert("force".into(), true.into()); }
    serde_json::Value::Object(body)
}

async fn cmd_volumes_create(
    api: &ApiClient,
    format: &OutputFormat,
    name: &str,
    replication_factor: Option<u32>,
    force: bool,
) -> anyhow::Result<()> {
    let body = volumes_create_body(name, replication_factor, force);
    let resp = api.post_checked("/api/v1/volumes", &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let status = resp["status"].as_str().unwrap_or("");
            let id = resp["volume_id"].as_str().unwrap_or(name);
            println!("Volume '{}' {}.", id, if status.is_empty() { "created" } else { status });
        }
    }
    Ok(())
}

/// Bug AV (AV-1-10) — `dtfs-cli volumes migrate-policy <id>`.
/// Calls the manager-side endpoint that clones the legacy
/// `replication` field into `mandatory_replicas`. Idempotent.
async fn cmd_volumes_migrate_policy(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
) -> anyhow::Result<()> {
    let url = format!("/api/v1/volumes/{}/migrate-policy", volume_id);
    let resp = api.post(&url, &serde_json::json!({})).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let m = &resp["mandatory_replicas"];
            println!(
                "Volume '{}' migrate-policy: {} (mandatory hot={} warm={} cold={} tape={})",
                volume_id,
                if changed { "applied" } else { "no-op (already migrated)" },
                m["hot"].as_u64().unwrap_or(0),
                m["warm"].as_u64().unwrap_or(0),
                m["cold"].as_u64().unwrap_or(0),
                m["tape"].as_u64().unwrap_or(0),
            );
        }
    }
    Ok(())
}

/// AV-3-5 — `dtfs-cli volumes show <id>`: render the full volume
/// config (including AV-introduced mandatory/cache/anchor fields).
async fn cmd_volumes_show(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
) -> anyhow::Result<()> {
    let url = format!("/api/v1/volumes/{}", volume_id);
    let v = api.get(&url).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&v)?),
        OutputFormat::Table => {
            println!("Volume: {}", v["volume_id"].as_str().unwrap_or("—"));
            println!("Name:   {}", v["name"].as_str().unwrap_or("—"));
            println!(
                "Chunk size:  {} bytes",
                v["chunk_size_bytes"].as_u64().unwrap_or(0)
            );
            let r = &v["replication"];
            println!(
                "Legacy replication:  hot={} warm={} cold={} tape={}",
                r["hot"].as_u64().unwrap_or(0),
                r["warm"].as_u64().unwrap_or(0),
                r["cold"].as_u64().unwrap_or(0),
                r["tape"].as_u64().unwrap_or(0),
            );
            let m = &v["mandatory_replicas"];
            println!(
                "Mandatory replicas:  hot={} warm={} cold={} tape={}",
                m["hot"].as_u64().unwrap_or(0),
                m["warm"].as_u64().unwrap_or(0),
                m["cold"].as_u64().unwrap_or(0),
                m["tape"].as_u64().unwrap_or(0),
            );
            let c = &v["cache_replicas"];
            println!("Cache replicas:");
            println!(
                "  warm: min={} max={}",
                c["warm"]["min"].as_u64().unwrap_or(0),
                c["warm"]["max"].as_u64().unwrap_or(0)
            );
            println!(
                "  hot:  min={} max={}",
                c["hot"]["min"].as_u64().unwrap_or(0),
                c["hot"]["max"].as_u64().unwrap_or(0)
            );
            println!("  per_dc:               {}", c["per_dc"].as_bool().unwrap_or(true));
            println!("  promote_reads:        {}", c["promote_reads"].as_u64().unwrap_or(2));
            println!("  promote_window_secs:  {}", c["promote_window_secs"].as_u64().unwrap_or(14_400));
            println!("  cooldown_secs:        {}", c["cooldown_secs"].as_u64().unwrap_or(300));
            if v["anchor"].is_null() {
                println!("Anchor:  (none)");
            } else {
                let a = &v["anchor"];
                println!(
                    "Anchor:  dc={} required_tier={}",
                    a["datacenter"].as_str().unwrap_or("—"),
                    a["required_tier"].as_str().unwrap_or("any"),
                );
            }
        }
    }
    Ok(())
}

/// Confirmation logic for `volumes delete`. `data` is `Some(summary)` when the
/// volume holds entries. `prompt` shows a question and returns the answer, or
/// `None` without terminal input. `Err` = abort, nothing is deleted.
fn confirm_volume_delete(
    volume_id: &str,
    data: Option<&str>,
    yes: bool,
    confirm: Option<&str>,
    prompt: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<(), String> {
    const NO_TTY: &str = "no terminal input; pass --yes (and --confirm <volume-id> for a volume with files)";
    if confirm.is_some_and(|c| c != volume_id) {
        return Err(format!("--confirm does not match the volume id '{volume_id}'"));
    }
    let question = match data {
        None => format!("Delete volume {volume_id}? [y/N] "),
        Some(s) => format!("Volume {volume_id} holds {s}. Delete the volume AND all its files? [y/N] "),
    };
    if !yes {
        let answer = prompt(&question).ok_or(NO_TTY)?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            return Err("aborted".into());
        }
    }
    if data.is_some() && confirm.is_none() {
        let typed = prompt("Type the volume id to confirm: ").ok_or(NO_TTY)?;
        if typed.trim() != volume_id {
            return Err(format!("typed id does not match '{volume_id}'; aborted"));
        }
    }
    Ok(())
}

/// Ask on stderr, read one line from stdin; `None` without a terminal or on EOF.
fn tty_prompt(question: &str) -> Option<String> {
    use std::io::{IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        return None;
    }
    eprint!("{question}");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    match std::io::stdin().read_line(&mut line) {
        Ok(n) if n > 0 => Some(line),
        _ => None,
    }
}

/// `dtfs-cli volumes delete <id> [--yes] [--confirm <id>]`.
async fn cmd_volumes_delete(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    yes: bool,
    confirm: Option<&str>,
) -> anyhow::Result<()> {
    let v = api.get(&format!("/api/v1/volumes/{}", volume_id)).await?;
    let (files, dirs, bytes) = (
        v["files"].as_u64().unwrap_or(0),
        v["directories"].as_u64().unwrap_or(0),
        v["bytes"].as_u64().unwrap_or(0),
    );
    let summary = (files + dirs > 0).then(|| {
        let d = if dirs > 0 { format!(", {dirs} directories") } else { String::new() };
        format!("{files} files{d} ({bytes} bytes)")
    });
    confirm_volume_delete(volume_id, summary.as_deref(), yes, confirm, &mut tty_prompt)
        .map_err(|e| anyhow::anyhow!("volume {volume_id} not deleted: {e}"))?;

    // Without consent to delete files, a file that appeared meanwhile makes the server refuse (409).
    let path = format!("/api/v1/volumes/{}?delete_files={}", volume_id, summary.is_some());
    let (status, resp) = api.delete_with_status(&path).await?;
    if !status.is_success() {
        let msg = resp["error"].as_str().map(str::to_string).unwrap_or_else(|| resp.to_string());
        anyhow::bail!("volume {volume_id} delete failed: HTTP {status}: {msg}");
    }
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => println!(
            "Volume '{}' deleted ({} entries deleted; bytes are reclaimed by the orphan reaper).",
            volume_id,
            resp["files_deleted"].as_u64().unwrap_or(0)
        ),
    }
    Ok(())
}

/// AV-3-5 — `dtfs-cli volumes set-cache <id> [--warm-min N ...]`:
/// patch the volume's `cache_replicas` policy. Only the supplied
/// flags are merged; others keep their current value.
#[allow(clippy::too_many_arguments)]
async fn cmd_volumes_set_cache(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    warm_min: Option<u32>,
    warm_max: Option<u32>,
    hot_min: Option<u32>,
    hot_max: Option<u32>,
    per_dc: Option<bool>,
    promote_reads: Option<u32>,
    promote_window_secs: Option<u64>,
    cooldown_secs: Option<u64>,
) -> anyhow::Result<()> {
    let mut body = serde_json::Map::new();
    if let Some(v) = warm_min { body.insert("warm_min".into(), v.into()); }
    if let Some(v) = warm_max { body.insert("warm_max".into(), v.into()); }
    if let Some(v) = hot_min  { body.insert("hot_min".into(),  v.into()); }
    if let Some(v) = hot_max  { body.insert("hot_max".into(),  v.into()); }
    if let Some(v) = per_dc   { body.insert("per_dc".into(),   v.into()); }
    if let Some(v) = promote_reads
        { body.insert("promote_reads".into(), v.into()); }
    if let Some(v) = promote_window_secs
        { body.insert("promote_window_secs".into(), v.into()); }
    if let Some(v) = cooldown_secs
        { body.insert("cooldown_secs".into(), v.into()); }
    let body = serde_json::Value::Object(body);

    let url = format!("/api/v1/volumes/{}/set-cache", volume_id);
    let resp = api.post(&url, &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let c = &resp["config"]["cache_replicas"];
            println!(
                "Volume '{}' set-cache: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!(
                "  warm: min={} max={}",
                c["warm"]["min"].as_u64().unwrap_or(0),
                c["warm"]["max"].as_u64().unwrap_or(0)
            );
            println!(
                "  hot:  min={} max={}",
                c["hot"]["min"].as_u64().unwrap_or(0),
                c["hot"]["max"].as_u64().unwrap_or(0)
            );
            println!("  per_dc:               {}", c["per_dc"].as_bool().unwrap_or(true));
            println!("  promote_reads:        {}", c["promote_reads"].as_u64().unwrap_or(2));
            println!("  promote_window_secs:  {}", c["promote_window_secs"].as_u64().unwrap_or(14_400));
            println!("  cooldown_secs:        {}", c["cooldown_secs"].as_u64().unwrap_or(300));
        }
    }
    Ok(())
}

/// Request body for `set-precache`: only the supplied flags.
fn set_precache_body(
    mode: Option<&str>,
    next_files: Option<u32>,
    next_max_gb: Option<u32>,
    series_max_gb: Option<u32>,
    max_series: Option<u32>,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    if let Some(v) = mode { body.insert("mode".into(), v.into()); }
    if let Some(v) = next_files { body.insert("next_files".into(), v.into()); }
    if let Some(v) = next_max_gb { body.insert("next_max_gb".into(), v.into()); }
    if let Some(v) = series_max_gb { body.insert("series_max_gb".into(), v.into()); }
    if let Some(v) = max_series { body.insert("max_series".into(), v.into()); }
    serde_json::Value::Object(body)
}

/// `dtfs-cli volumes set-precache <id> [--mode plex ...]`.
async fn cmd_volumes_set_precache(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    body: serde_json::Value,
) -> anyhow::Result<()> {
    let resp = api.post(&format!("/api/v1/volumes/{}/set-precache", volume_id), &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let c = &resp["config"]["precache"];
            println!(
                "Volume '{}' set-precache: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!("  mode:           {}", c["mode"].as_str().unwrap_or("off"));
            println!("  next_files:     {}", c["next_files"].as_u64().unwrap_or(0));
            println!("  next_max_gb:    {}", c["next_max_gb"].as_u64().unwrap_or(0));
            println!("  series_max_gb:  {}", c["series_max_gb"].as_u64().unwrap_or(0));
            println!("  max_series:     {}", c["max_series"].as_u64().unwrap_or(0));
        }
    }
    Ok(())
}

/// AV-4-1a — `dtfs-cli volumes set-mandatory <id> [--cold-replicas N ...]`:
/// patch the volume's `mandatory_replicas`. Only supplied flags
/// are merged; others keep current values. Server-side validation
/// rejects all-zero results and anchor-violating updates.
async fn cmd_volumes_set_mandatory(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    hot_replicas: Option<u32>,
    warm_replicas: Option<u32>,
    cold_replicas: Option<u32>,
    tape_replicas: Option<u32>,
) -> anyhow::Result<()> {
    let mut body = serde_json::Map::new();
    if let Some(v) = hot_replicas  { body.insert("hot_replicas".into(),  v.into()); }
    if let Some(v) = warm_replicas { body.insert("warm_replicas".into(), v.into()); }
    if let Some(v) = cold_replicas { body.insert("cold_replicas".into(), v.into()); }
    if let Some(v) = tape_replicas { body.insert("tape_replicas".into(), v.into()); }
    let body = serde_json::Value::Object(body);

    let url = format!("/api/v1/volumes/{}/set-mandatory", volume_id);
    let resp = api.post(&url, &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let m = &resp["config"]["mandatory_replicas"];
            println!(
                "Volume '{}' set-mandatory: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!(
                "  hot={} warm={} cold={} tape={}",
                m["hot"].as_u64().unwrap_or(0),
                m["warm"].as_u64().unwrap_or(0),
                m["cold"].as_u64().unwrap_or(0),
                m["tape"].as_u64().unwrap_or(0),
            );
        }
    }
    Ok(())
}

/// CF-3a — `dtfs-cli volumes set-write-safety <id>
/// [--min-replicas-before-ack N] [--write-timeout-secs N]`:
/// patch the volume's `write_safety`. Only supplied flags are
/// merged; others keep current values. Server-side validation
/// rejects zero values.
async fn cmd_volumes_set_write_safety(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    min_replicas_before_ack: Option<u32>,
    write_timeout_secs: Option<u32>,
) -> anyhow::Result<()> {
    let mut body = serde_json::Map::new();
    if let Some(v) = min_replicas_before_ack
        { body.insert("min_replicas_before_ack".into(), v.into()); }
    if let Some(v) = write_timeout_secs
        { body.insert("write_timeout_secs".into(), v.into()); }
    let body = serde_json::Value::Object(body);

    let url = format!("/api/v1/volumes/{}/set-write-safety", volume_id);
    let resp = api.post(&url, &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let w = &resp["config"]["write_safety"];
            println!(
                "Volume '{}' set-write-safety: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!(
                "  min_replicas_before_ack: {}",
                w["min_replicas_before_ack"].as_u64().unwrap_or(0),
            );
            println!(
                "  write_timeout_secs:      {}",
                w["write_timeout_secs"].as_u64().unwrap_or(0),
            );
        }
    }
    Ok(())
}

/// UIP-3-1 — `dtfs-cli volumes set-anchor <id> [--datacenter D]
/// [--required-tier T]`: patch the volume's anchor policy. Only
/// supplied flags are merged; others keep current values.
async fn cmd_volumes_set_anchor(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    datacenter: Option<String>,
    required_tier: Option<String>,
    force: bool,
) -> anyhow::Result<()> {
    let body = set_anchor_body(datacenter, required_tier, force);

    let url = format!("/api/v1/volumes/{}/set-anchor", volume_id);
    let resp = api.post_checked(&url, &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let a = &resp["config"]["anchor"];
            println!(
                "Volume '{}' set-anchor: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!(
                "  datacenter:     {}",
                a["datacenter"].as_str().unwrap_or(""),
            );
            println!(
                "  required_tier:  {}",
                a["required_tier"].as_str().unwrap_or(""),
            );
        }
    }
    Ok(())
}

/// UIP-3-2 — `dtfs-cli volumes set-spill <id> [--allow BOOL]
/// [--forbidden-tiers a,b] [--forbidden-datacenters a,b]`: patch the
/// volume's spill policy. Only supplied flags are merged; others
/// keep current values.
async fn cmd_volumes_set_spill(
    api: &ApiClient,
    format: &OutputFormat,
    volume_id: &str,
    allow: Option<bool>,
    forbidden_tiers: Option<Vec<String>>,
    forbidden_datacenters: Option<Vec<String>>,
) -> anyhow::Result<()> {
    let mut body = serde_json::Map::new();
    if let Some(v) = allow { body.insert("allow".into(), v.into()); }
    if let Some(v) = forbidden_tiers { body.insert("forbidden_tiers".into(), v.into()); }
    if let Some(v) = forbidden_datacenters { body.insert("forbidden_datacenters".into(), v.into()); }
    let body = serde_json::Value::Object(body);

    let url = format!("/api/v1/volumes/{}/set-spill", volume_id);
    let resp = api.post_checked(&url, &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let changed = resp["changed"].as_bool().unwrap_or(false);
            let s = &resp["config"]["spill"];
            println!(
                "Volume '{}' set-spill: {}",
                volume_id,
                if changed { "applied" } else { "no-op" },
            );
            println!("  allow:                  {}", s["allow"].as_bool().unwrap_or(true));
            println!("  forbidden_tiers:        {:?}", s["forbidden_tiers"]);
            println!("  forbidden_datacenters:  {:?}", s["forbidden_datacenters"]);
        }
    }
    Ok(())
}

/// AV-3-5 — `dtfs-cli files flush-cache <path> [--cache-tiers warm,hot]`:
/// drop cache replicas under a path prefix. Thin wrapper over
/// `POST /api/v1/files/flush_cache` (AV-1-9).
#[allow(clippy::too_many_arguments)]
async fn cmd_files_flush_cache(
    api: &ApiClient,
    format: &OutputFormat,
    path: &str,
    cache_tiers_csv: &str,
    cooldown_secs: u64,
    max_tasks: u32,
    allow_empty_prefix: bool,
) -> anyhow::Result<()> {
    let tiers: Vec<String> = cache_tiers_csv
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if tiers.is_empty() {
        anyhow::bail!("--cache-tiers must list at least one tier (e.g. warm or hot,warm)");
    }
    let body = serde_json::json!({
        "path_prefix": path,
        "cache_tiers": tiers,
        "cooldown_secs": cooldown_secs,
        "max_tasks": max_tasks,
        "allow_empty_prefix": allow_empty_prefix,
    });
    let resp = api.post_checked("/api/v1/files/flush_cache", &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let dropped = resp["replicas_dropped"].as_u64().unwrap_or(0);
            let refusals = resp["mandatory_refusals"].as_u64().unwrap_or(0);
            println!(
                "flush-cache prefix={} tiers={:?} cooldown={}s",
                path, tiers, cooldown_secs
            );
            println!(
                "  files_matched     = {}",
                resp["files_matched"].as_u64().unwrap_or(0)
            );
            println!(
                "  chunks_examined   = {}",
                resp["chunks_examined"].as_u64().unwrap_or(0)
            );
            println!("  replicas_dropped  = {}", dropped);
            println!("  mandatory_refusals = {}{}",
                refusals,
                if refusals > 0 {
                    "  ⚠  check audit: action=cache.flush_refused"
                } else { "" },
            );
            println!(
                "  unreachable_nodes = {}",
                resp["unreachable_nodes"].as_u64().unwrap_or(0)
            );
            println!(
                "  rpc_failures      = {}",
                resp["rpc_failures"].as_u64().unwrap_or(0)
            );
            println!(
                "  raft_failures     = {}",
                resp["raft_failures"].as_u64().unwrap_or(0)
            );
            let truncated = resp["truncated"].as_u64().unwrap_or(0);
            if truncated > 0 {
                println!("  truncated (re-run): {}", truncated);
            }
        }
    }
    if let Some(msg) = flush_failure_summary(&resp) {
        anyhow::bail!(msg);
    }
    Ok(())
}

/// UIP-4-3 — `dtfs-cli migrations create <path-prefix> --target-tier
/// T [--target-dc DC] [--allow-empty-prefix] [--max-tasks N]`: start
/// a manual data migration. `target_tier` is validated client-side
/// against hot|warm|cold|tape before any network call, matching the
/// enum `StartMigrationRequest` accepts server-side.
#[allow(clippy::too_many_arguments)]
async fn cmd_migrations_create(
    api: &ApiClient,
    format: &OutputFormat,
    path_prefix: &str,
    target_tier: &str,
    target_dc: Option<String>,
    allow_empty_prefix: bool,
    max_tasks: Option<usize>,
) -> anyhow::Result<()> {
    if !is_valid_target_tier(target_tier) {
        anyhow::bail!(
            "invalid --target-tier '{}': expected one of {}",
            target_tier,
            VALID_TARGET_TIERS.join("|"),
        );
    }

    let mut body = serde_json::Map::new();
    body.insert("path_prefix".into(), path_prefix.into());
    body.insert("target_tier".into(), target_tier.into());
    if let Some(v) = target_dc { body.insert("target_dc".into(), v.into()); }
    body.insert("allow_empty_prefix".into(), allow_empty_prefix.into());
    if let Some(v) = max_tasks { body.insert("max_tasks".into(), v.into()); }
    let body = serde_json::Value::Object(body);

    let resp = api.post_checked("/api/v1/migrations", &body).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            println!(
                "migrations create prefix={} target_tier={}",
                path_prefix, target_tier
            );
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
    }
    Ok(())
}

async fn cmd_volumes(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let resp = api.get("/api/v1/volumes").await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let volumes = resp["volumes"].as_array().cloned().unwrap_or_default();
            if volumes.is_empty() {
                println!("No volumes found.");
                return Ok(());
            }
            println!("{:<24} {:<10} {:<14}", "VOLUME ID", "FILES", "TOTAL SIZE");
            println!("{}", "-".repeat(50));
            for v in &volumes {
                println!(
                    "{:<24} {:<10} {:<14}",
                    v["volume_id"].as_str().unwrap_or("—"),
                    v["file_count"].as_u64().unwrap_or(0),
                    format_bytes(v["total_size"].as_u64().unwrap_or(0)),
                );
            }
        }
    }
    Ok(())
}

async fn cmd_files(api: &ApiClient, format: &OutputFormat, limit: usize) -> anyhow::Result<()> {
    // Page until `limit` files are collected or the server says it is done.
    // An older server ignores limit/offset and returns everything with no
    // `truncated` field: one request, cut to `limit` client-side.
    let mut files: Vec<Value> = Vec::new();
    let mut total: Option<u64> = None;
    let mut truncated = false;
    while files.len() < limit {
        let resp = api.get(&files_page_path(limit - files.len(), files.len())).await?;
        let page = resp["files"].as_array().cloned().unwrap_or_default();
        total = resp["total"].as_u64().or_else(|| resp["count"].as_u64());
        let more = resp["truncated"].as_bool();
        let empty = page.is_empty();
        files.extend(page);
        match more {
            Some(true) if !empty => continue,
            Some(m) => {
                truncated = m;
                break;
            }
            None => break,
        }
    }
    if files.len() > limit {
        files.truncate(limit);
        truncated = true;
    }
    let note = files_truncation_note(files.len(), total, truncated);
    let resp = json!({
        "count": files.len(),
        "total": total,
        "truncated": note.is_some(),
        "files": files,
    });
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let files = resp["files"].as_array().cloned().unwrap_or_default();
            println!("Total files: {}", total.unwrap_or(files.len() as u64));
            if files.is_empty() {
                return Ok(());
            }
            println!();
            println!(
                "{:<40} {:<12} {:<8} {:<12}",
                "PATH", "VOLUME", "CHUNKS", "SIZE"
            );
            println!("{}", "-".repeat(75));
            for f in &files {
                println!(
                    "{:<40} {:<12} {:<8} {:<12}",
                    truncate(f["path"].as_str().unwrap_or("—"), 38),
                    f["volume_id"].as_str().unwrap_or("—"),
                    f["chunk_count"].as_u64().unwrap_or(0),
                    format_bytes(f["total_size"].as_u64().unwrap_or(0)),
                );
            }
            if let Some(n) = &note {
                println!("{}", n);
            }
        }
    }
    Ok(())
}

async fn cmd_scrub(api: &ApiClient, level: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({ "level": level });

    let resp = api.post_checked("/api/v1/scrub", &body).await?;
    println!("Scrub triggered: all disks (level: {})", level);
    println!("Status: {}", resp["status"].as_str().unwrap_or("—"));
    Ok(())
}

fn orphan_reap_body(grace_secs: Option<u64>, dry_run: bool) -> Value {
    let mut b = serde_json::json!({ "dry_run": dry_run });
    if let Some(g) = grace_secs {
        b["grace_secs"] = g.into();
    }
    b
}

async fn cmd_scrub_orphans(
    api: &ApiClient,
    format: &OutputFormat,
    grace_secs: Option<u64>,
    dry_run: bool,
    wait: bool,
) -> anyhow::Result<()> {
    let resp = api.post_checked("/api/v1/scrub/orphans", &orphan_reap_body(grace_secs, dry_run)).await?;
    if !wait {
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
            OutputFormat::Table => println!(
                "Orphan reap started (grace_secs={}, dry_run={}); see `dtfs-cli scrub orphans-status`",
                resp["grace_secs"], resp["dry_run"]
            ),
        }
        return Ok(());
    }
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let v = api.get("/api/v1/scrub/orphans").await?;
        if v["running"] == false {
            print_orphan_status(format, &v)?;
            if let Some(e) = v["last_run"]["error"].as_str() {
                anyhow::bail!("orphan reap did not complete: {e}");
            }
            return Ok(());
        }
    }
}

fn print_orphan_status(format: &OutputFormat, v: &Value) -> anyhow::Result<()> {
    if matches!(format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(v)?);
        return Ok(());
    }
    println!("Running:          {}", v["running"]);
    println!("Configured grace: {} s (manual floor {} s)", v["configured_grace_secs"], v["manual_grace_floor_secs"]);
    let r = &v["last_run"];
    if r.is_null() {
        println!("Last run:         none on this manager (query the leader)");
        return Ok(());
    }
    let bytes = r["bytes_freed"].as_u64().map(format_bytes).unwrap_or_else(|| "n/a".into());
    println!(
        "Last run:         trigger={} grace={} s dry_run={} started_ms={} finished_ms={}",
        r["trigger"].as_str().unwrap_or("?"), r["grace_secs"], r["dry_run"], r["started_ms"], r["finished_ms"]
    );
    println!("Candidates:       {}  reaped: {}  freed: {}", r["candidates"], r["reaped"], bytes);
    if let Some(e) = r["error"].as_str() {
        println!("Error:            {e}");
    }
    if let Some(nodes) = r["nodes"].as_object() {
        println!("{:<10} {:>10} {:>11} {:>8} {:>8} {:>12}  ERROR", "NODE", "LISTED", "CANDIDATES", "REAPED", "FAILED", "FREED");
        for (id, n) in nodes {
            let freed = n["bytes_freed"].as_u64().map(format_bytes).unwrap_or_else(|| "n/a".into());
            println!(
                "{:<10} {:>10} {:>11} {:>8} {:>8} {:>12}  {}",
                id, n["listed"], n["candidates"], n["reaped"], n["delete_failed"], freed,
                n["error"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

async fn cmd_maintenance(
    api: &ApiClient,
    node_id: &str,
    enable: bool,
    disable: bool,
) -> anyhow::Result<()> {
    let should_enable = if disable { false } else { enable };

    let body = serde_json::json!({ "enable": should_enable });
    let resp = api
        .post_checked(&format!("/api/v1/nodes/{}/maintenance", node_id), &body)
        .await?;

    println!(
        "Maintenance mode {} for node {}",
        if should_enable { "ENABLED" } else { "DISABLED" },
        node_id
    );
    if let Some(m) = resp["maintenance"].as_bool() {
        println!("Current state: {}", if m { "maintenance" } else { "active" });
    }
    Ok(())
}

/// UIP-4-1 — `dtfs-cli unquarantine <node-id> --operator-id X
/// --reason-acknowledged "..."`: clear Bug BC-12's quarantine state.
/// Surfaces the server's JSON verbatim (including the 409 "not
/// Quarantined" body) and exits non-zero on failure.
async fn cmd_unquarantine(
    api: &ApiClient,
    node_id: &str,
    operator_id: &str,
    reason_acknowledged: &str,
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "operator_id": operator_id,
        "reason_acknowledged": reason_acknowledged,
    });
    let (status, value) = api
        .post_with_status(&format!("/api/v1/nodes/{}/unquarantine", node_id), &body)
        .await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    if !status.is_success() {
        anyhow::bail!(
            "unquarantine node {} failed: HTTP {}",
            node_id,
            status
        );
    }
    Ok(())
}

/// `dtfs-cli precache status`: table, or the server JSON with `--format json`.
/// A non-2xx answer (409 not the leader) prints the body and exits non-zero.
async fn cmd_precache_status(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let (status, v) = api.get_with_status("/api/v1/tiering/precache").await?;
    if !status.is_success() || matches!(format, OutputFormat::Json) {
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        print!("{}", render_precache_status(&v));
    }
    if !status.is_success() {
        anyhow::bail!("precache status failed: HTTP {}", status);
    }
    Ok(())
}

fn render_precache_group(out: &mut String, title: &str, groups: &Value) {
    let Some(gs) = groups.as_array().filter(|g| !g.is_empty()) else { return };
    out.push_str(&format!("
{title}:
"));
    for g in gs {
        let key = g["root"].as_str().or(g["dir"].as_str()).unwrap_or("?");
        out.push_str(&format!(
            "  {} {} {}  cached {} of {} (budget {})  cursor {}
",
            g["volume"].as_str().unwrap_or("?"),
            g["dc"].as_str().unwrap_or("?"),
            key,
            format_bytes(g["cached_bytes"].as_u64().unwrap_or(0)),
            format_bytes(g["desired_bytes"].as_u64().unwrap_or(0)),
            format_bytes(g["budget_bytes"].as_u64().unwrap_or(0)),
            g["cursor"].as_str().unwrap_or("-"),
        ));
        for f in g["files"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "    {}{}  {} of {}
",
                f["path"].as_str().unwrap_or("?"),
                if f["head"].as_bool() == Some(true) { " (head)" } else { "" },
                format_bytes(f["cached_bytes"].as_u64().unwrap_or(0)),
                format_bytes(f["desired_bytes"].as_u64().unwrap_or(0)),
            ));
        }
    }
}

/// Pure table rendering of `GET /api/v1/tiering/precache`; tolerates missing fields.
fn render_precache_status(v: &Value) -> String {
    let mut out = format!(
        "Leader: {}  rate: {} Mbps  yielded to repair: {}  in-flight tasks: {}
",
        v["is_leader"].as_bool().unwrap_or(false),
        v["rate_mbps"].as_u64().unwrap_or(0),
        v["yielded_to_repair"].as_bool().unwrap_or(false),
        v["inflight_tasks"].as_u64().unwrap_or(0),
    );
    let b = &v["last_batch"];
    out.push_str(&format!(
        "Last batch: new {} already {} deferred {} ({})
",
        b["new"].as_u64().unwrap_or(0),
        b["already"].as_u64().unwrap_or(0),
        b["deferred"].as_u64().unwrap_or(0),
        format_bytes(b["bytes"].as_u64().unwrap_or(0)),
    ));
    out.push_str(&format!("
{:<12} {:<6} {:>12} {:>12} {:>12}  {}
", "DC", "TIER", "ROOM", "DESIRED", "CACHED", "FLAGS"));
    for d in v["dcs"].as_array().into_iter().flatten() {
        let mut flags = Vec::new();
        if d["capacity_limited"].as_bool() == Some(true) {
            flags.push("capacity-limited");
        }
        if d["no_target"].as_bool() == Some(true) {
            flags.push("no-target");
        }
        out.push_str(&format!(
            "{:<12} {:<6} {:>12} {:>12} {:>12}  {}
",
            d["dc"].as_str().unwrap_or("?"),
            d["tier"].as_str().unwrap_or("?"),
            format_bytes(d["room_bytes"].as_u64().unwrap_or(0)),
            format_bytes(d["desired_bytes"].as_u64().unwrap_or(0)),
            format_bytes(d["cached_bytes"].as_u64().unwrap_or(0)),
            flags.join(","),
        ));
    }
    render_precache_group(&mut out, "Series", &v["series"]);
    render_precache_group(&mut out, "Cursors", &v["cursors"]);
    for s in v["skipped_volumes"].as_array().into_iter().flatten() {
        out.push_str(&format!(
            "
Skipped volume {}: {} ({})
",
            s["volume"].as_str().unwrap_or("?"),
            s["reason"].as_str().unwrap_or("?"),
            s["detail"].as_str().unwrap_or(""),
        ));
    }
    out
}

fn remove_dead_body(operator_id: &str, reason: &str, accept_data_loss: bool, dry_run: bool) -> serde_json::Value {
    serde_json::json!({
        "operator_id": operator_id,
        "reason": reason,
        "accept_data_loss": accept_data_loss,
        "dry_run": dry_run,
    })
}

/// UIP-4-2 — `dtfs-cli decommission <node-id> --operator-id X --reason
/// "..."`: remove a drained/dead node from the cluster registry.
/// Surfaces the server's JSON verbatim (including the 409 "replicas
/// remain" body) and exits non-zero on failure.
async fn cmd_decommission(
    api: &ApiClient,
    node_id: &str,
    operator_id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "operator_id": operator_id,
        "reason": reason,
    });
    let (status, value) = api
        .post_with_status(&format!("/api/v1/nodes/{}/decommission", node_id), &body)
        .await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    if !status.is_success() {
        anyhow::bail!(
            "decommission node {} failed: HTTP {}",
            node_id,
            status
        );
    }
    Ok(())
}

/// UIP-5-4 — `dtfs-cli files inspect <path>`: chunk layout + replica
/// placement of one file via `GET /api/v1/files/inspect?path=...`.
async fn cmd_files_inspect(api: &ApiClient, format: &OutputFormat, path: &str) -> anyhow::Result<()> {
    let url = format!("/api/v1/files/inspect?path={}", percent_encode_query(path));
    let resp = api.get(&url).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            println!("Path:        {}", resp["path"].as_str().unwrap_or(path));
            println!("Volume:      {}", resp["volume_id"].as_str().unwrap_or("—"));
            println!(
                "Size:        {}",
                format_bytes(resp["total_size"].as_u64().unwrap_or(0))
            );
            println!(
                "Chunk size:  {}",
                format_bytes(resp["chunk_size"].as_u64().unwrap_or(0))
            );
            println!("Chunks:      {}", resp["chunk_count"].as_u64().unwrap_or(0));
            let chunks = resp["chunks"].as_array().cloned().unwrap_or_default();
            if chunks.is_empty() {
                return Ok(());
            }
            println!();
            println!("{:<6} {:<20} {:<10} {}", "INDEX", "CHUNK_ID", "SIZE", "REPLICAS (node/disk tier)");
            println!("{}", "-".repeat(90));
            for c in &chunks {
                let replicas: Vec<String> = c["replicas"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|r| {
                        format!(
                            "{}/{} {}",
                            r["node_id"],
                            r["disk_id"],
                            r["tier"].as_str().unwrap_or("?")
                        )
                    })
                    .collect();
                println!(
                    "{:<6} {:<20} {:<10} {}",
                    c["chunk_index"].as_u64().unwrap_or(0),
                    truncate(c["chunk_id"].as_str().unwrap_or("—"), 18),
                    format_bytes(c["size"].as_u64().unwrap_or(0)),
                    replicas.join(", "),
                );
            }
        }
    }
    Ok(())
}

async fn cmd_audit(
    api: &ApiClient,
    format: &OutputFormat,
    limit: usize,
    action: Option<String>,
) -> anyhow::Result<()> {
    render_audit_entries(api, format, "/api/v1/audit", limit, action, "No audit entries found.").await
}

/// UIP-5-3 — `dtfs-cli integrity faults`: the manager's integrity-fault
/// view over the audit log (`GET /api/v1/integrity/faults`), same row
/// shape as `audit`.
async fn cmd_integrity_faults(
    api: &ApiClient,
    format: &OutputFormat,
    limit: usize,
    action: Option<String>,
) -> anyhow::Result<()> {
    render_audit_entries(
        api,
        format,
        "/api/v1/integrity/faults",
        limit,
        action,
        "No integrity faults recorded.",
    )
    .await
}

async fn render_audit_entries(
    api: &ApiClient,
    format: &OutputFormat,
    base: &str,
    limit: usize,
    action: Option<String>,
    empty_msg: &str,
) -> anyhow::Result<()> {
    let path = audit_path(base, limit, action.as_deref());

    let entries = api.get(&path).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&entries)?),
        OutputFormat::Table => {
            let entries = entries.as_array().cloned().unwrap_or_default();
            if entries.is_empty() {
                println!("{}", empty_msg);
                return Ok(());
            }
            println!(
                "{:<22} {:<14} {:<24} {:<30} {}",
                "TIME", "ACTOR", "ACTION", "TARGET", "RESULT"
            );
            println!("{}", "-".repeat(100));
            for e in &entries {
                let ts = e["timestamp"].as_i64().unwrap_or(0);
                let time = chrono::DateTime::from_timestamp(ts, 0)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "—".to_string());
                println!(
                    "{:<22} {:<14} {:<24} {:<30} {}",
                    time,
                    e["actor"].as_str().unwrap_or("—"),
                    e["action"].as_str().unwrap_or("—"),
                    truncate(e["target"].as_str().unwrap_or("—"), 28),
                    e["result"].as_str().unwrap_or("—"),
                );
            }
        }
    }
    Ok(())
}

async fn cmd_config(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let config = api.get("/api/v1/config").await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&config)?),
        OutputFormat::Table => {
            println!("Cluster Configuration");
            println!("=====================");
            println!(
                "Config Version: {}",
                config["version"].as_u64().unwrap_or(0)
            );
            println!();

            if let Some(defaults) = config.get("defaults") {
                println!("Defaults:");
                println!(
                    "  Chunk Size:       {} MB",
                    defaults["chunk_size_mb"].as_u64().unwrap_or(0)
                );
                if let Some(ws) = defaults.get("write_safety") {
                    println!(
                        "  Min Replicas ACK: {}",
                        ws["min_replicas_before_ack"].as_u64().unwrap_or(0)
                    );
                    println!(
                        "  Write Timeout:    {}s",
                        ws["write_timeout_secs"].as_u64().unwrap_or(0)
                    );
                }
                if let Some(repl) = defaults.get("replication") {
                    println!(
                        "  Replication:      hot={}, warm={}, cold={}",
                        repl["hot"].as_u64().unwrap_or(0),
                        repl["warm"].as_u64().unwrap_or(0),
                        repl["cold"].as_u64().unwrap_or(0),
                    );
                }
            }

            if let Some(tiering) = config.get("tiering") {
                println!();
                println!("Tiering:");
                println!(
                    "  Enabled:              {}",
                    tiering["enabled"].as_bool().unwrap_or(false)
                );
                println!(
                    "  Hot Threshold:        {} reads/hour",
                    tiering["hot_threshold_reads_per_hour"]
                        .as_u64()
                        .unwrap_or(0)
                );
                println!(
                    "  Cold Threshold:       {} days",
                    tiering["cold_threshold_days"].as_u64().unwrap_or(0)
                );
            }
        }
    }
    Ok(())
}

/// UIP-5-1 — `dtfs-cli config set --config-json '...' [--write-to-disk]
/// [--acknowledge-cluster-wide]`: apply a config override via `POST
/// /api/v1/config`. Surfaces the server's JSON verbatim (including a
/// 400 "preserved_current" body) and exits non-zero on failure, same
/// shape as `cmd_unquarantine`/`cmd_decommission`.
async fn cmd_config_set(
    api: &ApiClient,
    config_json: &str,
    write_to_disk: bool,
    acknowledge_cluster_wide: bool,
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "config_json": config_json,
        "write_to_disk": write_to_disk,
        "acknowledge_cluster_wide": acknowledge_cluster_wide,
    });
    let (status, value) = api.post_with_status("/api/v1/config", &body).await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    if !status.is_success() {
        anyhow::bail!("config set failed: HTTP {}", status);
    }
    Ok(())
}

/// UIP-5-2 — `dtfs-cli config effective`: list every effective config
/// key via `GET /config/effective`, rendering `{name, current, default,
/// is_default}` rows as a table (or raw JSON with `--format json`).
async fn cmd_config_effective(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let resp = api.get("/api/v1/config/effective").await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let entries = resp["entries"].as_array().cloned().unwrap_or_default();
            println!("{:<50} {:<16} {:<16} {}", "NAME", "CURRENT", "DEFAULT", "IS_DEFAULT");
            println!("{}", "-".repeat(100));
            for e in &entries {
                println!(
                    "{:<50} {:<16} {:<16} {}",
                    truncate(e["name"].as_str().unwrap_or("—"), 48),
                    truncate(&e["current"].to_string(), 14),
                    truncate(&e["default"].to_string(), 14),
                    e["is_default"].as_bool().unwrap_or(false),
                );
            }
        }
    }
    Ok(())
}

/// UIP-5-5 — `dtfs-cli stats`: the aggregate the webui Home page
/// renders from `GET /api/v1/stats`.
async fn cmd_stats(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let s = api.get("/api/v1/stats").await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&s)?),
        OutputFormat::Table => {
            let u = |k: &str| s[k].as_u64().unwrap_or(0);
            println!("DTFS Cluster Stats");
            println!("==================");
            println!("Health:            {}", s["health"].as_str().unwrap_or("—"));
            println!(
                "Nodes:             {} total, {} healthy, {} quarantined",
                u("total_nodes"),
                u("healthy_nodes"),
                u("quarantined_nodes")
            );
            println!(
                "Roles:             {} manager, {} data, {} client",
                u("manager_count"),
                u("data_node_count"),
                u("client_count")
            );
            println!("Disks:             {}", u("disk_count"));
            println!("Files:             {}", u("file_count"));
            println!(
                "Raw capacity:      {} used / {}",
                format_bytes(u("raw_used_bytes")),
                format_bytes(u("raw_total_bytes"))
            );
            println!(
                "Net capacity:      {} used / {}",
                format_bytes(u("net_used_bytes")),
                format_bytes(u("net_usable_bytes"))
            );
            println!("Logical bytes:     {}", format_bytes(u("logical_bytes")));
            println!(
                "I/O:               {}/s read, {}/s write",
                format_bytes(u("io_read_bytes_per_sec")),
                format_bytes(u("io_write_bytes_per_sec"))
            );
            println!("Anchor pending:    {}", u("anchor_pending"));

            let tiers = s["tier_breakdown"].as_array().cloned().unwrap_or_default();
            if !tiers.is_empty() {
                println!();
                println!("{:<6} {:<6} {:<12} {:<12}", "TIER", "DISKS", "USED", "TOTAL");
                println!("{}", "-".repeat(40));
                for t in &tiers {
                    println!(
                        "{:<6} {:<6} {:<12} {:<12}",
                        t["tier"].as_str().unwrap_or("—"),
                        t["disks"].as_u64().unwrap_or(0),
                        format_bytes(t["used_bytes"].as_u64().unwrap_or(0)),
                        format_bytes(t["total_bytes"].as_u64().unwrap_or(0)),
                    );
                }
            }

            let ls = &s["last_scrub"];
            println!();
            if ls.is_null() {
                println!("Last scrub:        none recorded");
            } else {
                let at = ls["completed_at_ms"].as_i64().unwrap_or(0) / 1000;
                let when = chrono::DateTime::from_timestamp(at, 0)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "—".to_string());
                println!(
                    "Last scrub:        {} level={} node={} disk={}",
                    when,
                    ls["level"].as_str().unwrap_or("—"),
                    ls["node_id"].as_str().unwrap_or("—"),
                    ls["disk_path"].as_str().unwrap_or("—")
                );
                println!(
                    "                   checked={} healthy={} corrupt={} missing={}",
                    ls["chunks_checked"].as_u64().unwrap_or(0),
                    ls["chunks_healthy"].as_u64().unwrap_or(0),
                    ls["chunks_corrupt"].as_u64().unwrap_or(0),
                    ls["chunks_missing"].as_u64().unwrap_or(0)
                );
            }
        }
    }
    Ok(())
}

async fn cmd_health(api: &ApiClient) -> anyhow::Result<()> {
    let health = api.get("/api/v1/health").await?;
    let status = health["status"].as_str().unwrap_or("unknown");
    if status == "healthy" {
        println!("Manager is healthy");
    } else {
        println!("Manager status: {}", status);
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CF-2 — raft membership commands
// ---------------------------------------------------------------------------

async fn cmd_raft_members(api: &ApiClient, format: &OutputFormat) -> anyhow::Result<()> {
    let state = api.get("/api/v1/raft/state").await?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&state)?);
        }
        OutputFormat::Table => {
            println!(
                "Queried: {} (state={}, term={})",
                state["hostname"].as_str().unwrap_or("?"),
                state["state"].as_str().unwrap_or("?"),
                state["current_term"]
            );
            let leader_id = state["current_leader"].as_str().unwrap_or("");
            println!(
                "{:<22} {:<22} {:<24} {:<8} {}",
                "HOSTNAME", "NODE_ID", "ADDR", "ROLE", ""
            );
            for m in state["membership"].as_array().into_iter().flatten() {
                let id = m["node_id"].as_str().unwrap_or("?");
                println!(
                    "{:<22} {:<22} {:<24} {:<8} {}",
                    m["hostname"].as_str().unwrap_or("?"),
                    id,
                    m["addr"].as_str().unwrap_or("?"),
                    if m["is_voter"].as_bool().unwrap_or(false) {
                        "voter"
                    } else {
                        "learner"
                    },
                    if id == leader_id { "(leader)" } else { "" },
                );
            }
            if state["membership"].as_array().map(|a| a.is_empty()).unwrap_or(true) {
                println!("(membership empty — this manager was never joined to a cluster)");
            }
        }
    }
    Ok(())
}

/// Shared POST runner for add-learner/promote/remove: prints the
/// server's JSON verbatim and exits non-zero on a non-2xx answer (the
/// body carries the reason, e.g. the 409 leader hint).
async fn cmd_raft_change(api: &ApiClient, path: &str, body: Value) -> anyhow::Result<()> {
    let (status, value) = api.post_with_status(path, &body).await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    if !status.is_success() {
        anyhow::bail!("{} failed: HTTP {}", path, status);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let exp = (bytes as f64).log(1024.0).floor() as usize;
    let exp = exp.min(UNITS.len() - 1);
    let value = bytes as f64 / 1024_f64.powi(exp as i32);
    format!("{:.1} {}", value, UNITS[exp])
}

/// Minimal RFC 3986 query-component percent-encoding (unreserved chars
/// and `/` pass through). Avoids pulling a crate in for one call site.
fn percent_encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        // Take whole chars whose cumulative byte length fits within the
        // budget left for the "..." suffix, so we never slice mid-char
        // (would panic on multi-byte UTF-8) and stay within `max` bytes.
        let budget = max.saturating_sub(3);
        let mut prefix = String::new();
        for c in s.chars() {
            if prefix.len() + c.len_utf8() > budget {
                break;
            }
            prefix.push(c);
        }
        format!("{prefix}...")
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    // CO-5-11 — truncate() byte-slices `&s[..max-3]`, which panics when
    // the cut index lands inside a multibyte UTF-8 char. It must instead
    // truncate on a char boundary and return something ending in "...".
    #[test]
    fn co_5_11_truncate_handles_multibyte_boundary() {
        // 40 x "é" (2 bytes each) = 80 bytes; max=10 -> cut at byte 7,
        // which is mid-"é" -> panic on current byte-slicing code.
        let s = "é".repeat(40);
        let out = truncate(&s, 10);
        assert!(out.ends_with("..."), "truncated string should end with ...: {out:?}");
        assert!(out.len() <= 10, "result must respect the max byte budget: {out:?}");

        // A real title that straddles the boundary.
        let title = "Amélie (2001) Directors Cut Remastered";
        let out2 = truncate(title, 6);
        assert!(out2.ends_with("..."), "got {out2:?}");
    }

    // CO-5-13 — `maintenance <node>` with neither --enable nor --disable
    // must be rejected by the parser (clap ArgGroup), not silently
    // default to enable=false (= disable). Parsing with no flag succeeds
    // today, so this fails (the parse is Ok when it should be Err).
    #[test]
    fn co_5_13_maintenance_requires_explicit_flag() {
        let res = Args::try_parse_from(["dtfs-cli", "maintenance", "n1"]);
        assert!(
            res.is_err(),
            "maintenance with no --enable/--disable must be rejected, got {:?}",
            res.map(|_| "Ok(parsed)")
        );

        // The explicit forms still parse.
        assert!(Args::try_parse_from(["dtfs-cli", "maintenance", "n1", "--enable"]).is_ok());
        assert!(Args::try_parse_from(["dtfs-cli", "maintenance", "n1", "--disable"]).is_ok());
    }

    // UIP-3-1 — `volumes set-anchor <id> --datacenter D` parses into the
    // expected variant/fields.
    #[test]
    fn uip_3_1_volumes_set_anchor_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "volumes", "set-anchor", "default", "--datacenter", "leli",
        ])
        .expect("set-anchor should parse");
        match args.command {
            Commands::Volumes(VolumesCmd::SetAnchor {
                volume_id,
                datacenter,
                required_tier,
                ..
            }) => {
                assert_eq!(volume_id, "default");
                assert_eq!(datacenter.as_deref(), Some("leli"));
                assert_eq!(required_tier, None);
            }
            other => panic!("expected VolumesCmd::SetAnchor, got {other:?}"),
        }
    }

    /// `volumes delete` confirmation: scripted answers, `None` = no terminal.
    fn run_confirm(data: Option<&str>, yes: bool, confirm: Option<&str>, answers: &[Option<&str>]) -> (Result<(), String>, usize) {
        let mut it = answers.iter();
        let mut asked = 0;
        let mut prompt = |_: &str| {
            asked += 1;
            it.next().copied().flatten().map(String::from)
        };
        let r = confirm_volume_delete("media", data, yes, confirm, &mut prompt);
        (r, asked)
    }

    #[test]
    fn volumes_delete_confirmation() {
        let data = Some("3 files (9 bytes)");
        // Empty: one question, --yes skips it.
        assert_eq!(run_confirm(None, false, None, &[Some("y\n")]), (Ok(()), 1));
        assert_eq!(run_confirm(None, false, None, &[Some("\n")]).0, Err("aborted".into()));
        assert_eq!(run_confirm(None, true, None, &[]), (Ok(()), 0));
        // Data: both questions, the second must be the exact id.
        assert_eq!(run_confirm(data, false, None, &[Some("yes\n"), Some("media\n")]), (Ok(()), 2));
        assert!(run_confirm(data, false, None, &[Some("y\n"), Some("Media\n")]).0.unwrap_err().contains("does not match"));
        assert_eq!(run_confirm(data, false, None, &[Some("n\n")]), (Err("aborted".into()), 1));
        // --yes only still asks for the id; without a terminal that aborts.
        assert_eq!(run_confirm(data, true, None, &[Some("media\n")]), (Ok(()), 1));
        assert!(run_confirm(data, true, None, &[None]).0.unwrap_err().starts_with("no terminal input"));
        // --yes with a wrong / right --confirm.
        assert!(run_confirm(data, true, Some("medi"), &[]).0.unwrap_err().contains("--confirm does not match"));
        assert_eq!(run_confirm(data, true, Some("media"), &[]), (Ok(()), 0));
        // No terminal, no flags: abort for both shapes.
        assert!(run_confirm(None, false, None, &[None]).0.unwrap_err().starts_with("no terminal input"));
        assert!(run_confirm(data, false, Some("media"), &[None]).0.unwrap_err().starts_with("no terminal input"));

        let a = Args::try_parse_from(["dtfs-cli", "volumes", "delete", "media", "--yes", "--confirm", "media"]).unwrap();
        assert!(matches!(a.command, Commands::Volumes(VolumesCmd::Delete { yes: true, confirm: Some(ref c), .. }) if c == "media"));
    }

    #[test]
    fn force_flag_parses_and_reaches_request_body() {
        let a = Args::try_parse_from(["dtfs-cli", "volumes", "create", "v", "--force"]).unwrap();
        assert!(matches!(a.command, Commands::Volumes(VolumesCmd::Create { force: true, .. })));
        let a = Args::try_parse_from(["dtfs-cli", "volumes", "set-anchor", "v", "--force"]).unwrap();
        assert!(matches!(a.command, Commands::Volumes(VolumesCmd::SetAnchor { force: true, .. })));
        let a = Args::try_parse_from(["dtfs-cli", "volumes", "create", "v"]).unwrap();
        assert!(matches!(a.command, Commands::Volumes(VolumesCmd::Create { force: false, .. })));

        assert_eq!(volumes_create_body("v", Some(2), true)["force"], true);
        assert_eq!(volumes_create_body("v", Some(2), false)["replication_hot"], 2);
        assert!(volumes_create_body("v", None, false).get("force").is_none());
        assert!(Args::try_parse_from(["dtfs-cli", "scrub", "--disk", "/x"]).is_err());
        let a = Args::try_parse_from(["dtfs-cli", "scrub", "--level", "quick"]).unwrap();
        assert!(matches!(a.command, Commands::Scrub { action: None, .. }));
        let a = Args::try_parse_from(["dtfs-cli", "scrub", "orphans", "--grace-secs", "3600", "--dry-run"]).unwrap();
        assert!(matches!(
            a.command,
            Commands::Scrub { action: Some(ScrubCmd::Orphans { grace_secs: Some(3600), dry_run: true, wait: false }), .. }
        ));
        assert!(Args::try_parse_from(["dtfs-cli", "scrub", "orphans", "--grace-secs", "-1"]).is_err());
        assert!(matches!(
            Args::try_parse_from(["dtfs-cli", "scrub", "orphans-status"]).unwrap().command,
            Commands::Scrub { action: Some(ScrubCmd::OrphansStatus), .. }
        ));
        assert_eq!(orphan_reap_body(None, true), serde_json::json!({"dry_run": true}));
        assert_eq!(orphan_reap_body(Some(7200), false)["grace_secs"], 7200);
        assert!(Args::try_parse_from(["dtfs-cli", "volumes", "create", "v", "--tier-policy", "x"]).is_err());
        let b = set_anchor_body(Some("leli".into()), None, true);
        assert_eq!((b["force"].clone(), b["datacenter"].clone()), (true.into(), "leli".into()));
        assert!(set_anchor_body(None, None, false).get("force").is_none());
    }

    // UIP-3-2 — `volumes set-spill <id> --allow true --forbidden-tiers
    // hot,warm` parses into the expected variant/fields, including
    // comma-delimited list splitting.
    #[test]
    fn uip_3_2_volumes_set_spill_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "volumes", "set-spill", "default",
            "--allow", "true", "--forbidden-tiers", "hot,warm",
        ])
        .expect("set-spill should parse");
        match args.command {
            Commands::Volumes(VolumesCmd::SetSpill {
                volume_id,
                allow,
                forbidden_tiers,
                forbidden_datacenters,
            }) => {
                assert_eq!(volume_id, "default");
                assert_eq!(allow, Some(true));
                assert_eq!(forbidden_tiers, Some(vec!["hot".to_string(), "warm".to_string()]));
                assert_eq!(forbidden_datacenters, None);
            }
            other => panic!("expected VolumesCmd::SetSpill, got {other:?}"),
        }
    }

    // UIP-4-1 — `unquarantine <node-id> --operator-id X
    // --reason-acknowledged "..."` requires both flags; either missing
    // is a parse error.
    #[test]
    fn uip_4_1_unquarantine_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "unquarantine", "n1",
            "--operator-id", "alice",
            "--reason-acknowledged", "investigated and re-hashed, chunk is trusted",
        ])
        .expect("unquarantine should parse");
        match args.command {
            Commands::Unquarantine {
                node_id,
                operator_id,
                reason_acknowledged,
            } => {
                assert_eq!(node_id, "n1");
                assert_eq!(operator_id, "alice");
                assert_eq!(reason_acknowledged, "investigated and re-hashed, chunk is trusted");
            }
            other => panic!("expected Commands::Unquarantine, got {other:?}"),
        }

        assert!(
            Args::try_parse_from([
                "dtfs-cli", "unquarantine", "n1",
                "--reason-acknowledged", "investigated and re-hashed, chunk is trusted",
            ])
            .is_err(),
            "missing --operator-id must be rejected"
        );
        assert!(
            Args::try_parse_from([
                "dtfs-cli", "unquarantine", "n1",
                "--operator-id", "alice",
            ])
            .is_err(),
            "missing --reason-acknowledged must be rejected"
        );
    }

    #[test]
    fn nodes_remove_dead_parses_and_plain_nodes_still_works() {
        let a = Args::try_parse_from(["dtfs-cli", "nodes", "remove-dead", "7", "--dry-run", "--accept-data-loss"])
            .expect("remove-dead parses");
        match a.command {
            Commands::Nodes { cmd: Some(NodesCmd::RemoveDead { node_id, accept_data_loss, dry_run, operator_id, .. }), .. } => {
                assert_eq!((node_id.as_str(), accept_data_loss, dry_run, operator_id.as_str()), ("7", true, true, "dtfs-cli"));
            }
            other => panic!("wrong parse: {other:?}"),
        }
        let a = Args::try_parse_from(["dtfs-cli", "nodes", "--id", "7"]).expect("nodes --id parses");
        assert!(matches!(a.command, Commands::Nodes { id: Some(_), cmd: None }));
        assert!(matches!(
            Args::try_parse_from(["dtfs-cli", "nodes"]).unwrap().command,
            Commands::Nodes { id: None, cmd: None }
        ));
        assert!(Args::try_parse_from(["dtfs-cli", "nodes", "remove-dead"]).is_err(), "node id required");
        let b = remove_dead_body("op", "why", true, false);
        assert_eq!((b["accept_data_loss"].as_bool(), b["dry_run"].as_bool()), (Some(true), Some(false)));
    }

    // UIP-4-2 — `decommission <node-id> --operator-id X --reason "..."`
    // requires both flags; either missing is a parse error.
    #[test]
    fn uip_4_2_decommission_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "decommission", "n1",
            "--operator-id", "alice",
            "--reason", "drained and dead, removing from registry",
        ])
        .expect("decommission should parse");
        match args.command {
            Commands::Decommission {
                node_id,
                operator_id,
                reason,
            } => {
                assert_eq!(node_id, "n1");
                assert_eq!(operator_id, "alice");
                assert_eq!(reason, "drained and dead, removing from registry");
            }
            other => panic!("expected Commands::Decommission, got {other:?}"),
        }

        assert!(
            Args::try_parse_from([
                "dtfs-cli", "decommission", "n1",
                "--reason", "drained and dead, removing from registry",
            ])
            .is_err(),
            "missing --operator-id must be rejected"
        );
        assert!(
            Args::try_parse_from([
                "dtfs-cli", "decommission", "n1",
                "--operator-id", "alice",
            ])
            .is_err(),
            "missing --reason must be rejected"
        );
    }

    // UIP-4-3 — `migrations create <prefix> --target-tier T
    // [--target-dc DC] [--allow-empty-prefix] [--max-tasks N]` parses
    // into the expected variant/fields, required/optional flags.
    #[test]
    fn uip_4_3_migrations_create_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "migrations", "create", "/movies/",
            "--target-tier", "cold", "--target-dc", "dc-a",
            "--allow-empty-prefix", "--max-tasks", "500",
        ])
        .expect("migrations create should parse");
        match args.command {
            Commands::Migrations(MigrationsCmd::Create {
                path_prefix,
                target_tier,
                target_dc,
                allow_empty_prefix,
                max_tasks,
            }) => {
                assert_eq!(path_prefix, "/movies/");
                assert_eq!(target_tier, "cold");
                assert_eq!(target_dc.as_deref(), Some("dc-a"));
                assert!(allow_empty_prefix);
                assert_eq!(max_tasks, Some(500));
            }
            other => panic!("expected MigrationsCmd::Create, got {other:?}"),
        }

        // Optional flags omitted still parse.
        let minimal = Args::try_parse_from([
            "dtfs-cli", "migrations", "create", "/movies/",
            "--target-tier", "hot",
        ])
        .expect("minimal migrations create should parse");
        match minimal.command {
            Commands::Migrations(MigrationsCmd::Create {
                target_dc, allow_empty_prefix, max_tasks, ..
            }) => {
                assert_eq!(target_dc, None);
                assert!(!allow_empty_prefix);
                assert_eq!(max_tasks, None);
            }
            other => panic!("expected MigrationsCmd::Create, got {other:?}"),
        }
    }

    // UIP-4-3 — client-side `target_tier` validation rejects an
    // invalid tier name before any network call; the four real tiers
    // (case-insensitively) are accepted.
    #[test]
    fn uip_4_3_invalid_target_tier_rejected_client_side() {
        assert!(is_valid_target_tier("hot"));
        assert!(is_valid_target_tier("Warm"));
        assert!(is_valid_target_tier("COLD"));
        assert!(is_valid_target_tier("tape"));
        assert!(!is_valid_target_tier("lukewarm"));
        assert!(!is_valid_target_tier(""));
    }

    // UIP-5-1 — bare `config` still parses to `action: None`, preserving
    // today's GET-and-render behaviour byte-for-byte.
    #[test]
    fn uip_5_1_bare_config_has_no_action() {
        let args = Args::try_parse_from(["dtfs-cli", "config"]).expect("bare config should parse");
        match args.command {
            Commands::Config { action, .. } => assert!(action.is_none()),
            other => panic!("expected Commands::Config, got {other:?}"),
        }
    }

    // UIP-5-1 — `config set --config-json '{}' --write-to-disk` parses
    // into `Some(ConfigAction::Set{..})` with the expected fields.
    #[test]
    fn uip_5_1_config_set_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "config", "set", "--config-json", "{}", "--write-to-disk",
        ])
        .expect("config set should parse");
        match args.command {
            Commands::Config { action, .. } => match action {
                Some(ConfigAction::Set {
                    config_json,
                    write_to_disk,
                    acknowledge_cluster_wide,
                }) => {
                    assert_eq!(config_json, "{}");
                    assert!(write_to_disk);
                    assert!(!acknowledge_cluster_wide);
                }
                other => panic!("expected Some(ConfigAction::Set), got {other:?}"),
            },
            other => panic!("expected Commands::Config, got {other:?}"),
        }
    }

    // UIP-5-2 — `config effective` parses into `Some(ConfigAction::Effective)`.
    #[test]
    fn uip_5_2_config_effective_parses() {
        let args = Args::try_parse_from(["dtfs-cli", "config", "effective"])
            .expect("config effective should parse");
        match args.command {
            Commands::Config { action, .. } => {
                assert!(matches!(action, Some(ConfigAction::Effective)));
            }
            other => panic!("expected Commands::Config, got {other:?}"),
        }
    }

    // UIP-5-3 — `integrity faults` parses with the audit-style flags.
    #[test]
    fn uip_5_3_integrity_faults_parses() {
        let args = Args::try_parse_from([
            "dtfs-cli", "integrity", "faults", "--limit", "5", "--action", "scrub.",
        ])
        .expect("integrity faults should parse");
        match args.command {
            Commands::Integrity(IntegrityCmd::Faults { limit, action }) => {
                assert_eq!(limit, 5);
                assert_eq!(action.as_deref(), Some("scrub."));
            }
            other => panic!("expected Commands::Integrity, got {other:?}"),
        }
    }

    // UIP-5-4 — `files inspect <path>` parses.
    #[test]
    fn uip_5_4_files_inspect_parses() {
        let args = Args::try_parse_from(["dtfs-cli", "files", "inspect", "/movies/x.mkv"])
            .expect("files inspect should parse");
        match args.command {
            Commands::Files(FilesCmd::Inspect { path }) => assert_eq!(path, "/movies/x.mkv"),
            other => panic!("expected Commands::Files(Inspect), got {other:?}"),
        }
    }

    #[test]
    fn cli_helpers_scheme_paging_flush_audit() {
        assert_eq!(base_url_for("httpd01:9402"), "http://httpd01:9402");
        assert_eq!(base_url_for("https://m:9402"), "https://m:9402");
        assert_eq!(base_url_for("http://m:9402"), "http://m:9402");
        assert_eq!(files_page_path(50, 100), "/api/v1/files?limit=50&offset=100");
        assert_eq!(files_truncation_note(50, Some(120), true).unwrap(), "showing 50 of 120 (raise --limit to see more)");
        assert!(files_truncation_note(120, Some(120), false).is_none());
        assert!(files_truncation_note(5, None, false).is_none());
        assert!(flush_failure_summary(&json!({"rpc_failures": 0, "raft_failures": 0})).is_none());
        assert_eq!(
            flush_failure_summary(&json!({"rpc_failures": 2, "raft_failures": 1})).unwrap(),
            "flush-cache incomplete: rpc_failures=2 raft_failures=1"
        );
        assert_eq!(audit_path("/api/v1/audit", 5, Some("a&b c")), "/api/v1/audit?limit=5&action=a%26b%20c");
        assert_eq!(audit_path("/api/v1/audit", 5, None), "/api/v1/audit?limit=5");
    }

    #[test]
    fn uip_5_4_percent_encode_query_escapes_reserved() {
        assert_eq!(percent_encode_query("/tv/a b&c=d.mkv"), "/tv/a%20b%26c%3Dd.mkv");
        assert_eq!(percent_encode_query("/plain-path_1.mkv"), "/plain-path_1.mkv");
    }

    // UIP-5-5 — bare `stats` parses.
    #[test]
    fn uip_5_5_stats_parses() {
        let args = Args::try_parse_from(["dtfs-cli", "stats"]).expect("stats should parse");
        assert!(matches!(args.command, Commands::Stats));
    }

    // Audit follow-up — `post_checked` must surface the server's `error`
    // string; exercised via the same extraction on a synthetic body.
    #[test]
    fn post_checked_error_extraction_prefers_error_field() {
        let value = serde_json::json!({"error": "interval must be >= 60 or 0"});
        let msg = value["error"].as_str().map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        assert_eq!(msg, "interval must be >= 60 or 0");
        let value = serde_json::json!({"leader": null});
        let msg = value["error"].as_str().map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        assert_eq!(msg, "{\"leader\":null}");
    }
}

#[cfg(test)]
mod precache_tests {
    use super::*;

    #[test]
    fn set_precache_body_only_carries_supplied_flags() {
        assert_eq!(
            set_precache_body(Some("plex"), None, None, Some(20), Some(3)),
            serde_json::json!({"mode":"plex","series_max_gb":20,"max_series":3})
        );
        assert_eq!(set_precache_body(None, None, None, None, None), serde_json::json!({}));
    }
}

#[cfg(test)]
mod precache_status_tests {
    use super::*;

    #[test]
    fn render_precache_status_shows_dcs_and_series_files() {
        let v = serde_json::json!({
            "is_leader": true, "rate_mbps": 10, "inflight_tasks": 3,
            "last_batch": {"new": 2, "already": 1, "deferred": 0, "bytes": 1048576},
            "dcs": [{"dc": "toro", "tier": "warm", "room_bytes": 1073741824u64, "desired_bytes": 2048,
                     "cached_bytes": 1024, "capacity_limited": true, "no_target": false}],
            "series": [{"volume": "media", "dc": "toro", "root": "/tv/Show", "cursor": "/tv/Show/S01/E02.mkv",
                        "last_read_ms": 1, "budget_bytes": 4096, "desired_bytes": 2048, "cached_bytes": 1024,
                        "files": [{"path": "/tv/Show/S01/E02.mkv", "bytes": 1, "desired_bytes": 2048,
                                   "cached_bytes": 1024, "head": true}]}],
            "cursors": [],
            "skipped_volumes": [{"volume": "old", "reason": "policy_invalid", "detail": "no cache tier"}],
        });
        let out = render_precache_status(&v);
        for want in ["toro", "/tv/Show", "/tv/Show/S01/E02.mkv (head)", "capacity-limited", "policy_invalid"] {
            assert!(out.contains(want), "missing {want} in {out}");
        }
    }

    #[test]
    fn render_precache_status_empty_object_does_not_panic() {
        let out = render_precache_status(&serde_json::json!({}));
        assert!(out.contains("Leader: false"), "{out}");
        assert!(!out.contains("Series"), "{out}");
    }
}
