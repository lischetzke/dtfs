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

    /// Output format
    #[arg(long, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
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

    /// List all data nodes
    Nodes {
        /// Show detailed info for a specific node
        #[arg(long)]
        id: Option<String>,
    },

    /// Manage volumes
    #[command(subcommand)]
    Volumes(VolumesCmd),

    /// File operations (list, flush cache)
    #[command(subcommand)]
    Files(FilesCmd),

    /// Trigger or view scrub operations
    Scrub {
        /// Target: --disk /path, --file /path, or --chunk <id>
        #[arg(long)]
        disk: Option<String>,
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        chunk: Option<String>,
        /// Scrub level: quick or deep
        #[arg(long, default_value = "deep")]
        level: String,
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

    /// View audit log
    Audit {
        /// Maximum entries to show
        #[arg(long, default_value = "20")]
        limit: usize,
        /// Filter by action prefix
        #[arg(long)]
        action: Option<String>,
    },

    /// View or update cluster configuration
    Config {
        /// Show current config
        #[arg(long)]
        show: bool,
    },

    /// Check manager health
    Health,

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
        /// Install the new binary but do not restart the service.
        #[arg(long)]
        no_restart: bool,
        /// Skip the confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
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
        /// Per-tier replication factor (hot)
        #[arg(long)]
        replication_factor: Option<u32>,
        /// Tier policy hint (currently informational)
        #[arg(long)]
        tier_policy: Option<String>,
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

struct ApiClient {
    base_url: String,
    client: reqwest::Client,
}

impl ApiClient {
    fn new(manager_addr: &str) -> Self {
        let base_url = if manager_addr.starts_with("http") {
            manager_addr.to_string()
        } else {
            format!("http://{}", manager_addr)
        };

        Self {
            base_url,
            client: reqwest::Client::new(),
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

    async fn post(&self, path: &str, body: &Value) -> anyhow::Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
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
        let resp = self.client.post(&url).json(body).send().await?;
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or_else(|_| json!({}));
        Ok((status, value))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let api = ApiClient::new(&args.manager);

    match args.command {
        Commands::Status => cmd_status(&api, &args.format).await?,
        Commands::Nodes { id } => cmd_nodes(&api, &args.format, id).await?,
        Commands::Volumes(sub) => match sub {
            VolumesCmd::List => cmd_volumes(&api, &args.format).await?,
            VolumesCmd::Create {
                name,
                replication_factor,
                tier_policy,
            } => cmd_volumes_create(&api, &args.format, &name, replication_factor, tier_policy.as_deref()).await?,
            VolumesCmd::MigratePolicy { volume_id } => {
                cmd_volumes_migrate_policy(&api, &args.format, &volume_id).await?
            }
            VolumesCmd::Show { volume_id } => {
                cmd_volumes_show(&api, &args.format, &volume_id).await?
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
        },
        Commands::Files(sub) => match sub {
            FilesCmd::List { limit } => cmd_files(&api, &args.format, limit).await?,
            FilesCmd::FlushCache {
                path, cache_tiers, cooldown_secs, max_tasks, allow_empty_prefix,
            } => cmd_files_flush_cache(
                &api, &args.format, &path, &cache_tiers,
                cooldown_secs, max_tasks, allow_empty_prefix,
            ).await?,
        },
        Commands::Scrub {
            disk,
            file,
            chunk,
            level,
        } => cmd_scrub(&api, disk, file, chunk, &level).await?,
        Commands::Maintenance {
            node_id,
            enable,
            disable,
        } => cmd_maintenance(&api, &node_id, enable, disable).await?,
        Commands::Audit { limit, action } => cmd_audit(&api, &args.format, limit, action).await?,
        Commands::Config { show: _ } => cmd_config(&api, &args.format).await?,
        Commands::Health => cmd_health(&api).await?,
        Commands::Raft(sub) => match sub {
            RaftCmd::Members => cmd_raft_members(&api, &args.format).await?,
            RaftCmd::AddLearner {
                hostname,
                addr,
                secret,
            } => {
                cmd_raft_change(
                    &api,
                    "/api/v1/raft/add-learner",
                    json!({"hostname": hostname, "addr": addr, "shared_secret": secret}),
                )
                .await?
            }
            RaftCmd::Promote { hostname, secret } => {
                cmd_raft_change(
                    &api,
                    "/api/v1/raft/promote",
                    json!({"hostname": hostname, "shared_secret": secret}),
                )
                .await?
            }
            RaftCmd::Remove { hostname, secret } => {
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
            no_restart,
            yes,
        } => updater::run(target, check, tag, repo_url, install_dir, config, no_restart, yes)?,
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

async fn cmd_volumes_create(
    api: &ApiClient,
    format: &OutputFormat,
    name: &str,
    replication_factor: Option<u32>,
    tier_policy: Option<&str>,
) -> anyhow::Result<()> {
    let mut body = serde_json::json!({ "name": name });
    if let Some(rf) = replication_factor {
        body["replication_factor"] = serde_json::json!(rf);
    }
    if let Some(t) = tier_policy {
        body["tier_policy"] = serde_json::json!(t);
    }
    let resp = api.post("/api/v1/volumes", &body).await?;
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
    let resp = api.post("/api/v1/files/flush_cache", &body).await?;
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

async fn cmd_files(api: &ApiClient, format: &OutputFormat, _limit: usize) -> anyhow::Result<()> {
    let resp = api.get("/api/v1/files").await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&resp)?),
        OutputFormat::Table => {
            let files = resp["files"].as_array().cloned().unwrap_or_default();
            println!("Total files: {}", resp["count"].as_u64().unwrap_or(0));
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
        }
    }
    Ok(())
}

async fn cmd_scrub(
    api: &ApiClient,
    disk: Option<String>,
    file: Option<String>,
    chunk: Option<String>,
    level: &str,
) -> anyhow::Result<()> {
    let (target_type, target) = if let Some(d) = disk {
        ("disk", d)
    } else if let Some(f) = file {
        ("file", f)
    } else if let Some(c) = chunk {
        ("chunk", c)
    } else {
        ("all", "*".to_string())
    };

    let body = serde_json::json!({
        "target_type": target_type,
        "target": target,
        "level": level,
    });

    let resp = api.post("/api/v1/scrub", &body).await?;
    println!(
        "Scrub triggered: {} {} (level: {})",
        target_type,
        target,
        level
    );
    println!("Status: {}", resp["status"].as_str().unwrap_or("—"));
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
        .post(&format!("/api/v1/nodes/{}/maintenance", node_id), &body)
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

async fn cmd_audit(
    api: &ApiClient,
    format: &OutputFormat,
    limit: usize,
    action: Option<String>,
) -> anyhow::Result<()> {
    let mut path = format!("/api/v1/audit?limit={}", limit);
    if let Some(ref a) = action {
        path.push_str(&format!("&action={}", a));
    }

    let entries = api.get(&path).await?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&entries)?),
        OutputFormat::Table => {
            let entries = entries.as_array().cloned().unwrap_or_default();
            if entries.is_empty() {
                println!("No audit entries found.");
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
                println!(
                    "  Migration BW Limit:   {} Mbps",
                    tiering["migration_bandwidth_limit_mbps"]
                        .as_u64()
                        .unwrap_or(0)
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
}
