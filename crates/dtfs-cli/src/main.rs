//! DTFS Administration CLI.
//!
//! Connects to a manager node's REST API to perform cluster operations.

use clap::{Parser, Subcommand};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(
    name = "dtfs-cli",
    about = "DTFS Administration CLI",
    version,
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

    /// List files in the cluster
    Files {
        /// Maximum number of files to list
        #[arg(long, default_value = "50")]
        limit: usize,
    },

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
        },
        Commands::Files { limit } => cmd_files(&api, &args.format, limit).await?,
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
        format!("{}...", &s[..max.saturating_sub(3)])
    }
}
