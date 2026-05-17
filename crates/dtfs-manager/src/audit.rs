//! Audit logging — Phase 3 (Clustering).
//!
//! Records security-relevant and administrative events such as node joins,
//! configuration changes, and access control decisions for compliance and
//! debugging purposes.
//!
//! Uses a ring buffer with configurable maximum size for fast in-memory queries;
//! when an archive directory is configured (P0-8), each entry is also appended
//! to a daily-rotated JSONL file so historical events survive ring-buffer
//! eviction and operator queries beyond the in-memory window.

use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: i64,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub result: String,
}

/// Optional filter for querying audit entries.
#[derive(Debug, Default)]
pub struct AuditFilter {
    /// Only return entries at or after this timestamp.
    pub since: Option<i64>,
    /// Only return entries at or before this timestamp.
    pub until: Option<i64>,
    /// Only return entries matching this action prefix.
    pub action: Option<String>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
}

/// In-memory ring-buffer audit log with optional disk archive.
pub struct AuditLog {
    entries: parking_lot::RwLock<Vec<AuditEntry>>,
    max_entries: usize,
    /// If set, every `log()` call appends the entry to
    /// `<archive_dir>/audit-YYYYMMDD.jsonl` so events survive ring-buffer
    /// eviction. Errors during archive writes are logged but do not fail the
    /// audit path (P0-8).
    archive_dir: Option<PathBuf>,
}

impl AuditLog {
    /// Create a new audit log with the given maximum capacity (in-memory only).
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: parking_lot::RwLock::new(Vec::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
            archive_dir: None,
        }
    }

    /// Create a new audit log that also archives every entry to a daily-rotated
    /// JSONL file under `archive_dir`. The directory is created on first use.
    pub fn new_with_archive(max_entries: usize, archive_dir: PathBuf) -> Self {
        Self {
            entries: parking_lot::RwLock::new(Vec::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
            archive_dir: Some(archive_dir),
        }
    }

    /// Record an audit event.
    pub fn log(&self, actor: &str, action: &str, target: &str, result: &str) {
        let entry = AuditEntry {
            timestamp: chrono::Utc::now().timestamp(),
            actor: actor.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            result: result.to_string(),
        };

        // Append to disk archive first (best-effort; errors don't fail audit).
        if let Some(dir) = &self.archive_dir {
            if let Err(e) = append_to_archive(dir, &entry) {
                tracing::error!(error = %e, "audit archive write failed");
            }
        }

        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            entries.remove(0);
        }
        entries.push(entry);
    }

    /// Query audit entries using an optional filter.
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let mut result: Vec<AuditEntry> = entries
            .iter()
            .filter(|e| {
                if let Some(since) = filter.since {
                    if e.timestamp < since {
                        return false;
                    }
                }
                if let Some(until) = filter.until {
                    if e.timestamp > until {
                        return false;
                    }
                }
                if let Some(ref action) = filter.action {
                    if !e.action.starts_with(action.as_str()) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        if let Some(limit) = filter.limit {
            result.truncate(limit);
        }

        result
    }

    /// Return the total number of entries currently stored.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(10_000)
    }
}

/// Append a single audit entry to the daily-rotated JSONL archive under `dir`.
/// File name pattern: `audit-YYYYMMDD.jsonl` (UTC date). Errors propagate up to
/// the caller, which logs them but does not abort the audit path.
fn append_to_archive(dir: &std::path::Path, entry: &AuditEntry) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let date = chrono::DateTime::from_timestamp(entry.timestamp, 0)
        .map(|dt| dt.format("%Y%m%d").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let path = dir.join(format!("audit-{date}.jsonl"));

    let line = serde_json::to_string(entry)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Query archived entries from disk, scanning daily files in `archive_dir`.
/// Returns up to `filter.limit` matching entries, oldest first.
pub fn query_archive(
    archive_dir: &std::path::Path,
    filter: &AuditFilter,
) -> std::io::Result<Vec<AuditEntry>> {
    use std::io::BufRead;
    if !archive_dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("audit-") && n.ends_with(".jsonl"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();

    let mut results: Vec<AuditEntry> = Vec::new();
    let limit = filter.limit.unwrap_or(usize::MAX);
    'outer: for path in files {
        let f = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(f);
        for line in reader.lines() {
            let line = match line {
                Ok(l) if !l.is_empty() => l,
                _ => continue,
            };
            let entry: AuditEntry = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Some(since) = filter.since {
                if entry.timestamp < since {
                    continue;
                }
            }
            if let Some(until) = filter.until {
                if entry.timestamp > until {
                    continue;
                }
            }
            if let Some(action) = &filter.action {
                if !entry.action.starts_with(action.as_str()) {
                    continue;
                }
            }
            results.push(entry);
            if results.len() >= limit {
                break 'outer;
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_archive_appends_and_rotates_by_date() {
        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().to_path_buf();
        let log = AuditLog::new_with_archive(5, archive_path.clone());
        log.log("test", "action.a", "tgt1", "ok");
        log.log("test", "action.b", "tgt2", "ok");

        let files: Vec<_> = std::fs::read_dir(&archive_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert_eq!(files.len(), 1, "expected one daily archive file");

        let queried = query_archive(
            &archive_path,
            &AuditFilter {
                action: Some("action.".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(queried.len(), 2);
        assert_eq!(queried[0].action, "action.a");
        assert_eq!(queried[1].action, "action.b");
    }

    #[test]
    fn test_archive_survives_ring_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::new_with_archive(2, tmp.path().to_path_buf());
        log.log("t", "a", "t", "r");
        log.log("t", "b", "t", "r");
        log.log("t", "c", "t", "r"); // evicts "a" from in-memory
        log.log("t", "d", "t", "r"); // evicts "b" from in-memory

        assert_eq!(log.len(), 2);
        let archived = query_archive(tmp.path(), &AuditFilter::default()).unwrap();
        assert_eq!(archived.len(), 4); // all four survive on disk
    }
}
