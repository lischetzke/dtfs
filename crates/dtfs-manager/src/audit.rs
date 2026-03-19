//! Audit logging — Phase 3 (Clustering).
//!
//! Records security-relevant and administrative events such as node joins,
//! configuration changes, and access control decisions for compliance and
//! debugging purposes.
//!
//! Uses a ring buffer with configurable maximum size. When the buffer is full
//! the oldest entries are dropped.

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

/// In-memory ring-buffer audit log.
pub struct AuditLog {
    entries: parking_lot::RwLock<Vec<AuditEntry>>,
    max_entries: usize,
}

impl AuditLog {
    /// Create a new audit log with the given maximum capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: parking_lot::RwLock::new(Vec::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
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

        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            // Remove the oldest entry to maintain the ring-buffer behaviour.
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
