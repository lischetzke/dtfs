//! Drift guard for deploy/grafana: every dashboard parses, every `dtfs_<name>`
//! measurement a query reads exists as the string literal `"<name>"` in some
//! crate source, and the InfluxQL (v1) and Flux (v2) sets stay identical.
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn collect_strings(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => out.push(s.clone()),
        Value::Array(a) => a.iter().for_each(|x| collect_strings(x, out)),
        Value::Object(o) => o.values().for_each(|x| collect_strings(x, out)),
        _ => {}
    }
}

/// Measurement names read by one query string: `FROM "dtfs_x"` / `FROM dtfs_x`
/// (InfluxQL) and `_measurement == "dtfs_x"` (Flux). Regex scopes such as
/// `r._measurement =~ /^dtfs_/` are not measurement references.
fn measurements_in(q: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (i, _) in q.match_indices("dtfs_") {
        let name: String = q[i + 5..].chars().take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_').collect();
        let before = q[..i].trim_end_matches('"').trim_end();
        let influxql = before.to_ascii_lowercase().ends_with("from");
        let flux = before.strip_suffix("==").is_some_and(|b| b.trim_end().ends_with("_measurement"));
        if !name.is_empty() && (influxql || flux) {
            out.push(name);
        }
    }
    out
}

/// Measurement names (without the `dtfs_` prefix) in a dashboard's queries.
fn referenced(dashboard: &str) -> Result<BTreeSet<String>, String> {
    let v: Value = serde_json::from_str(dashboard).map_err(|e| e.to_string())?;
    let mut strings = Vec::new();
    collect_strings(&v, &mut strings);
    Ok(strings.iter().flat_map(|s| measurements_in(s)).collect())
}

/// Referenced names missing from `known`.
fn unknown_measurements(dashboard: &str, known: &BTreeSet<String>) -> Result<Vec<String>, String> {
    Ok(referenced(dashboard)?.into_iter().filter(|n| !known.contains(n)).collect())
}

/// Every `"ident"` string literal (lowercase, digits, underscore) in `src`.
fn quoted_idents(src: &str, into: &mut BTreeSet<String>) {
    for (i, _) in src.match_indices('"') {
        let rest = &src[i + 1..];
        let end = rest.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')).unwrap_or(rest.len());
        if end > 0 && rest[end..].starts_with('"') {
            into.insert(rest[..end].to_string());
        }
    }
}

fn walk(dir: &Path, f: &mut dyn FnMut(&Path)) {
    for e in fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, f);
        } else {
            f(&p);
        }
    }
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn known_names() -> BTreeSet<String> {
    let mut known = BTreeSet::new();
    for c in fs::read_dir(repo().join("crates")).unwrap().flatten() {
        let src = c.path().join("src");
        if src.is_dir() {
            walk(&src, &mut |p| {
                if p.extension().is_some_and(|e| e == "rs") {
                    quoted_idents(&fs::read_to_string(p).unwrap(), &mut known);
                }
            });
        }
    }
    known
}

fn dashboards(flavor: &str) -> Vec<(String, String)> {
    let dir = repo().join("deploy/grafana").join(flavor);
    let mut v: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .map(|e| (e.file_name().to_string_lossy().into_owned(), fs::read_to_string(e.path()).unwrap()))
        .collect();
    v.sort();
    v
}

#[test]
fn dashboards_parse_and_reference_known_metrics() {
    let known = known_names();
    for flavor in ["influxdb-v1", "influxdb-v2"] {
        let ds = dashboards(flavor);
        assert!(ds.len() >= 5, "{flavor}: expected the five dashboards, got {}", ds.len());
        for (name, text) in ds {
            let refs = referenced(&text).unwrap_or_else(|e| panic!("{flavor}/{name}: invalid JSON: {e}"));
            assert!(!refs.is_empty(), "{flavor}/{name}: no dtfs_ measurement found in any query");
            let unknown = unknown_measurements(&text, &known).unwrap();
            assert!(unknown.is_empty(), "{flavor}/{name}: unknown measurements {unknown:?}");
        }
    }
}

#[test]
fn v1_and_v2_sets_are_identical() {
    let (v1, v2) = (dashboards("influxdb-v1"), dashboards("influxdb-v2"));
    let names = |d: &[(String, String)]| d.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    assert_eq!(names(&v1), names(&v2), "dashboard file sets differ");
    for ((n, a), (_, b)) in v1.iter().zip(v2.iter()) {
        assert_eq!(referenced(a).unwrap(), referenced(b).unwrap(), "{n}: measurement sets differ");
        let (ja, jb): (Value, Value) = (serde_json::from_str(a).unwrap(), serde_json::from_str(b).unwrap());
        assert_eq!(ja["panels"].as_array().map(Vec::len), jb["panels"].as_array().map(Vec::len), "{n}: panel counts differ");
        let vars = |j: &Value| -> Vec<String> {
            j["templating"]["list"].as_array().unwrap().iter().map(|v| v["name"].as_str().unwrap().to_string()).filter(|v| v != "bucket").collect()
        };
        assert_eq!(vars(&ja), vars(&jb), "{n}: template variables differ");
    }
}

fn dash(query: &str) -> String {
    serde_json::json!({"panels": [{"targets": [{"query": query}]}]}).to_string()
}

#[test]
fn unknown_influxql_measurement_is_reported() {
    let known: BTreeSet<String> = ["fuse_op_us".to_string()].into();
    let ok = dash("SELECT mean(\"p99\") FROM \"dtfs_fuse_op_us\" WHERE $timeFilter");
    assert!(unknown_measurements(&ok, &known).unwrap().is_empty());
    let bad = dash("SELECT mean(\"value\") FROM \"dtfs_no_such_metric\" WHERE $timeFilter");
    assert_eq!(unknown_measurements(&bad, &known).unwrap(), vec!["no_such_metric"]);
    let bare = dash("SELECT mean(\"value\") FROM dtfs_bare_missing WHERE $timeFilter");
    assert_eq!(unknown_measurements(&bare, &known).unwrap(), vec!["bare_missing"]);
}

#[test]
fn unknown_flux_measurement_is_reported() {
    let known: BTreeSet<String> = ["fuse_op_us".to_string()].into();
    let ok = dash("from(bucket: \"b\") |> filter(fn: (r) => r._measurement == \"dtfs_fuse_op_us\" and r._field == \"p99\")");
    assert!(unknown_measurements(&ok, &known).unwrap().is_empty());
    let bad = dash("from(bucket: \"b\") |> filter(fn: (r) => r._measurement == \"dtfs_no_such_metric\")");
    assert_eq!(unknown_measurements(&bad, &known).unwrap(), vec!["no_such_metric"]);
    // A regex scope in a variable query is not a measurement reference.
    let scope = dash("schema.tagValues(bucket: \"b\", tag: \"dc\", predicate: (r) => r._measurement =~ /^dtfs_/)");
    assert!(unknown_measurements(&scope, &known).unwrap().is_empty());
}

#[test]
fn invalid_json_is_an_error() {
    assert!(unknown_measurements("{not json", &BTreeSet::new()).is_err());
}
