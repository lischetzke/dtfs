# PLAN-UIP-2026-09 — webui / dtfs-cli parity with the manager REST API

Pure REST-client work. `crates/dtfs-manager/src/api.rs` already implements
every route used below; nothing in this plan changes a route, a request/
response struct, `proto/*`, `ClientRequest`, or `apply_request`. The two
clients (`crates/dtfs-manager/src/webui.rs` embedded SPA and
`crates/dtfs-cli/src/main.rs`) each expose a different subset of that API;
this plan closes both gaps so an operator can do everything from either
client.

**Branch decision: master, direct per-task commits.** AGENTS.md § B.1's
branch threshold (wire/on-disk format change, or a `#[serde]`-persisted
struct, or `apply_request`, or a Raft variant, or >6 files in one change,
or multi-session risk to a *deployable* master) does not fire: no task
touches a wire/on-disk format, and every task is independently additive —
a new CLI subcommand or a new webui button does not put `master` in a
half-working state the way a half-landed Raft variant would. The plan
spans many small tasks/sessions, but each commits a complete, working
unit. No branch, no PR (internal tooling change, no release-note-worthy
`bug:`/`feat:` subject required by itself, though individual commits may
still use `feat:` subjects per house convention).

## Goals

- Every mutating/read REST route in `api.rs` that has an operator use case
  is reachable from **both** the webui and `dtfs-cli`.
- No new `fetch()`/reqwest call bypasses the existing LIS-18 shared-secret
  plumbing (`mutateFetch` in webui.rs, `ApiClient::with_secret` /
  `apply_mutation_auth` in dtfs-cli).
- `docs/src/api-reference/cli-reference.md` and `AGENTS.md` § A.8 reflect
  the new command surface.

## Non-goals

- No `api.rs` route, request/response struct, or REST auth change.
- No `proto/*`, Raft `ClientRequest`, or `apply_request` change.
- No new `/files/browse` endpoint (explicit prior scope decision — the
  data explorer's client-side grouping stays as-is).
- No CLI `files tree` command — `dtfs-cli files inspect <path>` only.
- No rewrite of `docs/src/api-reference/rest-api.md`. That file already
  documents `POST /api/v1/config`, `GET /api/v1/config`, `GET
  /api/v1/stats`, `GET/POST /api/v1/raft/*`, `POST
  /api/v1/volumes/{id}/migrate-policy`, and `POST /api/v1/scrub`; it is
  missing sections for `POST /api/v1/volumes` (create),
  `set-write-safety`, `set-anchor`, `set-spill`, `set-schedules`,
  `set-cache`, `POST /nodes/{id}/{decommission,unquarantine}`, `POST
  /api/v1/migrations`, `GET /config/effective`, `GET /integrity/faults`,
  and `GET /files/inspect` — all pre-existing gaps in a file this plan
  does not touch. `cli-reference.md`'s new sections link to an existing
  `rest-api.md` anchor where one exists (config, stats, raft, migrate-
  policy) and document the request/response shape inline where it does
  not, exactly like the existing `set-cache`/`set-write-safety` sections
  already do.
- No change to `dtfs-cli volumes create` (already implemented, already
  covers the CLI side of volume creation — only the webui lacks it).

## Locked-in decisions

**D1 — CLI node-lifecycle commands are new top-level flat commands, not a
`nodes <verb>` nest.** `Commands::Nodes { id: Option<String> }` stays
exactly as-is (`dtfs-cli nodes`, `dtfs-cli nodes --id X` both keep
working unmodified). `unquarantine <node_id>` and `decommission
<node_id>` land as new top-level `Commands` variants, following the
existing flat-verb precedent set by `Maintenance { node_id, --enable,
--disable }` — not the noun-verb nest used by `volumes`/`files`/`raft`.
Rejected: converting `Nodes` into a `NodesCmd` subcommand enum
(`nodes list`/`nodes unquarantine`/`nodes decommission`) — clap subcommand
enums require an explicit subcommand token, so that conversion breaks the
documented bare `dtfs-cli nodes` / `dtfs-cli nodes --id X` invocations for
no benefit; `Maintenance` already proves the flat-verb shape is the house
convention for single-node lifecycle actions.

**D2 — `dtfs-cli config` gets an optional nested subcommand, not a second
top-level command.** Convert `Commands::Config { show: bool }` into
`Commands::Config { show: bool, #[command(subcommand)] action:
Option<ConfigAction> }` with `ConfigAction::Set { .. }` and
`ConfigAction::Effective`. Bare `dtfs-cli config` (action = `None`) keeps
today's GET-and-render behaviour byte-for-byte; `dtfs-cli config set …`
and `dtfs-cli config effective` are new. This matches the exact command
name webui.rs:3172 already hardcodes ("Change via `dtfs-cli config set`
or POST /api/v1/config") — confirmed still true at UIP-5-1's done-when
rather than assumed.

**D3 — `migrations` and `integrity` are new noun-verb subcommand groups**
(`Commands::Migrations(MigrationsCmd)`, `Commands::Integrity
(IntegrityCmd)`), matching the `volumes`/`files`/`raft` convention — no
back-compat constraint since neither noun exists today.

**D4 — `files inspect` is a new leaf on the existing `FilesCmd` enum**
(alongside `List`/`FlushCache`), not a new top-level noun.

**D5 — Webui "Scrub all" is one button + a level `<select>`** on the
Integrity page, POSTing `{ target: "*", target_type: "all", level }` —
per-target scrub stays a 400 at the API layer (unchanged); the webui
never offers a per-target control.

**D6 — `GET /api/v1/health` is wired as a plain (non-mutating) `fetch`**
in the webui, parallel to the existing `fetch('/api/v1/stats')` at
webui.rs:1102 — not through `mutateFetch` (GET, not a mutation), and
guarded by the same style of test as webui.rs:3493-3497 (`GET .../health`
must stay a plain fetch).

## Data-model reference (no changes — verified shapes for client code)

All confirmed against `crates/dtfs-manager/src/api.rs` (dtfs-explore
pass, see Deviations for token accounting). No struct listed here is
edited by this plan.

| Route | Request struct (fields) | Response (key fields) |
|---|---|---|
| `POST /api/v1/volumes` | `CreateVolumeRequest { name: String, chunk_size_mb: Option<u32>, replication_hot/warm/cold: Option<u32>, anchor_datacenter: Option<String> }` (api.rs:1778) | `{ volume_id, name, status }` |
| `POST /volumes/{id}/migrate-policy` | none (path only) | `{ volume_id, status, config }` |
| `POST /volumes/{id}/set-write-safety` | `SetWriteSafetyRequest { min_replicas_before_ack: Option<u32>, write_timeout_secs: Option<u32> }` (api.rs:2145) | `{ volume_id, changed, config }` |
| `POST /volumes/{id}/set-anchor` | `SetAnchorRequest { datacenter: Option<String>, required_tier: Option<String> }` (api.rs:2507) | `{ volume_id, changed, config }` |
| `POST /volumes/{id}/set-spill` | `SetSpillRequest { allow: Option<bool>, forbidden_tiers: Option<Vec<String>>, forbidden_datacenters: Option<Vec<String>> }` (api.rs:2631) | `{ volume_id, changed, config }` |
| `POST /volumes/{id}/set-schedules` | `SetSchedulesRequest { scrub_interval_secs: Option<u64>, fsck_interval_secs: Option<u64>, custom: Option<BTreeMap<String,u64>>, clear_unspecified: bool }` (api.rs:2390); intervals must be `>= 60` or `0` (api.rs:2424) | `{ volume_id, changed, config }` |
| `POST /api/v1/scrub` | `ScrubRequest { target: Option<String>, target_type: Option<String>, level: Option<String> }` (api.rs:4649); non-`"all"` `target_type` → 400 (api.rs:4674) | `{ status, target_type, target, level, id }` |
| `GET /api/v1/raft/state` | — | `{ node_id, hostname, running_state, state, is_leader, current_term, current_leader, voters[], learners[], membership[], millis_since_quorum_ack, … }` (api.rs:328-360) |
| `POST /raft/{add-learner,promote,remove}` | `MembershipChangeRequest { hostname: String, addr: Option<String> (required for add-learner), shared_secret: String }` (api.rs:368); leader-gated via `check_is_leader()` (api.rs:402), 409 + leader hint otherwise | `{ status, node_id, hostname, log_index }` (+`addr` for add-learner) |
| `GET /api/v1/health` | — | `{ status: "healthy" }` |
| `POST /nodes/{id}/decommission` | `DecommissionRequest { operator_id: String, reason: String }` (api.rs:1521); 409 if the node still holds chunk replicas (api.rs:1560) | `{ node_id, ok }` |
| `POST /nodes/{id}/unquarantine` | `UnquarantineRequest { operator_id: String, reason_acknowledged: String }` (api.rs:1443); 409 if the node is not Quarantined (api.rs:1482) | `{ node_id, ok }` |
| `POST /api/v1/migrations` | `StartMigrationRequest { path_prefix: String, target_tier: String ("hot"\|"warm"\|"cold"\|"tape"), target_dc: Option<String>, allow_empty_prefix: bool, max_tasks: Option<usize> }` (api.rs:4736) | chunk counts + migration task ids |
| `POST /api/v1/config` | `ConfigUpdateRequest { config_json: String, write_to_disk: bool, acknowledge_cluster_wide: bool }` (api.rs:3017) | merged config |
| `GET /config/effective` | — | `{ scope, node_hostname, entries: [{ name, current, default, is_default }, …] }` |
| `GET /integrity/faults` | `AuditQuery { limit: Option<usize>, action: Option<String> }` (api.rs:2792, same shape as `/api/v1/audit`) | array of `{ timestamp, actor, action, target, result, summary, origin_manager }` |
| `GET /files/inspect` | `InspectFileQuery { path: String }` (api.rs:1586) | `{ path, volume_id, total_size, chunk_size, chunks: [{ chunk_index, chunk_id, size, node_ids[], replicas: [{node_id, disk_id, tier}] }], chunk_count }` |
| `GET /api/v1/stats` | — | already wired in webui (webui.rs:1102); CLI-only gap |

REST mutation auth: `crates/dtfs-manager/src/rest_auth.rs::require_mutation_auth` (line 119), applied via `.route_layer()` (api.rs:202); checks `x-dtfs-shared-secret` or `Authorization: Bearer`; exempt paths are `/api/v1/join/*`, `/healthz`, `/readyz`, `/metrics`, and all GET/HEAD/OPTIONS.

Client-side auth plumbing already in place, reused verbatim by every task below:
- webui: `mutateFetch(url, options)` (webui.rs:1060-1073) — sets method, adds `X-Dtfs-Shared-Secret` via `authHeaders()`. A regression test at webui.rs:3487-3490 asserts `!html.contains("method: 'POST'")` — every new mutating call MUST go through `mutateFetch`, never an inline `fetch(url, {method:'POST', ...})`.
- dtfs-cli: `ApiClient { base_url, client, shared_secret }` (main.rs:319-324), `ApiClient::with_secret(&self, secret: &str) -> Self` (main.rs:351-357) clones with an overridden secret (the `raft` subcommand's pattern at main.rs:486-491: `let api = api.with_secret(&secret); ...`); `apply_mutation_auth` (main.rs:341-346) is invoked inside `api.post()`/`api.post_with_status()` and stamps the `X-Dtfs-Shared-Secret` header automatically for the CLI's own global `--secret`/`DTFS_SHARED_SECRET`. Every new mutating CLI command uses `api.post(...)`/`api.post_with_status(...)` directly (global secret already attached) — `with_secret` is only needed when a command accepts its own `--secret` override the way `raft` does; none of the new commands in this plan need a per-command secret override, so they use the ambient `api` handle unchanged.

None of the above are `#[serde]`-persisted structs or Raft `ClientRequest` variants — the AGENTS.md § A.3.1 `#[serde(default)]` + regression-test requirement does not apply to any task in this plan.

## Tasks

### Group 1 — webui volume operations

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-1-1 | `crates/dtfs-manager/src/webui.rs` | Replace the "No volumes yet… `dtfs-cli volume create`" placeholder (webui.rs:2196) with a create-volume modal (name, chunk_size_mb, replication_hot/warm/cold, anchor_datacenter fields) that POSTs `CreateVolumeRequest` to `/api/v1/volumes` via `mutateFetch`, then re-renders the volumes table. | `#[cfg(test)]` `html.contains(...)` on `render_dashboard(...)` output: asserts the new modal markup exists and the placeholder string is gone; anti-inline-POST test (webui.rs:3488) still passes. | Volumes page shows a working "Create volume" button/modal; no volumes-page text references `dtfs-cli` for creation. | ✅ |
| UIP-1-2 | `crates/dtfs-manager/src/webui.rs` | Add a "Write safety" section to the volume-edit modal (webui.rs:704-830, `openVolumeEdit`/`submitVolumeEdit` at 2264-2406): two fields (`min_replicas_before_ack`, `write_timeout_secs`), prefilled from `v.write_safety`, posted via the same `post(suffix, body)` helper (webui.rs:2372) to `set-write-safety` only when the operator touched a field. | `html.contains` test asserting the new field ids (`ve-write-safety-*` or similar) exist in the modal markup. | Editing a volume can change `min_replicas_before_ack`/`write_timeout_secs` from the webui without the CLI. | ✅ |
| UIP-1-3 | `crates/dtfs-manager/src/webui.rs` | Add a "Migrate policy" action button next to (or inside) the volume-edit modal that POSTs (no body) to `/volumes/{id}/migrate-policy` via `mutateFetch` and shows the returned `status`/`config` summary — this is a one-shot action, not a modal field, since the endpoint takes no request body. | `html.contains` test asserting the button markup and its `mutateFetch('/api/v1/volumes/.../migrate-policy'` call target exist. | An operator can run Bug AV's legacy→mandatory migration from the webui. | ✅ |

### Group 2 — webui scrub, health, raft, decommission

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-2-1 | `crates/dtfs-manager/src/webui.rs` | Replace the scrub placeholder (webui.rs:1399-1400, card header at 1754) with a "Scrub all" button + a `quick`/`deep` `<select>` inside the existing `.scrub-card`; POSTs `{target:"*", target_type:"all", level}` via `mutateFetch` to `/api/v1/scrub`. | `html.contains` test: button + select markup present, placeholder string gone. | Integrity page can trigger a cluster-wide scrub without the CLI. | ✅ |
| UIP-2-2 | `crates/dtfs-manager/src/webui.rs` | Add a plain (non-mutating) `fetch('/api/v1/health')` alongside the existing `fetch('/api/v1/stats')` (webui.rs:1102) and surface its `status` as a small manager-reachability indicator (e.g. next to the existing health banner at 611-618). | `html.contains` test mirroring webui.rs:3493-3497's pattern: asserts `fetch('/api/v1/health')` is present and is NOT wrapped in `mutateFetch(`. | A manager-reachability check independent of `/api/v1/stats` is visible on the dashboard. | ✅ |
| UIP-2-3 | `crates/dtfs-manager/src/webui.rs` | New read-only Raft panel: fetch `GET /api/v1/raft/state`, render node_id/state/term/leader plus a voters/learners table (hostname, node_id, addr, role). Plain `fetch`, not `mutateFetch` (read-only). | `html.contains` test asserting the panel's static markup (table headers, element ids) exists. | Raft membership is visible from the webui without `dtfs-cli raft members`. | ⬜ |
| UIP-2-4 | `crates/dtfs-manager/src/webui.rs` | Add add-learner/promote/remove forms to the Raft panel from UIP-2-3, POSTing `MembershipChangeRequest` via `mutateFetch` to `/raft/{add-learner,promote,remove}`; on a 409 (not leader), surface the leader hint from the response body instead of a generic error. | `html.contains` test asserting the three form/button markups and their `mutateFetch('/api/v1/raft/...'` targets exist. | Raft membership can be grown/shrunk from the webui; a non-leader manager shows the leader hint instead of a bare 409. | ⬜ |
| UIP-2-5 | `crates/dtfs-manager/src/webui.rs` | Add a "Decommission" button to the node detail/table (near the existing UNJOINED/LEADER badges, webui.rs:1484-1501/1984-1987) that opens a confirm dialog capturing `operator_id`+`reason`, POSTs `DecommissionRequest` via `mutateFetch` to `/nodes/{id}/decommission`, and surfaces the 409 "replicas remain" message verbatim on failure. | `html.contains` test asserting the button/dialog markup and its `mutateFetch('/api/v1/nodes/.../decommission'` target exist. | A node can be decommissioned from the webui; the replicas-remain 409 message is shown, not swallowed. | ⬜ |

### Group 3 — dtfs-cli volumes: set-anchor / set-spill / set-schedules

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-3-1 | `crates/dtfs-cli/src/main.rs` | Add `VolumesCmd::SetAnchor { volume_id, #[arg(long)] datacenter: Option<String>, #[arg(long)] required_tier: Option<String> }`; handler builds a `serde_json::Map` (pattern at main.rs:795-810) and `api.post(format!("/api/v1/volumes/{}/set-anchor", volume_id), &body)`. | `#[cfg(test)]` `Args::try_parse_from(["dtfs-cli","volumes","set-anchor","default","--datacenter","leli"])` parses into the expected variant/fields (pattern at main.rs:1359-1368). | `dtfs-cli volumes set-anchor <id> [--datacenter D] [--required-tier T]` round-trips against the existing endpoint. | ✅ |
| UIP-3-2 | `crates/dtfs-cli/src/main.rs` | Add `VolumesCmd::SetSpill { volume_id, #[arg(long)] allow: Option<bool>, #[arg(long, value_delimiter=',')] forbidden_tiers: Option<Vec<String>>, #[arg(long, value_delimiter=',')] forbidden_datacenters: Option<Vec<String>> }`; same POST-body pattern as UIP-3-1. | `try_parse_from` test covering `--allow true --forbidden-tiers hot,warm`. | `dtfs-cli volumes set-spill` exercises the endpoint. | ✅ |
| UIP-3-3 | `crates/dtfs-cli/src/main.rs` | Add `VolumesCmd::SetSchedules { volume_id, #[arg(long)] scrub_interval_secs: Option<u64>, #[arg(long)] fsck_interval_secs: Option<u64>, #[arg(long)] clear_unspecified: bool }` (skip `custom` — no other CLI command exposes an arbitrary BTreeMap flag; document as JSON-only via webui/direct API for now). POST to `set-schedules`; surface the server's `>= 60 or 0` validation error verbatim on 400. | `try_parse_from` test covering the interval flags plus a rejected `--scrub-interval-secs 5` (parses fine at the CLI layer; server 400 is exercised only by the existing REST test, not re-tested here). | `dtfs-cli volumes set-schedules` matches the webui's AZ-1 modal capability. | ✅ |

### Group 4 — dtfs-cli nodes lifecycle + migrations

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-4-1 | `crates/dtfs-cli/src/main.rs` | New top-level `Commands::Unquarantine { node_id: String, #[arg(long)] operator_id: String, #[arg(long)] reason_acknowledged: String }` (flat, mirrors `Maintenance`'s shape — see D1). POST `UnquarantineRequest` to `/nodes/{id}/unquarantine`; print the 409 "not quarantined" message verbatim on failure. | `try_parse_from` test: required `--operator-id`/`--reason-acknowledged` present → Ok; either missing → `Err`. | `dtfs-cli unquarantine <node-id> --operator-id X --reason-acknowledged "..."` works; `dtfs-cli nodes`/`nodes --id` unchanged. | ✅ |
| UIP-4-2 | `crates/dtfs-cli/src/main.rs` | New top-level `Commands::Decommission { node_id: String, #[arg(long)] operator_id: String, #[arg(long)] reason: String }`. POST `DecommissionRequest` to `/nodes/{id}/decommission`; print the 409 "replicas remain" message verbatim on failure. | `try_parse_from` test analogous to UIP-4-1. | `dtfs-cli decommission <node-id> --operator-id X --reason "..."` works. | ✅ |
| UIP-4-3 | `crates/dtfs-cli/src/main.rs` | New `Commands::Migrations(MigrationsCmd)` with `MigrationsCmd::Create { path_prefix: String, #[arg(long)] target_tier: String, #[arg(long)] target_dc: Option<String>, #[arg(long)] allow_empty_prefix: bool, #[arg(long)] max_tasks: Option<usize> }` (`target_tier` validated client-side against `hot`\|`warm`\|`cold`\|`tape` before the POST, matching the enum in `StartMigrationRequest`). POST to `/api/v1/migrations`. | `try_parse_from` test for the required/optional flags; a unit test rejecting an invalid `--target-tier` value before any network call. | `dtfs-cli migrations create /movies/ --target-tier cold` starts a migration; an invalid tier name fails fast client-side. | ✅ |

### Group 5 — dtfs-cli config / integrity / files / stats

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-5-1 | `crates/dtfs-cli/src/main.rs` | Per D2: `Commands::Config { show: bool, #[command(subcommand)] action: Option<ConfigAction> }`; `ConfigAction::Set { #[arg(long)] config_json: String, #[arg(long)] write_to_disk: bool, #[arg(long)] acknowledge_cluster_wide: bool }`. `action = None` keeps today's `cmd_config` (GET) behaviour byte-for-byte; `Some(Set{..})` POSTs `ConfigUpdateRequest` to `/api/v1/config`. Confirm webui.rs:3172's "Change via `dtfs-cli config set`" text is still accurate once this lands (fix the text in the same commit if the final flag names diverge). | `try_parse_from` tests: bare `["dtfs-cli","config"]` still parses to `action: None`; `["...","config","set","--config-json","{}","--write-to-disk"]` parses to `Some(Set{..})`. | `dtfs-cli config` unchanged; `dtfs-cli config set --config-json '...' --write-to-disk` applies a config update; webui text confirmed accurate. | ✅ |
| UIP-5-2 | `crates/dtfs-cli/src/main.rs` | `ConfigAction::Effective` → GET `/config/effective`, render `{name, current, default, is_default}` rows as a table (or raw JSON with `--format json`). | `try_parse_from` test for `config effective`. | `dtfs-cli config effective` lists every effective config key with its default/current values. | ✅ |
| UIP-5-3 | `crates/dtfs-cli/src/main.rs` | New `Commands::Integrity(IntegrityCmd)` with `IntegrityCmd::Faults { #[arg(long, default_value="20")] limit: usize, #[arg(long)] action: Option<String> }` (same query shape as the existing `audit` command) → GET `/integrity/faults`. | `try_parse_from` test mirroring the existing `audit` test. | `dtfs-cli integrity faults [--limit N] [--action PREFIX]` lists integrity faults. | ✅ |
| UIP-5-4 | `crates/dtfs-cli/src/main.rs` | Add `FilesCmd::Inspect { path: String }` (alongside `List`/`FlushCache`) → GET `/files/inspect?path=...`, render the chunk/replica table. | `try_parse_from` test: `files inspect /movies/x.mkv` parses. | `dtfs-cli files inspect <path>` shows a file's chunk layout and replica placement. | ✅ |
| UIP-5-5 | `crates/dtfs-cli/src/main.rs` | New flat `Commands::Stats` (no args, mirrors `Health`) → GET `/api/v1/stats`, render the summary fields already shown in `status`/`nodes` plus tier breakdown and scrub results. | `try_parse_from` test: `["dtfs-cli","stats"]` parses. | `dtfs-cli stats` surfaces cluster stats the webui already shows. | ✅ |

### Group 6 — docs

| ID | Files | Sketch | Test | Done when | Status |
|---|---|---|---|---|---|
| UIP-6-1 | `AGENTS.md` | Update the `dtfs-manager/src/webui.rs` § A.8 row to note full REST-mutation coverage (create/migrate-policy/write-safety/scrub-all/raft/decommission all wired through `mutateFetch`); add a new row for `dtfs-cli/src/main.rs` summarising its REST-client command surface and pointing at `docs/src/api-reference/cli-reference.md` for the full list. | None (doc-only; no test framework covers AGENTS.md prose). | § A.8 has a row for `dtfs-cli/src/main.rs`; the `webui.rs` row reflects the post-plan feature set. | ⬜ |
| UIP-6-2 | `docs/src/api-reference/cli-reference.md` | Add a `### <command>` section for every new leaf from Groups 3-5 (set-anchor, set-spill, set-schedules, unquarantine, decommission, migrations create, config set, config effective, integrity faults, files inspect, stats), following the existing section style (usage block, options table, example output, errors table, `See REST API → ...` link where `rest-api.md` already has the anchor — config/POST, config/GET, stats, raft/state, migrate-policy — and inline shape documentation otherwise, per the plan's Non-goals). | None (doc-only). | Every new CLI command in this plan has a matching `cli-reference.md` section. | ⬜ |

## Cross-cutting invariants

- **LIS-18 REST mutation auth (AGENTS.md § A.2).** Every new mutating call
  MUST route through the existing helpers — `mutateFetch` (webui) or
  `ApiClient::post`/`post_with_status` (dtfs-cli, ambient `--secret`
  already attached via `apply_mutation_auth`) — never a raw inline
  `fetch(url, {method:'POST', ...})` or a bare `reqwest` call that skips
  the shared-secret header. Mechanically enforced for webui by the
  existing regression test at webui.rs:3487-3490; no task in Group 1/2
  may make that test fail, and Group 2's two read-only additions (health,
  raft-state) must use plain `fetch`, not `mutateFetch` — mirroring the
  existing `GET /api/v1/stats` test at webui.rs:3493-3497 (a GET wrapped
  in `mutateFetch` is itself a regression).
- **AGENTS.md § A.3.4 (mandatory-vs-cache replicas) — not at risk.** No
  task computes, infers, or short-circuits replica placement client-side;
  every replica-affecting field (`set-anchor`, `set-spill`,
  `set-mandatory`-equivalent inputs) is forwarded verbatim to the
  existing server-side–validated endpoint. CO-1-2's anchor-vs-destination
  check remains entirely in `api.rs`/`scheduler.rs`, untouched here.
- **AGENTS.md § A.6 (data-integrity checklist) — not applicable.** This
  plan implements no write/migrate/delete/scrub *path*; `scrub`,
  `migrations create`, `decommission`, and `unquarantine` only trigger
  the existing, already-audited server-side implementations. No task
  needs a `dtfs-integrity-auditor` pass.
- **AGENTS.md § A.3.1 (`#[serde(default)]` + snapshot regression test) —
  not applicable.** No task adds or edits a `#[serde]`-persisted struct
  or a Raft `ClientRequest` variant.

## Verification log

- UIP-3-1: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  12 passed; after: 13 passed (new `uip_3_1_volumes_set_anchor_parses`).
- UIP-3-2: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  13 passed; after: 14 passed (new `uip_3_2_volumes_set_spill_parses`).
- UIP-3-3: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  14 passed; after: 15 passed (new `uip_3_3_volumes_set_schedules_parses`).
- UIP-1-1: `cargo test -p dtfs-manager --release webui` (Docker buildenv) —
  before: 13 passed; after: 15 passed (new
  `create_volume_modal_tests::create_volume_modal_present_and_wired_through_mutate_fetch`,
  `create_volume_modal_tests::old_cli_creation_placeholder_is_gone`).
- UIP-4-1: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  15 passed; after: 16 passed (new `uip_4_1_unquarantine_parses`).
- UIP-4-2: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  16 passed; after: 17 passed (new `uip_4_2_decommission_parses`).
- UIP-4-3: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  17 passed; after: 19 passed (new `uip_4_3_migrations_create_parses`,
  `uip_4_3_invalid_target_tier_rejected_client_side`).
- UIP-1-2: `cargo test -p dtfs-manager --release webui` (Docker buildenv) —
  before: 15 passed; after: 16 passed (new
  `bug_bk_scrub_tests::volume_modal_edits_write_safety`).
- UIP-1-3: `cargo test -p dtfs-manager --release webui` (Docker buildenv) —
  before: 16 passed; after: 17 passed (new
  `bug_bk_scrub_tests::volume_modal_migrate_policy_button_present`).
- UIP-5-1: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  19 passed; after: 21 passed (new `uip_5_1_bare_config_has_no_action`,
  `uip_5_1_config_set_parses`).
- UIP-5-2: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  21 passed; after: 22 passed (new `uip_5_2_config_effective_parses`).
- UIP-5-3: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  22 passed; after: 23 passed (new `uip_5_3_integrity_faults_parses`).
- UIP-5-4: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  23 passed; after: 25 passed (new `uip_5_4_files_inspect_parses`,
  `uip_5_4_percent_encode_query_escapes_reserved`).
- UIP-5-5: `cargo test -p dtfs-cli --release` (Docker buildenv) — before:
  25 passed; after: 26 passed (new `uip_5_5_stats_parses`).
- UIP-2-1: `cargo test -p dtfs-manager --release webui` (Docker buildenv) —
  before: 17 passed; after: 18 passed (new
  `uip_2_webui_ops_tests::scrub_all_button_present_and_wired_through_mutate_fetch`).
- UIP-2-2: `cargo test -p dtfs-manager --release webui` (Docker buildenv) —
  before: 18 passed; after: 19 passed (new
  `uip_2_webui_ops_tests::manager_health_indicator_uses_plain_fetch`).

## Deviations

- AIWF-5-1 tracking (this plan doubles as a roster dry run — recording
  `subagent_tokens` per stage per the operator's explicit request):
  - Intake stage: 57472 tokens (reported by the operator's intake spec).
  - Planning stage — three `dtfs-explore` dispatches used to verify
    exact api.rs/webui.rs/main.rs shapes before writing this plan:
    api.rs inventory 73302 tokens (27 tool uses), webui.rs inventory
    44084 tokens (30 tool uses), dtfs-cli main.rs inventory 36588 tokens
    (13 tool uses) — 153974 tokens of subagent work total for this
    planning stage, none of which entered the orchestrator's own context
    window directly (only the tightened summaries above did).
- UIP-1-3: the plan's task row describes the response as a `status`/
  `config` summary; the actual `migrate_volume_policy_endpoint` handler
  (api.rs, already implemented under Bug AV/AV-1-10) returns
  `{volume_id, changed, mandatory_replicas}` — matching what
  `dtfs-cli volumes migrate-policy` already parses (main.rs
  `cmd_volumes_migrate_policy`). The webui button surfaces `changed` and
  `mandatory_replicas` instead, to stay consistent with the real wire
  shape and the CLI's own rendering.
