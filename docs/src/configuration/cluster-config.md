# Cluster-wide Configuration Override

The manager REST API can store a **cluster configuration override**: a sparse JSON document kept in the Raft state machine with a version counter. It is a staging and inspection tool, not a runtime control plane.

## What it does and does not do

- `POST /api/v1/config` validates the override, stores it in Raft (version +1) and optionally rewrites `manager.toml` on the manager that handled the request.
- `GET /api/v1/config` and `GET /api/v1/config/effective` return the startup config with the stored override merged on top.
- **No daemon reads the override at runtime.** The scheduler, tiering engine, health monitor and volume defaults use the values loaded from `manager.toml` at startup. A stored override changes behaviour only if it was written to disk (`write_to_disk:true`) on each manager and the manager was restarted.
- Settings for existing volumes are not part of this document; use the volume endpoints ([Volume Configuration](./volume-config.md)).

For the keys themselves see [Manager Configuration](./manager.md); for which keys may be set here see [Scope](./scope.md).

## Writing an override

`POST /api/v1/config`:

```json
{
  "config_json": "{\"tiering\":{\"evaluation_interval_secs\":1800}}",
  "write_to_disk": false,
  "acknowledge_cluster_wide": false
}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `config_json` | string | required | JSON object, sparse. It replaces the previous stored override wholesale. |
| `write_to_disk` | bool | `false` | Also write the merged, validated config to `/etc/dtfs/manager.toml` (temp file, fsync, atomic rename, mode 0600) on this manager only. |
| `acknowledge_cluster_wide` | bool | `false` | Required when `write_to_disk` is `true` (otherwise `400`). |

Checks, all before anything is stored or written (failures return `400` with `{"error": ..., "preserved_current": true}`):

1. Valid JSON.
2. No node-scoped key (see [Scope](./scope.md)).
3. The override deep-merged onto this manager's startup config must deserialize as a complete `ManagerConfig`. Out-of-type values fail here.
4. For `write_to_disk`, the rendered TOML must parse back through the daemon's own loader.

The write is a compare-and-set on the version counter. A concurrent update returns `409`: re-read `GET /api/v1/config` and retry. Secrets (`shared_secret`, `secret`, `token`, `password`, and webhook URLs) read back as `"***"`; posting `"***"` back keeps the stored value.

The merge does not run the startup range checks (`[memory]`, `[placement]`, intervals, `[tls]` mode). A value that parses but would fail those checks is accepted here and then stops the manager from starting after `write_to_disk`. Review the value against [Manager Configuration](./manager.md) first.

## Reading

`GET /api/v1/config/effective` lists one row per leaf key: `name`, `scope`, `scope_kind`, `current`, `default`, `is_default`. The `default` column is a hand-written baseline in the API handler and does not always equal the compiled-in default documented on the configuration pages (for example it reports `defaults.chunk_size_mb` as 128, the real default is 4). Trust the configuration pages for defaults.

## Relationship to the TOML files

`manager.toml` is the source of truth on each manager. The override never reaches data nodes or clients except through the manager `[scrub]` policy heartbeat described in [Live Reload](./live-reload.md).
