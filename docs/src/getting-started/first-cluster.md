# First Cluster

This guide walks through a small multi-node cluster by hand: 3 managers, 4 data
nodes in 2 racks, 2 FUSE clients. It is the worked example behind the
step-by-step spec in [Deployment automation](../deployment/overview.md); when
you automate, follow that section, which also gives the exact checks after each
step. Binaries are installed as described in [Installation](./installation.md)
(release tarball, systemd units in place).

## Topology

```text
Region us-east, datacenter dc-1

  Rack A                          Rack B
  mgr-01 (manager, bootstrap)     mgr-02 (manager)
  mgr-03 (manager)
  data-01 (SSD hot+warm)          data-03 (SSD hot+warm)
  data-02 (HDD cold)              data-04 (HDD cold)
  client-01                       client-02
```

| Hostname | Role | Rack | Disks (position: path, tier) |
|----------|------|------|------------------------------|
| mgr-01, mgr-03 | Manager | rack-a | none |
| mgr-02 | Manager | rack-b | none |
| data-01 | Data | rack-a | 0: /mnt/ssd-a hot, 1: /mnt/ssd-b warm |
| data-02 | Data | rack-a | 0: /mnt/hdd-a cold |
| data-03 | Data | rack-b | 0: /mnt/ssd-c hot, 1: /mnt/ssd-d warm |
| data-04 | Data | rack-b | 0: /mnt/hdd-b cold |
| client-01, client-02 | FUSE | rack-a, rack-b | none |

Every daemon needs a unique hostname (`node.hostname`; for a client
`location.host`): the node id is derived from it.

## Step 1: Disks and secret

On each data node format, mount (by UUID in `/etc/fstab`) and give the disks to
the service user. The daemon creates its own directories and identity files on
the first start; do not pre-create anything inside a disk.

```bash
sudo mkfs.xfs /dev/sdb && sudo mkdir -p /mnt/ssd-a
echo 'UUID=<uuid> /mnt/ssd-a xfs noatime,inode64 0 0' | sudo tee -a /etc/fstab
sudo mount /mnt/ssd-a && sudo chown dtfs:dtfs /mnt/ssd-a
```

Each disk's position in `[[disks]]` becomes its permanent id; keep the order
(append only, see [Decommission and Replace](../deployment/decommission-replace.md)).

Generate one cluster secret and use it in every config file of every role:

```bash
openssl rand -hex 32
```

## Step 2: Configure managers

`/etc/dtfs/manager.toml` on `mgr-01` (the bootstrap manager). Start from
`deploy/examples/manager.toml`; the keys that matter here:

```toml
[node]
hostname = "mgr-01"
data_dir = "/var/lib/dtfs/manager"   # must match the unit's writable path

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "mgr-01"

[cluster]
name = "production"
shared_secret = "YOUR_SHARED_SECRET_HERE"
bootstrap = true                     # exactly one manager, first start only

[[cluster.initial_voters]]           # the voters seeded at bootstrap
hostname = "mgr-01"
addr = "10.0.0.1:9401"               # routable, never 0.0.0.0
[[cluster.initial_voters]]
hostname = "mgr-02"
addr = "10.0.0.2:9401"
[[cluster.initial_voters]]
hostname = "mgr-03"
addr = "10.0.0.3:9401"

[raft]
[grpc]
listen_addr = "0.0.0.0:9401"
[api]
[tls]
mode = "disabled"                    # see Security > TLS before enabling

[defaults]
anchor_datacenter = "dc-1"

[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30

[defaults.replication]
hot = 3
warm = 2
cold = 1

[tiering]
[monitoring]
```

`mgr-02` and `mgr-03` use the same file with their own `hostname`, `rack` and
`host` and `bootstrap = false`. Unlisted keys take their defaults (the chunk size
defaults to 4 MB; if you change `defaults.chunk_size_mb`, set the same
`chunk_size_mb` on every client).

## Step 3: Configure data nodes and clients

`data-01` (`/etc/dtfs/data.toml`), the other data nodes differ in hostname, rack,
`listen_addr` and `[[disks]]`:

```toml
[node]
hostname = "data-01"
data_dir = "/var/lib/dtfs/data"

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "data-01"

[cluster]
manager_addr = "10.0.0.1:9401"
manager_addrs = ["10.0.0.2:9401", "10.0.0.3:9401"]
shared_secret = "YOUR_SHARED_SECRET_HERE"

[grpc]
listen_addr = "10.0.1.1:9500"        # the routable address others dial

[tls]
mode = "disabled"

[[disks]]
path = "/mnt/ssd-a"
tier = "hot"

[[disks]]
path = "/mnt/ssd-b"
tier = "warm"

[cache]
[scrub]
```

`client-01` (`/etc/dtfs/fuse.toml`, mode 0640 root:root):

```toml
[client]
manager_addr = "10.0.0.1:9401"
manager_addrs = ["10.0.0.2:9401", "10.0.0.3:9401"]   # must include a voter
shared_secret = "YOUR_SHARED_SECRET_HERE"

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "client-01"

[mount]
mountpoint = "/mnt/dtfs"
allow_other = true

[readahead]
[cache]
[partition_safety]
```

The `location` block drives nearest-replica routing (a client in `rack-a` prefers
`data-01` and `data-02`); see [Data Distribution](../architecture/data-distribution.md).

## Step 4: Start in order

Use the systemd units from the tarball (`systemctl enable --now`); after each
step wait for the gate in [Fresh Install Order](../deployment/fresh-install.md).

1. Managers: start `mgr-01`, then `mgr-02` and `mgr-03`. Gate: each manager's
   `curl http://<mgr>:9402/api/v1/raft/state` shows `running_state: ok`, the same
   `current_leader`, and three `voters`.
2. Data nodes: `systemctl enable --now dtfs-data` on each. Gate:
   `curl http://mgr-01:9402/api/v1/nodes` lists every node `Healthy` with the
   expected disk count.
3. Clients: `systemctl enable --now "dtfs-fuse@$(systemd-escape --path /mnt/dtfs).service"`.
   Gate: `mountpoint -q /mnt/dtfs`.

A manager added later (not in `initial_voters` at bootstrap) joins as a learner
through `dtfs-cli raft add-learner` and `raft promote`, see
[Cluster Management](../operations/cluster-management.md#adding-a-manager-node).

## Step 5: Verify

```bash
dtfs-cli --manager mgr-01:9402 status
dd if=/dev/urandom of=/mnt/dtfs/test-replication.bin bs=1M count=256   # on client-01
curl -s "http://mgr-01:9402/api/v1/files/inspect?path=/test-replication.bin" | jq .
```

The 256 MB file is stored as 64 chunks of 4 MB; the inspect output lists each
chunk's replicas as `(node_id, disk_id, tier)`. A write is acknowledged only
after `min_replicas_before_ack` distinct replicas are durable. The WebUI at
`http://mgr-01:9402` shows nodes, disks and volumes. Delete the test file when
done; its chunks are reclaimed by the orphan reaper after the grace period, not
immediately.

## Next steps

- [Architecture Overview](../architecture/overview.md)
- [Tiering](../storage/tiering.md)
- [Monitoring](../operations/monitoring.md)
- [TLS](../security/tls.md)
