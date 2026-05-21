#!/usr/bin/env bash
# Deploy DTFS minicluster — pushes binaries + configs + systemd units to
# the 6 VMs provisioned by provision-mini.sh, then starts the services in
# the correct order (managers first, then data nodes, then FUSE client).
#
# Run from the builder VM (10.150.0.150) — it has the compiled binaries
# at /home/debian/dtfs2/target/release/ and the dtfs-lab SSH key.

set -euo pipefail

REPO=/home/debian/dtfs2
BIN=$REPO/target/release
KEY=$HOME/.ssh/dtfs-lab/id_ed25519
SSHOPTS="-i $KEY -o StrictHostKeyChecking=no -o BatchMode=yes"
SHARED_SECRET="minicluster-test-secret"

declare -A IP=(
    [mgr01]=10.150.0.171 [mgr02]=10.150.0.172 [mgr03]=10.150.0.173
    [data01]=10.150.0.174 [data02]=10.150.0.175 [client01]=10.150.0.176
)
declare -A DC=(   [mgr01]=dc-a [mgr02]=dc-a [mgr03]=dc-b [data01]=dc-a [data02]=dc-a [client01]=dc-a )
declare -A RACK=( [mgr01]=rack-1 [mgr02]=rack-2 [mgr03]=rack-1 [data01]=rack-1 [data02]=rack-2 [client01]=rack-3 )

# --- Helpers ---------------------------------------------------------------

remote() { local host=$1; shift; ssh $SSHOPTS "debian@${IP[$host]}" "$@"; }
push()   { local host=$1 local=$2 remote=$3; scp $SSHOPTS "$local" "debian@${IP[$host]}:$remote"; }

# Build a TOML snippet for the [[cluster.initial_voters]] block — the same
# list goes into every manager.toml so the bootstrap leader and the joining
# voters share an identical view.
voters_toml=""
for m in mgr01 mgr02 mgr03; do
    voters_toml+=$'\n[[cluster.initial_voters]]\n'
    voters_toml+="hostname = \"mini-$m\"\n"
    voters_toml+="addr = \"${IP[$m]}:9401\"\n"
    voters_toml+="region = \"lab\"\n"
    voters_toml+="datacenter = \"${DC[$m]}\"\n"
    voters_toml+="rack = \"${RACK[$m]}\"\n"
done

manager_addrs_toml='manager_addrs = ['
for m in mgr01 mgr02 mgr03; do
    manager_addrs_toml+=$"\n  \"${IP[$m]}:9401\","
done
manager_addrs_toml+=$"\n]"

# --- Step 1: install dtfs user, dirs, binaries, systemd units on every VM --

setup_common() {
    local host=$1
    echo "=== $host: dtfs user + dirs + binaries ==="
    remote $host "sudo getent passwd dtfs >/dev/null 2>&1 || sudo useradd --system --shell /usr/sbin/nologin --home /var/lib/dtfs dtfs
sudo mkdir -p /var/lib/dtfs/manager /var/lib/dtfs/data /etc/dtfs
sudo chown -R dtfs:dtfs /var/lib/dtfs"
}

push_binary() {
    local host=$1 bin=$2
    push $host "$BIN/$bin" "/tmp/$bin"
    remote $host "sudo install -m 755 /tmp/$bin /usr/local/bin/$bin"
}

push_systemd_unit() {
    local host=$1 unit=$2
    push $host "$REPO/deploy/systemd/$unit" "/tmp/$unit"
    remote $host "sudo install -m 644 /tmp/$unit /etc/systemd/system/$unit"
}

# --- Per-role config rendering --------------------------------------------

render_manager_toml() {
    local host=$1 is_bootstrap=$2
    cat <<TOML
# DTFS minicluster manager config — $host
[node]
id = "auto"
hostname = "mini-$host"
data_dir = "/var/lib/dtfs/manager"

[location]
region = "lab"
datacenter = "${DC[$host]}"
rack = "${RACK[$host]}"
host = "mini-$host"

[cluster]
name = "Minicluster"
shared_secret = "$SHARED_SECRET"
bootstrap = $is_bootstrap
$(printf "%b" "$voters_toml")

[raft]
listen_addr = "0.0.0.0:9400"

[grpc]
listen_addr = "0.0.0.0:9401"

[api]
listen_addr = "0.0.0.0:9402"

[tls]
mode = "off"

[defaults]
chunk_size_mb = 4
anchor_datacenter = "dc-a"

[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30

[defaults.replication]
hot = 0
warm = 2
cold = 1

[tiering]
enabled = true
hot_threshold_reads_per_hour = 5
cold_threshold_days = 14
frozen_threshold_days = 90
evaluation_interval_secs = 3600
migration_bandwidth_limit_mbps = 200

[monitoring]
heartbeat_interval_secs = 5
suspect_after_missed = 3
down_after_missed = 6

[alerts]

# Bug AH — fan audit events out through Raft so /api/v1/audit returns the
# same view from every manager.
[audit]
replicate_via_raft = true
TOML
}

render_data_toml() {
    local host=$1
    cat <<TOML
# DTFS minicluster data config — $host
[node]
id = "auto"
hostname = "mini-$host"
data_dir = "/var/lib/dtfs/data"

[location]
region = "lab"
datacenter = "${DC[$host]}"
rack = "${RACK[$host]}"
host = "mini-$host"

[cluster]
manager_addr = "${IP[mgr01]}:9401"
$(printf "%b" "$manager_addrs_toml")
shared_secret = "$SHARED_SECRET"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "off"

[[disks]]
path = "/mnt/${host}-warm"
tier = "warm"

[[disks]]
path = "/mnt/${host}-cold"
tier = "cold"

[cache]
max_memory_mb = 256
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
TOML
}

render_fuse_toml() {
    local host=$1
    cat <<TOML
# DTFS minicluster fuse config — $host
[client]
manager_addr = "${IP[mgr01]}:9401"
$(printf "%b" "$manager_addrs_toml")
shared_secret = "$SHARED_SECRET"

[location]
region = "lab"
datacenter = "${DC[$host]}"
rack = "${RACK[$host]}"
host = "mini-$host"

[mount]
mountpoint = "/mnt/dtfs"
allow_other = true

[readahead]
mode = "file-sequential"
prefetch_chunks = 4
prefetch_next_files = 4

[cache]
local_cache_max_gb = 0
prune_interval_secs = 3600

[partition_safety]
mode = "localreadonly"
quorum_loss_timeout_secs = 30
show_warning = true
TOML
}

# --- Step 2: deploy managers ----------------------------------------------

deploy_manager() {
    local host=$1 is_bootstrap=$2
    setup_common $host
    push_binary $host dtfs-manager
    push_systemd_unit $host dtfs-manager.service
    render_manager_toml $host $is_bootstrap > /tmp/manager-$host.toml
    push $host /tmp/manager-$host.toml /tmp/manager.toml
    remote $host "sudo install -m 644 /tmp/manager.toml /etc/dtfs/manager.toml && sudo chown dtfs:dtfs /etc/dtfs/manager.toml"
    remote $host "sudo systemctl daemon-reload && sudo systemctl enable --now dtfs-manager"
}

# --- Step 3: deploy data nodes --------------------------------------------

deploy_data() {
    local host=$1
    setup_common $host
    # Ensure dtfs user can write to the mounted disks.
    remote $host "sudo chown -R dtfs:dtfs /mnt/${host}-warm /mnt/${host}-cold"
    push_binary $host dtfs-data
    push_systemd_unit $host dtfs-data.service
    # Need ReadWritePaths for the mounts; drop a unit-override file.
    remote $host "sudo mkdir -p /etc/systemd/system/dtfs-data.service.d
echo -e '[Service]\\nReadWritePaths=/mnt/${host}-warm /mnt/${host}-cold' | sudo tee /etc/systemd/system/dtfs-data.service.d/disks.conf"
    render_data_toml $host > /tmp/data-$host.toml
    push $host /tmp/data-$host.toml /tmp/data.toml
    remote $host "sudo install -m 644 /tmp/data.toml /etc/dtfs/data.toml && sudo chown dtfs:dtfs /etc/dtfs/data.toml"
    remote $host "sudo systemctl daemon-reload && sudo systemctl enable --now dtfs-data"
}

# --- Step 4: deploy FUSE client -------------------------------------------

deploy_client() {
    local host=$1
    echo "=== $host: FUSE client ==="
    # fuse3 + user setup
    remote $host "sudo apt-get install -y fuse3 >/dev/null 2>&1 || true
sudo mkdir -p /mnt/dtfs /etc/dtfs
# Allow non-root to mount FUSE with allow_other.
echo 'user_allow_other' | sudo tee /etc/fuse.conf >/dev/null"
    push_binary $host dtfs-fuse
    push_systemd_unit $host 'dtfs-fuse@.service'
    render_fuse_toml $host > /tmp/fuse-$host.toml
    push $host /tmp/fuse-$host.toml /tmp/fuse.toml
    remote $host "sudo install -m 644 /tmp/fuse.toml /etc/dtfs/fuse.toml"
    # systemd-escape the mountpoint: /mnt/dtfs → -mnt-dtfs
    remote $host "sudo systemctl daemon-reload && sudo systemctl enable --now 'dtfs-fuse@-mnt-dtfs.service'"
}

# --- Orchestration --------------------------------------------------------

echo "=== Phase 1: bootstrap manager (mgr01) ==="
deploy_manager mgr01 true

echo
echo "=== Phase 2: wait 8s for bootstrap leader election ==="
sleep 8

echo
echo "=== Phase 3: join voters mgr02, mgr03 ==="
deploy_manager mgr02 false
deploy_manager mgr03 false
sleep 5

echo
echo "=== Phase 4: data nodes data01, data02 ==="
deploy_data data01
deploy_data data02
sleep 4

echo
echo "=== Phase 5: FUSE client01 ==="
deploy_client client01

echo
echo "=== Done. Verifying cluster status... ==="
sleep 5
for m in mgr01 mgr02 mgr03; do
    echo -n "  $m: "
    curl -fsS --max-time 4 "http://${IP[$m]}:9402/api/v1/raft/state" 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print('state='+str(d.get('state')), 'leader='+str(d.get('current_leader')))" \
        || echo "(unreachable)"
done
echo
echo "Cluster stats:"
curl -fsS --max-time 4 "http://${IP[mgr01]}:9402/api/v1/stats" 2>/dev/null \
    | python3 -c "
import json,sys
d=json.load(sys.stdin)
for k in ('manager_count','data_node_count','client_count','disk_count','file_count','health'):
    print(f'  {k}: {d.get(k)}')"
