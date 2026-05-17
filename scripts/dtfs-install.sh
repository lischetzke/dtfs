#!/usr/bin/env bash
# DTFS interactive installer with a whiptail/dialog TUI.
#
# Reads a build tarball (produced by dtfs-build.sh), prompts the operator
# for cluster topology + secrets via a TUI, renders config files, installs
# binaries and systemd units, and starts the service.
#
# Usage:
#   sudo ./scripts/dtfs-install.sh /path/to/dtfs-<rev>-linux-amd64.tar.gz
#   sudo ./scripts/dtfs-install.sh --uninstall
#
# Roles:
#   manager     — Raft + metadata + REST/gRPC API + WebUI
#   data        — chunk-store node, registers with managers
#   fuse-client — mounts /mnt/dtfs against a manager
#
# Idempotent: re-running on the same host upgrades binaries + configs without
# losing data. To wipe state pass --purge.

set -euo pipefail

# ─── tty/colour ──────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; BOLD='\033[1m'; OFF='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; BOLD=''; OFF=''
fi
info()  { echo -e "${GREEN}[INFO]${OFF}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${OFF}  $*"; }
error() { echo -e "${RED}[ERROR]${OFF} $*"; }
step()  { echo -e "\n${BOLD}>>> $*${OFF}"; }

# ─── early help handling (before root check) ────────────────────────────────
for a in "$@"; do
    if [ "$a" = "--help" ] || [ "$a" = "-h" ]; then
        sed -n '2,21p' "$0"
        exit 0
    fi
done

# ─── prereqs ─────────────────────────────────────────────────────────────────
if [ "$(id -u)" -ne 0 ]; then
    error "must run as root (sudo)"; exit 1
fi
if ! command -v whiptail >/dev/null 2>&1; then
    info "Installing whiptail (TUI library)..."
    apt-get install -y whiptail >/dev/null 2>&1 || {
        error "failed to install whiptail; install manually or use --noninteractive";
        exit 1
    }
fi

PURGE=0
NONINT=0
TARBALL=""
ROLE_ARG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --uninstall)
            step "Uninstall mode"
            systemctl disable --now dtfs-manager.service 2>/dev/null || true
            systemctl disable --now dtfs-data.service 2>/dev/null || true
            for u in $(systemctl list-units --type=service --no-legend 'dtfs-fuse@*' 2>/dev/null | awk '{print $1}'); do
                systemctl disable --now "$u" || true
            done
            rm -f /usr/local/bin/dtfs-{manager,data,fuse,cli}
            rm -f /etc/systemd/system/dtfs-*.service
            systemctl daemon-reload
            info "Uninstalled (data dirs preserved unless --purge)"
            [ "${2:-}" = "--purge" ] && {
                rm -rf /var/lib/dtfs /etc/dtfs
                info "Purged /var/lib/dtfs and /etc/dtfs"
            }
            exit 0
            ;;
        --noninteractive) NONINT=1 ;;
        --purge) PURGE=1 ;;
        --role)
            shift; ROLE_ARG="${1:-}"
            ;;
        --help|-h)
            sed -n '2,18p' "$0"
            exit 0
            ;;
        *)
            if [ -z "$TARBALL" ]; then TARBALL="$1"; else
                error "unknown arg: $1"; exit 1
            fi
            ;;
    esac
    shift
done

if [ -z "$TARBALL" ] || [ ! -f "$TARBALL" ]; then
    error "missing tarball; usage: $0 <tarball>"; exit 1
fi

# ─── TUI: gather role + cluster info ────────────────────────────────────────
if [ -n "$ROLE_ARG" ]; then
    ROLE="$ROLE_ARG"
else
    ROLE=$(whiptail --title "DTFS install" --menu "Select role for this host:" 16 70 6 \
        manager     "Cluster manager (Raft + metadata + API)" \
        data        "Data node (chunk store)" \
        fuse-client "FUSE client (POSIX mount)" \
        3>&1 1>&2 2>&3) || { error "cancelled"; exit 1; }
fi

HOSTNAME_FQ=$(hostname -f 2>/dev/null || hostname)
HOSTNAME_FQ=$(whiptail --inputbox "Hostname for this node:" 10 70 "$HOSTNAME_FQ" \
    --title "Node identity" 3>&1 1>&2 2>&3) || exit 1

SHARED_SECRET=$(whiptail --inputbox "Cluster shared secret (32+ hex chars):" \
    10 70 "$(openssl rand -hex 32 2>/dev/null || echo changeme)" \
    --title "Auth" 3>&1 1>&2 2>&3) || exit 1

MGR_LIST=$(whiptail --inputbox "Comma-separated manager addresses (host:9401):" \
    10 70 "" --title "Cluster managers" 3>&1 1>&2 2>&3) || exit 1
[ -z "$MGR_LIST" ] && { error "manager list required"; exit 1; }

CLUSTER_NAME=$(whiptail --inputbox "Cluster name:" 10 70 "dtfs-cluster" \
    --title "Cluster identity" 3>&1 1>&2 2>&3) || exit 1

case "$ROLE" in
    manager)
        BOOTSTRAP=$(whiptail --yesno "Is this the bootstrap manager? (first node)" \
            10 70 --title "Bootstrap?" 3>&1 1>&2 2>&3 && echo true || echo false)
        ;;
    data)
        DISKS=$(whiptail --inputbox "Disk paths (comma-separated, hot first):" \
            10 70 "/mnt/disk-a,/mnt/disk-b" \
            --title "Storage" 3>&1 1>&2 2>&3) || exit 1
        ;;
    fuse-client)
        MOUNTPOINT=$(whiptail --inputbox "Mount point:" \
            10 70 "/mnt/dtfs" \
            --title "Mount" 3>&1 1>&2 2>&3) || exit 1
        ;;
esac

# ─── unpack + install binaries ────────────────────────────────────────────────
step "Unpacking tarball"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
tar -C "$TMP" -xzf "$TARBALL"
[ -d "$TMP/dtfs" ] || { error "tarball missing dtfs/ root"; exit 1; }

step "Installing binaries to /usr/local/bin"
install -m 0755 "$TMP/dtfs/bin/dtfs-manager" /usr/local/bin/dtfs-manager
install -m 0755 "$TMP/dtfs/bin/dtfs-data"    /usr/local/bin/dtfs-data
install -m 0755 "$TMP/dtfs/bin/dtfs-fuse"    /usr/local/bin/dtfs-fuse
install -m 0755 "$TMP/dtfs/bin/dtfs-cli"     /usr/local/bin/dtfs-cli

step "Creating user/group"
getent group  dtfs >/dev/null || groupadd --system --gid 980 dtfs
getent passwd dtfs >/dev/null || useradd  --system --uid 980 --gid dtfs \
    --home-dir /var/lib/dtfs --shell /usr/sbin/nologin dtfs

step "Setting up directories"
install -d -o dtfs -g dtfs -m 0750 /etc/dtfs /var/lib/dtfs /var/log/dtfs

step "Installing systemd units"
install -m 0644 "$TMP/dtfs/systemd/"*.service /etc/systemd/system/ 2>/dev/null || true

# ─── render config + start service ───────────────────────────────────────────
case "$ROLE" in
    manager)
        IFS=',' read -ra MGRS <<<"$MGR_LIST"
        # initial_voters list as TOML inline tables
        VOTERS=""
        for m in "${MGRS[@]}"; do
            host="${m%:*}"
            VOTERS+="  { hostname = \"$host\", addr = \"$m\" },
"
        done
        cat > /etc/dtfs/manager.toml <<EOF
[node]
id = "auto"
hostname = "$HOSTNAME_FQ"
data_dir = "/var/lib/dtfs/manager"

[location]
region = "lab"
datacenter = "dc1"
rack = "default"
host = "$HOSTNAME_FQ"

[cluster]
name = "$CLUSTER_NAME"
shared_secret = "$SHARED_SECRET"
bootstrap = ${BOOTSTRAP:-false}
initial_voters = [
${VOTERS}]

[raft]
listen_addr = "0.0.0.0:9400"
heartbeat_interval_ms = 50
election_timeout_ms = 300
snapshot_threshold = 200

[grpc]
listen_addr = "0.0.0.0:9401"

[api]
listen_addr = "0.0.0.0:9402"

[tls]
mode = "off"
EOF
        chown dtfs:dtfs /etc/dtfs/manager.toml
        chmod 0640 /etc/dtfs/manager.toml
        systemctl daemon-reload
        systemctl enable --now dtfs-manager.service
        info "Manager installed and running. Check: dtfs-cli status"
        ;;
    data)
        IFS=',' read -ra MGRS <<<"$MGR_LIST"
        IFS=',' read -ra DKS <<<"$DISKS"
        cat > /etc/dtfs/data.toml <<EOF
[node]
id = "auto"
hostname = "$HOSTNAME_FQ"
data_dir = "/var/lib/dtfs/data"

[location]
region = "lab"
datacenter = "dc1"
rack = "default"
host = "$HOSTNAME_FQ"

[cluster]
manager_addr = "${MGRS[0]}"
manager_addrs = [$(printf '"%s",' "${MGRS[@]}" | sed 's/,$//')]
shared_secret = "$SHARED_SECRET"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "off"

EOF
        for i in "${!DKS[@]}"; do
            tier="hot"
            [ "$i" -ge 1 ] && tier="cold"
            cat >> /etc/dtfs/data.toml <<EOF
[[disks]]
path = "${DKS[$i]}"
tier = "$tier"

EOF
        done
        cat >> /etc/dtfs/data.toml <<EOF
[cache]
max_memory_mb = 2048
policy = "arc"

[scrub]
quick_interval_hours = 24
deep_interval_hours = 168
max_io_percent = 10
EOF
        chown dtfs:dtfs /etc/dtfs/data.toml
        chmod 0640 /etc/dtfs/data.toml
        systemctl daemon-reload
        systemctl enable --now dtfs-data.service
        info "Data node installed and running."
        ;;
    fuse-client)
        IFS=',' read -ra MGRS <<<"$MGR_LIST"
        mkdir -p "$MOUNTPOINT"
        cat > /etc/dtfs/fuse.toml <<EOF
[client]
manager_addr = "${MGRS[0]}"
manager_addrs = [$(printf '"%s",' "${MGRS[@]}" | sed 's/,$//')]

[location]
region = "lab"
datacenter = "dc1"
rack = "default"
host = "$HOSTNAME_FQ"

[mount]
mountpoint = "$MOUNTPOINT"
allow_other = true

[readahead]
mode = "folder-sequential"
prefetch_chunks = 4
prefetch_next_files = 2

[cache]
enabled = true
max_size_mb = 512
ttl_secs = 60

[partition_safety]
mode = "localreadonly"
quorum_loss_timeout_secs = 30
show_warning = true
EOF
        chmod 0644 /etc/dtfs/fuse.toml
        # Enable /etc/fuse.conf user_allow_other for allow_other.
        if [ -f /etc/fuse.conf ] && ! grep -q '^user_allow_other' /etc/fuse.conf; then
            echo "user_allow_other" >> /etc/fuse.conf
        fi
        ESC=$(systemd-escape -p "$MOUNTPOINT")
        systemctl daemon-reload
        systemctl enable --now "dtfs-fuse@${ESC}.service"
        info "FUSE client mounted at $MOUNTPOINT"
        ;;
esac

step "Installation complete"
info "Role:    $ROLE"
info "Config:  /etc/dtfs/"
info "Logs:    journalctl -u dtfs-${ROLE/fuse-client/fuse@*}"
