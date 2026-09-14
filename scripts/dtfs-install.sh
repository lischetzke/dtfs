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
        cat <<'AZ2HELP'

AZ-2 source selection flags (in addition to the legacy positional arg):
  --source local|remote|auto   default `auto`. `local` searches
                               $DTFS_LOCAL_BIN_DIR, ./target/release,
                               /opt/dtfs/staging/*/release, ~/.cache/dtfs;
                               `remote` downloads the release tarball from
                               $DTFS_REPO_URL (GitLab package registry, or
                               GitHub releases for a github.com URL;
                               `github` is accepted as an alias); `auto`
                               tries local first, then remote.
  --version <tag>              Release tag (default `latest` = newest full
                               vX.Y.Z release). Honoured for source=remote
                               and the auto fallback. Ignored for local.

Env vars:
  DTFS_LOCAL_BIN_DIR  highest-priority local search dir (optional).
  DTFS_REPO_URL       override the release repo. Default:
                      https://github.com/lischetzke/dtfs
  DTFS_REPO_TOKEN     GitLab deploy token (gldt-..., read_package_registry)
                      or access token (glpat-..., read_api) for a private
                      project; empty = anonymous.
AZ2HELP
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
# AZ-2 — source selection.
#   auto   : check local search paths first, fall back to github (default)
#   local  : only local search paths; fail if none found
#   github : skip local, fetch from GitHub release
SOURCE="auto"
VERSION=""        # tag / "latest". empty = "latest" when SOURCE != local
REPO="${DTFS_REPO_URL:-https://github.com/lischetzke/dtfs}"
REPO_TOKEN="${DTFS_REPO_TOKEN:-}"
STAGING_ROOT="/opt/dtfs/staging"
DOWNLOAD_CACHE="${HOME}/.cache/dtfs"

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
        --source)
            shift
            case "${1:-}" in
                local|auto) SOURCE="$1" ;;
                remote|github) SOURCE="remote" ;;
                *) error "--source must be local|remote|auto"; exit 1 ;;
            esac
            ;;
        --version)
            shift; VERSION="${1:-}"
            ;;
        --help|-h)
            sed -n '2,18p' "$0"
            cat <<'AZ2HELP'

AZ-2 source selection flags (in addition to the legacy positional arg):
  --source local|remote|auto   default `auto`. `local` searches
                               $DTFS_LOCAL_BIN_DIR, ./target/release,
                               /opt/dtfs/staging/*/release, ~/.cache/dtfs;
                               `remote` downloads the release tarball from
                               $DTFS_REPO_URL (GitLab package registry, or
                               GitHub releases for a github.com URL;
                               `github` is accepted as an alias); `auto`
                               tries local first, then remote.
  --version <tag>              Release tag (default `latest` = newest full
                               vX.Y.Z release). Honoured for source=remote
                               and the auto fallback. Ignored for local.

The legacy positional tarball arg still wins when supplied — the install
script unpacks it directly, no source-selection logic runs.

Env vars:
  DTFS_LOCAL_BIN_DIR  highest-priority local search dir (optional).
  DTFS_REPO_URL       override the release repo. Default:
                      https://github.com/lischetzke/dtfs
  DTFS_REPO_TOKEN     GitLab deploy token (gldt-..., read_package_registry)
                      or access token (glpat-..., read_api) for a private
                      project; empty = anonymous.
AZ2HELP
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

# AZ-2 — when no positional tarball was supplied, run the source-selection
# pipeline (local-first / github fallback per --source). Sets TARBALL to a
# concrete path on disk. Skipped entirely when the operator passed an
# explicit tarball (legacy workflow stays unchanged).
if [ -z "$TARBALL" ]; then
    select_tarball_local() {
        local candidates=(
            "${DTFS_LOCAL_BIN_DIR:-}/dtfs-*-linux-amd64.tar.gz"
            "./target/release/dtfs-*-linux-amd64.tar.gz"
            "/opt/dtfs/staging/"*"/dtfs-*-linux-amd64.tar.gz"
            "${DOWNLOAD_CACHE}/dtfs-*-linux-amd64.tar.gz"
        )
        for pattern in "${candidates[@]}"; do
            [ -z "$pattern" ] && continue
            for f in $pattern; do
                if [ -f "$f" ]; then
                    info "AZ-2: local source matched $f"
                    TARBALL="$f"
                    return 0
                fi
            done
        done
        return 1
    }
    # Header for a GitLab token: deploy tokens (gldt-...) use DEPLOY-TOKEN,
    # project/personal access tokens use PRIVATE-TOKEN (mirrors
    # dtfs-cli/src/updater.rs::token_header).
    repo_auth_args() {
        [ -n "$REPO_TOKEN" ] || return 0
        case "$REPO_TOKEN" in
            gldt-*) printf -- '-H\nDEPLOY-TOKEN: %s\n' "$REPO_TOKEN" ;;
            *)      printf -- '-H\nPRIVATE-TOKEN: %s\n' "$REPO_TOKEN" ;;
        esac
    }
    # GitLab API base for a project URL: https://host/group/project ->
    # https://host/api/v4/projects/group%2Fproject
    gitlab_api_base() {
        local u="${REPO%/}"; u="${u%.git}"
        local scheme="${u%%://*}" rest="${u#*://}"
        local host="${rest%%/*}" path="${rest#*/}"
        [ -n "$host" ] && [ -n "$path" ] && [ "$path" != "$rest" ] || return 1
        printf '%s://%s/api/v4/projects/%s' "$scheme" "$host" "${path//\//%2F}"
    }
    download_tarball_remote() {
        local tag="${VERSION:-latest}"
        local url
        local -a auth=()
        mapfile -t auth < <(repo_auth_args)
        case "$REPO" in
            https://github.com/*|http://github.com/*)
                if [ "$tag" = "latest" ]; then
                    url="${REPO}/releases/latest/download/dtfs-linux-amd64.tar.gz"
                else
                    url="${REPO}/releases/download/${tag}/dtfs-linux-amd64.tar.gz"
                fi
                ;;
            *)
                local api
                api="$(gitlab_api_base)" || { error "AZ-2: cannot derive a GitLab project from $REPO"; return 1; }
                if [ "$tag" = "latest" ]; then
                    # Newest full release (vX.Y.Z); alpha tags (vX.Y.Z_stamp) are skipped.
                    command -v jq >/dev/null 2>&1 || { error "AZ-2: jq is required to resolve 'latest' from GitLab"; return 1; }
                    tag="$(curl -fsSL "${auth[@]}" "${api}/releases?per_page=50" \
                        | jq -r '[.[] | .tag_name | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))] | first // empty')"
                    [ -n "$tag" ] || { error "AZ-2: no full release found at ${api}/releases"; return 1; }
                    info "AZ-2: latest full release is $tag"
                fi
                url="${api}/packages/generic/dtfs/${tag}/dtfs-linux-amd64.tar.gz"
                ;;
        esac
        mkdir -p "$DOWNLOAD_CACHE"
        local out="${DOWNLOAD_CACHE}/dtfs-${tag}-linux-amd64.tar.gz"
        info "AZ-2: downloading $url -> $out"
        if curl -fL "${auth[@]}" --output "$out.partial" "$url"; then
            mv "$out.partial" "$out"
            TARBALL="$out"
            return 0
        else
            rm -f "$out.partial"
            return 1
        fi
    }
    case "$SOURCE" in
        local)
            select_tarball_local || {
                error "AZ-2: --source=local but no tarball found in local search paths"
                exit 1
            }
            ;;
        remote)
            download_tarball_remote || {
                error "AZ-2: remote download failed for tag '${VERSION:-latest}'"
                exit 1
            }
            ;;
        auto)
            if select_tarball_local; then
                :
            elif download_tarball_remote; then
                :
            else
                error "AZ-2: no local source found and GitHub fallback failed"
                exit 1
            fi
            ;;
    esac
fi

if [ -z "$TARBALL" ] || [ ! -f "$TARBALL" ]; then
    error "missing tarball; usage: $0 [<tarball> | --source local|github|auto]"; exit 1
fi

# AZ-2 — stage the tarball into /opt/dtfs/staging/<version>/ before
# unpacking. Atomic install via `install -m 755` (done by individual
# binary copies below); the staging dir survives for forensics if
# the install fails partway through.
TARBALL_BASENAME="$(basename "$TARBALL")"
VERSION_TAG="$(echo "$TARBALL_BASENAME" | sed -E 's/^dtfs-([^-]+).*/\1/' || echo unknown)"
STAGING_DIR="${STAGING_ROOT}/${VERSION_TAG}"
mkdir -p "$STAGING_DIR"
cp -f "$TARBALL" "${STAGING_DIR}/${TARBALL_BASENAME}"
info "AZ-2: staged at ${STAGING_DIR}/${TARBALL_BASENAME}"

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
