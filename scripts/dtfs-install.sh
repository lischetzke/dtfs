#!/usr/bin/env bash
# DTFS interactive installer with a whiptail/dialog TUI.
#
# Reads a build tarball (produced by dtfs-build.sh), prompts the operator
# for cluster topology + secrets via a TUI, renders config files, installs
# binaries and systemd units, and starts the service.
#
# Usage:
#   sudo ./scripts/dtfs-install.sh /path/to/dtfs-<rev>-linux-amd64.tar.gz
#   sudo ./scripts/dtfs-install.sh --role data --noninteractive --managers m1:9401 ...
#   sudo ./scripts/dtfs-install.sh --uninstall [--purge --yes]
#
# Roles:
#   manager     - Raft + metadata + REST/gRPC API + WebUI
#   data        - chunk-store node, registers with managers
#   fuse-client - mounts /mnt/dtfs against a manager
#
# Idempotent: re-running upgrades binaries and keeps an existing
# /etc/dtfs/<role>.toml (and its secret) untouched; pass --reconfigure to
# re-render it. --uninstall keeps config and data unless --purge --yes.

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
        sed -n '2,/^set -e/p' "$0" | sed '$d'
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
                               vX.Y.Z release). With auto, a given tag skips
                               local candidates (no stale cache); with local,
                               only dtfs-<tag>-linux-amd64.tar.gz matches.

Config flags (also usable interactively as prompt defaults):
  --noninteractive             no prompts; --role and --managers required
  --reconfigure                re-render an existing /etc/dtfs/<role>.toml
  --hostname H  --cluster-name N  --managers h:9401,...  --secret S
  --managers for role manager: hostname=addr:port,... (hostname = that
                               manager's [node].hostname; bare addresses are
                               refused, except a single self entry with
                               --bootstrap true). data/fuse also accept
                               the hostname=addr form and ignore the name.
  --chown-disks                data role: chown the disk paths to dtfs:dtfs
                               (default: only print the chown command)
  --bootstrap true|false (manager)  --disks /a,/b (data)
  --mountpoint P (fuse-client)
  --uninstall [--purge --yes]  remove units/binaries; --purge --yes also
                               deletes /etc/dtfs and /var/lib/dtfs.

Env vars:
  DTFS_SHARED_SECRET  cluster secret (else existing config, else generated).
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
PURGE=0
YES=0
UNINSTALL=0
RECONFIG=0
NONINT=0
HOSTNAME_FQ=""
CLUSTER_NAME=""
MGR_LIST=""
SHARED_SECRET="${DTFS_SHARED_SECRET:-}"
BOOTSTRAP=""
DISKS=""
CHOWN_DISKS=0
MOUNTPOINT=""
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
        --uninstall) UNINSTALL=1 ;;
        --noninteractive) NONINT=1 ;;
        --purge) PURGE=1 ;;
        --yes) YES=1 ;;
        --reconfigure) RECONFIG=1 ;;
        --hostname)     shift; HOSTNAME_FQ="${1:-}" ;;
        --cluster-name) shift; CLUSTER_NAME="${1:-}" ;;
        --managers)     shift; MGR_LIST="${1:-}" ;;
        --secret)       shift; SHARED_SECRET="${1:-}" ;;
        --bootstrap)    shift; BOOTSTRAP="${1:-}" ;;
        --disks)        shift; DISKS="${1:-}" ;;
        --chown-disks)  CHOWN_DISKS=1 ;;
        --mountpoint)   shift; MOUNTPOINT="${1:-}" ;;
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
        *)
            if [ -z "$TARBALL" ]; then TARBALL="$1"; else
                error "unknown arg: $1"; exit 1
            fi
            ;;
    esac
    shift
done

if [ "$PURGE" -eq 1 ] && [ "$UNINSTALL" -ne 1 ]; then
    error "--purge only applies to --uninstall"; exit 1
fi
if [ "$PURGE" -eq 1 ] && [ "$YES" -ne 1 ]; then
    error "--purge deletes /etc/dtfs and /var/lib/dtfs (all chunks); add --yes to confirm"; exit 1
fi
if [ "$UNINSTALL" -eq 1 ]; then
    step "Uninstall mode"
    systemctl disable --now dtfs-manager.service 2>/dev/null || true
    systemctl disable --now dtfs-data.service 2>/dev/null || true
    for u in $(systemctl list-units --plain --no-legend --all 'dtfs-fuse@*' 2>/dev/null | awk '{print $1}'); do
        systemctl disable --now "$u" || true
    done
    rm -f /usr/local/bin/dtfs-{manager,data,fuse,cli}
    rm -f /etc/systemd/system/dtfs-*.service /etc/systemd/system/dtfs-data.service.d/disks.conf
    systemctl daemon-reload
    if [ "$PURGE" -eq 1 ]; then
        rm -rf /var/lib/dtfs /etc/dtfs
        info "Purged /var/lib/dtfs and /etc/dtfs"
    fi
    info "Uninstalled (config and data preserved unless --purge --yes)"
    exit 0
fi

if [ "$NONINT" -eq 0 ] && ! command -v whiptail >/dev/null 2>&1; then
    info "Installing whiptail (TUI library)..."
    apt-get install -y whiptail >/dev/null 2>&1 || {
        error "failed to install whiptail; install manually or use --noninteractive"
        exit 1
    }
fi

# AZ-2 — when no positional tarball was supplied, run the source-selection
# pipeline (local-first / github fallback per --source). Sets TARBALL to a
# concrete path on disk. Skipped entirely when the operator passed an
# explicit tarball (legacy workflow stays unchanged).
if [ -z "$TARBALL" ]; then
    select_tarball_local() {
        local tag="*"
        [ -z "$VERSION" ] || [ "$VERSION" = "latest" ] || tag="$VERSION"
        local candidates=(
            "${DTFS_LOCAL_BIN_DIR:-}/dtfs-${tag}-linux-amd64.tar.gz"
            "./target/release/dtfs-${tag}-linux-amd64.tar.gz"
            "/opt/dtfs/staging/"*"/dtfs-${tag}-linux-amd64.tar.gz"
            "${DOWNLOAD_CACHE}/dtfs-${tag}-linux-amd64.tar.gz"
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
            # An explicit tag must not be satisfied by whatever is cached locally.
            if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
                select_tarball_local || true
            fi
            if [ -n "$TARBALL" ]; then
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
    error "missing tarball; usage: $0 [<tarball> | --source local|remote|auto]"; exit 1
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

# ─── gather role + cluster info (TUI, or flags/env with --noninteractive) ────
# ask VAR TITLE PROMPT DEFAULT: keeps a flag/env preset, else prompts.
ask() {
    local v="${!1:-}"
    if [ "$NONINT" -eq 1 ]; then
        [ -n "$v" ] || v="$4"
    else
        v=$(whiptail --inputbox "$3" 10 70 "${v:-$4}" --title "$2" 3>&1 1>&2 2>&3) || exit 1
    fi
    printf -v "$1" '%s' "$v"
}

# Secret: existing config first (a re-run must not rotate it), else a fresh
# random one. No secure RNG means no install: a guessable secret is worse.
existing_secret() {
    grep -hE '^[[:space:]]*shared_secret[[:space:]]*=' /etc/dtfs/*.toml 2>/dev/null \
        | head -1 | sed -E 's/^[^"]*"([^"]*)".*/\1/' || true
}
gen_secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
    elif [ -r /dev/urandom ]; then
        head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
    else
        return 1
    fi
}

# Split "host=addr" --managers entries into MGR_HOSTS/MGR_ADDRS. Node ids
# derive from [node].hostname, so a voter name guessed from its address would
# not match the hostname that manager actually runs with.
parse_managers() {
    MGR_HOSTS=(); MGR_ADDRS=()
    local -a ents; local e h self=0
    IFS=',' read -ra ents <<<"$MGR_LIST"
    for e in "${ents[@]}"; do
        case "$e" in
            *=*) MGR_HOSTS+=("${e%%=*}"); MGR_ADDRS+=("${e#*=}") ;;
            *)
                if [ "$ROLE" = "manager" ]; then
                    if [ "$BOOTSTRAP" = "true" ] && [ "${#ents[@]}" -eq 1 ]; then
                        MGR_HOSTS+=("$HOSTNAME_FQ")
                    else
                        error "--managers entry '$e' has no hostname; use hostname=addr:port (hostname must equal that manager's [node].hostname)"
                        return 1
                    fi
                else
                    MGR_HOSTS+=("")
                fi
                MGR_ADDRS+=("$e")
                ;;
        esac
    done
    if [ "$ROLE" = "manager" ] && [ "$BOOTSTRAP" = "true" ]; then
        for h in "${MGR_HOSTS[@]}"; do [ "$h" = "$HOSTNAME_FQ" ] && self=1; done
        [ "$self" -eq 1 ] || { error "bootstrap manager '$HOSTNAME_FQ' is not named in --managers; its entry must be $HOSTNAME_FQ=<routable addr:port>"; return 1; }
    fi
}

if [ -n "$ROLE_ARG" ]; then
    ROLE="$ROLE_ARG"
elif [ "$NONINT" -eq 1 ]; then
    error "--noninteractive requires --role"; exit 1
else
    ROLE=$(whiptail --title "DTFS install" --menu "Select role for this host:" 16 70 6 \
        manager     "Cluster manager (Raft + metadata + API)" \
        data        "Data node (chunk store)" \
        fuse-client "FUSE client (POSIX mount)" \
        3>&1 1>&2 2>&3) || { error "cancelled"; exit 1; }
fi
case "$ROLE" in
    manager|data|fuse-client) ;;
    *) error "--role must be manager|data|fuse-client"; exit 1 ;;
esac

CFG="/etc/dtfs/${ROLE/fuse-client/fuse}.toml"
KEEP_CFG=0
if [ -f "$CFG" ] && [ "$RECONFIG" -eq 0 ]; then
    KEEP_CFG=1
    info "$CFG exists; keeping it (pass --reconfigure to re-render)"
fi

if [ "$KEEP_CFG" -eq 0 ]; then
    [ -n "$HOSTNAME_FQ" ] || HOSTNAME_FQ=$(hostname -f 2>/dev/null || hostname)
    ask HOSTNAME_FQ "Node identity" "Hostname for this node:" "$HOSTNAME_FQ"

    [ -n "$SHARED_SECRET" ] || SHARED_SECRET="$(existing_secret)"
    [ -n "$SHARED_SECRET" ] || SHARED_SECRET="$(gen_secret)" \
        || { error "no secure RNG (openssl or /dev/urandom); pass --secret"; exit 1; }
    ask SHARED_SECRET "Auth" "Cluster shared secret (32+ hex chars):" "$SHARED_SECRET"
    [ -n "$SHARED_SECRET" ] || { error "shared secret required"; exit 1; }

    ask MGR_LIST "Cluster managers" "Comma-separated managers (hostname=addr:9401; hostname = that manager's [node].hostname):" ""
    [ -z "$MGR_LIST" ] && { error "manager list required"; exit 1; }

    ask CLUSTER_NAME "Cluster identity" "Cluster name:" "dtfs-cluster"

    case "$ROLE" in
        manager)
            if [ -z "$BOOTSTRAP" ]; then
                if [ "$NONINT" -eq 1 ]; then
                    BOOTSTRAP=false
                else
                    BOOTSTRAP=$(whiptail --yesno "Is this the bootstrap manager? (first node)" \
                        10 70 --title "Bootstrap?" 3>&1 1>&2 2>&3 && echo true || echo false)
                fi
            fi
            ;;
        data)
            ask DISKS "Storage" "Disk paths (comma-separated, hot first):" "/mnt/disk-a,/mnt/disk-b"
            ;;
        fuse-client)
            ask MOUNTPOINT "Mount" "Mount point:" "/mnt/dtfs"
            ;;
    esac
    parse_managers || exit 1
fi
if [ "$ROLE" = "fuse-client" ] && [ -z "$MOUNTPOINT" ]; then
    MOUNTPOINT="$(grep -hE '^[[:space:]]*mountpoint[[:space:]]*=' "$CFG" | head -1 | sed -E 's/^[^"]*"([^"]*)".*/\1/')"
    [ -n "$MOUNTPOINT" ] || { error "cannot determine mountpoint from $CFG; pass --mountpoint"; exit 1; }
fi

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
# The units run with ProtectSystem=strict and ReadWritePaths on the role
# subdirectory; it must exist (else systemd fails with 226/NAMESPACE).
install -d -o dtfs -g dtfs -m 0750 /etc/dtfs /var/lib/dtfs /var/log/dtfs
case "$ROLE" in
    manager) install -d -o dtfs -g dtfs -m 0750 /var/lib/dtfs/manager ;;
    data)    install -d -o dtfs -g dtfs -m 0750 /var/lib/dtfs/data ;;
esac

step "Installing systemd units"
install -m 0644 "$TMP/dtfs/systemd/"*.service /etc/systemd/system/ 2>/dev/null || true

# ─── render config + start service ───────────────────────────────────────────
render_manager() {
    local VOTERS="" i
    for i in "${!MGR_ADDRS[@]}"; do
        VOTERS+="  { hostname = \"${MGR_HOSTS[$i]}\", addr = \"${MGR_ADDRS[$i]}\" },
"
    done
    cat > "$CFG" <<EOF
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

# Required tables (no serde default on ManagerConfig); empty = built-in defaults.
[defaults]

[tiering]

[monitoring]
EOF
    chown dtfs:dtfs "$CFG"; chmod 0640 "$CFG"
}

render_data() {
    local -a MGRS DKS; MGRS=("${MGR_ADDRS[@]}"); IFS=',' read -ra DKS <<<"$DISKS"
    cat > "$CFG" <<EOF
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
    local i tier
    for i in "${!DKS[@]}"; do
        tier="hot"
        [ "$i" -ge 1 ] && tier="cold"
        cat >> "$CFG" <<EOF
[[disks]]
path = "${DKS[$i]}"
tier = "$tier"

EOF
    done
    cat >> "$CFG" <<EOF
[cache]
max_memory_mb = 2048
policy = "arc"

[scrub]
quick_interval_hours = 24
deep_interval_hours = 168
max_io_percent = 10
EOF
    chown dtfs:dtfs "$CFG"; chmod 0640 "$CFG"
}

render_fuse() {
    local -a MGRS; MGRS=("${MGR_ADDRS[@]}")
    cat > "$CFG" <<EOF
[client]
manager_addr = "${MGRS[0]}"
manager_addrs = [$(printf '"%s",' "${MGRS[@]}" | sed 's/,$//')]
shared_secret = "$SHARED_SECRET"

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
    # Holds the secret; dtfs-fuse@.service runs as root.
    chown root:root "$CFG"; chmod 0640 "$CFG"
}

[ "$KEEP_CFG" -eq 1 ] || "render_${ROLE/fuse-client/fuse}"

case "$ROLE" in
    manager)
        systemctl daemon-reload
        systemctl enable dtfs-manager.service
        systemctl restart dtfs-manager.service
        info "Manager installed and running. Check: dtfs-cli status"
        ;;
    data)
        # Disks live outside /var/lib/dtfs; ProtectSystem=strict makes them
        # read-only unless listed. Derive from the config actually in use.
        DISK_PATHS="$(grep -E '^[[:space:]]*path[[:space:]]*=' "$CFG" | sed -E 's/^[^"]*"([^"]*)".*/\1/' | tr '\n' ' ')"
        install -d -m 0755 /etc/systemd/system/dtfs-data.service.d
        # "-" prefix: a not-yet-mounted disk must not fail the unit namespace.
        printf '[Service]\nReadWritePaths=%s\n' "$(for d in $DISK_PATHS; do printf -- '-%s ' "$d"; done)" \
            > /etc/systemd/system/dtfs-data.service.d/disks.conf
        for d in $DISK_PATHS; do
            [ -d "$d" ] || { warn "disk path $d does not exist; mount it and restart dtfs-data"; continue; }
            if [ "$CHOWN_DISKS" -eq 1 ]; then chown dtfs:dtfs "$d"; fi
            if ! runuser -u dtfs -- test -w "$d" 2>/dev/null; then
                warn "user dtfs cannot write $d; run: chown dtfs:dtfs $d  (or re-run with --chown-disks)"
            fi
        done
        systemctl daemon-reload
        systemctl enable dtfs-data.service
        systemctl restart dtfs-data.service
        info "Data node installed and running."
        ;;
    fuse-client)
        mkdir -p "$MOUNTPOINT"
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
