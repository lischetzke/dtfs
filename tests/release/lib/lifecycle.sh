# tests/release/lib/lifecycle.sh — clean-room cluster provisioning + teardown
# for `run.sh --provision`. Reuses the tested PVE provisioner (pwsh) and the
# ansible deploy playbooks. Depends on common.sh (fail) + health.sh
# (wait_cluster_ready, wait_all_healthy). Only invoked when --provision is set.

_LC_LIBDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Lab provisioning tooling (provision-3dc.ps1, pve-chaos.ps1) is not in the repo.
PROXMOX_DIR=${PROXMOX_DIR:-}
ANSIBLE_DIR=${ANSIBLE_DIR:-$PROXMOX_DIR/ansible}
INV3DC=${INV3DC:-$ANSIBLE_DIR/inventories/3dc/hosts.yml}

# Clone the 13-VM 3dc topology, deploy DTFS, mount tier disks, apply shaping,
# then block until the cluster answers and every node is healthy.
provision_cluster() {
    command -v pwsh >/dev/null 2>&1            || fail "pwsh required to provision (apt install powershell)"
    command -v ansible-playbook >/dev/null 2>&1 || fail "ansible-playbook required to provision"
    [ -n "${PVE_TOKEN:-}" ]                     || fail "PVE_TOKEN required to provision"

    echo "[lifecycle] provisioning 3dc VMs via PVE (provision-3dc.ps1)"
    PVE_TOKEN="$PVE_TOKEN" pwsh -NoProfile -File "$PROXMOX_DIR/provision-3dc.ps1" \
        || fail "VM provisioning failed"

    echo "[lifecycle] deploying DTFS (full-deploy.yml)"
    ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/full-deploy.yml"      || fail "full-deploy failed"
    ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/mount-data-disks.yml" || fail "mount-data-disks failed"
    ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/apply-shaping.yml" \
        || echo "[lifecycle] WARN: shaping apply failed (continuing unshaped)"

    echo "[lifecycle] waiting for cluster ready + healthy"
    wait_cluster_ready 300
    wait_all_healthy 300
    echo "[lifecycle] cluster up"
}

# Destroy the lab VMs unless KEEP_CLUSTER=1 (leave up for debugging).
teardown_cluster() {
    if [ "${KEEP_CLUSTER:-0}" = "1" ]; then
        echo "[lifecycle] KEEP_CLUSTER=1 — leaving cluster up"
        return 0
    fi
    command -v pwsh >/dev/null 2>&1 || { echo "[lifecycle] pwsh missing — cannot teardown"; return 0; }
    echo "[lifecycle] destroying 3dc VMs (pve-chaos destroy-all)"
    PVE_TOKEN="${PVE_TOKEN:-}" pwsh -NoProfile -File "$PROXMOX_DIR/pve-chaos.ps1" -Action destroy-all \
        || echo "[lifecycle] WARN: teardown reported errors"
}
