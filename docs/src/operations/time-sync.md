# Time Synchronisation

DTFS relies on accurate, monotonically-increasing wall-clock time across all
cluster nodes.  Reliable time is **not optional** — several subsystems depend
on it directly:

| Subsystem | Why time matters |
|---|---|
| Raft consensus | Leader election timeouts; log entry ordering |
| Audit log | Timestamps on every mutation event |
| Tiering engine | Hot/cold classification uses "days since last access" |
| Scrub scheduler | Interval calculations for quick/deep scrub cycles |
| JWT authentication | Token expiry checked against current time |
| Heartbeat monitoring | Suspect/down thresholds measured in wall-clock seconds |

> **Rule of thumb:** Keep all nodes within **100 ms** of each other.
> Raft tolerates moderate clock skew (it uses logical clocks internally),
> but audit timestamps, tiering decisions, and token expiry all use wall time.
> Skew beyond a few seconds can cause confusing behaviour.

---

## Required: chrony or ntpd

Every DTFS node (manager, data, FUSE client) **must** run a time-sync daemon.
The recommended choice on Debian/Ubuntu is **chrony** (ships by default on
Debian 12+).

### Install and enable chrony (Debian/Ubuntu)

```bash
sudo apt-get install -y chrony
sudo systemctl enable --now chrony
```

### Verify synchronisation status

```bash
chronyc tracking
```

Key fields to check:

| Field | Healthy value |
|---|---|
| `System time` | < 100 ms |
| `RMS offset` | < 50 ms |
| `Leap status` | `Normal` |
| `Reference ID` | Should not be `7F7F0101` (local fallback) |

```bash
chronyc sources -v   # shows all NTP sources and their state
```

---

## Recommended chrony configuration

`/etc/chrony/chrony.conf`:

```
# Use pool servers with iburst for fast initial sync
pool 2.debian.pool.ntp.org iburst maxsources 4

# Allow up to 1 second of step correction on startup only
makestep 1.0 3

# Drift file
driftfile /var/lib/chrony/chrony.drift

# Log statistics
logdir /var/log/chrony

# NTP hardware timestamping if supported
#hwtimestamp *
```

For an **air-gapped / private cluster** with no public internet access,
designate one node (typically `mgr-01`) as the internal NTP server:

```
# On mgr-01: act as NTP server for the cluster
local stratum 10
allow 10.0.0.0/8

# On all other nodes: point at mgr-01
server 10.0.1.1 iburst prefer
```

---

## Initial time correction

After provisioning a new node, force an immediate time synchronisation before
starting DTFS services:

```bash
sudo systemctl stop chrony
sudo chronyd -q 'pool 2.debian.pool.ntp.org iburst'
sudo systemctl start chrony
```

Or with timedatectl (systemd-timesyncd):

```bash
sudo timedatectl set-ntp true
sudo timedatectl status
```

---

## Monitoring time synchronisation

### Prometheus alert

Add this alert to your alerting rules to catch clock drift early:

```yaml
- alert: NodeClockDrift
  expr: abs(node_timex_offset_seconds) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Clock drift > 100 ms on {{ $labels.instance }}"
    description: >
      Node {{ $labels.instance }} has a clock offset of
      {{ $value | humanizeDuration }} from NTP.
      DTFS tiering decisions and audit timestamps may be inaccurate.

- alert: NodeClockSkewCritical
  expr: abs(node_timex_offset_seconds) > 1.0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Critical clock skew on {{ $labels.instance }}"
    description: >
      Clock offset of {{ $value | humanizeDuration }} detected.
      Raft election timeouts and JWT token validation may malfunction.
```

This requires the `node_exporter` with `--collector.ntp` enabled (or the
default `timex` collector which is enabled by default in node_exporter ≥ 1.0).

### Ansible health check

The DTFS Ansible role (`dtfs-common`) includes a pre-task that verifies
chrony is synchronised before deploying any DTFS service:

```yaml
- name: Verify time synchronisation
  command: chronyc tracking
  register: chrony_out
  failed_when: >
    'Not synchronised' in chrony_out.stdout or
    chrony_out.rc != 0
```

---

## Virtual machines and containers

On VMs and containers, hardware time may drift significantly.  Additional
precautions:

- **VMware/Hyper-V/KVM**: Enable guest time synchronisation in the hypervisor
  **and** still run chrony inside the guest (both layers working together
  gives the best accuracy).
- **Docker containers**: Containers share the host kernel clock.  The host
  must run chrony; containers need no additional config.
- **LXC/LXD containers**: Same as Docker — host-level NTP is sufficient.
- **Kubernetes pods**: Ensure the Kubernetes nodes run chrony; pod clocks
  inherit node time.

---

## What happens if clocks are out of sync?

| Clock skew | Impact |
|---|---|
| < 100 ms | No practical impact |
| 100 ms – 1 s | Audit timestamps inaccurate; tiering intervals slightly off |
| 1 s – 30 s | JWT tokens may expire prematurely or be accepted too long; heartbeat false-positives possible |
| > 30 s | Raft election stability may degrade; scrub scheduler drift; cold-tier migration triggered at wrong times |
| > 300 s | `min_replicas_before_ack` write-safety timeouts may fire spuriously |

---

## Leap second handling

Chrony handles leap seconds automatically using the "slew" method (spreading
the correction over a short period) which avoids the backward-jump problem.
No special DTFS configuration is needed.
