# Tape Storage (Frozen Tier)

DTFS supports **LTO tape drives and tape libraries** as a `tape`-tier storage
backend for frozen/archival data.  Tape provides the lowest cost-per-TB of any
storage medium and is ideal for data that has not been accessed in 60+ days.

Tape is integrated through **LTFS (Linear Tape File System)**, which presents
a tape cartridge as a regular filesystem directory.  DTFS writes chunks to the
LTFS mount point exactly as it would to any XFS disk — no special I/O path is
required.

---

## When to use tape

| Scenario | Suitable? |
|---|---|
| Archival video/film masters older than 60 days | ✅ Ideal |
| Backup copies of cold data already on HDDs | ✅ Good |
| Active project data accessed daily | ❌ Too slow |
| Warm/hot data requiring < 30 s access latency | ❌ Not suitable |

Tape access latency is typically **30 seconds to several minutes** (depending
on cartridge load time and tape position), so the tape tier should only be used
for data classified as `Frozen` by the tiering engine.

---

## LTO tape specifications reference

| Generation | Native Capacity | Native Speed | Notes |
|---|---|---|---|
| LTO-8 | 12 TB | 360 MB/s | Widely available, good price/TB |
| LTO-9 | 18 TB | 400 MB/s | Current mainstream standard |
| LTO-10 | 36 TB | 500 MB/s | Emerging (2025+) |

All LTO drives support LTFS.  For a tape library, you need:

- One or more tape **drives** (read/write units)
- A **library/autoloader** with a robotic arm for cartridge management
- The **`mt-st`** (tape control) and **`ltfs`** packages

---

## Prerequisites

### Install LTFS

```bash
# Debian/Ubuntu
sudo apt-get install -y ltfs mt-st sg3-utils

# Verify
ltfs --version
```

> Some distributions provide `ltfs` from the IBM LTFS distribution or
> HPE StoreOpen LTFS.  Either works with DTFS.

### Format a new tape cartridge

```bash
# Replace /dev/nst0 with your tape device
sudo mkltfs --device /dev/nst0 --tape-serial LTO001 --volume-name "dtfs-archive-01"
```

> ⚠️ `mkltfs` erases all data on the cartridge.  Verify the device path first
> with `ls -l /dev/nst*` and `sudo mt -f /dev/nst0 status`.

### Mount the tape (LTFS)

```bash
sudo mkdir -p /mnt/tape-a
sudo ltfs /mnt/tape-a -o devname=/dev/nst0
```

Add to `/etc/fstab` or a systemd mount unit for automatic mounting:

```ini
# /etc/systemd/system/mnt-tape-a.mount
[Unit]
Description=LTFS tape mount (LTO drive 0)
After=local-fs.target
Before=dtfs-data.service

[Mount]
What=ltfs
Where=/mnt/tape-a
Type=fuse
Options=devname=/dev/nst0,ro_basedir=/mnt/tape-a,sync_type=unmount

[Install]
WantedBy=multi-user.target
```

---

## Data node configuration

Add a `[[tape_devices]]` section to `/etc/dtfs/data.toml`:

```toml
# Standalone LTO drive
[[tape_devices]]
ltfs_path      = "/mnt/tape-a"
device         = "/dev/nst0"
label          = "lto9-drive-0"
capacity_gb    = 18000

# Tape library (autoloader)
[[tape_devices]]
ltfs_path      = "/mnt/tape-lib0"
device         = "/dev/nst1"
library_device = "/dev/sg2"          # changer device
label          = "lto9-library-main"
capacity_gb    = 18000
max_write_mbps = 350                 # leave headroom; 0 = unlimited
```

### Tier configuration

Reference tape devices in the disk tier configuration:

```toml
# Regular disks (not tape)
[[disks]]
path = "/mnt/ssd-a"
tier = "hot"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"

# Tape is declared separately via [[tape_devices]] above.
# The tiering engine recognises the "tape" tier automatically.
```

### Replication

In `/etc/dtfs/manager.toml`, set the tape replication factor:

```toml
[defaults.replication]
hot  = 3
warm = 2
cold = 1
tape = 1   # Usually 1; tape library provides cartridge-level redundancy
```

Set `tape = 2` only if you have two independent tape libraries and want a
second copy on each (e.g. off-site disaster recovery).

---

## How tiering to tape works

1. The tiering engine classifies chunks as `Frozen` after
   `frozen_threshold_days` (default: 60) of no access.
2. A migration task is created: `cold disk → tape LTFS mount`.
3. The data node writes the chunk sequentially to the LTFS path.
   LTFS buffers data and writes in large sequential blocks (optimal for tape).
4. After the write is confirmed, the source chunk on the cold disk is removed.
5. Chunk metadata in the manager's Raft state is updated to reflect the new
   tape location.

### Migration is crash-safe

Migration follows the same `Pending → Copying → Verifying → Completing → Done`
state machine as all other migrations.  The source chunk is **never** removed
until the tape write is verified.  If the data node crashes during the copy,
the migration task is retried from the beginning on restart.

---

## Chunk layout on tape (LTFS)

Chunks are stored on the LTFS volume using the same directory structure as
on-disk storage:

```
/mnt/tape-a/
├── .dtfs/
│   └── tape.meta          # Tape UUID, label, tier assignment
└── chunks/
    ├── ab/
    │   └── cd/
    │       ├── abcd...hash.dat   # Raw chunk data (128 MB)
    │       └── abcd...hash.meta  # Chunk metadata (checksum, size)
```

---

## Scrubbing on tape

**Quick scrub is not performed on tape.**  The LTFS layer maintains its own
internal checksums (ECC blocks written with every data block) and reports
media errors independently.

**Deep scrub on tape** reads the full chunk data and recomputes the BLAKE3
checksum, but is **strongly discouraged** as a routine operation:

- Reading a full tape cartridge takes hours and causes wear.
- It is only recommended after a media alert from LTFS or if data integrity
  is in question.

To trigger a deep scrub on a specific tape device:

```bash
dtfs-cli scrub --disk /mnt/tape-a --level deep
```

The manager rate-limits tape scrub I/O via `max_io_percent` to avoid
impacting concurrent migration writes.

---

## Tape library management

For automated tape libraries, DTFS uses the LTFS library interface.  The
changer device (e.g. `/dev/sg2`) is used to:

- Load/unload cartridges
- Query cartridge inventory
- Select the appropriate cartridge for a read or write operation

You can query the library state via the CLI:

```bash
# List tape devices and their status
dtfs-cli nodes --id <node_id> | jq '.tape_devices'

# Check LTFS mount status (on the data node directly)
ltfsck /dev/nst0       # Non-destructive consistency check
```

---

## Tape drive monitoring

Monitor tape drives using SMART-like reporting via `mt` and `tapeinfo`:

```bash
sudo tapeinfo -f /dev/nst0    # Drive status, load count, errors
sudo mt -f /dev/nst0 status   # Current tape position and drive state
```

> DTFS does not yet integrate with tape SMART/TapeAlert reporting (planned
> for a future release).  Set up external monitoring with `nagios-plugins-tape`
> or equivalent.

---

## Known limitations

| Limitation | Notes |
|---|---|
| No random writes | Tape is append-only; DTFS only writes new chunks sequentially. Chunks cannot be updated in place. |
| High access latency | 30 s – several minutes per access depending on tape position and cartridge load. Not suitable for warm/hot data. |
| No in-place scrub | LTFS handles integrity; DTFS deep-scrub on tape is read-only verification. |
| Single-drive bottleneck | Only one chunk can be read/written at a time per drive. A tape library with multiple drives improves parallelism. |
| Tape positioning overhead | Random reads across a tape are slow. Batch cold-to-tape migrations whenever possible to keep data sequential. |

---

## Comparison: cold HDD vs. tape

| Property | Cold HDD tier | Tape tier |
|---|---|---|
| Cost/TB (2025) | ~$20–30/TB | ~$5–8/TB (LTO-9) |
| Read latency | 5–20 ms | 30 s – 5 min |
| Throughput | 150–200 MB/s | 300–400 MB/s (sequential) |
| Power (idle) | 4–8 W | 0 W (library idle) |
| Lifespan | 3–5 years | 30+ years (LTO archival spec) |
| Replication | 1–2 copies | 1 copy (library provides redundancy) |
| Best for | Data accessed < monthly | Data accessed < yearly |
