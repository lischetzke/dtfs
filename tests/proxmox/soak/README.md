# DTFS Prod-Readiness Soak Campaign

Long-running validation harness for the 3-DC topology. Drives realistic
media-server workloads while injecting failures and verifying anchor +
data-integrity invariants throughout.

## Layout

```
soak/
├── run-soak.sh             # Master driver — runs scenarios + workloads on schedule
├── collect-metrics.py      # Background scraper: /metrics + REST every 30s
├── render-report.py        # End-of-run report generator
├── scenarios/SNN-*.sh      # 28 named scenarios (S01-S28)
└── workloads/
    ├── playback-client-a.sh
    ├── playback-client-b.sh
    └── playback-client-c.sh
```

## How to run

```bash
# From the Ansible control host, after cluster is deployed:
./tests/proxmox/soak/run-soak.sh \
    --duration 48h \
    --corpus /mnt/media \
    --results-dir ./tests/proxmox/soak/results/$(date +%Y%m%dT%H%M)
```

## Pass criteria

- BLAKE3 of every source file matches FUSE-mounted version end-to-end.
- ≥1 DC-A replica per file throughout the run (anchor invariant).
- Lease invariant: no observed double-writer commit.
- Aged cache entries evict on DC-B's client.
- Health probe never 503 for >2 min during a single manager restart.
- All scenarios pass on first attempt.

See `../../../.claude/plans/are-all-function-relevant-eventual-bubble.md`
for the full plan + 28-scenario catalogue.
