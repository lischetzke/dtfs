#!/usr/bin/env bash
# Probe write-then-read coherency through a single open fd.
# If this fails, every database engine on DTFS is broken.

set -u
M=${MOUNT:-/mnt/dtfs}
F=$M/coh-$(date +%s).bin

sudo rm -f "$F" 2>/dev/null || true

echo "=== Test 1: write then read via a single dd ==="
echo "deadbeef" | sudo tee "$F" >/dev/null
sudo dd if="$F" bs=10 count=1 status=none 2>&1
sudo rm -f "$F"

echo ""
echo "=== Test 2: write 16K, then read 16K via python on the SAME fd ==="
sudo python3 - <<'PY'
import os, sys
M = "/mnt/dtfs"
F = f"{M}/coh-py-{os.getpid()}.bin"
data = bytes(range(256)) * 64   # 16384 bytes, deterministic
with open(F, "w+b") as f:
    f.write(data)
    f.flush()
    os.fsync(f.fileno())
    f.seek(0)
    read = f.read(len(data))
    print(f"wrote={len(data)} read={len(read)} match={read == data}")
    if read != data:
        for i in range(0, min(len(data), len(read)), 4096):
            if read[i:i+4096] != data[i:i+4096]:
                print(f"  first divergent block at offset {i}: wrote[:8]={data[i:i+8].hex()} read[:8]={read[i:i+8].hex()}")
                break
os.unlink(F)
PY

echo ""
echo "=== Test 3: write 16K, CLOSE, reopen, read 16K ==="
sudo python3 - <<'PY'
import os
M = "/mnt/dtfs"
F = f"{M}/coh-reopen-{os.getpid()}.bin"
data = b"abcdefgh" * 2048   # 16 KiB
with open(F, "wb") as f:
    f.write(data)
    f.flush()
    os.fsync(f.fileno())
with open(F, "rb") as f:
    read = f.read()
    print(f"reopen wrote={len(data)} read={len(read)} match={read == data}")
os.unlink(F)
PY

echo ""
echo "=== Test 4: pwrite at offset, pread same offset (SQLite pattern) ==="
sudo python3 - <<'PY'
import os
M = "/mnt/dtfs"
F = f"{M}/coh-pwrite-{os.getpid()}.bin"
# create empty
with open(F, "wb"):
    pass
fd = os.open(F, os.O_RDWR)
# write 4K page at offset 0
page = b"X" * 4096
os.pwrite(fd, page, 0)
os.fsync(fd)
# write 4K page at offset 4096
page2 = b"Y" * 4096
os.pwrite(fd, page2, 4096)
os.fsync(fd)
# read back via pread
got0 = os.pread(fd, 4096, 0)
got1 = os.pread(fd, 4096, 4096)
print(f"page0 match={got0 == page} (read {got0[:8]!r}, expected {page[:8]!r})")
print(f"page1 match={got1 == page2} (read {got1[:8]!r}, expected {page2[:8]!r})")
os.close(fd)
os.unlink(F)
PY
