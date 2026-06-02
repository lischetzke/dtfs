#!/usr/bin/env bash
sudo find /mnt -name "*.dat" -printf "%s\n" 2>/dev/null | python3 -c "
import sys
sizes=[int(l) for l in sys.stdin if l.strip()]
if not sizes:
    print('no chunks'); sys.exit()
sizes.sort()
n=len(sizes)
s=sum(sizes)
print(f'count={n}')
print(f'sum_GB={s/1024/1024/1024:.2f}')
print(f'avg_MB={s/n/1024/1024:.2f}')
print(f'min_MB={min(sizes)/1024/1024:.4f}')
print(f'max_MB={max(sizes)/1024/1024:.2f}')
print(f'median_MB={sizes[n//2]/1024/1024:.2f}')
import collections
buckets=collections.Counter()
for sz in sizes:
    mb=sz//(1024*1024)
    if mb < 1:    b='<1MB'
    elif mb < 4:  b='1-4MB'
    elif mb < 5:  b='~4MB'
    elif mb < 16: b='5-15MB'
    elif mb < 64: b='16-63MB'
    else:         b='>=64MB'
    buckets[b]+=1
for k,v in buckets.most_common():
    print(f'  {k}: {v}')
"
