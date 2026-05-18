#!/usr/bin/env bash
# Common stub harness for not-yet-implemented scenarios. Reports
# verdict=warn with a descriptive message so the soak driver continues
# and the final report includes the gap in the WARN column.
SCENARIO="${SCENARIO:-unknown}"
REASON="${REASON:-not yet implemented}"
echo "=== $SCENARIO (stub) ==="
echo "reason: $REASON"
echo "verdict=warn: stub-only — $REASON"
exit 0
