#!/usr/bin/env bash
# Short soak: sustained mixed workload + chaos injection. 60 s wall clock.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="10-soak"

t_section "10 — Soak (60 s mixed workload)"

CL=client-01

s=$(t_start)
sssh $CL "sudo bash -c '
END=\$((\$(date +%s) + 60))
ITER=0
FAILS=0
WRITES=0
READS=0
DELETES=0
while [ \$(date +%s) -lt \$END ]; do
    ITER=\$((ITER+1))
    op=\$((RANDOM % 5))
    case \$op in
        0|1) # write
            sz=\$((RANDOM % 8 + 1))
            if dd if=/dev/urandom of=${MOUNT}/soak-\$ITER.bin bs=1M count=\$sz status=none 2>/dev/null; then
                WRITES=\$((WRITES+1))
            else
                FAILS=\$((FAILS+1))
            fi
            ;;
        2) # read
            files=( \$(ls ${MOUNT}/soak-*.bin 2>/dev/null) )
            if [ \${#files[@]} -gt 0 ]; then
                pick=\${files[\$((RANDOM % \${#files[@]}))]}
                if md5sum \"\$pick\" >/dev/null 2>&1; then
                    READS=\$((READS+1))
                else
                    FAILS=\$((FAILS+1))
                fi
            fi
            ;;
        3) # ls
            ls ${MOUNT}/ >/dev/null 2>&1 || FAILS=\$((FAILS+1))
            ;;
        4) # delete (with 1/3 chance)
            if [ \$((RANDOM % 3)) -eq 0 ]; then
                files=( \$(ls ${MOUNT}/soak-*.bin 2>/dev/null) )
                if [ \${#files[@]} -gt 0 ]; then
                    pick=\${files[\$((RANDOM % \${#files[@]}))]}
                    rm -f \"\$pick\" 2>/dev/null
                    DELETES=\$((DELETES+1))
                fi
            fi
            ;;
    esac
done
echo \"iter=\$ITER writes=\$WRITES reads=\$READS deletes=\$DELETES fails=\$FAILS\"
'" > /tmp/soak.out 2>&1
elapsed=$(t_elapsed $s)
result=$(cat /tmp/soak.out | tail -1)
echo "$result" > /tmp/soak.parsed
fails=$(echo "$result" | grep -oE "fails=[0-9]+" | cut -d= -f2)

if [ -n "$fails" ] && [ "$fails" -le 5 ]; then
    t_pass "T10.1 60-s mixed soak" "$elapsed" "$result"
else
    t_fail "T10.1 60-s mixed soak" "$elapsed" "$result"
fi

# Cleanup
sssh $CL "sudo rm -f ${MOUNT}/soak-*.bin" >/dev/null 2>&1
