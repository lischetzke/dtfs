#!/usr/bin/env bash
# Exercises verify_minisign() from scripts/dtfs-install.sh with a real
# minisign keypair (needs the minisign binary; skips with exit 0 if absent).
set -uo pipefail
SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/dtfs-install.sh"
command -v minisign >/dev/null 2>&1 || { echo "SKIP: minisign not installed"; exit 0; }

W="$(mktemp -d)"; trap 'rm -rf "$W"' EXIT
info() { :; }; warn() { echo "WARN $*" >>"$W/log"; }; error() { echo "ERR $*" >>"$W/log"; }
fetch_hops() { cp "${FAKE_REMOTE:?}/$(basename "$1")" "$2"; }
eval "$(sed -n '/^verify_minisign()/,/^}/p' "$SCRIPT")"

mkdir "$W/rel" "$W/rel2" "$W/rem"
printf 'secret-bytes' > "$W/rel/dtfs-v1-linux-amd64.tar.gz"
cp "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$W/rel/dtfs-linux-amd64.tar.gz"
( cd "$W/rel" && sha256sum dtfs-linux-amd64.tar.gz > SHA256SUMS )
minisign -G -W -p "$W/k.pub" -s "$W/k.sec" >/dev/null
minisign -G -W -f -p "$W/other.pub" -s "$W/other.sec" >/dev/null
minisign -S -s "$W/k.sec" -m "$W/rel/SHA256SUMS" -x "$W/rel/SHA256SUMS.minisig" </dev/null >/dev/null
PUB="$(sed -n 2p "$W/k.pub")"

pass=0; fail=0
ck() { # label expected_rc
    local l="$1" want="$2"; shift 2
    "$@"; local rc=$?
    if { [ "$want" = 0 ] && [ $rc -eq 0 ]; } || { [ "$want" != 0 ] && [ $rc -ne 0 ]; }; then pass=$((pass+1)); echo "  ok  $l"
    else fail=$((fail+1)); echo "  FAIL $l (rc=$rc)" >&2; fi
}
# shellcheck disable=SC2034  # read by the eval'd verify_minisign
run() { ( TARBALL="$1"; MINISIGN_KEY="$2"; SUMS_URL_BASE="${3:-}"; verify_minisign ); }

ck "good signature, base64 key"        0 run "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$PUB"
ck "good signature, .pub file key"     0 run "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$W/k.pub"
ck "no key only warns"                 0 run "$W/rel/dtfs-v1-linux-amd64.tar.gz" ""
grep -q "NOT verified" "$W/log" && { pass=$((pass+1)); echo "  ok  no-key warning logged"; } || { fail=$((fail+1)); echo "  FAIL no-key warning" >&2; }
ck "wrong key refused"                 1 run "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$W/other.pub"

cp -r "$W/rel/." "$W/rel2/"; printf 'tampered' > "$W/rel2/dtfs-v1-linux-amd64.tar.gz"
ck "tampered tarball refused"          1 run "$W/rel2/dtfs-v1-linux-amd64.tar.gz" "$PUB"

cp -r "$W/rel/." "$W/rel2/"; printf 'secret-bytes' > "$W/rel2/dtfs-v1-linux-amd64.tar.gz"
echo "0000 dtfs-linux-amd64.tar.gz" > "$W/rel2/SHA256SUMS"
ck "forged SHA256SUMS refused"         1 run "$W/rel2/dtfs-v1-linux-amd64.tar.gz" "$PUB"

rm -f "$W/rel2/SHA256SUMS.minisig"
ck "missing .minisig refused"          1 run "$W/rel2/dtfs-v1-linux-amd64.tar.gz" "$PUB"

mkdir "$W/cache"; cp "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$W/cache/"
export FAKE_REMOTE="$W/rel"
ck "remote sums fetched and verified"  0 run "$W/cache/dtfs-v1-linux-amd64.tar.gz" "$PUB" "https://x/y"

PATH=/nonexistent ck "missing minisign binary refused" 1 run "$W/rel/dtfs-v1-linux-amd64.tar.gz" "$PUB"

echo; [ $fail -eq 0 ] && { echo "PASS - $pass checks"; exit 0; } || { echo "FAIL - $fail failed, $pass ok" >&2; exit 1; }
