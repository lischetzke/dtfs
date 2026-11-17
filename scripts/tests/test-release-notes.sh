#!/usr/bin/env bash
# Guard for scripts/ci/release-notes.sh: how the release notes are composed
# from the release commit body and the git-cliff changelog.
set -u
SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/ci/release-notes.sh"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
PASS=0; FAIL=0

ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
has()  { if grep -qxF -- "$2" <<<"$OUT"; then ok "$1"; else bad "$1: missing line '$2'"; fi; }
lacks(){ if grep -qF -- "$2" <<<"$OUT"; then bad "$1: unexpected '$2'"; else ok "$1"; fi; }
count(){ local n; n="$(grep -cF -- "$2" <<<"$OUT" || true)"; [ "$n" = "$3" ] && ok "$1" || bad "$1: '$2' x$n, want $3"; }
line() { grep -nxF -- "$1" <<<"$OUT" | head -n1 | cut -d: -f1; }

printf '### Bug Fixes\n\n- bug: one (abc1234)\n' > "$T/cliff.md"
: > "$T/empty.md"
cat > "$T/body.txt" <<'EOF'
release v0.1.19

Summary paragraph.

> [!WARNING]
> **Update required: managers first.**

## Highlights

- Thing (fuse)

### Thing (fuse)

Details.

Also: small removal.

Co-Authored-By: Someone <a@example.com>
Claude-Session: https://example.invalid/s
EOF

echo "body + trailers"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/cliff.md" "$T/body.txt")"
has   "welcome first"           'Welcome to DTFS `v0.1.19`!'
[ "$(head -n1 <<<"$OUT")" = 'Welcome to DTFS `v0.1.19`!' ] && ok "welcome is line 1" || bad "welcome is line 1"
lacks "subject removed"         'release v0.1.19'
lacks "Co-Authored-By stripped" 'Co-Authored-By'
lacks "Claude-Session stripped" 'Claude-Session'
has   "Also line kept"          'Also: small removal.'
has   "ansible line"            'ansible-playbook -i /path/to/inventory.yml deploy/ansible/update-cluster.yml -e dtfs_version=v0.1.19'
has   "per host line"           'Or per host: `dtfs-cli update all --tag v0.1.19` (as root), managers first (followers, then the leader), then data nodes, then clients.'
has   "changelog kept"          '- bug: one (abc1234)'
a="$(line 'Summary paragraph.')"; b="$(line '## Highlights')"; c="$(line '## Upgrading')"; d="$(line '---')"; e="$(line "## What's Changed")"
[ -n "$a" ] && [ "$a" -lt "$b" ] && [ "$b" -lt "$c" ] && [ "$c" -lt "$d" ] && [ "$d" -lt "$e" ] && ok "section order" || bad "section order: $a $b $c $d $e"
count "headings survive"        '### Thing (fuse)' 1

echo "no body (subject + trailers only)"
printf 'release v0.1.19\n\nCo-Authored-By: Someone <a@example.com>\n' > "$T/nobody.txt"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/cliff.md" "$T/nobody.txt")"
[ "$(head -n1 <<<"$OUT")" = 'Welcome to DTFS `v0.1.19`!' ] && ok "welcome first" || bad "welcome first"
lacks "no trailer"              'Co-Authored-By'
lacks "no highlights"           '## Highlights'
has   "upgrading"               '## Upgrading'
has   "changelog"               '- bug: one (abc1234)'

echo "no message file"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/cliff.md")"
[ "$(head -n1 <<<"$OUT")" = 'Welcome to DTFS `v0.1.19`!' ] && ok "welcome first" || bad "welcome first"
has   "upgrading"               '## Upgrading'
has   "changelog"               '- bug: one (abc1234)'

echo "body that has its own welcome line"
printf 'release v0.1.19\n\nWelcome to DTFS `v0.1.19`!\n\nSummary.\n\nCo-Authored-By: S <a@example.com>\n' > "$T/w.txt"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/cliff.md" "$T/w.txt")"
count "one welcome line"        'Welcome to DTFS' 1
[ "$(head -n1 <<<"$OUT")" = 'Welcome to DTFS `v0.1.19`!' ] && ok "welcome is line 1" || bad "welcome is line 1"

echo "last paragraph that is not a trailer block stays"
printf 'release v0.1.19\n\nSummary.\n\nNote: this is prose\nand not a trailer.\n' > "$T/p.txt"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/cliff.md" "$T/p.txt")"
has   "prose kept"              'and not a trailer.'

echo "empty cliff file"
OUT="$(bash "$SCRIPT" v0.1.19 "$T/empty.md" "$T/body.txt")"
has   "heading present"         "## What's Changed"
has   "placeholder"             'No user-visible changes.'

echo "bad arguments"
for args in "" "0.1.19 $T/cliff.md" "v0.1 $T/cliff.md" "v0.1.19 $T/missing.md" "v0.1.19 $T/cliff.md $T/missing.txt"; do
    # shellcheck disable=SC2086
    if bash "$SCRIPT" $args >/dev/null 2>&1; then bad "should fail: '$args'"; else ok "fails: '$args'"; fi
done

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
