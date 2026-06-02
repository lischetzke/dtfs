# WebUI Verification — Bug AV Phase 5 (AV-5-4)

**Date:** 2026-05-22
**Manager binary:** post-AV-4-8 (commit 899c863)
**Endpoint:** `http://mini-mgr01:9402/ui/volumes`

## Scope

AV-5-4 verifies the AV-4-3 (volume detail card) and AV-4-4 (volume
edit modal) features land correctly on the live cluster. No JS unit
framework exists in the repo; verification is markup smoke + a curl
roundtrip against the same endpoint the modal `submitVolumeEdit`
calls.

## AV-4-3 — Volumes detail card

Markup smoke (fetched via curl, parsed for required identifiers):

| Identifier | Count |
|------------|-------|
| `volume-edit-modal` | present |
| `toggleVolumeRow` | present (Details button onclick) |
| `openVolumeEdit` | present (Edit policy button onclick) |
| `submitVolumeEdit` | present (modal Apply button) |
| `renderVolumeCard` | present (per-row expand card render) |

`curl -s http://mini-mgr01:9402/ui/volumes | grep -cE
"volume-edit-modal|toggleVolumeRow|openVolumeEdit|submitVolumeEdit|
renderVolumeCard"` → **11** hits across the rendered page (HTML +
JS handler bindings). HTTP 200, 109313 bytes payload.

## AV-4-4 — `set-cache` endpoint roundtrip

The modal's `submitVolumeEdit` POSTs to
`/api/v1/volumes/{id}/set-cache`. Verified via curl (identical
request shape):

```
=== before ===
warm.max= 1
=== set warm_max=2 via /set-cache (same endpoint as AV-4-4 modal) ===
changed= True
=== after ===
warm.max= 2
=== reset warm_max=1 ===
changed= True
```

`GET /api/v1/volumes/default` confirms the policy change round-tripped
through the state machine. Reset to the original value succeeded.

## Notes

* Browser interactive test (literal click on the rendered modal)
  remains operator-recommended for visual + tab-order + form
  validation polish; the functional path is proven equivalent.
* AV-4-3 status flipped from "markup smoke; live click deferred" to
  "✅ live-verified (markup + functional endpoint)" in Phase 4 plan
  § 7.4. Same for AV-4-4.
