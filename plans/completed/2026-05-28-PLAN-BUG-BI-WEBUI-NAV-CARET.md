# PLAN-BUG-BI — WebUI nav dropdown caret renders literal `\25BE`

Plan id: `BI`
Status: ✅ code complete (2026-05-28) — fix + unit green; live browser confirm pending next manager redeploy.
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28

---

## 0. Background

The grouped-nav dropdown buttons **Cluster** and **Data** show the
literal text `\25BE` next to the label instead of the intended ▾
(U+25BE, BLACK DOWN-POINTING SMALL TRIANGLE) dropdown caret. Reported
from a live WebUI screenshot, 2026-05-28.

### Root cause (code-confirmed)

`crates/dtfs-manager/src/webui.rs:129-130`:

```css
.nav .nav-group > button::after {
    content: ' \\25BE'; font-size: .7rem; margin-left: .3rem;
```

The whole page is built with a **Rust raw string**:
`format!(r##"<!DOCTYPE html> … "##)` (`webui.rs:60-61`). In a raw
string, `\\` is **two literal backslash characters** — there is no
escape processing. So the emitted CSS is literally `content: ' \\25BE'`.
The browser's CSS parser reads `\\` as an *escaped backslash* (one
literal `\`), then the plain text `25BE`, yielding the visible string
`\25BE` instead of the intended single-escape `\25BE` → ▾.

In a normal (non-raw) Rust string `\\` would collapse to one backslash
and the CSS escape would work; the doubling is only wrong because the
template is raw.

---

## 1. Goals / Non-goals

### Goals
- Nav-group dropdown buttons (Cluster, Data) show a ▾ caret.
- Robust against the raw-string escaping pitfall.

### Non-goals
- No restructure of the nav markup or the grouped-nav behaviour.
- No change to any other glyph/icon on the page.

---

## 2. Locked-in decisions

1. Use the **literal `▾` glyph** in the source `content:` declaration
   rather than a CSS hex escape. The file is UTF-8 and already embeds
   literal glyphs (e.g. `🏢` at `webui.rs:1371`, `⋯` at `webui.rs:2045`),
   so a literal caret avoids the raw-string backslash trap entirely.
   (Acceptable alternative: a single-backslash CSS escape with a
   delimiter — `' \25BE '` or `' \0025be'` — but the glyph is simpler
   and self-documenting.)

---

## 3. Task list

### BI-1 — Fix nav dropdown caret
- Status: ✅ (2026-05-28) — `content: ' \\25BE'` → `content: ' ▾'` (literal glyph) at webui.rs:130. Grep confirms no other `\\<hex>` CSS escape in the raw-string template. Unit `nav_caret_not_double_escaped` green; dtfs-manager 139/139. Live browser check pending the next manager redeploy (cosmetic; unit asserts the rendered HTML).
- **Files:** `crates/dtfs-manager/src/webui.rs`
- **Implementation sketch:** At `webui.rs:130`, replace
  `content: ' \\25BE';` with `content: ' ▾';` (literal glyph). Verify no
  other `\\<hex>` CSS escapes exist in the raw-string template that
  suffer the same doubling (grep `\\\\[0-9A-Fa-f]` within `webui.rs`); if
  any exist, convert them to literal glyphs too and note them here.
- **Tests (BI-1):** WebUI is a raw-string template returned by the
  render fn, so it is unit-testable in `dtfs-manager` without a browser:
  - Unit `nav_caret_not_double_escaped`: render the page HTML and assert
    it does **not** contain the literal `\\25BE` (double backslash) and
    **does** contain the caret glyph in the `nav-group > button::after`
    rule. Fails on current code, passes after the fix.
- **Done when:** Cluster / Data dropdown buttons render a ▾ caret in the
  browser; unit assertion green; `cargo test -p dtfs-manager --release`
  clean; live-verified on a manager WebUI.
- **Note (§ B.5.1):** pure cosmetic CSS fix, no cluster-visible
  behaviour change → no release-suite case required; the unit assertion
  covers regression.

---

## 4. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|
| BI-1 | 2026-05-28 | `cargo test -p dtfs-manager bug_bi_nav` | 1/1 pass; full suite 139/139 |

---

## 5. Deviations from plan

_(empty)_

---

## 6. Notes for the implementer

- Concurrent work is active on `master` (other agent). Stage only
  `webui.rs` (+ this plan) for the BI commit; do not fold in unrelated
  uncommitted edits in `crates/dtfs-client/` or `crates/dtfs-fuse/`.
