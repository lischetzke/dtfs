#!/usr/bin/env python3
"""Convert docs/src (mdBook) into GitHub Wiki pages.

Usage: sync-wiki.py <docs/src> <out-dir>

* Page set and order come from SUMMARY.md; files not listed there are skipped.
* Page name = SUMMARY title made wiki-safe ("Data Flow" -> Data-Flow.md);
  on a name collision the section title is prefixed.
* Home.md is the introduction, _Sidebar.md mirrors SUMMARY, _Footer.md says
  the pages are generated.
* Links: docs page -> wiki page (anchor kept, re-slugged the GitHub way);
  repo file or skipped docs page -> blob URL on master. Fenced code and
  inline code are left alone.

Python stdlib only. Exit 1 when a link target or anchor does not exist.
"""
import os
import re
import sys
from pathlib import Path

REPO = "https://github.com/lischetzke/dtfs"
BRANCH = "master"

ENTRY = re.compile(r"^(\s*)[-*]\s+\[([^\]]+)\]\(([^)]*)\)")
PLAIN = re.compile(r"^\[([^\]]+)\]\(([^)]+)\)\s*$")
LINK = re.compile(r"(!?)\[([^\]]*)\]\(([^)\s]+)((?:\s+\"[^\"]*\")?)\)")
CODE_SPAN = re.compile(r"(`+)(?:(?!\1).)+?\1")
FENCE = ("```", "~~~")


def wiki_name(title):
    """Wiki-safe page name: spaces -> '-', drop everything but [A-Za-z0-9._-]."""
    name = re.sub(r"\s+", "-", title.strip())
    name = re.sub(r"[^A-Za-z0-9._-]", "", name)
    return re.sub(r"-{2,}", "-", name).strip("-.") or "Page"


def mdbook_slug(heading):
    h = re.sub(r"[`*]", "", heading.strip().lower()).replace("---", "—").replace("--", "–")
    return "".join("-" if c.isspace() else c for c in h if c.isalnum() or c in "_- " or c.isspace())


def github_slug(heading):
    h = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)
    h = re.sub(r"[`*]", "", h).strip().lower()
    return "".join("-" if c.isspace() else c for c in h if c.isalnum() or c in "_- " or c.isspace())


def heading_anchors(text):
    """{mdbook slug: github slug} for the headings outside code fences."""
    found, fence = {}, False
    seen_m, seen_g = {}, {}
    for line in text.splitlines():
        if line.lstrip().startswith(FENCE):
            fence = not fence
            continue
        m = None if fence else re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
        if m:
            ms, gs = mdbook_slug(m.group(1)), github_slug(m.group(1))
            n, k = seen_m.get(ms, 0), seen_g.get(gs, 0)
            seen_m[ms], seen_g[gs] = n + 1, k + 1
            found[ms if n == 0 else f"{ms}-{n}"] = gs if k == 0 else f"{gs}-{k}"
    return found


def parse_summary(text):
    """-> list of dicts: title, path (relative to src), section, depth."""
    entries, section = [], ""
    for line in text.splitlines():
        h = re.match(r"^#\s+(.+?)\s*$", line)
        if h and not line.startswith("##"):
            section = "" if h.group(1) == "Summary" else h.group(1)
            continue
        m = ENTRY.match(line)
        if m and m.group(3):
            entries.append({"title": m.group(2), "path": m.group(3), "section": section,
                            "depth": len(m.group(1).replace("\t", "  ")) // 2})
            continue
        m = PLAIN.match(line)
        if m:
            entries.append({"title": m.group(1), "path": m.group(2), "section": section, "depth": 0})
    for e in entries:
        e["path"] = os.path.normpath(e["path"]).replace(os.sep, "/")
    return entries


def assign_names(entries):
    """Unique wiki names; the introduction is Home."""
    base = [wiki_name(e["title"]) for e in entries]
    for i, e in enumerate(entries):
        if e["path"] == "introduction.md":
            base[i] = "Home"
    names = []
    for i, e in enumerate(entries):
        n = base[i]
        if n != "Home" and (base.count(n) > 1 or n in ("Home", "_Sidebar", "_Footer")):
            n = wiki_name(f"{e['section']} {e['title']}") if e["section"] else n
        names.append(n)
    seen = {}
    for i, n in enumerate(names):  # last resort: numeric suffix
        k = seen.get(n, 0)
        seen[n] = k + 1
        if k:
            names[i] = f"{n}-{k + 1}"
    return names


def blob_url(repo_rel, is_dir=False):
    return f"{REPO}/{'tree' if is_dir else 'blob'}/{BRANCH}/{repo_rel}"


def convert_page(text, page_rel, src, repo_root, page_of, anchors, problems, label):
    """Rewrite the links of one page. page_rel: path of the page under src."""
    page_dir = (src / page_rel).parent

    def rewrite(m):
        bang, label_text, target, title = m.groups()
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):  # http:, mailto:, ...
            return m.group(0)
        path, _, frag = target.partition("#")
        if not path:  # same-page anchor
            gh = anchors.get(page_rel, {})
            if frag and frag not in gh:
                problems.append(f"{label}: no heading for anchor #{frag}")
                return m.group(0)
            return f"[{label_text}](#{gh[frag] if frag else ''}{title})"
        dest = Path(os.path.normpath(page_dir / path))
        try:
            rel = dest.relative_to(src).as_posix()
        except ValueError:
            rel = None
        if rel is not None and rel in page_of and not bang:
            gh = anchors[rel]
            if frag and frag not in gh:
                problems.append(f"{label}: no heading for anchor {target}")
                return m.group(0)
            return f"[{label_text}]({page_of[rel]}{'#' + gh[frag] if frag else ''}{title})"
        if not dest.exists():
            problems.append(f"{label}: broken link {target}")
            return m.group(0)
        repo_rel = dest.relative_to(repo_root).as_posix() if repo_root in dest.parents else None
        if repo_rel is None:
            problems.append(f"{label}: link leaves the repository: {target}")
            return m.group(0)
        if bang:  # image: serve the raw file
            return f"{bang}[{label_text}]({REPO}/raw/{BRANCH}/{repo_rel}{title})"
        url = blob_url(repo_rel, dest.is_dir())
        return f"[{label_text}]({url}{'#' + frag if frag else ''}{title})"

    out, fence = [], False
    for line in text.split("\n"):
        if line.lstrip().startswith(FENCE):
            fence = not fence
            out.append(line)
        elif fence:
            out.append(line)
        else:
            # Rewrite only the text between inline code spans.
            parts, last = [], 0
            for cm in CODE_SPAN.finditer(line):
                parts.append(LINK.sub(rewrite, line[last:cm.start()]))
                parts.append(cm.group(0))
                last = cm.end()
            parts.append(LINK.sub(rewrite, line[last:]))
            out.append("".join(parts))
    return "\n".join(out)


def sidebar(entries, names):
    lines, section = [], None
    for e, n in zip(entries, names):
        if e["section"] != section:
            section = e["section"]
            if section:
                lines += ["", f"**{section}**", ""]
        lines.append(f"{'  ' * e['depth']}- [{e['title']}]({n})")
    return "\n".join(lines).strip() + "\n"


def footer():
    return (
        "These pages are generated from [`docs/src`]"
        f"({REPO}/tree/{BRANCH}/docs/src) on `{BRANCH}`. "
        "Do not edit them here: changes are overwritten on the next sync. "
        f"Edit the source in the [repository]({REPO}) instead; "
        "the rendered book is on [GitHub Pages](https://lischetzke.github.io/dtfs/).\n"
    )


def build(src, out):
    src, out = Path(src).resolve(), Path(out)
    repo_root = src.parent.parent
    entries = parse_summary((src / "SUMMARY.md").read_text(encoding="utf-8"))
    names = assign_names(entries)
    page_of = {e["path"]: n for e, n in zip(entries, names)}
    texts = {}
    problems = []
    for e in entries:
        f = src / e["path"]
        if not f.is_file():
            problems.append(f"SUMMARY.md: missing page {e['path']}")
            continue
        texts[e["path"]] = f.read_text(encoding="utf-8")
    anchors = {p: heading_anchors(t) for p, t in texts.items()}
    out.mkdir(parents=True, exist_ok=True)
    for p, t in texts.items():
        body = convert_page(t, p, src, repo_root, page_of, anchors, problems, p)
        (out / f"{page_of[p]}.md").write_text(body, encoding="utf-8", newline="\n")
    (out / "_Sidebar.md").write_text(sidebar(entries, names), encoding="utf-8", newline="\n")
    (out / "_Footer.md").write_text(footer(), encoding="utf-8", newline="\n")
    return problems, len(texts)


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    problems, n = build(argv[1], argv[2])
    for p in problems:
        print(p, file=sys.stderr)
    print(f"sync-wiki: {n} page(s), {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
