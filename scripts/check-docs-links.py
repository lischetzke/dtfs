#!/usr/bin/env python3
"""Check docs/src: relative Markdown links resolve (including #anchors against
the target page's headings, slugged the way mdBook does), SUMMARY.md entries
exist, every page is listed in SUMMARY.md.

Usage: check-docs-links.py [docs/src]   (exit 1 on any problem)
"""
import re
import sys
from pathlib import Path

src = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/src").resolve()
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
problems = []


def slug(heading):
    # mdBook id_from_content: lowercase, whitespace -> "-", keep alnum _ -.
    # Smart punctuation runs first: "--" and "---" become dashes that are dropped.
    h = re.sub(r"[`*]", "", heading.strip().lower()).replace("---", "\u2014").replace("--", "\u2013")
    return "".join("-" if c.isspace() else c for c in h if c.isalnum() or c in "_- " or c.isspace())


_anchors = {}


def anchors_of(path):
    if path not in _anchors:
        found, fence = set(), False
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.lstrip().startswith(("```", "~~~")):
                fence = not fence
            elif not fence:
                m = re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
                if m:
                    found.add(slug(m.group(1)))
        _anchors[path] = found
    return _anchors[path]


summary = (src / "SUMMARY.md").read_text(encoding="utf-8")
listed = set()
for target in LINK.findall(summary):
    f = (src / target.split("#")[0]).resolve()
    listed.add(f)
    if not f.is_file():
        problems.append(f"SUMMARY.md: missing page {target}")

for md in sorted(src.rglob("*.md")):
    if md.name != "SUMMARY.md" and md.resolve() not in listed:
        problems.append(f"{md.relative_to(src)}: not listed in SUMMARY.md")
    in_fence = False
    for n, line in enumerate(md.read_text(encoding="utf-8").splitlines(), 1):
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        line = re.sub(r"`[^`]*`", "", line)  # ignore inline code
        for target in LINK.findall(line):
            if re.match(r"^([a-z]+:|mailto:)", target):
                continue
            path, _, frag = target.partition("#")
            dest = (md.parent / path).resolve() if path else md
            if not dest.exists():
                problems.append(f"{md.relative_to(src)}:{n}: broken link {target}")
            elif frag and dest.suffix == ".md" and frag not in anchors_of(dest):
                problems.append(f"{md.relative_to(src)}:{n}: no heading for anchor {target}")

for p in problems:
    print(p, file=sys.stderr)
print(f"docs link check: {len(problems)} problem(s)")
sys.exit(1 if problems else 0)
