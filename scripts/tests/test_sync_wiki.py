"""Unit tests for scripts/sync-wiki.py (python3 -m unittest, stdlib only)."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("sync_wiki", Path(__file__).resolve().parents[1] / "sync-wiki.py")
sw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sw)

SUMMARY = """# Summary

[Introduction](./introduction.md)

# Alpha

- [Overview](./a/overview.md)
  - [Deep Dive: A & B](./a/deep.md)

# Beta

- [Overview](./b/overview.md)
"""


def make(tmp, pages):
    root = Path(tmp)
    src = root / "docs" / "src"
    for rel, text in pages.items():
        f = src / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text, encoding="utf-8")
    (root / "scripts").mkdir()
    (root / "scripts" / "x.sh").write_text("#!/bin/sh\n")
    return src, root / "out"


BASE = {
    "SUMMARY.md": SUMMARY,
    "introduction.md": "# Intro\nSee [ov](a/overview.md#the-heading) and [b](./b/overview.md).\n",
    "a/overview.md": "# Alpha Overview\n## The Heading\n"
                     "[deep](deep.md) [self](#the-heading) [script](../../../scripts/x.sh) "
                     "[skipped](../skipped.md#frag) [ext](https://example.com/a.md) [claude](../../../scripts)\n",
    "a/deep.md": "# Deep\n```\n[no](rewrite.md)\n```\n`[no](rewrite.md)` [yes](../b/overview.md)\n",
    "b/overview.md": "# Beta Overview\n",
    "skipped.md": "# Skipped\n## frag\n",
}


class SyncWiki(unittest.TestCase):
    def run_build(self, pages):
        with tempfile.TemporaryDirectory() as tmp:
            src, out = make(tmp, pages)
            problems, n = sw.build(src, out)
            files = {p.name: p.read_text(encoding="utf-8") for p in out.iterdir()}
        return problems, files

    def test_names_collisions_and_special_pages(self):
        problems, files = self.run_build(BASE)
        self.assertEqual(problems, [])
        self.assertEqual(
            sorted(files),
            ["Alpha-Overview.md", "Beta-Overview.md", "Deep-Dive-A-B.md", "Home.md", "_Footer.md", "_Sidebar.md"],
        )

    def test_internal_links_and_anchors(self):
        _, files = self.run_build(BASE)
        self.assertIn("[ov](Alpha-Overview#the-heading)", files["Home.md"])
        self.assertIn("[b](Beta-Overview)", files["Home.md"])
        self.assertIn("[deep](Deep-Dive-A-B)", files["Alpha-Overview.md"])
        self.assertIn("[self](#the-heading)", files["Alpha-Overview.md"])

    def test_repo_and_skipped_links_become_blob_urls(self):
        _, files = self.run_build(BASE)
        page = files["Alpha-Overview.md"]
        self.assertIn("[script](https://github.com/lischetzke/dtfs/blob/master/scripts/x.sh)", page)
        self.assertIn("[skipped](https://github.com/lischetzke/dtfs/blob/master/docs/src/skipped.md#frag)", page)
        self.assertIn("[claude](https://github.com/lischetzke/dtfs/tree/master/scripts)", page)
        self.assertIn("[ext](https://example.com/a.md)", page)

    def test_code_is_untouched(self):
        _, files = self.run_build(BASE)
        deep = files["Deep-Dive-A-B.md"]
        self.assertEqual(deep.count("[no](rewrite.md)"), 2)
        self.assertIn("[yes](Beta-Overview)", deep)

    def test_sidebar_nesting_and_sections(self):
        _, files = self.run_build(BASE)
        side = files["_Sidebar.md"]
        self.assertIn("**Alpha**", side)
        self.assertIn("- [Overview](Alpha-Overview)\n  - [Deep Dive: A & B](Deep-Dive-A-B)", side)
        self.assertNotIn("Summary", side)
        self.assertIn("generated", files["_Footer.md"])

    def test_broken_link_and_anchor_are_reported(self):
        pages = dict(BASE)
        pages["b/overview.md"] = "# Beta Overview\n[gone](nope.md) [bad](../a/overview.md#missing)\n"
        problems, _ = self.run_build(pages)
        self.assertEqual(len(problems), 2)
        self.assertTrue(any("broken link nope.md" in p for p in problems))
        self.assertTrue(any("no heading for anchor ../a/overview.md#missing" in p for p in problems))

    def test_wiki_name(self):
        self.assertEqual(sw.wiki_name("Log Shipping (Filebeat → Elastic)"), "Log-Shipping-Filebeat-Elastic")
        self.assertEqual(sw.wiki_name("Chaos / Cross-DC: Test?"), "Chaos-Cross-DC-Test")

    def test_github_anchor_keeps_double_dash(self):
        self.assertEqual(sw.github_slug("a -- b"), "a----b")
        self.assertEqual(sw.mdbook_slug("a -- b"), "a--b")


if __name__ == "__main__":
    unittest.main()
