<!--
Body of the `release vX.Y.Z` commit. CI (scripts/ci/release-notes.sh) puts
`Welcome to DTFS vX.Y.Z!` in front, appends the Upgrading block and the
git-cliff changelog, and drops the commit subject and the trailing trailer
paragraph. Delete this comment and every section you do not need.

Style
- Present tense, second person. No marketing words.
- Name settings exactly (`[write] async_commit`). Give numbers only from real
  test-cluster runs, with the setup.
- Scope suffix on each highlight: fuse | data | manager | raft | cli |
  installer | webui (comma-separated when several).
- A patch release may be a summary paragraph only, or have no body at all.
- Write the message to a file and commit with `git commit -F <file>`; `-F`
  keeps the `#` lines. In an editor (`-e`, or no `-F`) git strips `#` lines
  unless you add `--cleanup=verbatim`.
- Trailers (Co-Authored-By, Claude-Session, ...) stay last, in one paragraph
  after a blank line. A last paragraph made only of `Token: value` lines is
  removed from the notes. The `Also:` line below is not treated as a trailer.
- Do not paste the changelog; "What's Changed" is generated.
-->
release vX.Y.Z

What the release is about, in two or three sentences. On the test cluster
(setup), measured result before -> after.

> [!WARNING]
> **Update required: managers first (followers, then the leader), then data nodes, then clients.**
> - Rollout flags to flip once every node runs vX.Y.Z, and where (`[cluster] flag = value`).
> - Version gates: which release is the new no-rollback floor and why.
> - Config changes that need a look before the update.

## Known issues

- What does not work yet, who is affected, the workaround.

## Highlights

- First highlight title (fuse)
- Second highlight title (manager, raft)

### First highlight title (fuse)

The problem before, in the user's terms. What changes. How to use it: the
settings, their defaults, and what to raise or lower for which case.

### Second highlight title (manager, raft)

Same shape.

Also: small removals or fixes that need one line each.

Co-Authored-By: Name <noreply@example.com>
Claude-Session: https://example.invalid/session
