## What and why

<!-- What does this change, and which problem does it solve? Link the issue: Fixes #123 -->

## Type

<!-- Keep one. It matches the commit subject prefix and becomes the release-note line. -->

- [ ] `bug`: fixes wrong behaviour
- [ ] `feat`: new capability
- [ ] `int`: internal change, no visible behaviour change
- [ ] `docs`: documentation only

## Tests

<!-- Name the tests and say what they assert. For a fix, confirm the test failed before the fix. -->

- [ ] A positive test covers the intended outcome
- [ ] Every new guard has a negative test that asserts the exact error
- [ ] A bug-fix test fails without the fix
- [ ] `cargo test -p <crate> --release` passes in the Docker build image (workspace suite for cross-crate or wire changes)
- [ ] Runtime-only behaviour was checked on `scripts/dev-cluster.sh`, or the reason it was not is stated below

## Checklist

- [ ] Follows [`CLAUDE.md`](https://github.com/lischetzke/dtfs/blob/master/CLAUDE.md), in particular the integrity rules (§ A.3.3) and Raft as the only mutation point (§ A.3.6)
- [ ] New persisted Raft state has `#[serde(default)]` and an old-snapshot-loads test (§ A.3.1)
- [ ] Config changes update `deploy/examples/` and `docs/src/configuration/` together
- [ ] Behaviour visible to operators is documented (`docs/src/`, `CLAUDE.md` where it applies)
- [ ] No secrets, tokens, HMACs or chunk bytes in logs
- [ ] Breaking changes or required operator steps are described below

## Notes for the reviewer

<!-- Risks, things you did not test, follow-up work. -->
