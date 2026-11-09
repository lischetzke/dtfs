# Contributing to DTFS

Contributions are welcome, from humans and AI agents alike. By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Before you start

- **Rules:** [`CLAUDE.md`](../CLAUDE.md) is the binding contract for every contributor. Part A covers the architecture and the invariants that must not break (data integrity, the LELI anchor, Raft as the only mutation point). Part B covers how a change lands.
- **Workflow:** the [contribution guide](../docs/src/development/contributing.md) explains how a change lands, the commit format and where things live.
- **Bugs and features:** open an issue with the matching template first for anything bigger than a small fix, so the approach can be agreed before you write code.
- **Security problems:** report them privately through a [security advisory](https://github.com/lischetzke/dtfs/security/advisories/new), never as a public issue.

## Making a change

1. Fork the repository and branch from `master`.
2. Write a test that fails without your change. Every fix gets a positive test, and every new guard a negative test that asserts the exact error.
3. Make the smallest change that fixes it.
4. Run the crate tests in the Docker build image (Linux toolchain only, always `--release`):

   ```bash
   docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .
   docker run --rm -v "$(pwd):/build" -v dtfs-target:/target \
     -v dtfs-registry:/usr/local/cargo/registry -w /build \
     dtfs-buildenv:latest cargo test -p <crate> --release
   ```

   On Git Bash for Windows, prefix the command with `MSYS_NO_PATHCONV=1`. For changes that touch more than one crate or the wire format, run `cargo test --workspace --release`.
5. Update the docs in the same commit when you change behaviour, configuration (`crates/dtfs-common/src/config.rs` together with `deploy/examples/` and `docs/src/configuration/`) or anything an operator sees.
6. Commit as `<type>: <summary>` with `<type>` one of `bug`, `feat`, `int`, `docs`. The subject becomes the release-note line.
7. Open a pull request against `master` and fill in the template.

## Licence

DTFS is licensed under the [GNU Affero General Public License v3.0](../LICENSE). By contributing you agree that your contribution is licensed under the same terms.
