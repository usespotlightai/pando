# Releasing Pando

Releases use semantic versioning (`v0.4.1`) and come from
[release-please](https://github.com/googleapis/release-please) on `main`, as in the other
repositories of the organization:

- **Commits:** every commit on `main` is a Conventional Commit. `feat` and `fix` make a release;
  `perf` and `revert` appear in the changelog too; `docs`, `chore`, `ci`, `test`, `refactor` and
  `build` do not make one. `feat!` or a `BREAKING CHANGE:` footer marks a breaking change, which
  before 1.0 raises the minor version.
- **Release pull request:** after each push to `main`, release-please updates one open pull
  request, `chore: release <version>`, with the next version in `Cargo.toml`, `Cargo.lock` and
  `fuzz/Cargo.lock`, and the version's `CHANGELOG.md` entries. It is opened with the
  infrastructure app's token (`INFRA_APP_ID`, `INFRA_PRIVATE_KEY`), so the required checks run on
  it as on any other pull request.
- **Publishing:** merging the release pull request tags the commit and creates a draft release.
  The [release workflow](../.github/workflows/release.yml) then builds `pando` for x86_64 and
  aarch64 Linux, fails if the binary's version is not the tag's, uploads the two archives,
  `install.sh` and `SHA256SUMS` to the draft with a build provenance attestation for each file,
  and publishes it.

Run the workflow by hand from the Actions tab to build the archives of any branch without
publishing them; their version is the workspace version with `-dev.<commit>` appended. Pull
requests that change the packaging build them too.

[docs/hosts.md](hosts.md) says how to install a release and upgrade a host to it.

## Using a release from another repository

A runner image or a Terraform module pins one release and downloads the archive for its
architecture, `pando-<version>-<arch>-linux.tar.gz` with `<arch>` `x86_64` or `aarch64`, from
`https://github.com/usespotlightai/pando/releases/download/v<version>/`. Check it against that
release's `SHA256SUMS` (`sha256sum -c --ignore-missing SHA256SUMS`), or its provenance with
`gh attestation verify <archive> --repo usespotlightai/pando`. The archive holds `pando` and the
systemd unit under `pando-<version>-<arch>-linux/`.

The aarch64 binary is built for generic aarch64, so it runs on every Graviton generation and on
AgentCore Runtime. Both binaries need glibc 2.39 or newer: Debian 13, Ubuntu 24.04 or later, not
Amazon Linux 2023, which has glibc 2.34.
