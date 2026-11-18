# Releasing Pando

1. On a branch, set `version` under `[workspace.package]` in `Cargo.toml` to the new version,
   run `tools/cargo.sh update --workspace` so `Cargo.lock` records it, and merge the pull
   request.
2. Tag the merge commit on main and push the tag:

   ```
   git tag -a v0.1.0 -m "pando 0.1.0"
   git push origin v0.1.0
   ```

3. The [release workflow](../.github/workflows/release.yml) builds `pando` for x86_64 and aarch64
   Linux, fails if the tag does not match the workspace version, and publishes a GitHub release
   with the two archives, `install.sh`, `SHA256SUMS` and a build provenance attestation for each
   file.

Run the workflow by hand from the Actions tab to build the archives of any branch without
publishing them; their version is the workspace version with `-dev.<commit>` appended.

[docs/hosts.md](hosts.md) says how to install a release and upgrade a host to it.
