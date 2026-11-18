# Fuzz targets for pando-format

Every byte Pando reads from the object store is decoded by `pando-format`. These
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets feed those decoders untrusted
input and check a property of the result; a decode error is fine, a panic or a failed
assertion is a bug. They need a nightly toolchain, pinned here to `nightly-2026-11-08`, and
cargo-fuzz 0.13.2.

## Running one target

The host has no Rust, so run it in the same `rust:1` container as `tools/cargo.sh`. From the
repository root, for target `node` and 60 seconds:

```sh
docker run --rm \
  -v pando-cargo:/usr/local/cargo/registry -v pando-rustup:/usr/local/rustup \
  -v pando-fuzz-target:/target -v pando-fuzz-bin:/fuzzbin \
  -e CARGO_TARGET_DIR=/target -e CARGO_INSTALL_ROOT=/fuzzbin \
  -v "$PWD":/w -w /w/fuzz rust:1 sh -c '
    rustup toolchain install nightly-2026-11-08 --profile minimal &&
    (test -x /fuzzbin/bin/cargo-fuzz || cargo install cargo-fuzz --locked --version 0.13.2) &&
    mkdir -p corpus/node &&
    PATH=/fuzzbin/bin:$PATH cargo +nightly-2026-11-08 fuzz run node corpus/node seeds/node -- -max_total_time=60'
```

`corpus/<target>` (ignored by git) collects the inputs the fuzzer finds; `seeds/<target>` holds
a few valid encodings made from the golden fixtures in `pando-format/tests/fixtures`. The
structured targets (`frame_builder`, `change_list_encode`, `recipe_encode`) have no seeds.
To replay one input, pass its file instead of the corpus directories, for example
`cargo +nightly-2026-11-08 fuzz run node regressions/node/<file>`. Crashes land in
`artifacts/<target>/` (ignored); a minimised reproducer of each bug found so far is kept in
`regressions/<target>/`.

## Targets

**frame**: the first byte picks which hashes to keep, the rest is a frame. If
`decode_frame(z, None)` succeeds, `decode_frame(z, Some(keep))` must succeed too, return only
objects and recipes the whole decode returned (byte for byte), and return every kept one.
Re-encoding the whole decode with `FrameBuilder` and decoding it again gives the same objects
and recipes.

**frame_builder**: structured. Pushes arbitrary objects into a `FrameBuilder` (correct and
wrong hashes, recipes naming other objects of the frame as sources) and checks the offsets
`push` returns, then that whole and partial decodes return exactly the expected objects, with
the sources of kept recipes, or an integrity error when a returned object has a wrong hash.

**node**: `Node::decode` accepts only canonical encodings: decode, encode, decode gives the
same node, and re-encoding a decoded node gives back the input bytes. A node is named by the
hash of its encoding, so a second encoding of the same node is a bug.

**index**: `SegmentIndex::decode` accepts only canonical encodings: re-encoding gives back the
input bytes.

**change_list**: if `ChangeList::decode` succeeds, its changes are sorted by chunk index, the
re-encoding has the input's length and keeps its header and node records byte for byte, and
decoding the re-encoding gives the same change list.

**change_list_encode**: structured. Encodes an arbitrary `ChangeList`, checks the encoded
length, and checks that decoding gives the same list with its changes stably sorted by index,
and that re-encoding an already sorted list gives the same bytes.

**recording**: if `Recording::decode` succeeds, the re-encoding has the input's length, decodes
to the same recording, and is byte for byte the input when every node flag in it is 0 or 1.

**recipe**: if `Recipe::parse` succeeds, `Recipe::sources_of` returns the same sources;
`rebuild` produces the chunk a straightforward model of the runs describes and checks its
hash; a failing or short source makes `rebuild` fail rather than panic; and encoding the
rebuilt chunk with `Recipe::encode` from the same sources and picks parses back, rebuilds the
same chunk and is a fixpoint of re-encoding.

**recipe_encode**: structured. Encodes a chunk from arbitrary picks over up to
`RECIPE_SOURCES_MAX` sources and checks that the recipe parses back to the same sources, that
its runs tile the chunk, match the picks and are maximally merged, that its length is what the
runs say, and that `rebuild` returns the chunk.

**reference**: if `Ref::decode` succeeds, decoding its encoding gives the same ref.

**entry**: the 60-byte binary entry form round trips exactly through `Entry::from_bytes` and
`to_bytes`; if `Entry::parse_text` accepts a string, parsing its printed form gives the same
entry.

**keys**: object key names from a store listing. When `keys::parse_segment` or
`keys::parse_change_list` accepts a name, the key built from the result parses back to it; when
`Hash` or `SegId` parse a string, their printed form parses back to the same value.

## Regressions

Each file in `regressions/` still fails against the current `pando-format`:

- `node/zero_hash_with_location`, `node/empty_record`: `Node::decode` accepts a child record
  whose hash is zero; `Node::encode` drops it, so the node has a second encoding.
- `recipe/255_sources`: `Recipe::parse` accepts 255 sources, one more than
  `RECIPE_SOURCES_MAX`, so `Recipe::encode` panics re-encoding it.
- `frame/huge_content_size`: a 61-byte frame whose zstd header claims a 1 TiB content size
  makes a partial `decode_frame` allocate about 6 GiB for the payload before any of it is read.
