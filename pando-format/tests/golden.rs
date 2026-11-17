//! Checks the codecs against the byte fixtures in `tests/fixtures`, so the encoding of every
//! stored object stays fixed. With PANDO_RUST_OUT set, also writes the encodings there, so
//! another decoder can check them.

#![allow(clippy::disallowed_methods, reason = "the tests read the fixture files")]

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;

use bytes::Bytes;
use pando_format::*;

struct Golden {
    dir: PathBuf,
    manifest: HashMap<String, String>,
}

impl Golden {
    fn load() -> Golden {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let text = std::fs::read_to_string(dir.join("manifest.txt")).expect("tests/fixtures/manifest.txt");
        let manifest = text
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Golden { dir, manifest }
    }

    fn file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
    }

    fn get(&self, key: &str) -> &str {
        self.manifest
            .get(key)
            .unwrap_or_else(|| panic!("manifest has no {key}"))
    }

    fn hash(&self, key: &str) -> Hash {
        self.get(key).parse().unwrap()
    }
}

fn golden_seg(n: u8) -> SegId {
    SegId(std::array::from_fn(|i| n + i as u8))
}

fn rust_out(name: &str, b: &[u8]) {
    if let Some(dir) = std::env::var_os("PANDO_RUST_OUT") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(PathBuf::from(dir).join(name), b).unwrap();
    }
}

#[test]
fn entries_and_refs_are_byte_exact() {
    let g = Golden::load();
    let a = g.file("chunk_a.bin");
    assert_eq!(Hash::of(&a), g.hash("chunk_a.hash"));
    let ea = Entry::parse_text(g.get("entry_a")).unwrap();
    assert_eq!(ea.loc.seg, golden_seg(0x10));
    assert_eq!(ea.to_bytes().as_slice(), g.file("entry_a.bin").as_slice());

    let r = Ref::decode(&g.file("ref.txt")).unwrap();
    assert_eq!(
        (r.size, r.generation, r.root, r.from.as_deref()),
        (1 << 30, 7, ea, Some("main"))
    );
    assert_eq!(r.encode(), g.file("ref.txt"));
    rust_out("ref.txt", &r.encode());
    let r = Ref::decode(&g.file("ref_nofrom.txt")).unwrap();
    assert_eq!((r.size, r.root, r.from.as_deref()), (4096, Entry::EMPTY, None));
    assert_eq!(r.encode(), g.file("ref_nofrom.txt"));
}

#[test]
fn the_store_format_marker_is_byte_exact() {
    let g = Golden::load();
    let b = g.file("store_format.txt");
    assert_eq!(StoreFormat::decode(&b).unwrap(), StoreFormat::CURRENT);
    assert_eq!(StoreFormat::CURRENT.encode(), b);
    rust_out("store_format.txt", &StoreFormat::CURRENT.encode());
}

#[test]
fn writer_ids_match() {
    let g = Golden::load();
    let w = WriterId::new("main", 5);
    assert_eq!(hex::encode(w.branch), g.get("writer_main_branch"));
    assert_eq!(w.generation.to_string(), g.get("writer_main_gen"));
    assert_eq!(
        golden_seg(0x40).writer().generation.to_string(),
        g.get("seg_writer_gen")
    );
}

#[test]
fn nodes_and_depths_are_byte_exact() {
    let g = Golden::load();
    let b = g.file("node.bin");
    let n = Node::decode(&b).unwrap();
    assert_eq!(n.encode(), b);
    assert_eq!(Hash::of(&b), g.hash("node.hash"));
    assert_eq!(n.children().map(|(s, _)| s).collect::<Vec<_>>(), vec![0, 7, 255]);
    rust_out("node.bin", &n.encode());
    assert_eq!(Node::new().encode(), g.file("node_empty.bin"));
    for (k, v) in g.manifest.iter().filter(|(k, _)| k.starts_with("depth_for.")) {
        let chunks: u64 = k["depth_for.".len()..].parse().unwrap();
        assert_eq!(depth_for(chunks).to_string(), *v, "depth for {chunks} chunks");
    }
    for name in g.get("tree_from.nodes").split(',') {
        let b = g.file(&format!("tree_from.{name}.bin"));
        assert_eq!(Hash::of(&b).to_string(), name);
        assert_eq!(Node::decode(&b).unwrap().encode(), b);
    }
}

#[test]
fn block_keys_match_xxh3() {
    let g = Golden::load();
    assert_eq!(BlockKey::zero().0.to_string(), g.get("block_key_zero"));
    let keys = BlockKeys::of(&g.file("chunk_a.bin"));
    let raw: Vec<u8> = keys.0.iter().flat_map(|k| k.0.to_le_bytes()).collect();
    assert_eq!(raw, g.file("block_keys_a.bin"));
}

#[test]
fn recipes_rebuild_and_encode_byte_exact() {
    let g = Golden::load();
    let a = Bytes::from(g.file("chunk_a.bin"));
    let enc = g.file("recipe_b.bin");
    let r = Recipe::parse(&enc).unwrap();
    let ea = Entry::parse_text(g.get("entry_a")).unwrap();
    assert_eq!(r.sources, vec![ea]);
    let b = r
        .rebuild(g.hash("chunk_b.hash"), |_| Ok::<_, Infallible>(a.clone()))
        .unwrap();

    let mut picks = [Pick::Zero; BLOCKS_PER_CHUNK];
    for run in &r.runs {
        for k in 0..run.n {
            picks[usize::from(run.dst + k)] = match run.src {
                0xff => Pick::Literal,
                0xfe => Pick::Zero,
                src => Pick::Source {
                    src,
                    block: run.src_block + k,
                },
            };
        }
    }
    let again = Recipe::encode(&r.sources, &picks, &b);
    assert_eq!(again, enc);
    rust_out("recipe_b.bin", &again);
}

#[test]
fn go_frames_decode_whole_and_in_part() {
    let g = Golden::load();
    let z = g.file("frame.zst");
    let (ha, hb, hs) = (
        g.hash("chunk_a.hash"),
        g.hash("chunk_b.hash"),
        g.hash("small.hash"),
    );
    let d = decode_frame(&z, None).unwrap();
    assert_eq!(&d.objects[&ha][..], g.file("chunk_a.bin").as_slice());
    assert_eq!(&d.objects[&hs][..], b"hello pando");
    assert_eq!(&d.recipes[&hb][..], g.file("recipe_b.bin").as_slice());

    let only_small = decode_frame(&z, Some(&|h: &Hash| *h == hs)).unwrap();
    assert_eq!((only_small.objects.len(), only_small.recipes.len()), (1, 0));
    let recipe_and_source = decode_frame(&z, Some(&|h: &Hash| *h == hb)).unwrap();
    assert!(recipe_and_source.recipes.contains_key(&hb));
    assert!(
        recipe_and_source.objects.contains_key(&ha),
        "a kept recipe brings its source in the frame"
    );

    let mut f = FrameBuilder::default();
    for i in 0..3 {
        let parts: Vec<&str> = g.get(&format!("frame.obj.{i}")).split(' ').collect();
        let h: Hash = parts[0].parse().unwrap();
        let bytes = if parts[3] == "true" {
            d.recipes[&h].clone()
        } else {
            d.objects[&h].bytes().clone()
        };
        let (inner, len) = f.push(h, &bytes, parts[3] == "true");
        assert_eq!(
            (inner.to_string(), len.to_string()),
            (parts[1].to_string(), parts[2].to_string())
        );
    }
    let ours = f.finish().unwrap();
    assert_eq!(decode_frame(&ours, None).unwrap().objects.len(), 2);
    rust_out("frame.zst", &ours);
}

#[test]
fn segment_indexes_are_byte_exact() {
    let g = Golden::load();
    let b = g.file("index.bin");
    let i = SegmentIndex::decode(&b).unwrap();
    assert_eq!(i.frames, vec![(0, 100), (100, 250), (350, 1000)]);
    assert_eq!(i.deps, vec![golden_seg(0x10)]);
    assert_eq!(i.encode(), b);
    rust_out("index.bin", &i.encode());
}

#[test]
fn change_lists_are_byte_exact() {
    let g = Golden::load();
    let b = g.file("change_list.bin");
    let cl = ChangeList::decode(&b).unwrap();
    assert_eq!(cl.depth.to_string(), g.get("change_list.depth"));
    assert_eq!(cl.changes.len().to_string(), g.get("change_list.changes"));
    assert_eq!(
        cl.changes.iter().map(|c| c.idx).collect::<Vec<_>>(),
        vec![1, 70000, 300000]
    );
    assert!(cl.changes[0].keys.is_none() && cl.changes[1].entry.is_empty());
    assert_eq!(cl.changes[2].keys, Some(BlockKeys::of(&g.file("chunk_a.bin"))));
    assert_eq!(cl.encode(), b);
    rust_out("change_list.bin", &cl.encode());
}
