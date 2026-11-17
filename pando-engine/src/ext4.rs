//! The chunks that hold an ext4 file system's metadata, found from the image itself.
//!
//! A cold `git status` or `find` reads the block and inode bitmaps, the used part of every
//! inode table, and every directory block. Attach fetches those before the workload asks, and
//! import packs them first, so they share few frames.

use std::collections::BTreeSet;

use pando_format::{CHUNK_SIZE, Entry};

use crate::cache::Cache;
use crate::tree::Tree;
use crate::{Error, Result};

const SUPER_MAGIC: u16 = 0xEF53;
const EXTENT_MAGIC: u16 = 0xF30A;
const INCOMPAT_META_BG: u32 = 0x10;
const INCOMPAT_64BIT: u32 = 0x80;
const RO_COMPAT_GDT_CSUM: u32 = 0x10;
const RO_COMPAT_METADATA_CSUM: u32 = 0x400;
const BG_INODE_UNINIT: u16 = 1;
const S_IFMT: u16 = 0xF000;
const S_IFDIR: u16 = 0x4000;
const EXTENTS_FL: u32 = 0x80000;
const MAX_EXTENT_DEPTH: usize = 5;
/// An extent longer than this is unwritten; its length is the excess.
const EXTENT_INIT_MAX: u64 = 32768;

fn u16_at(b: &[u8], at: usize) -> u16 {
    b.get(at..at + 2).map_or(0, |s| u16::from_le_bytes([s[0], s[1]]))
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    b.get(at..at + 4)
        .map_or(0, |s| u32::from_le_bytes(s.try_into().expect("4 bytes")))
}

/// A byte range of the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Span {
    off: u64,
    len: u64,
}

fn chunks(spans: &[Span]) -> Vec<u64> {
    let c = CHUNK_SIZE as u64;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for s in spans.iter().filter(|s| s.len > 0) {
        for i in s.off / c..=(s.off + s.len - 1) / c {
            if seen.insert(i) {
                out.push(i);
            }
        }
    }
    out
}

/// An image whose ext4 metadata [`metadata`] finds.
pub(crate) trait Image {
    /// Reads `len` bytes at `off`.
    async fn read(&mut self, off: u64, len: u64) -> Result<Vec<u8>>;
    /// Takes one wave of chunk indexes that hold metadata.
    async fn wave(&mut self, idxs: Vec<u64>) -> Result<()>;
}

/// Finds the chunks holding ext4 metadata in two waves: bitmaps and inode tables first, then
/// the directory blocks found by scanning those inode tables. The first wave comes before the
/// scan reads the tables, so it can fetch them in one batch.
pub(crate) async fn metadata(img: &mut impl Image) -> Result<()> {
    let not = |why: &str| Error::Corrupt(format!("ext4 metadata: {why}"));
    let sb = img.read(1024, 1024).await?;
    if u16_at(&sb, 56) != SUPER_MAGIC {
        return Err(not("not ext4"));
    }
    let (incompat, ro_compat) = (u32_at(&sb, 96), u32_at(&sb, 100));
    if incompat & INCOMPAT_META_BG != 0 {
        return Err(not("the meta_bg layout"));
    }
    let bs = 1024u64 << u32_at(&sb, 24).min(16);
    let mut blocks = u32_at(&sb, 4) as u64;
    let mut desc = 32u64;
    if incompat & INCOMPAT_64BIT != 0 {
        blocks |= (u32_at(&sb, 0x150) as u64) << 32;
        desc = u16_at(&sb, 0xFE) as u64;
    }
    let (first, per_group) = (u32_at(&sb, 20) as u64, u32_at(&sb, 32) as u64);
    let (inodes_per, inode_size) = (u32_at(&sb, 40) as u64, u16_at(&sb, 88) as u64);
    if per_group == 0 || desc < 32 || inode_size < 128 || blocks <= first {
        return Err(not("a bad superblock"));
    }
    let unused_valid = ro_compat & (RO_COMPAT_GDT_CSUM | RO_COMPAT_METADATA_CSUM) != 0;
    let groups = (blocks - first).div_ceil(per_group);
    let gdt = img.read((first + 1) * bs, groups * desc).await?;
    let mut meta = Vec::new();
    let mut tables = Vec::new();
    for g in 0..groups as usize {
        let d = &gdt[(g * desc as usize).min(gdt.len())..];
        let lohi = |lo: usize, hi: usize| {
            let x = u32_at(d, lo) as u64;
            if desc >= 64 {
                x | (u32_at(d, hi) as u64) << 32
            } else {
                x
            }
        };
        meta.push(Span {
            off: lohi(0, 0x20) * bs,
            len: bs,
        });
        meta.push(Span {
            off: lohi(4, 0x24) * bs,
            len: bs,
        });
        if u16_at(d, 0x12) & BG_INODE_UNINIT != 0 {
            continue;
        }
        let mut n = inodes_per;
        if unused_valid {
            let mut unused = u16_at(d, 0x1C) as u64;
            if desc >= 64 {
                unused |= (u16_at(d, 0x32) as u64) << 16;
            }
            n -= unused.min(n);
        }
        if n > 0 {
            let s = Span {
                off: lohi(8, 0x28) * bs,
                len: (n * inode_size).div_ceil(bs) * bs,
            };
            tables.push(s);
            meta.push(s);
        }
    }
    img.wave(chunks(&meta)).await?;
    let mut dirs = Vec::new();
    for t in &tables {
        let table = img.read(t.off, t.len).await?;
        for ino in table.chunks_exact(inode_size as usize) {
            if u16_at(ino, 0) & S_IFMT != S_IFDIR || u16_at(ino, 26) == 0 || u32_at(ino, 32) & EXTENTS_FL == 0
            {
                continue;
            }
            let mut stack = vec![(ino[40..100].to_vec(), 0)];
            while let Some((node, depth)) = stack.pop() {
                if node.len() < 12 || u16_at(&node, 0) != EXTENT_MAGIC || depth > MAX_EXTENT_DEPTH {
                    continue;
                }
                let (entries, leaf) = (u16_at(&node, 2) as usize, u16_at(&node, 6) == 0);
                for x in node[12..].as_chunks::<12>().0.iter().take(entries) {
                    if leaf {
                        let mut len = u16_at(x, 4) as u64;
                        if len > EXTENT_INIT_MAX {
                            len -= EXTENT_INIT_MAX;
                        }
                        let start = (u16_at(x, 6) as u64) << 32 | u32_at(x, 8) as u64;
                        dirs.push(Span {
                            off: start * bs,
                            len: len * bs,
                        });
                    } else {
                        let child = u32_at(x, 4) as u64 | (u16_at(x, 8) as u64) << 32;
                        stack.push((img.read(child * bs, bs).await?, depth + 1));
                    }
                }
            }
        }
    }
    dirs.sort();
    img.wave(chunks(&dirs)).await
}

/// Reads `len` bytes at `off` of the tree at `root` without recording the read as a first
/// read, with holes as zeros.
async fn read_tree(cache: &Cache, tree: &Tree, root: &Entry, off: u64, len: u64) -> Result<Vec<u8>> {
    let c = CHUNK_SIZE as u64;
    let mut out = vec![0u8; len as usize];
    let mut pos = off;
    while pos < off + len {
        let (idx, at) = (pos / c, (pos % c) as usize);
        let n = (CHUNK_SIZE - at).min((off + len - pos) as usize);
        let e = tree.lookup(root, idx, true).await?;
        if !e.is_empty() {
            let b = cache.get_background(&e).await?;
            let o = (pos - off) as usize;
            let have = b.len().saturating_sub(at).min(n);
            out[o..o + have].copy_from_slice(&b[at..at + have]);
        }
        pos += n as u64;
    }
    Ok(out)
}

struct TreeImage<'a> {
    cache: &'a Cache,
    tree: &'a Tree,
    root: &'a Entry,
    parallel: usize,
    frames: usize,
}

impl Image for TreeImage<'_> {
    async fn read(&mut self, off: u64, len: u64) -> Result<Vec<u8>> {
        read_tree(self.cache, self.tree, self.root, off, len).await
    }

    async fn wave(&mut self, idxs: Vec<u64>) -> Result<()> {
        let mut es = Vec::with_capacity(idxs.len());
        for i in idxs {
            let e = self.tree.lookup(self.root, i, true).await?;
            if !e.is_empty() {
                es.push(e);
            }
        }
        self.frames += self.cache.fetch_entries(&es, self.parallel).await;
        Ok(())
    }
}

/// Fetches the ext4 metadata of the tree at `root` with `parallel` fetches. Returns how many
/// frames held it.
pub(crate) async fn prefetch(cache: &Cache, tree: &Tree, root: &Entry, parallel: usize) -> Result<usize> {
    let frames = tree.prefetch_nodes(root, parallel).await?;
    let mut img = TreeImage {
        cache,
        tree,
        root,
        parallel,
        frames,
    };
    metadata(&mut img).await?;
    Ok(img.frames)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const BS: usize = 4096;
    const DIR_BLOCK: u64 = 3000;

    fn put16(b: &mut [u8], at: usize, v: u16) {
        b[at..at + 2].copy_from_slice(&v.to_le_bytes());
    }

    fn put32(b: &mut [u8], at: usize, v: u32) {
        b[at..at + 4].copy_from_slice(&v.to_le_bytes());
    }

    /// One group of 4 KiB blocks: bitmaps at blocks 2 and 3, the inode table at 4 with two
    /// used inodes, and the root directory's one block at [`DIR_BLOCK`].
    pub(crate) fn image() -> Vec<u8> {
        let mut img = vec![0u8; 4096 * BS];
        let sb = &mut img[1024..2048];
        put32(sb, 4, 4096);
        put32(sb, 20, 0);
        put32(sb, 24, 2);
        put32(sb, 32, 32768);
        put32(sb, 40, 16);
        put16(sb, 56, SUPER_MAGIC);
        put16(sb, 88, 256);
        put32(sb, 100, RO_COMPAT_GDT_CSUM);
        let gd = &mut img[BS..BS + 32];
        put32(gd, 0, 2);
        put32(gd, 4, 3);
        put32(gd, 8, 4);
        put16(gd, 0x1C, 14);
        let root = &mut img[4 * BS + 256..4 * BS + 512];
        put16(root, 0, 0x41ED);
        put16(root, 26, 2);
        put32(root, 32, EXTENTS_FL);
        put16(root, 40, EXTENT_MAGIC);
        put16(root, 42, 1);
        put16(root, 44, 4);
        put16(root, 52 + 4, 1);
        put32(root, 52 + 8, DIR_BLOCK as u32);
        img[DIR_BLOCK as usize * BS] = 2;
        img
    }

    struct Bytes {
        img: Vec<u8>,
        waves: Vec<Vec<u64>>,
    }

    impl Image for Bytes {
        async fn read(&mut self, off: u64, len: u64) -> Result<Vec<u8>> {
            Ok(self.img[off as usize..(off + len) as usize].to_vec())
        }

        async fn wave(&mut self, idxs: Vec<u64>) -> Result<()> {
            self.waves.push(idxs);
            Ok(())
        }
    }

    #[tokio::test]
    async fn bitmaps_and_tables_come_before_directory_blocks() {
        let mut b = Bytes {
            img: image(),
            waves: Vec::new(),
        };
        metadata(&mut b).await.unwrap();
        assert_eq!(
            b.waves,
            vec![vec![0], vec![DIR_BLOCK * BS as u64 / CHUNK_SIZE as u64]]
        );
    }

    #[tokio::test]
    async fn an_image_without_ext4_is_refused() {
        let mut b = Bytes {
            img: vec![0u8; 8192],
            waves: Vec::new(),
        };
        assert!(metadata(&mut b).await.is_err());
    }
}
