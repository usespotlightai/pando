//! Base images: one complete image of a published root per host, built once. Every branch opened
//! at that root starts as a clone of it that shares its disk blocks, reads its unchanged chunks
//! from it ([`BaseView`]), and uploads rewritten chunks as recipes of its blocks.
//!
//! Next to `<root>.img` lie `<root>.img.blocks` (its block map), `<root>.img.nodes` (a node pack
//! of the root's tree) and `<root>.img.used` (when a branch last used it). The image appears
//! under its name only once it and both files are complete.

mod build;
pub(crate) mod follow;
mod host_blocks;
mod node_pack;
mod view;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub use build::BuildReport;
pub(crate) use build::{BuildOptions, build};
use bytes::Bytes;
pub use follow::Follower;
use pando_format::{CHUNK_SIZE, Entry, Hash, depth_for};
pub(crate) use view::BaseView;

use crate::cache::Cache;
use crate::dedup::{BaseBlocks, BlockMap};
use crate::local::{read_file, write_atomic};
use crate::ports::{Clock, Cpu, LocalFs, LocalImage, Open};
use crate::tree::Tree;
use crate::{Error, Ports, Result};
use host_blocks::HostBlocks;

/// The other base images, besides the seed, whose blocks a seeded build reuses at most.
const HOST_BLOCK_BASES: usize = 3;

/// The base images of one host, in one directory.
#[derive(Clone, Debug)]
pub(crate) struct Bases {
    fs: Arc<dyn LocalFs>,
    image: Arc<dyn LocalImage>,
    clock: Arc<dyn Clock>,
    cpu: Arc<dyn Cpu>,
    dir: PathBuf,
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

impl Bases {
    pub(crate) fn new(ports: &Ports, dir: &Path) -> Bases {
        Bases {
            fs: Arc::clone(&ports.fs),
            image: Arc::clone(&ports.image),
            clock: Arc::clone(&ports.clock),
            cpu: Arc::clone(&ports.cpu),
            dir: dir.to_owned(),
        }
    }

    /// The image of `root`.
    pub(crate) fn path(&self, root: &Hash) -> PathBuf {
        self.dir.join(format!("{root}.img"))
    }

    pub(crate) fn blocks_path(image: &Path) -> PathBuf {
        with_suffix(image, ".blocks")
    }

    pub(crate) fn nodes_path(image: &Path) -> PathBuf {
        with_suffix(image, ".nodes")
    }

    async fn exists(&self, p: &Path) -> bool {
        self.fs.open(p, Open::Read).await.is_ok()
    }

    /// Whether this host has the complete image of `root`.
    pub(crate) async fn has(&self, root: &Hash) -> bool {
        self.exists(&self.path(root)).await
    }

    /// Marks the image at `image` as just used, so the next new root seeds from it.
    pub(crate) async fn touch(&self, image: &Path) -> io::Result<()> {
        let now = self
            .clock
            .wall()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        write_atomic(
            self.fs.as_ref(),
            &with_suffix(image, ".used"),
            Bytes::from(now.as_nanos().to_string()),
        )
        .await
    }

    async fn used(&self, image: &Path) -> u128 {
        match read_file(self.fs.as_ref(), &with_suffix(image, ".used")).await {
            Ok(Some(b)) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            _ => 0,
        }
    }

    /// The complete images of this host of `size` bytes, other than those of `skip`, the most
    /// recently used first.
    async fn images(&self, size: u64, skip: &[Hash]) -> Vec<(PathBuf, Hash)> {
        let Ok(entries) = self.fs.list(&self.dir).await else {
            return Vec::new();
        };
        let mut found = Vec::new();
        for de in entries {
            let Some(root) = de.name.strip_suffix(".img").and_then(|n| n.parse::<Hash>().ok()) else {
                continue;
            };
            if de.len != size || skip.contains(&root) {
                continue;
            }
            let p = self.dir.join(&de.name);
            if self.exists(&Bases::nodes_path(&p)).await && self.exists(&Bases::blocks_path(&p)).await {
                found.push((self.used(&p).await, p, root));
            }
        }
        found.sort_by_key(|a| std::cmp::Reverse(a.0));
        found.into_iter().map(|(_, p, r)| (p, r)).collect()
    }

    /// The image most recently used, other than `root`'s, of the same size.
    async fn nearest(&self, size: u64, root: &Hash) -> Option<(PathBuf, Hash)> {
        self.images(size, &[*root]).await.into_iter().next()
    }

    /// The block maps and images of up to [`HOST_BLOCK_BASES`] other bases of `size` bytes.
    async fn other_blocks(&self, size: u64, skip: &[Hash]) -> HostBlocks {
        let mut hb = HostBlocks::default();
        for (p, _) in self.images(size, skip).await.into_iter().take(HOST_BLOCK_BASES) {
            let (Ok(Some(map)), Ok(f)) = (
                BlockMap::load(self.fs.as_ref(), &Bases::blocks_path(&p)).await,
                self.fs.open(&p, Open::Read).await,
            ) else {
                continue;
            };
            hb.push(f, map);
        }
        hb
    }

    /// Loads the node pack of the image at `image` into `tree`.
    pub(crate) async fn load_nodes(&self, tree: &Tree, image: &Path) -> Result<usize> {
        let b = read_file(self.fs.as_ref(), &Bases::nodes_path(image))
            .await?
            .ok_or_else(|| Error::Corrupt(format!("{} has no node pack", image.display())))?;
        Ok(tree.load_nodes(node_pack::decode(&b)?))
    }

    /// The blocks of `root`'s image for uploads of a branch opened at `root`, with its nodes
    /// loaded into `tree`'s nodes; `None` when this host has no complete image of it. The
    /// blocks walk `root` at the depth of the image's size, whatever `tree`'s depth.
    pub(crate) async fn blocks_for(&self, tree: &Tree, root: Entry) -> Result<Option<BaseBlocks>> {
        let p = self.path(&root.hash);
        if !self.exists(&p).await {
            return Ok(None);
        }
        let len = self.fs.open(&p, Open::Read).await?.size().await?;
        let tree = Arc::new(tree.at_depth(depth_for(len.div_ceil(CHUNK_SIZE as u64))));
        self.load_nodes(&tree, &p).await?;
        BaseBlocks::open(self.fs.as_ref(), &Bases::blocks_path(&p), &p, tree, root).await
    }

    /// Starts a branch's image at `to` as a clone of `root`'s, sharing its disk blocks.
    pub(crate) async fn clone_to(&self, root: &Hash, to: &Path) -> io::Result<()> {
        let p = self.path(root);
        self.image.clone_file(&p, to).await?;
        let _ = self.touch(&p).await;
        Ok(())
    }
}

/// Builds in `dir` the base image of branch `name`'s published root, unless this host has it.
pub async fn build_branch(
    ports: &Ports,
    cache: &Arc<Cache>,
    dir: &Path,
    name: &str,
    parallel: usize,
) -> Result<BuildReport> {
    let (r, _) = cache
        .remote()
        .get_ref(name)
        .await?
        .ok_or_else(|| Error::Corrupt(format!("{name}: no such branch")))?;
    let tree = Tree::new(Arc::clone(cache), r.size.div_ceil(CHUNK_SIZE as u64));
    let opts = BuildOptions {
        parallel,
        serve: false,
        after: None,
    };
    build(&Bases::new(ports, dir), cache, &tree, r.root, r.size, opts).await
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests;
