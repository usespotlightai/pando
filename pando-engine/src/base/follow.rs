//! A host follows the branches that the branches it served were copied from. When such a
//! branch moves to a new root, the host builds that root's base image in the background, from
//! its nearest base, so the next copy of it hands off at once. Nobody keeps a list: an attach
//! notes the branch it was copied from, and the note ages out when no copy of that branch comes
//! back for [`FOLLOW_MAX_AGE`]. Base images age out by last use, within a count and a disk
//! budget.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use pando_format::{CHUNK_SIZE, Hash};

use super::{Bases, BuildOptions, build, with_suffix};
use crate::cache::Cache;
use crate::local::{read_file, write_atomic};
use crate::tree::Tree;
use crate::{Ports, Result};

pub(crate) const FOLLOW_MAX_AGE: Duration = Duration::from_hours(168);

fn escape(name: &str) -> String {
    name.replace('%', "%25").replace('/', "%2F")
}

fn unescape(file: &str) -> String {
    file.replace("%2F", "/").replace("%25", "%")
}

fn nanos(t: SystemTime) -> u128 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

impl Bases {
    fn follow_dir(&self) -> PathBuf {
        self.dir.join("follow")
    }

    /// Records that a copy of branch `name` was served on this host now.
    pub(crate) async fn note_follow(&self, name: &str) -> Result<()> {
        let dir = self.follow_dir();
        self.fs.create_dir_all(&dir).await?;
        let now = nanos(self.clock.wall()).to_string();
        Ok(write_atomic(self.fs.as_ref(), &dir.join(escape(name)), Bytes::from(now)).await?)
    }

    /// Keeps `keep` base images, and fewer while they take more than `budget` bytes of disk
    /// (zero: no budget): those of the roots in `current` first, then the most recently used;
    /// the first one always stays. A branch that runs on a base keeps its own clone, so
    /// removing the base never touches it. Returns how many it removed and the bytes they took.
    pub(crate) async fn evict(&self, keep: usize, budget: u64, current: &[Hash]) -> (usize, u64) {
        let Ok(entries) = self.fs.list(&self.dir).await else {
            return (0, 0);
        };
        let mut found = Vec::new();
        for de in entries {
            let Some(root) = de.name.strip_suffix(".img").and_then(|n| n.parse::<Hash>().ok()) else {
                continue;
            };
            let p = self.dir.join(&de.name);
            let size = self.image.allocated(&p).await.unwrap_or(de.len);
            let rank = (current.contains(&root), self.used(&p).await);
            found.push((rank, p, size));
        }
        found.sort_by_key(|b| std::cmp::Reverse(b.0));
        let (mut total, mut freed, mut n) = (0, 0, 0);
        for (i, (_, p, size)) in found.into_iter().enumerate() {
            total += size;
            if i == 0 || (i < keep && (budget == 0 || total <= budget)) {
                continue;
            }
            for suffix in ["", ".nodes", ".blocks", ".used"] {
                let _ = self.fs.remove(&with_suffix(&p, suffix)).await;
            }
            n += 1;
            freed += size;
        }
        (n, freed)
    }
}

/// Builds the base images of new roots of the branches this host follows.
#[derive(Debug)]
pub struct Follower {
    bases: Bases,
    cache: Arc<Cache>,
    parallel: usize,
    keep: usize,
    budget: u64,
    seen: HashMap<String, Hash>,
}

impl Follower {
    /// A follower of the branches noted in base image directory `dir`; it keeps the `keep` most
    /// recently used base images within `budget` bytes (zero: no budget).
    pub fn new(
        ports: &Ports,
        cache: Arc<Cache>,
        dir: &Path,
        parallel: usize,
        keep: usize,
        budget: u64,
    ) -> Follower {
        Follower {
            bases: Bases::new(ports, dir),
            cache,
            parallel,
            keep,
            budget,
            seen: HashMap::new(),
        }
    }

    /// Checks every followed branch once and builds the base image of each new root; returns
    /// how many it built.
    pub async fn once(&mut self) -> Result<usize> {
        let dir = self.bases.follow_dir();
        let fs = Arc::clone(&self.bases.fs);
        let Ok(entries) = fs.list(&dir).await else {
            return Ok(0);
        };
        let now = nanos(self.bases.clock.wall());
        let mut built = 0;
        for de in entries {
            let p = dir.join(&de.name);
            let noted = read_file(fs.as_ref(), &p)
                .await
                .ok()
                .flatten()
                .and_then(|b| std::str::from_utf8(&b).ok()?.trim().parse::<u128>().ok());
            let name = unescape(&de.name);
            let fresh = noted.is_some_and(|t| now.saturating_sub(t) <= FOLLOW_MAX_AGE.as_nanos());
            let r = if fresh {
                self.cache.remote().get_ref(&name).await
            } else {
                Ok(None)
            };
            let r = match r {
                Ok(Some((r, _))) => r,
                Ok(None) => {
                    let _ = fs.remove(&p).await;
                    self.seen.remove(&name);
                    continue;
                }
                Err(_) => continue,
            };
            if r.root.is_empty() || self.seen.get(&name) == Some(&r.root.hash) {
                continue;
            }
            let path = self.bases.path(&r.root.hash);
            if self.bases.has(&r.root.hash).await {
                let _ = self.bases.touch(&path).await;
            } else {
                let tree = Tree::new(Arc::clone(&self.cache), r.size.div_ceil(CHUNK_SIZE as u64));
                let opts = BuildOptions {
                    parallel: self.parallel,
                    serve: false,
                    after: None,
                };
                if build(&self.bases, &self.cache, &tree, r.root, r.size, opts)
                    .await
                    .is_err()
                {
                    continue;
                }
                built += 1;
            }
            self.seen.insert(name, r.root.hash);
        }
        let current: Vec<Hash> = self.seen.values().copied().collect();
        self.bases.evict(self.keep, self.budget, &current).await;
        Ok(built)
    }
}
