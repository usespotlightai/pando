//! Garbage collection: deletes segments no branch or checkpoint reaches, change lists no branch
//! or checkpoint reads, and the write logs of deleted branches.
//!
//! A segment nothing reaches is a fenced writer's leftover when its writer generation is older
//! than its branch's current one, or its branch is gone. It is kept while its writer is still
//! current, since that upload may yet publish it, and while it is younger than
//! [`GcOptions::keep_young`], since a run may still take it as a recipe source. Every segment a
//! kept segment's recipes read is kept too. Before deleting, GC marks again after
//! [`GcOptions::validate`], so a branch copied or a checkpoint taken meanwhile counts.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pando_format::{CHUNK_SIZE, Entry, Hash, SegId, WriterId};

use crate::cache::Cache;
use crate::dedup::learned;
use crate::ports::Clock;
use crate::remote::Remote;
use crate::tree::Tree;
use crate::{Error, Result};

/// Segments a mark fetches tree nodes from at once.
const MARK_PARALLEL: usize = 16;

/// What a [`collect`] run does and how long it waits.
#[derive(Clone, Copy, Debug)]
pub struct GcOptions {
    /// Delete what is dead; without it GC only reports.
    pub delete: bool,
    /// Keep segments uploaded less than this long ago, with what they depend on. It must be
    /// longer than a host keeps a learned recipe source.
    pub keep_young: Duration,
    /// How long after the first mark GC marks again. A branch copy must finish within it.
    pub validate: Duration,
}

impl Default for GcOptions {
    fn default() -> GcOptions {
        GcOptions {
            delete: false,
            keep_young: Duration::from_hours(24),
            validate: Duration::from_secs(30),
        }
    }
}

/// What a [`collect`] run found, and whether it deleted what it counted as dead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Segments some branch reaches, directly or through a recipe.
    pub live: usize,
    /// The stored bytes of the live segments.
    pub live_bytes: u64,
    /// The bytes of live segments that reachable objects use.
    pub used_bytes: u64,
    /// Segments no branch reaches and nothing keeps.
    pub dead: usize,
    /// The stored bytes of the dead segments.
    pub dead_bytes: u64,
    /// Unreached segments kept because their writer is still current.
    pub current_writer: usize,
    /// Unreached segments kept because they are younger than [`GcOptions::keep_young`].
    pub young: usize,
    /// Unreached segments kept because a recipe of a kept segment reads them.
    pub needed_by_kept: usize,
    /// Segments dead at the first mark that a branch reached at the second.
    pub revived: usize,
    /// Change lists whose root no branch holds, older than [`GcOptions::keep_young`].
    pub stale_change_lists: usize,
    /// Write log records of branches that no longer exist.
    pub stale_log_records: usize,
    /// Whether the dead segments and stale change lists were deleted.
    pub deleted: bool,
}

/// Per live segment, how many bytes reachable objects use in it; segments only recipes depend
/// on use none.
async fn mark(remote: &Remote, cache: &Arc<Cache>) -> Result<BTreeMap<SegId, u64>> {
    let mut live: BTreeMap<SegId, u64> = BTreeMap::new();
    let mut nodes: HashSet<Hash> = HashSet::new();
    let mut stored: HashSet<Entry> = HashSet::new();
    for (size, root) in roots(remote).await? {
        let tree = Tree::new(Arc::clone(cache), size.div_ceil(CHUNK_SIZE as u64));
        tree.prefetch_nodes(&root, MARK_PARALLEL).await?;
        tree.walk(&root, true, |e, node| {
            if !e.loc.is_pending() && stored.insert(*e) {
                *live.entry(e.loc.seg).or_default() += u64::from(e.loc.len);
            }
            !node || nodes.insert(e.hash)
        })
        .await?;
    }
    let mut queue: Vec<SegId> = live.keys().copied().collect();
    while let Some(id) = queue.pop() {
        let idx = remote
            .get_index(id)
            .await
            .map_err(|e| Error::Corrupt(format!("index of live segment {id}: {e}")))?;
        for dep in idx.deps {
            if let std::collections::btree_map::Entry::Vacant(v) = live.entry(dep) {
                v.insert(0);
                queue.push(dep);
            }
        }
    }
    Ok(live)
}

/// The size and root of every branch and every checkpoint.
async fn roots(remote: &Remote) -> Result<Vec<(u64, Entry)>> {
    let mut out = Vec::new();
    for name in remote.list_refs().await? {
        if let Some((r, _)) = remote.get_ref(&name).await? {
            out.push((r.size, r.root));
        }
    }
    for name in remote.list_checkpoints().await? {
        if let Some(c) = remote.get_checkpoint(&name).await? {
            out.push((c.size, c.root));
        }
    }
    Ok(out)
}

/// `ids` and every segment their recipes read, transitively. A missing index of one of `ids`
/// is skipped; of a segment one of them needs, it is an error.
async fn with_deps(remote: &Remote, ids: &[SegId]) -> Result<BTreeSet<SegId>> {
    let mut out: BTreeSet<SegId> = ids.iter().copied().collect();
    let mut queue = ids.to_vec();
    while let Some(id) = queue.pop() {
        let idx = match remote.get_index(id).await {
            Ok(idx) => idx,
            Err(_) if ids.contains(&id) => continue,
            Err(e) => {
                return Err(Error::Corrupt(format!(
                    "index of segment {id}, which a kept segment needs: {e}"
                )));
            }
        };
        for dep in idx.deps {
            if out.insert(dep) {
                queue.push(dep);
            }
        }
    }
    Ok(out)
}

/// What a store holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Refs in the store.
    pub branches: usize,
    /// Segments in the store, counting one whose data or index alone remains.
    pub segments: usize,
    /// The bytes of segment data, or of the index where the data is missing.
    pub segment_bytes: u64,
    /// Change lists in the store.
    pub change_lists: usize,
}

/// Counts what the store behind `cache` holds.
pub async fn usage(cache: &Arc<Cache>) -> Result<Usage> {
    let remote = cache.remote();
    let segs = remote.list_segments().await?;
    Ok(Usage {
        branches: remote.list_refs().await?.len(),
        segments: segs.len(),
        segment_bytes: segs.values().map(|s| s.size).sum(),
        change_lists: remote.list_change_lists().await?.len(),
    })
}

/// Collects the garbage of the store behind `cache`.
pub async fn collect(cache: &Arc<Cache>, clock: &dyn Clock, opts: &GcOptions) -> Result<GcReport> {
    debug_assert!(opts.keep_young > learned::MAX_AGE || opts.keep_young.is_zero());
    let remote = cache.remote();
    let segs = remote.list_segments().await?;
    let live = mark(remote, cache).await?;
    let mut current: HashMap<[u8; 8], u32> = HashMap::new();
    let mut roots: HashSet<Hash> = HashSet::new();
    let logs = remote.list_all_logs().await?;
    let mut names: HashSet<Hash> = HashSet::new();
    for name in remote.list_refs().await? {
        names.insert(Hash::of(name.as_bytes()));
        if let Some((r, _)) = remote.get_ref(&name).await? {
            current.insert(
                WriterId::new(&name, 0).branch,
                WriterId::new(&name, r.generation).generation,
            );
            roots.insert(r.root.hash);
        }
    }
    for name in remote.list_checkpoints().await? {
        if let Some(c) = remote.get_checkpoint(&name).await? {
            roots.insert(c.root.hash);
        }
    }
    let now = clock.wall();
    let young =
        |t| opts.keep_young > Duration::ZERO && now.duration_since(t).unwrap_or_default() < opts.keep_young;
    let mut report = GcReport {
        live: live.len(),
        deleted: opts.delete,
        ..GcReport::default()
    };
    let (mut candidates, mut kept) = (Vec::new(), Vec::new());
    for (id, info) in &segs {
        if let Some(used) = live.get(id) {
            report.live_bytes += info.size;
            report.used_bytes += used;
            continue;
        }
        let w = id.writer();
        if current.get(&w.branch).is_some_and(|g| w.generation >= *g) {
            report.current_writer += 1;
            kept.push(*id);
        } else if young(info.modified) {
            report.young += 1;
            kept.push(*id);
        } else {
            candidates.push(*id);
        }
    }
    let needed = with_deps(remote, &kept).await?;
    candidates.retain(|id| {
        let keep = needed.contains(id);
        report.needed_by_kept += usize::from(keep);
        !keep
    });
    tokio::time::sleep(opts.validate).await;
    let again = mark(remote, cache).await?;
    for id in candidates {
        if again.contains_key(&id) {
            report.revived += 1;
            continue;
        }
        report.dead += 1;
        report.dead_bytes += segs[&id].size;
        if opts.delete {
            remote.delete_segment(id).await?;
        }
    }
    for l in remote.list_change_lists().await? {
        if roots.contains(&l.root) || young(l.modified) {
            continue;
        }
        report.stale_change_lists += 1;
        if opts.delete {
            remote.delete_change_list(l.root, l.from).await?;
        }
    }
    for (key, branch, modified) in logs {
        if names.contains(&branch) || young(modified) {
            continue;
        }
        report.stale_log_records += 1;
        if opts.delete {
            remote.delete_key(&key).await?;
        }
    }
    Ok(report)
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::{Volume, branch, sim};

    async fn host(ports: &crate::Ports, dir: &str) -> Arc<Cache> {
        Arc::new(Cache::open(ports, Path::new(dir)).await.unwrap())
    }

    async fn write(ports: &crate::Ports, c: &Arc<Cache>, seed: u8) {
        let v = Volume::open(ports, Arc::clone(c), Path::new("/v"), "main")
            .await
            .unwrap();
        let b: Vec<u8> = (0..4 * CHUNK_SIZE)
            .map(|i| i.to_le_bytes()[0].wrapping_mul(31).wrapping_add(seed))
            .collect();
        v.write_at(&b, 0).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn gc_deletes_what_a_later_writer_replaced_and_keeps_the_rest() {
        let (ports, _, _) = sim::ports(1);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let c = host(&ports, "/c").await;
        write(&ports, &c, 1).await;
        let opts = GcOptions {
            delete: true,
            ..GcOptions::default()
        };

        let r = collect(&c, ports.clock.as_ref(), &opts).await.unwrap();
        assert_eq!((r.live, r.dead), (1, 0));

        write(&ports, &c, 2).await;
        let r = collect(&c, ports.clock.as_ref(), &opts).await.unwrap();
        assert_eq!((r.live, r.dead, r.young), (1, 0, 1), "a young segment stays");

        tokio::time::sleep(Duration::from_hours(25)).await;
        branch::copy(&ports, "main", "fork", None).await.unwrap();
        let r = collect(&c, ports.clock.as_ref(), &opts).await.unwrap();
        assert_eq!((r.live, r.dead, r.young), (1, 1, 0));
        assert_eq!(c.remote().list_segments().await.unwrap().len(), 1);

        let fresh = host(&ports, "/d").await;
        let v = Volume::open(&ports, fresh, Path::new("/w"), "fork")
            .await
            .unwrap();
        let mut b = vec![0; 4 * CHUNK_SIZE];
        v.read_at(&mut b, 0).await.unwrap();
        assert_eq!(b[CHUNK_SIZE + 1], 1u8.wrapping_mul(31).wrapping_add(2));
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_chunk_stored_in_two_segments_keeps_both() {
        let (ports, _, _) = sim::ports(5);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let c = host(&ports, "/c").await;
        let chunk = |seed: u8| -> Vec<u8> {
            (0..CHUNK_SIZE)
                .map(|i| i.to_le_bytes()[0].wrapping_mul(seed) ^ seed)
                .collect()
        };
        let at = |i: u64| i * CHUNK_SIZE as u64;
        let v = Volume::open(&ports, Arc::clone(&c), Path::new("/v"), "main")
            .await
            .unwrap();
        v.write_at(&chunk(3), 0).await.unwrap();
        v.checkpoint().await.unwrap();
        v.write_at(&chunk(5), at(1)).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
        let kept = branch::Checkpoints::default();
        branch::checkpoint(&ports, &kept, "main", "v1", None)
            .await
            .unwrap();
        let v = Volume::open(&ports, host(&ports, "/e").await, Path::new("/e"), "main")
            .await
            .unwrap();
        v.write_at(&chunk(3), at(2)).await.unwrap();
        v.write_at(&chunk(7), 0).await.unwrap();
        v.write_at(&chunk(9), at(1)).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;
        tokio::time::sleep(Duration::from_hours(25)).await;
        let opts = GcOptions {
            delete: true,
            ..GcOptions::default()
        };
        collect(&c, ports.clock.as_ref(), &opts).await.unwrap();

        branch::from_checkpoint(&ports, &kept, "v1", "back", None)
            .await
            .unwrap();
        let v = Volume::open(&ports, host(&ports, "/d").await, Path::new("/w"), "back")
            .await
            .unwrap();
        let mut b = vec![0; CHUNK_SIZE];
        v.read_at(&mut b, 0).await.unwrap();
        assert_eq!(b, chunk(3));
        v.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_current_writers_unpublished_segment_stays() {
        let (ports, _, _) = sim::ports(2);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let c = host(&ports, "/c").await;
        write(&ports, &c, 1).await;
        let (r, _) = c.remote().get_ref("main").await.unwrap().unwrap();
        let w = pando_format::Writer::new(WriterId::new("main", r.generation), 77);
        let idx = pando_format::SegmentIndex {
            frames: vec![],
            deps: vec![],
        }
        .encode();
        c.remote()
            .put_segment(w.next_seg_id(), bytes::Bytes::new(), idx.into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_hours(25)).await;
        let r = collect(
            &c,
            ports.clock.as_ref(),
            &GcOptions {
                delete: true,
                ..GcOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!((r.dead, r.current_writer), (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_checkpoint_keeps_what_its_root_reaches() {
        let (ports, _, _) = sim::ports(3);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        let c = host(&ports, "/c").await;
        write(&ports, &c, 1).await;
        let kept = branch::Checkpoints::default();
        branch::checkpoint(&ports, &kept, "main", "v1", None)
            .await
            .unwrap();
        write(&ports, &c, 2).await;
        tokio::time::sleep(Duration::from_hours(25)).await;
        let opts = GcOptions {
            delete: true,
            ..GcOptions::default()
        };
        let r = collect(&c, ports.clock.as_ref(), &opts).await.unwrap();
        assert_eq!((r.live, r.dead), (2, 0));

        branch::delete_checkpoint(&ports, &kept, "v1").await.unwrap();
        let r = collect(&c, ports.clock.as_ref(), &opts).await.unwrap();
        assert_eq!((r.live, r.dead), (1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn the_log_of_a_deleted_branch_goes() {
        let (ports, _, _) = sim::ports(4);
        Volume::create(&ports, "main", 1 << 20).await.unwrap();
        Volume::create(&ports, "gone", 1 << 20).await.unwrap();
        let c = host(&ports, "/c").await;
        let seal = |n| bytes::Bytes::from(pando_format::LogRecord::encode_seal(1, n));
        for name in ["main", "gone"] {
            c.remote().create_log_record(name, 1, 0, seal(0)).await.unwrap();
        }
        c.remote().delete_ref("gone").await.unwrap();
        tokio::time::sleep(Duration::from_hours(25)).await;
        let r = collect(
            &c,
            ports.clock.as_ref(),
            &GcOptions {
                delete: true,
                ..GcOptions::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(r.stale_log_records, 1);
        let main: Vec<_> = c
            .remote()
            .list_log("main")
            .await
            .unwrap()
            .into_iter()
            .map(|(g, n, _)| (g, n))
            .collect();
        assert_eq!(main, [(1, 0)]);
        assert_eq!(c.remote().list_log("gone").await.unwrap(), []);
    }
}
