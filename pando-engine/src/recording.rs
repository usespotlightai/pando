//! Recordings of the first reads of mounts at a root, so the next mount fetches them at once.
//!
//! The chunks a mount read come first, in the order it first read them; after them the earlier
//! ones it did not read, one mount older each. A chunk no mount read for [`AGE_MAX`] mounts
//! drops out, so the recording follows the workload. A mount saves the recording of the root it
//! opened at, and copies it to the root it closes at for the branches copied from it.

use std::collections::HashSet;
use std::sync::Arc;

use pando_format::{Entry, Hash, Recording};

use crate::Result;
use crate::cache::Cache;

pub(crate) const RECORDING_MAX: usize = 32768;
pub(crate) const AGE_MAX: u8 = 3;

/// The recording after a mount that read `used`, of which `nodes` are tree nodes.
pub(crate) fn merge(used: &[Entry], nodes: &HashSet<Hash>, old: &Recording) -> Recording {
    let mut seen = HashSet::new();
    let mut r = Recording::default();
    let mut add = |e: &Entry, age: u8, node: bool| {
        if r.entries.len() < RECORDING_MAX && seen.insert(e.hash) {
            r.entries.push(*e);
            r.ages.push(age);
            r.nodes.push(node);
        }
    };
    for e in used {
        add(e, 0, nodes.contains(&e.hash));
    }
    for (i, e) in old.entries.iter().enumerate() {
        let age = old.ages.get(i).copied().unwrap_or(0).saturating_add(1);
        if age < AGE_MAX {
            add(e, age, old.nodes.get(i).copied().unwrap_or(false));
        }
    }
    r
}

/// The recording of `root`; empty when it has none.
pub(crate) async fn load(cache: &Cache, root: &Entry) -> Recording {
    if root.is_empty() {
        return Recording::default();
    }
    cache
        .remote()
        .get_recording(root.hash)
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// Fetches what a recording names: its segment indexes come with it, and each frame that
/// holds a recorded node is decoded whole. Returns how many frames held them.
pub(crate) async fn prefetch(cache: &Arc<Cache>, rec: &Recording, parallel: usize) -> usize {
    cache.seed_indexes(&rec.indexes);
    let nodes = rec
        .entries
        .iter()
        .zip(&rec.nodes)
        .filter(|(_, n)| **n)
        .map(|(e, _)| e.hash)
        .collect();
    cache.fetch_chunks(&rec.entries, nodes, parallel).await
}

/// Saves the recording of a mount that opened at `opened` and closes at `end`. Returns how
/// many chunks it lists; zero when the mount read nothing.
pub(crate) async fn save(cache: &Cache, opened: &Entry, end: &Entry) -> Result<usize> {
    let (used, nodes) = cache.recorded();
    if used.is_empty() || opened.is_empty() {
        return Ok(0);
    }
    let mut r = merge(&used, &nodes, &load(cache, opened).await);
    r.indexes = cache.known_indexes(&r.entries);
    cache.remote().put_recording(opened.hash, &r).await?;
    if !end.is_empty() && end.hash != opened.hash {
        cache.remote().put_recording(end.hash, &r).await?;
    }
    Ok(r.entries.len())
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use pando_format::{CHUNK_SIZE, SegId, SegmentIndex, u64_from};

    use super::*;
    use crate::{Volume, sim};

    #[test]
    fn a_chunk_no_mount_reads_ages_out() {
        let e = |b: &[u8]| Entry {
            hash: Hash::of(b),
            ..Entry::EMPTY
        };
        let (a, b, c) = (e(b"a"), e(b"b"), e(b"c"));
        let mut r = merge(&[a, b], &HashSet::from([a.hash]), &Recording::default());
        assert_eq!(
            (r.entries.clone(), r.nodes.clone()),
            (vec![a, b], vec![true, false])
        );
        for _ in 1..AGE_MAX {
            r = merge(&[c], &HashSet::new(), &r);
        }
        assert_eq!(r.entries, vec![c, a, b]);
        assert_eq!(merge(&[c], &HashSet::new(), &r).entries, vec![c]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_next_mount_prefetches_what_the_last_one_read_with_its_indexes() {
        let (ports, _, _) = sim::ports(5);
        Volume::create(&ports, "main", 1 << 22).await.unwrap();
        let a = Arc::new(Cache::open(&ports, Path::new("/a")).await.unwrap());
        let v = Volume::open(&ports, Arc::clone(&a), Path::new("/va"), "main")
            .await
            .unwrap();
        v.write_at(&vec![7; 1 << 22], 0).await.unwrap();
        v.checkpoint().await.unwrap();
        v.close().await;

        let b = Arc::new(Cache::open(&ports, Path::new("/b")).await.unwrap());
        let v = Volume::open(&ports, Arc::clone(&b), Path::new("/vb"), "main")
            .await
            .unwrap();
        let mut buf = vec![0; 100];
        v.read_at(&mut buf, 5 * u64_from(CHUNK_SIZE)).await.unwrap();
        let opened = v.root();
        assert!(save(&b, &opened, &v.root()).await.unwrap() >= 2);
        v.close().await;

        let c = Arc::new(Cache::open(&ports, Path::new("/c")).await.unwrap());
        let rec = load(&c, &opened).await;
        let none: [(SegId, SegmentIndex); 0] = [];
        assert_ne!(rec.indexes, none);
        let gets = || c.remote().stats().gets.load(std::sync::atomic::Ordering::Relaxed);
        let before = gets();
        assert!(prefetch(&c, &rec, 8).await >= 1);
        let runs = c.stats().runs.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(gets() - before, runs, "range GETs only, no index GETs");
    }
}
