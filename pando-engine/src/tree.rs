//! The block map: a persistent radix tree (fanout 256) from chunk index to chunk entry.
//!
//! Nodes are content-addressed, so a branch is one root entry: free to create, and an update
//! copies only the path from the changed leaves to the root. New nodes are pending: they live
//! only in the local cache until an upload packs them.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use pando_format::{BlockKeys, Change, ChangeList, Entry, Hash, Node, NodeAt, depth_for, digit};

use crate::cache::Cache;
use crate::packer::Packer;
use crate::{Error, Result};

/// Chunks an upload reads and makes ready at once.
const PREPARE_PARALLEL: usize = 16;

/// The tree of one depth. Every root walked with it must be a root of that depth: the depth of
/// the size of the branch the root belongs to.
#[derive(Debug)]
pub(crate) struct Tree {
    cache: Arc<Cache>,
    depth: usize,
    nodes: Arc<Mutex<HashMap<Hash, Arc<Node>>>>,
}

impl Tree {
    pub(crate) fn new(cache: Arc<Cache>, chunks: u64) -> Tree {
        Tree {
            cache,
            depth: depth_for(chunks),
            nodes: Arc::default(),
        }
    }

    /// The tree of depth `depth` over the same cache, sharing the nodes this one loaded.
    pub(crate) fn at_depth(&self, depth: usize) -> Tree {
        Tree {
            cache: Arc::clone(&self.cache),
            depth,
            nodes: Arc::clone(&self.nodes),
        }
    }

    pub(crate) fn depth(&self) -> usize {
        self.depth
    }

    /// Root `root` of a tree of depth `from`, at most this tree's depth, as a root of this
    /// tree: each added level holds the root below in slot 0, so every chunk keeps its index.
    /// The new nodes are pending.
    pub(crate) async fn lift(&self, root: &Entry, from: usize) -> Result<Entry> {
        let mut e = *root;
        for _ in from..self.depth {
            if e.is_empty() {
                break;
            }
            let mut n = Node::new();
            n.set(0, e);
            e = self.store(n).await?;
        }
        Ok(e)
    }

    pub(crate) fn cache(&self) -> &Arc<Cache> {
        &self.cache
    }

    fn cached(&self, h: &Hash) -> Option<Arc<Node>> {
        self.nodes.lock().get(h).cloned()
    }

    pub(crate) fn has_node(&self, h: &Hash) -> bool {
        self.nodes.lock().contains_key(h)
    }

    pub(crate) async fn load(&self, e: &Entry, background: bool) -> Result<Arc<Node>> {
        if let Some(n) = self.cached(&e.hash) {
            return Ok(n);
        }
        let b = if background {
            self.cache.get_background(e).await?
        } else {
            let b = self.cache.get(e).await?;
            self.cache.touched_node(&e.hash);
            b
        };
        let n = Arc::new(Node::decode(&b)?);
        self.nodes.lock().insert(e.hash, Arc::clone(&n));
        Ok(n)
    }

    async fn node_or_empty(&self, e: &Entry) -> Result<Arc<Node>> {
        if e.is_empty() {
            Ok(Arc::new(Node::new()))
        } else {
            self.load(e, true).await
        }
    }

    /// Stores a node in the local cache and returns its pending entry.
    pub(crate) async fn store(&self, n: Node) -> Result<Entry> {
        let b = Bytes::from(n.encode());
        let h = Hash::of(&b);
        self.cache.put_local(h, b).await?;
        self.nodes.lock().insert(h, Arc::new(n));
        Ok(Entry {
            hash: h,
            ..Entry::EMPTY
        })
    }

    /// Forgets the loaded nodes among `dropped`.
    pub(crate) fn forget(&self, dropped: &HashSet<Hash>) {
        self.nodes.lock().retain(|h, _| !dropped.contains(h));
    }

    /// Records `n`, stored under `h` already, as a loaded node.
    pub(crate) fn keep(&self, h: Hash, n: Node) {
        self.nodes.lock().insert(h, Arc::new(n));
    }

    /// The entry of chunk `idx` under `root`; empty when the chunk was never written.
    pub(crate) async fn lookup(&self, root: &Entry, idx: u64, background: bool) -> Result<Entry> {
        let mut e = *root;
        for level in (0..self.depth).rev() {
            if e.is_empty() {
                return Ok(Entry::EMPTY);
            }
            e = *self.load(&e, background).await?.child(digit(idx, level));
        }
        Ok(e)
    }

    /// [`Self::lookup`] with only the nodes in memory: `None` when it needs another.
    pub(crate) fn lookup_nowait(&self, root: &Entry, idx: u64) -> Option<Entry> {
        let nodes = self.nodes.lock();
        let mut e = *root;
        for level in (0..self.depth).rev() {
            if e.is_empty() {
                return Some(Entry::EMPTY);
            }
            e = *nodes.get(&e.hash)?.child(digit(idx, level));
        }
        Some(e)
    }

    /// A new root with the chunk entries of `updates` set; an empty entry clears one.
    /// `updates` must be sorted by chunk index, without repeats.
    pub(crate) async fn update(&self, root: &Entry, updates: &[(u64, Entry)]) -> Result<Entry> {
        debug_assert!(updates.windows(2).all(|w| w[0].0 < w[1].0));
        self.update_at(*root, self.depth - 1, updates).await
    }

    fn update_at<'a>(
        &'a self,
        e: Entry,
        level: usize,
        ups: &'a [(u64, Entry)],
    ) -> BoxFuture<'a, Result<Entry>> {
        Box::pin(async move {
            let mut n = if e.is_empty() {
                Node::new()
            } else {
                (*self.load(&e, false).await?).clone()
            };
            for group in ups.chunk_by(|a, b| digit(a.0, level) == digit(b.0, level)) {
                let d = digit(group[0].0, level);
                let sub = if level == 0 {
                    group[0].1
                } else {
                    self.update_at(*n.child(d), level - 1, group).await?
                };
                n.set(d, sub);
            }
            if n.is_empty() {
                Ok(Entry::EMPTY)
            } else {
                self.store(n).await
            }
        })
    }

    /// Rewrites `root` so every pending object gets a location in the store: pending chunks
    /// first, in index order, then the rewritten nodes bottom-up. Uploaded subtrees stay as
    /// they are. A pending chunk missing from this host is [`Error::ChunkLost`].
    pub(crate) async fn pack(&self, root: &Entry, p: &mut Packer) -> Result<Entry> {
        let mut chunks = Vec::new();
        self.pending_chunks(root, &mut chunks).await?;
        let mut seen = HashSet::new();
        chunks.retain(|h| p.lookup(h).is_none() && seen.insert(*h));
        let pr = p.preparer().clone();
        let mut ready = futures::stream::iter(chunks)
            .map(|h| {
                let pr = pr.clone();
                async move {
                    if let Some(c) = pr.take(&h) {
                        return Ok((h, c));
                    }
                    let b = self.cache.local(&h).await?.ok_or(Error::ChunkLost { hash: h })?;
                    Ok::<_, Error>((h, pr.prepare(h, b).await))
                }
            })
            .buffered(PREPARE_PARALLEL);
        while let Some(r) = ready.next().await {
            let (h, c) = r?;
            p.commit(h, c).await;
        }
        drop(ready);
        self.pack_at(*root, self.depth - 1, p).await
    }

    async fn pending_chunks(&self, root: &Entry, out: &mut Vec<Hash>) -> Result<()> {
        let mut stack = vec![(*root, self.depth - 1)];
        while let Some((e, level)) = stack.pop() {
            if e.is_empty() || !e.loc.is_pending() {
                continue;
            }
            let n = self.load(&e, true).await?;
            let pending = n.children().filter(|(_, c)| c.loc.is_pending());
            if level == 0 {
                out.extend(pending.map(|(_, c)| c.hash));
            } else {
                stack.extend(
                    pending
                        .map(|(_, c)| (*c, level - 1))
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev(),
                );
            }
        }
        Ok(())
    }

    fn pack_at<'a>(&'a self, e: Entry, level: usize, p: &'a mut Packer) -> BoxFuture<'a, Result<Entry>> {
        Box::pin(async move {
            if e.is_empty() || !e.loc.is_pending() {
                return Ok(e);
            }
            let n = self.load(&e, true).await?;
            let mut out = (*n).clone();
            for (slot, c) in n.children() {
                if !c.loc.is_pending() {
                    continue;
                }
                let packed = if level == 0 {
                    let loc = if let Some(l) = p.lookup(&c.hash) {
                        l
                    } else {
                        let b = self
                            .cache
                            .local(&c.hash)
                            .await?
                            .ok_or(Error::ChunkLost { hash: c.hash })?;
                        p.append_chunk(c.hash, b).await?
                    };
                    Entry { hash: c.hash, loc }
                } else {
                    self.pack_at(*c, level - 1, p).await?
                };
                out.set(slot, packed);
            }
            let b = Bytes::from(out.encode());
            let h = Hash::of(&b);
            self.cache.put_local(h, b.clone()).await?;
            self.keep(h, out);
            let loc = p.append_object(h, &b).await?;
            Ok(Entry { hash: h, loc })
        })
    }

    /// Visits every reachable entry under `root`, nodes before their children, in index order.
    /// `visit` gets the entry and whether it is a node; `false` for a node skips its subtree.
    pub(crate) async fn walk(
        &self,
        root: &Entry,
        background: bool,
        mut visit: impl FnMut(&Entry, bool) -> bool,
    ) -> Result<()> {
        let mut stack = vec![(*root, self.depth - 1)];
        while let Some((e, level)) = stack.pop() {
            if e.is_empty() || !visit(&e, true) {
                continue;
            }
            let n = self.load(&e, background).await?;
            if level == 0 {
                for (_, c) in n.children() {
                    visit(c, false);
                }
            } else {
                stack.extend(
                    n.children()
                        .map(|(_, c)| (*c, level - 1))
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev(),
                );
            }
        }
        Ok(())
    }

    /// Visits every non-empty chunk under `root` with its index, in index order.
    pub(crate) async fn leaves(&self, root: &Entry, mut visit: impl FnMut(u64, Entry)) -> Result<()> {
        let mut stack = vec![(*root, self.depth - 1, 0u64)];
        while let Some((e, level, prefix)) = stack.pop() {
            if e.is_empty() {
                continue;
            }
            let n = self.load(&e, true).await?;
            let children = n.children().filter(|(_, c)| !c.is_empty());
            if level == 0 {
                for (slot, c) in children {
                    visit(prefix << 8 | u64::from(slot), *c);
                }
            } else {
                let start = stack.len();
                stack.extend(children.map(|(slot, c)| (*c, level - 1, prefix << 8 | u64::from(slot))));
                stack[start..].reverse();
            }
        }
        Ok(())
    }

    /// The encoded nodes reachable from `root`.
    pub(crate) async fn nodes_of(&self, root: &Entry) -> Result<HashMap<Hash, Bytes>> {
        let mut out = HashMap::new();
        let mut stack = vec![(*root, self.depth - 1)];
        while let Some((e, level)) = stack.pop() {
            if e.is_empty() || out.contains_key(&e.hash) {
                continue;
            }
            let n = self.load(&e, true).await?;
            out.insert(e.hash, Bytes::from(n.encode()));
            if level > 0 {
                stack.extend(n.children().map(|(_, c)| (*c, level - 1)));
            }
        }
        Ok(out)
    }

    /// Keeps the nodes of a node pack (see [`crate::base`]) whose bytes match their hashes;
    /// the tree fetches the others. Returns how many it kept.
    pub(crate) fn load_nodes(&self, nodes: impl IntoIterator<Item = (Hash, Bytes)>) -> usize {
        let mut kept = Vec::new();
        for (h, b) in nodes {
            if Hash::of(&b) != h {
                continue;
            }
            if let Ok(n) = Node::decode(&b) {
                kept.push((h, Arc::new(n)));
            }
        }
        let n = kept.len();
        let mut m = self.nodes.lock();
        for (h, node) in kept {
            m.entry(h).or_insert(node);
        }
        n
    }

    /// Loads every node under `root`, one level at a time: each level's missing nodes are one
    /// parallel, coalesced fetch, so a cold tree costs one round trip per level, not per node.
    pub(crate) async fn prefetch_nodes(&self, root: &Entry, parallel: usize) -> Result<usize> {
        let mut fetched = 0;
        let mut level = if root.is_empty() { vec![] } else { vec![*root] };
        for depth in (0..self.depth).rev() {
            if level.is_empty() {
                break;
            }
            let missing: Vec<Entry> = level
                .iter()
                .filter(|e| !self.has_node(&e.hash))
                .copied()
                .collect();
            fetched += self.cache.fetch_nodes(&missing, parallel).await;
            let mut next = Vec::new();
            for e in &level {
                let n = self.load(e, true).await?;
                if depth > 0 {
                    next.extend(n.children().map(|(_, c)| *c));
                }
            }
            level = next;
        }
        Ok(fetched)
    }

    /// Brings every chunk under `root` to this host: the nodes first, a level at a time, then
    /// the chunks as one background fetch in index order. Returns how many frames held them.
    pub(crate) async fn hydrate(&self, root: &Entry, parallel: usize) -> Result<usize> {
        self.prefetch_nodes(root, parallel).await?;
        let mut chunks = Vec::new();
        self.walk(root, true, |e, node| {
            if !node {
                chunks.push(*e);
            }
            true
        })
        .await?;
        Ok(self.cache.fetch_entries(&chunks, parallel).await)
    }

    /// The chunks of `b` that differ from `a`, with `b`'s entries, in index order. It walks both
    /// trees one level at a time and fetches each level's missing nodes as one batch.
    pub(crate) async fn changed_leaves(
        &self,
        a: &Entry,
        b: &Entry,
        parallel: usize,
    ) -> Result<Vec<(u64, Entry)>> {
        let mut out = Vec::new();
        let mut level = vec![(*a, *b, 0u64)];
        for depth in (0..self.depth).rev() {
            if level.is_empty() {
                break;
            }
            let missing: Vec<Entry> = level
                .iter()
                .flat_map(|(x, y, _)| [*x, *y])
                .filter(|e| !e.is_empty() && !self.has_node(&e.hash))
                .collect();
            self.cache.fetch_nodes(&missing, parallel).await;
            let mut next = Vec::new();
            for (x, y, prefix) in level {
                let (nx, ny) = (self.node_or_empty(&x).await?, self.node_or_empty(&y).await?);
                for slot in 0..=u8::MAX {
                    let (cx, cy) = (nx.child(slot), ny.child(slot));
                    if cx.hash == cy.hash {
                        continue;
                    }
                    let idx = prefix << 8 | u64::from(slot);
                    if depth == 0 {
                        out.push((idx, *cy));
                    } else {
                        next.push((*cx, *cy, idx));
                    }
                }
            }
            level = next;
        }
        Ok(out)
    }

    /// The change list of `root` from `from`: the chunks it changed, the nodes on their paths,
    /// and the block keys `keys_of` knows.
    pub(crate) async fn change_list(
        &self,
        from: &Entry,
        root: &Entry,
        parallel: usize,
        keys_of: impl Fn(&Hash) -> Option<BlockKeys>,
    ) -> Result<ChangeList> {
        let depth = u8::try_from(self.depth)
            .map_err(|_| Error::Corrupt(format!("a tree of depth {}", self.depth)))?;
        let changes = self.changed_leaves(from, root, parallel).await?;
        let mut seen: HashSet<NodeAt> = HashSet::new();
        let mut nodes = Vec::new();
        for (idx, _) in &changes {
            let mut e = *root;
            for level in (0..depth).rev() {
                if e.is_empty() {
                    break;
                }
                let at = NodeAt {
                    level,
                    prefix: idx.checked_shr(8 * (u32::from(level) + 1)).unwrap_or(0),
                };
                if seen.insert(at) {
                    nodes.push((at, e));
                }
                e = *self.load(&e, true).await?.child(digit(*idx, usize::from(level)));
            }
        }
        let changes = changes
            .into_iter()
            .map(|(idx, entry)| Change {
                idx,
                entry,
                keys: if entry.is_empty() {
                    None
                } else {
                    keys_of(&entry.hash)
                },
            })
            .collect();
        Ok(ChangeList {
            depth,
            nodes,
            changes,
        })
    }

    /// Checks change list `cl` of `root` from `from` and returns its changes: it rebuilds
    /// `root`'s nodes on the path of each change from `from`'s nodes and checks that they hash
    /// to `root`. A node the list does not name must come out empty: the change removed its
    /// subtree. Rebuilt nodes are kept, so later lookups under `root` need no fetch.
    pub(crate) async fn apply_change_list(
        &self,
        from: &Entry,
        root: &Entry,
        cl: ChangeList,
    ) -> Result<Vec<Change>> {
        if usize::from(cl.depth) != self.depth {
            return Err(Error::Corrupt(format!(
                "change list has depth {}, the tree {}",
                cl.depth, self.depth
            )));
        }
        let nodes: HashMap<NodeAt, Entry> = cl.nodes.into_iter().collect();
        let changes: Vec<(u64, Entry)> = cl.changes.iter().map(|c| (c.idx, c.entry)).collect();
        let got = self.rebuild(*from, self.depth - 1, 0, &changes, &nodes).await?;
        if got.hash != root.hash {
            return Err(Error::Corrupt(format!(
                "change list leads to {}, not to {}",
                got.hash.short(),
                root.hash.short()
            )));
        }
        Ok(cl.changes)
    }

    fn rebuild<'a>(
        &'a self,
        old: Entry,
        level: usize,
        prefix: u64,
        changes: &'a [(u64, Entry)],
        nodes: &'a HashMap<NodeAt, Entry>,
    ) -> BoxFuture<'a, Result<Entry>> {
        Box::pin(async move {
            let at = NodeAt {
                level: u8::try_from(level)
                    .map_err(|_| Error::Corrupt(format!("a tree node at level {level}")))?,
                prefix,
            };
            let want = nodes.get(&at).copied().unwrap_or(Entry::EMPTY);
            let mut n = if old.is_empty() {
                Node::new()
            } else {
                (*self.load(&old, true).await?).clone()
            };
            for group in changes.chunk_by(|a, b| digit(a.0, level) == digit(b.0, level)) {
                let d = digit(group[0].0, level);
                let sub = if level == 0 {
                    if group.len() > 1 {
                        return Err(Error::Corrupt(format!(
                            "change list repeats chunk {}",
                            group[0].0
                        )));
                    }
                    group[0].1
                } else {
                    self.rebuild(*n.child(d), level - 1, prefix << 8 | u64::from(d), group, nodes)
                        .await?
                };
                n.set(d, sub);
            }
            if n.is_empty() {
                if !want.is_empty() {
                    return Err(Error::Corrupt(format!(
                        "change list: node at level {level}, prefix {prefix} is empty"
                    )));
                }
                return Ok(Entry::EMPTY);
            }
            let enc = Bytes::from(n.encode());
            if Hash::of(&enc) != want.hash {
                return Err(Error::Corrupt(format!(
                    "change list: node at level {level}, prefix {prefix} does not match"
                )));
            }
            self.cache.put_local(want.hash, enc).await?;
            self.keep(want.hash, n);
            Ok(want)
        })
    }
}

#[cfg(test)]
#[cfg(feature = "sim")]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::remote::Remote;
    use crate::sim;

    fn leaf(i: u8) -> Entry {
        Entry {
            hash: Hash::of(&[i]),
            ..Entry::EMPTY
        }
    }

    async fn tree(seed: u64) -> Tree {
        let (ports, _, _) = sim::ports(seed);
        let c = Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            Remote::new(Arc::clone(&ports.store)),
            Path::new("/c"),
        )
        .await
        .unwrap();
        Tree::new(Arc::new(c), 1 << 20)
    }

    #[tokio::test]
    async fn update_lookup_walk_and_diff() {
        let t = tree(1).await;
        assert_eq!(t.depth(), 3);
        let a = t
            .update(&Entry::EMPTY, &[(0, leaf(1)), (70000, leaf(2)), (70001, leaf(1))])
            .await
            .unwrap();
        let b = t
            .update(&a, &[(1, leaf(2)), (70000, Entry::EMPTY), (300_000, leaf(1))])
            .await
            .unwrap();
        assert_eq!(t.lookup(&b, 0, false).await.unwrap(), leaf(1));
        assert_eq!(t.lookup(&b, 70000, false).await.unwrap(), Entry::EMPTY);
        assert_eq!(t.lookup_nowait(&b, 300_000), Some(leaf(1)));
        assert_eq!(
            t.lookup(&a, 70000, false).await.unwrap(),
            leaf(2),
            "the old root is untouched"
        );

        let mut leaves = 0;
        t.walk(&b, false, |_, node| {
            leaves += usize::from(!node);
            true
        })
        .await
        .unwrap();
        assert_eq!(leaves, 4);

        let diff = t.changed_leaves(&a, &b, 4).await.unwrap();
        assert_eq!(
            diff,
            vec![(1, leaf(2)), (70000, Entry::EMPTY), (300_000, leaf(1))]
        );
        assert_eq!(
            t.update(
                &b,
                &[
                    (0, Entry::EMPTY),
                    (1, Entry::EMPTY),
                    (70001, Entry::EMPTY),
                    (300_000, Entry::EMPTY)
                ]
            )
            .await
            .unwrap(),
            Entry::EMPTY
        );
    }

    #[tokio::test]
    async fn a_lifted_root_keeps_every_chunk_at_its_index() {
        let small = tree(2).await.at_depth(1);
        let root = small
            .update(&Entry::EMPTY, &[(0, leaf(1)), (7, leaf(2)), (255, leaf(3))])
            .await
            .unwrap();
        let big = small.at_depth(3);
        let lifted = big.lift(&root, 1).await.unwrap();
        assert!(lifted.loc.is_pending());
        for (idx, want) in [(0, leaf(1)), (7, leaf(2)), (255, leaf(3)), (256, Entry::EMPTY)] {
            assert_eq!(
                big.lookup(&lifted, idx, false).await.unwrap(),
                want,
                "chunk {idx}"
            );
        }
        let grown = big.update(&lifted, &[(70000, leaf(4))]).await.unwrap();
        assert_eq!(
            big.changed_leaves(&lifted, &grown, 4).await.unwrap(),
            vec![(70000, leaf(4))]
        );
        assert_eq!(big.lift(&Entry::EMPTY, 1).await.unwrap(), Entry::EMPTY);
        assert_eq!(small.lift(&root, 1).await.unwrap(), root);
    }

    #[tokio::test]
    async fn a_packed_root_reads_on_another_host() {
        use pando_format::{CHUNK_SIZE, Writer, WriterId};

        let (ports, _, _) = sim::ports(5);
        let remote = Remote::new(Arc::clone(&ports.store));
        let c = Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            remote.clone(),
            Path::new("/a"),
        )
        .await
        .unwrap();
        let t = Tree::new(Arc::new(c), 1 << 20);
        let mut ups = Vec::new();
        for i in [3u64, 9, 70000, 900_000] {
            let b = Bytes::from(vec![i.to_le_bytes()[0] + 1; CHUNK_SIZE]);
            let h = Hash::of(&b);
            t.cache().put_local(h, b).await.unwrap();
            ups.push((
                i,
                Entry {
                    hash: h,
                    ..Entry::EMPTY
                },
            ));
        }
        let root = t.update(&Entry::EMPTY, &ups).await.unwrap();
        let writer = Arc::new(Writer::new(WriterId::new("main", 1), 0));
        let mut p = Packer::new(
            remote.clone(),
            Arc::clone(&ports.cpu),
            writer,
            crate::packer::SEG_MAX,
        );
        let packed = t.pack(&root, &mut p).await.unwrap();
        p.finish().await.unwrap();
        assert!(!packed.loc.is_pending());
        assert_eq!(
            p.stats().objects,
            4 + 1 + 3 + 3,
            "chunks, the root, and two nodes per lower level"
        );

        let c2 = Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            remote,
            Path::new("/b"),
        )
        .await
        .unwrap();
        let t2 = Tree::new(Arc::new(c2), 1 << 20);
        assert_eq!(
            t2.prefetch_nodes(&packed, 4).await.unwrap(),
            1,
            "every node is in one frame"
        );
        let e = t2.lookup(&packed, 70000, false).await.unwrap();
        assert_eq!(
            t2.cache().get(&e).await.unwrap(),
            Bytes::from(vec![70000u64.to_le_bytes()[0] + 1; CHUNK_SIZE])
        );

        let c3 = Cache::open_with(
            Arc::clone(&ports.fs),
            Arc::clone(&ports.cpu),
            t2.cache().remote().clone(),
            Path::new("/c"),
        )
        .await
        .unwrap();
        let t3 = Tree::new(Arc::new(c3), 1 << 20);
        assert_eq!(
            t3.hydrate(&packed, 4).await.unwrap(),
            0,
            "the node frame held every chunk"
        );
        for (_, e) in &ups {
            assert!(t3.cache().has(&e.hash).await);
        }
    }

    #[tokio::test]
    async fn a_change_list_rebuilds_the_root_and_a_bad_one_fails() {
        let t = tree(2).await;
        let from = t
            .update(&Entry::EMPTY, &[(0, leaf(1)), (70000, leaf(2))])
            .await
            .unwrap();
        let root = t
            .update(&from, &[(5, leaf(3)), (70000, Entry::EMPTY)])
            .await
            .unwrap();
        let cl = t.change_list(&from, &root, 4, |_| None).await.unwrap();
        let cl = ChangeList::decode(&cl.encode()).unwrap();

        let other = tree(3).await;
        let copy = other
            .update(&Entry::EMPTY, &[(0, leaf(1)), (70000, leaf(2))])
            .await
            .unwrap();
        assert_eq!(copy, from);
        let changes = other.apply_change_list(&from, &root, cl.clone()).await.unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(other.lookup_nowait(&root, 5), Some(leaf(3)));

        let mut bad = cl;
        bad.changes[0].entry = leaf(9);
        assert!(other.apply_change_list(&from, &root, bad).await.is_err());
    }
}
