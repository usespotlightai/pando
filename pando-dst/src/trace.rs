//! Traces of one branch's ref and write log: every call a host makes on them, in the order the
//! store applies them, and each flush a writer commits, written as a TLA+ module that
//! `specs/trace/WriteLogTrace.tla` checks against `specs/WriteLog.tla`. The branch's lives are
//! numbered from 0 in the order their refs were created, as the spec numbers them.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use pando_engine::LogMode;
use pando_engine::ports::{Object, ObjectInfo, Store, StoreError, Version};
use pando_format::{BranchId, LogBody, LogPos, LogRecord, Ref, keys, u64_from};

/// What a log record holds, as a trace names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A seal.
    Seal,
    /// The writes of flushes `from + 1` to `to`.
    Writes {
        /// The last flush before the record's.
        from: u64,
        /// The record's last flush.
        to: u64,
    },
}

/// One step a host took on the branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    /// The host that took it.
    pub host: String,
    /// What it did.
    pub step: Step,
}

/// A call on the branch's ref or log, a commit, or the end of a host's process. Records are
/// named by the life of the branch, writer generation, then number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// An open's read of the ref, with the mode the open logs in.
    GetRef(LogMode),
    /// A delete's read of the ref.
    DeleteGet,
    /// The delete of the ref.
    DeleteRef,
    /// The create of the ref of a new life of the branch.
    CreateRef,
    /// A read of a record, and what it found.
    GetLog(u64, u64, u64, Option<Kind>),
    /// A create of a record holding a [`Kind`]; true when no record was there.
    CreateLog(u64, u64, u64, Kind, bool),
    /// A conditional update of the ref to a writer generation and log position; true when the
    /// ref still had the version the host named. An advance also names the flushes of the
    /// root it moves the ref to, in the order they were committed.
    UpdateRef(u64, LogPos, bool, Vec<u64>),
    /// A delete of a record.
    DeleteLog(u64, u64, u64),
    /// A list of the branch's log.
    ListLog,
    /// The host's writer wrote the data of a flush.
    Write,
    /// The host's writer committed a flush.
    Flush,
    /// The host's process ended; its local state stays.
    Stop,
    /// The host is gone with its local state.
    Lose,
}

/// The events of one branch, from every host's [`TracedStore`].
#[derive(Debug)]
pub struct Tracer {
    reference: String,
    log: String,
    lives: Mutex<Vec<BranchId>>,
    events: Mutex<Vec<Event>>,
    unsupported: Mutex<Option<String>>,
}

impl Tracer {
    /// A tracer of branch `branch`.
    pub fn new(branch: &str) -> Arc<Tracer> {
        Arc::new(Tracer {
            reference: keys::reference(branch),
            log: keys::log_name(branch),
            lives: Mutex::default(),
            events: Mutex::default(),
            unsupported: Mutex::default(),
        })
    }

    /// Records that `host` took `step`.
    pub fn push(&self, host: &str, step: Step) {
        self.insert(usize::MAX, host, step);
    }

    /// How many events there are so far.
    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    /// Whether there are no events yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Records that `host` took `step` when there were `at` events.
    pub fn insert(&self, at: usize, host: &str, step: Step) {
        let mut events = self.events.lock();
        let at = at.min(events.len());
        events.insert(
            at,
            Event {
                host: host.to_owned(),
                step,
            },
        );
    }

    /// The events so far.
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().clone()
    }

    /// Why the trace cannot be checked, when a call did something the spec has no step for.
    pub fn unsupported(&self) -> Option<String> {
        self.unsupported.lock().clone()
    }

    fn refuse(&self, why: String) {
        self.unsupported.lock().get_or_insert(why);
    }

    /// Numbers life `id` of the branch next, as the life its ref was created for.
    pub fn begin(&self, id: BranchId) {
        self.lives.lock().push(id);
    }

    fn number(&self, id: BranchId) -> u64 {
        let lives = self.lives.lock();
        match lives.iter().position(|&l| l == id) {
            Some(i) => u64_from(i),
            None => {
                drop(lives);
                self.refuse(format!("{id}: a life of the branch with no create"));
                0
            }
        }
    }

    fn life(&self, dir: &str) -> Option<BranchId> {
        dir.strip_prefix(&self.log)?.strip_prefix('/')?.parse().ok()
    }

    fn record(&self, key: &str) -> Option<(u64, u64, u64)> {
        let (id, rest) = key.strip_prefix(&self.log)?.strip_prefix('/')?.split_once('/')?;
        let (g, n) = keys::parse_log_record(rest)?;
        Some((self.number(id.parse().ok()?), g, n))
    }
}

/// The store of one host: it passes every call on to the shared store and records the calls on
/// the traced branch's ref and log. The read of a record a create found taken belongs to that
/// create, and is not recorded.
#[derive(Debug)]
pub struct TracedStore {
    inner: Arc<dyn Store>,
    host: String,
    tracer: Arc<Tracer>,
    mode: Mutex<LogMode>,
    root: Mutex<Vec<u64>>,
    deleting: Mutex<bool>,
    read_back: Mutex<HashSet<String>>,
}

impl TracedStore {
    /// The store of host `host`.
    pub fn new(inner: Arc<dyn Store>, host: &str, tracer: Arc<Tracer>) -> Arc<TracedStore> {
        Arc::new(TracedStore {
            inner,
            host: host.to_owned(),
            tracer,
            mode: Mutex::new(LogMode::default()),
            root: Mutex::default(),
            deleting: Mutex::default(),
            read_back: Mutex::default(),
        })
    }

    /// Whether the calls of this host are a delete's, from its read of the ref to the create of
    /// the new life's.
    pub fn set_deleting(&self, on: bool) {
        *self.deleting.lock() = on;
    }

    /// The mode the next open on this host logs in.
    pub fn set_mode(&self, mode: LogMode) {
        *self.mode.lock() = mode;
    }

    /// The flushes of the root the next advance on this host moves the ref to.
    pub fn set_root(&self, root: Vec<u64>) {
        *self.root.lock() = root;
    }

    fn push(&self, step: Step) {
        self.tracer.push(&self.host, step);
    }

    fn kind(&self, key: &str, body: &[u8]) -> Kind {
        match LogRecord::decode(body).map(|r| r.body) {
            Ok(LogBody::Seal) => Kind::Seal,
            Ok(LogBody::Writes(w)) => Kind::Writes {
                from: w.from,
                to: w.to,
            },
            Err(e) => {
                self.tracer.refuse(format!("{key}: {e}"));
                Kind::Seal
            }
        }
    }

    fn failed(&self, key: &str, e: &StoreError) {
        self.tracer.refuse(format!("{key}: {e}"));
    }
}

#[async_trait]
impl Store for TracedStore {
    async fn get(&self, key: &str) -> Result<Object, StoreError> {
        let read_back = self.read_back.lock().remove(key);
        let r = self.inner.get(key).await;
        if read_back {
            return r;
        }
        if key == self.tracer.reference {
            if *self.deleting.lock() {
                self.push(Step::DeleteGet);
            } else {
                let mode = *self.mode.lock();
                self.push(Step::GetRef(mode));
            }
        } else if let Some((l, g, n)) = self.tracer.record(key) {
            let found = match &r {
                Ok(o) => Some(self.kind(key, &o.body)),
                Err(StoreError::NotFound { .. }) => None,
                Err(e) => {
                    self.failed(key, e);
                    None
                }
            };
            self.push(Step::GetLog(l, g, n, found));
        }
        r
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        self.inner.get_range(key, range).await
    }

    async fn get_tail(&self, key: &str, n: u64) -> Result<(Bytes, u64), StoreError> {
        self.inner.get_tail(key, n).await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        if key == self.tracer.reference || self.tracer.record(key).is_some() {
            self.tracer.refuse(format!("{key}: an unconditional put"));
        }
        self.inner.put(key, body).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<Version, StoreError> {
        if key == self.tracer.reference {
            let made = Ref::decode(&body);
            let r = self.inner.create(key, body).await;
            match (&r, made) {
                (Ok(_), Ok(made)) if *self.deleting.lock() => {
                    self.tracer.begin(made.id);
                    self.push(Step::CreateRef);
                }
                _ => self
                    .tracer
                    .refuse(format!("{key}: a create of the ref outside a delete")),
            }
            return r;
        }
        let Some((l, g, n)) = self.tracer.record(key) else {
            return self.inner.create(key, body).await;
        };
        let kind = self.kind(key, &body);
        let r = self.inner.create(key, body).await;
        let ok = match &r {
            Ok(_) => true,
            Err(StoreError::AlreadyExists { .. }) => {
                self.read_back.lock().insert(key.to_owned());
                false
            }
            Err(e) => {
                self.failed(key, e);
                false
            }
        };
        self.push(Step::CreateLog(l, g, n, kind, ok));
        r
    }

    async fn update(&self, key: &str, body: Bytes, expected: &Version) -> Result<Version, StoreError> {
        if key != self.tracer.reference {
            return self.inner.update(key, body, expected).await;
        }
        let wanted = Ref::decode(&body);
        let r = self.inner.update(key, body, expected).await;
        let ok = match &r {
            Ok(_) => true,
            Err(StoreError::PreconditionFailed { .. } | StoreError::NotFound { .. }) => false,
            Err(e) => {
                self.failed(key, e);
                false
            }
        };
        match wanted {
            Ok(w) => {
                let root = self.root.lock().clone();
                self.push(Step::UpdateRef(w.generation, w.log.unwrap_or_default(), ok, root));
            }
            Err(e) => self.tracer.refuse(format!("{key}: {e}")),
        }
        r
    }

    async fn list(&self, dir: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        let r = self.inner.list(dir).await;
        if self.tracer.life(dir).is_some() {
            self.push(Step::ListLog);
        }
        r
    }

    async fn keeps_versions(&self) -> Result<bool, StoreError> {
        self.inner.keeps_versions().await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let r = self.inner.delete(key).await;
        if key == self.tracer.reference {
            if *self.deleting.lock() && r.is_ok() {
                self.push(Step::DeleteRef);
            } else {
                self.tracer
                    .refuse(format!("{key}: a delete of the ref outside a delete"));
            }
        }
        if let Some((l, g, n)) = self.tracer.record(key) {
            self.push(Step::DeleteLog(l, g, n));
        }
        r
    }
}

fn tla_bool(b: bool) -> &'static str {
    if b { "TRUE" } else { "FALSE" }
}

/// One event as a TLA+ record. Every record has every field, so TLC compares them freely.
#[derive(Default)]
struct Row<'a> {
    op: &'a str,
    mode: &'a str,
    l: u64,
    g: u64,
    n: u64,
    found: Option<Kind>,
    ok: bool,
    generation: u64,
    log: LogPos,
    root: &'a [u64],
}

impl<'a> Row<'a> {
    fn of(step: &'a Step) -> Row<'a> {
        let op = |op| Row { op, ..Row::default() };
        match *step {
            Step::GetRef(mode) => Row {
                mode: match mode {
                    LogMode::Off => "off",
                    LogMode::Behind => "behind",
                    LogMode::Strict => "strict",
                },
                ..op("get_ref")
            },
            Step::DeleteGet => op("delete_get"),
            Step::DeleteRef => op("delete_ref"),
            Step::CreateRef => op("create_ref"),
            Step::GetLog(l, g, n, found) => Row {
                l,
                g,
                n,
                found,
                ..op("get_log")
            },
            Step::CreateLog(l, g, n, kind, ok) => Row {
                l,
                g,
                n,
                found: Some(kind),
                ok,
                ..op("create_log")
            },
            Step::UpdateRef(generation, log, ok, ref root) => Row {
                ok,
                generation,
                log,
                root,
                ..op("update_ref")
            },
            Step::DeleteLog(l, g, n) => Row {
                l,
                g,
                n,
                ..op("delete_log")
            },
            Step::ListLog => op("list_log"),
            Step::Flush => op("flush"),
            Step::Write => op("write"),
            Step::Stop => op("stop"),
            Step::Lose => op("lose"),
        }
    }

    fn tla(&self, host: &str) -> String {
        let (found, kind, from, to) = match self.found {
            None => (false, "none", 0, 0),
            Some(Kind::Seal) => (true, "seal", 0, 0),
            Some(Kind::Writes { from, to }) => (true, "writes", from, to),
        };
        let root: Vec<String> = self.root.iter().map(u64::to_string).collect();
        format!(
            "[op |-> \"{}\", host |-> \"{host}\", mode |-> \"{}\", l |-> {}, g |-> {}, n |-> {}, found |-> {}, \
             kind |-> \"{kind}\", from |-> {from}, to |-> {to}, ok |-> {}, gen |-> {}, lgen |-> {}, \
             lflush |-> {}, lnext |-> {}, lclosed |-> {}, root |-> <<{}>>]",
            self.op,
            self.mode,
            self.l,
            self.g,
            self.n,
            tla_bool(found),
            tla_bool(self.ok),
            self.generation,
            self.log.generation,
            self.log.flush,
            self.log.next,
            tla_bool(self.log.closed),
            root.join(", "),
        )
    }
}

/// The trace as module `TraceData`: the hosts, the host that runs advances and deletes, bounds
/// on opens, writes, advances and deletes, and the events.
pub fn module(
    hosts: &[&str],
    advancer: &str,
    bounds: (usize, usize, usize, usize),
    events: &[Event],
) -> String {
    let (opens, writes, advances, deletes) = bounds;
    let names: Vec<String> = hosts.iter().map(|h| format!("\"{h}\"")).collect();
    let mut out = String::from("---- MODULE TraceData ----\n");
    let _ = writeln!(out, "TraceHosts == {{{}}}", names.join(", "));
    let _ = writeln!(out, "TraceOpens == {opens}");
    let _ = writeln!(out, "TraceWrites == {writes}");
    let _ = writeln!(out, "TraceAdvancer == \"{advancer}\"");
    let _ = writeln!(out, "TraceAdvances == {advances}");
    let _ = writeln!(out, "TraceDeletes == {deletes}");
    out.push_str("Trace == <<\n");
    for (i, e) in events.iter().enumerate() {
        let sep = if i + 1 == events.len() { "" } else { "," };
        let _ = writeln!(out, "  {}{sep}", Row::of(&e.step).tla(&e.host));
    }
    out.push_str(">>\n====\n");
    out
}
