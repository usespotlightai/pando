use std::fmt::Write as _;

use crate::{BranchId, Entry, FormatError, LogPos};

/// A branch: its size, its root, and the generation of its current writer. Opening a branch
/// for writing takes generation + 1, which fences every older writer at once. `from` names the
/// ref the branch was forked from, so hosts can follow it. `log` is where the branch's write
/// log stands; a branch without it has no records the root lacks, except from its current
/// writer. `id` names this life of the branch: a branch made again under a deleted name has
/// another. `writer` is the token of the take that made the current writer the writer.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ref {
    /// The branch's size in bytes.
    pub size: u64,
    /// This life of the branch; [`BranchId::NONE`] only in a ref the Go proof of concept wrote.
    pub id: BranchId,
    /// The root of the branch's tree; empty for a branch with no data.
    pub root: Entry,
    /// The generation of the branch's current writer.
    pub generation: u64,
    /// The name of the ref the branch was forked from.
    pub from: Option<String>,
    /// Where the branch's write log stands; `None` for a branch that has never had a writer.
    pub log: Option<LogPos>,
    /// The token the current writer drew when it took the branch, so a writer that put the ref
    /// tells its own take from a rival's of the same generation; `None` for a branch that has
    /// never had a writer, and in a ref the Go proof of concept wrote.
    pub writer: Option<u64>,
}

impl Ref {
    /// `<size> <generation> <root entry>\n`, then `id <hex>\n` unless the id is
    /// [`BranchId::NONE`], then `from <ref>\n` when forked, then
    /// `log <generation> <flush> <next>\n`, with ` closed` before the newline when closed, when
    /// it has a log position, then `writer <16 hex digits>\n` when it has a writer token.
    pub fn encode(&self) -> Vec<u8> {
        let mut s = format!("{} {} {}\n", self.size, self.generation, self.root);
        if self.id != BranchId::NONE {
            s.push_str("id ");
            s.push_str(&self.id.to_string());
            s.push('\n');
        }
        if let Some(from) = &self.from {
            s.push_str("from ");
            s.push_str(from);
            s.push('\n');
        }
        if let Some(l) = &self.log {
            for part in [
                "log ",
                &l.generation.to_string(),
                " ",
                &l.flush.to_string(),
                " ",
                &l.next.to_string(),
            ] {
                s.push_str(part);
            }
            s.push_str(if l.closed { " closed\n" } else { "\n" });
        }
        if let Some(w) = self.writer {
            let _ = writeln!(s, "writer {w:016x}");
        }
        s.into_bytes()
    }

    /// Decodes the text form [`Ref::encode`] writes, ignoring surrounding whitespace.
    pub fn decode(b: &[u8]) -> Result<Ref, FormatError> {
        let bad = |why: &str| FormatError::malformed("ref", why.to_string());
        let s = std::str::from_utf8(b).map_err(|_| bad("not UTF-8"))?.trim();
        let mut lines = s.split('\n');
        let first = lines.next().unwrap_or_default();
        let mut f = first.splitn(3, ' ');
        let (Some(size), Some(generation), Some(root)) = (f.next(), f.next(), f.next()) else {
            return Err(bad("want size, generation and root"));
        };
        let (mut id, mut from, mut log, mut writer) = (None, None, None, None);
        for line in lines.map(str::trim).filter(|l| !l.is_empty()) {
            if let Some(i) = line.strip_prefix("id ")
                && id.is_none()
                && from.is_none()
                && log.is_none()
                && writer.is_none()
            {
                let i: BranchId = i.parse()?;
                if i == BranchId::NONE {
                    return Err(bad("zero id"));
                }
                id = Some(i);
            } else if let Some(f) = line.strip_prefix("from ")
                && from.is_none()
                && log.is_none()
                && writer.is_none()
            {
                from = Some(f.to_string());
            } else if let Some(l) = line.strip_prefix("log ")
                && log.is_none()
                && writer.is_none()
            {
                log = Some(log_pos(l).ok_or_else(|| bad("log position"))?);
            } else if let Some(w) = line.strip_prefix("writer ")
                && writer.is_none()
            {
                if w.len() != 16 || !w.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
                    return Err(bad("writer token"));
                }
                writer = Some(u64::from_str_radix(w, 16).map_err(|_| bad("writer token"))?);
            } else {
                return Err(bad("unknown line"));
            }
        }
        Ok(Ref {
            size: size.parse().map_err(|_| bad("size"))?,
            id: id.unwrap_or(BranchId::NONE),
            generation: generation.parse().map_err(|_| bad("generation"))?,
            root: Entry::parse_text(root)?,
            from,
            log,
            writer,
        })
    }
}

fn log_pos(s: &str) -> Option<LogPos> {
    let mut f = s.split(' ');
    let (generation, flush, next) = (
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
        f.next()?.parse().ok()?,
    );
    let closed = match f.next() {
        None => false,
        Some("closed") => true,
        Some(_) => return None,
    };
    if f.next().is_some() {
        return None;
    }
    Some(LogPos {
        generation,
        flush,
        next,
        closed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn ref_round_trips_with_and_without_from_and_log() {
        let logs = [
            None,
            Some(LogPos {
                generation: 3,
                flush: 17,
                next: 4,
                closed: false,
            }),
            Some(LogPos {
                generation: 2,
                flush: 0,
                next: 0,
                closed: true,
            }),
        ];
        for (id, from) in [
            (BranchId::NONE, None),
            (BranchId::from_parts(7, 9), None),
            (BranchId::from_parts(7, 9), Some("main".to_string())),
        ] {
            for (log, writer) in logs
                .into_iter()
                .flat_map(|l| [(l, None), (l, Some(0x0123_4567_89ab_cdef))])
            {
                let r = Ref {
                    size: 1 << 30,
                    id,
                    root: Entry {
                        hash: Hash::of(b"x"),
                        ..Entry::EMPTY
                    },
                    generation: 3,
                    from: from.clone(),
                    log,
                    writer,
                };
                assert_eq!(Ref::decode(&r.encode()).unwrap(), r);
            }
        }
    }

    #[test]
    fn ref_with_a_bad_log_line_is_rejected() {
        for line in [
            "log 1 2\n",
            "log 1 2 3 open\n",
            "log 1 2 3 closed x\n",
            "log a 2 3\n",
        ] {
            let mut b = Ref::default().encode();
            b.extend_from_slice(line.as_bytes());
            assert!(Ref::decode(&b).is_err(), "{line:?}");
        }
        let mut b = Ref::default().encode();
        b.extend_from_slice(b"log 1 2 3\nlog 1 2 3\n");
        assert!(Ref::decode(&b).is_err());
    }

    #[test]
    fn ref_with_a_bad_or_misplaced_writer_line_is_rejected() {
        for tail in [
            "writer 12\n",
            "writer 000000000000000g\n",
            "writer 000000000000000A\n",
            "writer +00000000000000a\n",
            "writer 0000000000000001\nwriter 0000000000000001\n",
            "writer 0000000000000001\nlog 1 2 3\n",
        ] {
            let mut b = Ref::default().encode();
            b.extend_from_slice(tail.as_bytes());
            assert!(Ref::decode(&b).is_err(), "{tail:?}");
        }
    }

    #[test]
    fn ref_with_a_bad_or_late_id_is_rejected() {
        for tail in [
            "id 00000000000000000000000000000000\n",
            "id 0001\n",
            "id 00000000000000070000000000000009\nid 00000000000000070000000000000009\n",
            "from main\nid 00000000000000070000000000000009\n",
        ] {
            let mut b = Ref::default().encode();
            b.extend_from_slice(tail.as_bytes());
            assert!(Ref::decode(&b).is_err(), "{tail:?}");
        }
    }

    #[test]
    fn ref_with_an_unknown_line_is_rejected() {
        let mut b = Ref::default().encode();
        b.extend_from_slice(b"junk\n");
        assert!(Ref::decode(&b).is_err());
    }
}
