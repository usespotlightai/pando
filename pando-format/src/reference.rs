use crate::{Entry, FormatError, LogPos};

/// A branch: its size, its root, and the generation of its current writer. Opening a branch
/// for writing takes generation + 1, which fences every older writer at once. `from` names the
/// ref the branch was forked from, so hosts can follow it. `log` is where the branch's write
/// log stands; a branch without it has no records the root lacks, except from its current
/// writer.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ref {
    /// The branch's size in bytes.
    pub size: u64,
    /// The root of the branch's tree; empty for a branch with no data.
    pub root: Entry,
    /// The generation of the branch's current writer.
    pub generation: u64,
    /// The name of the ref the branch was forked from.
    pub from: Option<String>,
    /// Where the branch's write log stands; `None` for a branch that has never had a writer.
    pub log: Option<LogPos>,
}

impl Ref {
    /// `<size> <generation> <root entry>\n`, then `from <ref>\n` when forked, then
    /// `log <generation> <flush> <next>\n`, with ` closed` before the newline when closed, when
    /// it has a log position.
    pub fn encode(&self) -> Vec<u8> {
        let mut s = format!("{} {} {}\n", self.size, self.generation, self.root);
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
        let (mut from, mut log) = (None, None);
        for line in lines.map(str::trim).filter(|l| !l.is_empty()) {
            if let Some(f) = line.strip_prefix("from ")
                && from.is_none()
                && log.is_none()
            {
                from = Some(f.to_string());
            } else if let Some(l) = line.strip_prefix("log ")
                && log.is_none()
            {
                log = Some(log_pos(l).ok_or_else(|| bad("log position"))?);
            } else {
                return Err(bad("unknown line"));
            }
        }
        Ok(Ref {
            size: size.parse().map_err(|_| bad("size"))?,
            generation: generation.parse().map_err(|_| bad("generation"))?,
            root: Entry::parse_text(root)?,
            from,
            log,
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
        for from in [None, Some("main".to_string())] {
            for log in logs {
                let r = Ref {
                    size: 1 << 30,
                    root: Entry {
                        hash: Hash::of(b"x"),
                        ..Entry::EMPTY
                    },
                    generation: 3,
                    from: from.clone(),
                    log,
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
    fn ref_with_an_unknown_line_is_rejected() {
        let mut b = Ref::default().encode();
        b.extend_from_slice(b"junk\n");
        assert!(Ref::decode(&b).is_err());
    }
}
