use crate::{Entry, FormatError};

/// A branch: its size, its root, and the generation of its current writer. Opening a branch
/// for writing takes generation + 1, which fences every older writer at once. `from` names the
/// ref the branch was forked from, so hosts can follow it.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Ref {
    pub size: u64,
    pub root: Entry,
    pub generation: u64,
    pub from: Option<String>,
}

impl Ref {
    /// `<size> <generation> <root entry>\n`, then `from <ref>\n` when forked.
    pub fn encode(&self) -> Vec<u8> {
        let mut s = format!("{} {} {}\n", self.size, self.generation, self.root);
        if let Some(from) = &self.from {
            s += &format!("from {from}\n");
        }
        s.into_bytes()
    }

    pub fn decode(b: &[u8]) -> Result<Ref, FormatError> {
        let bad = |why: &str| FormatError::malformed("ref", why.to_string());
        let s = std::str::from_utf8(b).map_err(|_| bad("not UTF-8"))?.trim();
        let (first, rest) = s.split_once('\n').unwrap_or((s, ""));
        let mut f = first.splitn(3, ' ');
        let (Some(size), Some(generation), Some(root)) = (f.next(), f.next(), f.next()) else {
            return Err(bad("want size, generation and root"));
        };
        let rest = rest.trim();
        let from = match rest {
            "" => None,
            _ => Some(
                rest.strip_prefix("from ")
                    .ok_or_else(|| bad("unknown line"))?
                    .to_string(),
            ),
        };
        Ok(Ref {
            size: size.parse().map_err(|_| bad("size"))?,
            generation: generation.parse().map_err(|_| bad("generation"))?,
            root: Entry::parse_text(root)?,
            from,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn ref_round_trips_with_and_without_from() {
        for from in [None, Some("main".to_string())] {
            let r = Ref {
                size: 1 << 30,
                root: Entry {
                    hash: Hash::of(b"x"),
                    ..Entry::EMPTY
                },
                generation: 3,
                from,
            };
            assert_eq!(Ref::decode(&r.encode()).unwrap(), r);
        }
    }

    #[test]
    fn ref_with_an_unknown_line_is_rejected() {
        let mut b = Ref::default().encode();
        b.extend_from_slice(b"junk\n");
        assert!(Ref::decode(&b).is_err());
    }
}
