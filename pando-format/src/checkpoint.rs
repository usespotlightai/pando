use crate::{Entry, FormatError};

/// A checkpoint: a branch's size and published root, frozen under a name. A checkpoint is
/// created once and never changes, so a host may keep it and branch from it without reading
/// it first. `branch` names the branch it was taken from.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Checkpoint {
    /// The branch's size in bytes.
    pub size: u64,
    /// The root of the branch's tree.
    pub root: Entry,
    /// The branch the checkpoint was taken from.
    pub branch: String,
}

impl Checkpoint {
    /// `checkpoint <size> <root entry>\n`, then `branch <name>\n`.
    pub fn encode(&self) -> Vec<u8> {
        format!("checkpoint {} {}\nbranch {}\n", self.size, self.root, self.branch).into_bytes()
    }

    /// Decodes [`Self::encode`]'s form.
    pub fn decode(b: &[u8]) -> Result<Checkpoint, FormatError> {
        let bad = |why: &str| FormatError::malformed("checkpoint", why.to_string());
        let s = std::str::from_utf8(b).map_err(|_| bad("not UTF-8"))?;
        let mut lines = s
            .strip_suffix('\n')
            .ok_or_else(|| bad("no final newline"))?
            .split('\n');
        let (Some(first), Some(second), None) = (lines.next(), lines.next(), lines.next()) else {
            return Err(bad("want two lines"));
        };
        let (size, root) = first
            .strip_prefix("checkpoint ")
            .and_then(|r| r.split_once(' '))
            .ok_or_else(|| bad("want size and root"))?;
        let branch = second.strip_prefix("branch ").ok_or_else(|| bad("want branch"))?;
        if branch.is_empty() {
            return Err(bad("empty branch"));
        }
        Ok(Checkpoint {
            size: size.parse().map_err(|_| bad("size"))?,
            root: Entry::parse_text(root)?,
            branch: branch.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    fn sample() -> Checkpoint {
        Checkpoint {
            size: 4 << 30,
            root: Entry {
                hash: Hash::of(b"root"),
                ..Entry::EMPTY
            },
            branch: "main".to_owned(),
        }
    }

    #[test]
    fn checkpoint_round_trips() {
        let c = sample();
        assert_eq!(Checkpoint::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn a_checkpoint_with_another_line_is_rejected() {
        let mut b = sample().encode();
        b.extend_from_slice(b"junk\n");
        assert!(Checkpoint::decode(&b).is_err());
        assert!(Checkpoint::decode(b"checkpoint 1\nbranch main\n").is_err());
        assert!(Checkpoint::decode(&sample().encode()[1..]).is_err());
    }
}
