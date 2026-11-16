//! The owner record next to a handoff image: the writer generation that wrote the image, the
//! base it was cloned from, and whether the device moved to it. A host that restarts reads it
//! to decide whether an image left behind holds writes the store lacks.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use pando_format::Hash;

use crate::local::{read_file, write_durable};
use crate::ports::LocalFs;
use crate::{Error, Result};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Owner {
    pub(crate) generation: u64,
    pub(crate) base: Option<Hash>,
    /// The handoff copied and flushed everything and was about to move the device to the
    /// image. Without it the image is a partial copy, older than the cache and the journal.
    pub(crate) switched: bool,
}

pub(crate) fn path(image: &Path) -> PathBuf {
    let mut s = image.as_os_str().to_owned();
    s.push(".owner");
    PathBuf::from(s)
}

impl Owner {
    fn encode(&self) -> String {
        let mut s = format!("generation {}\n", self.generation);
        if let Some(b) = self.base {
            s += &format!("base {b}\n");
        }
        if self.switched {
            s += "switched\n";
        }
        s
    }

    fn decode(s: &str) -> Option<Owner> {
        let mut o = Owner {
            generation: 0,
            base: None,
            switched: false,
        };
        let mut generation = None;
        for line in s.lines() {
            match line.split_once(' ').unwrap_or((line, "")) {
                ("generation", v) => generation = Some(v.parse().ok()?),
                ("base", v) => o.base = Some(v.parse().ok()?),
                ("switched", "") => o.switched = true,
                _ => return None,
            }
        }
        o.generation = generation?;
        Some(o)
    }

    pub(crate) async fn write(&self, fs: &dyn LocalFs, image: &Path) -> Result<()> {
        Ok(write_durable(fs, &path(image), Bytes::from(self.encode())).await?)
    }

    pub(crate) async fn read(fs: &dyn LocalFs, image: &Path) -> Result<Option<Owner>> {
        let Some(b) = read_file(fs, &path(image)).await? else {
            return Ok(None);
        };
        let s = std::str::from_utf8(&b)
            .map_err(|_| Error::Corrupt(format!("{}: owner is not text", image.display())))?;
        Owner::decode(s)
            .map(Some)
            .ok_or_else(|| Error::Corrupt(format!("{}: malformed owner record", image.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_owner_record_round_trips() {
        for o in [
            Owner {
                generation: 7,
                base: None,
                switched: false,
            },
            Owner {
                generation: 1,
                base: Some(Hash::of(b"x")),
                switched: true,
            },
        ] {
            assert_eq!(Owner::decode(&o.encode()), Some(o));
        }
        assert_eq!(Owner::decode("generation x\n"), None);
    }
}
