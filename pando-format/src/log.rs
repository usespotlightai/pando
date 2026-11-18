use bytes::Bytes;

use crate::{CHUNK_SIZE, FormatError, Hash, Verified};

/// The first bytes of every log record.
pub const LOG_MAGIC: &[u8; 4] = b"plg1";
/// The zstd level log records are written with: a record is on the path of a durable flush,
/// so it favours speed.
pub const LOG_ZSTD_LEVEL: i32 = 1;

const HEADER: usize = 4 + 1 + 8 + 8;
const WRITES_HEADER: usize = 8 + 8 + 8 + 4;
const CHUNK_HEAD: usize = 8 + 1;
const SEAL: u8 = 1;
const WRITES: u8 = 0;

/// Where a branch's write log stands, as its ref records it. Records of writer `generation`
/// numbered below `next` hold only flushes up to `flush`, which the ref's root holds; with
/// `closed`, that writer appends no more records.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct LogPos {
    /// The writer whose records the position counts.
    pub generation: u64,
    /// The last flush of that writer the ref's root holds.
    pub flush: u64,
    /// The first record number that may hold a flush after `flush`.
    pub next: u64,
    /// Whether that writer appends no more records.
    pub closed: bool,
}

/// One record of a branch's write log, created once at
/// [`crate::keys::log_record`]`(branch, generation, n)`. A writer numbers its records from 0
/// without gaps. A seal ends a writer's log: a later writer creates it at the first free
/// number, so the earlier writer's next append fails.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogRecord {
    /// The writer that created the record.
    pub generation: u64,
    /// The record's number in that writer's log.
    pub n: u64,
    /// What the record holds.
    pub body: LogBody,
}

/// What a log record holds.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LogBody {
    /// The end of a writer's log.
    Seal,
    /// The chunks some flushes changed.
    Writes(LogWrites),
}

/// The chunks that flushes `from + 1` to `to` of the writer changed, each with its bytes as of
/// flush `to`, or `None` for a chunk that reads as zero. `size` is the branch's size at `to`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LogWrites {
    /// The last flush before the record's.
    pub from: u64,
    /// The record's last flush.
    pub to: u64,
    /// The branch's size at `to`.
    pub size: u64,
    /// Each changed chunk's index and bytes.
    pub chunks: Vec<(u64, Option<Verified>)>,
}

impl LogRecord {
    /// Magic, kind u8 (0 writes, 1 seal), generation u64 LE, n u64 LE. A writes record goes
    /// on with from, to and size u64 LE, the chunk count u32 LE and one zstd frame of
    /// `[index u64][has data u8][hash][64 KiB]` per chunk. Every record ends with the hash of
    /// the bytes before it.
    pub fn encode_seal(generation: u64, n: u64) -> Vec<u8> {
        let mut b = header(SEAL, generation, n);
        seal_with_hash(&mut b);
        b
    }

    /// The writes record of `chunks`, whose hashes name their bytes; see [`Self::encode_seal`].
    pub fn encode_writes(
        generation: u64,
        n: u64,
        from: u64,
        to: u64,
        size: u64,
        chunks: &[(u64, Option<(Hash, Bytes)>)],
    ) -> Result<Vec<u8>, FormatError> {
        let mut raw = Vec::with_capacity(chunks.len() * (CHUNK_HEAD + 32 + CHUNK_SIZE));
        for (idx, c) in chunks {
            raw.extend_from_slice(&idx.to_le_bytes());
            match c {
                None => raw.push(0),
                Some((h, data)) => {
                    if data.len() != CHUNK_SIZE {
                        return Err(FormatError::malformed("log record", "a chunk is not 64 KiB"));
                    }
                    raw.push(1);
                    raw.extend_from_slice(&h.0);
                    raw.extend_from_slice(data);
                }
            }
        }
        let z = zstd::bulk::compress(&raw, LOG_ZSTD_LEVEL)?;
        let mut b = header(WRITES, generation, n);
        b.reserve(WRITES_HEADER + z.len() + 32);
        for v in [from, to, size] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        let count = u32::try_from(chunks.len())
            .map_err(|_| FormatError::malformed("log record", "too many chunks"))?;
        b.extend_from_slice(&count.to_le_bytes());
        b.extend_from_slice(&z);
        seal_with_hash(&mut b);
        Ok(b)
    }

    /// Decodes a record and checks its hash and the hash of each chunk.
    pub fn decode(b: &[u8]) -> Result<LogRecord, FormatError> {
        let bad = |why: &str| FormatError::malformed("log record", why.to_string());
        if b.len() < HEADER + 32 || !b.starts_with(LOG_MAGIC) {
            return Err(FormatError::OtherFormat { what: "log record" });
        }
        let (body, tail) = b.split_last_chunk::<32>().ok_or_else(|| bad("short"))?;
        let want = Hash(*tail);
        if Hash::of(body) != want {
            return Err(crate::IntegrityError { expected: want }.into());
        }
        let mut r = Cursor(&body[LOG_MAGIC.len()..]);
        let kind = r.take::<1>().ok_or_else(|| bad("short header"))?[0];
        let generation = r.u64().ok_or_else(|| bad("short header"))?;
        let n = r.u64().ok_or_else(|| bad("short header"))?;
        let body = match kind {
            SEAL if r.0.is_empty() => LogBody::Seal,
            SEAL => return Err(bad("a seal with a body")),
            WRITES => LogBody::Writes(decode_writes(&mut r).ok_or_else(|| bad("short writes header"))??),
            _ => return Err(bad("unknown kind")),
        };
        Ok(LogRecord { generation, n, body })
    }
}

/// The body of a writes record after its header, or `None` when the fixed fields are cut.
fn decode_writes(r: &mut Cursor<'_>) -> Option<Result<LogWrites, FormatError>> {
    let (from, to, size) = (r.u64()?, r.u64()?, r.u64()?);
    let count = usize::try_from(u32::from_le_bytes(r.take()?)).ok()?;
    Some(
        writes_chunks(r.0, count)
            .map(|chunks| LogWrites {
                from,
                to,
                size,
                chunks,
            })
            .and_then(|w| {
                if w.from >= w.to {
                    return Err(FormatError::malformed(
                        "log record",
                        "empty flush range".to_string(),
                    ));
                }
                Ok(w)
            }),
    )
}

fn writes_chunks(frame: &[u8], count: usize) -> Result<Vec<(u64, Option<Verified>)>, FormatError> {
    let bad = |why: &str| FormatError::malformed("log record", why.to_string());
    let limit = count
        .checked_mul(CHUNK_HEAD + 32 + CHUNK_SIZE)
        .ok_or_else(|| bad("chunk count"))?;
    let raw = zstd::bulk::decompress(frame, limit)?;
    let mut r = Cursor(&raw);
    let mut chunks = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = r.u64().ok_or_else(|| bad("short chunk"))?;
        match r.take::<1>().ok_or_else(|| bad("short chunk"))? {
            [0] => chunks.push((idx, None)),
            [1] => {
                let h = Hash(r.take().ok_or_else(|| bad("short hash"))?);
                let data = r.bytes(CHUNK_SIZE).ok_or_else(|| bad("short chunk"))?;
                chunks.push((idx, Some(Verified::check(h, Bytes::copy_from_slice(data))?)));
            }
            _ => return Err(bad("bad chunk flag")),
        }
    }
    if !r.0.is_empty() {
        return Err(bad("trailing bytes"));
    }
    Ok(chunks)
}

/// Reads fixed-width fields from the front of a slice.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }

    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(n)?;
        self.0 = rest;
        Some(head)
    }
}

fn header(kind: u8, generation: u64, n: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(HEADER + 32);
    b.extend_from_slice(LOG_MAGIC);
    b.push(kind);
    b.extend_from_slice(&generation.to_le_bytes());
    b.extend_from_slice(&n.to_le_bytes());
    b
}

fn seal_with_hash(b: &mut Vec<u8>) {
    let h = Hash::of(b);
    b.extend_from_slice(&h.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(seed: u8) -> (Hash, Bytes) {
        let b = Bytes::from(
            (0..CHUNK_SIZE)
                .map(|i| u8::try_from(i % 256).unwrap().wrapping_add(seed))
                .collect::<Vec<u8>>(),
        );
        (Hash::of(&b), b)
    }

    #[test]
    fn records_round_trip() {
        let (h, b) = chunk(3);
        let enc =
            LogRecord::encode_writes(7, 2, 4, 9, 1 << 30, &[(5, Some((h, b.clone()))), (6, None)]).unwrap();
        let r = LogRecord::decode(&enc).unwrap();
        assert_eq!((r.generation, r.n), (7, 2));
        let LogBody::Writes(w) = r.body else {
            panic!("not writes")
        };
        assert_eq!((w.from, w.to, w.size), (4, 9, 1 << 30));
        assert_eq!(w.chunks.len(), 2);
        assert_eq!(w.chunks[0].0, 5);
        assert_eq!(w.chunks[0].1.as_ref().unwrap().bytes(), &b);
        assert_eq!(w.chunks[1], (6, None));

        let s = LogRecord::decode(&LogRecord::encode_seal(7, 3)).unwrap();
        assert_eq!((s.generation, s.n, s.body), (7, 3, LogBody::Seal));
    }

    #[test]
    fn a_damaged_record_is_rejected() {
        let enc = LogRecord::encode_writes(1, 0, 0, 1, 4096, &[(0, Some(chunk(1)))]).unwrap();
        for at in [0, 5, 30, enc.len() / 2, enc.len() - 1] {
            let mut b = enc.clone();
            b[at] ^= 1;
            assert!(LogRecord::decode(&b).is_err(), "flipped byte {at}");
        }
        assert!(LogRecord::decode(&enc[..enc.len() - 1]).is_err());
    }

    #[test]
    fn a_chunk_that_does_not_match_its_hash_is_rejected() {
        let (_, b) = chunk(1);
        let enc = LogRecord::encode_writes(1, 0, 0, 1, 4096, &[(0, Some((Hash::of(b"other"), b)))]).unwrap();
        assert!(LogRecord::decode(&enc).is_err());
    }
}
