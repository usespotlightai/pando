//! The Pando store format.
//!
//! Everything a writer puts in the object store and a reader takes out of it: content hashes,
//! segment and writer ids, entries, refs, tree nodes, zstd frames, segment indexes, recipes,
//! change lists, block keys and the object key layout. The crate does no I/O and has no clock.
//!
//! Bytes that come from the store reach the rest of Pando only as [`Verified`], which only a
//! hash check creates.

mod block;
mod change_list;
mod entry;
mod error;
mod frame;
mod hash;
mod ids;
mod index;
pub mod keys;
mod node;
mod recipe;
mod recording;
mod reference;
mod verified;

pub use block::{BLOCK_SIZE, BLOCKS_PER_CHUNK, BlockKey, BlockKeys, CHUNK_SIZE, is_zero};
pub use change_list::{CHANGE_LIST_MAGIC, Change, ChangeList, NodeAt};
pub use entry::{ENTRY_SIZE, Entry, Loc};
pub use error::{FormatError, IntegrityError};
pub use frame::{DecodedFrame, FRAME_MAX, FrameBuilder, FrameObject, ZSTD_LEVEL, decode_frame, encode_frame};
pub use hash::Hash;
pub use ids::{SegId, Writer, WriterId};
pub use index::SegmentIndex;
pub use node::{FANOUT, Node, depth_for, digit};
pub use recipe::{Pick, RECIPE_SOURCES_MAX, Recipe, RecipeError, Run};
pub use recording::{RECORDING_MAGIC, Recording};
pub use reference::Ref;
pub use verified::Verified;
