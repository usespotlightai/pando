//! The exit statuses that tell a caller why a command failed, as `docs/cli.md` lists them.

use pando_engine::Error;
use pando_engine::ports::StoreError;

use crate::Drained;
use crate::device::StillPublishing;

/// Why a command failed, as its exit status says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// Anything the other kinds do not cover. Status 1.
    Other,
    /// A device, mount or cache another process holds. Status 3.
    InUse,
    /// The store did not answer, or kept failing in a way a retry may fix. Status 4.
    StoreUnreachable,
    /// Another writer took the branch; this one must stop. Status 5.
    Fenced,
    /// This kernel or host lacks what the command needs. Status 6.
    Unsupported,
    /// The work goes on and finishes later: an attach that drained, or a detach that ran out of
    /// `--publish-wait`. Status 75.
    TryAgain,
}

impl Failure {
    /// The kind of failure `e` is: the first error in its chain that has a kind.
    pub(crate) fn of(e: &anyhow::Error) -> Failure {
        e.chain().find_map(kind).unwrap_or(Failure::Other)
    }

    /// The exit status.
    pub(crate) fn code(self) -> u8 {
        match self {
            Failure::Other => 1,
            Failure::InUse => 3,
            Failure::StoreUnreachable => 4,
            Failure::Fenced => 5,
            Failure::Unsupported => 6,
            Failure::TryAgain => crate::device::TRY_AGAIN,
        }
    }

    /// The name `--json` gives the failure.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Failure::Other => "failed",
            Failure::InUse => "in-use",
            Failure::StoreUnreachable => "store-unreachable",
            Failure::Fenced => "fenced",
            Failure::Unsupported => "unsupported",
            Failure::TryAgain => "try-again",
        }
    }

    /// The `--json` line that reports `e`.
    pub(crate) fn event(e: &anyhow::Error) -> serde_json::Value {
        let f = Failure::of(e);
        serde_json::json!({
            "event": "error",
            "kind": f.name(),
            "code": f.code(),
            "message": format!("{e:#}"),
        })
    }
}

fn kind(e: &(dyn std::error::Error + 'static)) -> Option<Failure> {
    if e.is::<InUse>() {
        return Some(Failure::InUse);
    }
    if e.is::<Unsupported>() {
        return Some(Failure::Unsupported);
    }
    if e.is::<StillPublishing>() || e.is::<Drained>() {
        return Some(Failure::TryAgain);
    }
    if let Some(e) = e.downcast_ref::<Error>() {
        if let Error::Store(s) = e.kind() {
            return store(s);
        }
        return matches!(e.kind(), Error::Fenced { .. }).then_some(Failure::Fenced);
    }
    if let Some(s) = e.downcast_ref::<StoreError>() {
        return store(s);
    }
    e.downcast_ref::<std::io::Error>()
        .filter(|e| e.kind() == std::io::ErrorKind::ResourceBusy)
        .map(|_| Failure::InUse)
}

fn store(e: &StoreError) -> Option<Failure> {
    matches!(e, StoreError::Transient { .. }).then_some(Failure::StoreUnreachable)
}

/// A device, mount or file another process holds, so the command left it as it was.
#[derive(Debug)]
pub(crate) struct InUse(pub(crate) String);

impl std::fmt::Display for InUse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InUse {}

/// What this kernel or host lacks for the command, and what to do about it.
#[derive(Debug)]
pub(crate) struct Unsupported(pub(crate) String);

impl std::fmt::Display for Unsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unsupported {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Context;

    use super::*;

    fn transient() -> StoreError {
        StoreError::Transient {
            key: "refs/main".into(),
            source: "connection refused".into(),
        }
    }

    #[test]
    fn each_failure_has_its_own_status_through_any_context() {
        let of = |e: anyhow::Error| Failure::of(&e).code();
        assert_eq!(of(anyhow::anyhow!("no")), 1);
        assert_eq!(of(anyhow::Error::new(InUse("/dev/ublkb1 is in use".into()))), 3);
        assert_eq!(of(Err::<(), _>(transient()).context("list refs").unwrap_err()), 4);
        assert_eq!(
            of(anyhow::Error::new(Error::Shared(Arc::new(Error::Store(transient())))).context("open main")),
            4
        );
        let fenced = Error::Fenced {
            branch: "main".into(),
        };
        assert_eq!(of(anyhow::Error::new(fenced).context("publish main")), 5);
        assert_eq!(of(anyhow::Error::new(Unsupported("no ublk".into()))), 6);
        assert_eq!(of(anyhow::Error::new(Drained)), 75);
        let permanent = StoreError::Permanent {
            key: "refs/main".into(),
            source: "access denied".into(),
        };
        assert_eq!(of(anyhow::Error::new(Error::Store(permanent))), 1);
        let busy = std::io::Error::new(std::io::ErrorKind::ResourceBusy, "held");
        assert_eq!(of(anyhow::Error::new(busy).context("open the cache")), 3);
    }

    #[test]
    fn the_error_line_names_the_failure() {
        let e = anyhow::Error::new(Unsupported("no ublk".into())).context("attach main");
        assert_eq!(
            Failure::event(&e),
            serde_json::json!({
                "event": "error",
                "kind": "unsupported",
                "code": 6,
                "message": "attach main: no ublk",
            })
        );
    }
}
