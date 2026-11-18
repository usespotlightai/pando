//! Replays the inputs in `fuzz/regressions`, each of which once broke a decoder, and checks
//! that the decoder now rejects it.

#![cfg(test)]
#![expect(clippy::disallowed_methods, reason = "the tests read the regression files")]

use std::path::PathBuf;

use pando_format::{Hash, Node, Recipe, decode_frame};

fn inputs(target: &str) -> Vec<(String, Vec<u8>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fuzz/regressions")
        .join(target);
    let mut found: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read(&path).unwrap())
        })
        .collect();
    found.sort();
    assert!(!found.is_empty(), "no inputs in {}", dir.display());
    found
}

#[test]
fn nodes_with_an_empty_child_are_rejected() {
    for (name, b) in inputs("node") {
        assert!(Node::decode(&b).is_err(), "{name} decoded");
    }
}

#[test]
fn recipes_naming_more_sources_than_runs_can_reach_are_rejected() {
    for (name, b) in inputs("recipe") {
        assert!(Recipe::parse(&b).is_err(), "{name} parsed");
    }
}

#[test]
fn frames_claiming_more_payload_than_they_hold_are_rejected() {
    for (name, b) in inputs("frame") {
        let (&sel, z) = b.split_first().unwrap();
        let keep = |h: &Hash| h.0[0] < sel;
        assert!(decode_frame(z, Some(&keep)).is_err(), "{name} decoded in part");
        assert!(decode_frame(z, None).is_err(), "{name} decoded whole");
    }
}
