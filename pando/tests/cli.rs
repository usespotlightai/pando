//! The parts of the contract in docs/cli.md that hold without a device or a store; the
//! kernel test tests/kernel/cli.sh covers the rest.

use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

fn pando(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_pando"))
        .args(args)
        .env_clear()
        .env("S3_BUCKET", "pando-cli-test")
        .env("S3_ENDPOINT", "https://127.0.0.1:9")
        .env("S3_ACCESS_KEY_ID", "test")
        .env("S3_SECRET_ACCESS_KEY", "test")
        .output()
}

fn lines(o: &Output) -> Result<Vec<Value>, String> {
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).map_err(|e| format!("{l:?} is not JSON: {e}")))
        .collect()
}

#[test]
fn check_host_reports_each_check_then_whether_attach_works() {
    let o = pando(&["--json", "check-host"]).unwrap();
    let events = lines(&o).unwrap();
    let host = events.iter().find(|e| e["event"] == "host").expect("a host line");
    let works = host["ublk"] == true || host["fuse"] == true;
    let checks: Vec<&str> = events
        .iter()
        .filter(|e| e["event"] == "check")
        .map(|e| e["check"].as_str().unwrap())
        .collect();
    if cfg!(target_os = "linux") {
        assert_eq!(
            checks,
            ["ublk", "device-mapper", "loop", "fuse", "user-recovery", "root"]
        );
    }
    for e in events.iter().filter(|e| e["event"] == "check") {
        assert_eq!(e["ok"] == false, e["fix"].is_string(), "{e}");
    }
    if works {
        assert_eq!(o.status.code(), Some(0));
    } else {
        assert_eq!(o.status.code(), Some(6));
        assert_eq!(events.last().unwrap()["kind"], "unsupported");
    }
}

#[test]
fn attach_without_ublk_fails_as_unsupported_before_it_touches_the_store() {
    if Path::new("/dev/ublk-control").exists() {
        eprintln!("skipped: this host has ublk; CI's test container, which has none, runs this test");
        return;
    }
    let cache = tempfile::tempdir().unwrap();
    let o = pando(&[
        "--json",
        "--cache",
        cache.path().to_str().unwrap(),
        "attach",
        "main",
        "--device",
        "/dev/ublkb999",
    ])
    .unwrap();
    assert_eq!(o.status.code(), Some(6), "{}", String::from_utf8_lossy(&o.stderr));
    let events = lines(&o).unwrap();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["event"], "error");
    assert_eq!(events[0]["kind"], "unsupported");
    assert_eq!(events[0]["code"], 6);
}

#[test]
fn arguments_that_do_not_parse_exit_with_2() {
    assert_eq!(pando(&["--json", "attach"]).unwrap().status.code(), Some(2));
}
