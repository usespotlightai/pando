//! The seeded scenarios, over many seeds.

use pando_dst::scenario;

#[tokio::test(start_paused = true)]
async fn crash_and_restart_holds_for_many_seeds() {
    let seeds: u64 = std::env::var("PANDO_DST_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let mut total = scenario::Report::default();
    let first: u64 = std::env::var("PANDO_DST_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    for seed in first..first + seeds {
        let r = scenario::crash_and_restart(seed, 120)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        total.crashes += r.crashes;
        total.faults += r.faults;
        total.reads += r.reads;
    }
    assert!(
        total.crashes > 0 && total.faults > 0 && total.reads > 0,
        "{total:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn writers_taking_turns_fence_each_other() {
    for seed in 0..10 {
        scenario::writers_take_turns(seed, 6)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }
}

#[tokio::test(start_paused = true)]
async fn a_strict_log_survives_lost_hosts() {
    let mut flushes = 0;
    for seed in 0..20 {
        let r = scenario::hosts_lost_with_a_strict_log(seed, 4, 40)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        flushes += r.flushes;
    }
    assert!(flushes > 0);
}

#[tokio::test(start_paused = true)]
async fn a_refusing_store_stops_the_uploader_and_loses_nothing() {
    let mut total = scenario::Report::default();
    for seed in 0..20 {
        let r = scenario::a_refusing_store(seed, 60)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        total.checkpoints += r.checkpoints;
        total.faults += r.faults;
    }
    assert!(total.checkpoints > 0 && total.faults == 20, "{total:?}");
}

#[tokio::test(start_paused = true)]
async fn a_store_outage_fails_io_in_time_and_loses_nothing() {
    let mut total = scenario::Report::default();
    for seed in 0..20 {
        let r = scenario::a_store_outage(seed, 60)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        total.writes += r.writes;
        total.flushes += r.flushes;
        total.reads += r.reads;
        total.unanswered += r.unanswered;
    }
    assert!(
        total.unanswered > 0 && total.reads > 0 && total.writes > 0 && total.flushes > 0,
        "{total:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn racing_hosts_open_prefixes_and_keep_strict_flushes() {
    use pando_dst::race;
    let seeds: u64 = std::env::var("PANDO_DST_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let first: u64 = std::env::var("PANDO_DST_FIRST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let traces = std::env::var_os("PANDO_TLA_TRACES").map(std::path::PathBuf::from);
    let mut total = race::Race::default();
    for seed in first..first + seeds {
        let r = race::race(seed, 160).await.unwrap_or_else(|e| panic!("{e}"));
        if let (Some(dir), Some(m)) = (&traces, &r.module) {
            let dir = dir.join(format!("race-{seed}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("TraceData.tla"), m).unwrap();
        }
        assert!(r.unsupported.is_none(), "seed {seed}: {:?}", r.unsupported);
        total.opens += r.opens;
        total.taken += r.taken;
        total.flushes += r.flushes;
        total.strict += r.strict;
        total.scans += r.scans;
        total.seals += r.seals;
        total.refused_appends += r.refused_appends;
        total.lost_takes += r.lost_takes;
        total.advances += r.advances;
        total.advance_seals += r.advance_seals;
    }
    eprintln!("{total:?}");
    assert!(
        total.scans > 0
            && total.seals > 0
            && total.strict > 0
            && total.refused_appends > 0
            && total.lost_takes > 0
            && total.advances > 0
            && total.advance_seals > 0
            && total.taken < total.opens,
        "{total:?}"
    );
}
