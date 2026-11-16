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
