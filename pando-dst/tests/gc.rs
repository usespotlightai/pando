//! GC racing branch operations and writers, over many seeds.

use pando_dst::gc_race::{self, GcRaces, VALIDATE};

fn seeds() -> std::ops::Range<u64> {
    let var = |name: &str, or: u64| {
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(or)
    };
    let first = var("PANDO_DST_FIRST", 0);
    first..first + var("PANDO_DST_SEEDS", 20)
}

#[tokio::test(start_paused = true)]
async fn gc_never_deletes_what_a_branch_a_checkpoint_or_a_writer_needs() {
    let mut total = GcRaces::default();
    for seed in seeds() {
        let r = gc_race::gc_races(seed, 24, Some(VALIDATE))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        total.add(&r);
    }
    assert!(
        total.deleted > 0
            && total.revived > 0
            && total.current_writer > 0
            && total.undone > 0
            && total.held_publishes > 0
            && total.fenced > 0,
        "{total:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_slow_copy_left_in_place_names_what_gc_deleted() {
    for seed in 0..40 {
        if let Err(e) = gc_race::gc_races(seed, 24, None).await {
            assert!(
                e.contains("does not read whole") || e.contains("gc:"),
                "seed {seed}: {e}"
            );
            return;
        }
    }
    panic!("no slow copy named a deleted segment in 40 seeds");
}
