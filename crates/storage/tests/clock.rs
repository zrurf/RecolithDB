use recolith_common::CommitTs;
use recolith_storage::CommitClock;

#[test]
fn reserves_monotonic_timestamps() {
    let clock = CommitClock::new(CommitTs::new(10));

    assert_eq!(clock.reserve(), CommitTs::new(11));
    assert_eq!(clock.reserve(), CommitTs::new(12));
}

#[test]
fn observe_advances_high_water_mark() {
    let clock = CommitClock::default();

    clock.observe(CommitTs::new(100));
    assert_eq!(clock.high_water_mark(), CommitTs::new(100));
    assert_eq!(clock.reserve(), CommitTs::new(101));
}
