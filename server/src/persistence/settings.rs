use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn next_wal_sync_sequence() -> u64 {
    wal_write_sequence().fetch_add(1, Ordering::Relaxed) + 1
}

pub(super) fn next_wal_sync_sequence_batch_start(batch_size: u64) -> u64 {
    wal_write_sequence().fetch_add(batch_size, Ordering::Relaxed) + 1
}

pub(super) fn should_sync_this_write(
    sync_on_write: bool,
    sync_every_n_writes: u64,
    write_seq: u64,
) -> bool {
    if sync_on_write {
        return true;
    }
    if sync_every_n_writes == 0 {
        return false;
    }
    write_seq.checked_rem(sync_every_n_writes) == Some(0)
}

pub(super) fn batch_should_sync(
    sync_on_write: bool,
    sync_every_n_writes: u64,
    seq_start: u64,
    batch_size: u64,
) -> bool {
    if sync_on_write {
        return true;
    }
    if sync_every_n_writes == 0 || batch_size == 0 {
        return false;
    }
    let seq_end = seq_start.saturating_add(batch_size.saturating_sub(1));
    let remainder = seq_start.checked_rem(sync_every_n_writes).unwrap_or(0);
    let first_multiple = if remainder == 0 {
        seq_start
    } else {
        seq_start.saturating_add(sync_every_n_writes - remainder)
    };
    first_multiple <= seq_end
}

pub(super) fn sync_due_by_interval(sync_interval_seconds: u64, now_seconds: u64) -> bool {
    if sync_interval_seconds == 0 || now_seconds == 0 {
        return false;
    }

    let last = wal_last_sync_unix_seconds().load(Ordering::Relaxed);
    if last == 0 {
        let _ = wal_last_sync_unix_seconds().compare_exchange(
            0,
            now_seconds,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        return false;
    }

    now_seconds.saturating_sub(last) >= sync_interval_seconds
}

pub(super) fn record_successful_sync(now_seconds: u64) {
    if now_seconds == 0 {
        return;
    }
    wal_last_sync_unix_seconds().store(now_seconds, Ordering::Relaxed);
}

pub(super) fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn wal_write_sequence() -> &'static AtomicU64 {
    static WAL_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    &WAL_WRITE_SEQUENCE
}

fn wal_last_sync_unix_seconds() -> &'static AtomicU64 {
    static WAL_LAST_SYNC_UNIX_SECONDS: AtomicU64 = AtomicU64::new(0);
    &WAL_LAST_SYNC_UNIX_SECONDS
}

#[cfg(test)]
pub(super) fn reset_sync_state_for_tests() {
    wal_last_sync_unix_seconds().store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::{
        batch_should_sync, record_successful_sync, reset_sync_state_for_tests,
        should_sync_this_write, sync_due_by_interval,
    };

    fn sync_state_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn wal_sync_policy_prefers_explicit_sync_on_write() {
        assert!(should_sync_this_write(true, 0, 1));
        assert!(should_sync_this_write(true, 32, 7));
    }

    #[test]
    fn wal_sync_policy_supports_periodic_sync_when_disabled() {
        assert!(!should_sync_this_write(false, 0, 1));
        assert!(!should_sync_this_write(false, 3, 1));
        assert!(!should_sync_this_write(false, 3, 2));
        assert!(should_sync_this_write(false, 3, 3));
        assert!(should_sync_this_write(false, 3, 6));
    }

    #[test]
    fn wal_batch_sync_detects_multiples_inside_range() {
        assert!(!batch_should_sync(false, 4, 1, 3));
        assert!(batch_should_sync(false, 4, 2, 3));
        assert!(batch_should_sync(false, 4, 4, 1));
        assert!(batch_should_sync(true, 0, 1, 8));
    }

    #[test]
    fn wal_interval_sync_is_disabled_when_interval_is_zero() {
        let _guard = sync_state_test_guard();
        reset_sync_state_for_tests();
        assert!(!sync_due_by_interval(0, 1_000));
    }

    #[test]
    fn wal_interval_sync_waits_for_next_window() {
        let _guard = sync_state_test_guard();
        reset_sync_state_for_tests();
        assert!(!sync_due_by_interval(10, 1_000));
        assert!(!sync_due_by_interval(10, 1_005));
        assert!(sync_due_by_interval(10, 1_010));
    }

    #[test]
    fn wal_interval_sync_resets_after_successful_sync() {
        let _guard = sync_state_test_guard();
        reset_sync_state_for_tests();
        assert!(!sync_due_by_interval(10, 1_000));
        assert!(sync_due_by_interval(10, 1_010));
        record_successful_sync(1_010);
        assert!(!sync_due_by_interval(10, 1_015));
        assert!(sync_due_by_interval(10, 1_020));
    }
}
