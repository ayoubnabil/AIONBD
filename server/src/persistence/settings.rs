use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

const CHECKPOINT_COMPACT_AFTER: usize = 64;
const ASYNC_CHECKPOINTS_DEFAULT: bool = false;
const WAL_SYNC_EVERY_N_DEFAULT: u64 = 0;
const WAL_GROUP_MAX_BATCH_DEFAULT: usize = 16;
const WAL_GROUP_FLUSH_DELAY_MS_DEFAULT: u64 = 0;

pub(crate) fn configured_async_checkpoints() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_ASYNC_CHECKPOINTS") else {
            return ASYNC_CHECKPOINTS_DEFAULT;
        };
        match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => {
                tracing::warn!(
                    %raw,
                    default = ASYNC_CHECKPOINTS_DEFAULT,
                    "invalid AIONBD_ASYNC_CHECKPOINTS; using default"
                );
                ASYNC_CHECKPOINTS_DEFAULT
            }
        }
    })
}

pub(crate) fn configured_checkpoint_compact_after() -> usize {
    static COMPACT_AFTER: OnceLock<usize> = OnceLock::new();
    *COMPACT_AFTER.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_CHECKPOINT_COMPACT_AFTER") else {
            return CHECKPOINT_COMPACT_AFTER;
        };
        match raw.parse::<usize>() {
            Ok(0) => {
                tracing::warn!(
                    %raw,
                    default = CHECKPOINT_COMPACT_AFTER,
                    "AIONBD_CHECKPOINT_COMPACT_AFTER must be > 0; using default"
                );
                CHECKPOINT_COMPACT_AFTER
            }
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = CHECKPOINT_COMPACT_AFTER,
                    "invalid AIONBD_CHECKPOINT_COMPACT_AFTER; using default"
                );
                CHECKPOINT_COMPACT_AFTER
            }
        }
    })
}

pub(crate) fn configured_wal_sync_every_n_writes() -> u64 {
    static EVERY_N_WRITES: OnceLock<u64> = OnceLock::new();
    *EVERY_N_WRITES.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_WAL_SYNC_EVERY_N_WRITES") else {
            return WAL_SYNC_EVERY_N_DEFAULT;
        };
        match raw.parse::<u64>() {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = WAL_SYNC_EVERY_N_DEFAULT,
                    "invalid AIONBD_WAL_SYNC_EVERY_N_WRITES; using default"
                );
                WAL_SYNC_EVERY_N_DEFAULT
            }
        }
    })
}

pub(crate) fn configured_wal_group_commit_max_batch() -> usize {
    static MAX_BATCH: OnceLock<usize> = OnceLock::new();
    *MAX_BATCH.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_WAL_GROUP_COMMIT_MAX_BATCH") else {
            return WAL_GROUP_MAX_BATCH_DEFAULT;
        };
        match raw.parse::<usize>() {
            Ok(0) => {
                tracing::warn!(
                    %raw,
                    default = WAL_GROUP_MAX_BATCH_DEFAULT,
                    "AIONBD_WAL_GROUP_COMMIT_MAX_BATCH must be > 0; using default"
                );
                WAL_GROUP_MAX_BATCH_DEFAULT
            }
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = WAL_GROUP_MAX_BATCH_DEFAULT,
                    "invalid AIONBD_WAL_GROUP_COMMIT_MAX_BATCH; using default"
                );
                WAL_GROUP_MAX_BATCH_DEFAULT
            }
        }
    })
}

pub(crate) fn configured_wal_group_commit_flush_delay_ms() -> u64 {
    static FLUSH_DELAY_MS: OnceLock<u64> = OnceLock::new();
    *FLUSH_DELAY_MS.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS") else {
            return WAL_GROUP_FLUSH_DELAY_MS_DEFAULT;
        };
        match raw.parse::<u64>() {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = WAL_GROUP_FLUSH_DELAY_MS_DEFAULT,
                    "invalid AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS; using default"
                );
                WAL_GROUP_FLUSH_DELAY_MS_DEFAULT
            }
        }
    })
}

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

fn wal_write_sequence() -> &'static AtomicU64 {
    static WAL_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    &WAL_WRITE_SEQUENCE
}

#[cfg(test)]
mod tests {
    use super::{batch_should_sync, should_sync_this_write};

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
}
