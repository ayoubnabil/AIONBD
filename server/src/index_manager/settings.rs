use std::sync::{Arc, OnceLock};

use tokio::sync::Semaphore;

const DEFAULT_L2_BUILD_COOLDOWN_MS: u64 = 1_000;
const DEFAULT_L2_WARMUP_ON_BOOT: bool = true;
const DEFAULT_L2_BUILD_MAX_IN_FLIGHT: usize = 2;

pub(crate) fn configured_l2_build_cooldown_ms() -> u64 {
    static COOLDOWN_MS: OnceLock<u64> = OnceLock::new();
    *COOLDOWN_MS.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_L2_INDEX_BUILD_COOLDOWN_MS") else {
            return DEFAULT_L2_BUILD_COOLDOWN_MS;
        };
        match raw.parse::<u64>() {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = DEFAULT_L2_BUILD_COOLDOWN_MS,
                    "invalid AIONBD_L2_INDEX_BUILD_COOLDOWN_MS; using default"
                );
                DEFAULT_L2_BUILD_COOLDOWN_MS
            }
        }
    })
}

pub(crate) fn configured_l2_warmup_on_boot() -> bool {
    static WARMUP_ON_BOOT: OnceLock<bool> = OnceLock::new();
    *WARMUP_ON_BOOT.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_L2_INDEX_WARMUP_ON_BOOT") else {
            return DEFAULT_L2_WARMUP_ON_BOOT;
        };
        match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => {
                tracing::warn!(
                    %raw,
                    default = DEFAULT_L2_WARMUP_ON_BOOT,
                    "invalid AIONBD_L2_INDEX_WARMUP_ON_BOOT; using default"
                );
                DEFAULT_L2_WARMUP_ON_BOOT
            }
        }
    })
}

pub(crate) fn configured_l2_build_max_in_flight() -> usize {
    static MAX_IN_FLIGHT: OnceLock<usize> = OnceLock::new();
    *MAX_IN_FLIGHT.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT") else {
            return DEFAULT_L2_BUILD_MAX_IN_FLIGHT;
        };
        match raw.parse::<usize>() {
            Ok(0) => {
                tracing::warn!(
                    %raw,
                    default = DEFAULT_L2_BUILD_MAX_IN_FLIGHT,
                    "AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT must be > 0; using default"
                );
                DEFAULT_L2_BUILD_MAX_IN_FLIGHT
            }
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = DEFAULT_L2_BUILD_MAX_IN_FLIGHT,
                    "invalid AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT; using default"
                );
                DEFAULT_L2_BUILD_MAX_IN_FLIGHT
            }
        }
    })
}

pub(crate) fn l2_build_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(configured_l2_build_max_in_flight())))
}
