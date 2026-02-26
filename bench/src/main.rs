#![forbid(unsafe_code)]
//! Core and in-memory collection benchmarks for AIONBD.

use std::env;
use std::process;

use collection_churn_bench::run_collection_churn_bench;
use core_scan_bench::{run_collection_bench, run_dot_bench, run_l2_bench};
use list_points_bench::run_list_points_bench;
use persistence_write_bench::run_persistence_write_bench;
use search_quality_bench::run_search_quality_bench;

mod collection_churn_bench;
mod core_scan_bench;
mod list_points_bench;
mod persistence_write_bench;
mod persistence_write_utils;
mod search_quality_bench;
mod search_quality_ivf;

fn main() {
    if cfg!(debug_assertions) && env::var("AIONBD_ALLOW_DEBUG_BENCH").as_deref() != Ok("1") {
        eprintln!(
            "error=debug_build_not_allowed message=\"run `cargo run --release -p aionbd-bench`\""
        );
        process::exit(2);
    }

    let mode = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let scenario = env::var("AIONBD_BENCH_SCENARIO").unwrap_or_else(|_| "all".to_string());
    let ok = match scenario.as_str() {
        "all" => {
            run_dot_bench(mode)
                && run_l2_bench(mode)
                && run_collection_bench(mode)
                && run_collection_churn_bench(mode)
                && run_list_points_bench(mode)
                && run_persistence_write_bench(mode)
                && run_search_quality_bench(mode)
        }
        "dot" => run_dot_bench(mode),
        "l2" => run_l2_bench(mode),
        "collection" => run_collection_bench(mode),
        "collection_churn" => run_collection_churn_bench(mode),
        "list_points" => run_list_points_bench(mode),
        "persistence_write" => run_persistence_write_bench(mode),
        "search_quality" => run_search_quality_bench(mode),
        _ => {
            eprintln!(
                "error=invalid_scenario scenario=\"{}\" allowed=\"all,dot,l2,collection,collection_churn,list_points,persistence_write,search_quality\"",
                scenario
            );
            false
        }
    };

    if !ok {
        process::exit(1);
    }
}
