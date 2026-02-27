# AIONBD Rust SDK

Official Rust client for the AIONBD HTTP API.

Current scope:
- Supports `http://` base URLs.
- Uses a synchronous/blocking client model.

## Requirements

- Rust `>= 1.78`

## Install (path dependency)

Add to your `Cargo.toml`:

```toml
[dependencies]
aionbd-sdk-rust = { path = "../AIONBD/sdk/rust" }
```

## Quick Example

```rust
use aionbd_sdk_rust::{AionBDClient, SearchTopKOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = AionBDClient::new("http://127.0.0.1:8080")?;

    let live = client.live()?;
    println!("{}", live.status);

    client.create_collection("demo", 3, true)?;
    client.upsert_point("demo", 1, &[1.0, 0.0, 0.0], None)?;
    client.upsert_point("demo", 2, &[0.0, 1.0, 0.0], None)?;

    let result = client.search_collection_top_k(
        "demo",
        &[1.0, 0.0, 0.0],
        Some(SearchTopKOptions {
            limit: Some(2),
            ..SearchTopKOptions::default()
        }),
    )?;

    println!("hits={}", result.hits.len());
    Ok(())
}
```

## Auth Usage

```rust
use aionbd_sdk_rust::{AionBDClient, ClientOptions};

let client = AionBDClient::with_options(
    "http://127.0.0.1:8080",
    ClientOptions {
        api_key: Some("secret-key-a".to_string()),
        bearer_token: Some("token-a".to_string()),
        ..ClientOptions::default()
    },
)?;
```

## API Coverage

- `live`, `ready`, `health`
- `metrics`, `metrics_prometheus`
- `distance`
- `create_collection`
- `list_collections`, `get_collection`, `delete_collection`
- `upsert_point`, `upsert_points_batch`
- `get_point`, `delete_point`
- `list_points`
- `search_collection`
- `search_collection_top_k`
- `search_collection_top_k_batch`

## Run Tests

```bash
cd sdk/rust
cargo test
```
