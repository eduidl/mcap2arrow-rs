# mcap2arrow-rs

[![CI](https://github.com/eduidl/mcap2arrow-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcap2arrow-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcap2arrow.svg)](https://crates.io/crates/mcap2arrow)

Rust workspace for converting MCAP data into Apache Arrow representations.

## Crates

- [`mcap2arrow`](mcap2arrow/mcap2arrow): library entry point for decoding MCAP into Arrow `RecordBatch`
- [`transmcap`](tools/transmcap): CLI for converting MCAP to `jsonl/csv/parquet`
- `mcap2arrow-*`: internal/support crates used by `mcap2arrow`

## Start Here

- CLI usage and options: [`tools/transmcap/README.md`](tools/transmcap/README.md)
- Library usage and feature flags: [`mcap2arrow/mcap2arrow/README.md`](mcap2arrow/mcap2arrow/README.md)

## Quick CLI Usage (`transmcap`)

```bash
cargo run -p transmcap -- convert <input.mcap> --topic <topic> --format jsonl
cargo run -p transmcap -- schema <input.mcap> --topic <topic>
```

Use `-o/--output` to write files (`parquet` requires `-o`).

## Quick Commands

```bash
cargo build -p transmcap
cargo test --workspace
```
