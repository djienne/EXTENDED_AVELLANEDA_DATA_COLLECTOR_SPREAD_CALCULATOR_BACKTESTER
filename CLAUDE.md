# CLAUDE.md

Data collector for Extended DEX (Starknet perpetuals). Collects orderbook depth and public trades via WebSocket and saves to CSV files.

## Quick Commands

```bash
cargo build --release
cargo run --bin collect_data
RUST_LOG=debug cargo run --bin collect_data
```

## Core Files

- `src/bin/collect_data.rs` - Main data collection binary
- `src/websocket.rs` - WebSocket streaming client
- `src/data_collector.rs` - CSV writers for orderbook and trades
- `src/data_collection_task.rs` - Data collection task logic
- `src/rest.rs` - REST API client
- `src/types.rs` - Data types

## Features

**WebSocket Data Collection**:
- Orderbook depth (configurable levels, default 20)
- Public trades
- Automatic deduplication
- State persistence for resume capability
- Handles SNAPSHOT and DELTA updates

**CSV Format**:
- `orderbook_depth.csv`: Horizontal format, one row per snapshot, N levels as columns
  - Headers: `timestamp_ms,datetime,market,seq,bid_price0,bid_qty0,ask_price0,ask_qty0,...`
- `trades.csv`: One row per trade
  - Headers: `timestamp_ms,datetime,market,side,price,quantity,trade_id,trade_type`
- Buffered writing (8KB), flush every 5 seconds
- Time-sorted with deduplication

**WebSocket Depth**:
- Initial SNAPSHOT (full orderbook state)
- Subsequent UPDATE/DELTA (partial changes)
- Maintains full orderbook state and merges deltas

## Configuration (config.json)

```json
{
  "markets": ["BTC-USD", "ETH-USD", "SOL-USD"],
  "data_directory": "data",
  "collect_orderbook": true,
  "collect_trades": true,
  "collect_full_orderbook": true,
  "max_depth_levels": 20
}
```

**Key Settings**:
- `markets`: List of markets to collect data for
- `data_directory`: Where to save CSV files (default: `data/`)
- `collect_orderbook`: Collect best bid/ask (default: `true`)
- `collect_trades`: Collect public trades (default: `true`)
- `collect_full_orderbook`: Collect full orderbook depth (default: `true`)
- `max_depth_levels`: Number of orderbook levels to collect (default: 20)

## Data Storage

Data is saved to:
```
data/
  {market}/
    orderbook_depth.csv    # Full depth (N levels)
    orderbook.csv          # Best bid/ask only
    trades.csv             # Public trades
    state.json             # Resume state
```

## Resume Capability

The collector saves state to `state.json` every 30 seconds and on shutdown (Ctrl+C). When restarted, it:
- Skips already-collected trades (by trade ID)
- Skips already-seen orderbook updates (by sequence number)
- Maintains time-sorted order
- Continues from last position

## No API Key Required

Public data collection doesn't require authentication. For authenticated WebSocket subscriptions (account updates), set:

```bash
# .env (optional, only for authenticated endpoints)
API_KEY=...
```

## Examples

- `examples/collect_data.rs` - Basic data collection example
- `examples/public_trades.rs` - Public trades subscription
- `examples/ws_latency_test.rs` - WebSocket latency testing
