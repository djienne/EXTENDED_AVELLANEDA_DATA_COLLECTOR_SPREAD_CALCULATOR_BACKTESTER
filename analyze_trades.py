import pandas as pd
import numpy as np

# Load trades and orderbook
trades = pd.read_csv('data/ETH_USD/trades.csv')
trades['timestamp_ms'] = trades['timestamp_ms'].astype(int)

# Load orderbook (first few columns: timestamp, datetime, market, seq, then bid/ask pairs)
orderbook = pd.read_csv('data/ETH_USD/orderbook_depth.csv')
orderbook['timestamp'] = orderbook['timestamp'].astype(int)

# Calculate mid price from best bid/ask
orderbook['mid'] = (orderbook['bid_price0'] + orderbook['ask_price0']) / 2
orderbook['spread'] = orderbook['ask_price0'] - orderbook['bid_price0']

# Merge trades with closest orderbook snapshot
trades['ob_idx'] = trades['timestamp_ms'].apply(
    lambda t: (orderbook['timestamp'] - t).abs().idxmin()
)

trades_merged = trades.merge(
    orderbook[['timestamp', 'mid', 'bid_price0', 'ask_price0', 'spread']],
    left_on='ob_idx',
    right_index=True,
    suffixes=('', '_ob')
)

# Calculate distance from mid
trades_merged['dist_from_mid'] = trades_merged['price'] - trades_merged['mid']
trades_merged['dist_from_mid_bps'] = (trades_merged['dist_from_mid'] / trades_merged['mid']) * 10000
trades_merged['market_spread_bps'] = (trades_merged['spread'] / trades_merged['mid']) * 10000

# Summary statistics
print("=" * 80)
print("TRADE VS ORDERBOOK ANALYSIS")
print("=" * 80)
print(f"\nTotal Trades: {len(trades_merged)}")
print(f"\nMarket Spread (Best Bid/Ask):")
print(f"  Average: {trades_merged['market_spread_bps'].mean():.2f} bps")
print(f"  Median:  {trades_merged['market_spread_bps'].median():.2f} bps")
print(f"  Min:     {trades_merged['market_spread_bps'].min():.2f} bps")
print(f"  Max:     {trades_merged['market_spread_bps'].max():.2f} bps")

print(f"\nTrade Distance from Mid:")
print(f"  Average: {trades_merged['dist_from_mid_bps'].abs().mean():.2f} bps")
print(f"  Median:  {trades_merged['dist_from_mid_bps'].abs().median():.2f} bps")
print(f"  75th percentile: {trades_merged['dist_from_mid_bps'].abs().quantile(0.75):.2f} bps")
print(f"  95th percentile: {trades_merged['dist_from_mid_bps'].abs().quantile(0.95):.2f} bps")

print(f"\nBuy vs Sell:")
buys = trades_merged[trades_merged['side'] == 'buy']
sells = trades_merged[trades_merged['side'] == 'sell']
print(f"  Buy trades:  {len(buys)} (avg {buys['dist_from_mid_bps'].mean():.2f} bps from mid)")
print(f"  Sell trades: {len(sells)} (avg {sells['dist_from_mid_bps'].mean():.2f} bps from mid)")

print(f"\nRecommended AS Spread:")
# For fills, we need to be competitive with market spread
recommended_half_spread = trades_merged['dist_from_mid_bps'].abs().quantile(0.75)
print(f"  To capture 75% of trades: {recommended_half_spread * 2:.2f} bps total spread")
print(f"  Half-spread for bid/ask: {recommended_half_spread:.2f} bps each side")

# Check sample trades
print(f"\nSample Trades:")
print(trades_merged[['timestamp_ms', 'side', 'price', 'mid', 'dist_from_mid', 'dist_from_mid_bps', 'market_spread_bps']].head(20).to_string())
