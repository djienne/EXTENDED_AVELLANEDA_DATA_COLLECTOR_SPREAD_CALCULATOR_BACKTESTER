pub mod data_collection_task;
pub mod data_collector;
pub mod error;
pub mod rest;
pub mod types;
pub mod websocket;
pub mod model_types;
pub mod data_loader;
pub mod metrics;
pub mod calibration;
pub mod spread_model;

// Re-export commonly used types
pub use data_collection_task::{run_data_collection_task, DataCollectionConfig};
pub use data_collector::{CollectorState, OrderbookCsvWriter, FullOrderbookCsvWriter, TradesCsvWriter, OrderbookState};
pub use error::{ConnectorError, Result};
pub use rest::RestClient;
pub use types::{
    AccountInfo, AccountUpdate, Balance, BidAsk, FeeInfo, FundingRateInfo, L2Config, MarketConfig, MarketInfo,
    OrderBook, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, Position, PositionSide,
    PublicTrade, Settlement, Signature, TimeInForce, Trade, TradeType, TradingConfig,
    WsAccountUpdateMessage, WsOrder, WsOrderBookMessage, WsPublicTradesMessage,
};
pub use websocket::{MultiMarketSubscriber, WebSocketClient};

/// Initialize logging for the library
pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(true)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_exports() {
        // Just verify that main exports are accessible
        let _ = RestClient::new_mainnet(None);
        let _ = WebSocketClient::new_mainnet(None);
    }
}
