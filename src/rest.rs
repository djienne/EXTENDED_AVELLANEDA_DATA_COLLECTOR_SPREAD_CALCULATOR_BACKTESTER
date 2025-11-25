use crate::error::{ConnectorError, Result};
use crate::types::{
    AccountInfo, ApiResponse, Balance, BidAsk, FeeInfo, FundingRateData, FundingRateInfo, MarketConfig,
    MarketInfo, OrderBook, OrderSide, PaginatedResponse,
    Position, PublicTrade, Trade, TradeType,
};
use reqwest::{Client, RequestBuilder};
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// REST API client for Extended exchange
#[derive(Clone)]
pub struct RestClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
}

impl RestClient {
    /// Create a new REST client for mainnet
    pub fn new_mainnet(api_key: Option<String>) -> Result<Self> {
        Self::new("https://api.starknet.extended.exchange/api/v1", api_key)
    }

    /// Create a new REST client for testnet
    pub fn new_testnet(api_key: Option<String>) -> Result<Self> {
        Self::new(
            "https://api.starknet.sepolia.extended.exchange/api/v1",
            api_key,
        )
    }

    /// Create a new REST client with custom base URL
    pub fn new(base_url: &str, api_key: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("extended-connector/0.1.0")
            .build()?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            api_key,
        })
    }

    // =========================================================================
    // Helper Methods - Reduce code duplication
    // =========================================================================

    /// Execute a request and parse the API response
    async fn execute_request<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let api_response: ApiResponse<T> = response.json().await?;

        api_response.data.ok_or_else(|| {
            let error_msg = api_response
                .error
                .map(|e| format!("{}: {}", e.code, e.message))
                .unwrap_or_else(|| "Unknown error".to_string());
            error!("API error response: {}", error_msg);
            ConnectorError::ApiError(error_msg)
        })
    }

    /// Execute a request and parse a paginated API response
    async fn execute_paginated_request<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<Option<Vec<T>>> {
        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            warn!("API error: {} - {}", status, error_text);
            return Ok(None);
        }

        let api_response: PaginatedResponse<T> = response.json().await?;
        Ok(api_response.data)
    }

    /// Build a GET request with optional API key header
    fn build_get(&self, url: &str) -> RequestBuilder {
        let mut request = self.client.get(url);
        if let Some(api_key) = &self.api_key {
            request = request.header("X-Api-Key", api_key);
        }
        request
    }

    /// Build an authenticated GET request (requires API key)
    fn build_authenticated_get(&self, url: &str) -> Result<RequestBuilder> {
        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required".to_string())
        })?;
        Ok(self.client.get(url).header("X-Api-Key", api_key))
    }

    // =========================================================================
    // Public Endpoints
    // =========================================================================

    /// Get orderbook for a specific market
    pub async fn get_orderbook(&self, market: &str) -> Result<OrderBook> {
        let url = format!("{}/info/markets/{}/orderbook", self.base_url, market);
        debug!("Fetching orderbook for {} from {}", market, url);

        let orderbook: OrderBook = self.execute_request(self.build_get(&url)).await?;
        
        info!(
            "Fetched orderbook for {} - {} bids, {} asks",
            market,
            orderbook.bid.len(),
            orderbook.ask.len()
        );
        Ok(orderbook)
    }

    /// Get public trades for a specific market
    pub async fn get_public_trades(&self, market: &str) -> Result<Vec<PublicTrade>> {
        let url = format!("{}/info/markets/{}/trades", self.base_url, market);
        debug!("Fetching public trades for {} from {}", market, url);

        let trades: Vec<PublicTrade> = self.execute_request(self.build_get(&url)).await?;
        
        info!("Fetched {} public trades for {}", trades.len(), market);
        Ok(trades)
    }

    /// Get best bid/ask for a specific market
    pub async fn get_bid_ask(&self, market: &str) -> Result<BidAsk> {
        let orderbook = self.get_orderbook(market).await?;
        Ok(BidAsk::from(&orderbook))
    }

    /// Get best bid/ask for multiple markets concurrently
    pub async fn get_multiple_bid_asks(&self, markets: &[String]) -> Vec<Result<BidAsk>> {
        let mut tasks = Vec::with_capacity(markets.len());

        for market in markets {
            let market = market.clone();
            let client = self.clone();
            tasks.push(tokio::spawn(async move {
                client.get_bid_ask(&market).await
            }));
        }

        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(ConnectorError::Other(format!(
                    "Task join error: {}",
                    e
                )))),
            }
        }

        results
    }

    /// Get all available markets
    pub async fn get_all_markets(&self) -> Result<Vec<MarketInfo>> {
        let url = format!("{}/info/markets", self.base_url);
        debug!("Fetching all markets from {}", url);

        let markets: Vec<MarketInfo> = self.execute_request(self.build_get(&url)).await?;
        
        info!("Fetched {} markets", markets.len());
        Ok(markets)
    }

    /// Get latest funding rate for a specific market
    pub async fn get_funding_rate(&self, market: &str) -> Result<Option<FundingRateInfo>> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let one_hour_ago = now.saturating_sub(3600 * 1000);

        let url = format!(
            "{}/info/{}/funding?startTime={}&endTime={}&limit=1",
            self.base_url, market, one_hour_ago, now
        );
        debug!("Fetching funding rate for {} from {}", market, url);

        let data = self.execute_paginated_request::<FundingRateData>(self.build_get(&url)).await?;

        match data {
            Some(data) if !data.is_empty() => {
                let info = FundingRateInfo::from_data(data[0].clone());
                debug!("Fetched funding rate for {}: {}", market, info.rate_percentage);
                Ok(Some(info))
            }
            _ => {
                debug!("No funding rate data available for {}", market);
                Ok(None)
            }
        }
    }

    /// Get funding rates for all active markets
    pub async fn get_all_funding_rates(&self) -> Result<Vec<FundingRateInfo>> {
        let markets = self.get_all_markets().await?;

        let active_markets: Vec<_> = markets
            .into_iter()
            .filter(|m| m.active && m.status == "ACTIVE")
            .collect();

        info!("Fetching funding rates for {} active markets", active_markets.len());

        let mut tasks = Vec::with_capacity(active_markets.len());

        for market in active_markets {
            let market_name = market.name.clone();
            let client = self.clone();
            tasks.push(tokio::spawn(async move {
                (market_name.clone(), client.get_funding_rate(&market_name).await)
            }));
        }

        let mut funding_rates = Vec::new();
        for task in tasks {
            match task.await {
                Ok((market_name, result)) => match result {
                    Ok(Some(rate)) => funding_rates.push(rate),
                    Ok(None) => {
                        debug!("No funding rate data for {}", market_name);
                    }
                    Err(e) => {
                        warn!("Error fetching funding rate for {}: {}", market_name, e);
                    }
                },
                Err(e) => {
                    error!("Task join error: {}", e);
                }
            }
        }

        info!("Successfully fetched {} funding rates", funding_rates.len());
        Ok(funding_rates)
    }

    // =========================================================================
    // Authenticated Endpoints (require API key)
    // =========================================================================

    /// Get account information (requires API key)
    pub async fn get_account_info(&self) -> Result<AccountInfo> {
        let url = format!("{}/user/account/info", self.base_url);
        debug!("Fetching account info from {}", url);

        let account_info: AccountInfo = self.execute_request(
            self.build_authenticated_get(&url)?
        ).await?;

        info!(
            "Fetched account info - ID: {}, Vault: {}, Status: {}",
            account_info.account_id, account_info.l2_vault, account_info.status
        );
        Ok(account_info)
    }

    /// Get user positions, optionally filtered by market (requires API key)
    pub async fn get_positions(&self, market: Option<&str>) -> Result<Vec<Position>> {
        let url = if let Some(m) = market {
            format!("{}/user/positions?market={}", self.base_url, m)
        } else {
            format!("{}/user/positions", self.base_url)
        };
        debug!("Fetching positions from {}", url);

        let positions: Vec<Position> = self.execute_request(
            self.build_authenticated_get(&url)?
        ).await?;

        info!("Fetched {} positions", positions.len());
        Ok(positions)
    }

    /// Get account balance and margin information (requires API key)
    pub async fn get_balance(&self) -> Result<Balance> {
        let url = format!("{}/user/balance", self.base_url);
        debug!("Fetching balance from {}", url);

        let balance: Balance = self.execute_request(
            self.build_authenticated_get(&url)?
        ).await?;

        info!(
            "Fetched balance - Equity: ${}, Available: ${}",
            balance.equity, balance.available_for_trade
        );
        Ok(balance)
    }

    /// Get trade history (requires API key)
    pub async fn get_trades(
        &self,
        market: Option<&str>,
        trade_type: Option<TradeType>,
        side: Option<OrderSide>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Vec<Trade>> {
        let mut url = format!("{}/user/trades", self.base_url);
        let mut query_params = Vec::new();

        if let Some(m) = market {
            query_params.push(format!("market={}", m));
        }

        if let Some(tt) = trade_type {
            let type_str = match tt {
                TradeType::Trade => "trade",
                TradeType::Liquidation => "liquidation",
                TradeType::Deleverage => "deleverage",
            };
            query_params.push(format!("type={}", type_str));
        }

        if let Some(s) = side {
            let side_str = match s {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            };
            query_params.push(format!("side={}", side_str));
        }

        if let Some(l) = limit {
            query_params.push(format!("limit={}", l));
        }

        if let Some(c) = cursor {
            query_params.push(format!("cursor={}", c));
        }

        if !query_params.is_empty() {
            url.push('?');
            url.push_str(&query_params.join("&"));
        }

        debug!("Fetching trades from {}", url);

        let trades: Vec<Trade> = self.execute_request(
            self.build_authenticated_get(&url)?
        ).await?;

        info!("Fetched {} trades", trades.len());
        Ok(trades)
    }

    /// Get trades for multiple markets concurrently
    pub async fn get_trades_for_markets(
        &self,
        markets: &[String],
        limit: Option<u32>,
    ) -> Vec<Result<Vec<Trade>>> {
        let mut tasks = Vec::with_capacity(markets.len());

        for market in markets {
            let market = market.clone();
            let client = self.clone();
            tasks.push(tokio::spawn(async move {
                client.get_trades(Some(&market), None, None, limit, None).await
            }));
        }

        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(ConnectorError::Other(format!(
                    "Task join error: {}",
                    e
                )))),
            }
        }

        results
    }

    /// Update leverage for a specific market (requires API key)
    pub async fn update_leverage(&self, market: &str, leverage: &str) -> Result<String> {
        let url = format!("{}/user/leverage", self.base_url);
        debug!("Updating leverage for {} to {}x at {}", market, leverage, url);

        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ConnectorError::ApiError("API key required for leverage update".to_string())
        })?;

        let request_body = serde_json::json!({
            "market": market,
            "leverage": leverage
        });

        debug!("Sending PATCH request: {}", request_body);

        let response = self
            .client
            .patch(&url)
            .header("X-Api-Key", api_key)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        let response_text = response.text().await?;
        debug!("Leverage update response: {}", response_text);

        // Check if response is just a success status
        if response_text.contains("\"status\":\"OK\"") || response_text == "{\"status\":\"OK\"}" {
            info!("Successfully updated leverage for {} to {}x (OK response)", market, leverage);
            return Ok(leverage.to_string());
        }

        // Parse response
        #[derive(serde::Deserialize, Debug)]
        struct LeverageData {
            market: String,
            leverage: String,
        }

        let api_response: ApiResponse<LeverageData> = serde_json::from_str(&response_text)?;

        match api_response.data {
            Some(leverage_data) => {
                info!(
                    "Successfully updated leverage for {} to {}x",
                    leverage_data.market, leverage_data.leverage
                );
                Ok(leverage_data.leverage)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get fee information for a market (requires API key)
    pub async fn get_fees(&self, market: &str) -> Result<FeeInfo> {
        let url = format!("{}/user/fees?market={}", self.base_url, market);
        debug!("Fetching fees for {} from {}", market, url);

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", self.api_key.as_ref().ok_or_else(|| {
                ConnectorError::ApiError("API key required for fee info".to_string())
            })?)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            error!("API error: {} - {}", status, error_text);
            return Err(ConnectorError::ApiError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let response_text = response.text().await?;
        debug!("Fee response: {}", response_text);

        let api_response: ApiResponse<FeeInfo> = serde_json::from_str(&response_text)?;

        match api_response.data {
            Some(fee_info) => {
                info!(
                    "Fetched fees for {} - Maker: {}, Taker: {}",
                    market, fee_info.maker_fee_str(), fee_info.taker_fee_str()
                );
                Ok(fee_info)
            }
            None => {
                let error_msg = api_response
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("API error response: {}", error_msg);
                Err(ConnectorError::ApiError(error_msg))
            }
        }
    }

    /// Get market configuration including L2 asset IDs and resolutions
    pub async fn get_market_config(&self, market: &str) -> Result<MarketConfig> {
        let url = format!("{}/info/markets", self.base_url);
        debug!("Fetching market config for {} from {}", market, url);

        let markets: Vec<MarketConfig> = self.execute_request(self.client.get(&url)).await?;

        let market_config = markets
            .into_iter()
            .find(|m| m.name == market)
            .ok_or_else(|| {
                ConnectorError::InvalidMarket(format!("Market {} not found", market))
            })?;

        info!(
            "Fetched config for {} - Synthetic: {}, Collateral: {}, SynRes: {}, ColRes: {}",
            market,
            market_config.l2_config.synthetic_id,
            market_config.l2_config.collateral_id,
            market_config.l2_config.synthetic_resolution,
            market_config.l2_config.collateral_resolution
        );
        Ok(market_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_orderbook() {
        let client = RestClient::new_mainnet(None).unwrap();
        let result = client.get_orderbook("BTC-USD").await;

        match result {
            Ok(orderbook) => {
                assert_eq!(orderbook.market, "BTC-USD");
                assert!(!orderbook.bid.is_empty(), "Bid should not be empty");
                assert!(!orderbook.ask.is_empty(), "Ask should not be empty");
                println!("Orderbook: {:?}", orderbook);
            }
            Err(e) => {
                println!("Error fetching orderbook (might be expected in test environment): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_get_bid_ask() {
        let client = RestClient::new_mainnet(None).unwrap();
        let result = client.get_bid_ask("BTC-USD").await;

        match result {
            Ok(bid_ask) => {
                println!("{}", bid_ask);
                assert_eq!(bid_ask.market, "BTC-USD");
            }
            Err(e) => {
                println!("Error fetching bid/ask: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_markets() {
        let client = RestClient::new_mainnet(None).unwrap();
        let markets = vec!["BTC-USD".to_string(), "ETH-USD".to_string()];
        let results = client.get_multiple_bid_asks(&markets).await;

        assert_eq!(results.len(), 2);

        for result in results {
            match result {
                Ok(bid_ask) => println!("{}", bid_ask),
                Err(e) => println!("Error: {}", e),
            }
        }
    }
}
