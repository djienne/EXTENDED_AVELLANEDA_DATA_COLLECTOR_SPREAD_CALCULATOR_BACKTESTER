use crate::data_loader::OrderbookSnapshot;
use crate::model_types::EffectiveQuote;
use rust_decimal::Decimal;

pub fn calculate_effective_price(
    snapshot: &OrderbookSnapshot,
    volume_threshold: Decimal,
) -> Option<EffectiveQuote> {
    let (effective_bid, weighted_bid) = calculate_side_effective_price(&snapshot.bids, volume_threshold)?;
    let (effective_ask, weighted_ask) = calculate_side_effective_price(&snapshot.asks, volume_threshold)?;

    // Guard against zero prices
    if effective_bid.is_zero() || effective_ask.is_zero() {
        return None;
    }

    let mid = (effective_bid + effective_ask) / Decimal::TWO;

    Some(EffectiveQuote {
        bid: effective_bid,
        ask: effective_ask,
        mid,
        weighted_bid,
        weighted_ask,
    })
}

fn calculate_side_effective_price(
    levels: &[(Decimal, Decimal)], // (price, qty)
    threshold: Decimal,
) -> Option<(Decimal, Decimal)> {
    if levels.is_empty() || threshold <= Decimal::ZERO {
        return None;
    }

    let mut accumulated_value = Decimal::ZERO;
    let mut accumulated_qty = Decimal::ZERO;
    let mut weighted_price_sum = Decimal::ZERO;
    
    // For bids, we want the price where we can sell 'threshold' value.
    // For asks, we want the price where we can buy 'threshold' value.
    // The levels should be sorted best to worst.
    // Bids: Descending price. Asks: Ascending price.
    // The data loader loads them as they are in CSV. 
    // In CSV: bid_price0 is best bid. ask_price0 is best ask.
    // So they are already sorted best to worst.

    let mut final_price = Decimal::ZERO;

    for (price, qty) in levels {
        // Skip invalid levels
        if *price <= Decimal::ZERO || *qty <= Decimal::ZERO {
            continue;
        }

        let value = price * qty;
        
        // If this level fills the remaining threshold
        let remaining_value = threshold - accumulated_value;
        
        if remaining_value <= Decimal::ZERO {
            break;
        }

        if value >= remaining_value {
            // Partial fill of this level
            let needed_qty = remaining_value / price;
            accumulated_value += remaining_value;
            accumulated_qty += needed_qty;
            weighted_price_sum += price * needed_qty;
            final_price = *price;
            break;
        } else {
            // Full fill of this level
            accumulated_value += value;
            accumulated_qty += *qty;
            weighted_price_sum += price * qty;
            final_price = *price;
        }
    }

    // Must have accumulated some quantity to calculate VWAP
    if accumulated_qty.is_zero() {
        return None;
    }

    // Check if we reached the threshold (optional: could return partial results)
    if accumulated_value < threshold {
        // Not enough depth to reach threshold
        // Return what we have if we have something meaningful
        if final_price.is_zero() {
            return None;
        }
    }

    let vwap = weighted_price_sum / accumulated_qty;
    
    // We return (marginal_price, vwap_price)
    // The "Effective Price" usually refers to the VWAP cost, 
    // but for mid-price calc, using the marginal price (the price of the last unit) 
    // might be more "current", while VWAP is "cost".
    // The prompt says "effective bid ask prices that contain at least 1000$ of volume".
    // Usually this means the VWAP of that depth.
    
    Some((final_price, vwap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_levels() {
        let result = calculate_side_effective_price(&[], Decimal::from(1000));
        assert!(result.is_none());
    }

    #[test]
    fn test_zero_threshold() {
        let levels = vec![(Decimal::from(100), Decimal::from(10))];
        let result = calculate_side_effective_price(&levels, Decimal::ZERO);
        assert!(result.is_none());
    }

    #[test]
    fn test_single_level_sufficient() {
        let levels = vec![(Decimal::from(100), Decimal::from(20))]; // 2000 value
        let result = calculate_side_effective_price(&levels, Decimal::from(1000));
        assert!(result.is_some());
        let (final_price, vwap) = result.unwrap();
        assert_eq!(final_price, Decimal::from(100));
        assert_eq!(vwap, Decimal::from(100));
    }

    #[test]
    fn test_multiple_levels() {
        let levels = vec![
            (Decimal::from(100), Decimal::from(5)),  // 500 value
            (Decimal::from(99), Decimal::from(10)),  // 990 value
        ];
        let result = calculate_side_effective_price(&levels, Decimal::from(1000));
        assert!(result.is_some());
    }
}
