"""
Cost Basis Matching Module
Implements FIFO, LIFO, and HIFO methods for matching sales to acquisition lots.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass
import pandas as pd
from acquisition_lots import AcquisitionLot
from sales_transactions import SaleTransaction


@dataclass
class MatchedLot:
    """Represents a match between a sale and an acquisition lot."""
    sale_date: datetime
    sale_amount: float
    sale_price_per_btc: float
    lot_date: datetime
    lot_amount_used: float
    lot_cost_basis_per_btc: float
    cost_basis_used: float
    gain_loss: float
    holding_period_days: int
    is_long_term: bool  # True if > 365 days, False if <= 365 days
    sale_source: str
    lot_source: str


@dataclass
class MatchingResults:
    """Results from cost basis matching."""
    method: str  # "FIFO", "LIFO", or "HIFO"
    matched_lots: List[MatchedLot]
    total_realized_gain: float
    short_term_gain: float
    long_term_gain: float
    total_cost_basis: float
    total_proceeds: float


def calculate_holding_period(acquisition_date: datetime, sale_date: datetime) -> Tuple[int, bool]:
    """
    Calculate holding period in days and determine if long-term.
    
    Args:
        acquisition_date: Date BTC was acquired
        sale_date: Date BTC was sold
    
    Returns:
        Tuple of (holding_period_days, is_long_term)
    """
    holding_period = (sale_date - acquisition_date).days
    is_long_term = holding_period > 365
    return holding_period, is_long_term


def match_sales_to_lots_fifo(
    acquisition_lots: List[AcquisitionLot],
    sales: List[SaleTransaction]
) -> MatchingResults:
    """
    Match sales to acquisition lots using FIFO (First In, First Out) method.
    Oldest lots are sold first.
    
    Args:
        acquisition_lots: List of acquisition lots (will be sorted by date)
        sales: List of sales transactions (should be in chronological order)
    
    Returns:
        MatchingResults object
    """
    # Sort lots by date (oldest first) for FIFO
    sorted_lots = sorted(acquisition_lots, key=lambda x: x.date)
    
    # Track remaining amounts in each lot
    remaining_lots = [(lot, lot.btc_amount) for lot in sorted_lots]
    
    matched_lots = []
    
    # Process each sale in chronological order
    for sale in sales:
        remaining_sale_amount = sale.btc_amount
        
        # Match against lots in order (oldest first for FIFO)
        for i, (lot, remaining_amount) in enumerate(remaining_lots):
            if remaining_sale_amount <= 0:
                break
            
            if remaining_amount <= 0:
                continue
            
            # Can only sell BTC that was acquired before the sale date
            if lot.date > sale.date:
                continue
            
            # Calculate how much to take from this lot
            amount_to_use = min(remaining_sale_amount, remaining_amount)
            cost_basis_used = (amount_to_use / lot.btc_amount) * lot.cost_basis_usd
            gain_loss = (sale.sale_price_per_btc - lot.price_per_btc) * amount_to_use
            
            # Calculate holding period
            holding_period_days, is_long_term = calculate_holding_period(lot.date, sale.date)
            
            # Create matched lot record
            matched = MatchedLot(
                sale_date=sale.date,
                sale_amount=sale.btc_amount,
                sale_price_per_btc=sale.sale_price_per_btc,
                lot_date=lot.date,
                lot_amount_used=amount_to_use,
                lot_cost_basis_per_btc=lot.price_per_btc,
                cost_basis_used=cost_basis_used,
                gain_loss=gain_loss,
                holding_period_days=holding_period_days,
                is_long_term=is_long_term,
                sale_source=sale.source,
                lot_source=lot.source
            )
            matched_lots.append(matched)
            
            # Update remaining amounts
            remaining_sale_amount -= amount_to_use
            remaining_lots[i] = (lot, remaining_amount - amount_to_use)
    
    # Calculate totals
    total_realized_gain = sum(m.gain_loss for m in matched_lots)
    short_term_gain = sum(m.gain_loss for m in matched_lots if not m.is_long_term)
    long_term_gain = sum(m.gain_loss for m in matched_lots if m.is_long_term)
    total_cost_basis = sum(m.cost_basis_used for m in matched_lots)
    total_proceeds = sum(m.sale_price_per_btc * m.lot_amount_used for m in matched_lots)
    
    return MatchingResults(
        method="FIFO",
        matched_lots=matched_lots,
        total_realized_gain=total_realized_gain,
        short_term_gain=short_term_gain,
        long_term_gain=long_term_gain,
        total_cost_basis=total_cost_basis,
        total_proceeds=total_proceeds
    )


def match_sales_to_lots_lifo(
    acquisition_lots: List[AcquisitionLot],
    sales: List[SaleTransaction]
) -> MatchingResults:
    """
    Match sales to acquisition lots using LIFO (Last In, First Out) method.
    Newest lots are sold first.
    
    Args:
        acquisition_lots: List of acquisition lots (will be sorted by date)
        sales: List of sales transactions (should be in chronological order)
    
    Returns:
        MatchingResults object
    """
    # Sort lots by date (newest first) for LIFO
    sorted_lots = sorted(acquisition_lots, key=lambda x: x.date, reverse=True)
    
    # Track remaining amounts in each lot
    remaining_lots = [(lot, lot.btc_amount) for lot in sorted_lots]
    
    matched_lots = []
    
    # Process each sale in chronological order
    for sale in sales:
        remaining_sale_amount = sale.btc_amount
        
        # Match against lots in order (newest first for LIFO)
        for i, (lot, remaining_amount) in enumerate(remaining_lots):
            if remaining_sale_amount <= 0:
                break
            
            if remaining_amount <= 0:
                continue
            
            # Can only sell BTC that was acquired before the sale date
            if lot.date > sale.date:
                continue
            
            # Calculate how much to take from this lot
            amount_to_use = min(remaining_sale_amount, remaining_amount)
            cost_basis_used = (amount_to_use / lot.btc_amount) * lot.cost_basis_usd
            gain_loss = (sale.sale_price_per_btc - lot.price_per_btc) * amount_to_use
            
            # Calculate holding period
            holding_period_days, is_long_term = calculate_holding_period(lot.date, sale.date)
            
            # Create matched lot record
            matched = MatchedLot(
                sale_date=sale.date,
                sale_amount=sale.btc_amount,
                sale_price_per_btc=sale.sale_price_per_btc,
                lot_date=lot.date,
                lot_amount_used=amount_to_use,
                lot_cost_basis_per_btc=lot.price_per_btc,
                cost_basis_used=cost_basis_used,
                gain_loss=gain_loss,
                holding_period_days=holding_period_days,
                is_long_term=is_long_term,
                sale_source=sale.source,
                lot_source=lot.source
            )
            matched_lots.append(matched)
            
            # Update remaining amounts
            remaining_sale_amount -= amount_to_use
            remaining_lots[i] = (lot, remaining_amount - amount_to_use)
    
    # Calculate totals
    total_realized_gain = sum(m.gain_loss for m in matched_lots)
    short_term_gain = sum(m.gain_loss for m in matched_lots if not m.is_long_term)
    long_term_gain = sum(m.gain_loss for m in matched_lots if m.is_long_term)
    total_cost_basis = sum(m.cost_basis_used for m in matched_lots)
    total_proceeds = sum(m.sale_price_per_btc * m.lot_amount_used for m in matched_lots)
    
    return MatchingResults(
        method="LIFO",
        matched_lots=matched_lots,
        total_realized_gain=total_realized_gain,
        short_term_gain=short_term_gain,
        long_term_gain=long_term_gain,
        total_cost_basis=total_cost_basis,
        total_proceeds=total_proceeds
    )


def match_sales_to_lots_hifo(
    acquisition_lots: List[AcquisitionLot],
    sales: List[SaleTransaction]
) -> MatchingResults:
    """
    Match sales to acquisition lots using HIFO (Highest In, First Out) method.
    Highest cost basis lots are sold first.
    
    Args:
        acquisition_lots: List of acquisition lots (will be sorted by cost basis)
        sales: List of sales transactions (should be in chronological order)
    
    Returns:
        MatchingResults object
    """
    # Sort lots by cost basis per BTC (highest first) for HIFO
    sorted_lots = sorted(acquisition_lots, key=lambda x: x.price_per_btc, reverse=True)
    
    # Track remaining amounts in each lot
    remaining_lots = [(lot, lot.btc_amount) for lot in sorted_lots]
    
    matched_lots = []
    
    # Process each sale in chronological order
    for sale in sales:
        remaining_sale_amount = sale.btc_amount
        
        # Match against lots in order (highest cost basis first for HIFO)
        for i, (lot, remaining_amount) in enumerate(remaining_lots):
            if remaining_sale_amount <= 0:
                break
            
            if remaining_amount <= 0:
                continue
            
            # Can only sell BTC that was acquired before the sale date
            if lot.date > sale.date:
                continue
            
            # Calculate how much to take from this lot
            amount_to_use = min(remaining_sale_amount, remaining_amount)
            cost_basis_used = (amount_to_use / lot.btc_amount) * lot.cost_basis_usd
            gain_loss = (sale.sale_price_per_btc - lot.price_per_btc) * amount_to_use
            
            # Calculate holding period
            holding_period_days, is_long_term = calculate_holding_period(lot.date, sale.date)
            
            # Create matched lot record
            matched = MatchedLot(
                sale_date=sale.date,
                sale_amount=sale.btc_amount,
                sale_price_per_btc=sale.sale_price_per_btc,
                lot_date=lot.date,
                lot_amount_used=amount_to_use,
                lot_cost_basis_per_btc=lot.price_per_btc,
                cost_basis_used=cost_basis_used,
                gain_loss=gain_loss,
                holding_period_days=holding_period_days,
                is_long_term=is_long_term,
                sale_source=sale.source,
                lot_source=lot.source
            )
            matched_lots.append(matched)
            
            # Update remaining amounts
            remaining_sale_amount -= amount_to_use
            remaining_lots[i] = (lot, remaining_amount - amount_to_use)
    
    # Calculate totals
    total_realized_gain = sum(m.gain_loss for m in matched_lots)
    short_term_gain = sum(m.gain_loss for m in matched_lots if not m.is_long_term)
    long_term_gain = sum(m.gain_loss for m in matched_lots if m.is_long_term)
    total_cost_basis = sum(m.cost_basis_used for m in matched_lots)
    total_proceeds = sum(m.sale_price_per_btc * m.lot_amount_used for m in matched_lots)
    
    return MatchingResults(
        method="HIFO",
        matched_lots=matched_lots,
        total_realized_gain=total_realized_gain,
        short_term_gain=short_term_gain,
        long_term_gain=long_term_gain,
        total_cost_basis=total_cost_basis,
        total_proceeds=total_proceeds
    )


def compare_methods(
    fifo_results: MatchingResults,
    lifo_results: MatchingResults,
    hifo_results: MatchingResults
) -> pd.DataFrame:
    """
    Compare results from all three cost basis methods.
    
    Args:
        fifo_results: FIFO matching results
        lifo_results: LIFO matching results
        hifo_results: HIFO matching results
    
    Returns:
        DataFrame comparing the three methods
    """
    comparison_data = {
        'Method': ['FIFO', 'LIFO', 'HIFO'],
        'Total Realized Gain (USD)': [
            fifo_results.total_realized_gain,
            lifo_results.total_realized_gain,
            hifo_results.total_realized_gain
        ],
        'Short-Term Gain (USD)': [
            fifo_results.short_term_gain,
            lifo_results.short_term_gain,
            hifo_results.short_term_gain
        ],
        'Long-Term Gain (USD)': [
            fifo_results.long_term_gain,
            lifo_results.long_term_gain,
            hifo_results.long_term_gain
        ],
        'Total Cost Basis (USD)': [
            fifo_results.total_cost_basis,
            lifo_results.total_cost_basis,
            hifo_results.total_cost_basis
        ],
        'Total Proceeds (USD)': [
            fifo_results.total_proceeds,
            lifo_results.total_proceeds,
            hifo_results.total_proceeds
        ]
    }
    
    return pd.DataFrame(comparison_data)


def export_matching_results_to_csv(
    results: MatchingResults,
    output_path: str
):
    """
    Export detailed matching results to CSV.
    
    Args:
        results: MatchingResults object
        output_path: Path to output CSV file
    """
    data = []
    for match in results.matched_lots:
        data.append({
            'Sale Date': match.sale_date.strftime('%Y-%m-%d %H:%M:%S'),
            'Sale Amount (BTC)': match.sale_amount,
            'Sale Price per BTC (USD)': match.sale_price_per_btc,
            'Sale Source': match.sale_source,
            'Lot Date': match.lot_date.strftime('%Y-%m-%d %H:%M:%S'),
            'Lot Amount Used (BTC)': match.lot_amount_used,
            'Lot Cost Basis per BTC (USD)': match.lot_cost_basis_per_btc,
            'Cost Basis Used (USD)': match.cost_basis_used,
            'Gain/Loss (USD)': match.gain_loss,
            'Holding Period (Days)': match.holding_period_days,
            'Is Long-Term': match.is_long_term,
            'Lot Source': match.lot_source
        })
    
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Exported {len(results.matched_lots)} matched lot records to {output_path}")


def calculate_remaining_lots(
    acquisition_lots: List[AcquisitionLot],
    matched_lots: List[MatchedLot]
) -> List[Tuple[AcquisitionLot, float]]:
    """
    Calculate remaining amounts in lots after matching.
    
    Args:
        acquisition_lots: Original acquisition lots
        matched_lots: Matched lot records from matching
    
    Returns:
        List of tuples (lot, remaining_amount)
    """
    # Track how much was used from each lot
    lot_usage = {}
    for match in matched_lots:
        # Find the lot that matches this match (by date and source)
        lot_key = (match.lot_date, match.lot_source)
        if lot_key not in lot_usage:
            lot_usage[lot_key] = 0.0
        lot_usage[lot_key] += match.lot_amount_used
    
    # Calculate remaining amounts
    remaining = []
    for lot in acquisition_lots:
        lot_key = (lot.date, lot.source)
        used = lot_usage.get(lot_key, 0.0)
        remaining_amount = lot.btc_amount - used
        if remaining_amount > 0.00000001:  # Avoid floating point issues
            remaining.append((lot, remaining_amount))
    
    return remaining

