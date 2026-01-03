"""
Acquisition Lots Builder
Builds acquisition lots from buy and mining transactions.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from utilities import clean_currency, get_btc_price_for_date, fetch_btc_prices_batch, load_config


@dataclass
class AcquisitionLot:
    """Represents a single acquisition lot of BTC."""
    date: datetime
    btc_amount: float
    cost_basis_usd: float
    source: str  # "BUY", "MINING", or "LEGACY"
    price_per_btc: float  # Cost basis per BTC
    
    def __post_init__(self):
        """Calculate price per BTC."""
        if self.btc_amount > 0:
            self.price_per_btc = self.cost_basis_usd / self.btc_amount
        else:
            self.price_per_btc = 0.0


def parse_buy_transactions(csv_path: str, price_lookup: Optional[Dict] = None) -> List[AcquisitionLot]:
    """
    Parse River Buys CSV file.
    
    Expected columns: Date, River Ref #, BTC Value, BTC Price, Fee (USD), USD Value
    Cost basis = (yfinance BTC price * BTC amount) + Fee (USD from CSV)
    Note: Uses yfinance prices instead of CSV prices (which are estimates)
    """
    df = pd.read_csv(csv_path)
    
    lots = []
    dates_to_fetch = []
    date_rows = []  # Store (date, row) pairs for processing
    
    # First pass: collect all dates
    for _, row in df.iterrows():
        try:
            date_str = str(row['Date']).strip()
            date = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date):
                continue  # Skip rows with invalid dates
            
            btc_amount = float(row['BTC Value']) if pd.notna(row['BTC Value']) else 0.0
            if btc_amount <= 0:
                continue  # Skip rows with no BTC
            
            dates_to_fetch.append(date)
            date_rows.append((date, row))
        except (KeyError, ValueError):
            continue
    
    # Batch fetch all prices at once
    if price_lookup is None and dates_to_fetch:
        print(f"Fetching BTC prices for {len(dates_to_fetch)} buy transactions...")
        batch_prices = fetch_btc_prices_batch(dates_to_fetch)
    else:
        batch_prices = {}
    
    # Second pass: create lots using fetched prices
    for date, row in date_rows:
        # Get BTC price from yfinance
        if price_lookup:
            date_key = date.date()
            btc_price = price_lookup.get(date_key, 0.0)
        else:
            date_key = date.date()
            # Check batch prices first, then cache, then fetch individually
            if date_key in batch_prices:
                btc_price = batch_prices[date_key]
            else:
                btc_price = get_btc_price_for_date(date, price_lookup)
        
        if btc_price <= 0:
            print(f"Warning: Could not get BTC price for buy date {date_key}. Skipping.")
            continue
        
        # Get fee from CSV (actual fee paid)
        fee_usd = clean_currency(row.get('Fee (USD)', 0))
        
        # Calculate cost basis: (yfinance price * amount) + fee
        btc_amount = float(row['BTC Value'])
        cost_basis = (btc_price * btc_amount) + fee_usd
        
        lot = AcquisitionLot(
            date=date,
            btc_amount=btc_amount,
            cost_basis_usd=cost_basis,
            source="BUY",
            price_per_btc=0.0  # Will be calculated in __post_init__
        )
        lots.append(lot)
    
    return lots
def parse_mining_transactions(csv_path: str, price_lookup: Optional[Dict] = None) -> List[AcquisitionLot]:
    """
    Parse F2Pool Mining CSV file.
    
    Expected columns: Account ID, Date, Amount, Address, Status, TXID
    Cost basis = BTC price at mining time (FMV at receipt)
    """
    df = pd.read_csv(csv_path)
    
    lots = []
    dates_to_fetch = []
    date_rows = []  # Store (date, row) pairs for processing
    
    # First pass: collect all dates
    for _, row in df.iterrows():
        try:
            date_str = str(row['Date']).strip()
            date = pd.to_datetime(date_str, errors='coerce')
            if pd.isna(date):
                continue
            
            btc_amount = float(row['Amount']) if pd.notna(row['Amount']) else 0.0
            if btc_amount <= 0:
                continue
            
            dates_to_fetch.append(date)
            date_rows.append((date, row))
        except (KeyError, ValueError):
            continue
    
    # Batch fetch all prices at once
    if price_lookup is None and dates_to_fetch:
        print(f"Fetching BTC prices for {len(dates_to_fetch)} mining transactions...")
        batch_prices = fetch_btc_prices_batch(dates_to_fetch)
    else:
        batch_prices = {}
    
    # Second pass: create lots using fetched prices
    for date, row in date_rows:
        # Get BTC price
        if price_lookup:
            date_key = date.date()
            btc_price = price_lookup.get(date_key, 0.0)
        else:
            date_key = date.date()
            # Check batch prices first, then cache, then fetch individually
            if date_key in batch_prices:
                btc_price = batch_prices[date_key]
            else:
                btc_price = get_btc_price_for_date(date, price_lookup)
        
        if btc_price <= 0:
            print(f"Warning: Could not get BTC price for mining date {date_key}. Skipping.")
            continue
        
        btc_amount = float(row['Amount'])
        cost_basis = btc_price * btc_amount
        
        lot = AcquisitionLot(
            date=date,
            btc_amount=btc_amount,
            cost_basis_usd=cost_basis,
            source="MINING",
            price_per_btc=0.0  # Will be calculated in __post_init__
        )
        lots.append(lot)
    
    return lots


def build_acquisition_lots(
    buy_csv_path: Optional[str] = None,
    mining_csv_path: Optional[str] = None,
    current_balance_btc: Optional[float] = None,
    legacy_acquisition_date: Optional[datetime] = None,
    price_lookup: Optional[Dict] = None,
    config_path: str = "config.yaml"
) -> List[AcquisitionLot]:
    """
    Build all acquisition lots from buys, mining, and legacy BTC.
    
    Args:
        buy_csv_path: Path to River Buys CSV (overrides config if provided)
        mining_csv_path: Path to F2Pool Mining CSV (overrides config if provided)
        current_balance_btc: Current BTC balance (overrides config if provided)
        legacy_acquisition_date: Date to use for legacy BTC (overrides config if provided)
        price_lookup: Optional dict for BTC prices by date (for mining)
        config_path: Path to config YAML file
    
    Returns:
        List of AcquisitionLot objects, sorted by date
    """
    # Load config
    config = load_config(config_path)
    
    # Get file paths from config if not provided
    if buy_csv_path is None:
        buy_csv_path = config.get('data_files', {}).get('buy_csv', 'data/buys.csv')
    if mining_csv_path is None:
        mining_csv_path = config.get('data_files', {}).get('mining_csv', 'data/mining.csv')
    
    # Get balance and legacy date from config if not provided
    if current_balance_btc is None:
        current_balance_btc = config.get('current_balance_btc', 0.0)
    if legacy_acquisition_date is None:
        legacy_date_str = config.get('legacy', {}).get('acquisition_date', '2009-01-03')
        legacy_acquisition_date = datetime.strptime(legacy_date_str, '%Y-%m-%d')
    # Parse buys
    buy_lots = parse_buy_transactions(buy_csv_path, price_lookup)
    print(f"Parsed {len(buy_lots)} buy transactions")
    
    # Parse mining
    mining_lots = parse_mining_transactions(mining_csv_path, price_lookup)
    print(f"Parsed {len(mining_lots)} mining transactions")
    
    # Combine known sources
    all_lots = buy_lots + mining_lots
    
    # Calculate total from known sources
    total_known_btc = sum(lot.btc_amount for lot in all_lots)
    print(f"Total BTC from known sources: {total_known_btc:.8f}")
    
    # Calculate legacy BTC
    legacy_btc = current_balance_btc - total_known_btc
    print(f"Legacy BTC (current balance - known sources): {legacy_btc:.8f}")
    
    # Add legacy lot if there's any legacy BTC (skip if negative - means sales have occurred)
    if legacy_btc > 0.00000001:  # Small threshold to avoid floating point issues
        legacy_date = legacy_acquisition_date or datetime(2009, 1, 3)
        legacy_lot = AcquisitionLot(
            date=legacy_date,
            btc_amount=legacy_btc,
            cost_basis_usd=0.0,  # Cost basis = $0 for unknown legacy BTC
            source="LEGACY",
            price_per_btc=0.0  # Will be 0.0 after __post_init__
        )
        all_lots.append(legacy_lot)
        print(f"Added legacy lot: {legacy_btc:.8f} BTC from {legacy_date.date()}")
    
    # Sort by date
    all_lots.sort(key=lambda x: x.date)
    
    print(f"\nTotal acquisition lots: {len(all_lots)}")
    print(f"Total BTC: {sum(lot.btc_amount for lot in all_lots):.8f}")
    
    return all_lots


def export_lots_to_csv(lots: List[AcquisitionLot], output_path: str):
    """
    Export acquisition lots to CSV file.
    
    Args:
        lots: List of AcquisitionLot objects
        output_path: Path to output CSV file
    """
    data = []
    for lot in lots:
        data.append({
            'Date': lot.date.strftime('%Y-%m-%d %H:%M:%S'),
            'BTC Amount': lot.btc_amount,
            'Cost Basis (USD)': lot.cost_basis_usd,
            'Price per BTC (USD)': lot.price_per_btc,
            'Source': lot.source
        })
    
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"\nExported {len(lots)} acquisition lots to {output_path}")


if __name__ == "__main__":
    # Test the acquisition lot builder
    # File paths will be loaded from config.yaml
    config_file = "config.yaml"
    
    lots = build_acquisition_lots(config_path=config_file)
    
    print("\n=== Acquisition Lots Summary ===")
    for i, lot in enumerate(lots[:10], 1):  # Show first 10
        print(f"{i}. {lot.date.date()} | {lot.btc_amount:.8f} BTC | "
              f"${lot.cost_basis_usd:,.2f} | ${lot.price_per_btc:,.2f}/BTC | {lot.source}")
    
    if len(lots) > 10:
        print(f"... and {len(lots) - 10} more lots")
    
    # Export to CSV
    import os
    os.makedirs("outputs", exist_ok=True)
    export_lots_to_csv(lots, "outputs/acquisition_lots.csv")

