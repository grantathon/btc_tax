"""
Sales Transactions Parser
Parses sales and disposal transactions (River Sells, Compass Mining Payments).
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from utilities import clean_currency, get_btc_price_for_date, fetch_btc_prices_batch, load_config
import os


@dataclass
class SaleTransaction:
    """Represents a single sale/disposal of BTC."""
    date: datetime
    btc_amount: float
    sale_price_per_btc: float  # Price per BTC at sale time (from yfinance)
    sale_proceeds_usd: float  # Total sale proceeds (price * amount)
    fee_usd: float  # Fees paid (from CSV)
    net_proceeds_usd: float  # Sale proceeds minus fees
    source: str  # "RIVER_SELL" or "COMPASS_PAYMENT"
    
    def __post_init__(self):
        """Calculate net proceeds."""
        self.net_proceeds_usd = self.sale_proceeds_usd - self.fee_usd


def parse_river_sells(csv_path: str, price_lookup: Optional[Dict] = None) -> List[SaleTransaction]:
    """
    Parse River Sells CSV file.
    
    Expected columns: Date, River Ref #, BTC Value, BTC Price, Fee (USD), USD Value
    Uses yfinance prices instead of CSV prices (which are estimates)
    """
    df = pd.read_csv(csv_path)
    
    sales = []
    dates_to_fetch = []
    date_rows = []  # Store (date, row) pairs for processing
    
    # First pass: collect all dates
    for _, row in df.iterrows():
        try:
            date_str = str(row['Date']).strip()
            date = pd.to_datetime(date_str, errors='coerce', utc=True)
            if pd.isna(date):
                continue
            # Convert to timezone-naive datetime
            if date.tz is not None:
                date = date.tz_localize(None)
            
            btc_amount = float(row['BTC Value']) if pd.notna(row['BTC Value']) else 0.0
            if btc_amount <= 0:
                continue
            
            dates_to_fetch.append(date)
            date_rows.append((date, row))
        except (KeyError, ValueError):
            continue
    
    # Batch fetch all prices at once
    if price_lookup is None and dates_to_fetch:
        print(f"Fetching BTC prices for {len(dates_to_fetch)} River sell transactions...")
        batch_prices = fetch_btc_prices_batch(dates_to_fetch)
    else:
        batch_prices = {}
    
    # Second pass: create sales using fetched prices
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
            print(f"Warning: Could not get BTC price for sale date {date_key}. Skipping.")
            continue
        
        # Get fee from CSV (actual fee paid)
        fee_usd = clean_currency(row.get('Fee (USD)', 0))
        
        # Calculate sale proceeds: yfinance price * amount
        btc_amount = float(row['BTC Value'])
        sale_proceeds = btc_price * btc_amount
        
        sale = SaleTransaction(
            date=date,
            btc_amount=btc_amount,
            sale_price_per_btc=btc_price,
            sale_proceeds_usd=sale_proceeds,
            fee_usd=fee_usd,
            net_proceeds_usd=0.0,  # Will be calculated in __post_init__
            source="RIVER_SELL"
        )
        sales.append(sale)
    
    return sales


def parse_compass_payments(csv_path: str, price_lookup: Optional[Dict] = None) -> List[SaleTransaction]:
    """
    Parse Compass Mining Payments CSV file.
    
    Expected columns: Date, Amount, Fee, Total Amount, Notes, TXID
    These are expenses paid in BTC (hosting costs), treated as sales/disposals.
    Uses yfinance prices instead of CSV prices.
    """
    df = pd.read_csv(csv_path)
    
    sales = []
    dates_to_fetch = []
    date_rows = []  # Store (date, row) pairs for processing
    
    # First pass: collect all dates
    for _, row in df.iterrows():
        try:
            date_str = str(row['Date']).strip()
            date = pd.to_datetime(date_str, errors='coerce', utc=True)
            if pd.isna(date):
                continue
            # Convert to timezone-naive datetime
            if date.tz is not None:
                date = date.tz_localize(None)
            
            # Amount is the BTC amount sold/paid
            btc_amount = float(row['Amount']) if pd.notna(row['Amount']) else 0.0
            if btc_amount <= 0:
                continue
            
            dates_to_fetch.append(date)
            date_rows.append((date, row))
        except (KeyError, ValueError):
            continue
    
    # Batch fetch all prices at once
    if price_lookup is None and dates_to_fetch:
        print(f"Fetching BTC prices for {len(dates_to_fetch)} Compass payment transactions...")
        batch_prices = fetch_btc_prices_batch(dates_to_fetch)
    else:
        batch_prices = {}
    
    # Second pass: create sales using fetched prices
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
            print(f"Warning: Could not get BTC price for payment date {date_key}. Skipping.")
            continue
        
        # Fee is in BTC (convert to USD using sale price)
        fee_btc = float(row.get('Fee', 0)) if pd.notna(row.get('Fee', 0)) else 0.0
        fee_usd = fee_btc * btc_price
        
        # Calculate sale proceeds: yfinance price * amount
        btc_amount = float(row['Amount'])
        sale_proceeds = btc_price * btc_amount
        
        sale = SaleTransaction(
            date=date,
            btc_amount=btc_amount,
            sale_price_per_btc=btc_price,
            sale_proceeds_usd=sale_proceeds,
            fee_usd=fee_usd,
            net_proceeds_usd=0.0,  # Will be calculated in __post_init__
            source="COMPASS_PAYMENT"
        )
        sales.append(sale)
    
    return sales


def parse_all_sales(
    river_sells_csv_path: Optional[str] = None,
    compass_payments_csv_path: Optional[str] = None,
    price_lookup: Optional[Dict] = None,
    config_path: str = "config.yaml"
) -> List[SaleTransaction]:
    """
    Parse all sales transactions from River Sells and Compass Payments.
    
    Args:
        river_sells_csv_path: Path to River Sells CSV (overrides config if provided)
        compass_payments_csv_path: Path to Compass Payments CSV (overrides config if provided)
        price_lookup: Optional dict for BTC prices by date
        config_path: Path to config YAML file
    
    Returns:
        List of SaleTransaction objects, sorted by date
    """
    # Load config
    config = load_config(config_path)
    
    # Get file paths from config if not provided
    if river_sells_csv_path is None:
        river_sells_csv_path = config.get('data_files', {}).get('sell_csv', 'data/sells.csv')
    if compass_payments_csv_path is None:
        compass_payments_csv_path = config.get('data_files', {}).get('compass_payments_csv', 'data/compass_payments.csv')
    
    all_sales = []
    
    # Parse River Sells
    if os.path.exists(river_sells_csv_path):
        river_sales = parse_river_sells(river_sells_csv_path, price_lookup)
        print(f"Parsed {len(river_sales)} River sell transactions")
        all_sales.extend(river_sales)
    else:
        print(f"Warning: River sells CSV not found: {river_sells_csv_path}")
    
    # Parse Compass Payments
    if os.path.exists(compass_payments_csv_path):
        compass_sales = parse_compass_payments(compass_payments_csv_path, price_lookup)
        print(f"Parsed {len(compass_sales)} Compass payment transactions")
        all_sales.extend(compass_sales)
    else:
        print(f"Warning: Compass payments CSV not found: {compass_payments_csv_path}")
    
    # Sort by date
    all_sales.sort(key=lambda x: x.date)
    
    print(f"\nTotal sales transactions: {len(all_sales)}")
    print(f"Total BTC sold: {sum(sale.btc_amount for sale in all_sales):.8f}")
    
    return all_sales


def export_sales_to_csv(sales: List[SaleTransaction], output_path: str):
    """
    Export sales transactions to CSV file.
    
    Args:
        sales: List of SaleTransaction objects
        output_path: Path to output CSV file
    """
    data = []
    for sale in sales:
        data.append({
            'Date': sale.date.strftime('%Y-%m-%d %H:%M:%S'),
            'BTC Amount': sale.btc_amount,
            'Sale Price per BTC (USD)': sale.sale_price_per_btc,
            'Sale Proceeds (USD)': sale.sale_proceeds_usd,
            'Fee (USD)': sale.fee_usd,
            'Net Proceeds (USD)': sale.net_proceeds_usd,
            'Source': sale.source
        })
    
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"\nExported {len(sales)} sales transactions to {output_path}")


if __name__ == "__main__":
    # Test the sales parser
    # File paths will be loaded from config.yaml
    config_file = "config.yaml"
    
    sales = parse_all_sales(config_path=config_file)
    
    print("\n=== Sales Transactions Summary ===")
    for i, sale in enumerate(sales[:10], 1):  # Show first 10
        print(f"{i}. {sale.date.date()} | {sale.btc_amount:.8f} BTC | "
              f"${sale.sale_price_per_btc:,.2f}/BTC | ${sale.net_proceeds_usd:,.2f} net | {sale.source}")
    
    if len(sales) > 10:
        print(f"... and {len(sales) - 10} more sales")
    
    # Export to CSV
    os.makedirs("outputs", exist_ok=True)
    export_sales_to_csv(sales, "outputs/sales_transactions.csv")

