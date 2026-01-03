"""
Bitcoin Tax Calculator - Master Script
Orchestrates the entire tax calculation process.
"""

import os
from datetime import datetime
from acquisition_lots import build_acquisition_lots, export_lots_to_csv, AcquisitionLot
from sales_transactions import parse_all_sales, export_sales_to_csv, SaleTransaction
from utilities import load_config


def main():
    """Main entry point for the Bitcoin tax calculation system."""
    print("=" * 70)
    print("Bitcoin Tax Calculator")
    print("=" * 70)
    print()
    
    # Load configuration
    config_path = "config.yaml"
    config = load_config(config_path)
    
    # Create outputs directory if it doesn't exist
    os.makedirs("outputs", exist_ok=True)
    
    # Step 1: Build Acquisition Lots
    print("STEP 1: Building Acquisition Lots")
    print("-" * 70)
    lots = build_acquisition_lots(config_path=config_path)
    export_lots_to_csv(lots, "outputs/acquisition_lots.csv")
    print()
    
    # Step 2: Parse Sales Transactions
    print("STEP 2: Parsing Sales Transactions")
    print("-" * 70)
    sales = parse_all_sales(config_path=config_path)
    export_sales_to_csv(sales, "outputs/sales_transactions.csv")
    print()
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    total_acquired_btc = sum(lot.btc_amount for lot in lots)
    total_sold_btc = sum(sale.btc_amount for sale in sales)
    current_balance = config.get('current_balance_btc', 0.0)
    
    print(f"Total Acquisition Lots: {len(lots)}")
    print(f"Total BTC Acquired: {total_acquired_btc:.8f}")
    print(f"Total Sales Transactions: {len(sales)}")
    print(f"Total BTC Sold: {total_sold_btc:.8f}")
    print(f"Current BTC Balance (from config): {current_balance:.8f}")
    
    # Calculate expected balance
    expected_balance = total_acquired_btc - total_sold_btc
    print(f"Expected Balance (acquired - sold): {expected_balance:.8f}")
    
    # Legacy BTC calculation (will be updated after matching sales to acquisitions)
    legacy_btc = current_balance - expected_balance
    if legacy_btc > 0.00000001:
        print(f"Legacy BTC (current - expected): {legacy_btc:.8f}")
    elif legacy_btc < -0.00000001:
        print(f"NOTE: Current balance is less than expected (sold more than acquired)")
        print(f"  Difference: {abs(legacy_btc):.8f} BTC")
    else:
        print("Balance matches expected (no legacy BTC needed)")
    
    print()
    print("Output files:")
    print("  - outputs/acquisition_lots.csv")
    print("  - outputs/sales_transactions.csv")
    print()
    print("=" * 70)
    print("Ready for Step 3: Cost Basis Matching (FIFO, LIFO, HIFO)")
    print("=" * 70)


if __name__ == "__main__":
    main()

