"""
Bitcoin Tax Calculator - Master Script
Orchestrates the entire tax calculation process.
"""

import os
from datetime import datetime
from acquisition_lots import build_acquisition_lots, export_lots_to_csv, AcquisitionLot
from sales_transactions import parse_all_sales, export_sales_to_csv, SaleTransaction
from cost_basis_matching import (
    match_sales_to_lots_fifo,
    match_sales_to_lots_lifo,
    match_sales_to_lots_hifo,
    compare_methods,
    export_matching_results_to_csv,
    calculate_remaining_lots
)
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
    
    # Step 3: Cost Basis Matching
    print("STEP 3: Cost Basis Matching (FIFO, LIFO, HIFO)")
    print("-" * 70)
    
    # Run all three matching methods
    print("Running FIFO matching...")
    fifo_results = match_sales_to_lots_fifo(lots, sales)
    
    print("Running LIFO matching...")
    lifo_results = match_sales_to_lots_lifo(lots, sales)
    
    print("Running HIFO matching...")
    hifo_results = match_sales_to_lots_hifo(lots, sales)
    
    # Export detailed results for each method
    export_matching_results_to_csv(fifo_results, "outputs/matching_results_fifo.csv")
    export_matching_results_to_csv(lifo_results, "outputs/matching_results_lifo.csv")
    export_matching_results_to_csv(hifo_results, "outputs/matching_results_hifo.csv")
    
    # Compare methods
    comparison_df = compare_methods(fifo_results, lifo_results, hifo_results)
    comparison_path = "outputs/method_comparison.csv"
    comparison_df.to_csv(comparison_path, index=False)
    print(f"\nExported method comparison to {comparison_path}")
    
    print("\n" + "=" * 70)
    print("METHOD COMPARISON")
    print("=" * 70)
    print(comparison_df.to_string(index=False))
    print()
    
    # Find optimal method (lowest total gain = lowest taxes)
    optimal_method = comparison_df.loc[comparison_df['Total Realized Gain (USD)'].idxmin(), 'Method']
    optimal_gain = comparison_df.loc[comparison_df['Total Realized Gain (USD)'].idxmin(), 'Total Realized Gain (USD)']
    
    print(f"OPTIMAL METHOD: {optimal_method}")
    print(f"  Total Realized Gain: ${optimal_gain:,.2f}")
    optimal_row = comparison_df[comparison_df['Method'] == optimal_method].iloc[0]
    print(f"  Short-Term Gain: ${optimal_row['Short-Term Gain (USD)']:,.2f}")
    print(f"  Long-Term Gain: ${optimal_row['Long-Term Gain (USD)']:,.2f}")
    print()
    
    # Calculate remaining lots using optimal method
    optimal_results = hifo_results if optimal_method == "HIFO" else (lifo_results if optimal_method == "LIFO" else fifo_results)
    remaining_lots = calculate_remaining_lots(lots, optimal_results.matched_lots)
    total_remaining_btc = sum(remaining for _, remaining in remaining_lots)
    
    # Recalculate legacy BTC after matching
    current_balance = config.get('current_balance_btc', 0.0)
    legacy_btc_after_matching = current_balance - total_remaining_btc
    
    print("=" * 70)
    print("REMAINING BTC AFTER MATCHING")
    print("=" * 70)
    print(f"Total Remaining BTC (from matched lots): {total_remaining_btc:.8f}")
    print(f"Current BTC Balance (from config): {current_balance:.8f}")
    if legacy_btc_after_matching > 0.00000001:
        print(f"Legacy BTC (current - remaining): {legacy_btc_after_matching:.8f}")
    elif legacy_btc_after_matching < -0.00000001:
        print(f"NOTE: Balance mismatch of {abs(legacy_btc_after_matching):.8f} BTC")
    else:
        print("Balance matches expected (no legacy BTC)")
    print()
    
    print("=" * 70)
    print("Output files:")
    print("  - outputs/acquisition_lots.csv")
    print("  - outputs/sales_transactions.csv")
    print("  - outputs/matching_results_fifo.csv")
    print("  - outputs/matching_results_lifo.csv")
    print("  - outputs/matching_results_hifo.csv")
    print("  - outputs/method_comparison.csv")
    print("=" * 70)


if __name__ == "__main__":
    main()

