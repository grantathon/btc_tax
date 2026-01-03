"""
Tax Reporting Module
Generates tax-ready reports for Form 8949 and Schedule D.
"""

from datetime import datetime
from typing import List
import pandas as pd
from cost_basis_matching import MatchingResults, MatchedLot


def generate_form_8949_report(results: MatchingResults) -> pd.DataFrame:
    """
    Generate a Form 8949 ready report from matching results.
    
    Form 8949 columns:
    - Description of property
    - Date acquired
    - Date sold
    - Proceeds (sales price)
    - Cost or other basis
    - Code (adjustment codes)
    - Amount of adjustment
    - Gain or (loss)
    
    Args:
        results: MatchingResults object
    
    Returns:
        DataFrame formatted for Form 8949
    """
    data = []
    
    for match in results.matched_lots:
        # Group by sale date for Form 8949 (each sale gets one row, or multiple if different holding periods)
        # For simplicity, we'll create one row per matched lot
        # In practice, you might want to aggregate by sale date
        
        description = f"Bitcoin ({match.lot_amount_used:.8f} BTC)"
        
        # Format dates
        date_acquired = match.lot_date.strftime('%m/%d/%Y')
        date_sold = match.sale_date.strftime('%m/%d/%Y')
        
        # Proceeds (sales price * amount)
        proceeds = match.sale_price_per_btc * match.lot_amount_used
        
        # Cost basis
        cost_basis = match.cost_basis_used
        
        # Gain or loss
        gain_loss = match.gain_loss
        
        # Adjustment code and amount (typically blank unless you have specific adjustments)
        adjustment_code = ""
        adjustment_amount = 0.0
        
        data.append({
            'Description': description,
            'Date Acquired': date_acquired,
            'Date Sold': date_sold,
            'Proceeds (Sales Price)': proceeds,
            'Cost or Other Basis': cost_basis,
            'Code': adjustment_code,
            'Amount of Adjustment': adjustment_amount,
            'Gain or (Loss)': gain_loss,
            'Holding Period': 'Long-Term' if match.is_long_term else 'Short-Term',
            'Sale Source': match.sale_source,
            'Lot Source': match.lot_source
        })
    
    df = pd.DataFrame(data)
    
    # Sort by date sold, then by date acquired
    df['Date Sold Sort'] = pd.to_datetime(df['Date Sold'])
    df['Date Acquired Sort'] = pd.to_datetime(df['Date Acquired'])
    df = df.sort_values(['Date Sold Sort', 'Date Acquired Sort'])
    df = df.drop(['Date Sold Sort', 'Date Acquired Sort'], axis=1)
    
    return df


def generate_summary_report(results: MatchingResults) -> dict:
    """
    Generate a summary report with totals for Schedule D.
    
    Args:
        results: MatchingResults object
    
    Returns:
        Dictionary with summary statistics
    """
    # Separate short-term and long-term
    short_term_matches = [m for m in results.matched_lots if not m.is_long_term]
    long_term_matches = [m for m in results.matched_lots if m.is_long_term]
    
    short_term_proceeds = sum(m.sale_price_per_btc * m.lot_amount_used for m in short_term_matches)
    short_term_cost_basis = sum(m.cost_basis_used for m in short_term_matches)
    short_term_gain = sum(m.gain_loss for m in short_term_matches)
    
    long_term_proceeds = sum(m.sale_price_per_btc * m.lot_amount_used for m in long_term_matches)
    long_term_cost_basis = sum(m.cost_basis_used for m in long_term_matches)
    long_term_gain = sum(m.gain_loss for m in long_term_matches)
    
    total_proceeds = short_term_proceeds + long_term_proceeds
    total_cost_basis = short_term_cost_basis + long_term_cost_basis
    total_gain = short_term_gain + long_term_gain
    
    return {
        'Method': results.method,
        'Short-Term Transactions': len(short_term_matches),
        'Short-Term Proceeds': short_term_proceeds,
        'Short-Term Cost Basis': short_term_cost_basis,
        'Short-Term Gain (Loss)': short_term_gain,
        'Long-Term Transactions': len(long_term_matches),
        'Long-Term Proceeds': long_term_proceeds,
        'Long-Term Cost Basis': long_term_cost_basis,
        'Long-Term Gain (Loss)': long_term_gain,
        'Total Transactions': len(results.matched_lots),
        'Total Proceeds': total_proceeds,
        'Total Cost Basis': total_cost_basis,
        'Total Gain (Loss)': total_gain
    }


def generate_accountant_summary(results: MatchingResults) -> str:
    """
    Generate a text summary report suitable for an accountant.
    
    Args:
        results: MatchingResults object
    
    Returns:
        Formatted string report
    """
    summary = generate_summary_report(results)
    
    report = []
    report.append("=" * 80)
    report.append(f"BITCOIN TAX REPORT - {results.method} METHOD")
    report.append("=" * 80)
    report.append("")
    report.append(f"Cost Basis Method: {summary['Method']}")
    report.append("")
    report.append("-" * 80)
    report.append("SHORT-TERM CAPITAL GAINS/LOSSES (Held 1 year or less)")
    report.append("-" * 80)
    report.append(f"Number of Transactions: {summary['Short-Term Transactions']}")
    report.append(f"Total Proceeds: ${summary['Short-Term Proceeds']:,.2f}")
    report.append(f"Total Cost Basis: ${summary['Short-Term Cost Basis']:,.2f}")
    report.append(f"Net Gain (Loss): ${summary['Short-Term Gain (Loss)']:,.2f}")
    report.append("")
    report.append("-" * 80)
    report.append("LONG-TERM CAPITAL GAINS/LOSSES (Held more than 1 year)")
    report.append("-" * 80)
    report.append(f"Number of Transactions: {summary['Long-Term Transactions']}")
    report.append(f"Total Proceeds: ${summary['Long-Term Proceeds']:,.2f}")
    report.append(f"Total Cost Basis: ${summary['Long-Term Cost Basis']:,.2f}")
    report.append(f"Net Gain (Loss): ${summary['Long-Term Gain (Loss)']:,.2f}")
    report.append("")
    report.append("-" * 80)
    report.append("TOTALS")
    report.append("-" * 80)
    report.append(f"Total Transactions: {summary['Total Transactions']}")
    report.append(f"Total Proceeds: ${summary['Total Proceeds']:,.2f}")
    report.append(f"Total Cost Basis: ${summary['Total Cost Basis']:,.2f}")
    report.append(f"Total Net Gain (Loss): ${summary['Total Gain (Loss)']:,.2f}")
    report.append("")
    report.append("=" * 80)
    report.append("")
    report.append("NOTE: Detailed transaction-by-transaction data is available in")
    report.append("the Form 8949 CSV file for complete tax filing documentation.")
    report.append("")
    
    return "\n".join(report)


def export_tax_reports(results: MatchingResults, output_dir: str = "outputs"):
    """
    Export all tax reporting documents.
    
    Args:
        results: MatchingResults object
        output_dir: Directory to save output files
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate Form 8949 report
    form_8949_df = generate_form_8949_report(results)
    form_8949_path = f"{output_dir}/form_8949_{results.method.lower()}.csv"
    form_8949_df.to_csv(form_8949_path, index=False)
    print(f"Exported Form 8949 report to {form_8949_path}")
    
    # Generate summary report
    summary = generate_summary_report(results)
    summary_df = pd.DataFrame([summary])
    summary_path = f"{output_dir}/schedule_d_summary_{results.method.lower()}.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"Exported Schedule D summary to {summary_path}")
    
    # Generate text summary for accountant
    accountant_report = generate_accountant_summary(results)
    accountant_path = f"{output_dir}/accountant_summary_{results.method.lower()}.txt"
    with open(accountant_path, 'w') as f:
        f.write(accountant_report)
    print(f"Exported accountant summary to {accountant_path}")
    
    return form_8949_path, summary_path, accountant_path

