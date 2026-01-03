"""
Utility functions shared across the bitcoin tax calculation modules.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List
import yfinance as yf
import yaml
import os


# Global cache for BTC prices to avoid repeated API calls
_btc_price_cache: Dict[datetime.date, float] = {}
_btc_ticker = None


def clean_currency(value: str) -> float:
    """
    Convert currency string like '$39,226.82' to float.
    
    Args:
        value: Currency string with $ and commas
    
    Returns:
        Float value, or 0.0 if invalid
    """
    if pd.isna(value) or value == '' or str(value).strip() == '#N/A':
        return 0.0
    # Remove $, commas, and whitespace
    cleaned = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return 0.0


def get_btc_price_for_date(date: datetime, price_lookup: Optional[Dict] = None) -> float:
    """
    Get BTC price for a given date using yfinance.
    
    Args:
        date: The date to get price for
        price_lookup: Optional dict mapping dates to prices (for testing/caching)
    
    Returns:
        BTC price in USD, or 0.0 if not available
    """
    # Use provided lookup if available (for testing)
    if price_lookup:
        date_key = date.date()
        return price_lookup.get(date_key, 0.0)
    
    # Check cache first
    date_key = date.date()
    if date_key in _btc_price_cache:
        return _btc_price_cache[date_key]
    
    # Fetch from yfinance
    global _btc_ticker
    if _btc_ticker is None:
        _btc_ticker = yf.Ticker("BTC-USD")
    
    try:
        # Get historical data for the date (use 'Close' price)
        hist = _btc_ticker.history(start=date_key, end=date_key + pd.Timedelta(days=1))
        
        if not hist.empty:
            price = float(hist['Close'].iloc[0])
            _btc_price_cache[date_key] = price
            return price
        else:
            # If no data for exact date, try getting data around that date
            hist = _btc_ticker.history(start=date_key - pd.Timedelta(days=7), end=date_key + pd.Timedelta(days=1))
            if not hist.empty:
                # Use the closest available date
                price = float(hist['Close'].iloc[-1])
                _btc_price_cache[date_key] = price
                return price
            else:
                print(f"Warning: Could not fetch BTC price for {date_key}")
                return 0.0
    except Exception as e:
        print(f"Error fetching BTC price for {date_key}: {e}")
        return 0.0


def fetch_btc_prices_batch(dates: List[datetime]) -> Dict[datetime.date, float]:
    """
    Fetch BTC prices for multiple dates in a single batch API call.
    
    Args:
        dates: List of dates to fetch prices for
    
    Returns:
        Dictionary mapping dates to prices
    """
    if not dates:
        return {}
    
    # Get date range
    date_objects = [d.date() if isinstance(d, datetime) else d for d in dates]
    min_date = min(date_objects)
    max_date = max(date_objects)
    
    # Fetch all data in one call
    global _btc_ticker
    if _btc_ticker is None:
        _btc_ticker = yf.Ticker("BTC-USD")
    
    try:
        # Fetch all data for the date range (add buffer for weekends/holidays)
        hist = _btc_ticker.history(start=min_date - pd.Timedelta(days=7), end=max_date + pd.Timedelta(days=1))
        
        prices = {}
        for date_obj in date_objects:
            # Try exact date match first
            if date_obj in hist.index.date:
                date_idx = [d.date() for d in hist.index].index(date_obj)
                prices[date_obj] = float(hist['Close'].iloc[date_idx])
            else:
                # Find closest date (forward fill)
                mask = hist.index.date <= date_obj
                if mask.any():
                    closest_idx = hist.index[mask][-1]
                    prices[date_obj] = float(hist.loc[closest_idx, 'Close'])
                else:
                    # No data available before this date, use first available
                    if len(hist) > 0:
                        prices[date_obj] = float(hist['Close'].iloc[0])
                    else:
                        prices[date_obj] = 0.0
        
        # Update global cache
        global _btc_price_cache
        _btc_price_cache.update(prices)
        
        return prices
    except Exception as e:
        print(f"Error fetching batch BTC prices: {e}")
        return {}


def load_config(config_path: str = "config.yaml") -> Dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config YAML file
    
    Returns:
        Dictionary with configuration values
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

