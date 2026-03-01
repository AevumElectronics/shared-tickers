import numpy as np  # Kept as in original, even if not used
from datetime import datetime, timezone
from formulas.moving_average import calculate_moving_average
from formulas.slope import calculate_slope
from formulas.fibonacci_levels import calculate_fibonacci_levels
from formulas.exponential_moving_average import calculate_exponential_moving_average 
from formulas.rsi import calculate_rsi
from formulas.macd import calculate_macd
from formulas.bollinger_bands import calculate_bollinger_bands

def perform_calculations(data):
    """Perform all stock calculations."""
    if not data or 'values' not in data:
        return None

    try:
        # Get the latest price and last 10 days' prices
        #current_price = float(data['values'][-1]['close'])
        #recent_prices = [float(entry['close']) for entry in data['values'][-10:]]
        current_price = float(data['values'][0]['close'])
        recent_prices = [float(entry['close']) for entry in data['values'][10:]]

        # Calculate 200-week moving average
        ma_200_week = calculate_moving_average(data, 200 * 5)

        # Determine isInteresting
        is_interesting = any(price < ma_200_week for price in ([current_price] + recent_prices)) if ma_200_week else False
        
        return {
            'current_price': current_price,
            '50_week_moving_average': calculate_moving_average(data, 50),
            '50_week_moving_average_slope': calculate_slope(data, 50),
            '50_week_ema': calculate_exponential_moving_average(data, 50),  # Nuova EMA 50
            '200_week_moving_average': ma_200_week,
            '200_week_moving_average_slope': calculate_slope(data, 200),
            '200_week_ema': calculate_exponential_moving_average(data, 200),  # Nuova EMA 200
            'fibonacci_levels': calculate_fibonacci_levels(data),
            'rsi': calculate_rsi(data, 14),
            'macd': calculate_macd(data),
            'bollinger_bands': calculate_bollinger_bands(data, 20, 2), 
            #'pivot_points': calculate_pivot_points(data),
            'isInteresting': is_interesting,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        print(f"Error in perform_calculations: {e}")
        return None
