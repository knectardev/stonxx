"""
Check which symbols might be problematic for trading - analyze tradability details
"""
from alpaca.trading.client import TradingClient
from database import get_symbols_with_data, get_latest_bar
import yaml
import time

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']

def analyze_symbols():
    """Analyze symbols to find problematic ones"""
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    
    symbols = get_symbols_with_data('1Min')
    
    print("=" * 80)
    print("SYMBOL TRADABILITY ANALYSIS")
    print("=" * 80)
    print(f"\nAnalyzing {len(symbols)} symbols...\n")
    
    issues = {
        'not_found': [],
        'not_tradable': [],
        'not_active': [],
        'not_us_equity': [],
        'preferred_shares': [],
        'warrants': [],
        'units': []
    }
    
    valid_symbols = []
    
    # Categorize by symbol pattern
    preferred_patterns = [s for s in symbols if '.' in s and not s.endswith('.U') and not s.endswith('.WS')]
    warrants = [s for s in symbols if '.WS' in s]
    units = [s for s in symbols if '.U' in s]
    
    issues['preferred_shares'] = preferred_patterns
    issues['warrants'] = warrants
    issues['units'] = units
    
    print(f"Symbol patterns found:")
    print(f"  Preferred shares (.PR*): {len(preferred_patterns)}")
    print(f"  Warrants (.WS): {len(warrants)}")
    print(f"  Units (.U): {len(units)}")
    print(f"  Regular stocks: {len(symbols) - len(preferred_patterns) - len(warrants) - len(units)}")
    
    print(f"\n{'='*80}")
    print("Checking tradability (sampling first 50)...")
    print(f"{'='*80}\n")
    
    # Check a sample
    sample_size = min(50, len(symbols))
    sample_symbols = symbols[:sample_size]
    
    for i, symbol in enumerate(sample_symbols, 1):
        try:
            asset = trading_client.get_asset(symbol)
            
            if asset.asset_class != "us_equity":
                issues['not_us_equity'].append((symbol, asset.asset_class))
                continue
            
            if asset.status != "active":
                issues['not_active'].append((symbol, asset.status))
                continue
            
            if not asset.tradable:
                issues['not_tradable'].append(symbol)
                continue
            
            valid_symbols.append(symbol)
            
        except Exception as e:
            issues['not_found'].append((symbol, str(e)[:50]))
    
    print(f"\n{'='*80}")
    print("ANALYSIS RESULTS (from sample of 50)")
    print(f"{'='*80}\n")
    
    print(f"Valid tradable symbols: {len(valid_symbols)}/{sample_size}")
    print(f"\nIssues found (in sample):")
    
    if issues['not_found']:
        print(f"  Not found/delisted: {len(issues['not_found'])}")
        for sym, reason in issues['not_found'][:5]:
            print(f"    - {sym}: {reason}")
    
    if issues['not_tradable']:
        print(f"  Not tradable: {len(issues['not_tradable'])}")
        for sym in issues['not_tradable'][:5]:
            print(f"    - {sym}")
    
    if issues['not_active']:
        print(f"  Not active: {len(issues['not_active'])}")
        for sym, status in issues['not_active'][:5]:
            print(f"    - {sym}: {status}")
    
    if issues['not_us_equity']:
        print(f"  Not US equity: {len(issues['not_us_equity'])}")
        for sym, asset_class in issues['not_us_equity'][:5]:
            print(f"    - {sym}: {asset_class}")
    
    print(f"\n{'='*80}")
    print("RECOMMENDATIONS")
    print(f"{'='*80}\n")
    
    print(f"Potential symbols to filter out:")
    print(f"  1. Preferred shares: {len(issues['preferred_shares'])} symbols")
    print(f"     Examples: {', '.join(issues['preferred_shares'][:10])}")
    print(f"\n  2. Warrants: {len(issues['warrants'])} symbols")
    print(f"     Examples: {', '.join(issues['warrants'][:10]) if issues['warrants'] else 'None'}")
    print(f"\n  3. Units: {len(issues['units'])} symbols")
    print(f"     Examples: {', '.join(issues['units'][:10]) if issues['units'] else 'None'}")
    
    print(f"\nNote: Preferred shares, warrants, and units are often:")
    print(f"  - Less liquid")
    print(f"  - Harder to trade")
    print(f"  - May have different trading characteristics")
    
    total_special = len(issues['preferred_shares']) + len(issues['warrants']) + len(issues['units'])
    regular_stocks = len(symbols) - total_special
    
    print(f"\nBreakdown:")
    print(f"  Regular stocks: {regular_stocks}")
    print(f"  Preferred/Warrants/Units: {total_special}")
    print(f"  Total: {len(symbols)}")
    
    print(f"\n{'='*80}\n")

if __name__ == '__main__':
    analyze_symbols()

