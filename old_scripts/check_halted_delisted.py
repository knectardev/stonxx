"""
Check remaining symbols for halted, delisted, or inactive status.
Remove symbols that are no longer tradeable.
"""
from alpaca.trading.client import TradingClient
from database import get_connection, get_symbols_with_data
import yaml
import time

# Load config
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

ALPACA_API_KEY = config['alpaca']['api_key']
ALPACA_SECRET_KEY = config['alpaca']['api_secret']

def check_halted_delisted():
    """Check all symbols for halted/delisted status"""
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    conn = get_connection()
    
    print("=" * 80)
    print("CHECKING FOR HALTED/DELISTED SYMBOLS")
    print("=" * 80)
    
    symbols = get_symbols_with_data('1Min')
    print(f"\nFound {len(symbols)} symbols to check\n")
    
    issues = {
        'not_found': [],
        'inactive': [],
        'not_tradable': [],
        'halted': [],
        'suspended': []
    }
    
    valid_symbols = []
    
    print(f"Checking status for each symbol...\n")
    checked = 0
    start_time = time.time()
    
    for symbol in symbols:
        checked += 1
        
        try:
            asset = trading_client.get_asset(symbol)
            
            # Check various statuses
            status = asset.status.lower() if asset.status else 'unknown'
            tradable = asset.tradable if hasattr(asset, 'tradable') else True
            
            if status == 'inactive':
                issues['inactive'].append((symbol, 'inactive'))
            elif not tradable:
                issues['not_tradable'].append((symbol, 'not tradable'))
            elif status not in ['active']:
                issues['suspended'].append((symbol, f'status: {status}'))
            else:
                valid_symbols.append(symbol)
        
        except Exception as e:
            error_msg = str(e).lower()
            if 'not found' in error_msg or '404' in error_msg:
                issues['not_found'].append((symbol, 'not found/delisted'))
            else:
                issues['not_found'].append((symbol, str(e)[:50]))
        
        # Progress update every 50 symbols
        if checked % 50 == 0:
            percent = (checked / len(symbols)) * 100
            elapsed = time.time() - start_time
            rate = checked / elapsed if elapsed > 0 else 0
            remaining = (len(symbols) - checked) / rate if rate > 0 else 0
            print(f"  Checked {checked}/{len(symbols)} ({percent:.1f}%) | "
                  f"Valid: {len(valid_symbols)} | "
                  f"Issues: {sum(len(v) for v in issues.values())} | "
                  f"ETA: {remaining/60:.1f}min", end='\r')
        
        # Rate limiting
        time.sleep(0.1)
    
    print()  # New line after progress
    
    # Print results
    print(f"\n{'='*80}")
    print("CHECK RESULTS")
    print(f"{'='*80}\n")
    
    print(f"Valid active symbols: {len(valid_symbols)}/{len(symbols)}")
    print(f"\nIssues found:")
    
    total_issues = 0
    
    if issues['not_found']:
        print(f"\n  Not found/Delisted: {len(issues['not_found'])}")
        for sym, reason in issues['not_found'][:10]:
            print(f"    - {sym}: {reason}")
        if len(issues['not_found']) > 10:
            print(f"    ... and {len(issues['not_found']) - 10} more")
        total_issues += len(issues['not_found'])
    
    if issues['inactive']:
        print(f"\n  Inactive: {len(issues['inactive'])}")
        for sym, reason in issues['inactive'][:10]:
            print(f"    - {sym}: {reason}")
        if len(issues['inactive']) > 10:
            print(f"    ... and {len(issues['inactive']) - 10} more")
        total_issues += len(issues['inactive'])
    
    if issues['not_tradable']:
        print(f"\n  Not tradable: {len(issues['not_tradable'])}")
        for sym, reason in issues['not_tradable'][:10]:
            print(f"    - {sym}: {reason}")
        if len(issues['not_tradable']) > 10:
            print(f"    ... and {len(issues['not_tradable']) - 10} more")
        total_issues += len(issues['not_tradable'])
    
    if issues['suspended']:
        print(f"\n  Suspended/Other: {len(issues['suspended'])}")
        for sym, reason in issues['suspended'][:10]:
            print(f"    - {sym}: {reason}")
        if len(issues['suspended']) > 10:
            print(f"    ... and {len(issues['suspended']) - 10} more")
        total_issues += len(issues['suspended'])
    
    if total_issues == 0:
        print("\n  ✓ No issues found - all symbols are active and tradable!")
    
    # Combine all issues for removal
    all_issues = (issues['not_found'] + issues['inactive'] + 
                  issues['not_tradable'] + issues['suspended'])
    
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total symbols checked: {len(symbols)}")
    print(f"Valid symbols: {len(valid_symbols)}")
    print(f"Symbols with issues: {total_issues}")
    
    if all_issues:
        print(f"\nSymbols to remove: {len(all_issues)}")
        print(f"\nTo remove these symbols, run:")
        print(f"  python remove_halted_delisted.py")
    
    conn.close()
    print(f"\n{'='*80}\n")
    
    return all_issues

if __name__ == '__main__':
    try:
        from alpaca.trading.client import TradingClient
    except ImportError:
        print("❌ ERROR: alpaca-py not installed!")
        print("Install it with: pip install alpaca-py")
        exit(1)
    
    issues = check_halted_delisted()
    
    if issues:
        print(f"\nFound {len(issues)} symbols with issues that should be removed.")
    else:
        print("\n✓ All symbols are valid and active!")

