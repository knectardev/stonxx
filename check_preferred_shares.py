"""
Check if any preferred shares (.PR*) remain in the database
"""
from database import get_symbols_with_data
import re

symbols = get_symbols_with_data('1Min')

# Check for preferred shares matching .PR[A-Z] pattern
pr_pattern = [s for s in symbols if re.search(r'\.PR[A-Z]', s, re.IGNORECASE)]
any_dot_symbols = [s for s in symbols if '.' in s]

print("=" * 60)
print("CHECKING FOR PREFERRED SHARES")
print("=" * 60)
print(f"\nTotal symbols: {len(symbols)}")
print(f"\nSymbols matching .PR[A-Z] pattern: {len(pr_pattern)}")
if pr_pattern:
    print(f"Found: {pr_pattern}")
else:
    print("✓ No symbols matching .PR[A-Z] pattern remain")

print(f"\nSymbols with any dot (.): {len(any_dot_symbols)}")
if any_dot_symbols:
    print(f"Found: {any_dot_symbols}")
    print("\nThese might be other special classes (warrants, units, etc.)")
else:
    print("✓ No symbols with dots remain")

print("\n" + "=" * 60)

