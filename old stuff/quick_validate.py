#!/usr/bin/env python3
"""
Quick validation script to test individual city entries or small batches.
"""

import pandas as pd
import requests
from urllib.parse import urlparse
import re

def quick_check_city(city, state, url):
    """
    Quick check for a single city entry.
    Returns a simple pass/fail with reasoning.
    """
    print(f"\n{'='*60}")
    print(f"Checking: {city}, {state}")
    print(f"Website: {url}")
    print(f"{'='*60}")
    
    # Check URL pattern
    city_clean = city.lower().replace(' ', '').replace('-', '').replace('_', '')
    domain = urlparse(url).netloc.lower()
    domain_clean = re.sub(r'[^a-z]', '', domain)
    
    url_contains_city = city_clean in domain_clean
    print(f"URL contains city name: {'✓' if url_contains_city else '✗'}")
    
    # Try to fetch the page
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        content = response.text.lower()
        
        print(f"Website accessible: ✓")
        
        # Check for city name in title
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', content)
        title_has_city = False
        if title_match:
            title = title_match.group(1)
            title_has_city = city.lower() in title
            print(f"Title contains city name: {'✓' if title_has_city else '✗'}")
            print(f"Page title: {title[:100]}...")
        
        # Check for government indicators
        gov_indicators = ['government', 'official', 'city hall', 'municipal', 'township']
        has_gov_indicator = any(indicator in content for indicator in gov_indicators)
        print(f"Has government indicators: {'✓' if has_gov_indicator else '✗'}")
        
        # Check if city and state appear together
        city_and_state_together = city.lower() in content and state.lower() in content
        print(f"City and state both present: {'✓' if city_and_state_together else '✗'}")
        
        # Overall assessment
        score = sum([url_contains_city, title_has_city, has_gov_indicator, city_and_state_together])
        confidence = "HIGH" if score >= 3 else "MEDIUM" if score >= 2 else "LOW"
        
        print(f"\n{'='*30}")
        print(f"CONFIDENCE: {confidence} ({score}/4 checks passed)")
        print(f"{'='*30}")
        
        return score >= 2
        
    except requests.RequestException as e:
        print(f"Website not accessible: ✗ ({e})")
        print(f"CONFIDENCE: LOW (Cannot access website)")
        return False

def test_first_few_entries():
    """Test the first few entries from the CSV."""
    df = pd.read_csv('civicplus_results_cleaned.csv')
    
    print("Testing first 5 entries from CSV:")
    for i in range(min(5, len(df))):
        row = df.iloc[i]
        is_valid = quick_check_city(row['municipality'], row['state'], row['website'])
        print(f"Result: {'PASS' if is_valid else 'FAIL'}")
        
        if i < 4:  # Don't sleep after the last one
            print("\nWaiting 2 seconds before next check...")
            import time
            time.sleep(2)

def manual_test():
    """Allow manual testing of specific entries."""
    while True:
        print("\n" + "="*60)
        print("MANUAL TESTING MODE")
        print("="*60)
        
        city = input("Enter city name (or 'quit' to exit): ").strip()
        if city.lower() == 'quit':
            break
            
        state = input("Enter state: ").strip()
        url = input("Enter website URL: ").strip()
        
        quick_check_city(city, state, url)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'manual':
        manual_test()
    else:
        test_first_few_entries()
        
        print("\n" + "="*60)
        print("To test specific entries manually, run:")
        print("python3 quick_validate.py manual")
        print("="*60) 