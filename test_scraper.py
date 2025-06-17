#!/usr/bin/env python3
"""
Test script for CivicPlus scraper - tests 10 random municipalities
"""

import asyncio
import random
import pandas as pd
from civicplus_scraper import CivicPlusScraper

async def test_random_municipalities():
    """Test the scraper on 10 random municipalities."""
    
    # Load the input data
    df = pd.read_csv('civicplus_results_cleaned.csv')
    
    # Randomly select 10 municipalities
    sample_munis = df.sample(n=10).to_dict('records')
    
    print("Testing scraper on 10 random municipalities:")
    print("=" * 50)
    
    for i, muni in enumerate(sample_munis, 1):
        print(f"{i}. {muni['municipality']}, {muni['state']} - {muni['website']}")
    
    print("\nStarting scraper test...")
    print("=" * 50)
    
    total_results = 0
    
    async with CivicPlusScraper() as scraper:
        for i, muni in enumerate(sample_munis, 1):
            municipality = muni['municipality']
            state = muni['state']
            website = muni['website']
            
            print(f"\n[{i}/10] Testing {municipality}, {state}...")
            
            try:
                results = await scraper.scrape_municipality(municipality, state, website)
                total_results += len(results)
                
                print(f"  ✓ Found {len(results)} records")
                
                # Show a sample of results
                if results:
                    for j, result in enumerate(results[:3]):  # Show first 3 results
                        print(f"    {j+1}. {result['Type']}: {result.get('Person', result.get('Department', 'N/A'))}")
                    if len(results) > 3:
                        print(f"    ... and {len(results) - 3} more")
                else:
                    print("    No results found")
                    
            except Exception as e:
                print(f"  ✗ Error: {e}")
                scraper.total_errors += 1
    
    print(f"\n" + "=" * 50)
    print(f"Test completed!")
    print(f"Total results found: {total_results}")
    print(f"Total errors: {scraper.total_errors}")
    print(f"Success rate: {((10 - scraper.total_errors) / 10) * 100:.1f}%")

if __name__ == "__main__":
    asyncio.run(test_random_municipalities())