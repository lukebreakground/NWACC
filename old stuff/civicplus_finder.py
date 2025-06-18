#!/usr/bin/env python3
"""
Fast script to check websites from websites.csv for CivicPlus HTML pattern.
Uses async requests with parallel processing for maximum speed on M3 MacBook Pro.
"""

import csv
import asyncio
import aiohttp
import logging
from typing import List, Tuple, Optional
from tqdm import tqdm
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('civicplus_finder.log'),
        logging.StreamHandler()
    ]
)

class CivicPlusFinder:
    """Fast CivicPlus pattern finder with smart concurrency control"""
    
    def __init__(self):
        # Target HTML pattern to search for
        self.target_pattern = 'Government Websites by <a href="https://connect.civicplus.com/referral"'
        self.alt_pattern = 'cpBylineTextTS'  # Alternative pattern to increase match chances
        
        # MAXIMUM SPEED settings for M3 MacBook Pro
        self.concurrent_limit = 150  # Massive concurrency for blazing speed
        self.timeout = 3  # Ultra-fast timeout - government sites should be fast
        self.max_retries = 1  # Single retry only to avoid delays
        
    async def check_website_for_civicplus(self, session: aiohttp.ClientSession, place: str, website: str) -> Tuple[str, str, bool, str]:
        """
        Check a single website for CivicPlus pattern.
        
        Args:
            session: aiohttp session
            place: Place name
            website: Website URL
            
        Returns:
            Tuple of (place, website, found_pattern, error_message)
        """
        # Ensure website has proper protocol
        if not website.startswith(('http://', 'https://')):
            website = f'https://{website}'
            
        for attempt in range(self.max_retries):
            try:
                async with session.get(
                    website, 
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                    },
                    ssl=False  # Skip SSL verification for maximum speed
                ) as response:
                    if response.status == 200:
                        # Read response content
                        content = await response.text()
                        
                        # Check for both patterns
                        has_main_pattern = self.target_pattern in content
                        has_alt_pattern = self.alt_pattern in content
                        
                        found = has_main_pattern or has_alt_pattern
                        
                        if found:
                            logging.info(f"✅ CivicPlus found on {website} ({place})")
                            return (place, website, True, "")
                        else:
                            return (place, website, False, "")
                    else:
                        error_msg = f"HTTP {response.status}"
                        logging.debug(f"❌ {website}: {error_msg}")
                        return (place, website, False, error_msg)
                        
            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    continue  # No sleep delay for maximum speed
                return (place, website, False, "Timeout")
            except Exception as e:
                if attempt < self.max_retries - 1:
                    continue  # No sleep delay for maximum speed
                error_msg = f"Error: {str(e)[:50]}"
                return (place, website, False, error_msg)
                
        return (place, website, False, "Max retries exceeded")

    async def process_batch(self, session: aiohttp.ClientSession, batch: List[Tuple[str, str]]) -> List[Tuple[str, str, bool, str]]:
        """Process a batch of websites concurrently"""
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        
        async def process_with_semaphore(place_website):
            place, website = place_website
            async with semaphore:
                return await self.check_website_for_civicplus(session, place, website)
        
        tasks = [process_with_semaphore(pw) for pw in batch]
        return await asyncio.gather(*tasks, return_exceptions=False)

    def load_websites_from_csv(self, filename: str) -> List[Tuple[str, str]]:
        """Load place-website pairs from CSV file"""
        websites = []
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header row
                for row in reader:
                    if len(row) >= 2 and row[0].strip() and row[1].strip():
                        place = row[0].strip().strip('"')
                        website = row[1].strip().strip('"')
                        websites.append((place, website))
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            raise
        
        logging.info(f"Loaded {len(websites)} websites to check")
        return websites

    def save_results_to_csv(self, results: List[Tuple[str, str, bool, str]], filename: str):
        """Save results to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['place', 'website', 'has_civicplus', 'error'])
            for result in results:
                writer.writerow(result)
        logging.info(f"Results saved to {filename}")

    async def find_civicplus_websites(self, input_csv: str = "websites.csv", output_csv: str = "civicplus_results.csv"):
        """Main function to find CivicPlus websites"""
        start_time = time.time()
        
        # Load websites
        websites = self.load_websites_from_csv(input_csv)
        
        # Setup async session with MAXIMUM SPEED settings
        connector = aiohttp.TCPConnector(
            limit=500,  # Massive connection pool
            limit_per_host=50,  # High per-host limit
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True,
            ssl=False  # Skip SSL for speed
        )
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Process websites in smaller batches for maximum parallelism
            batch_size = 50
            all_results = []
            
            with tqdm(total=len(websites), desc="Checking websites") as pbar:
                for i in range(0, len(websites), batch_size):
                    batch = websites[i:i + batch_size]
                    batch_results = await self.process_batch(session, batch)
                    all_results.extend(batch_results)
                    pbar.update(len(batch))
                    
                    # Log progress stats (less frequent for speed)
                    if len(all_results) % 500 == 0:  # Log every 500 results instead of every batch
                        found_count = sum(1 for r in all_results if r[2])
                        logging.info(f"Progress: {len(all_results)}/{len(websites)}, CivicPlus found: {found_count}")
        
        # Save results
        self.save_results_to_csv(all_results, output_csv)
        
        # Final stats
        total_time = time.time() - start_time
        found_count = sum(1 for r in all_results if r[2])
        error_count = sum(1 for r in all_results if r[3] and not r[2])
        
        logging.info(f"\n🎯 FINAL RESULTS:")
        logging.info(f"Total websites checked: {len(all_results)}")
        logging.info(f"CivicPlus websites found: {found_count}")
        logging.info(f"Errors encountered: {error_count}")
        logging.info(f"Time taken: {total_time:.2f} seconds")
        logging.info(f"Rate: {len(all_results)/total_time:.2f} websites/second")
        
        # Show found websites
        if found_count > 0:
            logging.info("\n✅ CivicPlus websites found:")
            for place, website, found, error in all_results:
                if found:
                    logging.info(f"  - {place}: {website}")
        
        return all_results

async def main():
    """Main entry point"""
    finder = CivicPlusFinder()
    await finder.find_civicplus_websites()

if __name__ == "__main__":
    asyncio.run(main()) 