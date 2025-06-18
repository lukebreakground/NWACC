#!/usr/bin/env python3
"""
Fast Municipal Website Validator

Uses async requests and concurrent processing to quickly validate
that websites in the CSV correspond to the correct cities.

Requirements:
    pip install aiohttp beautifulsoup4 tqdm pandas

Usage:
    python3 validate_city_websites.py
"""

import asyncio
import pandas as pd
import aiohttp
from urllib.parse import urlparse
import re
import time
from typing import Tuple, Optional, List, Dict
import logging
from tqdm.asyncio import tqdm
from bs4 import BeautifulSoup

# Configuration
MAX_CONCURRENT_REQUESTS = 50  # High concurrency for speed
REQUEST_TIMEOUT = 10  # Seconds
MAX_RETRIES = 2
RETRY_DELAY = 0.5  # Seconds between retries

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FastCityWebsiteValidator:
    def __init__(self, csv_file: str):
        """Initialize the fast validator."""
        self.csv_file = csv_file
        
    def normalize_city_name(self, city_name: str) -> str:
        """Normalize city name for comparison."""
        normalized = city_name.lower()
        normalized = re.sub(r'\s+(city|town|village|borough|township|county)$', '', normalized)
        normalized = re.sub(r'^(city|town|village|borough|township)\s+of\s+', '', normalized)
        normalized = re.sub(r'\s+', '', normalized)  # Remove all spaces
        return normalized
        
    def check_url_pattern(self, city: str, state: str, url: str) -> bool:
        """Check if the URL contains indicators of the correct city."""
        city_norm = self.normalize_city_name(city)
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        
        # Check domain for city name
        domain_clean = re.sub(r'[^a-z]', '', domain)
        if city_norm in domain_clean:
            return True
            
        # Check for common city website patterns
        city_patterns = [
            city_norm,
            city.lower().replace(' ', ''),
            city.lower().replace(' ', '-'),
            city.lower().replace(' ', '_')
        ]
        
        for pattern in city_patterns:
            if pattern in domain or pattern in path:
                return True
                
        return False

    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str) -> Optional[Tuple[int, str]]:
        """Fetch a URL with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                    if response.status == 404:
                        return (404, "")
                    
                    # Get content for non-404 responses
                    content = await response.text()
                    return (response.status, content)
                    
            except asyncio.TimeoutError:
                logger.debug(f"Timeout for {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            except aiohttp.ClientError as e:
                logger.debug(f"Client error for {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
            except Exception as e:
                logger.debug(f"Unexpected error for {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
        
        return None

    def check_page_content(self, city: str, state: str, content: str) -> bool:
        """Check if page content contains city information."""
        if not content:
            return False
            
        content_lower = content.lower()
        city_lower = city.lower()
        state_lower = state.lower()
        
        # Look for city name in title tag
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', content_lower)
        if title_match and city_lower in title_match.group(1):
            return True
            
        # Look for common government website indicators with city name
        gov_patterns = [
            rf'\b{re.escape(city_lower)}\s+(city|town|village|borough|municipality)',
            rf'(city|town|village|borough|municipality)\s+of\s+{re.escape(city_lower)}',
            rf'{re.escape(city_lower)}\s+government',
            rf'welcome\s+to\s+{re.escape(city_lower)}',
        ]
        
        for pattern in gov_patterns:
            if re.search(pattern, content_lower):
                return True
                
        # Check if both city and state appear together
        if city_lower in content_lower and state_lower in content_lower:
            # Look for them appearing close together
            city_positions = [m.start() for m in re.finditer(re.escape(city_lower), content_lower)]
            state_positions = [m.start() for m in re.finditer(re.escape(state_lower), content_lower)]
            
            for city_pos in city_positions:
                for state_pos in state_positions:
                    if abs(city_pos - state_pos) <= 100:  # Within 100 characters
                        return True
                        
        return False

    async def validate_single_row(self, session: aiohttp.ClientSession, row: pd.Series, 
                                semaphore: asyncio.Semaphore, pbar: tqdm) -> Dict[str, any]:
        """Validate a single row asynchronously."""
        async with semaphore:
            city = row['municipality']
            state = row['state']
            url = row['website']
            row_number = row.name + 1  # pandas index + 1
            
            # Check URL pattern first
            url_match = self.check_url_pattern(city, state, url)
            
            # Fetch and check page content
            result = await self.fetch_with_retry(session, url)
            
            if result is None:
                validation_result = {
                    'row_number': row_number,
                    'municipality': city,
                    'state': state,
                    'website': url,
                    'is_valid': False,
                    'validation_reason': "Could not fetch website content"
                }
            else:
                status_code, content = result
                if status_code != 200:
                    validation_result = {
                        'row_number': row_number,
                        'municipality': city,
                        'state': state,
                        'website': url,
                        'is_valid': False,
                        'validation_reason': f"HTTP {status_code} error"
                    }
                else:
                    content_match = self.check_page_content(city, state, content)
                    
                    # Determine result
                    if url_match and content_match:
                        is_valid = True
                        reason = "URL and content both match"
                    elif url_match:
                        is_valid = True
                        reason = "URL matches (content check inconclusive)"
                    elif content_match:
                        is_valid = True
                        reason = "Content matches (URL doesn't contain city name)"
                    else:
                        is_valid = False
                        reason = "Neither URL nor content match city"
                    
                    validation_result = {
                        'row_number': row_number,
                        'municipality': city,
                        'state': state,
                        'website': url,
                        'is_valid': is_valid,
                        'validation_reason': reason
                    }
            
            pbar.update(1)
            return validation_result

    async def validate_all_async(self) -> pd.DataFrame:
        """Validate all rows using async processing."""
        logger.info(f"Loading CSV file: {self.csv_file}")
        df = pd.read_csv(self.csv_file)
        total_rows = len(df)
        
        logger.info(f"Starting fast validation of {total_rows} entries...")
        
        # Create HTTP session with connection limits
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        ) as session:
            
            # Create progress bar
            with tqdm(total=total_rows, desc="Validating websites", unit="site") as pbar:
                
                # Create semaphore for controlling concurrency
                semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
                
                # Create tasks for all rows
                tasks = []
                for idx, row in df.iterrows():
                    task = self.validate_single_row(session, row, semaphore, pbar)
                    tasks.append(task)
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Filter out exceptions and log them
                valid_results = []
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Error validating row {i+1}: {result}")
                        # Create a failure record for exceptions
                        row = df.iloc[i]
                        valid_results.append({
                            'row_number': i + 1,
                            'municipality': row['municipality'],
                            'state': row['state'],
                            'website': row['website'],
                            'is_valid': False,
                            'validation_reason': f"Processing error: {str(result)}"
                        })
                    else:
                        valid_results.append(result)
        
        return pd.DataFrame(valid_results)

    def save_results(self, results_df: pd.DataFrame, output_file: str = 'fast_validation_results.csv'):
        """Save validation results to a CSV file."""
        results_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Print summary
        valid_count = results_df['is_valid'].sum()
        total_count = len(results_df)
        invalid_count = total_count - valid_count
        
        print(f"\n{'='*60}")
        print(f"🚀 FAST VALIDATION SUMMARY")
        print(f"{'='*60}")
        print(f"Total entries validated: {total_count}")
        print(f"✅ Valid entries: {valid_count} ({valid_count/total_count*100:.1f}%)")
        print(f"❌ Invalid entries: {invalid_count} ({invalid_count/total_count*100:.1f}%)")
        
        if invalid_count > 0:
            print(f"\n❌ Invalid entries:")
            invalid_entries = results_df[~results_df['is_valid']]
            for _, row in invalid_entries.head(10).iterrows():  # Show first 10 invalid
                print(f"  Row {row['row_number']}: {row['municipality']}, {row['state']} - {row['validation_reason']}")
            
            if invalid_count > 10:
                print(f"  ... and {invalid_count - 10} more invalid entries")
        
        print(f"\n📁 Full results saved to: {output_file}")

async def main():
    """Main function to run the fast validation."""
    csv_file = 'civicplus_results_cleaned.csv'
    
    # Create validator
    validator = FastCityWebsiteValidator(csv_file)
    
    # Record start time
    start_time = time.time()
    
    # Validate all rows asynchronously
    results = await validator.validate_all_async()
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Save results
    validator.save_results(results, 'fast_validation_results.csv')
    
    print(f"\n⚡ Validation completed in {elapsed_time:.1f} seconds!")
    print(f"🏎️  Average speed: {len(results)/elapsed_time:.1f} sites/second")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Validation interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise 