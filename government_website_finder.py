#!/usr/bin/env python3
"""
Optimized script to find official government websites for places using Serper.dev search.
Uses parallel processing for maximum throughput while respecting API limits.
"""

import csv
import json
import os
import asyncio
import logging
import time
from typing import List, Dict, Tuple, Optional, Set
from tqdm import tqdm
import random
from dotenv import load_dotenv
import aiohttp
import re

class SmartRateLimiter:
    """Smart rate limiter that uses parallel processing with backoff"""
    
    def __init__(self):
        self.concurrent_limit = 15  # Start with 15 concurrent requests
        self.min_concurrent = 3
        self.max_concurrent = 50
        self.request_delay = 0.1    # Small delay between request batches
        self.backoff_factor = 0.7   # Reduce concurrency by 30% on rate limit
        self.speedup_factor = 1.2   # Increase concurrency by 20% on success
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.total_requests = 0
        self.rate_limited_requests = 0
        
    def can_increase_concurrency(self) -> bool:
        """Check if we can increase concurrency"""
        return (self.consecutive_successes >= 5 and 
                self.concurrent_limit < self.max_concurrent)
    
    def should_decrease_concurrency(self) -> bool:
        """Check if we should decrease concurrency"""
        return self.consecutive_failures >= 2
    
    def record_success_batch(self, batch_size: int):
        """Record successful batch"""
        self.consecutive_successes += batch_size
        self.consecutive_failures = 0
        self.total_requests += batch_size
        
        if self.can_increase_concurrency():
            old_limit = self.concurrent_limit
            self.concurrent_limit = min(self.max_concurrent, 
                                      int(self.concurrent_limit * self.speedup_factor))
            self.consecutive_successes = 0
            logging.info(f"🚀 Increased concurrency: {old_limit} → {self.concurrent_limit}")
    
    def record_rate_limit_batch(self, failed_count: int):
        """Record rate limited batch"""
        self.consecutive_failures += failed_count
        self.consecutive_successes = 0
        self.total_requests += failed_count
        self.rate_limited_requests += failed_count
        
        if self.should_decrease_concurrency():
            old_limit = self.concurrent_limit
            self.concurrent_limit = max(self.min_concurrent, 
                                      int(self.concurrent_limit * self.backoff_factor))
            self.consecutive_failures = 0
            logging.info(f"⚠️ Reduced concurrency due to rate limits: {old_limit} → {self.concurrent_limit}")
    
    def get_stats(self) -> str:
        """Get rate limiter statistics"""
        if self.total_requests == 0:
            return f"Concurrency: {self.concurrent_limit}, No requests yet"
        
        success_rate = (self.total_requests - self.rate_limited_requests) / self.total_requests * 100
        return f"Concurrency: {self.concurrent_limit}, Success: {success_rate:.1f}%, Rate limits: {self.rate_limited_requests}"

# Global rate limiter
rate_limiter = SmartRateLimiter()

def load_places_from_csv(filename: str) -> List[str]:
    """
    Load place names from CSV file.
    
    Args:
        filename: Path to the CSV file
        
    Returns:
        List of place names (e.g., "New York, New York")
    """
    places = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            if row and row[0].strip():  # Skip empty rows
                places.append(row[0].strip().strip('"'))
    return places

def load_existing_results(filename: str) -> Set[str]:
    """
    Load already processed places from existing results CSV.
    
    Args:
        filename: Path to the results CSV file
        
    Returns:
        Set of place names that have already been processed
    """
    existing_places = set()
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row
            for row in reader:
                if row and row[0].strip():  # Skip empty rows
                    existing_places.add(row[0].strip())
        logging.info(f"Found {len(existing_places)} already processed places in {filename}")
    except FileNotFoundError:
        logging.info(f"No existing results file found at {filename}, starting fresh")
    except Exception as e:
        logging.warning(f"Error reading existing results from {filename}: {str(e)}")
    
    return existing_places

def filter_unprocessed_places(all_places: List[str], existing_places: Set[str]) -> List[str]:
    """
    Filter out places that have already been processed.
    
    Args:
        all_places: List of all places to process
        existing_places: Set of places already processed
        
    Returns:
        List of places that still need to be processed
    """
    unprocessed = [place for place in all_places if place not in existing_places]
    skipped_count = len(all_places) - len(unprocessed)
    
    if skipped_count > 0:
        logging.info(f"Skipping {skipped_count} already processed places")
        logging.info(f"Processing {len(unprocessed)} remaining places")
    
    return unprocessed

async def search_with_serper(session: aiohttp.ClientSession, place: str, serper_api_key: str) -> Optional[List[Dict]]:
    """
    Search for a place using Serper.dev API.
    
    Args:
        session: aiohttp session
        place: Place name to search for
        serper_api_key: Serper API key
        
    Returns:
        List of search results or None if failed
    """
    url = "https://google.serper.dev/search"
    query = f"{place} official government website"
    
    payload = {
        "q": query,
        "location": "United States"
    }
    
    headers = {
        'X-API-KEY': serper_api_key,
        'Content-Type': 'application/json'
    }
    
    try:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('organic', [])
            elif response.status == 429:
                logging.debug(f"Rate limited for {place}")
                return "rate_limited"
            else:
                logging.warning(f"Serper API returned status {response.status} for {place}")
                return None
    except Exception as e:
        logging.error(f"Error searching for {place}: {str(e)}")
        return None

def is_government_website(url: str, title: str) -> bool:
    """
    Determine if a URL/title looks like an official government website.
    Excludes state-level government websites.
    
    Args:
        url: Website URL
        title: Website title
        
    Returns:
        True if it looks like a government website (excluding state websites)
    """
    url_lower = url.lower()
    title_lower = title.lower()
    
    # Define state government domains to exclude
    state_domains = [
        'alabama.gov', 'al.gov', 'alaska.gov', 'ak.gov', 'arizona.gov', 'az.gov',
        'arkansas.gov', 'ar.gov', 'california.gov', 'ca.gov', 'colorado.gov', 'co.gov',
        'connecticut.gov', 'ct.gov', 'delaware.gov', 'de.gov', 'florida.gov', 'fl.gov',
        'georgia.gov', 'ga.gov', 'hawaii.gov', 'hi.gov', 'idaho.gov', 'id.gov',
        'illinois.gov', 'il.gov', 'indiana.gov', 'in.gov', 'iowa.gov', 'ia.gov',
        'kansas.gov', 'ks.gov', 'kentucky.gov', 'ky.gov', 'louisiana.gov', 'la.gov',
        'maine.gov', 'me.gov', 'maryland.gov', 'md.gov', 'massachusetts.gov', 'ma.gov',
        'michigan.gov', 'mi.gov', 'minnesota.gov', 'mn.gov', 'mississippi.gov', 'ms.gov',
        'missouri.gov', 'mo.gov', 'montana.gov', 'mt.gov', 'nebraska.gov', 'ne.gov',
        'nevada.gov', 'nv.gov', 'nh.gov', 'nj.gov', 'newmexico.gov', 'nm.gov',
        'ny.gov', 'nc.gov', 'nd.gov', 'ohio.gov', 'oh.gov', 'oklahoma.gov', 'ok.gov',
        'oregon.gov', 'or.gov', 'pa.gov', 'ri.gov', 'sc.gov', 'sd.gov', 'tn.gov',
        'texas.gov', 'tx.gov', 'utah.gov', 'ut.gov', 'vermont.gov', 'vt.gov',
        'virginia.gov', 'va.gov', 'wa.gov', 'wv.gov', 'wisconsin.gov', 'wi.gov',
        'wyoming.gov', 'wy.gov','usa.gov'
    ]
    
    # Skip state government websites
    if any(state_domain in url_lower for state_domain in state_domains):
        return False
    
    # Prefer .gov domains (but not state domains)
    if '.gov' in url_lower:
        return True
    
    # Skip obvious non-government sites
    skip_domains = ['google.com', 'wikipedia.org', 'yelp.com', 'facebook.com', 
                   'maps.google', 'twitter.com', 'instagram.com', 'linkedin.com',
                   'yellowpages.com', 'whitepages.com']
    
    if any(skip in url_lower for skip in skip_domains):
        return False
    
    # Look for government-related terms in title
    gov_terms = ['city of', 'town of', 'village of', 'county of', 'borough of',
                'municipal', 'government', 'official', 'city hall', 'town hall',
                'county government', 'city government']
    
    if any(term in title_lower for term in gov_terms):
        return True
    
    # Look for government-related domains
    gov_domains = ['.org', '.us']  # Some government sites use .org or .us
    if any(domain in url_lower for domain in gov_domains):
        # Additional check for government-related terms in URL
        url_gov_terms = ['city', 'town', 'village', 'county', 'municipal', 'gov']
        if any(term in url_lower for term in url_gov_terms):
            return True
    
    return False

async def find_government_website_parallel(session: aiohttp.ClientSession, place: str, serper_api_key: str) -> Tuple[str, str]:
    """
    Find government website for a place using parallel-optimized approach.
    
    Args:
        session: aiohttp session
        place: Place name to search for
        serper_api_key: Serper API key
        
    Returns:
        Tuple of (place, website_url or "none")
    """
    search_results = await search_with_serper(session, place, serper_api_key)
    
    if search_results == "rate_limited":
        return place, "rate_limited"
    
    if not search_results:
        return place, "none"
    
    # Analyze first 10 results for government websites
    for result in search_results[:10]:
        url = result.get('link', '')
        title = result.get('title', '')
        
        if url and is_government_website(url, title):
            logging.debug(f"Found government website for {place}: {url}")
            return place, url
    
    logging.debug(f"No government website found for {place}")
    return place, "none"

async def process_batch_parallel(session: aiohttp.ClientSession, batch: List[str], serper_api_key: str) -> List[Tuple[str, str]]:
    """
    Process a batch of places in parallel with smart concurrency control.
    
    Args:
        session: aiohttp session
        batch: List of place names to process
        serper_api_key: Serper API key
        
    Returns:
        List of results as (place, website) tuples
    """
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(rate_limiter.concurrent_limit)
    
    async def process_with_semaphore(place: str) -> Tuple[str, str]:
        async with semaphore:
            # Small delay to spread requests
            await asyncio.sleep(random.uniform(0.05, 0.15))
            return await find_government_website_parallel(session, place, serper_api_key)
    
    # Process all places in the batch concurrently
    tasks = [process_with_semaphore(place) for place in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle any exceptions and collect results
    final_results = []
    rate_limited_count = 0
    success_count = 0
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Error processing {batch[i]}: {str(result)}")
            final_results.append((batch[i], "none"))
        else:
            place, website = result
            final_results.append((place, website))
            
            if website == "rate_limited":
                rate_limited_count += 1
                # Convert rate limited to "none" for final output
                final_results[-1] = (place, "none")
            elif website != "none":
                success_count += 1
    
    # Update rate limiter based on results
    if rate_limited_count > 0:
        rate_limiter.record_rate_limit_batch(rate_limited_count)
    if success_count > 0:
        rate_limiter.record_success_batch(success_count)
    
    return final_results

async def process_all_places_parallel(places: List[str], serper_api_key: str, batch_size: int = 50, output_file: str = "websites.csv", existing_results: Set[str] = None) -> List[Tuple[str, str]]:
    """
    Process all places using optimized parallel processing.
    
    Args:
        places: List of place names
        serper_api_key: Serper API key
        batch_size: Number of places per batch
        output_file: CSV file to write results to
        existing_results: Set of already processed places
        
    Returns:
        List of tuples (place, website)
    """
    all_results = []
    
    # Create aiohttp session with optimized settings
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Create batches
        batches = [places[i:i + batch_size] for i in range(0, len(places), batch_size)]
        
        # Process batches with progress bar
        with tqdm(total=len(places), desc="Finding government websites", unit="place") as pbar:
            start_time = time.time()
            
            for batch_idx, batch in enumerate(batches):
                batch_start = time.time()
                logging.info(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} places) - Concurrency: {rate_limiter.concurrent_limit}")
                
                try:
                    # Process batch in parallel
                    batch_results = await process_batch_parallel(session, batch, serper_api_key)
                    all_results.extend(batch_results)
                    
                    # Write results to CSV after each batch
                    use_append = existing_results is not None and len(existing_results) > 0
                    if use_append:
                        write_results_to_csv(batch_results, output_file, write_header=False, append_mode=True)
                    else:
                        write_results_to_csv(all_results, output_file, write_header=True, append_mode=False)
                        existing_results = set()  # Switch to append mode for next batches
                    
                    # Calculate and log performance
                    batch_time = time.time() - batch_start
                    batch_rate = len(batch) / batch_time if batch_time > 0 else 0
                    
                    found_count = sum(1 for _, website in batch_results if website != "none")
                    success_rate = found_count / len(batch) * 100
                    
                    total_time = time.time() - start_time
                    overall_rate = len(all_results) / total_time if total_time > 0 else 0
                    
                    logging.info(f"Batch {batch_idx + 1}: {found_count}/{len(batch)} found ({success_rate:.1f}%) | "
                               f"Batch rate: {batch_rate:.1f} req/s | Overall rate: {overall_rate:.1f} req/s | "
                               f"{rate_limiter.get_stats()}")
                    
                    # Update progress bar
                    pbar.update(len(batch))
                    
                    # Brief pause between batches to avoid overwhelming the API
                    if batch_idx < len(batches) - 1:
                        await asyncio.sleep(rate_limiter.request_delay)
                        
                except Exception as e:
                    logging.error(f"Error processing batch {batch_idx + 1}: {str(e)}")
                    # Add failed results as "none"
                    failed_results = [(place, "none") for place in batch]
                    all_results.extend(failed_results)
                    
                    # Still write results even on error
                    use_append = existing_results is not None and len(existing_results) > 0
                    if use_append:
                        write_results_to_csv(failed_results, output_file, write_header=False, append_mode=True)
                    else:
                        write_results_to_csv(all_results, output_file, write_header=True, append_mode=False)
                    
                    pbar.update(len(batch))
    
    return all_results

def write_results_to_csv(results: List[Tuple[str, str]], filename: str, write_header: bool = True, append_mode: bool = False):
    """
    Write results to CSV file.
    
    Args:
        results: List of tuples (place, website)
        filename: Output CSV filename
        write_header: Whether to write the CSV header
        append_mode: Whether to append to existing file or overwrite
    """
    mode = 'a' if append_mode else 'w'
    
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if write_header and not append_mode:  # Only write header when not appending
            writer.writerow(['place', 'website'])  # Header
        writer.writerows(results)
    
    action = "appended to" if append_mode else "written to"
    logging.info(f"Results {action} {filename} ({len(results)} entries)")

async def main():
    """Main function to orchestrate the website finding process."""
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('government_website_finder.log'),
            logging.StreamHandler()
        ]
    )
    
    logging.info("🚀 Starting optimized parallel government website finder")
    
    # Check for Serper API key
    serper_api_key = os.getenv('SERPER_API_KEY')
    if not serper_api_key:
        logging.error("SERPER_API_KEY environment variable not set")
        print("Error: SERPER_API_KEY environment variable not set")
        print("Please set your Serper API key: export SERPER_API_KEY='your-key-here'")
        return
    
    # Load places from CSV
    try:
        places = load_places_from_csv('places.csv')
        logging.info(f"Loaded {len(places)} places from places.csv")
    except Exception as e:
        logging.error(f"Error loading places.csv: {str(e)}")
        print(f"Error loading places.csv: {str(e)}")
        return
    
    # Load existing results to avoid reprocessing
    existing_results = load_existing_results('websites.csv')
    
    # Filter out already processed places
    unprocessed_places = filter_unprocessed_places(places, existing_results)
    
    if not unprocessed_places:
        logging.info("All places have already been processed!")
        print("All places have already been processed!")
        return
    
    # Limit to first 1000 places for testing
    places_to_process = unprocessed_places[:10000]
    logging.info(f"Processing {len(places_to_process)} places")
    
    # Process places using parallel approach
    start_time = time.time()
    results = await process_all_places_parallel(
        places_to_process, 
        serper_api_key, 
        batch_size=50,  # Larger batches for better parallelization
        output_file='websites.csv',
        existing_results=existing_results
    )
    
    # Calculate final statistics
    total_time = time.time() - start_time
    found_count = sum(1 for _, website in results if website != "none")
    success_rate = found_count / len(results) * 100 if results else 0
    overall_rate = len(results) / total_time if total_time > 0 else 0
    
    logging.info(f"✅ Processing completed!")
    logging.info(f"Total processed: {len(results)} places")
    logging.info(f"Websites found: {found_count} ({success_rate:.1f}%)")
    logging.info(f"Overall rate: {overall_rate:.1f} requests/second")
    logging.info(f"Total time: {total_time:.1f} seconds")
    logging.info(f"Results written to websites.csv")
    
    print(f"\n✅ Completed processing {len(results)} places in {total_time:.1f} seconds")
    print(f"🎯 Found {found_count} government websites ({success_rate:.1f}% success rate)")
    print(f"⚡ Average rate: {overall_rate:.1f} requests/second")
    print(f"📄 Results saved to websites.csv")

if __name__ == "__main__":
    asyncio.run(main()) 