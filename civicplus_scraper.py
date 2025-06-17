#!/usr/bin/env python3
"""
CivicPlus Website Scraper

This script scrapes municipal department and staff information from CivicPlus websites.
It processes DID pages (departments) and EID pages (staff) asynchronously with proper
error handling, progress tracking, and graceful shutdown capabilities.

Usage:
    python civicplus_scraper.py

Requirements:
    - civicplus_results_cleaned.csv (input file)
    - aiohttp, beautifulsoup4, tqdm, pandas, lxml
"""

import asyncio
import csv
import logging
import signal
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup, Tag
from tqdm.asyncio import tqdm


# Configuration
INPUT_FILE = "civicplus_results_cleaned.csv"
OUTPUT_FILE = "civicplus_scraped_results.csv"
BATCH_SIZE = 50  # Number of records to write at once
MAX_CONCURRENT_REQUESTS = 10  # Limit concurrent requests to be respectful
REQUEST_TIMEOUT = 30  # Seconds
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_DELAY = 0.5  # Seconds between requests to be respectful

# Global variables for graceful shutdown
shutdown_requested = False
results_buffer = []


class CivicPlusScraper:
    """Async scraper for CivicPlus municipal websites."""
    
    def __init__(self):
        """Initialize the scraper with session and logging."""
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.total_processed = 0
        self.total_errors = 0
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('civicplus_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    async def __aenter__(self):
        """Async context manager entry."""
        # Setup HTTP session with timeout and headers
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; CivicPlusScraper/1.0; +research)'
        }
        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def fetch_with_retry(self, url: str) -> Optional[Tuple[int, str]]:
        """
        Fetch URL with retry logic and rate limiting.
        
        Returns:
            Tuple of (status_code, html_content) or None if failed
        """
        if not self.session:
            raise RuntimeError("HTTP session not initialized")
            
        async with self.semaphore:
            for attempt in range(REQUEST_RETRY_ATTEMPTS):
                try:
                    await asyncio.sleep(REQUEST_DELAY)  # Rate limiting
                    
                    async with self.session.get(url) as response:
                        if response.status == 404:
                            return None  # Expected for non-existent pages
                        
                        html = await response.text()
                        return response.status, html
                        
                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                except aiohttp.ClientError as e:
                    self.logger.warning(f"Client error for {url}: {e} (attempt {attempt + 1})")
                except Exception as e:
                    self.logger.error(f"Unexpected error for {url}: {e} (attempt {attempt + 1})")
                
                if attempt < REQUEST_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            self.total_errors += 1
            return None
    
    def parse_department_page(self, html: str, url: str) -> Optional[Dict[str, str]]:
        """
        Parse a DID (department) page and extract information.
        
        Returns:
            Dictionary with department info or None if parsing failed
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract department name from h1
            department_elem = soup.find('h1')
            department = department_elem.get_text(strip=True) if department_elem else ""
            
            # Extract phone from p:nth-child(10)
            phone = ""
            try:
                phone_elem = soup.select('p:nth-child(10)')
                if phone_elem:
                    phone = phone_elem[0].get_text(strip=True)
            except Exception:
                pass
            
            # Extract address from .viewMap + p
            address = ""
            try:
                address_elem = soup.select('.viewMap + p')
                if address_elem:
                    address = address_elem[0].get_text(strip=True)
            except Exception:
                pass
            
            return {
                'department': department,
                'phone': phone,
                'address': address,
                'url': url
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing department page {url}: {e}")
            return None
    
    def parse_staff_page(self, html: str, url: str) -> Optional[Dict[str, str]]:
        """
        Parse an EID (staff) page and extract information.
        
        Returns:
            Dictionary with staff info or None if parsing failed
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract person name from .BioName
            person_elem = soup.find(class_='BioName')
            person = person_elem.get_text(strip=True) if person_elem else ""
            
            # Extract info from first .BioText div
            department = ""
            title = ""
            phone = ""
            email = ""
            
            bio_text_elem = soup.find('div', class_='BioText')
            if bio_text_elem:
                # Get all text and split by lines
                bio_text = bio_text_elem.get_text('\n', strip=True)
                lines = [line.strip() for line in bio_text.split('\n') if line.strip()]
                
                if lines:
                    # First line is department
                    department = lines[0]
                
                # Look for title, phone, email in subsequent lines
                for line in bio_text.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('title:'):
                        title = line[6:].strip()
                    elif line.lower().startswith('phone:'):
                        phone = line[6:].strip()
                
                # Extract email from anchor tag
                if isinstance(bio_text_elem, Tag):
                    email_links = bio_text_elem.find_all('a')
                    for link in email_links:
                        href = link.get('href', '')
                        if href and href.startswith('mailto:'):
                            email = href.replace('mailto:', '')
                            break
            
            return {
                'person': person,
                'department': department,
                'title': title,
                'phone': phone,
                'email': email,
                'url': url
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing staff page {url}: {e}")
            return None
    
    async def scrape_municipality(self, municipality: str, state: str, website: str) -> List[Dict[str, str]]:
        """
        Scrape all departments and staff for a single municipality.
        
        Returns:
            List of result dictionaries
        """
        results = []
        organization = f"{municipality}, {state}"
        
        # Process DID pages (departments, 1-99)
        department_tasks = []
        for did in range(1, 100):
            if shutdown_requested:
                break
            url = urljoin(website.rstrip('/'), f"/Directory.aspx?DID={did}")
            department_tasks.append(self.scrape_department(url, organization))
        
        if department_tasks:
            dept_results = await asyncio.gather(*department_tasks, return_exceptions=True)
            for result in dept_results:
                if isinstance(result, dict) and result:
                    results.append(result)
        
        # Process EID pages (staff, 1-999)
        if not shutdown_requested:
            staff_tasks = []
            for eid in range(1, 1000):
                if shutdown_requested:
                    break
                url = urljoin(website.rstrip('/'), f"/directory.aspx?EID={eid}")
                staff_tasks.append(self.scrape_staff(url, organization))
            
            if staff_tasks:
                staff_results = await asyncio.gather(*staff_tasks, return_exceptions=True)
                for result in staff_results:
                    if isinstance(result, dict) and result:
                        results.append(result)
        
        return results
    
    async def scrape_department(self, url: str, organization: str) -> Optional[Dict[str, str]]:
        """Scrape a single department page."""
        response = await self.fetch_with_retry(url)
        if not response:
            return None
        
        status_code, html = response
        if status_code != 200:
            return None
        
        dept_info = self.parse_department_page(html, url)
        if not dept_info:
            return None
        
        return {
            'Type': 'Municipal Department',
            'Organization': organization,
            'Person': '',
            'Department': dept_info['department'],
            'Title': '',
            'Phone': dept_info['phone'],
            'Email': '',
            'Address': dept_info['address'],
            'Note': url
        }
    
    async def scrape_staff(self, url: str, organization: str) -> Optional[Dict[str, str]]:
        """Scrape a single staff page."""
        response = await self.fetch_with_retry(url)
        if not response:
            return None
        
        status_code, html = response
        if status_code != 200:
            return None
        
        staff_info = self.parse_staff_page(html, url)
        if not staff_info:
            return None
        
        return {
            'Type': 'Municipal Staff',
            'Organization': organization,
            'Person': staff_info['person'],
            'Department': staff_info['department'],
            'Title': staff_info['title'],
            'Phone': staff_info['phone'],
            'Email': staff_info['email'],
            'Address': '',
            'Note': url
        }


def write_results_batch(results: List[Dict[str, str]], output_file: str, write_header: bool = False):
    """Write a batch of results to CSV file."""
    try:
        mode = 'w' if write_header else 'a'
        with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
            if results:
                fieldnames = ['Type', 'Organization', 'Person', 'Department', 'Title', 'Phone', 'Email', 'Address', 'Note']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if write_header:
                    writer.writeheader()
                
                for result in results:
                    writer.writerow(result)
                
                logging.info(f"Wrote {len(results)} records to {output_file}")
    
    except Exception as e:
        logging.error(f"Error writing results to {output_file}: {e}")


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    global shutdown_requested
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True


async def main():
    """Main function to orchestrate the scraping process."""
    global results_buffer, shutdown_requested
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Validate input file exists
    if not Path(INPUT_FILE).exists():
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        sys.exit(1)
    
    # Load municipalities data
    try:
        df = pd.read_csv(INPUT_FILE)
        municipalities = df.to_dict('records')
        print(f"Loaded {len(municipalities)} municipalities from {INPUT_FILE}")
    except Exception as e:
        print(f"Error loading input file: {e}")
        sys.exit(1)
    
    # Initialize output file with header
    write_results_batch([], OUTPUT_FILE, write_header=True)
    
    async with CivicPlusScraper() as scraper:
        print(f"Starting to scrape {len(municipalities)} municipalities...")
        print("Press Ctrl+C to stop gracefully.")
        
        with tqdm(total=len(municipalities), desc="Municipalities") as pbar:
            for i, muni in enumerate(municipalities):
                if shutdown_requested:
                    print("\nShutdown requested. Stopping...")
                    break
                
                municipality = muni['municipality']
                state = muni['state']
                website = muni['website']
                
                pbar.set_description(f"Scraping {municipality}, {state}")
                
                try:
                    # Scrape this municipality
                    results = await scraper.scrape_municipality(municipality, state, website)
                    results_buffer.extend(results)
                    
                    # Write results in batches
                    if len(results_buffer) >= BATCH_SIZE:
                        write_results_batch(results_buffer, OUTPUT_FILE)
                        results_buffer.clear()
                    
                    scraper.total_processed += 1
                    
                except Exception as e:
                    scraper.logger.error(f"Error scraping {municipality}, {state}: {e}")
                    scraper.total_errors += 1
                
                pbar.update(1)
                pbar.set_postfix({
                    'Processed': scraper.total_processed,
                    'Errors': scraper.total_errors,
                    'Buffer': len(results_buffer)
                })
        
        # Write any remaining results
        if results_buffer:
            write_results_batch(results_buffer, OUTPUT_FILE)
            results_buffer.clear()
        
        print(f"\nScraping completed!")
        print(f"Processed: {scraper.total_processed} municipalities")
        print(f"Errors: {scraper.total_errors}")
        print(f"Results saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)