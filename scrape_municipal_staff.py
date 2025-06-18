#!/usr/bin/env python3
"""
Municipal Staff Scraper for CivicPlus Websites

This script reads a CSV of municipalities and scrapes their staff directory pages
to extract contact information for municipal employees.

Requirements:
    pip install aiohttp beautifulsoup4 tqdm

Usage:
    python3 scrape_municipal_staff.py
"""

import asyncio
import csv
import signal
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


# Configuration
INPUT_FILE = "civicplus_results_ready.csv"
OUTPUT_FILE = "municipal_staff_results.csv"
SUCCESS_LOG_FILE = "successful_eids.csv"  # Log of successful EIDs
NOT_FOUND_LOG_FILE = "404_eids.csv"      # Log of 404 EIDs
BATCH_SIZE = 500  # Write to CSV every N records (increased from 50)
AUTO_SAVE_INTERVAL = 300  # Force write buffer every 5 minutes (300 seconds)
MAX_CONCURRENT_REQUESTS = 100  # Limit concurrent requests (increased from 20)
EID_BATCH_SIZE = 15  # Process EIDs in parallel batches within each municipality
REQUEST_TIMEOUT = 30  # Seconds
MAX_RETRIES = 3
RETRY_DELAY = 1  # Seconds between retries
INTER_BATCH_DELAY = 0.01  # Delay between EID batches for same domain (reduced from 0.1)

# Global variables for graceful shutdown
should_stop = False
results_buffer = []
output_writer = None
output_file_handle = None
success_writer = None
success_file_handle = None
not_found_writer = None
not_found_file_handle = None
last_auto_save = None


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scraper.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    global should_stop
    logging.info(f"Received signal {signum}. Initiating graceful shutdown...")
    should_stop = True


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def check_url_exists(session: aiohttp.ClientSession, url: str) -> Optional[int]:
    """
    Check if URL exists using HEAD request (faster).
    
    Returns:
        Status code or None if failed
    """
    for attempt in range(MAX_RETRIES):
        try:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                return response.status
                
        except asyncio.TimeoutError:
            logging.warning(f"Timeout checking {url} (attempt {attempt + 1}/{MAX_RETRIES})")
        except aiohttp.ClientError as e:
            logging.warning(f"Client error checking {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        except Exception as e:
            logging.error(f"Unexpected error checking {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    
    return None


async def fetch_content(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    """
    Fetch URL content (only for URLs we know exist).
    
    Returns:
        HTML content or None if failed
    """
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                if response.status == 200:
                    return await response.text()
                return None
                
        except asyncio.TimeoutError:
            logging.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{MAX_RETRIES})")
        except aiohttp.ClientError as e:
            logging.warning(f"Client error fetching {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        except Exception as e:
            logging.error(f"Unexpected error fetching {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    
    return None


def extract_email_from_javascript(html_content: str) -> str:
    """
    Extract email from JavaScript variables like:
    var wsd = "username"; var xsd = "domain.com"; var ysd = "Display Name";
    document.write("<a class='BioLink' href=\"mailto:" + wsd + '@' + xsd + '\">'+ ysd + '</a>');
    """
    import re
    
    try:
        # Look for JavaScript variable declarations
        # Pattern to find var wsd = "something"; var xsd = "something";
        wsd_match = re.search(r'var\s+wsd\s*=\s*["\']([^"\']+)["\']', html_content, re.IGNORECASE)
        xsd_match = re.search(r'var\s+xsd\s*=\s*["\']([^"\']+)["\']', html_content, re.IGNORECASE)
        
        if wsd_match and xsd_match:
            username = wsd_match.group(1)
            domain = xsd_match.group(1)
            email = f"{username}@{domain}"
            return email
            
        # Alternative pattern: look for direct email construction in JavaScript
        email_construct = re.search(r'["\']([^"\']+)["\']\s*\+\s*["\']@["\']\s*\+\s*["\']([^"\']+)["\']', html_content)
        if email_construct:
            username = email_construct.group(1)
            domain = email_construct.group(2)
            return f"{username}@{domain}"
            
    except Exception as e:
        logging.warning(f"Error parsing JavaScript for email: {e}")
    
    return ""


def extract_staff_info(html_content: str, url: str) -> Optional[Dict[str, str]]:
    """
    Extract staff information from HTML content.
    
    Returns:
        Dictionary with extracted staff info or None if parsing failed
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract person name from .BioName element
        bio_name_elem = soup.find(class_='BioName')
        person = bio_name_elem.get_text(strip=True) if bio_name_elem else ""
        
        # Extract info from first .BioText div
        bio_text_elem = soup.find('div', class_='BioText')
        department = ""
        title = ""
        phone = ""
        email = ""
        
        if bio_text_elem:
            # Get text lines from BioText
            bio_text = bio_text_elem.get_text('\n', strip=True)
            lines = [line.strip() for line in bio_text.split('\n') if line.strip()]
            
            # Skip first line if it matches the person name (avoid duplication)
            start_idx = 0
            if lines and person and lines[0].lower() == person.lower():
                start_idx = 1
            
            if len(lines) > start_idx:
                department = lines[start_idx]  # First non-duplicate line is department
            
            # Look for Title and Phone in the text
            for i, line in enumerate(lines):
                if line.lower().startswith('title:'):
                    title = line[6:].strip()  # Remove "Title:" prefix
                elif line.lower().startswith('phone:'):
                    # Phone might be on the same line or the next line
                    phone_text = line[6:].strip()  # Remove "Phone:" prefix
                    if not phone_text and i + 1 < len(lines):
                        # Phone is on the next line
                        phone = lines[i + 1].strip()
                    else:
                        phone = phone_text
            
            # Extract email from <a> tag inside BioText
            email_link = bio_text_elem.find('a', href=True)
            if email_link and email_link['href'].startswith('mailto:'):
                email = email_link['href'][7:]  # Remove "mailto:" prefix
        
        # Also search for email links outside BioText (like BioLink class)
        if not email:
            # Look for any mailto link in the entire page
            all_email_links = soup.find_all('a', href=True)
            for link in all_email_links:
                href = link.get('href', '')
                if href.startswith('mailto:'):
                    email = href[7:]  # Remove "mailto:" prefix
                    break
                    
            # If still no email, try JavaScript extraction
            if not email and 'mailto:' in html_content.lower():
                email = extract_email_from_javascript(html_content)
        
        return {
            'person': person,
            'department': department,
            'title': title,
            'phone': phone,
            'email': email,
            'url': url
        }
        
    except Exception as e:
        logging.error(f"Error parsing HTML for {url}: {e}")
        return None


def write_batch_to_csv(batch: List[Dict[str, str]]):
    """Write a batch of results to the CSV file."""
    global output_writer, last_auto_save
    
    if not batch or not output_writer:
        return
    
    for record in batch:
        output_writer.writerow([
            "Municipal Staff",  # Type
            record['organization'],  # Organization
            record['person'],  # Person
            record['department'],  # Department
            record['title'],  # Title
            record['phone'],  # Phone
            record['email'],  # Email
            "",  # Address (empty as specified)
            record['url']  # Note (URL)
        ])
    
    # Only flush periodically for better performance
    import time
    current_time = time.time()
    
    # Update last auto-save time
    last_auto_save = current_time
    
    # Flush every 10 batches or if it's been more than 60 seconds since last flush
    global last_flush_time
    if not hasattr(write_batch_to_csv, 'batch_count'):
        write_batch_to_csv.batch_count = 0
        write_batch_to_csv.last_flush_time = current_time
    
    write_batch_to_csv.batch_count += 1
    
    if (write_batch_to_csv.batch_count % 10 == 0 or 
        current_time - write_batch_to_csv.last_flush_time > 60):
        output_file_handle.flush()
        write_batch_to_csv.last_flush_time = current_time


def log_successful_eid(municipality: str, state: str, url: str, person: str, department: str):
    """Log a successful EID to the success tracking file."""
    global success_writer
    
    if success_writer:
        success_writer.writerow([
            municipality,
            state, 
            url,
            person,
            department
        ])
        success_file_handle.flush()


def log_404_eid(municipality: str, state: str, url: str):
    """Log a 404 EID to the not found tracking file."""
    global not_found_writer
    
    if not_found_writer:
        not_found_writer.writerow([
            municipality,
            state,
            url
        ])
        not_found_file_handle.flush()


def check_auto_save():
    """Check if we need to auto-save the buffer based on time."""
    global results_buffer, last_auto_save
    
    if not results_buffer:
        return
    
    import time
    current_time = time.time()
    
    # Initialize last_auto_save if not set
    if last_auto_save is None:
        last_auto_save = current_time
        return
    
    # Check if it's time for auto-save
    if current_time - last_auto_save >= AUTO_SAVE_INTERVAL:
        if results_buffer:
            logging.info(f"⏰ Auto-saving {len(results_buffer)} buffered records after {AUTO_SAVE_INTERVAL}s")
            write_batch_to_csv(results_buffer)
            results_buffer.clear()
            logging.info("✅ Auto-save completed")


async def process_eid_batch(session: aiohttp.ClientSession, eid_base: str, eid_range: range, 
                          municipality: str, state: str, pbar: tqdm) -> List[Dict[str, str]]:
    """
    Process a batch of EIDs in parallel for a single municipality.
    
    Returns:
        List of staff records found in this batch
    """
    global should_stop, results_buffer
    
    batch_records = []
    organization = f"{municipality}, {state}"
    
    # Create URLs for this batch
    urls_and_eids = [(f"{eid_base}{eid}", eid) for eid in eid_range]
    
    # Phase 1: Fast check which URLs exist (HEAD requests only)
    check_tasks = [check_url_exists(session, url) for url, _ in urls_and_eids]
    status_results = await asyncio.gather(*check_tasks, return_exceptions=True)
    
    # Filter to only valid URLs (200 status)
    valid_urls = []
    for (url, eid), status_result in zip(urls_and_eids, status_results):
        if should_stop:
            break
            
        pbar.update(1)
        
        if isinstance(status_result, Exception) or status_result is None:
            continue
            
        if status_result == 404:
            # log_404_eid(municipality, state, url)
            continue
            
        if status_result == 200:
            valid_urls.append((url, eid))
        else:
            logging.warning(f"Non-200 response ({status_result}) for {url}")
    
    # Phase 2: Only fetch content for URLs we know exist
    if valid_urls:
        content_tasks = [fetch_content(session, url) for url, _ in valid_urls]
        content_results = await asyncio.gather(*content_tasks, return_exceptions=True)
        
        for (url, eid), content in zip(valid_urls, content_results):
            if should_stop:
                break
                
            if isinstance(content, Exception) or content is None:
                continue
            
            # Extract staff info
            staff_info = extract_staff_info(content, url)
            if staff_info and staff_info['person']:
                # Log successful EID
                log_successful_eid(municipality, state, url, staff_info['person'], staff_info['department'])
                
                record = {
                    'organization': organization,
                    'person': staff_info['person'],
                    'department': staff_info['department'],
                    'title': staff_info['title'],
                    'phone': staff_info['phone'],
                    'email': staff_info['email'],
                    'url': staff_info['url']
                }
                batch_records.append(record)
                results_buffer.append(record)
                
                # Write batch if buffer is full
                if len(results_buffer) >= BATCH_SIZE:
                    write_batch_to_csv(results_buffer)
                    total_written = len(results_buffer)
                    results_buffer.clear()
                    logging.info(f"✅ Wrote batch of {total_written} records to CSV")
            
            # Check for auto-save (time-based backup)
            check_auto_save()
    
    return batch_records


async def process_municipality(session: aiohttp.ClientSession, municipality: str, state: str, 
                             website: str, pbar: tqdm) -> List[Dict[str, str]]:
    """
    Process all EID pages for a single municipality using parallel batching.
    
    Returns:
        List of staff records found
    """
    global should_stop
    
    records = []
    
    # Ensure website has www. prefix to avoid redirects
    if not website.startswith(('http://', 'https://')):
        website = 'https://' + website
    
    from urllib.parse import urlparse
    parsed = urlparse(website)
    if not parsed.netloc.startswith('www.'):
        # Add www. to avoid redirect issues
        website = f"{parsed.scheme}://www.{parsed.netloc}{parsed.path}"
    
    # Construct base URL for EID pages
    eid_base = urljoin(website.rstrip('/'), '/directory.aspx?EID=')
    
    # Process EIDs 1-999 in parallel batches
    eid_batches = [range(i, min(i + EID_BATCH_SIZE, 1000)) for i in range(1, 1000, EID_BATCH_SIZE)]
    
    for batch_range in eid_batches:
        if should_stop:
            break
            
        # Process this batch of EIDs in parallel
        batch_records = await process_eid_batch(session, eid_base, batch_range, municipality, state, pbar)
        records.extend(batch_records)
        
        # Small delay between batches to be respectful to servers
        await asyncio.sleep(INTER_BATCH_DELAY)
    
    return records


async def main():
    """Main function to orchestrate the scraping process."""
    global should_stop, results_buffer, output_writer, output_file_handle
    global success_writer, success_file_handle, not_found_writer, not_found_file_handle
    
    setup_logging()
    setup_signal_handlers()
    
    logging.info("Starting municipal staff scraper with optimizations...")
    logging.info(f"⚡ Performance settings: {MAX_CONCURRENT_REQUESTS} concurrent municipalities, "
                f"{EID_BATCH_SIZE} EID batch size, {BATCH_SIZE} CSV batch size")
    
    # Check if input file exists
    if not Path(INPUT_FILE).exists():
        logging.error(f"Input file {INPUT_FILE} not found!")
        return 1
    
    # Read municipalities from CSV
    municipalities = []
    try:
        with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            municipalities = list(reader)
        logging.info(f"Loaded {len(municipalities)} municipalities from {INPUT_FILE}")
    except Exception as e:
        logging.error(f"Error reading input file: {e}")
        return 1
    
    if not municipalities:
        logging.error("No municipalities found in input file")
        return 1
    
    # Prepare output CSV files
    try:
        # Main results file
        output_file_handle = open(OUTPUT_FILE, 'w', newline='', encoding='utf-8')
        output_writer = csv.writer(output_file_handle)
        output_writer.writerow([
            'Type', 'Organization', 'Person', 'Department', 'Title', 
            'Phone', 'Email', 'Address', 'Note'
        ])
        output_file_handle.flush()
        logging.info(f"📝 Created main output file: {OUTPUT_FILE}")
        
        # Success tracking file
        success_file_handle = open(SUCCESS_LOG_FILE, 'w', newline='', encoding='utf-8')
        success_writer = csv.writer(success_file_handle)
        success_writer.writerow(['Municipality', 'State', 'URL', 'Person', 'Department'])
        success_file_handle.flush()
        logging.info(f"✅ Created success log file: {SUCCESS_LOG_FILE}")
        
        # 404 tracking file
        not_found_file_handle = open(NOT_FOUND_LOG_FILE, 'w', newline='', encoding='utf-8')
        not_found_writer = csv.writer(not_found_file_handle)
        not_found_writer.writerow(['Municipality', 'State', 'URL'])
        not_found_file_handle.flush()
        logging.info(f"🔍 Created 404 log file: {NOT_FOUND_LOG_FILE}")
        
    except Exception as e:
        logging.error(f"Error creating output files: {e}")
        return 1
    
    # Calculate total EIDs to process
    total_eids = len(municipalities) * 999
    
    try:
        # Create HTTP session with optimized connection limits and disabled SSL verification
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS,
            limit_per_host=30,  # Allow more connections per host
            ttl_dns_cache=300,  # DNS cache for 5 minutes
            use_dns_cache=True,
            ssl=False,  # Disable SSL verification for problematic government sites
        )
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create progress bar
            with tqdm(total=total_eids, desc="Processing EIDs", unit="EID") as pbar:
                
                # Process municipalities with controlled concurrency
                # Use a lower semaphore limit since we're doing parallel EID processing within each municipality
                municipality_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS // 2)
                
                async def process_with_semaphore(municipality_data):
                    async with municipality_semaphore:
                        return await process_municipality(
                            session, 
                            municipality_data['municipality'],
                            municipality_data['state'],
                            municipality_data['website'],
                            pbar
                        )
                
                # Process all municipalities
                tasks = [process_with_semaphore(muni) for muni in municipalities]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Log any exceptions
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logging.error(f"Error processing {municipalities[i]['municipality']}: {result}")
    
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error during processing: {e}")
    finally:
        # Write any remaining buffered records
        if results_buffer:
            final_count = len(results_buffer)
            write_batch_to_csv(results_buffer)
            results_buffer.clear()
            logging.info(f"💾 Wrote final batch of {final_count} records on shutdown")
        else:
            logging.info("📝 No remaining records in buffer to write")
        
        # Close all output files
        files_closed = []
        if output_file_handle:
            output_file_handle.close()
            files_closed.append(OUTPUT_FILE)
        if success_file_handle:
            success_file_handle.close()
            files_closed.append(SUCCESS_LOG_FILE)
        if not_found_file_handle:
            not_found_file_handle.close()
            files_closed.append(NOT_FOUND_LOG_FILE)
        
        if files_closed:
            logging.info(f"💾 Closed and saved files: {', '.join(files_closed)}")
        
        logging.info("🔒 All data has been safely written to disk")
    
    logging.info("Scraping completed")
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
        sys.exit(0) 