#!/usr/bin/env python3
"""
Test script to verify scraping logic on a single website.

This script tests the municipal staff scraping logic on just one municipality
to verify everything works correctly before running the full batch.

Requirements:
    pip install aiohttp beautifulsoup4

Usage:
    python3 test_single_website.py
"""

import asyncio
import csv
import logging
from pathlib import Path
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup


# Configuration
INPUT_FILE = "civicplus_results_cleaned.csv"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


async def fetch_with_retry(session: aiohttp.ClientSession, url: str):
    """
    Fetch a URL with retry logic.
    
    Returns:
        Tuple of (status_code, html_content) or None if failed
    """
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                if response.status == 404:
                    return (404, "")
                
                # Get content for non-404 responses
                content = await response.text()
                return (response.status, content)
                
        except asyncio.TimeoutError:
            print(f"Timeout for {url} (attempt {attempt + 1}/{MAX_RETRIES})")
        except aiohttp.ClientError as e:
            print(f"Client error for {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        except Exception as e:
            print(f"Unexpected error for {url}: {e} (attempt {attempt + 1}/{MAX_RETRIES})")
        
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    
    print(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
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
        print(f"    Error parsing JavaScript for email: {e}")
    
    return ""


def extract_staff_info(html_content: str, url: str):
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
            
            print(f"    Raw BioText lines: {lines}")
            
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
            print(f"    🔍 Found {len(all_email_links)} total links on page")
            
            mailto_links = []
            for link in all_email_links:
                href = link.get('href', '')
                if href.startswith('mailto:'):
                    mailto_links.append(href)
                    email = href[7:]  # Remove "mailto:" prefix
                    print(f"    📧 Found mailto link: {href}")
                    break
                    
            if not mailto_links:
                print(f"    ❌ No mailto links found on page")
                # Let's see what links we did find
                sample_links = [link.get('href', '') for link in all_email_links[:5]]
                print(f"    📋 Sample links found: {sample_links}")
                
                # Let's also search for any text containing "mailto" or "email"
                html_lower = html_content.lower()
                if 'mailto:' in html_lower:
                    print(f"    🔍 HTML contains 'mailto:' - checking for parsing issues")
                    # Find the context around mailto
                    mailto_pos = html_lower.find('mailto:')
                    context = html_content[max(0, mailto_pos-50):mailto_pos+100]
                    print(f"    📄 Context around mailto: {context}")
                    
                    # Try to extract email from JavaScript
                    email = extract_email_from_javascript(html_content)
                    if email:
                        print(f"    ✅ Extracted email from JavaScript: {email}")
                elif 'email' in html_lower:
                    print(f"    📧 HTML contains 'email' but no 'mailto:' found")
        
        return {
            'person': person,
            'department': department,
            'title': title,
            'phone': phone,
            'email': email,
            'url': url
        }
        
    except Exception as e:
        print(f"Error parsing HTML for {url}: {e}")
        return None


async def test_municipality(municipality: str, state: str, website: str):
    """
    Test scraping for a single municipality.
    """
    print(f"\n=== Testing: {municipality}, {state} ===")
    print(f"Website: {website}")
    
    organization = f"{municipality}, {state}"
    
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
    print(f"Original website: {municipality}, {state}")
    print(f"Using website: {website}")
    print(f"EID base URL: {eid_base}")
    
    found_records = []
    
    async with aiohttp.ClientSession() as session:
        # Test all EIDs 1-999 to find all staff
        print(f"\nTesting all EIDs 1-999...")
        total_checked = 0
        total_404s = 0
        total_errors = 0
        
        for eid in range(1, 1000):
            url = f"{eid_base}{eid}"
            print(f"Checking EID {eid:3d}/999: {url}")
            
            # Fetch the page
            result = await fetch_with_retry(session, url)
            total_checked += 1
            
            if not result:
                print(f"  ❌ Failed to fetch")
                total_errors += 1
                continue
                
            status_code, html_content = result
            

            
            # Skip 404 pages
            if status_code == 404:
                print(f"  🔍 404 - Not found")
                total_404s += 1
                continue
            
            # Skip non-200 responses
            if status_code != 200:
                print(f"  ⚠️  Non-200 response: {status_code}")
                total_errors += 1
                continue
            
            # Extract staff info
            staff_info = extract_staff_info(html_content, url)
            if staff_info and staff_info['person']:
                print(f"  ✅ Found staff member: {staff_info['person']}")
                print(f"     Department: {staff_info['department']}")
                print(f"     Title: {staff_info['title']}")
                print(f"     Phone: {staff_info['phone']}")
                print(f"     Email: {staff_info['email']}")
                
                record = {
                    'Type': 'Municipal Staff',
                    'Organization': organization,
                    'Person': staff_info['person'],
                    'Department': staff_info['department'],
                    'Title': staff_info['title'],
                    'Phone': staff_info['phone'],
                    'Email': staff_info['email'],
                    'Address': '',
                    'Note': url  # Use original URL
                }
                found_records.append(record)
                
                # Show running total
                print(f"     🎯 Total found so far: {len(found_records)}")
            else:
                print(f"  📄 Page exists but no staff info found")
                # Show a preview of what we got to help debug
                preview = html_content[:300].replace('\n', ' ').replace('\r', '').strip()
                print(f"     🔍 Content preview: {preview}...")
                
                # Check for common patterns
                if '<title>' in html_content.lower():
                    import re
                    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html_content, re.IGNORECASE)
                    if title_match:
                        print(f"     📝 Page title: {title_match.group(1).strip()}")
            
            # Small delay to be respectful
            await asyncio.sleep(0.1)
        
        print(f"\n=== Detailed Statistics ===")
        print(f"Total EIDs checked: {total_checked}")
        print(f"404 responses: {total_404s}")
        print(f"Error responses: {total_errors}")
        print(f"Successful pages: {total_checked - total_404s - total_errors}")
        print(f"Staff members found: {len(found_records)}")
    
    print(f"\n=== Summary for {municipality}, {state} ===")
    print(f"Found {len(found_records)} staff members")
    
    if found_records:
        print("\nCSV Output Preview:")
        print("Type,Organization,Person,Department,Title,Phone,Email,Address,Note")
        for record in found_records:
            print(f'"{record["Type"]}","{record["Organization"]}","{record["Person"]}","{record["Department"]}","{record["Title"]}","{record["Phone"]}","{record["Email"]}","{record["Address"]}","{record["Note"]}"')
    
    return found_records


async def main():
    """Main function to test a single municipality."""
    setup_logging()
    
    print("=== Municipal Staff Scraper - Single Website Test ===")
    
    # Check if input file exists
    if not Path(INPUT_FILE).exists():
        print(f"Input file {INPUT_FILE} not found!")
        
        # Provide a hardcoded example for testing
        print("Using hardcoded test municipality...")
        await test_municipality("Provo", "Utah", "https://provo.gov")
        return
    
    # Read first municipality from CSV
    try:
        with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            first_municipality = next(reader)
        
        await test_municipality(
            first_municipality['municipality'],
            first_municipality['state'],
            first_municipality['website']
        )
        
    except Exception as e:
        print(f"Error reading input file: {e}")
        print("Using hardcoded test municipality...")
        await test_municipality("Provo", "Utah", "https://provo.gov")


if __name__ == "__main__":
    asyncio.run(main()) 