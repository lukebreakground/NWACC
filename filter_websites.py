#!/usr/bin/env python3
import csv
import os
from urllib.parse import urlparse

# State government domains to exclude
STATE_GOV_DOMAINS = {
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
}

def extract_base_domain(url):
    """
    Extract the base domain from a URL.
    Examples:
    - https://www.example.com/path -> example.com
    - http://subdomain.example.com -> example.com
    """
    try:
        if not url or url.lower() == 'none':
            return None
            
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
            
        # For domains like subdomain.example.com, try to get just example.com
        # This is a simple heuristic - for more complex cases, you'd want a proper
        # public suffix list library like tldextract
        parts = domain.split('.')
        if len(parts) >= 2:
            # Keep last two parts (domain.tld)
            domain = '.'.join(parts[-2:])
            
        return domain if domain else None
        
    except Exception as e:
        print(f"Error processing URL '{url}': {e}")
        return None

def process_websites_csv():
    """
    Process websites.csv to:
    1. Remove rows with 'none' in website column
    2. Extract base domains from URLs
    3. Remove domains with less than 6 characters
    4. Remove state government domains
    5. Update the CSV with base domains
    """
    input_file = 'websites.csv'
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return
    
    rows_kept = 0
    rows_removed_none = 0
    rows_removed_short = 0
    rows_removed_gov = 0
    
    # Read the CSV and process it
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        
        # Read header
        header = next(reader)
        
        # Find the website column index
        website_col_index = header.index('website')
        
        # Collect processed rows
        processed_rows = [header]  # Start with header
        
        for row in reader:
            if len(row) <= website_col_index:
                rows_removed_none += 1
                continue
                
            website_url = row[website_col_index]
            
            # Skip rows with 'none'
            if website_url.lower() == 'none':
                rows_removed_none += 1
                continue
            
            # Extract base domain
            base_domain = extract_base_domain(website_url)
            
            if not base_domain:
                rows_removed_none += 1
                continue
            
            # Check if domain has at least 6 characters
            if len(base_domain) < 6:
                rows_removed_short += 1
                continue
            
            # Check if domain is a state government domain
            if base_domain in STATE_GOV_DOMAINS:
                rows_removed_gov += 1
                continue
            
            # Update the row with the base domain
            row[website_col_index] = base_domain
            processed_rows.append(row)
            rows_kept += 1
    
    # Write the processed data back to the original file
    with open(input_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(processed_rows)
    
    print(f"Processing complete!")
    print(f"Rows kept: {rows_kept}")
    print(f"Rows removed (none/invalid): {rows_removed_none}")
    print(f"Rows removed (domain <6 chars): {rows_removed_short}")
    print(f"Rows removed (state gov domains): {rows_removed_gov}")
    print(f"Total rows removed: {rows_removed_none + rows_removed_short + rows_removed_gov}")
    print(f"Updated {input_file}")

if __name__ == "__main__":
    process_websites_csv()