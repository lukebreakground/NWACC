#!/usr/bin/env python3
"""
Quick patch to disable 404 logging and clear the existing 404 file.
Run this while the main scraper is paused.
"""

import os

# Clear the existing 404 file
if os.path.exists('404_eids.csv'):
    with open('404_eids.csv', 'w') as f:
        f.write('Municipality,State,URL\n')  # Keep header only
    print("✅ Cleared 404_eids.csv file")

# Create a modified version of the scraper with 404 logging disabled
print("Now modify scrape_municipal_staff.py to comment out the 404 logging line:")
print("Find this line:")
print("    log_404_eid(municipality, state, url)")
print("And change it to:")
print("    # log_404_eid(municipality, state, url)  # Disabled to prevent huge file") 