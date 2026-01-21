#!/usr/bin/env python3
"""
This script attempts to extract Anthem PPO machine-readable file URLs for New York state
from the CMS Price Transparency index file.

Usage:
    python anthem_ny_ppo_scraper.py [--output OUTPUT_FILE]
"""

import gzip
import json
import argparse
import urllib.request
from typing import Generator, Any
import ijson  # For streaming JSON parsing of large files


S3_URL = "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2026-01-01_anthem_index.json.gz"


def stream_index_file(url: str) -> Generator[dict, None, None]:
    """
    Stream and parse the gzipped JSON index file.
    Uses ijson for memory-efficient parsing of large files.
    """
    print(f"Fetching index file from: {url}")
    
    with urllib.request.urlopen(url) as response:
        with gzip.GzipFile(fileobj=response) as gz_file:
            # Use ijson to stream parse the JSON
            # The structure is: { "reporting_structure": [ { ... }, { ... } ] }
            parser = ijson.items(gz_file, 'reporting_structure.item')
            
            for item in parser:
                yield item


def is_ny_ppo_network(in_network_files: list[dict]) -> tuple[bool, list[dict]]:
    """
    Checks to see if any of the in_network_files represent a New York PPO network.
    
    Returns:
        (is_match, matching_files) tuple
    
    Exampme NY indicators in file descriptions:
    - "New York", "NY" in description
    - "Empire" (Empire BCBS is Anthem's NY brand)
    - "Excellus" (Excellus BCBS covers upstate NY)
    - "Highmark...NY" (Highmark Blue Shield of Northeastern NY)
    
    Example PPO indicators:
    - "PPO" in description
    """
    matching_files = []
    
    for file_info in in_network_files:
        description = file_info.get("description", "").upper()
        location = file_info.get("location", "")
        
        # Check for PPO
        is_ppo = "PPO" in description
        
        # Check for New York
        ny_indicators = [
            "NEW YORK",
            " NY ",
            " NY:",
            "_NY_",
            "(NY)",
            "EMPIRE",
        ]
        is_ny = any(indicator in description for indicator in ny_indicators)
        
        # Edge case: Highmark Northeastern NY
        is_ny = is_ny or ("HIGHMARK" in description and "NY" in description)
        
        # Edge case: Excellus is NY-based, also check for PPO
        is_excellus_ppo = "EXCELLUS" in description and is_ppo
        
        if (is_ppo and is_ny) or is_excellus_ppo:
            matching_files.append({
                "url": location,
                "description": file_info.get("description", "")
            })
    
    return len(matching_files) > 0, matching_files


def extract_in_network_urls(item: dict) -> list[dict]:
    """
    Extract ALL in-network file URLs from a reporting_structure item.
    """
    urls = []
    
    in_network_files = item.get("in_network_files", [])
    for file_info in in_network_files:
        url = file_info.get("location", "")
        description = file_info.get("description", "")
        urls.append({
            "url": url,
            "description": description
        })
    
    return urls


def main():
    parser = argparse.ArgumentParser(
        description="Extract Anthem PPO machine-readable file URLs for New York state"
    )
    parser.add_argument(
        "--output", "-o",
        default="ny_ppo_urls.json",
        help="Output file path (default: ny_ppo_urls.json)"
    )
    args = parser.parse_args()
    
    results = []
    processed = 0
    matched = 0
    
    print("Starting to process Anthem index file...")
    print("Looking for PPO plans in New York state...\n")
    
    for item in stream_index_file(S3_URL):
        processed += 1
        
        in_network_files = item.get("in_network_files", [])
        reporting_plans = item.get("reporting_plans", [])
        
        is_match, matching_files = is_ny_ppo_network(in_network_files)
        
        if is_match:
            matched += 1
            
            result = {
                "reporting_plans": reporting_plans,
                "ny_ppo_files": matching_files
            }
            results.append(result)
            
            # Print match info
            for f in matching_files[:2]:
                print(f"  Found: {f['description'][:80]}")
    
    print(f"Total items processed: {processed}")
    print(f"NY PPO matches found: {matched}")
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump({
            "total_processed": processed,
            "total_matches": matched,
            "results": results
        }, f, indent=2)
    
    print(f"Results saved to: {args.output}")
    
    # Print summary of unique URLs
    all_urls = set()
    for result in results:
        for file_info in result["ny_ppo_files"]:
            all_urls.add(file_info["url"])
    
    print(f"\nUnique machine-readable file URLs: {len(all_urls)}")


if __name__ == "__main__":
    main()
