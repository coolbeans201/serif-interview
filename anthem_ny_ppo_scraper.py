#!/usr/bin/env python3
"""
This script attempts to extract Anthem PPO machine-readable file URLs for New York state
from the CMS Price Transparency index file.

Usage:
    python anthem_ny_ppo_scraper.py [--output OUTPUT_FILE]

Key design decisions:
1. Uses streaming JSON parsing (ijson) to handle large file without loading into memory
2. Deduplicates URLs by file identifier, not full URL (as the same file can be served from multiple CDNs)
3. Identifies NY PPO networks by both description field and URL domain (for Empire BCBS)
"""

import gzip
import argparse
import urllib.request
import re
from typing import Generator
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


def extract_file_id(url: str) -> str:
    """
    Extract the file identifier from a URL, ignoring CDN prefix and query params.

    The same file often is served from multiple CDN domains (anthembcbsco, anthembcbsct,
    empirebcbs, etc.). Therefore, we deduplicate by the actual file identifier.

    Example:
        https://anthembcca.mrf.bcbs.com/2026-01_302_42B0_in-network-rates_1_of_3.json.gz?...
        -> 2026-01_302_42B0_in-network-rates_1_of_3.json.gz
    """
    base = url.split("?")[0]
    return base.split("/")[-1]


def is_ny_ppo_network(in_network_files: list[dict]) -> tuple[bool, list[dict]]:
    """
    Checks to see if any of the in_network_files represent a New York PPO network.

    Returns:
        (is_match, matching_files) tuple

    Detection is based on the description field, which identifies the network.
    
    Note: The URL domain (e.g., empirebcbs.mrf.bcbs.com) indicates which CDN is serving
    the file. The same file may be served from multiple CDNs.
    
    Highmark (Western NY, Northeastern NY) is a separate BCBS licensee, not Anthem.
    We include them as they serve NY PPO networks, but they are distinct from Anthem/Empire.
    """
    matching_files = []

    for file_info in in_network_files:
        description = file_info.get("description", "")
        location = file_info.get("location", "")
        desc_upper = description.upper()

        # Check for PPO indicator
        is_ppo = "PPO" in desc_upper
        
        # Check for NY indicators in description
        ny_indicators = ["NEW YORK", " NY ", " NY:", "_NY_", "(NY)"]
        is_ny = any(indicator in desc_upper for indicator in ny_indicators)
        is_ny = is_ny or ("HIGHMARK" in desc_upper and "NY" in desc_upper)
        
        # Excellus BCBS covers upstate NY (Rochester, Syracuse, Utica)
        is_excellus_ppo = "EXCELLUS" in desc_upper and is_ppo

        if (is_ppo and is_ny) or is_excellus_ppo:
            matching_files.append({
                "url": location,
                "description": description,
                "file_id": extract_file_id(location)
            })

    return len(matching_files) > 0, matching_files


def main():
    parser = argparse.ArgumentParser(
        description="Extract Anthem PPO machine-readable file URLs for New York state"
    )
    parser.add_argument(
        "--output", "-o",
        default="ny_ppo_urls.txt",
        help="Output file path (default: ny_ppo_urls.txt)"
    )
    args = parser.parse_args()

    # Collect unique file IDs by network (deduplicate across CDNs)
    file_ids_by_network: dict[str, dict[str, str]] = {}  # network -> {file_id: sample_url}
    processed = 0

    print("Starting to process the Anthem index file...")
    print("Looking for PPO plans in New York state...\n")

    for item in stream_index_file(S3_URL):
        processed += 1

        in_network_files = item.get("in_network_files", [])
        is_match, matching_files = is_ny_ppo_network(in_network_files)

        if is_match:
            for f in matching_files:
                network = f["description"]
                file_id = f["file_id"]
                url = f["url"]

                if network not in file_ids_by_network:
                    file_ids_by_network[network] = {}

                # Store one URL per file_id (they're all equivalent, just different CDNs)
                if file_id not in file_ids_by_network[network]:
                    file_ids_by_network[network][file_id] = url

    # Count totals
    total_unique_files = sum(len(files) for files in file_ids_by_network.values())

    # Also count truly unique file IDs across all networks
    all_file_ids = set()
    for files in file_ids_by_network.values():
        all_file_ids.update(files.keys())

    print(f"Total items processed: {processed}")
    print(f"Networks found: {len(file_ids_by_network)}")
    print(f"Unique file IDs (deduplicated across CDNs): {len(all_file_ids)}")

    # Write URLs to output file
    with open(args.output, 'w') as f:
        for network in sorted(file_ids_by_network.keys()):
            files = file_ids_by_network[network]
            f.write(f"# {network} ({len(files)} files)\n")
            for file_id in sorted(files.keys()):
                url = files[file_id]
                f.write(f"{url}\n")
            f.write("\n")

    print(f"Results saved to: {args.output}")


if __name__ == "__main__":
    main()
