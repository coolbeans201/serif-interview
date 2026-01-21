# Anthem NY PPO Price Transparency Scraper

Extracts machine-readable file URLs for Anthem PPO networks in New York state from CMS Price Transparency index files.

## Background

Under the [Transparency in Coverage](https://www.cms.gov/healthplan-price-transparency) rule, health insurers must publish machine-readable files with negotiated rates. This script parses Anthem's table-of-contents index file to find URLs for NY PPO networks.

## Design Decisions

1. Uses streaming JSON parsing (ijson) to handle large file without loading into memory
2. Deduplicates URLs by file identifier, not full URL (same file served from multiple CDNs)
3. Identifies NY PPO networks by both description field AND URL domain (for Empire BCBS)

## Requirements

- Python 3.9+
- `ijson` (for streaming JSON parsing of large files)

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Full extraction (takes ~10 minutes, file is ~5GB)
python anthem_ny_ppo_scraper.py

# Custom output file
python anthem_ny_ppo_scraper.py -o my_results.json
```

## Solution

This took around 1 hour to write, with the first 30 minutes spent doing analysis on the data set itself to understand the basic structure and how to dive in deeper on the filters used. After that, I was able to quickly put together the solution.

## Output

The script produces a JSON file with:

```json
{
  "total_processed": 148776,
  "total_matches": 135772,
  "results": [
    {
      "reporting_plans": [
        {"plan_name": "...", "plan_id": "...", "plan_id_type": "EIN", "plan_market_type": "group"}
      ],
      "ny_ppo_files": [
        {"url": "https://...", "description": "Excellus BCBS : BluePPO"}
      ]
    }
  ]
}
```

## Networks Found

Here are some of the networks I found based on the indicators I used:

| Network | Coverage Area |
|---------|---------------|
| Excellus BCBS : BluePPO | Upstate NY (Rochester, Syracuse, Utica) |
| Highmark BCBS Western NY | Buffalo area |
| Highmark BS Northeastern NY | Albany area |

## Customization

To filter for different states or plan types, we can modify the `is_ny_ppo_network()` function in the script. The key identifiers being used right now, after some analysis:

- **NY indicators**: "NEW YORK", "NY", "EMPIRE", "EXCELLUS", "HIGHMARK...NY"
- **PPO indicators**: "PPO" in the file description

As more indicators come through for NY, this should be adjusted accordingly.
