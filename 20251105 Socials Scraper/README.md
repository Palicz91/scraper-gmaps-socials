# Social Media Scraper

This Python script scrapes social media information from business websites listed in a CSV file.

## Features

- **Email Extraction**: Finds professional email addresses with intelligent prioritization
- **Social Media Detection**: Scrapes Facebook, Instagram, LinkedIn, and Snapchat URLs
- **Professional Email Scoring**: Ranks emails by professionalism (info@, contact@, etc.)
- **Error Handling**: Robust error handling and logging
- **CSV Processing**: Reads input CSV and generates output with scraped data

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:
```bash
playwright install
```

## Usage

1. Place your input CSV file named `input.csv` in the same directory
2. Ensure your CSV has a `website` column with the URLs to scrape
3. Run the scraper:
```bash
python social_media_scraper.py
```

The script will generate an `output.csv` file with the original data plus these new columns:
- `scraped_email`: Most professional email found
- `scraped_facebook`: Facebook page URL
- `scraped_instagram`: Instagram page URL  
- `scraped_linkedin`: LinkedIn page URL
- `scraped_snapchat`: Snapchat profile URL

## Email Prioritization Logic

The script uses a scoring system to identify the most professional email:

**High Priority (10-8 points):**
- info@, contact@, hello@, support@

**Medium Priority (7-4 points):**
- sales@, admin@, office@, business@

**Low Priority (3-1 points):**
- general@, noreply@, no-reply@

**Penalties:**
- Personal domains (gmail.com, yahoo.com): -5 points
- Test/temp emails: -10 points

## Configuration

You can modify the scraper behavior by editing these parameters in the script:

- `headless=True`: Run browser in background (set to False to see browser)
- `timeout=30000`: Page load timeout in milliseconds
- `await asyncio.sleep(1)`: Delay between requests (be respectful!)

## Logging

The script creates a `scraper.log` file with detailed information about the scraping process.

## Error Handling

- Timeout handling for slow-loading pages
- Invalid URL handling
- Network error recovery
- Graceful failure with partial results

## Requirements

- Python 3.7+
- Playwright
- Pandas
- Internet connection
