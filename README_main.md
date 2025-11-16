# 🧠 GMaps + Social Media Scraper (Full Pipeline)

This project automates the process of collecting business data from **Google Maps** and then enriching it with **social media and contact information** from the businesses’ own websites.

---

## 🚀 Overview

The pipeline runs **in four stages**:

1. **Make Queries** – Generates all Google Maps search queries  
2. **Search Query** – Searches all queries and collects place URLs  
3. **Get Place Data** – Scrapes all places for name, address, website, rating, etc.  
4. **Social Media Scraper** – Enriches that data with emails & social links (Facebook, Instagram, LinkedIn, etc.)

All four scripts are automatically executed by **`run_all.py`**, in the correct order.

---

## 🧩 Folder Structure

20251105 Scraper/
├── run_all.py
├── run_all_log.txt
├── 20251105 GMaps Scraper/
│ ├── make_queries.py
│ ├── search_query.py
│ ├── get_place_data.py
│ ├── locations.txt
│ ├── brands.txt
│ ├── categories.txt
│ ├── google_maps_queries.txt
│ ├── links.txt
│ └── places_data.csv
└── 20251105 Socials Scraper/
├── social_media_scraper.py
├── input.csv
├── output.xlsx
└── scraper.log

yaml
Copy code

---

## ⚙️ How to Run Everything (Full Automation)

From the main folder:

```bash
cd ~/Downloads/"20251105 Scraper"
python3 run_all.py


What happens automatically:

Runs the 3 Google Maps scrapers (queries → links → data)

Copies places_data.csv into the Social scraper folder as input.csv (overwrites any previous input)

Launches the Social Media Scraper

Generates output.xlsx with enriched social data

If any script fails, it will retry automatically and log errors in run_all_log.txt.

🪄 Part 1: Google Maps Scraper
📍 Step 1 – Make Queries
Input files:

locations.txt → List of cities or regions

brands.txt → List of brand names (optional)

categories.txt → Business categories (e.g., "restaurant", "barber")

Run manually (optional):

bash
Copy code
python make_queries.py
Output:

google_maps_queries.txt → Contains all combined search queries

🔍 Step 2 – Search Query
Reads the queries from google_maps_queries.txt, searches each on Google Maps, and saves all found business URLs.

Run manually (optional):

bash
Copy code
python search_query.py
Output:

links.txt → List of all discovered business place URLs

🧾 Step 3 – Get Place Data
Reads all links from links.txt, opens each business page, and scrapes detailed data.

Run manually (optional):

bash
Copy code
python get_place_data.py
Output:

places_data.csv → Contains name, category, address, rating, reviews, website, phone, etc.

⚙️ Features
Headless Mode: Runs Chrome invisibly (no open browser window)

Automatic Retry: Re-attempts on network or rendering errors

Progress Resume: Saves last_processed.txt to continue after interruptions

Logging: Errors are written to scraper_log.txt

📱 Part 2: Social Media Scraper
This script enriches the places_data.csv data with contact and social media information.

🔹 Input
Automatically created by run_all.py — it copies places_data.csv into this folder as input.csv.
Your CSV must include a website column.

🔹 Run the Scraper
bash
Copy code
python social_media_scraper.py
🔹 Output
output.xlsx – Contains the original columns plus:

scraped_email

scraped_phone

scraped_whatsapp

scraped_facebook

scraped_instagram

scraped_linkedin

scraped_twitter

scraped_tiktok

🧠 Features
Smart Email Prioritization (info@, contact@, hello@, etc.)

Automatic Social Detection (Facebook, Instagram, LinkedIn, TikTok, Twitter)

Phone & WhatsApp Extraction

Robust Error Handling & Logging

Incremental Save: Saves progress after each row

Runs in Headless Mode: Browser invisible by default

⚙️ Installation
Install dependencies once (from the main folder):

bash
Copy code
pip install playwright pandas chardet openpyxl selenium scrapy
playwright install
Make sure Chrome or Chromium is installed on your machine.

🪵 Logs and Outputs
run_all_log.txt → Full pipeline log

scraper.log → Social scraper logs

places_data.csv → Raw GMaps output

output.xlsx → Final enriched data

🧩 Summary
✅ The GMaps scraper collects all business data
✅ The Social scraper enriches it with contact & social profiles
✅ The run_all.py script runs the full pipeline automatically — start to finish

With this setup, you can collect, enrich, and export verified business intelligence data with a single command.
