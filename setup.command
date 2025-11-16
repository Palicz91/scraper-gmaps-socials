#!/bin/bash
echo "============================================"
echo " 🧠 GMaps + Social Media Scraper Setup"
echo "============================================"

set -e
cd "$(dirname "$0")"

echo "📦 Checking Python..."
if ! command -v python3 &>/dev/null; then
  echo "❌ Python3 is not installed. Please install Python 3.9+ first."
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "🐍 Creating virtual environment..."
  python3 -m venv venv
else
  echo "✅ Virtual environment already exists."
fi

echo "🔧 Activating environment..."
source venv/bin/activate

echo "⬆️  Upgrading pip..."
pip install --upgrade pip

echo "📦 Installing required Python packages..."
pip install playwright pandas chardet openpyxl selenium scrapy

echo "🌐 Installing Playwright browsers (Chrome/Chromium)..."
playwright install

echo "📁 Checking folder structure..."
mkdir -p "20251105 GMaps Scraper"
mkdir -p "20251105 Socials Scraper"

touch "20251105 GMaps Scraper/locations.txt"
touch "20251105 GMaps Scraper/brands.txt"
touch "20251105 GMaps Scraper/categories.txt"

echo "✅ Setup complete!"
echo ""
echo "You can now run the scraper by typing:"
echo ""
echo "  source venv/bin/activate"
echo "  python3 run_all.py"
echo ""
echo "Or simply double-click the ▶️ Run Scraper.command file."
echo "============================================"
