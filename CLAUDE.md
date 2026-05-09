# GM Scraper 2

## Mi ez?
Aktív, produkciós Google Maps scraper review analízissel és cold email pipeline-nal. A Scraper 1 modernizált utódja. Fő különbségek:
- **Review analízis**: csillag eloszlás (1-5), megválaszolt/megválaszolatlan arány, negatív review detekció
- **Email validáció**: Reacher API integráció (find + verify)
- **Email generálás**: Gemini 9 prompt set-tel (3 bucket × 3 opener)
- **Telegram bot**: run_config.json toggle-ökkel (review, validation, classification, generation, push)

## Pipeline
`run_all.py` futtatja sorban a teljes pipeline-t:

1. **make_queries.py** — locations.txt + categories.txt → google_maps_queries.txt
2. **search_query.py** — GMaps keresés → links.txt (helyURL-ek)
3. **get_place_data.py** — Hely részletek scrape → places_data.csv
4. **social_media_scraper.py** — Email, telefon, social linkek → output.xlsx

## Scraper indítás
1. Nézd meg `locations.txt` és `categories.txt` formátumát a `20251105 GMaps Scraper/` mappában
2. Ha várost kérek: generálj 50-100 részletes lokációt, minden kerület/neighborhood külön sorban
   Formátum: `[neighborhood] [city]` — pl. `Hackney London`, `Shoreditch London`
   Fedj le: belváros, külváros, trendy negyedek, éttermes utcák, turista zónák
3. `categories.txt` default: restaurants (hacsak mást nem mondok)
4. Indítás: `source ../venv/bin/activate && python3 run_all.py`
5. A scraper2 Telegram botja (`bot.py`) külön fut hello userként — ne indítsd el újra

## Fő fájlok
| Fájl | Szerep |
|------|--------|
| `run_all.py` | Master orchestrátor, retry logika, Telegram értesítés |
| `bot.py` | Telegram bot (/locations, /categories, /run, /status) |
| `postprocess_places.py` | Duplikáció szűrés, adattisztítás |
| `.env` | API kulcsok (Gemini, Reacher, Smartlead, Telegram) |

## Input / Output fájlok
- **Input**: `locations.txt`, `categories.txt` (a `20251105 GMaps Scraper/` mappában)
- **Köztes**: `google_maps_queries.txt`, `links.txt`, `places_data.csv`
- **Output**: `output.xlsx` (email, telefon, social linkekkel gazdagítva)

## Logging és resume
- `run_all_log.txt` — pipeline log
- `last_processed.txt` — social scraper resume checkpoint (soronkénti mentés)
- `bot.log` — Telegram bot log

## Review analízis
A `get_place_data.py` minden helyhez kinyeri:
- **Összes review szám** + **csillag eloszlás** (stars_1 — stars_5)
- **Megválaszolt/megválaszolatlan arány** (reviews_answered, reviews_unanswered, unanswered_pct)
- **Becsült megválaszolatlan negatívok**: `est_neg_unanswered = (stars_1 + stars_2) * (unanswered / total)`
- Max 50 scroll a review panelben, soronkénti feldolgozás
- Ha csillag adat hiányzik: becslés az összesített ratingből

**Formula**: `LRPI = C01×0.25 + C15×0.20 + C04×0.15 + C07×0.12 + C05×0.10 + C11×0.10 + C08×0.08`

| Kód | Komponens | Súly | Leírás |
|-----|-----------|------|--------|
| C01 | Response Rate | 25% | answered / total × 100 |
| C15 | BSS-Lite Inverted | 20% | Backlog severity: vol_penalty (40%) + vol_severity (35%) + neg_exposure (25%) |
| C04 | Est Unanswered Negatives | 15% | Megválaszolatlan negatívok büntetése (20-anként -100 pont) |
| C07 | Positive Concentration | 12% | (stars_4 + stars_5) / total × 100 |
| C05 | Rating vs Cluster Avg | 10% | 50 + (rating - cluster_avg) / 2 × 50 |
| C11 | Volume Percentile | 10% | Percentilis rang a cluster review eloszlásában |
| C08 | Negative Ratio Inverted | 8% | 100 - negatív arány (4× multiplier) |

**Confidence adjustment**: `adj = raw × min(reviews/50, 1.0) + 50 × (1 - confidence)` — <50 review penalizálva

**Band → Bucket routing**:
- **A (Burning)**: Critical + Poor, VAGY magas rating de ignorálja a review-kat (≥4.5 rating, <30% response)
- **B (Eroding)**: Average + ≥5 megválaszolatlan + ≥25% unanswered_pct
- **D (Sleeping)**: Elit non-responder (magas rating, alacsony válaszarány)
- **SKIP**: Good, Excellent, alacsony fájdalmú Average, vagy managed helyek (<25% unanswered)

## Adatfolyam
```
places_data.csv (GMaps + reviews)
  → output.csv (social enriched)
  → output_cleared.csv (postprocess: dedup, clean, email priority)
  → output_emails.csv (Reacher: find + verify)
  → Smartlead (CRM push)
```

## Fontos tudnivalók
- Python 3 + Playwright (headless Chrome), venv: `../venv/`
- A social scraper soronként ment — megszakítás után folytatható
- Pipeline indítás előtt a régi köztes fájlokat automatikusan törli
- Ha bot.log-ban "Conflict" hiba van: két bot instance fut egyszerre, az egyiket le kell állítani

## Pipeline lifecycle és watchdog

### Normál flow
1. `run_forever.sh` indítja a `run_all.py`-t (crash esetén exponential backoff restart, max 20×)
2. `run_all.py` végigfuttatja a pipeline-t (GMaps → Socials → Email → Cold Email → Push)
3. Sikeres befejezés → `pipeline_complete.marker` fájl íródik + `run_forever.sh` kilép
4. Watchdog (*/30 cron) ellenőrzi: ha marker <24h-s, NEM indít újra

### Watchdog (watchdog.sh)
- Cron: `*/30 * * * *` claude-bot crontab
- Check 1: `pipeline_complete.marker` — ha friss (<24h), skip (pipeline sikeresen végzett)
- Check 2: fut-e `run_forever.sh`? Ha nem → restart
- Check 3: frissül-e valamelyik output fájl? Ha >60 perc stale → kill + restart
- Figyelt fájlok: places_data.csv, links.txt, Socials output.csv

### Manuális újraindítás
```bash
# Ha a pipeline kész de újra akarod indítani:
rm /home/hello/scraper/scraper2/pipeline_complete.marker
# A watchdog 30 percen belül automatikusan elindítja

# Vagy kézzel:
sudo -u hello bash -c "cd /home/hello/scraper/scraper2 && nohup ./run_forever.sh >> run_all_log.txt 2>&1 &"
```
