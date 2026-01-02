import csv
import re
import sys
from pathlib import Path
from urllib.parse import unquote

PLACEHOLDER_DOMAINS = {
    "domain.com",
    "example.com",
    "test.com",
    "email.com",
    "yourdomain.com",
    "sampleemail.com",
    "company.com",
    "business.com",
    "website.com",
    "mail.com",
    "mysite.com",
    "mystore.com",
    "godaddy.com",
}

EMAIL_RE = re.compile(
    r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
    re.IGNORECASE,
)

PHONE_TOKEN_SPLIT_RE = re.compile(r"[,\s/]+")

def email_priority(email: str) -> int:
    """Magasabb = jobb business email."""
    if not email:
        return -100
    
    lower = email.lower()
    
    # Business email prefixes (highest priority)
    if any(p in lower for p in ['info@', 'contact@', 'hello@', 'sales@']):
        return 10
    
    # Other business-like prefixes (medium priority)
    if any(p in lower for p in ['admin@', 'office@', 'support@', 'business@']):
        return 5
    
    # Personal email providers (lower priority)
    if any(d in lower for d in ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']):
        return -5
    
    # Default business domains get neutral score
    return 0

def simplify_name(name: str) -> str:
    if not name:
        return ""
    # Első rész " - " előtt
    base = name.split(" - ")[0].strip()
    # Zárójeles rész levágása a végéről
    base = re.sub(r"\s*\(.*?\)\s*$", "", base).strip()
    # Minden nem-ASCII karakter kidobása (arab, kínai, stb.)
    ascii_only = "".join(ch for ch in base if ord(ch) < 128).strip()
    return ascii_only or base or name.strip()

def clean_phone(phone: str) -> str:
    if not phone:
        return ""
    phone = phone.strip()
    if phone.lower().startswith("phone:"):
        phone = phone[6:]
    return phone.strip()

def normalize_phone_token(token: str) -> str:
    if not token:
        return ""
    token = token.strip()

    # Ha "Phone:" előtag, dobjuk
    if token.lower().startswith("phone:"):
        token = token[6:].strip()

    # Tartsuk meg az elején a + jelet, az összes többi nem szám karaktert dobjuk
    has_plus = token.startswith("+")
    digits = "".join(ch for ch in token if ch.isdigit())

    if not digits:
        return ""

    # Ha + nélkül, de "00" prefixszel kezdődik → +XX...
    if not has_plus and digits.startswith("00"):
        digits = digits[2:]
        has_plus = True

    # Alap hossz check: túl rövid / túl hosszú → kuka
    # (Facebook ID-k, Google ID-k stb. gyakran 15+ vagy 5-)
    if len(digits) < 7 or len(digits) > 15:
        return ""

    if has_plus:
        return "+" + digits
    return digits  # maradhat országkód nélkül is, ha nem volt + jel

def split_phones(raw: str):
    if not raw:
        return []
    parts = PHONE_TOKEN_SPLIT_RE.split(raw)
    seen = set()
    result = []

    for p in parts:
        norm = normalize_phone_token(p)
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)

    return result

def extract_country(address: str, plus_code: str) -> str:
    parts_to_search = [address or "", plus_code or ""]

    for text in parts_to_search:
        if not text:
            continue

        cleaned = text.strip()

        # "Plus code: ..." elejét vágjuk le, ha van
        if cleaned.lower().startswith("plus code:"):
            cleaned = cleaned.split(":", 1)[1].strip()

        # 1) vessző utáni utolsó rész
        if "," in cleaned:
            candidate = cleaned.split(",")[-1].strip()
            candidate = re.sub(r"\b\d+\b", "", candidate).strip()

            # ha még mindig hosszú, és van benne " - ", vegyük annak az utolsó részét
            if " - " in candidate:
                sub = candidate.split(" - ")[-1].strip()
                sub = re.sub(r"\b\d+\b", "", sub).strip()
                if sub:
                    return sub

            if candidate:
                return candidate

        # 2) ha nem volt vessző, próbáljuk közvetlenül a " - " utáni utolsó részt
        if " - " in cleaned:
            candidate = cleaned.split(" - ")[-1].strip()
            candidate = re.sub(r"\b\d+\b", "", candidate).strip()
            if candidate:
                return candidate

    return ""

def clean_email_before_validation(email: str) -> str:
    """Email előtisztítás validáció előtt."""
    if not email:
        return ""
    
    # URL decode FIRST, then strip
    email = unquote(email).strip()
    
    # Query string levágás
    if "?" in email:
        email = email.split("?")[0]
    
    # Trailing pont/vessző/pontosvessző
    email = email.rstrip(".,;")
    
    return email

def is_valid_email(email: str) -> bool:
    if not email:
        return False
    
    # Use the cleaning function for consistency
    email = clean_email_before_validation(email)
    
    # 3. Üres vagy túl rövid
    if len(email) < 5:
        return False
    
    lower = email.lower()
    
    # 4. Képfájl pattern - BŐVÍTETT
    # @2x, @3x retina képek kiszűrése
    if re.search(r"@\d+x", lower):
        return False
    
    # 5. Fájlkiterjesztés check - bárhol az emailben
    image_exts = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico")
    if any(ext in lower for ext in image_exts):
        return False
    
    # 6. Útvonal jellegű
    if "/" in lower:
        return False
    
    # 7. Nincs @ vagy több @ van
    if lower.count("@") != 1:
        return False
    
    local, domain = lower.split("@")
    
    # 8. Honeypot/spam trap patterns
    if "sentry" in local:
        return False
    
    # 9. Értelmetlen random stringek kiszűrése (pl. "wsentryer", "xyzabc")
    # Ha a local part 6+ karakter és nincs benne magánhangzó → garbage
    if len(local) >= 6:
        vowels = set("aeiou")
        if not any(c in vowels for c in local.replace(".", "")):
            return False
    
    # 10. Local part validáció
    if not local or not re.match(r"^[a-z0-9._%+-]+$", local):
        return False
    
    # 11. Domain validáció
    if not domain:
        return False
    
    # 12. Dupla pont a domainben
    if ".." in domain:
        return False
    
    # 13. Domain formátum - legalább egy pont, csak alnum és kötőjel
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", domain):
        return False
    
    # 14. Placeholder domain check
    if domain in PLACEHOLDER_DOMAINS:
        return False
    
    if domain.endswith(".local"):
        return False
    
    # 15. Túl rövid domain (valószínűleg garbage)
    if len(domain) < 4:
        return False

    return True

def split_emails(raw: str):
    if not raw:
        return []
    
    # Egységesítés
    tmp = re.sub(r"[;\s]+", ",", raw)
    candidates = [p.strip() for p in tmp.split(",") if p.strip()]

    seen = set()
    result = []
    
    for c in candidates:
        # ✅ ELŐTISZTÍTÁS
        cleaned = clean_email_before_validation(c)
        
        if not cleaned:
            continue
            
        if cleaned.lower() not in seen and is_valid_email(cleaned):
            seen.add(cleaned.lower())
            result.append(cleaned)  # ✅ TISZTÍTOTT VERZIÓ
    
    result.sort(key=email_priority, reverse=True)
    return result

def process(in_path: Path, out_path: Path):
    # Global email deduplication across entire dataset
    global_emails_seen = set()
    
    with in_path.open("r", encoding="utf-8-sig", newline="") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []

        # új oszlopok
        if "simple_name" not in fieldnames:
            fieldnames = ["simple_name"] + fieldnames
        if "country" not in fieldnames:
            fieldnames = fieldnames + ["country"]

        rows_out = []

        for row in reader:
            # 1) egyszerűsített név
            name = row.get("name", "")
            row["simple_name"] = simplify_name(name)

            # 2) phone tisztítás + validáció
            phone_raw = row.get("phone", "")
            phones = split_phones(phone_raw)
            row["phone"] = phones[0] if phones else ""

            # 3) ország cím / plus code alapján
            address = row.get("address", "")
            plus_code = row.get("plus_code", "")
            row["country"] = extract_country(address, plus_code)

            # 4–5) email logika - ✅ TISZTÍTÁSSAL
            scraped_email = clean_email_before_validation(row.get("scraped_email") or "")
            scraped_email_raw = (row.get("scraped_email_raw") or "").strip()

            # ha teljes egyezés, raw törlése
            if scraped_email and scraped_email_raw:
                # Compare cleaned versions
                cleaned_raw = clean_email_before_validation(scraped_email_raw)
                if scraped_email == cleaned_raw:
                    scraped_email_raw = ""
                    row["scraped_email_raw"] = ""

            # scraped_email validáció
            if scraped_email and not is_valid_email(scraped_email):
                scraped_email = ""
            
            # MINDIG frissítsd a row-t
            row["scraped_email"] = scraped_email

            # raw további emailjei
            if scraped_email_raw:
                emails_from_raw = split_emails(scraped_email_raw)
            else:
                emails_from_raw = []

            # 6) scraped_phone és scraped_whatsapp validáció
            scraped_phone_raw = row.get("scraped_phone", "") or ""
            scraped_whatsapp_raw = row.get("scraped_whatsapp", "") or ""

            phones_scraped = split_phones(scraped_phone_raw)
            phones_whatsapp = split_phones(scraped_whatsapp_raw)

            row["scraped_phone"] = ", ".join(phones_scraped) if phones_scraped else ""
            row["scraped_whatsapp"] = ", ".join(phones_whatsapp) if phones_whatsapp else ""

            # egyesített email lista
            email_set = []
            if scraped_email and is_valid_email(scraped_email):
                email_set.append(scraped_email)
            for e in emails_from_raw:
                if e not in email_set:
                    email_set.append(e)

            # Sort emails by business priority
            email_set.sort(key=email_priority, reverse=True)

            # ha nincs használható email: egy sor marad
            if not email_set:
                rows_out.append(row)
                continue

            # ha van 1+ email: sor(oka)t duplikálunk, és a raw-t eldobjuk
            # GLOBAL DEDUPLICATION: csak olyan emaileket használunk, amik még nem voltak
            for email in email_set:
                if email.lower() in global_emails_seen:
                    continue  # már volt ilyen email másik cégnél
                global_emails_seen.add(email.lower())
                new_row = dict(row)
                new_row["scraped_email"] = email
                new_row["scraped_email_raw"] = ""
                rows_out.append(new_row)

    with out_path.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) != 1:
        print("Használat: python3 postprocess_places.py input.csv")
        sys.exit(1)

    in_path = Path(argv[0])
    if not in_path.exists():
        print(f"Hiba: {in_path} nem létezik.")
        sys.exit(1)

    # Mindig új fájlba írunk, nem írunk felül semmit
    out_path = in_path.with_name(in_path.stem + "_cleared.csv")

    process(in_path, out_path)
    print(f"Kész: {out_path}")

if __name__ == "__main__":
    main()
