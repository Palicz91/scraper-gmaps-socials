import streamlit as st
import subprocess
import time
import pandas as pd
from pathlib import Path
import shutil

# Paths
BASE_DIR = Path(__file__).resolve().parent
GMAPS_DIR = BASE_DIR / "20251105 GMaps Scraper"
SOCIAL_DIR = BASE_DIR / "20251105 Socials Scraper"

st.set_page_config(page_title="GMaps Scraper", layout="wide")
st.title("🗺️ Google Maps + Social Scraper")

# --- SIDEBAR: Input fájlok ---
st.sidebar.header("1. Input adatok")

brands_input = st.sidebar.text_area(
    "Brands (soronként 1)", 
    placeholder="McDonald's\nBurger King\nKFC",
    height=100
)

categories_input = st.sidebar.text_area(
    "Categories (opcionális, soronként 1)", 
    placeholder="restaurant\nfast food",
    height=80
)

locations_input = st.sidebar.text_area(
    "Locations (soronként 1)", 
    placeholder="Budapest\nDebrecen\nSzeged",
    height=100
)

# --- FÁJLOK MENTÉSE ---
def save_inputs():
    (GMAPS_DIR / "brands.txt").write_text(brands_input.strip(), encoding="utf-8")
    (GMAPS_DIR / "categories.txt").write_text(categories_input.strip(), encoding="utf-8")
    (GMAPS_DIR / "locations.txt").write_text(locations_input.strip(), encoding="utf-8")

# --- SCRIPT FUTTATÁS ---
def run_script(script_path: Path, cwd: Path, status_placeholder):
    status_placeholder.info(f"🚀 Futtatás: {script_path.name}...")
    try:
        result = subprocess.run(
            ["python3", str(script_path)],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=3600  # 1 óra max
        )
        if result.returncode == 0:
            status_placeholder.success(f"✅ {script_path.name} kész!")
            return True
        else:
            status_placeholder.error(f"❌ Hiba: {result.stderr[:500]}")
            return False
    except subprocess.TimeoutExpired:
        status_placeholder.error("⏱️ Timeout (1 óra)")
        return False
    except Exception as e:
        status_placeholder.error(f"❌ {e}")
        return False

# --- TABS ---
tab1, tab2, tab3 = st.tabs(["🚀 Teljes Pipeline", "📊 Eredmények", "⚙️ Haladó"])

# --- TAB 1: TELJES PIPELINE ---
with tab1:
    st.subheader("Teljes scraping folyamat")
    
    col1, col2 = st.columns(2)
    with col1:
        cleanup = st.checkbox("🧹 Előző fájlok törlése", value=True)
    with col2:
        skip_social = st.checkbox("⏭️ Social scraping kihagyása", value=False)
    
    if st.button("▶️ Indítás", type="primary", use_container_width=True):
        if not brands_input.strip() or not locations_input.strip():
            st.error("❌ Brands és Locations kötelező!")
        else:
            save_inputs()
            
            progress = st.progress(0)
            status = st.empty()
            
            # Cleanup
            if cleanup:
                status.info("🧹 Törlés...")
                for f in ["links.txt", "places_data.csv", "last_processed.txt", 
                          "google_maps_queries.txt", "scraper_log.txt"]:
                    p = GMAPS_DIR / f
                    if p.exists():
                        p.unlink()
                for f in ["input.csv", "output.csv", "output_cleared.csv", "scraper.log"]:
                    p = SOCIAL_DIR / f
                    if p.exists():
                        p.unlink()
            
            # Step 1: make_queries
            progress.progress(10)
            if not run_script(GMAPS_DIR / "make_queries.py", GMAPS_DIR, status):
                st.stop()
            
            # Step 2: search_query
            progress.progress(30)
            if not run_script(GMAPS_DIR / "search_query.py", GMAPS_DIR, status):
                st.stop()
            
            # Step 3: get_place_data
            progress.progress(50)
            if not run_script(GMAPS_DIR / "get_place_data.py", GMAPS_DIR, status):
                st.stop()
            
            # Step 4: Social scraper (opcionális)
            if not skip_social:
                progress.progress(70)
                gmaps_csv = GMAPS_DIR / "places_data.csv"
                if gmaps_csv.exists():
                    shutil.copy2(gmaps_csv, SOCIAL_DIR / "input.csv")
                    if not run_script(SOCIAL_DIR / "social_media_scraper.py", SOCIAL_DIR, status):
                        st.warning("⚠️ Social scraper hiba, folytatás...")
                    
                    # Step 5: Postprocess
                    progress.progress(90)
                    output_csv = SOCIAL_DIR / "output.csv"
                    if output_csv.exists():
                        run_script(SOCIAL_DIR / "postprocess_places.py", SOCIAL_DIR, status)
            
            progress.progress(100)
            st.success("🎉 Kész! Nézd meg az Eredmények tabot.")
            st.balloons()

# --- TAB 2: EREDMÉNYEK ---
with tab2:
    st.subheader("Letölthető fájlok")
    
    col1, col2, col3 = st.columns(3)
    
    # places_data.csv
    places_csv = GMAPS_DIR / "places_data.csv"
    with col1:
        st.markdown("**📍 GMaps adatok**")
        if places_csv.exists():
            df = pd.read_csv(places_csv)
            st.metric("Sorok", len(df))
            st.download_button(
                "⬇️ places_data.csv",
                places_csv.read_bytes(),
                "places_data.csv",
                "text/csv"
            )
        else:
            st.info("Még nincs adat")
    
    # output.csv
    output_csv = SOCIAL_DIR / "output.csv"
    with col2:
        st.markdown("**📧 Social adatok**")
        if output_csv.exists():
            df = pd.read_csv(output_csv)
            st.metric("Sorok", len(df))
            st.download_button(
                "⬇️ output.csv",
                output_csv.read_bytes(),
                "output.csv",
                "text/csv"
            )
        else:
            st.info("Még nincs adat")
    
    # output_cleared.csv
    cleared_csv = SOCIAL_DIR / "output_cleared.csv"
    with col3:
        st.markdown("**✨ Tisztított**")
        if cleared_csv.exists():
            df = pd.read_csv(cleared_csv)
            st.metric("Sorok", len(df))
            st.download_button(
                "⬇️ output_cleared.csv",
                cleared_csv.read_bytes(),
                "output_cleared.csv",
                "text/csv"
            )
        else:
            st.info("Még nincs adat")
    
    # Preview
    st.divider()
    st.subheader("Előnézet")
    preview_file = st.selectbox("Fájl", ["places_data.csv", "output.csv", "output_cleared.csv"])
    
    preview_path = {
        "places_data.csv": places_csv,
        "output.csv": output_csv,
        "output_cleared.csv": cleared_csv
    }[preview_file]
    
    if preview_path.exists():
        df = pd.read_csv(preview_path)
        st.dataframe(df.head(100), use_container_width=True)
    else:
        st.info("A fájl még nem létezik.")

# --- TAB 3: HALADÓ ---
with tab3:
    st.subheader("Egyedi scriptek futtatása")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**GMaps Scraper**")
        if st.button("1️⃣ make_queries.py"):
            save_inputs()
            with st.spinner("Futtatás..."):
                run_script(GMAPS_DIR / "make_queries.py", GMAPS_DIR, st.empty())
        
        if st.button("2️⃣ search_query.py"):
            with st.spinner("Futtatás..."):
                run_script(GMAPS_DIR / "search_query.py", GMAPS_DIR, st.empty())
        
        if st.button("3️⃣ get_place_data.py"):
            with st.spinner("Futtatás..."):
                run_script(GMAPS_DIR / "get_place_data.py", GMAPS_DIR, st.empty())
    
    with col2:
        st.markdown("**Social Scraper**")
        if st.button("4️⃣ social_media_scraper.py"):
            gmaps_csv = GMAPS_DIR / "places_data.csv"
            if gmaps_csv.exists():
                shutil.copy2(gmaps_csv, SOCIAL_DIR / "input.csv")
                with st.spinner("Futtatás..."):
                    run_script(SOCIAL_DIR / "social_media_scraper.py", SOCIAL_DIR, st.empty())
            else:
                st.error("Nincs places_data.csv!")
        
        if st.button("5️⃣ postprocess_places.py"):
            with st.spinner("Futtatás..."):
                subprocess.run(
                    ["python3", "postprocess_places.py", "output.csv"],
                    cwd=str(SOCIAL_DIR)
                )
                st.success("✅ Kész!")
    
    st.divider()
    st.subheader("Logok")
    
    log_files = [
        GMAPS_DIR / "scraper_log.txt",
        SOCIAL_DIR / "scraper.log",
        BASE_DIR / "run_all_log.txt"
    ]
    
    for log in log_files:
        if log.exists():
            with st.expander(f"📄 {log.name}"):
                st.code(log.read_text()[-5000:])  # utolsó 5000 karakter
