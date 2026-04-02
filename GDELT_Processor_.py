import requests
import zipfile
import os
import io
import pandas as pd
import geopandas as gpd
import sqlite3
import datetime
from datetime import timedelta
import urllib3
import time
from arcgis.gis import GIS

# =============================================================================
# CONFIGURATION
# =============================================================================
DATABASE_FILE_PATH = "gdelt_events.db"
ARCGIS_ENTERPRISE_PORTAL_URL = "[enterprise_portal_url_here]"  # e.g., "https://myportal.domain.com/instance/sharing/rest"

# --- Processing Settings ---
NUMBER_OF_TRANSLATION_FILES_TO_PROCESS_PER_RUN = 1164  # This is a key configuration: Adjust this number based on how many recent files you want to process each time.
REBUILD_DATABASE_BASELINE_FROM_SCRATCH = False 
BASELINE_TRANSLATION_FILE_COUNT = 35040

# --- Filter Configuration ---
CAMEO_ROOT_CODE_MINIMUM = 10
CAMEO_ROOT_CODE_MAXIMUM = 20

# --- ArcGIS Items ---
MAP_HOSTED_FEATURE_LAYER_ITEM_IDENTIFIER = "[item_id_here]" 
HISTORICAL_SUMMARY_TABLE_ITEM_IDENTIFIER = "[item_id_here]" 
EVENT_BREAKDOWN_TABLE_ITEM_IDENTIFIER = "[item_id_here]" 
TOP_REPUTABLE_EVENTS_TABLE_ITEM_IDENTIFIER = "[item_id_here]" 

# Reputable Sources Whitelist
REPUTABLE_DOMAINS = ['apnews.com', 'reuters.com', 'bbc.com', 'npr.org', 'nytimes.com', 'washingtonpost.com', 'theguardian.com', 'aljazeera.com', 'dw.com']

# =============================================================================
# NETWORK / AUTH / DB SETUP
# =============================================================================
USE_INSECURE_SSL_CERTIFICATE_VERIFICATION = False
DEFAULT_HTTP_TIMEOUT_SECONDS = 90
if USE_INSECURE_SSL_CERTIFICATE_VERIFICATION:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def http_get_request(url, timeout_seconds=DEFAULT_HTTP_TIMEOUT_SECONDS):
    return requests.get(url, timeout=timeout_seconds, verify=(not USE_INSECURE_SSL_CERTIFICATE_VERIFICATION))

def connect_to_arcgis_enterprise_portal():
    print("Connecting to ArcGIS Enterprise portal...")
    username = os.environ.get("[ARCGIS_USER]")
    password = os.environ.get("[ARCGIS_PASS]")
    if not username or not password: raise RuntimeError("Missing ARCGIS_USER/ARCGIS_PASS")
    gis = GIS(ARCGIS_ENTERPRISE_PORTAL_URL, username, password, verify_cert=(not USE_INSECURE_SSL_CERTIFICATE_VERIFICATION))
    print(f"Authenticated as: {gis.users.me.username}")
    return gis

def setup_sqlite_database(rebuild_database=False):
    with sqlite3.connect(DATABASE_FILE_PATH) as con:
        cur = con.cursor()
        if rebuild_database:
            cur.execute("DROP TABLE IF EXISTS events_log")
            cur.execute("DROP TABLE IF EXISTS historical_summary")
            cur.execute("DROP TABLE IF EXISTS cameo_codes")

        cur.execute("""CREATE TABLE IF NOT EXISTS events_log (
            GLOBALEVENTID INTEGER PRIMARY KEY, timestamp TEXT, country_code TEXT, 
            EventRootCode INTEGER, AvgTone REAL, GoldsteinScale REAL, NumSources INTEGER,
            NumArticles INTEGER, SOURCEURL TEXT)""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS historical_summary (
            country_code TEXT, summary_type TEXT, summary_date TEXT, 
            avg_tone REAL, event_count INTEGER, avg_goldstein REAL, 
            PRIMARY KEY (country_code, summary_type, summary_date))""")
            
        cur.execute("CREATE TABLE IF NOT EXISTS cameo_codes (code INTEGER PRIMARY KEY, description TEXT)")
        cameos = [(1, "MAKE PUBLIC STATEMENT"), (2, "APPEAL"), (3, "EXPRESS INTENT TO COOPERATE"), (4, "CONSULT"), 
                  (5, "ENGAGE IN DIPLOMATIC COOPERATION"), (6, "ENGAGE IN MATERIAL COOPERATION"), (7, "PROVIDE AID"), 
                  (8, "YIELD"), (9, "INVESTIGATE"), (10, "DEMAND"), 
                  (11, "DISAPPROVE"), (12, "REJECT"), (13, "THREATEN"), (14, "PROTEST"), (15, "EXHIBIT FORCE POSTURE"),
                  (16, "REDUCE RELATIONS"), (17, "COERCE"), (18, "ASSAULT"), (19, "FIGHT"), (20, "MASS VIOLENCE")]
        cur.executemany("INSERT OR IGNORE INTO cameo_codes VALUES (?, ?)", cameos)
    print(f"SQLite database ready.")

# =============================================================================
# DATA FETCHING & PROCESSING
# =============================================================================
GDELT_HEADERS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
    "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
    "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale",
    "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL"
]

def get_country_polygons():
    url = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/World_Countries_(Generalized)/FeatureServer/0/query?where=1%3D1&outFields=COUNTRY,ISO&outSR=4326&f=geojson"
    gdf = gpd.read_file(http_get_request(url).text)
    gdf = gdf.rename(columns={"COUNTRY": "country_name", "ISO": "country_code"})
    return gdf[gdf['country_code'].str.match(r'^[A-Z]{2}$', na=False)]

def read_file(url):
    try:
        r = http_get_request(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])), sep='\t', header=None, names=GDELT_HEADERS, dtype=str)
        for c in ["GLOBALEVENTID", "NumSources", "NumArticles", "GoldsteinScale", "EventRootCode", "AvgTone", "ActionGeo_Lat", "ActionGeo_Long"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c].astype(str).str.strip(), errors='coerce')
        df["added_datetime"] = pd.to_datetime(df["DATEADDED"], format="%Y%m%d%H%M%S", errors='coerce')
        return df
    except Exception as e:
        print(f"Failed {url}: {e}")
        return pd.DataFrame()

def iterate_batches(urls, batch_size=200):
    for start in range(0, len(urls), batch_size):
        dfs = [read_file(url) for url in urls[start:start+batch_size]]
        dfs = [d for d in dfs if not d.empty]
        if not dfs: continue
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined[(combined["EventRootCode"] >= CAMEO_ROOT_CODE_MINIMUM) & (combined["EventRootCode"] <= 20)].copy()
        combined["timestamp"] = combined["added_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        clean = combined[["GLOBALEVENTID", "GoldsteinScale", "AvgTone", "NumSources", "NumArticles", "SOURCEURL", "ActionGeo_Lat", "ActionGeo_Long", "EventRootCode", "timestamp"]].copy()
        clean.dropna(subset=["GLOBALEVENTID", "timestamp", "ActionGeo_Lat", "ActionGeo_Long"], inplace=True)
        yield clean

# =============================================================================
# UPDATE FUNCTIONS
# =============================================================================
def update_map_feature_layer_averages(con, gis):
    log_step("4.2", "Calculating Time-Windowed Averages for Map")
    df = pd.read_sql_query("SELECT country_code, AvgTone, timestamp FROM events_log", con)
    df['dt'] = pd.to_datetime(df['timestamp'])
    
    # RELATIVE WINDOWING: Calculate from the last date in your dataset
    latest_data = df['dt'].max()
    print(f"Latest data found: {latest_data}. Calculating windows from this date.")
    
    avg_yesterday = df[(df['dt'] <= latest_data) & (df['dt'] > (latest_data - timedelta(days=1)))].groupby('country_code')['AvgTone'].mean()
    avg_7d = df[df['dt'] > (latest_data - timedelta(days=7))].groupby('country_code')['AvgTone'].mean()
    avg_30d = df[df['dt'] > (latest_data - timedelta(days=30))].groupby('country_code')['AvgTone'].mean()
    avg_365d = df.groupby('country_code')['AvgTone'].mean()

    item = gis.content.get(MAP_HOSTED_FEATURE_LAYER_ITEM_IDENTIFIER)
    lyr = item.layers[0]
    
    # SCHEMA FIX: Using 'iso'
    cc_field = "iso" 
    fset = lyr.query(where="1=1", out_fields=["objectid", cc_field])
    
    updates = []
    for feat in fset.features:
        cc = feat.attributes.get(cc_field) # This retrieves the country code from the 'iso' field
        if cc in avg_365d:
            # Updating the specific windowed fields from schema
            feat.attributes["avg_tone_yesterday"] = float(avg_yesterday.get(cc, 0))
            feat.attributes["avg_tone_7d"] = float(avg_7d.get(cc, 0))
            feat.attributes["avg_tone_30d"] = float(avg_30d.get(cc, 0))
            feat.attributes["avg_tone_365d"] = float(avg_365d.get(cc, 0))
            updates.append(feat)
            
    if updates:
        lyr.edit_features(updates=updates)
        print(f"Successfully updated map averages for {len(updates)} countries.")

def update_historical_summary_table(con, gis):
    print("\n--- Refreshing Historical Summary (Cloud & Local) ---")
    try:
        # 1. Pull and process data
        df = pd.read_sql_query("SELECT timestamp, country_code, AvgTone, GoldsteinScale FROM events_log", con)
        if df.empty: 
            print("No data in events_log to summarize.")
            return
            
        df['dt'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['dt'])
        df['summary_date'] = df['dt'].dt.date
        
        summary_df = df.groupby(['summary_date', 'country_code']).agg({
            'AvgTone': 'mean', 
            'timestamp': 'count', 
            'GoldsteinScale': 'mean'
        }).reset_index()
        
        summary_df.rename(columns={
            'timestamp': 'event_count', 
            'AvgTone': 'avg_tone', 
            'GoldsteinScale': 'avg_goldstein'
        }, inplace=True)
        
        # 2. Update ArcGIS Table
        item = gis.content.get(HISTORICAL_SUMMARY_TABLE_ITEM_IDENTIFIER)
        tbl = item.tables[0] if item.tables else item.layers[0]
        tbl.manager.truncate()
       
        adds = [{"attributes": {
            "country_code": r['country_code'],
            "summary_type": "daily",
            "summary_date": int(pd.to_datetime(r['summary_date']).timestamp() * 1000),
            "avg_tone": float(r['avg_tone']),
            "event_count": int(r['event_count']),
            "avg_goldstein": float(r['avg_goldstein'])
        }} for _, r in summary_df.iterrows()]
        
        for i in range(0, len(adds), 200):
            tbl.edit_features(adds=adds[i:i+200])
        print("Cloud Table Updated.")

        # 3. Save to Local SQLite
        summary_df.to_sql('historical_summary', con, if_exists='replace', index=False)
        print("Local historical_summary table updated successfully.")

    except Exception as e: 
        print(f"Historical Summary Update Failed: {e}")

def update_top_reputable_events_table(con, gis):
    print("\n--- Generating Top 5 Reputable Events (Waterfall) ---")
    try:
        pattern = "|".join(REPUTABLE_DOMAINS).replace(".", "\.")
        cutoff = (datetime.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        
        # Pull data from local SQLite
        df = pd.read_sql_query("""
            SELECT country_code, SOURCEURL, NumArticles, NumSources, EventRootCode 
            FROM events_log 
            WHERE timestamp >= ?
        """, con, params=(cutoff,))
        
        if df.empty:
            print("No recent data found in SQLite.")
            return

        # Identify reputable sources and sort
        df['is_reputable'] = df['SOURCEURL'].str.contains(pattern, case=False, na=False).astype(int)
        df = df.drop_duplicates(subset=['country_code', 'SOURCEURL'], keep='first')
        df = df.sort_values(['country_code', 'is_reputable', 'NumSources'], ascending=[True, False, False])
        top_5 = df.groupby('country_code').head(5)
        
        item = gis.content.get(TOP_REPUTABLE_EVENTS_TABLE_ITEM_IDENTIFIER)
        tbl = item.tables[0] if item.tables else item.layers[0]

        print(f"Truncating and pushing {len(top_5)} records to Waterfall...")
        tbl.manager.truncate()
        
        adds = []
        for _, r in top_5.iterrows():
            # MAPPING: Matches ArcGIS table fields
            adds.append({"attributes": {
                "country_code": r['country_code'],
                "sourceurl": str(r['SOURCEURL']),    
                "numarticles": int(r['NumArticles']), 
                "numsources": int(r['NumSources']),  
                "eventrootcode": int(r['EventRootCode']),
                "is_reputable": int(r['is_reputable'])
            }})
            
        # Push to ArcGIS
        result = tbl.edit_features(adds=adds)
        
        # Verify specific commit success
        adds_res = result.get('addResults', [])
        actual_success = [res for res in adds_res if res.get('success')]
        print(f"Success: {len(actual_success)} records committed to Waterfall.")
        
    except Exception as e: 
        print(f"Waterfall Update Failed: {e}")

# =============================================================================
# PIPELINE HELPERS
# =============================================================================
def log_step(step_num, description):
    """Prints a formatted header for pipeline stages."""
    print(f"\n{'='*15} STEP {step_num}: {description} {'='*15}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    start_time = time.time()
    
    # ---------------------------------------------------------
    # STEP 1: Initialization & Sanity Check (ArcGIS Connection)
    # ---------------------------------------------------------
    log_step(1, "Initializing Environment & ArcGIS")
    setup_sqlite_database(REBUILD_DATABASE_BASELINE_FROM_SCRATCH)
    gis = connect_to_arcgis_enterprise_portal()
    
    print("Fetching reference country polygons...")
    countries_gdf = get_country_polygons()
    
    # Sanity Check: Ensure polygons are loaded
    if countries_gdf.empty:
        raise RuntimeError("Sanity Check Failed: Could not fetch country polygons. Check ArcGIS Portal connection.")
    print(f"Success: Loaded {len(countries_gdf)} country boundaries for spatial joining.")


    # ---------------------------------------------------------
    # STEP 2: GDELT Masterfile Discovery (REGULAR UPDATES)
    # ---------------------------------------------------------
    log_step(2, "Fetching GDELT URL List")
    txt = http_get_request("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt").text
    all_urls = [x.split()[-1] for x in txt.splitlines() if ".translation.export.CSV.zip" in x]
    
    # Sanity Check: Ensure URL list isn't empty
    if not all_urls:
        raise ValueError("Sanity Check Failed: GDELT Masterfile is empty or unreachable.")
    
    # Select the latest N files based on your CONFIGURATION setting
    urls = sorted(all_urls, key=lambda x: x.split("/")[-1])[-NUMBER_OF_TRANSLATION_FILES_TO_PROCESS_PER_RUN:]
    
    print(f"Queueing {len(urls)} recent files for this execution.")

    # ---------------------------------------------------------
    # STEP 3: Batch Processing & Local Database Updates
    # ---------------------------------------------------------
    log_step(3, "Processing GDELT Data Batches")
    batch_count = 0
    for clean_df in iterate_batches(urls):
        events_gdf = gpd.GeoDataFrame(clean_df, geometry=gpd.points_from_xy(clean_df["ActionGeo_Long"], clean_df["ActionGeo_Lat"]), crs="EPSG:4326")
        joined = gpd.sjoin(events_gdf, countries_gdf[["country_code", "geometry"]], how="inner", predicate="within")
        
        if not joined.empty:
            with sqlite3.connect(DATABASE_FILE_PATH) as con:
                db_ready = joined[["GLOBALEVENTID", "timestamp", "country_code", "EventRootCode", "AvgTone", "GoldsteinScale", "NumSources", "NumArticles", "SOURCEURL"]].copy()
                rows = [tuple(x) for x in db_ready.values]
                cur = con.cursor()
                cur.executemany("""
                    INSERT INTO events_log VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(GLOBALEVENTID) DO UPDATE SET 
                        NumArticles = MAX(NumArticles, excluded.NumArticles), 
                        NumSources = MAX(NumSources, excluded.NumSources)
                """, rows)
                con.commit()
        
        batch_count += 1
        if batch_count >= 1:
            print(f"Progress Update: Completed {batch_count} processing sub-batches.")

    # ---------------------------------------------------------
    # STEP 4: Synchronizing Dashboard Widgets
    # ---------------------------------------------------------
    log_step(4, "Synchronizing Dashboard Widgets")
    with sqlite3.connect(DATABASE_FILE_PATH) as con:
        # 4.1: Historical Trends (Daily averages for Line/Bar charts)
        update_historical_summary_table(con, gis)   
        
        # 4.2: Map Polygons (7d/30d/365d Windowed Averages)
        update_map_feature_layer_averages(con, gis) 
        
        # 4.3: News Waterfall (Top Article Links)
        update_top_reputable_events_table(con, gis)

    # ---------------------------------------------------------
    # STEP 5: Final Benchmarking
    # ---------------------------------------------------------
    duration = (time.time() - start_time) / 60
    log_step(5, f"Pipeline execution complete. (Total Time: {duration:.2f} minutes)")