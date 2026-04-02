# GDELT_Processor
Script to process, clean, and publish GDELT data to ArcGIS online layers.

<img width="2547" height="1257" alt="image" src="https://github.com/user-attachments/assets/c844a7f3-ce2a-403a-8366-cee85c26d030" />


## 1. Project Overview
The **Automated GDELT Sentiment Pipeline** ingests near real-time data from the GDELT Project 2.0 Translation Export feed, filters instability-related events, computes statistical sentiment baselines, and publishes analyzed metrics to an ArcGIS Enterprise dashboard.

The system identifies statistically significant “escalations” by comparing the current 24-hour sentiment against a rolling 365-day historical baseline per country.

**Key outputs:**
* **Average Tone:** Raw sentiment score from global news.
* **Z-Score:** Statistical anomaly detection (measuring current deviation from the norm).
* **Rolling Averages:** 7-day, 30-day, and 365-day metrics.
* **News Waterfall:** Top 5 reputable articles per country based on volume and source credibility.

---

## 2. Environment Setup

### 2.1 Clone ArcGIS Pro Python Environment (Required)
Because this pipeline depends on the ArcGIS API and `geopandas` stack, it must run inside a cloned ArcGIS Pro environment.

**Step 1 – Open ArcGIS Pro Python Command Prompt** From Start Menu: `ArcGIS Pro > Python Command Prompt`

**Step 2 – Clone Default Environment**
```bash
conda create --name gdelt_env --clone arcgispro-py3
conda activate gdelt_env
```

**Why clone?**
* Prevents corruption of the ArcGIS Pro base environment.
* Allows safe dependency management.
* Keeps the production environment reproducible.

### 2.2 Required Python Libraries
The following libraries must exist in the environment:

| Library | Purpose |
| :--- | :--- |
| **pandas** | Data manipulation |
| **geopandas** | Spatial joins |
| **sqlite3** | Local database management |
| **arcgis** | Enterprise publishing and GIS analysis |
| **requests** | HTTP downloads for GDELT files |
| **urllib3** | SSL handling |
| **zipfile / io** | File extraction and memory buffering |

**Install missing packages:**
```bash
conda install pandas geopandas
pip install requests urllib3
```

---

## 3. Secure Authentication
### 3.1 Security Design
The script does **not** hardcode login credentials for the ArcGIS Enterprise portal. Credentials are retrieved securely via environment variables to prevent:
* Credential exposure in source control.
* Security audit violations.
* Plaintext password storage.

```python
username = os.environ.get("ARCGIS_USER")
password = os.environ.get("ARCGIS_PASS")
```

### 3.2 Creating Windows Environment Variables

**Temporary (Session Only)**
```cmd
set ARCGIS_USER=your_username
set ARCGIS_PASS=your_password
```

**Persistent (Recommended)**
1.  Open **System Properties** > **Advanced** > **Environment Variables**.
2.  Under **User Variables**, click **New**:
    * **Variable Name:** `ARCGIS_USER`
    * **Variable Value:** `your_username`
3.  Repeat for `ARCGIS_PASS`.
4.  **Restart** your terminal or IDE for changes to take effect.

---

## 4. Database Modes: TRUE vs FALSE
**Configuration Variable:** `REBUILD_DATABASE_BASELINE_FROM_SCRATCH`

* **TRUE**
    * Drops all current database tables (`events_log`, `historical_summary`, `cameo_codes`).
    * Rebuilds database schema and re-ingests baseline data.
    * *Use for:* First-time deployment or full annual rebuilds.
    * **WARNING:** Deletes all existing historical data.

* **FALSE (Operational Mode)**
    * Preserves the database and appends new records.
    * Maintains the rolling 365-day window.
    * *Use for:* Normal hourly or daily operations.

---

## 5. Understanding GDELT File Frequency
Each GDELT translation file represents 15 minutes of global events.

| Files | Time Coverage |
| :--- | :--- |
| 4 | 1 hour |
| 96 | 24 hours |
| 669 | ~7 days |
| 35,040 | ~365 days |

**Formula:** 4 files/hour × 24 hours × 365 days = 35,040 files.

---

## 6. Script Execution Stages

### STEP 1 – Initialization & Authentication
* Initializes SQLite database.
* Authenticates to ArcGIS Enterprise via environment variables.
* Downloads country polygons from Esri and performs sanity checks.

### STEP 2 – GDELT Masterfile Discovery
* Downloads `masterfilelist-translation.txt`.
* Extracts all translation CSV ZIP URLs and sorts them chronologically.
* Selects latest `N` files based on user configuration.

### STEP 3 – Batch Processing & Spatial Join
For each batch, the script:
1.  Downloads and parses the strict 61-column GDELT schema.
2.  Filters **CAMEO codes 10–20** (Instability/Conflict).
3.  Converts coordinates to a **GeoDataFrame**.
4.  **Spatially joins** event points to country polygons.
5.  Inserts data into SQLite using `ON CONFLICT` to ensure **idempotent ingestion** (updates existing records instead of creating duplicates).

### STEP 4 – Dashboard Synchronization
1.  **Historical Summary:** Aggregates daily averages and pushes to the cloud.
2.  **Map Feature Layer:** Calculates rolling averages (7/30/365 day) and updates polygon attributes.
3.  **News Waterfall:** Filters the last 24 hours against a **reputable domain whitelist** and pushes the Top 5 articles per country.

### STEP 5 – Benchmarking
* Calculates and prints total execution time in minutes.

---

## 7. Configuration Section
Adjust these fields within the script to control behavior:

* **`DATABASE_FILE_PATH`**: Location of the SQLite database file.
* **`ARCGIS_ENTERPRISE_PORTAL_URL`**: Your organization's portal endpoint.
* **`NUMBER_OF_TRANSLATION_FILES_TO_PROCESS_PER_RUN`**: Controls ingestion volume (4 = 1 hour).
* **`CAMEO_ROOT_CODE_MINIMUM / MAXIMUM`**: Default is **10–20**. Narrowing this focuses only on severe violence.
* **`MAP_HOSTED_FEATURE_LAYER_ITEM_IDENTIFIER`**: ItemID for the polygon layer that drives the main map.
* **`REPUTABLE_DOMAINS`**: Whitelist for the News Waterfall article validation.
* **`USE_INSECURE_SSL_CERTIFICATE_VERIFICATION`**: Set to `TRUE` only if working within restricted networks that require bypassing SSL.

---

## 8. Operational Modes Summary

| Mode | Files | Purpose |
| :--- | :--- | :--- |
| **Baseline Build** | 35,040 | Build 365-day history from scratch |
| **Daily Repair** | 96 | Backfill a missing 24-hour window |
| **Hourly Heartbeat** | 4 | Standard live update |
| **Weekly Catch-up** | 669 | Syncing after a week of downtime |

---

## 9. Maintenance Notes
* **Pruning:** The database automatically prunes records older than 365 days to maintain performance.
* **Optimization:** To shrink the local `.db` file size, run `VACUUM;` in SQLite.
* **Schema Updates:** If GDELT changes its 61-column schema, `GDELT_HEADERS` must be updated immediately.

---

## 10. Interpretation Guidance
* **Average Tone:** Represents the raw emotional language of coverage, not necessarily verified public opinion.
* **Z-Score:** Standardizes volatility. This answers: *“Is this country behaving abnormally relative to its own historical pattern?”* It does not define absolute instability, but rather relative change.
