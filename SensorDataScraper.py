import json
import time
import threading
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import Dict, List, Any
import logging
import os

# Set all internal file reads/writes to be relative to /tmp
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Runtime storage (always updated here)
SENSOR_DATA_FILE = os.path.join("/tmp", "sensor_data.json")
CSV_FILE_PATH = os.path.join("/tmp", "sensor_data.csv")

# Fallback copy from your repo (read-only)
DEFAULT_SENSOR_DATA_FILE = os.path.join(BASE_DIR, "sensor_data.json")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# FastAPI Web App
app = FastAPI(title="Flood Data Scraper API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

SENSOR_CATEGORIES = {
    "rain_gauge": [
        "QCPU", "Masambong", "Batasan Hills", "Ugong Norte", "Ramon Magsaysay HS",
        "UP Village", "Dona Imelda", "Kaligayahan", "Emilio Jacinto Sr HS", "Payatas ES",
        "Ramon Magsaysay Brgy Hall", "Phil-Am", "Holy Spirit", "Libis",
        "South Triangle", "Nagkaisang Nayon", "Tandang Sora", "Talipapa",
        "Balingasa High School", "Toro Hills Elementary School", "Quezon City University San Francisco Campus", 
        "Maharlika Brgy Hall", "Bagong Silangan Evacuation Center", "Dona Juana Elementary School",
        "Quirino High School", "Old Balara Elementary School", "Pansol Kaingin 1 Brgy Satellite Office",
        "Jose P Laurel Senior High School", "Pinyahan Multipurose Hall", "Sikatuna Brgy Hall",
        "Kalusugan Brgy Hall", "Laging Handa Barangay Hall", "Amoranto Sport Complex", "Maligaya High School",
        "San Agustin Brgy Hall", "Jose Maria Panganiban Senior High School", "North Fairview Elementary School",
        "Sauyo Elementary School", "New Era Brgy Hall", "Ismael Mathay Senior High School"
    ],
    "flood_sensors": [
        "North Fairview", "Batasan-San Mateo", "Bahay Toro", "Sta Cruz", "San Bartolome"
    ],
    "street_flood_sensors": [
        "N.S. Amoranto Street", "New Greenland", "Kalantiaw Street", "F. Calderon Street",
        "Christine Street", "Ramon Magsaysay Brgy Hall", "Phil-Am", "Holy Spirit",
        "Libis", "South Triangle", "Nagkaisang Nayon", "Tandang Sora", "Talipapa"
    ],
    "flood_risk_index": [
        "N.S. Amoranto Street", "New Greenland", "Kalantiaw Street", "F. Calderon Street",
        "Christine Street", "Ramon Magsaysay Brgy Hall", "Phil-Am", "Holy Spirit",
        "Libis", "South Triangle", "Nagkaisang Nayon", "Tandang Sora", "Talipapa"
    ],
    "earthquake_sensors": ["QCDRRMO", "QCDRRMO REC"]
}


def setup_chrome_driver():
    """Setup Chrome WebDriver with proper options and error handling"""
    try:
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium"
        chrome_options.add_argument("--headless=new")  # Use new headless mode
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        chrome_options.add_argument("--disable-features=NetworkService,NetworkServiceInProcess,NetworkServiceInProcess2")
        chrome_options.add_argument("--disable-gpu-sandbox")
        chrome_options.add_argument("--disable-accelerated-2d-canvas")
        chrome_options.add_argument("--disable-accelerated-jpeg-decoding")
        chrome_options.add_argument("--disable-accelerated-mjpeg-decode")
        chrome_options.add_argument("--disable-accelerated-video-decode")
        chrome_options.add_argument("--disable-accelerated-video-encode")
        chrome_options.add_argument("--disable-webgl")
        chrome_options.add_argument("--disable-webgl2")
        chrome_options.add_argument("--disable-3d-apis")

        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(30)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize Chrome WebDriver: {str(e)}")
        raise


def wait_for_page_load(driver, url, max_retries=3):
    """Wait for page to load with retry logic"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1} to load page: {url}")
            driver.get(url)
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(5)
    return False


def scrape_sensor_data():
    driver = None
    try:
        logger.info("Initializing Chrome WebDriver...")
        driver = setup_chrome_driver()
        url = "https://web.iriseup.ph/sensor_networks"
        logger.info(f"🌍 Fetching data from: {url}")

        if not wait_for_page_load(driver, url):
            raise TimeoutError("Failed to load page after multiple attempts")

        # ✅ Initialize after successful page load
        sensor_data = []

        # --- Rain Gauge Table (1st table) ---
        rain_rows = driver.find_elements(By.XPATH, "(//table)[1]//tbody//tr")
        for row in rain_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 4:
                sensor_data.append({
                    "CATEGORY": "rain_gauge",
                    "SENSOR NAME": cols[0].text.strip(),
                    "OBS TIME": cols[1].text.strip(),
                    "NORMAL LEVEL": cols[3].text.strip(),
                    "CURRENT": cols[2].text.strip()
                })

        # --- Rain Gauge Nowcast Table (2nd table) ---
        nowcast_rows = driver.find_elements(By.XPATH, "(//table)[2]//tbody//tr")
        for row in nowcast_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 2:
                sensor_data.append({
                    "CATEGORY": "rain_gauge_nowcast",
                    "SENSOR NAME": cols[0].text.strip(),
                    "CURRENT": cols[1].text.strip()
                })

        # --- Flood Sensors Table (3rd table) ---
        flood_rows = driver.find_elements(By.XPATH, "(//table)[3]//tbody//tr")
        for row in flood_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                sensor_data.append({
                    "CATEGORY": "flood_sensors",
                    "SENSOR NAME": cols[0].text.strip(),
                    "NORMAL LEVEL": cols[2].text.strip(),
                    "CURRENT": cols[3].text.strip()
                })

        # --- Street Flood Table (4th table) ---
        street_rows = driver.find_elements(By.XPATH, "(//table)[4]//tbody//tr")
        for row in street_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 5:
                sensor_data.append({
                    "CATEGORY": "street_flood_sensors",
                    "SENSOR NAME": cols[0].text.strip(),
                    "NORMAL LEVEL": cols[2].text.strip(),
                    "CURRENT": cols[3].text.strip(),
                    "DESCRIPTION": cols[4].text.strip()
                })

        # --- Flood Risk Index Table (5th table) ---
        risk_rows = driver.find_elements(By.XPATH, "(//table)[5]//tbody//tr")
        for row in risk_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 4:
                sensor_data.append({
                    "CATEGORY": "flood_risk_index",
                    "SENSOR NAME": cols[0].text.strip(),
                    "OBS TIME": cols[1].text.strip(),
                    "NORMAL LEVEL": cols[3].text.strip(),
                    "CURRENT": cols[2].text.strip()
                })

        # --- River Flow Sensor Table (6th table) ---
        river_rows = driver.find_elements(By.XPATH, "(//table)[6]//tbody//tr")
        for row in river_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                sensor_data.append({
                    "CATEGORY": "river_flow_sensor",
                    "SENSOR NAME": cols[0].text.strip(),
                    "NORMAL LEVEL": cols[3].text.strip(),
                    "CURRENT": cols[2].text.strip()
                })

        # --- Earthquake Sensors Table (7th table) ---
        eq_rows = driver.find_elements(By.XPATH, "(//table)[7]//tbody//tr")
        for row in eq_rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                sensor_data.append({
                    "CATEGORY": "earthquake_sensors",
                    "SENSOR NAME": cols[0].text.strip(),
                    "OBS TIME": cols[1].text.strip(),
                    "CURRENT": cols[2].text.strip()
                })

        # ✅ This should run after ALL scraping, not inside last loop
        if not sensor_data:
            raise ValueError("No sensor data extracted. Check website structure.")

        logger.info(f"✅ Successfully scraped {len(sensor_data)} sensor records")
        save_csv(sensor_data)
        convert_csv_to_json()
        logger.info("✅ Sensor data updated successfully")

    except Exception as e:
        logger.error(f"❌ Scraping Failed: {str(e)}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"Error closing WebDriver: {str(e)}")

def save_csv(sensor_data):
    df = pd.DataFrame(sensor_data)
    df.to_csv(CSV_FILE_PATH, index=False)
    print("✅ CSV file saved successfully with all sensor data.")


def convert_csv_to_json():
    df = pd.read_csv(CSV_FILE_PATH)

    categorized = {
        "rain_gauge": [],   # merged rain_gauge + rain_gauge_nowcast
        "flood_sensors": [],
        "street_flood_sensors": [],
        "flood_risk_index": [],
        "river_flow_sensor": [],
        "earthquake_sensors": []
    }

    for _, row in df.iterrows():
        category = row["CATEGORY"]

        # --- Rain Gauge + Nowcast merged (only SENSOR NAME + CURRENT) ---
        if category in ["rain_gauge", "rain_gauge_nowcast"]:
            categorized["rain_gauge"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "CURRENT": row.get("CURRENT", "")
            })

        elif category == "flood_sensors":
            categorized["flood_sensors"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "NORMAL LEVEL": row.get("NORMAL LEVEL", ""),
                "CURRENT": row.get("CURRENT", "")
            })

        elif category == "street_flood_sensors":
            categorized["street_flood_sensors"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "NORMAL LEVEL": row.get("NORMAL LEVEL", ""),
                "CURRENT": row.get("CURRENT", ""),
                "DESCRIPTION": row.get("DESCRIPTION", "")
            })

        elif category == "flood_risk_index":
            categorized["flood_risk_index"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "NORMAL LEVEL": row.get("NORMAL LEVEL", ""),
                "CURRENT": row.get("CURRENT", "")
            })

        elif category == "river_flow_sensor":
            categorized["river_flow_sensor"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "NORMAL LEVEL": row.get("NORMAL LEVEL", ""),
                "CURRENT": row.get("CURRENT", "")
            })

        elif category == "earthquake_sensors":
            categorized["earthquake_sensors"].append({
                "SENSOR NAME": row["SENSOR NAME"],
                "CURRENT": row.get("CURRENT", "")
            })

    # Save to JSON
    with open(SENSOR_DATA_FILE, "w") as f:
        json.dump(categorized, f, indent=4)

    logger.info("✅ JSON file updated (rain_gauge + rain_gauge_nowcast merged, only SENSOR NAME + CURRENT)")


@app.get("/api/sensor-data")
async def get_sensor_data():
    if os.path.exists(SENSOR_DATA_FILE):
        with open(SENSOR_DATA_FILE, "r") as f:
            return json.load(f)
    raise HTTPException(status_code=404, detail="Sensor data not available")


def start_auto_scraper():
    while True:
        print("🔄 Running data scraper...")
        try:
            scrape_sensor_data()
        except Exception as e:
            logger.error(f"Error in background scraper: {e}")
        print("⏳ Waiting 60 seconds before the next scrape...")
        time.sleep(60)


# ✅ Ensure sensor_data.json exists on startup
try:
    if not os.path.exists(SENSOR_DATA_FILE):
        print("⚡ No runtime sensor_data.json found, running initial scrape...")
        try:
            scrape_sensor_data()  # try live scrape
        except Exception as scrape_error:
            logger.error(f"❌ Initial scrape failed: {scrape_error}")

            if os.path.exists(DEFAULT_SENSOR_DATA_FILE):
                import shutil
                shutil.copy(DEFAULT_SENSOR_DATA_FILE, SENSOR_DATA_FILE)
                print("📦 Copied fallback sensor_data.json from repo to /tmp.")
            else:
                # Create empty JSON so API won’t crash
                with open(SENSOR_DATA_FILE, "w") as f:
                    json.dump({key: [] for key in SENSOR_CATEGORIES.keys()}, f, indent=4)
                print("⚠️ No repo fallback found. Created empty /tmp/sensor_data.json.")
except Exception as e:
    print(f"Error in startup data init: {e}")

# ✅ Start background scraper thread
scraper_thread = threading.Thread(target=start_auto_scraper, daemon=True)
scraper_thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=False)
