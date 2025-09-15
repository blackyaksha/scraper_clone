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

SENSOR_DATA_FILE = os.path.join("/tmp", "sensor_data.json")
CSV_FILE_PATH = os.path.join("/tmp", "sensor_data.csv")
DEFAULT_SENSOR_DATA_FILE = os.path.join(BASE_DIR, "sensor_data.json")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Flood Data Scraper API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sensor categories
SENSOR_CATEGORIES = {
    "rain_gauge": [
        "QCPU", "Masambong", "Batasan Hills", "Ugong Norte", "Ramon Magsaysay HS",
        "UP Village", "Dona Imelda", "Kaligayahan", "Emilio Jacinto Sr HS", "Payatas ES",
        "Ramon Magsaysay Brgy Hall", "Phil-Am", "Holy Spirit", "Libis", "South Triangle",
        "Nagkaisang Nayon", "Tandang Sora", "Talipapa", "Balingasa High School",
        "Toro Hills Elementary School", "Quezon City University San Francisco Campus", 
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

# ------------------- Chrome WebDriver Setup -------------------
def setup_chrome_driver():
    try:
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium"
        chrome_options.add_argument("--headless=new")
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

# ------------------- Scraper Function -------------------
def scrape_sensor_data():
    driver = None
    try:
        logger.info("Initializing Chrome WebDriver...")
        driver = setup_chrome_driver()
        url = "https://web.iriseup.ph/sensor_networks"
        logger.info(f"🌍 Fetching data from: {url}")

        if not wait_for_page_load(driver, url):
            raise TimeoutError("Failed to load page after multiple attempts")

        # Dynamic headers
        headers = [th.text.strip().upper() for th in driver.find_elements(By.CSS_SELECTOR, "table thead th")]
        logger.info(f"Detected table headers: {headers}")

        sensor_data = []
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= len(headers):
                row_data = {headers[i]: cols[i].text.strip() for i in range(len(headers))}
                sensor_data.append({
                    "SENSOR NAME": row_data.get("SENSOR NAME", ""),
                    "OBS TIME": row_data.get("OBS TIME", ""),
                    "NORMAL LEVEL": row_data.get("NORMAL LEVEL", ""),
                    "CURRENT": row_data.get("CURRENT", ""),
                    "DESCRIPTION": row_data.get("DESCRIPTION", "N/A")
                })

        if not sensor_data:
            raise ValueError("No sensor data extracted. Check website structure.")

        logger.info(f"✅ Successfully scraped {len(sensor_data)} sensor records")
        save_csv(sensor_data)
        convert_csv_to_json(sensor_data)
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

# ------------------- CSV / JSON Save -------------------
def save_csv(sensor_data):
    df = pd.DataFrame(sensor_data)
    df.to_csv(CSV_FILE_PATH, index=False)
    print("✅ CSV file saved successfully with all sensor data.")

def convert_csv_to_json(sensor_data: List[Dict[str, Any]]):
    categorized_data = {category: [] for category in SENSOR_CATEGORIES}

    for row in sensor_data:
        sensor_name = row.get("SENSOR NAME", "").strip()
        current_value = row.get("CURRENT", "N/A")
        normal_value = row.get("NORMAL LEVEL", "N/A")
        description = row.get("DESCRIPTION", "N/A")

        if sensor_name in SENSOR_CATEGORIES["rain_gauge"]:
            categorized_data["rain_gauge"].append({
                "SENSOR NAME": sensor_name,
                "CURRENT": current_value
            })
        elif sensor_name in SENSOR_CATEGORIES["flood_sensors"]:
            categorized_data["flood_sensors"].append({
                "SENSOR NAME": sensor_name,
                "NORMAL LEVEL": normal_value,
                "CURRENT": current_value
            })
        elif sensor_name in SENSOR_CATEGORIES["street_flood_sensors"]:
            categorized_data["street_flood_sensors"].append({
                "SENSOR NAME": sensor_name,
                "NORMAL LEVEL": normal_value,
                "CURRENT": current_value,
                "DESCRIPTION": description
            })
        elif sensor_name in SENSOR_CATEGORIES["flood_risk_index"]:
            categorized_data["flood_risk_index"].append({
                "SENSOR NAME": sensor_name,
                "CURRENT": current_value,
                "RISK INDEX": normal_value
            })
        elif sensor_name in SENSOR_CATEGORIES["earthquake_sensors"]:
            categorized_data["earthquake_sensors"].append({
                "SENSOR NAME": sensor_name,
                "CURRENT": current_value
            })

    # Save JSON
    with open(SENSOR_DATA_FILE, "w") as f:
        json.dump(categorized_data, f, indent=4)
    print("✅ JSON data structured correctly with hardcoded formats.")

    # Save CSV with category column
    csv_rows = []
    for category, sensors in categorized_data.items():
        for s in sensors:
            row = {"CATEGORY": category}
            row.update(s)
            csv_rows.append(row)

    csv_df = pd.DataFrame(csv_rows)
    csv_df.to_csv(CSV_FILE_PATH, index=False)
    print("✅ CSV file saved with category-based arrangement.")

# ------------------- API Endpoint -------------------
@app.get("/api/sensor-data")
async def get_sensor_data():
    if os.path.exists(SENSOR_DATA_FILE):
        with open(SENSOR_DATA_FILE, "r") as f:
            return json.load(f)
    raise HTTPException(status_code=404, detail="Sensor data not available")

# ------------------- Background Scraper -------------------
def start_auto_scraper():
    while True:
        print("🔄 Running data scraper...")
        try:
            scrape_sensor_data()
        except Exception as e:
            logger.error(f"Error in background scraper: {e}")
        print("⏳ Waiting 60 seconds before the next scrape...")
        time.sleep(60)

# ------------------- Startup -------------------
try:
    if not os.path.exists(SENSOR_DATA_FILE):
        print("⚡ No runtime sensor_data.json found, running initial scrape...")
        try:
            scrape_sensor_data()
        except Exception as scrape_error:
            logger.error(f"❌ Initial scrape failed: {scrape_error}")
            if os.path.exists(DEFAULT_SENSOR_DATA_FILE):
                import shutil
                shutil.copy(DEFAULT_SENSOR_DATA_FILE, SENSOR_DATA_FILE)
                print("📦 Copied fallback sensor_data.json from repo to /tmp.")
            else:
                with open(SENSOR_DATA_FILE, "w") as f:
                    json.dump({key: [] for key in SENSOR_CATEGORIES.keys()}, f, indent=4)
                print("⚠️ No repo fallback found. Created empty /tmp/sensor_data.json.")
except Exception as e:
    print(f"Error in startup data init: {e}")

# Start scraper thread
scraper_thread = threading.Thread(target=start_auto_scraper, daemon=True)
scraper_thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=False)
