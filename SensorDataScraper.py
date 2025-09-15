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

        sensor_data = []
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 5:
                sensor_name = cols[0].text.strip()
                location = cols[1].text.strip()
                current_level = cols[3].text.strip()
                normal_level = cols[2].text.strip()
                description = cols[4].text.strip() if len(cols) > 4 else "N/A"
                sensor_data.append({
                    "SENSOR NAME": sensor_name,
                    "OBS TIME": location,
                    "NORMAL LEVEL": normal_level,
                    "CURRENT": current_level,
                    "DESCRIPTION": description
                })

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

    # Hardcoded schema rules (OBS TIME dropped where not needed)
    category_schemas = {
        "rain_gauge": ["SENSOR NAME", "CURRENT"],  # no obs time, no normal
        "flood_sensors": ["SENSOR NAME", "NORMAL LEVEL", "CURRENT", "DESCRIPTION"],
        "street_flood_sensors": ["SENSOR NAME", "NORMAL LEVEL", "CURRENT", "DESCRIPTION"],  # obs time dropped
        "flood_risk_index": ["SENSOR NAME", "CURRENT"],  # obs time dropped
        "earthquake_sensors": ["SENSOR NAME", "CURRENT"],  # obs time dropped
    }

    # JSON structured by category
    categorized_data = {category: [] for category in SENSOR_CATEGORIES}

    # CSV → union of all needed fields + Category column
    all_fields = sorted(set(sum(category_schemas.values(), [])))
    csv_columns = ["Category"] + all_fields
    csv_rows = []

    for category, sensors in SENSOR_CATEGORIES.items():
        for sensor_name in sensors:
            matching_sensor = df[df["SENSOR NAME"].str.casefold() == sensor_name.casefold()]

            if not matching_sensor.empty:
                row = matching_sensor.iloc[0]

                # Build JSON entry (only schema fields)
                sensor_entry = {field: row[field] if field in row else "N/A"
                                for field in category_schemas[category]}
                categorized_data[category].append(sensor_entry)

                # Build CSV entry
                csv_entry = {col: "" for col in csv_columns}
                csv_entry["Category"] = category
                for field in category_schemas[category]:
                    csv_entry[field] = row[field] if field in row else "N/A"
                csv_rows.append(csv_entry)

            else:
                # Defaults if not found
                sensor_entry = {field: "N/A" for field in category_schemas[category]}
                sensor_entry["SENSOR NAME"] = sensor_name
                if category == "street_flood_sensors":
                    sensor_entry["CURRENT"] = "0.0m"
                else:
                    sensor_entry["CURRENT"] = 0.0
                categorized_data[category].append(sensor_entry)

                # CSV defaults
                csv_entry = {col: "" for col in csv_columns}
                csv_entry["Category"] = category
                for field in category_schemas[category]:
                    csv_entry[field] = sensor_entry[field]
                csv_rows.append(csv_entry)

    # Save JSON
    with open(SENSOR_DATA_FILE, "w") as f:
        json.dump(categorized_data, f, indent=4)
    print("✅ JSON file saved with strict category-based schema.")

    # Save single CSV
    df_csv = pd.DataFrame(csv_rows, columns=csv_columns)
    df_csv.to_csv(CSV_FILE_PATH, index=False)
    print("✅ Single CSV file saved with strict category-based schema.")

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
