import os
import logging
import csv
import time
import threading
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from flask_socketio import SocketIO
 
# ================================================================
# CONFIGURATION
# ================================================================
CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\141.0.7390.76\chromedriver-win32\chromedriver.exe"
 
url_dict = {
    "Homepage": {
        "url": "https://beta.bseindia.com/",
        "extractor": "sensex_bankex"
    },
    "BSE Sensex Streamer": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter:gainer*all$all$",
        "extractor": ""
    },
    "BSE Derivatives Streamer": {
        "url": "https://beta.bseindia.com/markets/Equity/equitysensexstream",
        "extractor": ""
    },
    "Special Pre-open": {
        "url": "https://beta.bseindia.com/eqstreamer/StockTickerSplPreOpen",
        "extractor": ""
    },
    "Market Watch (Equity T+1)": {
        "url": "https://beta.bseindia.com/eqstreamer/StreamerMarketwatch?flag:1",
        "extractor": ""
    },
    "Market Watch (T+0)": {
        "url": "https://beta.bseindia.com/eqstreamer/StreamerMarketwatch?flag:1",
        "extractor": ""
    },
    "Market Watch (Derivatives)": {
        "url": "https://beta.bseindia.com/eqstreamer/StreamerMarketwatch?flag:1",
        "extractor": ""
    },
    "Index Watch": {
        "url": "https://beta.bseindia.com/eqstreamer/StreamerMarketwatch?flag:1",
        "extractor": ""
    },
    "Currency Watch": {
        "url": "https://beta.bseindia.com/eqstreamer/StreamerMarketwatch?flag=1",
        "extractor": ""
    }
}
 
# ================================================================
# LOGGING SETUP
# ================================================================
LOG_DIR = "logs"
CSV_DIR = "reports"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
 
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "scraping_1sec.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
# ================================================================
# DRIVER MANAGER
# ================================================================
class DriverManager:
    def __init__(self, path=CHROMEDRIVER_PATH):
        self.path = path
 
    def get_driver(self):
        """Create ChromeDriver instance with stable options."""
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        service = Service(self.path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(25)
        return driver
 
# ================================================================
# SOCKET MANAGER
# ================================================================
class SocketManager:
    def __init__(self, socketio=None):
        self.socketio = socketio
 
    def emit_update(self, checklist, status):
        """Send real-time updates to UI via SocketIO (if available)."""
        if self.socketio:
            self.socketio.emit("update_status", {"checklist": checklist, "status": status})
 
# ================================================================
# FETCHER
# ================================================================
class Fetcher:
    def __init__(self, driver_manager: DriverManager, socket_manager: SocketManager):
        self.dm = driver_manager
        self.socket = socket_manager
        self.data_cache = {}
        self.extractor_map = {
            "sensex_bankex": self.fetch_sensex_bankex
        }
 
    # -----------------------------
    # Extractor Function
    # -----------------------------
    def fetch_sensex_bankex(self, driver):
        """Extract Sensex and Bankex values (dynamic .tickervalue)."""
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".tickervalue"))
            )
 
            values = driver.execute_script("""
                const els = document.querySelectorAll('.tickervalue');
                return Array.from(els).map(e => e.textContent.trim()).filter(v => v.length > 0);
            """)
 
            sensex_val = values[0] if len(values) > 0 else None
            bankex_val = values[1] if len(values) > 1 else None
            time_stamp = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
 
            logging.info(f"[FETCH] Sensex: {sensex_val}, Bankex: {bankex_val} at {time_stamp}")
            return {"sensex": sensex_val, "bankex": bankex_val, "timestamp": time_stamp}
 
        except Exception as e:
            logging.error(f"[FETCH ERROR] {e}")
            return {"sensex": None, "bankex": None, "timestamp": datetime.now().strftime("%d-%b-%Y %H:%M:%S")}
 
    # -----------------------------
    # Monitor Function (Main Loop)
    # -----------------------------
    def monitor_url(self, checklist_name, url, extractor_key):
        """Continuously monitor given URL every second."""
        while True:
            driver = None
            try:
                driver = self.dm.get_driver()
                driver.get(url)
                extractor_func = self.extractor_map.get(extractor_key)
 
                if not extractor_func:
                    logging.warning(f"No extractor for {checklist_name}, skipping...")
                    return
 
                self.data_cache[url] = {"last_value": None, "last_update": time.time()}
                logging.info(f"Started monitoring {checklist_name}")
 
                last_refresh = time.time()
 
                while True:
                    data = extractor_func(driver)
                    current_time = time.time()
 
                    last_vals = self.data_cache[url].get("last_value", {})
                    last_time = self.data_cache[url].get("last_update", current_time)
                    new_sensex = data.get("sensex")
                    new_bankex = data.get("bankex")
 
                    # Initialize cache
                    if not last_vals:
                        self.data_cache[url]["last_value"] = {"sensex": new_sensex, "bankex": new_bankex}
                        self.data_cache[url]["last_update"] = current_time
                        self.socket.emit_update(checklist_name, f"ok - {data['timestamp']}")
 
                    else:
                        updated = False
                        if new_sensex and new_sensex != last_vals.get("sensex"):
                            self.data_cache[url]["last_value"]["sensex"] = new_sensex
                            updated = True
                        if new_bankex and new_bankex != last_vals.get("bankex"):
                            self.data_cache[url]["last_value"]["bankex"] = new_bankex
                            updated = True
 
                        if updated:
                            self.data_cache[url]["last_update"] = current_time
                            self.socket.emit_update(checklist_name, f"ok - {data['timestamp']}")
                        elif current_time - last_time > 3:
                            self.socket.emit_update(checklist_name, f"stale - {data['timestamp']}")
                            self.write_to_csv(checklist_name, url)
                            logging.warning(f"{checklist_name}: No update >3 sec")
 
                    # Auto-refresh every 60 sec
                    if current_time - last_refresh > 60:
                        logging.info(f"Refreshing driver for {checklist_name}")
                        driver.refresh()
                        last_refresh = current_time
 
                    time.sleep(1)
 
            except (WebDriverException, TimeoutException) as e:
                logging.error(f"[{checklist_name}] WebDriver error: {e}")
            except Exception as e:
                logging.error(f"[{checklist_name}] Unexpected error: {e}")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                logging.info(f"[{checklist_name}] Restarting thread in 5 seconds...")
                time.sleep(5)
 
    # -----------------------------
    # Write stale entries to CSV
    # -----------------------------
    def write_to_csv(self, checklist_name, url):
        csv_path = os.path.join(CSV_DIR, f"stale_report_{datetime.now().date()}.csv")
        file_exists = os.path.isfile(csv_path)
        with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Timestamp", "Checklist", "URL", "Status"])
            writer.writerow([datetime.now(), checklist_name, url, "Not Updated"])
 
# ================================================================
# THREAD LAUNCHER
# ================================================================
def start_threads(socketio=None):
    driver_manager = DriverManager()
    socket_manager = SocketManager(socketio)
    fetcher = Fetcher(driver_manager, socket_manager)
 
    for checklist, info in url_dict.items():
        t = threading.Thread(
            target=fetcher.monitor_url,
            args=(checklist, info["url"], info["extractor"]),
            daemon=True
        )
        t.start()
        logging.info(f"Started thread for {checklist}")
 
    logging.info("✅ All monitoring threads launched successfully.")
 
# ================================================================
# MAIN EXECUTION
# ================================================================
if __name__ == "__main__":
    start_threads()
    while True:
        time.sleep(60)