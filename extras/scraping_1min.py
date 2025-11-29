# scraping_1min.py
import os, time, logging, threading
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

# ============================================================
# CONFIG
# ============================================================
CHROMEDRIVER_PATH = r"C:\Users\hriti\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# URL DICTIONARY (timestamp based pages)
# ============================================================
url_dict = {
    "Gainers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Losers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Spurt in Volume": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/Missprtvol",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Illiquid Scrips": {
        "url": "https://beta.bseindia.com/markets/Equity/EQReports/Illiquid_Scrips",
        "type": "timestamp",
        "selector": "#ContentPlaceHolder1_lblNoteDate"
    },
    "Circuit Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CFSummary",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Circuit Filter": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CircuitFillter",
        "type": "timestamp",
        "selector": "span.me-2"
    },
    "52 Weeks High": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=H",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "52 Weeks Low": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=L",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Industry Watch": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/industrywatchList",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Trading Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/TradeSummary",
        "type": "timestamp",
        "selector": "span.resizable-font"
    },
    "Derivatives Chain": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriOptionchain",
        "type": "timestamp",
        "selector": "span.me-2.resizable-font"
    },
    "Market Summary": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriArchive_PG/flag/0",
        "type": "timestamp",
        "selector": "span.me-2.resizable-font"
    },
    "Index Watch": {
        "url": "https://beta.bseindia.com/sensex/IndexHighlight",
        "type": "timestamp",
        "selector": "span.resizable-font"
    }
}

# ============================================================
# DRIVER MANAGER
# ============================================================
class DriverManager:
    def get_driver(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        service = Service(CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(45)
        return driver


# ============================================================
# UNIVERSAL TIMESTAMP EXTRACTOR
# ============================================================
def read_timestamp(driver, selector):
    """
    1) Try CSS selector
    2) Try fallback XPaths
    3) Return text containing 'As on'
    """
    try:
        el = driver.find_element(By.CSS_SELECTOR, selector)
        txt = el.text.strip()
        if "As on" in txt:
            return txt
    except:
        pass

    fallback_xpaths = [
        "//span[contains(@class,'resizable-font') and contains(text(),'As on')]",
        "//span[contains(@class,'me-2') and contains(text(),'As on')]",
        "//*[@id='ContentPlaceHolder1_lblNoteDate' and contains(text(),'As on')]",
        "(//*[contains(text(),'As on')])[1]"
    ]

    for xp in fallback_xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            txt = el.text.strip()
            if "As on" in txt:
                return txt
        except:
            pass

    return None


# ============================================================
# WORKER CLASS
# ============================================================
class Worker:
    def __init__(self, socketio=None, stale_threshold=3):
        self.dm = DriverManager()
        self.socketio = socketio
        self.stale_threshold = stale_threshold
        self.cache = {}   # key=url → { last_value, stale_count }

    def emit(self, checklist, status):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "checklist": checklist,
            "status": status,
            "last_update": now,
            "tab": "tab1min"
        }

        if self.socketio:
            self.socketio.emit("update_status", payload)

        logging.info(f"EMIT: {payload}")

    def monitor(self):
        next_run = time.time()

        while True:
            # wait until exact minute boundary
            if time.time() < next_run:
                time.sleep(0.3)
                continue

            driver = None
            try:
                driver = self.dm.get_driver()

                for checklist, info in url_dict.items():
                    url = info["url"]
                    selector = info["selector"]

                    try:
                        driver.get(url)
                        time.sleep(1)

                        raw = read_timestamp(driver, selector)
                        if not raw:
                            self.emit(checklist, "invalid format")
                            continue

                        raw_clean = raw.lower().replace("as on", "").strip()
                        key = url
                        last = self.cache.get(key, {}).get("last_value")

                        if last is None or last != raw_clean:
                            # NEW VALUE
                            self.cache[key] = {"last_value": raw_clean, "stale_count": 0}
                            self.emit(checklist, "ok")
                        else:
                            # NO CHANGE → stale counter
                            self.cache[key]["stale_count"] += 1
                            if self.cache[key]["stale_count"] >= self.stale_threshold:
                                self.emit(checklist, "stale")

                    except Exception as e:
                        logging.error(f"[{checklist}] Fetch error: {e}")
                        self.emit(checklist, "error")

            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass

            next_run = time.time() + 60  # RUN EVERY 60 SECONDS


# ============================================================
# THREAD LAUNCHER
# ============================================================
def start_threads(socketio=None):
    worker = Worker(socketio)
    thread = threading.Thread(target=worker.monitor, daemon=True)
    thread.start()
    logging.info("Started 1-min worker thread.")


# ============================================================
# MAIN EXECUTION (only when standalone)
# ============================================================
if __name__ == "__main__":
    start_threads()
    while True:
        time.sleep(60)
