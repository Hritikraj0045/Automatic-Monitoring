import os
import time
import logging
import threading
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


# ===========================================================
# CONFIG
# ===========================================================
CHROMEDRIVER_PATH = r"C:\Users\hriti\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"scraping_1sec_{datetime.now().strftime('%Y%m%d')}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ===========================================================
# URL DICT
# ===========================================================
url_dict = {
    "Homepage": {
        "url": "https://beta.bseindia.com/",
        "type": "tickervalue",
        "selector": ".tickervalue"
    }
}


# ===========================================================
# DRIVER MANAGER
# ===========================================================
class DriverManager:
    def __init__(self, path=CHROMEDRIVER_PATH):
        self.path = path

    def get_driver(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")

        service = Service(self.path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        return driver


# ===========================================================
# WORKER
# ===========================================================
class Worker:
    def __init__(self, socketio=None, stale_threshold=3):
        self.dm = DriverManager()
        self.socketio = socketio
        self.stale_threshold = stale_threshold
        self.cache = {}     # url → {last_value, stale_count, last_update}

    # ---------------------------
    # Emit with last_value internal only
    # ---------------------------
    def emit(self, checklist, status, last_value=None):
        now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # full payload for logs
        full_payload = {
            "checklist": checklist,
            "status": status,
            "last_update": now_iso,
            "last_value": last_value,      # stored internally
            "tab": "tab1sec"
        }

        # safe payload to UI (no last_value)
        ui_payload = {
            "checklist": checklist,
            "status": status,
            "last_update": now_iso,
            "tab": "tab1sec"
        }

        if self.socketio:
            self.socketio.emit("update_status", ui_payload)

        logging.info(f"EMIT: {full_payload}")

    # ---------------------------
    def read_tickervalue(self, driver, selector):
        try:
            values = driver.execute_script("""
                const els = document.querySelectorAll(arguments[0]);
                return Array.from(els).map(e => e.textContent.trim());
            """, selector)
            return values or []
        except:
            return []

    def read_text(self, driver, selector):
        try:
            return driver.execute_script("""
                const el=document.querySelector(arguments[0]);
                return el ? el.textContent.trim() : null;
            """, selector)
        except:
            return None

    # =======================================================
    # MONITOR LOOP
    # =======================================================
    def monitor(self):
        while True:
            driver = None

            try:
                driver = self.dm.get_driver()

                for checklist, info in url_dict.items():

                    url = info["url"]
                    selector = info["selector"]
                    typ = info["type"]

                    try:
                        driver.get(url)
                        time.sleep(0.4)

                        now_iso = datetime.now().isoformat(sep=" ", timespec="seconds")

                        # --------------------------
                        # tickervalue
                        # --------------------------
                        if typ == "tickervalue":
                            vals = self.read_tickervalue(driver, selector)
                            key = (url, "tick")
                            last = self.cache.get(key, {}).get("last_value")

                            if last is None or vals != last:
                                self.cache[key] = {
                                    "last_value": vals,
                                    "stale_count": 0
                                }
                                self.emit(checklist, "ok", vals)
                            else:
                                self.cache[key]["stale_count"] += 1
                                if self.cache[key]["stale_count"] >= self.stale_threshold:
                                    self.emit(checklist, "stale", vals)

                        # --------------------------
                        # simple value extraction
                        # --------------------------
                        elif typ == "value":
                            txt = self.read_text(driver, selector)
                            key = (url, "val")
                            last = self.cache.get(key, {}).get("last_value")

                            if last is None or txt != last:
                                self.cache[key] = {
                                    "last_value": txt,
                                    "stale_count": 0
                                }
                                self.emit(checklist, "ok", txt)
                            else:
                                self.cache[key]["stale_count"] += 1
                                if self.cache[key]["stale_count"] >= self.stale_threshold:
                                    self.emit(checklist, "stale", txt)

                        # --------------------------
                        # timestamp based
                        # --------------------------
                        elif typ == "timestamp":
                            txt = (self.read_text(driver, selector) or "").lower().replace("as on", "").strip()
                            key = (url, "ts")
                            last = self.cache.get(key, {}).get("last_value")

                            if "|" in txt:
                                if last is None or txt != last:
                                    self.cache[key] = {
                                        "last_value": txt,
                                        "stale_count": 0
                                    }
                                    self.emit(checklist, "ok", txt)
                                else:
                                    self.cache[key]["stale_count"] += 1
                                    if self.cache[key]["stale_count"] >= self.stale_threshold:
                                        self.emit(checklist, "stale", txt)
                            else:
                                self.emit(checklist, "invalid format", txt)

                        else:
                            self.emit(checklist, "ok")

                    except Exception as e:
                        logging.error(f"[{checklist}] fetch error: {e}")
                        self.emit(checklist, "error")

                time.sleep(1)

            except Exception as e:
                logging.error(f"Worker error: {e}")
                time.sleep(5)

            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass


# ===========================================================
# THREAD START
# ===========================================================
def start_threads(socketio=None):
    worker = Worker(socketio)
    threading.Thread(target=worker.monitor, daemon=True).start()
    logging.info("Started 1-sec worker thread.")
