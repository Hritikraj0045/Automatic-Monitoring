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
from selenium.common.exceptions import TimeoutException, WebDriverException
from flask_socketio import SocketIO
 
# ================================================================
# CONFIGURATION
# ================================================================
CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.134\chromedriver-win32\chromedriver.exe"
 
url_dict = {
    "Gainers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
        "extractor": "gainers",
        "selector": "span.resizable-font"
    },
    "Losers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
        "extractor": "losers",
        "selector": "span.resizable-font"
    },
    "Spurt in Volume": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/Missprtvol",
        "extractor":"spurt",
        "selector": "span.resizable-font"
    },
    "Illiquid Scrips": {
        "url": "https://beta.bseindia.com/markets/Equity/EQReports/Illiquid_Scrips",
        "extractor": "illiquid",
        "selector": "#ContentPlaceHolder1_lblNoteDate"
    },
    "Circuit Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CFSummary",
        "extractor": "circuit",
        "selector": "span.resizable-font"
    },
    "Circuit Filter": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CircuitFillter",
        "extractor": "circuit_filter",
        "selector": "span.me-2"
    },
    "52 Weeks High": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=H",
        "extractor": "week_high",
        "selector": "span.resizable-font"
    },
    "52 Weeks Low": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=L",
        "extractor": "week_low",
        "selector": "span.resizable-font"
    },
    "Industry Watch": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/industrywatchList",
        "extractor": "ind_watch",
        "selector": "span.resizable-font"
    },
    # "Industry Watch- Heat Map": {
    #     "url": "https://www.bseindia.com/markets/Equity/EQReports/industrywatch.aspx?page=IN020101002&scripname=2/3%20Wheelers",
    #     "extractor": "ind_heatmap",
    #     "selector": ""
    # },
    "Trading Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/TradeSummary",
        "extractor": "trading_summary",
        "selector": "span.resizable-font"
    },
    "Derivatives Chain": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriOptionchain",
        "extractor": "der_chain",
        "selector": "span.me-2.resizable-font"
    },
    "Market Summary": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriArchive_PG/flag/0",
        "extractor": "market_summ",
        "selector": "span.me-2.resizable-font"
    },
    "Index Watch": {
        "url": "https://beta.bseindia.com/sensex/IndexHighlight",
        "extractor": "index_watch",
        "selector": "span.resizable-font"
    }
    # "Corporate Announcements": {
    #     "url": "https://beta.bseindia.com/sensex/IndexHighlight",
    #     "extractor": "corp_ann",
    #     "selector": "span.resizable-font"
    # }
}
 
# ================================================================
# LOGGING SETUP
# ================================================================
LOG_DIR = "logs"
CSV_DIR = "reports"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
 
logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log"),
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
        """Create an isolated ChromeDriver for each thread."""
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
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
        if self.socketio:
            self.socketio.emit("update_status", {"checklist": checklist, "status": status})
 
# ================================================================
# FETCHER CLASS
# ================================================================
class Fetcher:
    def __init__(self, driver_manager: DriverManager, socket_manager: SocketManager):
        self.dm = driver_manager
        self.socket = socket_manager
        self.extractor_map = {
            "gainers": self.fetch_page_timestamp,
            "losers": self.fetch_page_timestamp,
            "spurt": self.fetch_page_timestamp,
            "illiquid": self.fetch_page_timestamp,
            "circuit": self.fetch_page_timestamp,
            "circuit_filter": self.fetch_page_timestamp,
            "week_high": self.fetch_page_timestamp,
            "week_low": self.fetch_page_timestamp,
            "ind_watch": self.fetch_page_timestamp,
            # "ind_heatmap": self.fetch_page_timestamp,
            "trading_summary": self.fetch_page_timestamp,
            "der_chain": self.fetch_page_timestamp,
            "market_summ": self.fetch_page_timestamp,
            "index_watch": self.fetch_page_timestamp
        }
 
    # -----------------------------------------------------------
    # UNIVERSAL FUNCTION FOR BOTH GAINERS & LOSERS
    # -----------------------------------------------------------
    def fetch_page_timestamp(self, driver,checklist_name, selector):
        """Fetch timestamp from any page with 'span.resizable-font'."""
        try:
            WebDriverWait(driver, 25).until(
                lambda d: "|" in d.find_element(By.CSS_SELECTOR, selector).text
            )
            raw_text = driver.execute_script(f"""
                return document.querySelector("{selector}")?.textContent.trim();
            """)
            if raw_text and "|" in raw_text:
                logging.info(f"[FETCH] {checklist_name} Raw timestamp: {raw_text}")
                return {"timestamp": raw_text}
            else:
                logging.warning(f"[FETCH] {checklist_name} Incomplete or invalid timestamp: {raw_text}")
                return {"timestamp": None}
        except TimeoutException:
            logging.error(f"[TIMEOUT] {checklist_name} Waiting for timestamp failed.")
            return {"timestamp": None}
        except Exception as e:
            logging.error(f"[ERROR] {checklist_name} fetch_page_timestamp(): {e}")
            return {"timestamp": None}
 
    # -----------------------------------------------------------
    # MONITOR FUNCTION
    # -----------------------------------------------------------
    def monitor_url(self, checklist_name, url, extractor_key, selector):
        """Monitor the page every minute using an independent ChromeDriver."""
        last_time = None
        last_update_time = None
        stale_count = 0
        today_date = datetime.now().strftime("%d %b %Y")
        
 
        while True:
            driver = None
            try:
                driver = self.dm.get_driver()
                driver.get(url)
                extractor_func = self.extractor_map.get(extractor_key)
                if not extractor_func:
                    logging.error(f"No extractor defined for {checklist_name}")
                    return
 
                # Fetch the timestamp
                data = extractor_func(driver, checklist_name, selector)
                if not data.get("timestamp"):
                    logging.info(f"{checklist_name}: Retrying timestamp fetch in 5 sec...")
                    time.sleep(5)
                    data = extractor_func(driver, checklist_name, selector)
 
                current_timestamp = data.get("timestamp")
                driver.quit()
 
                # --- Parse and Validate Timestamp ---
                if current_timestamp:
                    current_timestamp = current_timestamp.lower().replace("as on", "").strip()
                    if "|" not in current_timestamp:
                        logging.warning(f"Unexpected format: {current_timestamp}")
                        self.socket.emit_update(checklist_name, "invalid format")
                        continue
 
                    date_part, time_part = [x.strip() for x in current_timestamp.split("|", 1)]
                    try:
                        # Normalize date from page
                        if len(date_part.split()[-1]) == 2:  # e.g., "11 Nov 25"
                            page_date = datetime.strptime(date_part, "%d %b %y")
                        else:  # e.g., "11 Nov 2025"
                            page_date = datetime.strptime(date_part, "%d %b %Y")

                                # Today's date normalized
                        today = datetime.now().date()

                        if page_date.date() != today:
                            logging.warning(f"{checklist_name}: Date mismatch ({date_part})")
                            self.socket.emit_update(checklist_name, f"date mismatch - {date_part}")
                            continue
                    except ValueError:
                        logging.error(f"Invalid date format: {date_part}")
                        self.socket.emit_update(checklist_name, "invalid date")
                        continue
 
                    # --- Update vs Stale Detection ---
                    if time_part != last_time:
                        stale_count = 0
                        last_time = time_part
                        last_update_time = datetime.now()
                        self.socket.emit_update(checklist_name, f"ok - {time_part}")
                        logging.info(f"{checklist_name} updated: {time_part}")
                    else:
                        stale_count += 1
                        if stale_count >= 3:  # 3-minute stale
                            stale_duration = (datetime.now() - last_update_time).total_seconds()
                            self.socket.emit_update(checklist_name, f"stale - {time_part}")
                            self.write_to_csv(checklist_name, url)
                            logging.warning(f"{checklist_name} stale for {stale_duration}.")
                else:
                    logging.warning(f"{checklist_name}: No timestamp fetched.")
 
                time.sleep(60)  # Wait 1 minute
 
            except (TimeoutException, WebDriverException) as e:
                logging.error(f"[{checklist_name}] WebDriver error: {e}")
                if driver:
                    driver.quit()
                time.sleep(30)
            except Exception as e:
                logging.error(f"Unexpected error in {checklist_name}: {e}")
                if driver:
                    driver.quit()
                time.sleep(30)
 
    # -----------------------------------------------------------
    # CSV Logger
    # -----------------------------------------------------------
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
            args=(checklist, info["url"], info["extractor"], info["selector"]),
            daemon=True
        )
        t.start()
        logging.info(f"Started thread for {checklist}")
 
    logging.info("✅ All 1-minute monitoring threads launched successfully.")
 
# ================================================================
# MAIN EXECUTION
# ================================================================
if __name__ == "__main__":
    start_threads()
    while True:
        time.sleep(60)













# import os
# import logging
# import csv
# import time
# import threading
# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException, WebDriverException
# from flask_socketio import SocketIO
 
# # ================================================================
# # CONFIGURATION
# # ================================================================
# CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.134\chromedriver-win32\chromedriver.exe"
 
# url_dict = {
#     "Gainers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
#         "extractor": "gainers"
#     },
#     "Losers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
#         "extractor": "losers"
#     }
# }
 
# # ================================================================
# # LOGGING SETUP
# # ================================================================
# LOG_DIR = "logs"
# CSV_DIR = "reports"
# os.makedirs(LOG_DIR, exist_ok=True)
# os.makedirs(CSV_DIR, exist_ok=True)
 
# logging.basicConfig(
#     filename=os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log"),
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
 
# # ================================================================
# # DRIVER MANAGER
# # ================================================================
# class DriverManager:
#     def __init__(self, path=CHROMEDRIVER_PATH):
#         self.path = path
 
#     def get_driver(self):
#         options = Options()
#         options.add_argument("--headless=new")
#         options.add_argument("--no-sandbox")
#         options.add_argument("--disable-dev-shm-usage")
#         options.add_argument("--disable-gpu")
#         options.add_argument("--window-size=1920,1080")
#         service = Service(self.path)
#         driver = webdriver.Chrome(service=service, options=options)
#         driver.set_page_load_timeout(25)
#         return driver
 
# # ================================================================
# # SOCKET MANAGER
# # ================================================================
# class SocketManager:
#     def __init__(self, socketio=None):
#         self.socketio = socketio
 
#     def emit_update(self, checklist, status):
#         if self.socketio:
#             self.socketio.emit("update_status", {"checklist": checklist, "status": status})
 
# # ================================================================
# # FETCHER
# # ================================================================
# class Fetcher:
#     def __init__(self, driver_manager: DriverManager, socket_manager: SocketManager):
#         self.dm = driver_manager
#         self.socket = socket_manager
#         self.extractor_map = {"gainers": self.fetch_gainers,"losers":self.fetch_losers}
 
#     # -----------------------------
#     # Extractor Function
#     # -----------------------------

#     #Gainers
#     def fetch_gainers(self, driver):
#         """Extract timestamp from Gainers page reliably."""
#         try:
#             # Wait for timestamp text to include "|"
#             WebDriverWait(driver, 25).until(
#                 lambda d: "|" in d.find_element(By.CSS_SELECTOR, "span.resizable-font").text
#             )
 
#             raw_text = driver.execute_script("""
#                 return document.querySelector('span.resizable-font')?.textContent.trim();
#             """)
 
#             # Validate timestamp
#             if raw_text and "|" in raw_text:
#                 logging.info(f"[FETCH] Raw timestamp: {raw_text}")
#                 return {"timestamp": raw_text}
#             else:
#                 logging.warning(f"[FETCH] Timestamp incomplete or invalid: {raw_text}")
#                 return {"timestamp": None}
 
#         except TimeoutException:
#             logging.error("[TIMEOUT] Waiting for complete timestamp failed (no '|')")
#             return {"timestamp": None}
#         except Exception as e:
#             logging.error(f"[ERROR] fetch_gainers(): {e}")
#             return {"timestamp": None}

#     #LOSERS
#     def fetch_losers(self, driver):
#         """Extract timestamp from Losers page reliably."""
#         try:
#             # Wait for timestamp text to include "|"
#             WebDriverWait(driver, 25).until(
#                 lambda d: "|" in d.find_element(By.CSS_SELECTOR, "span.resizable-font").text
#             )
 
#             raw_text = driver.execute_script("""
#                 return document.querySelector('span.resizable-font')?.textContent.trim();
#             """)
 
#             # Validate timestamp
#             if raw_text and "|" in raw_text:
#                 logging.info(f"[FETCH] Raw timestamp: {raw_text}")
#                 return {"timestamp": raw_text}
#             else:
#                 logging.warning(f"[FETCH] Timestamp incomplete or invalid: {raw_text}")
#                 return {"timestamp": None}
 
#         except TimeoutException:
#             logging.error("[TIMEOUT] Waiting for complete timestamp failed (no '|')")
#             return {"timestamp": None}
#         except Exception as e:
#             logging.error(f"[ERROR] fetch_losers(): {e}")
#             return {"timestamp": None}

#     # -----------------------------
#     # Monitor Function (Main Loop)
#     # -----------------------------
#     def monitor_url(self, checklist_name, url, extractor_key):
#         """Continuously monitor given URL every minute."""
#         last_time = None
#         stale_count = 0
#         today_date = datetime.now().strftime("%d %b %Y")
 
#         while True:
#             driver = None
#             try:
#                 driver = self.dm.get_driver()
#                 driver.get(url)
#                 extractor_func = self.extractor_map.get(extractor_key)
 
#                 if not extractor_func:
#                     logging.error(f"No extractor found for {checklist_name}")
#                     return
 
#                 data = extractor_func(driver)
 
#                 # Retry once if missing or incomplete
#                 if not data.get("timestamp"):
#                     logging.info(f"{checklist_name}: Retrying timestamp fetch in 5 sec...")
#                     time.sleep(5)
#                     data = extractor_func(driver)
 
#                 current_timestamp = data.get("timestamp")
#                 driver.quit()
 
#                 if current_timestamp:
#                     # Clean text
#                     current_timestamp = current_timestamp.replace("As on", "").strip()
 
#                     if "|" not in current_timestamp:
#                         logging.warning(f"Unexpected timestamp format: {current_timestamp}")
#                         self.socket.emit_update(checklist_name, "invalid format")
#                     else:
#                         date_part, time_part = [x.strip() for x in current_timestamp.split("|", 1)]
 
#                         if date_part != today_date:
#                             logging.warning(f"{checklist_name}: Date mismatch ({date_part})")
#                             self.socket.emit_update(checklist_name, f"date mismatch - {date_part}")
#                         else:
#                             # Detect update/stale
#                             if time_part != last_time:
#                                 stale_count = 0
#                                 last_time = time_part
#                                 self.socket.emit_update(checklist_name, f"ok - {time_part}")
#                                 logging.info(f"{checklist_name} updated: {time_part}")
#                             else:
#                                 stale_count += 1
#                                 if stale_count >= 3:  # 3-minute stale
#                                     self.socket.emit_update(checklist_name, f"stale - {time_part}")
#                                     self.write_to_csv(checklist_name, url)
#                                     logging.warning(f"{checklist_name} stale for 3 minutes")
#                 else:
#                     logging.warning(f"{checklist_name}: No timestamp fetched.")
 
#                 time.sleep(60)  # Wait 1 minute
 
#             except (TimeoutException, WebDriverException) as e:
#                 logging.error(f"[{checklist_name}] WebDriver error: {e}")
#                 if driver:
#                     driver.quit()
#                 time.sleep(30)
 
#             except Exception as e:
#                 logging.error(f"Unexpected error in {checklist_name}: {e}")
#                 if driver:
#                     driver.quit()
#                 time.sleep(30)
 
#     # -----------------------------
#     # CSV Logger for stale data
#     # -----------------------------
#     def write_to_csv(self, checklist_name, url):
#         csv_path = os.path.join(CSV_DIR, f"stale_report_{datetime.now().date()}.csv")
#         file_exists = os.path.isfile(csv_path)
#         with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
#             writer = csv.writer(f)
#             if not file_exists:
#                 writer.writerow(["Timestamp", "Checklist", "URL", "Status"])
#             writer.writerow([datetime.now(), checklist_name, url, "Not Updated"])
 
# # ================================================================
# # THREAD LAUNCHER
# # ================================================================
# def start_threads(socketio=None):
#     driver_manager = DriverManager()
#     socket_manager = SocketManager(socketio)
#     fetcher = Fetcher(driver_manager, socket_manager)
 
#     for checklist, info in url_dict.items():
#         t = threading.Thread(
#             target=fetcher.monitor_url,
#             args=(checklist, info["url"], info["extractor"]),
#             daemon=True
#         )
#         t.start()
#         logging.info(f"Started thread for {checklist}")
 
#     logging.info("✅ All 1-minute monitoring threads launched successfully.")
 
# # ================================================================
# # MAIN EXECUTION
# # ================================================================
# if __name__ == "__main__":
#     start_threads()
#     while True:
#         time.sleep(60)



