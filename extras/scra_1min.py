# scraping_1min.py
import os
import time
import json
import logging
import threading
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# ---------------- CONFIG ----------------
CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"
LOG_DIR = "logs"
STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "monitor_state_1min.json")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

# how many identical minute reads before marking stale
STALE_THRESHOLD = 3  # minutes

# retry settings for invalid-format reads (only for URLs that return invalid)
INVALID_RETRIES = 3
INVALID_RETRY_DELAY = 0.6  # seconds between retries

# ---------------- logger (module-specific file) ----------------
logger = logging.getLogger("scraping_1min")
logger.setLevel(logging.INFO)
logger.propagate = False
log_filename = os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log")
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_filename for h in logger.handlers):
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

# ---------------- UI mapping / url_dict ----------------
ui_name_mapping = {
    "Gainers": "Gainers",
    "Losers": "Losers",
    "Spurt in Volume": "Spurt in Volume",
    "Illiquid Scrips": "Illiquid Scrips",
    "Circuit Summary": "Circuit Summary",
    "Circuit Filter": "Circuit Filter",
    "52 Weeks High": "52 Weeks High",
    "52 Weeks Low": "52 Weeks Low",
    "Industry Watch": "Industry Watch",
    "Industry Watch- Heat Map": "Industry Watch- Heat Map",
    "Trading Summary": "Trading Summary",
    "Derivatives Chain": "Derivatives Chain",
    "Market Summary": "Market Summary",
    "Index Watch": "Index Watch",
}

# Note: selectors / key_ids must match your UI rows
url_dict = {
    "Gainers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
        "type": "timestamp",
        "selector": "span.resizable-font.me-2",
        "tab": "tab1min",
        "key_id": "row-Gainers"
    },
    "Losers": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
        "type": "timestamp",
        "selector": "span.resizable-font.me-2",
        "tab": "tab1min",
        "key_id": "row-Losers"
    },
    "Spurt in Volume": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/Missprtvol",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Spurt-in-Volume"
    },
    "Illiquid Scrips": {
        "url": "https://beta.bseindia.com/markets/Equity/EQReports/Illiquid_Scrips",
        "type": "timestamp",
        "selector": "#ContentPlaceHolder1_lblNoteDate",
        "tab": "tab1min",
        "key_id": "row-Illiquid-Scrips"
    },
    "Circuit Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CFSummary",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Circuit-Summary"
    },
    "Circuit Filter": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/CircuitFillter",
        "type": "timestamp",
        "selector": "span.me-2",
        "tab": "tab1min",
        "key_id": "row-Circuit-Filter"
    },
    "52 Weeks High": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=H",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-52-Weeks-High"
    },
    "52 Weeks Low": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=L",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-52-Weeks-Low"
    },
    "Industry Watch": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/industrywatchList",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Industry-Watch"
    },
    "Industry Watch- Heat Map": {
        "url": "https://beta.bseindia.com/markets/Equity/EQReports/industrywatch?page=IN020101002&scripname=2%2F3%20Wheelers",
        "type": "timestamp",
        "selector": "#tbldate",
        "tab": "tab1min",
        "key_id": "row-Industry-Watch-Heat-Map"
    },
    "Trading Summary": {
        "url": "https://beta.bseindia.com/markets/equity/EQReports/TradeSummary",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Trading-Summary"
    },
    "Derivatives Chain": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriOptionchain",
        "type": "timestamp",
        "selector": "span.me-2.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Derivatives-Chain"
    },
    "Market Summary": {
        "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriArchive_PG/flag/0",
        "type": "timestamp",
        "selector": "#ContentPlaceHolder1_lblAsOn",  # you said this selector is reliable
        "tab": "tab1min",
        "key_id": "row-Market-Summary"
    },
    "Index Watch": {
        "url": "https://beta.bseindia.com/sensex/IndexHighlight",
        "type": "timestamp",
        "selector": "span.resizable-font",
        "tab": "tab1min",
        "key_id": "row-Index-Watch-1min"
    },
}

# ---------------- helpers ----------------
def now_iso():
    # local detection timestamp with seconds precision (no milliseconds)
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("load_state failed")
        return {}

def save_state(state):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception:
        logger.exception("save_state failed")

# robust timestamp parser
def parse_reported_ts(raw_text):
    """
    Parse strings like:
      "As on 19 Nov 2025 | 12:5"  -> returns datetime
      "As on 19 Nov 25 | 12:05"
      "19 Nov 25 12:05"
      "24 Nov 2025 | 12:21 pm"  (heatmap case)
    Returns a datetime or None.
    """
    if not raw_text or not isinstance(raw_text, str):
        return None
    txt = raw_text.strip()

    # remove various casings of 'As on'
    for token in ("As on", "AS ON", "AS on", "as on", "As On"):
        txt = txt.replace(token, "")
    txt = txt.strip()

    # split on '|' if present, else try sensible splits
    parts = []
    if "|" in txt:
        parts = [p.strip() for p in txt.split("|") if p.strip()]
    else:
        # try splitting on double spaces then single space
        parts = [p.strip() for p in txt.split("  ") if p.strip()]
        if not parts:
            parts = [p.strip() for p in txt.split(" ") if p.strip()]

    if not parts:
        return None

    date_part = None
    time_part = None

    if len(parts) >= 2:
        date_part = parts[0]
        time_part = parts[1]
    else:
        single = parts[0]
        tokens = single.split()
        if tokens and ":" in tokens[-1]:
            time_part = tokens[-1]
            date_part = " ".join(tokens[:-1]) if len(tokens) > 1 else None
        else:
            date_part = single

    # handle AM/PM in time_part
    if time_part:
        tp = time_part.lower().replace(".", "")
        # pad minutes if needed like 12:5 -> 12:05
        if ":" in tp:
            hh, mm = tp.split(":")[:2]
            hh = hh.zfill(2)
            mm = mm.zfill(2)
            tp = f"{hh}:{mm}" + ((" " + time_part.split()[-1]) if len(time_part.split()) > 1 else "")
        time_part = tp

    candidates = []
    if date_part and time_part:
        candidates.append(f"{date_part} {time_part}")
    if date_part:
        candidates.append(date_part)

    fmts = [
        "%d %b %Y %H:%M",
        "%d %b %y %H:%M",
        "%d %b %Y %I:%M %p",
        "%d %b %y %I:%M %p",
        "%d %b %Y %H:%M:%S",
        "%d %b %y %H:%M:%S",
        "%d %b %Y",
        "%d %b %y"
    ]

    for cand in candidates:
        cand = cand.strip()
        for fmt in fmts:
            try:
                dt = datetime.strptime(cand, fmt)
                return dt
            except Exception:
                continue

    # final fallback try compact parse
    compact = " ".join(txt.replace(",", " ").split())
    for fmt in ("%d %b %y %H:%M", "%d %b %Y %H:%M"):
        try:
            dt = datetime.strptime(compact, fmt)
            return dt
        except Exception:
            pass

    return None

# ---------------- driver ----------------
class DriverManager:
    """
    Single-driver manager that creates a driver and can recreate if needed.
    Keep driver alive across cycles to speed up many URLs.
    """
    def __init__(self, path=CHROMEDRIVER_PATH):
        self.path = path
        self.driver = None

    def get_driver(self):
        if self.driver:
            try:
                # quick check if alive
                self.driver.title
                return self.driver
            except Exception:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None

        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        service = Service(self.path)
        d = webdriver.Chrome(service=service, options=opts)
        d.implicitly_wait(2)
        self.driver = d
        return self.driver

    def quit(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

# ---------------- worker ----------------
class Worker:
    def __init__(self, socketio=None):
        self.socketio = socketio
        self.dm = DriverManager()
        # persistent cache structure:
        # key -> { last_value, stale_count, last_changed, stale_events: [iso,...] }
        self.cache = load_state() or {}
        for k in url_dict.keys():
            if k not in self.cache:
                self.cache[k] = {"last_value": None, "stale_count": 0, "last_changed": "", "stale_events": []}

    def write_state(self):
        try:
            save_state(self.cache)
        except Exception:
            logger.exception("write_state failed")

    def emit_payload(self, checklist_key, status):
        ui_name = ui_name_mapping.get(checklist_key, checklist_key)
        entry = self.cache.get(checklist_key, {})
        payload = {
            "checklist": ui_name,
            "key_id": url_dict.get(checklist_key, {}).get("key_id"),
            "status": status,
            # last_changed is local detection time when the site's content last changed (seconds precision)
            "last_changed": entry.get("last_changed", ""),
            "last_value": entry.get("last_value"),
            "tab": url_dict.get(checklist_key, {}).get("tab", "tab1min")
        }
        logger.info("EMIT -> %s", payload)
        if self.socketio:
            try:
                self.socketio.emit("update_status", payload)
            except Exception:
                logger.exception("socket emit failed")

    def extract_ts(self, driver, selector):
        """
        Try to read text by CSS selector.
        Return the raw text or None.
        """
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            txt = el.text.strip()
            return txt if txt else None
        except Exception:
            return None

    def monitor_once_for_url(self, driver, key, info):
        """
        Scrape a single URL using existing driver.
        Return one of: ("ok", reported_iso_or_none), ("stale", ...), ("invalid", ...), ("error", ...)
        Also updates cache and writes state where needed.
        """
        url = info["url"]
        selector = info["selector"]

        try:
            driver.get(url)
        except Exception as e:
            logger.error("[%s] page load fail: %s", key, e)
            return "error"

        # attempt read, with retries only if invalid format
        raw = self.extract_ts(driver, selector)
        if not raw:
            # retry a few times (this helps with JS render delays)
            for attempt in range(INVALID_RETRIES):
                time.sleep(INVALID_RETRY_DELAY)
                raw = self.extract_ts(driver, selector)
                if raw:
                    break

        record = self.cache.get(key, {"last_value": None, "stale_count": 0, "last_changed": "", "stale_events": []})

        if not raw:
            # still invalid -> emit invalid format (do not update last_changed)
            # Note: we record this event inside state but as "invalid" count only if you want.
            logger.info("[%s] invalid format read", key)
            # keep existing last_changed/stale_count
            self.cache[key] = record
            self.write_state()
            return "invalid"

        # parse reported site timestamp (optional) - used to decide "ts_behind" staleness
        parsed_dt = parse_reported_ts(raw)
        reported_iso = parsed_dt.strftime("%Y-%m-%d %H:%M:%S") if parsed_dt else None

        # first discovery
        if record.get("last_value") is None:
            now = now_iso()
            record["last_value"] = raw
            record["stale_count"] = 0
            record["last_changed"] = now
            record.setdefault("stale_events", [])
            self.cache[key] = record
            self.write_state()

            # evaluate immediate staleness if site timestamp exists and behind local clock > threshold
            if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
                # record stale event (timestamp when staleness observed)
                record["stale_events"].append(now)
                self.cache[key] = record
                self.write_state()
                return "stale"
            return "ok"

        # change detection
        if raw != record["last_value"]:
            now = now_iso()
            record["last_value"] = raw
            record["stale_count"] = 0
            record["last_changed"] = now
            record.setdefault("stale_events", [])
            self.cache[key] = record
            self.write_state()

            # if site-reported timestamp exists but behind local time > threshold -> stale
            if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
                # record stale event (we still update last_changed to now but mark stale)
                record["stale_events"].append(now)
                self.cache[key] = record
                self.write_state()
                return "stale"
            return "ok"
        else:
            # unchanged value
            record["stale_count"] = record.get("stale_count", 0) + 1
            self.cache[key] = record
            self.write_state()

            # also treat as stale if site timestamp behind local clock
            ts_behind = parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD))
            if record["stale_count"] >= STALE_THRESHOLD or ts_behind:
                # log time of stale occurrence
                now = now_iso()
                record.setdefault("stale_events", []).append(now)
                self.cache[key] = record
                self.write_state()
                return "stale"

            # no UI churn otherwise
            return None

    def monitor(self):
        """
        Main loop: runs every 60 seconds (fixed cadence). Uses a single ChromeDriver instance for all URLs.
        """
        driver = None
        next_run = time.time()
        while True:
            # ensure fixed cadence
            if time.time() < next_run:
                time.sleep(0.3)
                continue

            try:
                driver = self.dm.get_driver()

                # iterate through urls with the single driver
                for key, info in url_dict.items():
                    try:
                        status = self.monitor_once_for_url(driver, key, info)
                    except Exception as e:
                        logger.exception("error monitoring %s: %s", key, e)
                        status = "error"

                    # if status is None -> no UI change (value unchanged but not yet stale)
                    if status:
                        self.emit_payload(key, status)

                # schedule next run at exact 60s increments (avoid drift)
                next_run = next_run + 60
                # if in case next_run is already in past (due to delays), snap to now + 60
                if next_run < time.time() - 2:
                    next_run = time.time() + 60

            except Exception as e:
                logger.exception("1min worker crashed outer loop: %s", e)
                # recreate driver next loop
                try:
                    self.dm.quit()
                except:
                    pass
                time.sleep(2)
                next_run = time.time() + 60
            finally:
                # do not quit driver here; keep it alive across cycles for speed
                pass

def start_threads(socketio=None):
    w = Worker(socketio)
    t = threading.Thread(target=w.monitor, daemon=True)
    t.start()
    logger.info("Started scraping_1min worker")

if __name__ == "__main__":
    # quick local run test
    start_threads(None)
    while True:
        time.sleep(60)












# # scraping_1min.py  (FINAL)
# import os
# import time
# import json
# import logging
# import threading
# import re
# from datetime import datetime, timedelta
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By

# # ---------------- CONFIG ----------------
# CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"
# LOG_DIR = "logs"
# STATE_DIR = "state"
# STATE_FILE = os.path.join(STATE_DIR, "monitor_state_1min.json")
# os.makedirs(LOG_DIR, exist_ok=True)
# os.makedirs(STATE_DIR, exist_ok=True)

# # how many identical minute reads before marking stale
# STALE_THRESHOLD = 3  # minutes

# # ---------------- logger (module-specific file) ----------------
# logger = logging.getLogger("scraping_1min")
# logger.setLevel(logging.INFO)
# logger.propagate = False
# log_filename = os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log")
# if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_filename for h in logger.handlers):
#     fh = logging.FileHandler(log_filename, encoding="utf-8")
#     fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#     logger.addHandler(fh)

# # ---------------- UI mapping / url_dict ----------------
# ui_name_mapping = {
#     "Gainers": "Gainers",
#     "Losers": "Losers",
#     "Spurt in Volume": "Spurt in Volume",
#     "Illiquid Scrips": "Illiquid Scrips",
#     "Circuit Summary": "Circuit Summary",
#     "Circuit Filter": "Circuit Filter",
#     "52 Weeks High": "52 Weeks High",
#     "52 Weeks Low": "52 Weeks Low",
#     "Industry Watch": "Industry Watch",
#     "Industry Watch- Heat Map": "Industry Watch- Heat Map",
#     "Trading Summary": "Trading Summary",
#     "Derivatives Chain": "Derivatives Chain",
#     "Market Summary": "Market Summary",
#     "Index Watch": "Index Watch",
# }

# # url_dict (as you provided) — note Gainers/Losers selector update and Market Summary selector
# url_dict = {
#     "Gainers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
#         "type": "timestamp",
#         "selector": "span.resizable-font.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Gainers"
#     },
#     "Losers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
#         "type": "timestamp",
#         "selector": "span.resizable-font.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Losers"
#     },
#     "Spurt in Volume": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/Missprtvol",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Spurt-in-Volume"
#     },
#     "Illiquid Scrips": {
#         "url": "https://beta.bseindia.com/markets/Equity/EQReports/Illiquid_Scrips",
#         "type": "timestamp",
#         "selector": "#ContentPlaceHolder1_lblNoteDate",
#         "tab": "tab1min",
#         "key_id": "row-Illiquid-Scrips"
#     },
#     "Circuit Summary": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/CFSummary",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Circuit-Summary"
#     },
#     "Circuit Filter": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/CircuitFillter",
#         "type": "timestamp",
#         "selector": "span.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Circuit-Filter"
#     },
#     "52 Weeks High": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=H",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-52-Weeks-High"
#     },
#     "52 Weeks Low": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=L",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-52-Weeks-Low"
#     },
#     "Industry Watch": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/industrywatchList",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Industry-Watch"
#     },
#     "Industry Watch- Heat Map": {
#         "url": "https://beta.bseindia.com/markets/Equity/EQReports/industrywatch?page=IN020101002&scripname=2%2F3%20Wheelers",
#         "type": "timestamp",
#         "selector": "#tbldate",  # returns "24 Nov 2025 | 12:21 pm" style
#         "tab": "tab1min",
#         "key_id": "row-Industry-Watch-Heat-Map"
#     },
#     "Trading Summary": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/TradeSummary",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Trading-Summary"
#     },
#     "Derivatives Chain": {
#         "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriOptionchain",
#         "type": "timestamp",
#         "selector": "span.me-2.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Derivatives-Chain"
#     },
#     "Market Summary": {
#         "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriArchive_PG/flag/0",
#         # try the specific element if present; fallback handled by parse
#         "type": "timestamp",
#         "selector": "#ContentPlaceHolder1_lblAsOn",
#         "tab": "tab1min",
#         "key_id": "row-Market-Summary"
#     },
#     "Index Watch": {
#         "url": "https://beta.bseindia.com/sensex/IndexHighlight",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Index-Watch-1min"
#     },
# }

# # ---------------- helpers ----------------
# def now_iso():
#     # local detection timestamp with seconds precision (NO milliseconds)
#     return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# def load_state():
#     if not os.path.exists(STATE_FILE):
#         return {}
#     try:
#         with open(STATE_FILE, "r", encoding="utf-8") as f:
#             return json.load(f)
#     except Exception:
#         logger.exception("load_state failed")
#         return {}

# def save_state(state):
#     tmp = STATE_FILE + ".tmp"
#     try:
#         with open(tmp, "w", encoding="utf-8") as f:
#             json.dump(state, f, indent=2)
#         os.replace(tmp, STATE_FILE)
#     except Exception:
#         logger.exception("save_state failed")

# # ---------------- robust timestamp parser ----------------
# def parse_reported_ts(raw_text):
#     """
#     Safe timestamp parser.
#     Handles:
#     - "As on 19 Nov 2025 | 12:5" -> pads minute to 12:05
#     - "As on 19 Nov 25 | 12:5"
#     - "24 Nov 2025 | 12:21 pm"  (handles am/pm)
#     - "19 Nov 2025 12:05"
#     - returns a datetime object or None
#     """
#     if not raw_text or not isinstance(raw_text, str):
#         return None

#     txt = raw_text.strip()

#     # remove 'As on' in any case
#     txt = re.sub(r'(?i)\bas\s*on\b', '', txt).strip()

#     # unify separators and remove extra commas
#     txt = txt.replace(",", " ").strip()

#     # If '|' present split on it; else try other splits
#     if "|" in txt:
#         parts = [p.strip() for p in txt.split("|") if p.strip()]
#     else:
#         # sometimes there are double spaces; try to keep date and time tokens
#         parts = [p.strip() for p in re.split(r'\s{2,}', txt) if p.strip()]
#         if not parts:
#             parts = [p.strip() for p in txt.split() if p.strip()]
#             # if all tokens, keep as single string (we'll attempt to detect time token)
#             if len(parts) > 3:
#                 parts = [" ".join(parts)]

#     if not parts:
#         return None

#     date_part = None
#     time_part = None

#     if len(parts) >= 2:
#         date_part = parts[0]
#         time_part = parts[1]
#     else:
#         # single chunk: try to split last token as time if contains ':'
#         single = parts[0]
#         tokens = single.split()
#         if tokens and ":" in tokens[-1]:
#             time_part = tokens[-1]
#             date_part = " ".join(tokens[:-1]) if len(tokens) > 1 else None
#         else:
#             date_part = single

#     # normalize time like "12:5" -> "12:05"
#     if time_part:
#         # handle AM/PM presence
#         m = re.match(r'(\d{1,2}):(\d{1,2})(?::\d{1,2})?\s*(am|pm|AM|PM)?', time_part)
#         if m:
#             hh = m.group(1).zfill(2)
#             mm = m.group(2).zfill(2)
#             ampm = m.group(3) or ""
#             time_part = f"{hh}:{mm}" + (f" {ampm}" if ampm else "")

#     candidates = []
#     if date_part and time_part:
#         candidates.append(f"{date_part} {time_part}")
#     if date_part:
#         candidates.append(date_part)

#     # Try formats, including AM/PM and two-digit year
#     fmts = [
#         "%d %b %Y %H:%M",
#         "%d %b %y %H:%M",
#         "%d %b %Y %I:%M %p",
#         "%d %b %y %I:%M %p",
#         "%d %b %Y %H:%M:%S",
#         "%d %b %y %H:%M:%S",
#         "%d %b %Y",
#         "%d %b %y",
#     ]

#     for cand in candidates:
#         cand = cand.strip()
#         for fmt in fmts:
#             try:
#                 dt = datetime.strptime(cand, fmt)
#                 return dt
#             except Exception:
#                 continue

#     # final fallback: compact and try some extra formats
#     compact = " ".join(txt.split())
#     for fmt in ("%d %b %y %H:%M", "%d %b %Y %H:%M", "%d %b %y %I:%M %p", "%d %b %Y %I:%M %p"):
#         try:
#             dt = datetime.strptime(compact, fmt)
#             return dt
#         except Exception:
#             pass

#     return None

# # ---------------- driver ----------------
# class DriverManager:
#     def get_driver(self):
#         opts = Options()
#         opts.add_argument("--headless=new")
#         opts.add_argument("--disable-gpu")
#         opts.add_argument("--no-sandbox")
#         opts.add_argument("--disable-dev-shm-usage")
#         # careful with window-size (some pages render differently); keep default 1920x1080
#         opts.add_argument("--window-size=1920,1080")
#         service = Service(CHROMEDRIVER_PATH)
#         d = webdriver.Chrome(service=service, options=opts)
#         d.implicitly_wait(3)
#         return d

# # ---------------- worker ----------------
# class Worker:
#     def __init__(self, socketio=None):
#         self.socketio = socketio
#         self.dm = DriverManager()
#         self.cache = load_state() or {}
#         # ensure keys exist with correct structure
#         for k in url_dict.keys():
#             if k not in self.cache:
#                 self.cache[k] = {"last_value": None, "stale_count": 0, "last_changed": ""}

#     def write_state(self):
#         try:
#             save_state(self.cache)
#         except Exception:
#             logger.exception("write_state failed")

#     def emit_payload(self, checklist_key, status):
#         ui_name = ui_name_mapping.get(checklist_key, checklist_key)
#         entry = self.cache.get(checklist_key, {})
#         payload = {
#             "checklist": ui_name,
#             "key_id": url_dict.get(checklist_key, {}).get("key_id"),
#             "status": status,
#             # last_changed is local detection time when the site's content last changed (seconds precision)
#             "last_changed": entry.get("last_changed", ""),
#             "last_value": entry.get("last_value"),
#             # UI tab mapping
#             "tab": url_dict.get(checklist_key, {}).get("tab", "tab1min")
#         }
#         logger.info("EMIT -> %s", payload)
#         if self.socketio:
#             try:
#                 self.socketio.emit("update_status", payload)
#             except Exception:
#                 logger.exception("socket emit failed")

#     def extract_ts_with_retries(self, driver, selector, retries=3, delay=0.8):
#         """
#         Try to extract timestamp from a page element.
#         Retries only used for URLs that would otherwise yield invalid format.
#         """
#         for attempt in range(retries):
#             try:
#                 el = driver.find_element(By.CSS_SELECTOR, selector)
#                 txt = el.text.strip()
#                 if txt:
#                     return txt
#             except Exception:
#                 # element not present yet or JS loading - retry
#                 time.sleep(delay)
#                 continue
#         return None

#     def monitor(self):
#         driver = None
#         while True:
#             try:
#                 driver = self.dm.get_driver()
#                 for key, info in url_dict.items():
#                     url = info["url"]
#                     selector = info["selector"]

#                     try:
#                         driver.get(url)
#                         # short wait so JS can populate; don't over-wait (we retry selectively)
#                         time.sleep(0.8)
#                     except Exception as e:
#                         logger.error("[%s] load fail: %s", key, e)
#                         self.emit_payload(key, "error")
#                         continue

#                     # Attempt quick read first (fast), if empty -> retry few times
#                     raw = None
#                     try:
#                         el = driver.find_element(By.CSS_SELECTOR, selector)
#                         raw = el.text.strip() if el.text.strip() else None
#                     except Exception:
#                         raw = None

#                     if not raw:
#                         # retry loop ONLY for invalid-format candidates (per your requirement)
#                         raw = self.extract_ts_with_retries(driver, selector, retries=3, delay=0.8)

#                     record = self.cache.get(key, {})

#                     if not raw:
#                         # still no timestamp -> invalid format
#                         self.emit_payload(key, "invalid format")
#                         continue

#                     # parse reported timestamp (site's reported date/time) if possible
#                     parsed_dt = parse_reported_ts(raw)
#                     reported_iso = parsed_dt.strftime("%Y-%m-%d %H:%M:%S") if parsed_dt else None

#                     # FIRST DISCOVERY: no previous value
#                     if record.get("last_value") is None:
#                         now = now_iso()
#                         record["last_value"] = raw
#                         record["stale_count"] = 0
#                         record["last_changed"] = now  # local detection time
#                         self.cache[key] = record
#                         self.write_state()
#                         # If site timestamp exists but is behind local clock beyond threshold => mark stale
#                         if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
#                             self.emit_payload(key, "stale")
#                         else:
#                             self.emit_payload(key, "ok")
#                         continue

#                     # CHANGED?
#                     if raw != record["last_value"]:
#                         # update last_value and set last_changed to local detection time (precise to seconds)
#                         now = now_iso()
#                         record["last_value"] = raw
#                         record["stale_count"] = 0
#                         record["last_changed"] = now
#                         self.cache[key] = record
#                         self.write_state()
#                         # If site-reported timestamp exists but is behind local clock more than threshold -> stale
#                         if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
#                             self.emit_payload(key, "stale")
#                         else:
#                             self.emit_payload(key, "ok")
#                     else:
#                         # unchanged -> increment stale_count and maybe emit stale
#                         record["stale_count"] = record.get("stale_count", 0) + 1
#                         self.cache[key] = record
#                         self.write_state()
#                         ts_behind = parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD))
#                         if record["stale_count"] >= STALE_THRESHOLD or ts_behind:
#                             self.emit_payload(key, "stale")
#                         # otherwise do nothing to avoid UI churn

#                 # wait one minute before next cycle (1-min worker)
#                 time.sleep(60)

#             except Exception as e:
#                 logger.exception("1min worker crashed: %s", e)
#                 # backoff then continue
#                 time.sleep(5)
#             finally:
#                 if driver:
#                     try:
#                         driver.quit()
#                     except Exception:
#                         pass

# def start_threads(socketio=None):
#     w = Worker(socketio)
#     t = threading.Thread(target=w.monitor, daemon=True)
#     t.start()
#     logger.info("Started scraping_1min worker")












# # scraping_1min.py  (FINAL)
# import os
# import time
# import json
# import logging
# import threading
# import re
# from datetime import datetime, timedelta
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By

# # ---------------- CONFIG ----------------
# CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"
# LOG_DIR = "logs"
# STATE_DIR = "state"
# STATE_FILE = os.path.join(STATE_DIR, "monitor_state_1min.json")
# os.makedirs(LOG_DIR, exist_ok=True)
# os.makedirs(STATE_DIR, exist_ok=True)

# # how many identical minute reads before marking stale
# STALE_THRESHOLD = 3  # minutes

# # ---------------- logger (module-specific file) ----------------
# logger = logging.getLogger("scraping_1min")
# logger.setLevel(logging.INFO)
# logger.propagate = False
# log_filename = os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log")
# if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_filename for h in logger.handlers):
#     fh = logging.FileHandler(log_filename, encoding="utf-8")
#     fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#     logger.addHandler(fh)

# # ---------------- UI mapping / url_dict ----------------
# ui_name_mapping = {
#     "Gainers": "Gainers",
#     "Losers": "Losers",
#     "Spurt in Volume": "Spurt in Volume",
#     "Illiquid Scrips": "Illiquid Scrips",
#     "Circuit Summary": "Circuit Summary",
#     "Circuit Filter": "Circuit Filter",
#     "52 Weeks High": "52 Weeks High",
#     "52 Weeks Low": "52 Weeks Low",
#     "Industry Watch": "Industry Watch",
#     "Industry Watch- Heat Map": "Industry Watch- Heat Map",
#     "Trading Summary": "Trading Summary",
#     "Derivatives Chain": "Derivatives Chain",
#     "Market Summary": "Market Summary",
#     "Index Watch": "Index Watch",
# }

# # url_dict (as you provided) — note Gainers/Losers selector update and Market Summary selector
# url_dict = {
#     "Gainers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=gainer*all$all$",
#         "type": "timestamp",
#         "selector": "span.resizable-font.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Gainers"
#     },
#     "Losers": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/mktwatchR?filter=loser*all$all$",
#         "type": "timestamp",
#         "selector": "span.resizable-font.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Losers"
#     },
#     "Spurt in Volume": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/Missprtvol",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Spurt-in-Volume"
#     },
#     "Illiquid Scrips": {
#         "url": "https://beta.bseindia.com/markets/Equity/EQReports/Illiquid_Scrips",
#         "type": "timestamp",
#         "selector": "#ContentPlaceHolder1_lblNoteDate",
#         "tab": "tab1min",
#         "key_id": "row-Illiquid-Scrips"
#     },
#     "Circuit Summary": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/CFSummary",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Circuit-Summary"
#     },
#     "Circuit Filter": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/CircuitFillter",
#         "type": "timestamp",
#         "selector": "span.me-2",
#         "tab": "tab1min",
#         "key_id": "row-Circuit-Filter"
#     },
#     "52 Weeks High": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=H",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-52-Weeks-High"
#     },
#     "52 Weeks Low": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/HighLow?flag=L",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-52-Weeks-Low"
#     },
#     "Industry Watch": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/industrywatchList",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Industry-Watch"
#     },
#     "Industry Watch- Heat Map": {
#         "url": "https://beta.bseindia.com/markets/Equity/EQReports/industrywatch?page=IN020101002&scripname=2%2F3%20Wheelers",
#         "type": "timestamp",
#         "selector": "#tbldate",  # returns "24 Nov 2025 | 12:21 pm" style
#         "tab": "tab1min",
#         "key_id": "row-Industry-Watch-Heat-Map"
#     },
#     "Trading Summary": {
#         "url": "https://beta.bseindia.com/markets/equity/EQReports/TradeSummary",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Trading-Summary"
#     },
#     "Derivatives Chain": {
#         "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriOptionchain",
#         "type": "timestamp",
#         "selector": "span.me-2.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Derivatives-Chain"
#     },
#     "Market Summary": {
#         "url": "https://beta.bseindia.com/markets/Derivatives/DeriReports/DeriArchive_PG/flag/0",
#         # try the specific element if present; fallback handled by parse
#         "type": "timestamp",
#         "selector": "#ContentPlaceHolder1_lblAsOn",
#         "tab": "tab1min",
#         "key_id": "row-Market-Summary"
#     },
#     "Index Watch": {
#         "url": "https://beta.bseindia.com/sensex/IndexHighlight",
#         "type": "timestamp",
#         "selector": "span.resizable-font",
#         "tab": "tab1min",
#         "key_id": "row-Index-Watch-1min"
#     },
# }

# # ---------------- helpers ----------------
# def now_iso():
#     # local detection timestamp with seconds precision (NO milliseconds)
#     return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# def load_state():
#     if not os.path.exists(STATE_FILE):
#         return {}
#     try:
#         with open(STATE_FILE, "r", encoding="utf-8") as f:
#             return json.load(f)
#     except Exception:
#         logger.exception("load_state failed")
#         return {}

# def save_state(state):
#     tmp = STATE_FILE + ".tmp"
#     try:
#         with open(tmp, "w", encoding="utf-8") as f:
#             json.dump(state, f, indent=2)
#         os.replace(tmp, STATE_FILE)
#     except Exception:
#         logger.exception("save_state failed")

# # ---------------- robust timestamp parser ----------------
# def parse_reported_ts(raw_text):
#     """
#     Safe timestamp parser.
#     Handles:
#     - "As on 19 Nov 2025 | 12:5" -> pads minute to 12:05
#     - "As on 19 Nov 25 | 12:5"
#     - "24 Nov 2025 | 12:21 pm"  (handles am/pm)
#     - "19 Nov 2025 12:05"
#     - returns a datetime object or None
#     """
#     if not raw_text or not isinstance(raw_text, str):
#         return None

#     txt = raw_text.strip()

#     # remove 'As on' in any case
#     txt = re.sub(r'(?i)\bas\s*on\b', '', txt).strip()

#     # unify separators and remove extra commas
#     txt = txt.replace(",", " ").strip()

#     # If '|' present split on it; else try other splits
#     if "|" in txt:
#         parts = [p.strip() for p in txt.split("|") if p.strip()]
#     else:
#         # sometimes there are double spaces; try to keep date and time tokens
#         parts = [p.strip() for p in re.split(r'\s{2,}', txt) if p.strip()]
#         if not parts:
#             parts = [p.strip() for p in txt.split() if p.strip()]
#             # if all tokens, keep as single string (we'll attempt to detect time token)
#             if len(parts) > 3:
#                 parts = [" ".join(parts)]

#     if not parts:
#         return None

#     date_part = None
#     time_part = None

#     if len(parts) >= 2:
#         date_part = parts[0]
#         time_part = parts[1]
#     else:
#         # single chunk: try to split last token as time if contains ':'
#         single = parts[0]
#         tokens = single.split()
#         if tokens and ":" in tokens[-1]:
#             time_part = tokens[-1]
#             date_part = " ".join(tokens[:-1]) if len(tokens) > 1 else None
#         else:
#             date_part = single

#     # normalize time like "12:5" -> "12:05"
#     if time_part:
#         # handle AM/PM presence
#         m = re.match(r'(\d{1,2}):(\d{1,2})(?::\d{1,2})?\s*(am|pm|AM|PM)?', time_part)
#         if m:
#             hh = m.group(1).zfill(2)
#             mm = m.group(2).zfill(2)
#             ampm = m.group(3) or ""
#             time_part = f"{hh}:{mm}" + (f" {ampm}" if ampm else "")

#     candidates = []
#     if date_part and time_part:
#         candidates.append(f"{date_part} {time_part}")
#     if date_part:
#         candidates.append(date_part)

#     # Try formats, including AM/PM and two-digit year
#     fmts = [
#         "%d %b %Y %H:%M",
#         "%d %b %y %H:%M",
#         "%d %b %Y %I:%M %p",
#         "%d %b %y %I:%M %p",
#         "%d %b %Y %H:%M:%S",
#         "%d %b %y %H:%M:%S",
#         "%d %b %Y",
#         "%d %b %y",
#     ]

#     for cand in candidates:
#         cand = cand.strip()
#         for fmt in fmts:
#             try:
#                 dt = datetime.strptime(cand, fmt)
#                 return dt
#             except Exception:
#                 continue

#     # final fallback: compact and try some extra formats
#     compact = " ".join(txt.split())
#     for fmt in ("%d %b %y %H:%M", "%d %b %Y %H:%M", "%d %b %y %I:%M %p", "%d %b %Y %I:%M %p"):
#         try:
#             dt = datetime.strptime(compact, fmt)
#             return dt
#         except Exception:
#             pass

#     return None

# # ---------------- driver ----------------
# class DriverManager:
#     def get_driver(self):
#         opts = Options()
#         opts.add_argument("--headless=new")
#         opts.add_argument("--disable-gpu")
#         opts.add_argument("--no-sandbox")
#         opts.add_argument("--disable-dev-shm-usage")
#         # careful with window-size (some pages render differently); keep default 1920x1080
#         opts.add_argument("--window-size=1920,1080")
#         service = Service(CHROMEDRIVER_PATH)
#         d = webdriver.Chrome(service=service, options=opts)
#         d.implicitly_wait(3)
#         return d

# # ---------------- worker ----------------
# class Worker:
#     def __init__(self, socketio=None):
#         self.socketio = socketio
#         self.dm = DriverManager()
#         self.cache = load_state() or {}
#         # ensure keys exist with correct structure
#         for k in url_dict.keys():
#             if k not in self.cache:
#                 self.cache[k] = {"last_value": None, "stale_count": 0, "last_changed": ""}

#     def write_state(self):
#         try:
#             save_state(self.cache)
#         except Exception:
#             logger.exception("write_state failed")

#     def emit_payload(self, checklist_key, status):
#         ui_name = ui_name_mapping.get(checklist_key, checklist_key)
#         entry = self.cache.get(checklist_key, {})
#         payload = {
#             "checklist": ui_name,
#             "key_id": url_dict.get(checklist_key, {}).get("key_id"),
#             "status": status,
#             # last_changed is local detection time when the site's content last changed (seconds precision)
#             "last_changed": entry.get("last_changed", ""),
#             "last_value": entry.get("last_value"),
#             # UI tab mapping
#             "tab": url_dict.get(checklist_key, {}).get("tab", "tab1min")
#         }
#         logger.info("EMIT -> %s", payload)
#         if self.socketio:
#             try:
#                 self.socketio.emit("update_status", payload)
#             except Exception:
#                 logger.exception("socket emit failed")

#     def extract_ts_with_retries(self, driver, selector, retries=3, delay=0.8):
#         """
#         Try to extract timestamp from a page element.
#         Retries only used for URLs that would otherwise yield invalid format.
#         """
#         for attempt in range(retries):
#             try:
#                 el = driver.find_element(By.CSS_SELECTOR, selector)
#                 txt = el.text.strip()
#                 if txt:
#                     return txt
#             except Exception:
#                 # element not present yet or JS loading - retry
#                 time.sleep(delay)
#                 continue
#         return None

#     def monitor(self):
#         driver = None
#         while True:
#             try:
#                 driver = self.dm.get_driver()
#                 for key, info in url_dict.items():
#                     url = info["url"]
#                     selector = info["selector"]

#                     try:
#                         driver.get(url)
#                         # short wait so JS can populate; don't over-wait (we retry selectively)
#                         time.sleep(0.8)
#                     except Exception as e:
#                         logger.error("[%s] load fail: %s", key, e)
#                         self.emit_payload(key, "error")
#                         continue

#                     # Attempt quick read first (fast), if empty -> retry few times
#                     raw = None
#                     try:
#                         el = driver.find_element(By.CSS_SELECTOR, selector)
#                         raw = el.text.strip() if el.text.strip() else None
#                     except Exception:
#                         raw = None

#                     if not raw:
#                         # retry loop ONLY for invalid-format candidates (per your requirement)
#                         raw = self.extract_ts_with_retries(driver, selector, retries=3, delay=0.8)

#                     record = self.cache.get(key, {})

#                     if not raw:
#                         # still no timestamp -> invalid format
#                         self.emit_payload(key, "invalid format")
#                         continue

#                     # parse reported timestamp (site's reported date/time) if possible
#                     parsed_dt = parse_reported_ts(raw)
#                     reported_iso = parsed_dt.strftime("%Y-%m-%d %H:%M:%S") if parsed_dt else None

#                     # FIRST DISCOVERY: no previous value
#                     if record.get("last_value") is None:
#                         now = now_iso()
#                         record["last_value"] = raw
#                         record["stale_count"] = 0
#                         record["last_changed"] = now  # local detection time
#                         self.cache[key] = record
#                         self.write_state()
#                         # If site timestamp exists but is behind local clock beyond threshold => mark stale
#                         if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
#                             self.emit_payload(key, "stale")
#                         else:
#                             self.emit_payload(key, "ok")
#                         continue

#                     # CHANGED?
#                     if raw != record["last_value"]:
#                         # update last_value and set last_changed to local detection time (precise to seconds)
#                         now = now_iso()
#                         record["last_value"] = raw
#                         record["stale_count"] = 0
#                         record["last_changed"] = now
#                         self.cache[key] = record
#                         self.write_state()
#                         # If site-reported timestamp exists but is behind local clock more than threshold -> stale
#                         if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
#                             self.emit_payload(key, "stale")
#                         else:
#                             self.emit_payload(key, "ok")
#                     else:
#                         # unchanged -> increment stale_count and maybe emit stale
#                         record["stale_count"] = record.get("stale_count", 0) + 1
#                         self.cache[key] = record
#                         self.write_state()
#                         ts_behind = parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD))
#                         if record["stale_count"] >= STALE_THRESHOLD or ts_behind:
#                             self.emit_payload(key, "stale")
#                         # otherwise do nothing to avoid UI churn

#                 # wait one minute before next cycle (1-min worker)
#                 time.sleep(60)

#             except Exception as e:
#                 logger.exception("1min worker crashed: %s", e)
#                 # backoff then continue
#                 time.sleep(5)
#             finally:
#                 if driver:
#                     try:
#                         driver.quit()
#                     except Exception:
#                         pass

# def start_threads(socketio=None):
#     w = Worker(socketio)
#     t = threading.Thread(target=w.monitor, daemon=True)
#     t.start()
#     logger.info("Started scraping_1min worker")
