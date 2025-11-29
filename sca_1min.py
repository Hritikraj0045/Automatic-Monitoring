#!/usr/bin/env python3
# sca_1min.py (updated) — Option A: daily state JSON rotation
# Features:
# - new state file each day: state/monitor_state_1min_YYYY-MM-DD.json
# - stop scraping after end time for a URL, emit final completed payload (last_value + last_changed)
# - do not re-scrape completed URLs until next day
# - resume next day's scraping after rotating state file
# - conservative emit behavior to avoid spamming (emitted_completed flag)

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
import re

# ---------------- CONFIG ----------------
CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.163\chromedriver-win64\chromedriver.exe"
LOG_DIR = "logs"
STATE_DIR = "state"
CONFIG_DIR = "config"
URL_DICT_PATH = os.path.join(CONFIG_DIR, "url_dict_1min.json")
NAME_MAPPING_PATH = os.path.join(CONFIG_DIR, "url_name_mapping.json")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

STALE_THRESHOLD = 3  # minutes
INVALID_RETRY = 3
INVALID_RETRY_DELAY = 0.6  # seconds

# ---------------- LOGGER ----------------
logger = logging.getLogger("scraping_1min")
logger.setLevel(logging.INFO)
logger.propagate = False
log_filename = os.path.join(LOG_DIR, f"scraping_1min_{datetime.now().strftime('%Y%m%d')}.log")
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_filename for h in logger.handlers):
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

# ---------------- LOAD JSON CONFIGS ----------------
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception(f"Failed to load JSON: {path} | Error: {e}")
        raise

# validate config files exist (fail early)
if not os.path.exists(URL_DICT_PATH):
    logger.error("URL dict missing: %s", URL_DICT_PATH)
    raise SystemExit(1)
if not os.path.exists(NAME_MAPPING_PATH):
    logger.error("Name mapping missing: %s", NAME_MAPPING_PATH)
    raise SystemExit(1)

url_dict = load_json(URL_DICT_PATH)
ui_name_mapping = load_json(NAME_MAPPING_PATH)

# ---------------- HELPERS ----------------
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def state_filename_for_day(day_str):
    # day_str expected "YYYY-MM-DD"
    return os.path.join(STATE_DIR, f"monitor_state_1min_{day_str}.json")

def load_state_file(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("load_state failed for %s", path)
        return {}

def save_state_file(path, state):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        logger.exception("save_state failed for %s", path)

# --------------- TIMESTAMP PARSER ---------------
def parse_reported_ts(raw_text):
    if not raw_text or not isinstance(raw_text, str):
        return None
    txt = raw_text.strip()
    txt = re.sub(r"(?i)\bas\s*on\b", "", txt).strip()
    txt = txt.replace("\u00A0", " ").replace("\u200B", "").strip()

    if "|" in txt:
        parts = [p.strip() for p in txt.split("|") if p.strip()]
    else:
        parts = [p.strip() for p in re.split(r"\s{2,}", txt) if p.strip()]

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

    if time_part and ":" in time_part:
        m = re.match(r"^(\d{1,2}):(\d{1,2})(?::\d{1,2})?\s*(am|pm|AM|PM)?$", time_part.strip())
        if m:
            hh = m.group(1).zfill(2)
            mm = m.group(2).zfill(2)
            ampm = m.group(3)
            time_part = f"{hh}:{mm}" + (f" {ampm.lower()}" if ampm else "")

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
        "%d %b %y",
    ]

    for cand in candidates:
        cand = cand.strip()
        for fmt in fmts:
            try:
                return datetime.strptime(cand, fmt)
            except Exception:
                continue

    compact = " ".join(txt.replace(",", " ").split())
    for fmt in ("%d %b %y %H:%M", "%d %b %Y %H:%M", "%d %b %Y %I:%M %p"):
        try:
            return datetime.strptime(compact, fmt)
        except Exception:
            pass

    return None

# ---------------- DRIVER MANAGER ----------------
class DriverManager:
    def get_driver(self):
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        service = Service(CHROMEDRIVER_PATH)
        d = webdriver.Chrome(service=service, options=opts)
        d.implicitly_wait(1)
        return d

# ---------------- WORKER ----------------
class Worker:
    def __init__(self, socketio=None):
        self.socketio = socketio
        self.dm = DriverManager()

        # day tracked in this process
        self.state_day = datetime.now().strftime("%Y-%m-%d")
        self.state_file = state_filename_for_day(self.state_day)

        # load state for today if exists, else try to carry forward last file (optional)
        self.cache = load_state_file(self.state_file) or {}

        # ensure all keys exist with defaults; keep last_value/last_changed if present
        for k in url_dict.keys():
            if k not in self.cache:
                self.cache[k] = {
                    "last_value": None,
                    "stale_count": 0,
                    "last_changed": "",
                    "stale_times": [],
                    "completed": False,
                    "emitted_completed": False  # controls repeated emits in same process run
                }
            else:
                # normalize missing fields in an existing loaded state
                self.cache[k].setdefault("last_value", None)
                self.cache[k].setdefault("stale_count", 0)
                self.cache[k].setdefault("last_changed", "")
                self.cache[k].setdefault("stale_times", [])
                self.cache[k].setdefault("completed", False)
                self.cache[k].setdefault("emitted_completed", False)

        # persist initial state file
        save_state_file(self.state_file, self.cache)

    def write_state(self):
        try:
            save_state_file(self.state_file, self.cache)
        except Exception:
            logger.exception("write_state failed")

    def emit_payload(self, checklist_key, status):
        ui_name = ui_name_mapping.get(checklist_key, checklist_key)
        entry = self.cache.get(checklist_key, {})
        payload = {
            "checklist": ui_name,
            "key_id": url_dict.get(checklist_key, {}).get("key_id"),
            "status": status,
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
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            txt = el.text.strip()
            return txt or None
        except Exception:
            parts = [p.strip() for p in selector.split(",") if p.strip()]
            for p in parts:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, p)
                    txt = el.text.strip()
                    if txt:
                        return txt
                except Exception:
                    continue
            return None

    def _in_time_window(self, cfg):
        """
        Return (in_window: bool, window_state: None|'skip'|'completed')
        """
        now = datetime.now().time()
        start_s = cfg.get("start")
        end_s = cfg.get("end")
        if not start_s and not end_s:
            return True, None

        def parse_hm(s):
            try:
                hh, mm = s.split(":")
                return int(hh), int(mm)
            except Exception:
                return None

        if start_s:
            st = parse_hm(start_s)
            if st:
                # today's start_time
                start_time = datetime.now().replace(hour=st[0], minute=st[1], second=0, microsecond=0).time()
                if now < start_time:
                    return False, "skip"

        if end_s:
            et = parse_hm(end_s)
            if et:
                end_time = datetime.now().replace(hour=et[0], minute=et[1], second=0, microsecond=0).time()
                if now > end_time:
                    return False, "completed"

        return True, None

    def rotate_state_if_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.state_day:
            logger.info("New day detected: rotating state file from %s -> %s", self.state_day, today)
            # finalize current day's file (already persisted), then create new one
            self.state_day = today
            self.state_file = state_filename_for_day(self.state_day)
            # init fresh cache for the new day but keep previous day's file intact
            self.cache = {}
            for k in url_dict.keys():
                self.cache[k] = {
                    "last_value": None,
                    "stale_count": 0,
                    "last_changed": "",
                    "stale_times": [],
                    "completed": False,
                    "emitted_completed": False
                }
            save_state_file(self.state_file, self.cache)
            logger.info("Created new state file: %s", self.state_file)

    def monitor(self):
        driver = None
        next_run = time.time()
        try:
            driver = self.dm.get_driver()
        except Exception as e:
            logger.exception("initial driver creation failed: %s", e)
            driver = None

        while True:
            # keep 60s cadence
            if time.time() < next_run:
                time.sleep(0.25)
                continue

            # rotate state if new day
            try:
                self.rotate_state_if_new_day()
            except Exception:
                logger.exception("rotate_state_if_new_day failed")

            # ensure driver
            if driver is None:
                try:
                    driver = self.dm.get_driver()
                except Exception as e:
                    logger.exception("driver creation failed in loop: %s", e)
                    time.sleep(5)
                    next_run += 60
                    continue

            cycle_start = time.time()

            # iterate configured URLs
            for key, info in url_dict.items():
                try:
                    # ensure state record exists
                    record = self.cache.get(key)
                    if record is None:
                        record = {
                            "last_value": None,
                            "stale_count": 0,
                            "last_changed": "",
                            "stale_times": [],
                            "completed": False,
                            "emitted_completed": False
                        }
                        self.cache[key] = record

                    # If already completed for today -> do not scrape this URL
                    if record.get("completed"):
                        # emit completed once (e.g. on process start / UI refresh) to allow UI to lock row
                        if not record.get("emitted_completed"):
                            # do not modify last_value/last_changed, simply emit final state
                            try:
                                self.emit_payload(key, "completed")
                                record["emitted_completed"] = True
                                self.write_state()
                            except Exception:
                                logger.exception("emit completed on startup failed for %s", key)
                        continue

                    # check time window (start/end)
                    in_window, window_state = self._in_time_window(info)
                    if window_state == "completed":
                        # mark completed and emit final payload once
                        if not record.get("completed"):
                            record["completed"] = True
                            if not record.get("last_changed"):
                                record["last_changed"] = now_iso()
                            # ensure emitted_completed is reset so UI gets the final emit immediately
                            record["emitted_completed"] = False
                            self.cache[key] = record
                            self.write_state()
                            # emit final completed payload (will include last_value)
                            self.emit_payload(key, "completed")
                        # stop scraping this URL for the rest of the day
                        continue

                    if not in_window:
                        # pre-start: do nothing (UI can show not-started if you choose)
                        # we avoid emitting "not-started" every cycle to reduce churn
                        continue

                    # OK: in-window -> perform scrape
                    url = info.get("url")
                    selector = info.get("selector")
                    typ = info.get("type", "timestamp")

                    # load page
                    try:
                        driver.get(url)
                    except Exception as e:
                        logger.error("[%s] load fail: %s", key, e)
                        # emit error and do not change last_value/last_changed
                        self.emit_payload(key, "error")
                        continue

                    # allow small render pause
                    time.sleep(1)

                    # try to extract raw text (with retry)
                    raw = self.extract_ts(driver, selector)
                    if raw is None or raw == "":
                        retried = False
                        for attempt in range(INVALID_RETRY):
                            time.sleep(INVALID_RETRY_DELAY)
                            raw = self.extract_ts(driver, selector)
                            if raw:
                                retried = True
                                break
                        if not raw:
                            logger.warning("[%s] invalid format after retries", key)
                            self.emit_payload(key, "invalid format")
                            continue

                    # got raw data — compare and update state
                    parsed_dt = parse_reported_ts(raw)
                    reported_iso = parsed_dt.strftime("%Y-%m-%d %H:%M:%S") if parsed_dt else None

                    # first discovery
                    if record.get("last_value") is None:
                        now = now_iso()
                        record["last_value"] = raw
                        record["stale_count"] = 0
                        record["last_changed"] = now
                        record.setdefault("stale_times", [])
                        record.setdefault("completed", False)
                        record.setdefault("emitted_completed", False)
                        self.cache[key] = record
                        self.write_state()
                        # if reported timestamp is old relative to local, mark stale else ok
                        if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
                            record["stale_times"].append(now)
                            self.write_state()
                            self.emit_payload(key, "stale")
                        else:
                            self.emit_payload(key, "ok")
                        continue

                    # change detected
                    if raw != record.get("last_value"):
                        now = now_iso()
                        record["last_value"] = raw
                        record["stale_count"] = 0
                        record["last_changed"] = now
                        # when a new value is captured during the day, ensure completed flag stays False
                        record["completed"] = False
                        record["emitted_completed"] = False
                        self.cache[key] = record
                        self.write_state()

                        if parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD)):
                            record["stale_times"].append(now)
                            self.write_state()
                            self.emit_payload(key, "stale")
                        else:
                            self.emit_payload(key, "ok")
                    else:
                        # unchanged -> potentially become stale
                        record["stale_count"] = record.get("stale_count", 0) + 1
                        self.cache[key] = record
                        self.write_state()

                        ts_behind = parsed_dt and (datetime.now() - parsed_dt > timedelta(minutes=STALE_THRESHOLD))
                        if record["stale_count"] >= STALE_THRESHOLD or ts_behind:
                            now = now_iso()
                            record.setdefault("stale_times", []).append(now)
                            self.cache[key] = record
                            self.write_state()
                            self.emit_payload(key, "stale")
                        # else keep quiet to avoid UI churn

                except Exception as e:
                    logger.exception("per-url handling error for %s: %s", key, e)
                    # emit generic error so UI shows issue
                    try:
                        self.emit_payload(key, "error")
                    except Exception:
                        logger.exception("emit failure after per-url exception")

            # end for all URLs in cycle

            # schedule next run to keep 60-second cadence
            next_run += 60
            # avoid drift explosion
            if next_run < time.time() - 60:
                next_run = time.time() + 60

            elapsed = time.time() - cycle_start
            logger.info("1-min cycle elapsed: %.2f sec", elapsed)

        # cleanup driver if loop exits
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

def start_threads(socketio=None):
    w = Worker(socketio)
    t = threading.Thread(target=w.monitor, daemon=True)
    t.start()
    logger.info("Started scraping_1min worker")

# if run standalone for debugging
if __name__ == "__main__":
    start_threads(None)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping scraper (keyboard interrupt)")
