#!/usr/bin/env python3
# sca_1sec.py (updated) — Option A: daily state JSON rotation, per-URL threads
# - New daily state file: state/monitor_state_1sec_YYYY-MM-DD.json
# - Stop scraping after end time for a URL, emit final completed payload (last_value + last_changed)
# - Do not re-scrape completed URLs until next day
# - Resume next day's scraping after rotating state file
# - Thread-per-URL model preserved

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
CHROMEDRIVER_PATH = r"C:\Users\Hritikraj.arya\.wdm\drivers\chromedriver\win64\142.0.7444.134\chromedriver-win32\chromedriver.exe"
LOG_DIR = "logs"
STATE_DIR = "state"
CONFIG_DIR = "config"
URL_DICT_PATH = os.path.join(CONFIG_DIR, "url_dict_1sec.json")
NAME_MAPPING_PATH = os.path.join(CONFIG_DIR, "url_name_mapping1sec.json")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

DEFAULT_INTERVAL = 1
RESTART_WAIT = 1  # seconds

# ---------------- LOGGER ----------------
logger = logging.getLogger("scraping_1sec")
logger.setLevel(logging.INFO)
logger.propagate = False
log_filename = os.path.join(LOG_DIR, f"scraping_1sec_{datetime.now().strftime('%Y%m%d')}.log")
if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_filename for h in logger.handlers):
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

# ---------------- LOAD CONFIGS ----------------
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("Failed to load JSON: %s | %s", path, e)
        raise

try:
    url_dict = load_json(URL_DICT_PATH)
except Exception as e:
    logger.exception("Failed to load %s: %s", URL_DICT_PATH, e)
    raise SystemExit(1)

try:
    ui_name_mapping = load_json(NAME_MAPPING_PATH)
except Exception as e:
    logger.exception("Failed to load %s: %s", NAME_MAPPING_PATH, e)
    # fallback: identity mapping
    ui_name_mapping = {k: k for k in url_dict.keys()}

# ---------------- STATE (daily file) ----------------
def state_filename_for_day(day_str):
    return os.path.join(STATE_DIR, f"monitor_state_1sec_{day_str}.json")

def load_state_file(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("load_state_file failed for %s", path)
        return {}

def save_state_file(path, state):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        logger.exception("save_state_file failed for %s", path)

state_lock = threading.Lock()

# ---------------- helpers ----------------
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# timestamp parser (copied/adapted)
def parse_reported_ts(raw_text):
    if not raw_text or not isinstance(raw_text, str):
        return None
    txt = raw_text.strip()
    txt = re.sub(r"(?i)\\bas\\s*on\\b", "", txt).strip()
    txt = txt.replace("\\u00A0", " ").replace("\\u200B", "").strip()

    if "|" in txt:
        parts = [p.strip() for p in txt.split("|") if p.strip()]
    else:
        parts = [p.strip() for p in re.split(r"\\s{2,}", txt) if p.strip()]

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
        m = re.match(r"^(\\d{1,2}):(\\d{1,2})(?::\\d{1,2})?\\s*(am|pm|AM|PM)?$", time_part.strip())
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
        opts.add_argument("--disable-extensions")
        service = Service(CHROMEDRIVER_PATH)
        d = webdriver.Chrome(service=service, options=opts)
        d.implicitly_wait(0)
        return d

# ---------------- URLWorker (per-URL thread) ----------------
class URLWorker(threading.Thread):
    def __init__(self, key, cfg, state_file, state_cache, driver_manager, socketio=None):
        super().__init__(daemon=True)
        self.key = key
        self.cfg = cfg
        self.state_file = state_file
        self.state_cache = state_cache  # shared dict reference
        self.dm = driver_manager
        self.socketio = socketio

        self.interval = int(cfg.get("interval", DEFAULT_INTERVAL))
        if self.interval < 1:
            self.interval = DEFAULT_INTERVAL

        self.selector = cfg.get("selector", "")
        self.typ = cfg.get("type", "timestamp")
        self.url = cfg.get("url")
        self.key_id = cfg.get("key_id")
        self.tab = cfg.get("tab", "tab1sec")

        self.driver = None
        self.stop_event = threading.Event()
        self.next_run = time.time()
        self.fail_count = 0
        self.MAX_FAILS_BEFORE_RESTART = 3

    def ensure_driver(self):
        if self.driver is None:
            try:
                self.driver = self.dm.get_driver()
                logger.info("[%s] driver created", self.key)
            except Exception as e:
                logger.exception("[%s] driver create failed: %s", self.key, e)
                self.driver = None

    def safe_quit_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

    def emit_payload(self, status):
        ui_name = ui_name_mapping.get(self.key, self.key)
        with state_lock:
            entry = self.state_cache.get(self.key, {})
            payload = {
                "checklist": ui_name,
                "key_id": self.cfg.get("key_id"),
                "status": status,
                "last_changed": entry.get("last_changed", ""),
                "last_value": entry.get("last_value"),
                "tab": self.cfg.get("tab", "tab1sec"),
            }
        logger.info("EMIT -> %s", payload)
        if self.socketio:
            try:
                self.socketio.emit("update_status", payload)
            except Exception:
                logger.exception("socket emit failed for %s", self.key)

    def update_cache_ok(self, raw):
        with state_lock:
            rec = self.state_cache.setdefault(self.key, {})
            changed = (rec.get("last_value") != raw)
            rec["last_value"] = raw
            rec.setdefault("stale_count", 0)
            rec.setdefault("stale_times", [])
            if changed:
                rec["stale_count"] = 0
                rec["last_changed"] = now_iso()
                rec["completed"] = False
                rec["emitted_completed"] = False
            else:
                rec["stale_count"] = rec.get("stale_count", 0) + 1
            rec["status"] = "ok"
            save_state_file(self.state_file, self.state_cache)

    def update_cache_status(self, status):
        with state_lock:
            rec = self.state_cache.setdefault(self.key, {})
            rec["status"] = status
            save_state_file(self.state_file, self.state_cache)

    def extract_ts(self):
        if not self.driver or not self.selector:
            return None
        try:
            el = self.driver.find_element(By.CSS_SELECTOR, self.selector)
            txt = el.text.strip()
            return txt or None
        except Exception:
            parts = [p.strip() for p in self.selector.split(",") if p.strip()]
            for p in parts:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, p)
                    txt = el.text.strip()
                    if txt:
                        return txt
                except Exception:
                    continue
            return None

    def extract_tickervalue(self):
        try:
            script = """
                try {
                    const els = document.querySelectorAll(arguments[0]);
                    return Array.from(els).map(e => e.textContent.trim());
                } catch(e) {
                    return [];
                }
            """
            vals = self.driver.execute_script(script, self.selector)
            if vals and isinstance(vals, list):
                return vals
        except Exception:
            pass
        return []

    def _in_time_window(self):
        now = datetime.now().time()
        start_s = self.cfg.get("start")
        end_s = self.cfg.get("end")
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

    def fetch_and_process(self):
        # check time window first
        in_window, window_state = self._in_time_window()
        if window_state == "skip":
            return "skip"
        if window_state == "completed":
            # mark completed and persist (once)
            with state_lock:
                rec = self.state_cache.setdefault(self.key, {})
                if not rec.get("completed"):
                    rec["completed"] = True
                    if not rec.get("last_changed"):
                        rec["last_changed"] = now_iso()
                    rec["status"] = "completed"
                    rec["emitted_completed"] = False
                    save_state_file(self.state_file, self.state_cache)
                    # emit final completed payload
                    self.emit_payload("completed")
            return "completed"

        # ensure driver exists
        if self.driver is None:
            return "driver-missing"

        try:
            try:
                self.driver.get(self.url)
            except Exception as e:
                logger.error("[%s] load fail: %s", self.key, e)
                return "load-error"

            # tiny render pause
            time.sleep(0.08)

            if self.typ == "tickervalue":
                vals = self.extract_tickervalue()
                if not vals:
                    return "invalid format"
                raw = vals
            else:
                txt = self.extract_ts()
                if not txt:
                    return "invalid format"
                raw = txt

            # update cache with OK/raw
            self.update_cache_ok(raw)
            return "ok"
        except Exception as e:
            logger.exception("[%s] fetch exception: %s", self.key, e)
            return "error"

    def run(self):
        logger.info("[%s] URLWorker started", self.key)
        while not self.stop_event.is_set():
            nowt = time.time()
            if nowt < self.next_run:
                time.sleep(0.002)
                continue

            # quick completed check
            with state_lock:
                entry = self.state_cache.get(self.key, {})
                if entry.get("completed"):
                    # emit completed one-time per process run if not yet emitted
                    if not entry.get("emitted_completed"):
                        try:
                            self.emit_payload("completed")
                        except Exception:
                            logger.exception("[%s] emit completed failed", self.key)
                        # mark emitted to avoid spamming
                        entry["emitted_completed"] = True
                        save_state_file(self.state_file, self.state_cache)
                    # sleep until next interval (no fetching)
                    self.next_run += self.interval
                    if self.next_run < time.time() - 5:
                        self.next_run = time.time() + self.interval
                    time.sleep(0.001)
                    continue

            # ensure driver
            if self.driver is None:
                self.ensure_driver()
                if self.driver is None:
                    self.update_cache_status("driver-create-failed")
                    time.sleep(RESTART_WAIT)
                    self.next_run += self.interval
                    continue

            status = self.fetch_and_process()
            if status != "ok":
                # handle failures and restarts
                self.fail_count += 1
                self.update_cache_status(status)
                logger.warning("[%s] fetch status: %s (fail_count=%d)", self.key, status, self.fail_count)
                if self.fail_count >= self.MAX_FAILS_BEFORE_RESTART:
                    logger.info("[%s] restarting driver after %d failures", self.key, self.fail_count)
                    self.safe_quit_driver()
                    self.fail_count = 0
                    time.sleep(RESTART_WAIT)
            else:
                self.fail_count = 0

            # schedule next run precisely
            self.next_run += self.interval
            if self.next_run < time.time() - 5:
                self.next_run = time.time() + self.interval

        logger.info("[%s] URLWorker stopping, quitting driver", self.key)
        self.safe_quit_driver()

    def stop(self):
        self.stop_event.set()
        self.safe_quit_driver()


# ---------------- Worker (controller) ----------------
class Worker:
    def __init__(self, socketio=None):
        self.socketio = socketio
        self.dm = DriverManager()
        self.threads = {}
        self.stop_event = threading.Event()

        # day tracked in this process
        self.state_day = datetime.now().strftime("%Y-%m-%d")
        self.state_file = state_filename_for_day(self.state_day)

        # load today's state if exists, else try to fall back to last existing file (optional)
        self.state_cache = load_state_file(self.state_file) or {}

        # ensure keys exist with defaults but preserve last_value/last_changed
        for k in url_dict.keys():
            if k not in self.state_cache:
                self.state_cache[k] = {
                    "last_value": None,
                    "stale_count": 0,
                    "last_changed": "",
                    "stale_times": [],
                    "completed": False,
                    "emitted_completed": False,
                    "status": "not-started"
                }
            else:
                self.state_cache[k].setdefault("last_value", None)
                self.state_cache[k].setdefault("stale_count", 0)
                self.state_cache[k].setdefault("last_changed", "")
                self.state_cache[k].setdefault("stale_times", [])
                self.state_cache[k].setdefault("completed", False)
                self.state_cache[k].setdefault("emitted_completed", False)
                self.state_cache[k].setdefault("status", "not-started")

        # persist initial state file
        save_state_file(self.state_file, self.state_cache)

        # create URLWorker threads
        for key, cfg in url_dict.items():
            w = URLWorker(key, cfg, self.state_file, self.state_cache, self.dm, socketio)
            self.threads[key] = w

    def rotate_state_if_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.state_day:
            logger.info("New day detected: rotating state file from %s -> %s", self.state_day, today)
            # finalize current file (already persisted), then create new day's file
            self.state_day = today
            self.state_file = state_filename_for_day(self.state_day)
            # initialize fresh cache for the new day but keep previous day's file intact
            self.state_cache = {}
            for k in url_dict.keys():
                self.state_cache[k] = {
                    "last_value": None,
                    "stale_count": 0,
                    "last_changed": "",
                    "stale_times": [],
                    "completed": False,
                    "emitted_completed": False,
                    "status": "not-started"
                }
            save_state_file(self.state_file, self.state_cache)
            # restart threads with new shared cache/state_file
            logger.info("Restarting URLWorkers with new state file")
            self.stop_workers()
            # recreate URLWorkers
            self.threads = {}
            for key, cfg in url_dict.items():
                w = URLWorker(key, cfg, self.state_file, self.state_cache, self.dm, self.socketio)
                self.threads[key] = w
            self.start_workers()

    def start_workers(self):
        for key, w in self.threads.items():
            if not w.is_alive():
                w.start()
                logger.info("started URLWorker for %s", key)

    def stop_workers(self):
        for key, w in self.threads.items():
            try:
                w.stop()
            except Exception:
                logger.exception("failed stopping worker %s", key)

    def emit_payload(self, checklist_key, status):
        ui_name = ui_name_mapping.get(checklist_key, checklist_key)
        with state_lock:
            entry = self.state_cache.get(checklist_key, {})
            payload = {
                "checklist": ui_name,
                "key_id": url_dict.get(checklist_key, {}).get("key_id"),
                "status": status,
                "last_changed": entry.get("last_changed", ""),
                "last_value": entry.get("last_value"),
                "tab": url_dict.get(checklist_key, {}).get("tab", "tab1sec")
            }
        logger.info("EMIT -> %s", payload)
        if self.socketio:
            try:
                self.socketio.emit("update_status", payload)
            except Exception:
                logger.exception("socket emit failed")

    def monitor(self):
        logger.info("Worker.monitor starting - spawning URLWorkers")
        # start threads
        self.start_workers()

        next_run = time.time()
        try:
            while not self.stop_event.is_set():
                if time.time() < next_run:
                    time.sleep(0.02)
                    continue

                # rotate state if new day detected; this will restart workers for new day
                try:
                    self.rotate_state_if_new_day()
                except Exception:
                    logger.exception("rotate_state_if_new_day failed")

                # emit current status for each URL every second (controller heartbeat)
                keys = list(url_dict.keys())
                for key in keys:
                    with state_lock:
                        entry = self.state_cache.get(key, {})
                        # if completed -> ensure last_changed present and emit completed
                        if entry.get("completed"):
                            if not entry.get("last_changed"):
                                entry["last_changed"] = now_iso()
                                save_state_file(self.state_file, self.state_cache)
                            # emit completed (don't spam; URLWorker also emits once on transition)
                            self.emit_payload(key, "completed")
                            continue

                        # if before start -> emit skip/not-started optionally
                        in_window, window_state = self._in_time_window_for_key(key)
                        if window_state == "skip":
                            self.emit_payload(key, "skip")
                            continue

                        # otherwise emit last known status
                        status = entry.get("status", "unknown")
                        self.emit_payload(key, status)

                next_run += DEFAULT_INTERVAL
                if next_run < time.time() - 5:
                    next_run = time.time() + DEFAULT_INTERVAL

        except Exception:
            logger.exception("Worker.monitor crashed")
        finally:
            logger.info("Worker.monitor stopping - stopping URLWorkers")
            self.stop_workers()

    # helper used by monitor to check time-window per key
    def _in_time_window_for_key(self, key):
        cfg = url_dict.get(key, {})
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

    def stop(self):
        self.stop_event.set()
        self.stop_workers()


# ---------------- start_threads (naming preserved) ----------------
def start_threads(socketio=None):
    w = Worker(socketio)
    t = threading.Thread(target=w.monitor, daemon=True)
    t.start()
    logger.info("Started scraping_1sec Worker (monitor thread)")
    return w, t

# ---------------- standalone run support ----------------
if __name__ == "__main__":
    worker, thr = start_threads(None)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping sca_1sec...")
        worker.stop()
