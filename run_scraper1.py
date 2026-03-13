import sys
import os
import time
import json
import copy
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_week_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 12
BATCH_SIZE = 5
RESTART_EVERY_ROWS = 15
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# Layout Adjustment: A=Symbol, B=Date, C...=Values
WEEK_OUTPUT_START_COL = 3  # Starts at Column C

if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

def get_end_col(start_col, count):
    return col_num_to_letter(start_col + count - 1)

WEEK_START_COL_LETTER = col_num_to_letter(WEEK_OUTPUT_START_COL)      # C
WEEK_END_COL_LETTER = get_end_col(WEEK_OUTPUT_START_COL, EXPECTED_COUNT)  # N

log(f"📍 WEEK start column = {WEEK_START_COL_LETTER}")
log(f"📍 WEEK end column   = {WEEK_END_COL_LETTER}")

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [WEEK] [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:100]}")
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver: driver.quit()
    except: pass
    driver = None
    time.sleep(3)

# ---------------- HELPERS ---------------- #
def wait_for_page_ready(drv, timeout=25):
    WebDriverWait(drv, timeout).until(lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"])

def get_visible_value_elements(drv):
    elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    return [el.text.strip() for el in elems if el.is_displayed() and el.text.strip()]

def stable_read_values(drv, pause=1.2):
    first = get_visible_value_elements(drv)
    time.sleep(pause)
    second = get_visible_value_elements(drv)
    return second if len(second) >= len(first) else first

def bs4_fallback_values(drv):
    try:
        soup = BeautifulSoup(drv.page_source, "html.parser")
        raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
        return [el.get_text(strip=True) for el in raw_values if el.get_text(strip=True)]
    except: return []

def validate_week(values):
    return bool(values) and len(values) >= EXPECTED_COUNT

# ---------------- SCRAPER ---------------- #
def scrape_week(url):
    if not url: return []
    log(f"    📡 Navigating WEEK: {url}")
    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)
            wait_for_page_ready(drv, timeout=25)
            try:
                WebDriverWait(drv, 20).until(lambda d: len(get_visible_value_elements(d)) >= EXPECTED_COUNT)
            except: pass
            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(1); drv.execute_script("window.scrollTo(0, 0);"); time.sleep(2)
            values = stable_read_values(drv)
            if not validate_week(values):
                drv.refresh(); time.sleep(4)
                values = stable_read_values(drv)
            if not validate_week(values): values = bs4_fallback_values(drv)
            if validate_week(values):
                values = values[:EXPECTED_COUNT]
                log(f"    📊 Found {len(values)} WEEK values")
                return values
        except Exception as e:
            log(f"    ❌ WEEK ERROR: {str(e)[:120]}")
            restart_driver()
    return []

# ---------------- SHEETS ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 WEEK").worksheet("Sheet1")
    return sheet_main, sheet_data

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = sheet_main.col_values(1)
    url_week_list = sheet_main.col_values(8)
    log(f"✅ WEEK Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows, sheet_data
    if not batch_list: return True
    log(f"🚀 UPLOADING WEEK BATCH: Sending {buffered_rows} rows...")
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option="RAW")
            batch_list = []; buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ Retry {attempt+1}: {str(e)[:100]}")
            time.sleep(5)
            sheet_main, sheet_data = connect_sheets()
    return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")
        u_week = url_week_list[i].strip() if i < len(url_week_list) and url_week_list[i].startswith("http") else None
        vals_week = scrape_week(u_week)
        
        row_idx = i + 1
        week_range = f"{WEEK_START_COL_LETTER}{row_idx}:{WEEK_END_COL_LETTER}{row_idx}"
        
        # Compact Column Mapping: A=Name, B=Date, C-N=Values
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": week_range, "values": [vals_week + [""] * (EXPECTED_COUNT - len(vals_week))]})

        buffered_rows += 1
        with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        
        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()
        if buffered_rows >= BATCH_SIZE:
            if not flush_batch(): break
            restart_driver()
finally:
    flush_batch()
    restart_driver()
    log("🏁 WEEK Shard Completed.")
