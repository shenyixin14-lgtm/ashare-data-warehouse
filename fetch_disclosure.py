#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Disclosure-Date Scraper (cninfo.com.cn)
=======================================

Fetches the ACTUAL publication date of each A-share financial report, which is
the key input for point-in-time alignment in the data warehouse. These dates
are not available from the usual data libraries (the packaged endpoint was
broken), so the underlying cninfo.com.cn endpoint is called directly.

Bypassing the WAF
-----------------
Raw requests to the endpoint return 403. The site is fronted by a WAF that
rejects requests not resembling a real browser. Two findings, both discovered
by inspecting real browser traffic, make it work:
    1. A session WARM-UP: GET the disclosure page first so the server issues a
       fresh session cookie (JSESSIONID); the data POST then reuses it.
    2. A minimal, non-standard User-Agent passes, while a full browser UA is
       actively blocked (the WAF appears to target UA-spoofing scrapers).

Query strategy
--------------
The endpoint returns one report period per (stock, period). Querying the whole
market per period returns tens of thousands of rows across hundreds of pages,
so instead we query per STOCK x per PERIOD: each request returns a single row,
no pagination. For a fixed stock universe this is far more targeted.

Robustness (for unattended long runs)
-------------------------------------
    - Resume: one CSV per stock; already-fetched stocks are skipped on rerun.
    - Retry with back-off on failure; the session is re-warmed each retry.
    - Circuit breaker: several consecutive empty results imply rate-limiting,
      so sleep 10 minutes and continue (rather than burning requests).
    - Empty results are NOT saved, so a rerun re-attempts them.

Output: one CSV per stock in ./disclosure_by_stock/, columns
(code, report_date, publish_date). Merge them and load into the warehouse.

Author: shenyixin
"""

import os
import time
import requests
import pandas as pd
import sqlite3

# ============================================================
# Configuration
# ============================================================
URL = "https://www.cninfo.com.cn/new/information/getPrbookInfo"
WARMUP_URL = "https://www.cninfo.com.cn/new/commonUrl?url=data/yypl"

# Minimal UA on purpose: a full browser UA is blocked by the WAF, this passes.
HEADERS = {
    'User-Agent': '...',
    'Referer': WARMUP_URL,
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}

DB_PATH  = "stock.db"
SAVE_DIR = "disclosure_by_stock"          # one CSV per stock (enables resume)

# Report periods to fetch: all four quarter-ends, 2015 to present.
PERIODS = []
for _y in range(2015, 2027):
    for _md in ['03-31', '06-30', '09-30', '12-31']:
        PERIODS.append(f"{_y}-{_md}")
PERIODS = [p for p in PERIODS if p <= '2026-09-15']

# Field names in the JSON response.
F_REPORT_DATE  = 'f001d_0102'             # report period end
F_PUBLISH_DATE = 'f006d_0102'             # ACTUAL publication date


# ============================================================
# Session with warm-up (WAF bypass)
# ============================================================
def new_session(max_try=10):
    """Create a session and warm it up until it holds a valid cookie.

    The GET to the disclosure page makes the server issue a fresh JSESSIONID;
    the subsequent data POST reuses it. Warm-up is retried because the WAF
    rejects some attempts at random.
    """
    for _ in range(max_try):
        s = requests.Session()
        s.headers.update(HEADERS)
        try:
            w = s.get(WARMUP_URL, timeout=10)
            if w.status_code == 200 and s.cookies.get_dict():
                return s
        except Exception:
            pass
        time.sleep(5)
    return s                              # return anyway; per-request retry handles it


# ============================================================
# Fetch one stock (all report periods)
# ============================================================
def fetch_stock(session, code, sleep=0.15):
    """Fetch disclosure dates for one stock across all report periods.

    One request per period; each returns a single row (or none if the stock had
    not yet listed). Failed requests re-warm the session and retry with back-off.
    """
    out = []
    for period in PERIODS:
        data = {
            'sectionTime': period, 'firstTime': '', 'lastTime': '', 'market': 'szsh',
            'stockCode': code, 'orderClos': '', 'isDesc': '', 'pagesize': 10, 'pagenum': 1,
        }
        for attempt in range(6):
            try:
                r = session.post(URL, data=data, timeout=15)
                if r.status_code == 200:
                    for row in r.json().get('prbookinfos', []):
                        out.append({
                            'code':         row['seccode'],
                            'report_date':  row[F_REPORT_DATE],
                            'publish_date': row[F_PUBLISH_DATE],
                        })
                    break
            except Exception:
                pass
            session = new_session()
            time.sleep(min(2 * attempt + 2, 30))     # back-off, capped at 30s
        time.sleep(sleep)
    return session, out


# ============================================================
# Main loop: resume + circuit breaker + live progress
# ============================================================
def main():
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Stock universe = whatever is already in the price table.
    conn = sqlite3.connect(DB_PATH)
    codes = pd.read_sql("SELECT DISTINCT code FROM daily_price;", conn)['code'].tolist()
    conn.close()

    total = len(codes)
    done = len([f for f in os.listdir(SAVE_DIR) if f.endswith('.csv')])
    print(f"{total} stocks x {len(PERIODS)} periods; {done} already done", flush=True)

    session = new_session()
    empty_streak = 0                      # consecutive empty results -> rate-limited

    for i, code in enumerate(codes, 1):
        code = str(code).zfill(6)
        fpath = os.path.join(SAVE_DIR, f"{code}.csv")

        if os.path.exists(fpath):         # resume: skip finished stocks
            continue

        session, records = fetch_stock(session, code)

        if len(records) == 0:
            # Empty: count toward circuit breaker, do NOT save (rerun retries it).
            empty_streak += 1
            print(f"[{i}/{total}] {code}: 0 rows (empty streak {empty_streak})", flush=True)
            if empty_streak >= 3:
                print("    Likely rate-limited; sleeping 10 min...", flush=True)
                time.sleep(600)
                session = new_session()
                empty_streak = 0
            continue

        empty_streak = 0
        pd.DataFrame(records).to_csv(fpath, index=False)
        print(f"[{i}/{total}] {code}: {len(records)} rows", flush=True)

    print("===== done =====", flush=True)


# ============================================================
# Merge per-stock files into one table
# ============================================================
def merge(out_csv="disclosure.csv"):
    """Combine all per-stock CSVs into a single file, ready to load into the DB."""
    frames = [pd.read_csv(os.path.join(SAVE_DIR, f))
              for f in os.listdir(SAVE_DIR) if f.endswith('.csv')]
    df = pd.concat(frames, ignore_index=True)
    df['code'] = df['code'].astype(str).str.zfill(6)
    df = df.drop_duplicates(subset=['code', 'report_date'])
    df.to_csv(out_csv, index=False)
    print(f"Merged {len(df)} rows into {out_csv}")
    return df


if __name__ == "__main__":
    main()
    # merge()      # run after main() completes to produce disclosure.csv
