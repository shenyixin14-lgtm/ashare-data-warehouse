#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A-Share Financial Data Warehouse
================================

A SQLite-based data warehouse for A-share (Chinese) equities, serving as a
single source of truth for quantitative research. It integrates three data
sources and aligns them with point-in-time correctness to prevent look-ahead
bias at the data layer.

Tables
------
    daily_price   One row per (stock, trading day): OHLCV + turnover.
    financials    One row per (stock, report period): ROE, margins, leverage,
                  growth. Fundamentals are reported quarterly.
    disclosure    One row per (stock, report period): the ACTUAL publication
                  date of each report, reverse-engineered from cninfo.com.cn.
                  This is the key to point-in-time alignment.

Design highlights
-----------------
    - Composite primary keys enforce data integrity at the schema level:
      duplicates are rejected on write, not cleaned up after the fact.
    - Idempotent UPSERT pipeline: re-running never produces duplicates,
      making incremental daily updates safe to repeat.
    - Point-in-time alignment via real disclosure dates: on any trading day,
      only fundamentals that were ACTUALLY PUBLISHED by that day are visible.
      Using the report period (e.g. Q1 ending 03-31) instead of the real
      publish date (e.g. late April) would leak future information -- the same
      class of look-ahead bias controlled for in the backtesting framework,
      here eliminated at the data source.

Author: shenyixin
"""

import os
import time
import sqlite3
import pandas as pd
import akshare as ak


# ============================================================
# Configuration
# ============================================================
DB_PATH = "stock.db"
ADJUST  = "qfq"          # forward-adjusted prices (needed for continuous series)
SLEEP   = 0.5            # delay between requests (rate limiting)
RETRIES = 2             # retries per stock on fetch failure

# Column order for the price table (must match the schema; ? placeholders are
# positional, so order matters).
PRICE_COLS = ['code', 'date', 'open', 'high', 'low', 'close',
              'volume', 'amount', 'outstanding_share', 'turnover']

# The four fundamental factors we keep (quality / profitability / leverage / growth).
FIN_FIELDS = {
    '净资产收益率(ROE)': 'roe',
    '销售净利率':        'net_margin',
    '资产负债率':        'debt_ratio',
    '营业总收入增长率':   'revenue_growth',
}


# ============================================================
# 1. Schema -- create the three tables
# ============================================================
def create_tables(conn):
    """Create price / financials / disclosure tables if absent.

    code is TEXT (A-share codes have leading zeros that integers would drop).
    Each table uses a composite primary key so duplicates are rejected on write.
    """
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS daily_price (
        code               TEXT NOT NULL,
        date               TEXT NOT NULL,
        open               REAL,
        high               REAL,
        low                REAL,
        close              REAL,
        volume             REAL,
        amount             REAL,
        outstanding_share  REAL,
        turnover           REAL,
        PRIMARY KEY (code, date)
    );

    CREATE TABLE IF NOT EXISTS financials (
        code            TEXT NOT NULL,
        report_date     TEXT NOT NULL,      -- period end, e.g. 2024-03-31
        roe             REAL,
        net_margin      REAL,
        debt_ratio      REAL,
        revenue_growth  REAL,
        PRIMARY KEY (code, report_date)
    );

    CREATE TABLE IF NOT EXISTS disclosure (
        code          TEXT NOT NULL,
        report_date   TEXT NOT NULL,        -- period end, e.g. 2024-03-31
        publish_date  TEXT NOT NULL,        -- ACTUAL publication date
        PRIMARY KEY (code, report_date)
    );
    """)
    conn.commit()


def create_indexes(conn):
    """Index by query pattern.

    The (code, date) primary key already indexes per-stock time-series lookups.
    We add a date index to speed up cross-sectional queries (all stocks on a
    given day) -- the basis for computing cross-sectional Rank IC in backtests.
    Columns used only for computation (volume, close, ...) are NOT indexed:
    they are pulled into pandas and computed there, never used as query filters,
    so indexing them would only slow writes and waste space.
    """
    conn.execute("CREATE INDEX IF NOT EXISTS idx_price_date ON daily_price(date);")
    conn.commit()


# ============================================================
# 2. Price data -- fetch and incremental UPSERT
# ============================================================
def fetch_price(code, start_date=None, end_date=None, adjust=ADJUST):
    """Fetch daily OHLCV for one stock from AkShare.

    stock_zh_a_daily returns columns already matching our schema, so no column
    mapping is needed. start_date / end_date are 'YYYYMMDD'; omit for full history.
    """
    name = ('sh' + code) if code.startswith('6') else ('sz' + code)
    kwargs = {'symbol': name, 'adjust': adjust}
    if start_date:
        kwargs['start_date'] = start_date
    if end_date:
        kwargs['end_date'] = end_date
    raw = ak.stock_zh_a_daily(**kwargs)
    raw['code'] = code.zfill(6)
    return raw


def upsert_prices(df, conn, table="daily_price"):
    """Batch UPSERT a price DataFrame.

    Insert rows; on (code, date) conflict, update instead. This makes the
    operation idempotent -- repeated runs update existing rows (no-op if
    unchanged) and insert new ones, never erroring or duplicating.
    """
    df = df.copy()
    df['code'] = df['code'].astype(str).str.zfill(6)
    df = df[PRICE_COLS]

    placeholders = ', '.join(['?'] * len(PRICE_COLS))
    col_names    = ', '.join(PRICE_COLS)
    update_set   = ', '.join([f"{c}=excluded.{c}"
                              for c in PRICE_COLS if c not in ('code', 'date')])
    sql = f"""
    INSERT INTO {table} ({col_names})
    VALUES ({placeholders})
    ON CONFLICT(code, date) DO UPDATE SET
        {update_set}
    """
    conn.executemany(sql, df.values.tolist())
    conn.commit()
    return len(df)


def update_price_one(code, conn, adjust=ADJUST):
    """Incrementally update one stock: fetch only dates after the latest in DB."""
    code = code.zfill(6)
    r = pd.read_sql("SELECT MAX(date) AS last_date FROM daily_price WHERE code=?;",
                    conn, params=(code,))
    last_date = r['last_date'][0]
    start = None if last_date is None else \
        (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y%m%d')

    df = fetch_price(code, start_date=start, adjust=adjust)
    if len(df) == 0:
        return 0
    return upsert_prices(df, conn)


def update_price_all(codes, conn, sleep=SLEEP, retries=RETRIES):
    """Incrementally update many stocks, with retry and per-stock error isolation."""
    stats = {'updated': 0, 'uptodate': 0, 'failed': 0}
    failed = []
    for i, code in enumerate(codes, 1):
        code = str(code).zfill(6)
        ok = False
        for attempt in range(retries):
            try:
                n = update_price_one(code, conn)
                stats['updated' if n > 0 else 'uptodate'] += 1
                ok = True
                break
            except Exception as e:
                print(f"  {code} attempt {attempt+1} failed: {type(e).__name__}")
                time.sleep(2)
        if not ok:
            stats['failed'] += 1
            failed.append(code)
        print(f"progress {i}/{len(codes)}")
        time.sleep(sleep)
    print(f"\nupdated: {stats['updated']} | up-to-date: {stats['uptodate']} | failed: {stats['failed']}")
    return stats, failed


# ============================================================
# 3. Fundamentals -- fetch, reshape wide->long, load
# ============================================================
def fetch_financials(code):
    """Fetch fundamentals for one stock and reshape into long form.

    The source returns a WIDE table (one row per metric, one column per period).
    We select the four metrics we need, transpose to LONG form (one row per
    period, one column per metric), standardize the date format, and rename
    metrics to English -- so it lines up with the price table for joining.
    """
    code = code.zfill(6)
    raw = ak.stock_financial_abstract(symbol=code)

    # Select the four metrics, drop duplicate metric rows (same metric appears
    # under multiple section headers with identical values).
    sub = raw[raw['指标'].isin(FIN_FIELDS.keys())].drop_duplicates(subset=['指标'])

    # Wide -> long: drop the section column, set metric as index, transpose.
    sub = sub.drop(columns=['选项']).set_index('指标').T

    fin = sub.rename(columns=FIN_FIELDS).reset_index().rename(columns={'index': 'report_date'})
    fin.columns.name = None
    # Report period '20240331' -> '2024-03-31' (parse validates it as a real date,
    # then reformat to match the price table's date format).
    fin['report_date'] = pd.to_datetime(fin['report_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    fin['code'] = code
    return fin[['code', 'report_date', 'roe', 'net_margin', 'debt_ratio', 'revenue_growth']]

def update_financials_all(codes, conn, sleep=0.3, retries=2):
    """Fetch and UPSERT fundamentals for many stocks.

    Mirrors update_price_all: per-stock retry, error isolation, progress, and a
    final summary. Uses UPSERT so re-running is idempotent (updates existing rows,
    inserts new ones, never duplicates).
    """
    fin_cols = ['code', 'report_date', 'roe', 'net_margin', 'debt_ratio', 'revenue_growth']
    stats = {'ok': 0, 'failed': 0}
    failed = []

    # Build the UPSERT statement once
    update_set = ', '.join([f"{c}=excluded.{c}"
                            for c in fin_cols if c not in ('code', 'report_date')])
    sql = f"""
    INSERT INTO financials ({', '.join(fin_cols)})
    VALUES ({', '.join(['?'] * len(fin_cols))})
    ON CONFLICT(code, report_date) DO UPDATE SET
        {update_set}
    """

    for i, code in enumerate(codes, 1):
        code = str(code).zfill(6)
        ok = False
        for attempt in range(retries):
            try:
                fin = fetch_financials(code)
                fin = fin.drop_duplicates(subset=['code', 'report_date'])[fin_cols]
                conn.executemany(sql, fin.values.tolist())
                conn.commit()
                stats['ok'] += 1
                ok = True
                break
            except Exception as e:
                print(f"  {code} attempt {attempt+1} failed: {type(e).__name__}")
                time.sleep(2)
        if not ok:
            stats['failed'] += 1
            failed.append(code)
        print(f"progress {i}/{len(codes)}")
        time.sleep(sleep)

    print(f"\nok: {stats['ok']} | failed: {stats['failed']}")
    if failed:
        print("failed:", failed)
    return stats, failed


# ============================================================
# 4. Disclosure dates -- load pre-scraped real publication dates
# ============================================================
# NOTE: disclosure dates are the ACTUAL dates each report was published,
# scraped from cninfo.com.cn (the endpoint was reverse-engineered from browser
# traffic; the WAF is bypassed with a session warm-up that yields a valid
# cookie). Scraping lives in a separate script; here we just load the result.
def load_disclosure(csv_path, conn):
    """Load scraped disclosure dates (code, report_date, publish_date) into DB."""
    df = pd.read_csv(csv_path)
    df['code'] = df['code'].astype(str).str.zfill(6)
    df = df.drop_duplicates(subset=['code', 'report_date'])
    df.to_sql('disclosure', conn, if_exists='append', index=False)
    return len(df)


# ============================================================
# 5. Point-in-time alignment -- the core of the warehouse
# ============================================================
def point_in_time_panel(conn, code=None, start=None, end=None):
    """Return a price panel joined with point-in-time-correct fundamentals.

    For each trading day, attach the fundamentals of the most recent report
    that had ACTUALLY BEEN PUBLISHED by that day. This is enforced by matching
    on publish_date (the real publication date), not report_date -- so a Q1
    report (period end 03-31, published late April) only becomes visible on the
    first trading day AFTER its April publication, never on 03-31. This removes
    look-ahead bias at the data source.

    Mechanism: a correlated subquery finds, per (stock, trading day), the latest
    publish_date <= that day; the outer joins pull in that report's fundamentals.
    """
    sql = """
    SELECT p.code, p.date, p.close,
           d.report_date, f.roe, f.net_margin, f.debt_ratio, f.revenue_growth
    FROM daily_price p
    JOIN disclosure d
      ON d.code = p.code
     AND d.publish_date = (
            SELECT MAX(d2.publish_date)
            FROM disclosure d2
            WHERE d2.code = p.code
              AND d2.publish_date <= p.date
         )
    JOIN financials f
      ON f.code = d.code AND f.report_date = d.report_date
    WHERE 1=1
    """
    params = []
    if code:
        sql += " AND p.code = ?"
        params.append(code.zfill(6))
    if start:
        sql += " AND p.date >= ?"
        params.append(start)
    if end:
        sql += " AND p.date <= ?"
        params.append(end)
    sql += " ORDER BY p.code, p.date"
    return pd.read_sql(sql, conn, params=tuple(params))


# ============================================================
# 6. Query helpers
# ============================================================
def get_prices(conn, code=None, start=None, end=None):
    """Read raw prices into a DataFrame (data layer serves; pandas computes)."""
    sql = "SELECT * FROM daily_price WHERE 1=1"
    params = []
    if code:
        sql += " AND code=?"; params.append(code.zfill(6))
    if start:
        sql += " AND date>=?"; params.append(start)
    if end:
        sql += " AND date<=?"; params.append(end)
    sql += " ORDER BY code, date"
    return pd.read_sql(sql, conn, params=tuple(params))


def summary(conn):
    """Overview of each table: stock count, row count, date range."""
    price = pd.read_sql("""
        SELECT COUNT(DISTINCT code) AS n_stocks, COUNT(*) AS n_rows,
               MIN(date) AS first_date, MAX(date) AS last_date FROM daily_price;""", conn)
    fin = pd.read_sql("SELECT COUNT(*) AS n_rows FROM financials;", conn)
    dis = pd.read_sql("SELECT COUNT(*) AS n_rows FROM disclosure;", conn)
    return {'daily_price': price, 'financials': fin, 'disclosure': dis}


# ============================================================
# Example usage / full pipeline
# ============================================================
if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)

    # --- Step 1: initialize schema and indexes (first run only) ---
    create_tables(conn)
    create_indexes(conn)

    # --- Step 2: the CSI 300 stock universe ---
    # On the very first run, seed the universe however you like (e.g. from an
    # index-constituent list). Afterwards it is simply whatever is in the DB.
    codes = pd.read_sql("SELECT DISTINCT code FROM daily_price;", conn)['code'].tolist()

    # --- Step 3: load / update the three data sources ---
    # Prices: incremental UPSERT (only fetches dates newer than the DB).
    update_price_all(codes, conn)

    # Fundamentals: fetch + UPSERT for all stocks.
    update_financials_all(codes, conn)

    # Disclosure dates: load the pre-scraped file (scraping lives in
    # fetch_disclosure.py, which handles the cninfo.com.cn WAF).
    load_disclosure("disclosure.csv", conn)

    # --- Step 4: inspect ---
    print("Warehouse summary:")
    for name, df in summary(conn).items():
        print(f"  {name}: {df.to_dict('records')}")

    # --- Step 5: point-in-time panel (example: one stock) ---
    panel = point_in_time_panel(conn, code='000001',
                                start='2025-04-10', end='2025-06-30')
    print("\nPoint-in-time panel (000001):")
    print(panel.head(15))

    conn.close()