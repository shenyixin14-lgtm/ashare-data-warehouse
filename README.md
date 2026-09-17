# A-Share Point-in-Time Financial Data Warehouse

A SQLite data warehouse for A-share (Chinese) equities that integrates daily prices, quarterly fundamentals, and **actual report disclosure dates**, aligning them with **point-in-time correctness** to eliminate look-ahead bias at the data source. Built as the data layer for a [multi-factor backtesting framework](https://github.com/shenyixin14-lgtm/ashare-multifactor-backtest), covering a ~288-stock CSI 300 universe from 2015 to 2026.

## Why this exists

Backtests are only as trustworthy as the data feeding them. A subtle but common way to corrupt a fundamental backtest is to join financial data on its **reporting period** (e.g. a Q1 report is stamped `2024-03-31`) when in reality that report is not published until **late April**. Using it any earlier means trading on information that did not yet exist — look-ahead bias.

Most hobby projects approximate this with a fixed reporting lag (e.g. "assume everything is published 90 days after period end"). That introduces systematic error: some firms report early, some late, and a fixed lag is wrong for both. This warehouse instead aligns fundamentals on the **real publication date of each report**, so every trading day sees only what was genuinely knowable then.

## Architecture

```
AkShare ──► daily_price ─┐
                         │
AkShare ──► financials ──┼──► point-in-time join ──► aligned panel
                         │      (on real publish date)
cninfo  ──► disclosure ──┘
```

| Table | Grain | Key columns |
|-------|-------|-------------|
| `daily_price` | one row per (stock, trading day) | OHLCV, turnover |
| `financials` | one row per (stock, report period) | ROE, net margin, debt ratio, revenue growth |
| `disclosure` | one row per (stock, report period) | actual publication date |

Each table uses a **composite primary key** so duplicates are rejected on write, not cleaned up afterward.

## Key features

**Point-in-time alignment.** For each trading day, a correlated-subquery SQL join attaches the fundamentals of the most recent report *actually published* by that day — matched on `publish_date`, never on `report_date`. A Q1 report (period end March 31, published late April) becomes visible only on the first trading day after its April publication.

**Idempotent incremental pipeline.** Daily updates use `INSERT ... ON CONFLICT DO UPDATE` (UPSERT), so re-running is always safe: existing rows are updated (no-op if unchanged), new rows inserted, never a duplicate or an error. Incremental fetches only pull dates after the latest already in the database.

**Real disclosure dates.** The publication dates come from cninfo.com.cn (the official CSRC-designated disclosure platform). When the packaged data API broke, the underlying endpoint was recovered through reverse-engineering of browser traffic (including a session warm-up step to obtain a valid cookie past the WAF), retrieving exact publication dates for all 288 stocks rather than settling for an estimate.

## The point-in-time query

The core of the warehouse is this join. For every trading day, the correlated subquery finds the latest report whose real publication date is on or before that day:

```sql
SELECT p.code, p.date, p.close,
       d.report_date, f.roe, f.net_margin, f.debt_ratio, f.revenue_growth
FROM daily_price p
JOIN disclosure d
  ON d.code = p.code
 AND d.publish_date = (
        SELECT MAX(d2.publish_date)
        FROM disclosure d2
        WHERE d2.code = p.code
          AND d2.publish_date <= p.date        -- only reports known by this day
     )
JOIN financials f
  ON f.code = d.code AND f.report_date = d.report_date
ORDER BY p.code, p.date;
```

Example output for one stock around its Q1 2025 disclosure (published 2025-04-19):

| date | report_date | roe |
|------|-------------|-----|
| 2025-04-18 | 2024-12-31 | 10.08 |
| 2025-04-21 | 2025-03-31 | 2.80 |

The fundamentals switch to the Q1 report only *after* its real publication date — not on the March 31 period end.

## Design notes

- **Why SQLite, if pandas is faster at this scale?** At ~1M rows, pandas computes faster in memory. The database is not for speed — it is for schema-enforced integrity, safe incremental updates, multi-source joins, and a single source of truth. Storage and retrieval are the database's job; computation stays in pandas.
- **Codes are stored as TEXT.** A-share tickers have leading zeros (`000300`); integers would silently drop them.
- **Indexing by query pattern.** The primary key covers per-stock time-series lookups; a secondary index on `date` accelerates cross-sectional queries (all stocks on one day). Computation-only columns are not indexed.

## Stack

Python · pandas · SQLite · AkShare · requests

## Files

- `ashare_data_warehouse.py` — schema, pipelines, and point-in-time alignment
- `fetch_disclosure.py` — disclosure-date scraper (reverse-engineered endpoint, with retry, back-off, and resume)
