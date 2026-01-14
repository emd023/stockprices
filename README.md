# StockPrices – ETL + Daily Movers (Supabase)

A small, reproducible setup to:
- load daily and historical prices into Supabase (`prices_2025`),
- snapshot the **top daily movers** into `movers_daily` (≥ 15% by default),
- query/Export results (CSV/UI later),
- and eventually schedule via cron/GitHub Actions.

---

## Folder layout

StockPrices/
├─ .env # local secrets (NOT committed)
├─ .env.example # template with placeholders (committed)
├─ .gitignore
├─ README.md
├─ requirements.txt
│
├─ Scripts/
│ ├─ daily_prices_2025.py # job: latest trading day → prices_2025
│ ├─ load_range_to_supabase.py # ad-hoc backfill: --start/--end (yfinance)
│ ├─ load_parquet_to_supabase.py # optional: load from Parquet
│ └─ legacy/ # optional: old scripts you keep
│
├─ db/
│ ├─ migrations/
│ │ └─ 2025-08-26_create_movers_daily.sql # movers_daily + vw_movers_latest
│ ├─ routines/
│ │ └─ snapshot_movers_daily.sql # function: snapshot_movers_daily()
│ └─ queries/
│ ├─ movers_latest.sql # SELECT * FROM vw_movers_latest …
│ └─ sanity_counts.sql # daily counts / overlap checks
│
├─ data/
│ ├─ sources/ # your Parquet(s); not committed
│ ├─ exports/ # CSV exports; not committed
│ └─ temp/ # scratch; not committed
└─ logs/ # optional run logs; not committed

pip install pandas pyarrow yfinance supabase python-dotenv

## One-time DB setup

1) **Prices table** (already created earlier; ensure it exists)

Columns:  
`symbol text, name text, date date, adj_close double precision, close double precision, volume bigint, dollar_volume double precision, addv_20d double precision`, PK `(symbol, date)`.  

2) **Daily movers** snapshot + view  
Open Supabase **SQL Editor** and run your two files once:
- `db/migrations/2025-08-26_create_movers_daily.sql`
- `db/routines/snapshot_movers_daily.sql`

This creates:
- `movers_daily` table (with generated columns `abs_change_1d`, `direction`)
- `vw_movers_latest` view
- `snapshot_movers_daily(threshold_pct := 15, in_as_of := null)` function

---

## Load data
Date range (e.g., 2025-08-18 → 2025-08-25)
### Latest trading day (tested job)
From repo root:
python ./Scripts/daily_prices_2025.py

# full universe
python ./Scripts/load_range_to_supabase.py --start 2025-08-18 --end 2025-08-25 --universe all

# or just tracked tickers (if you use tickers.is_tracked = TRUE)
python ./Scripts/load_range_to_supabase.py --start 2025-08-18 --end 2025-08-25 --universe tracked

# dry run (prints universe size, doesn’t write)
python ./Scripts/load_range_to_supabase.py --start 2025-08-18 --end 2025-08-25 --universe all --dry-run

The loader paginates tickers to avoid the 1,000-row cap and maps symbols for Yahoo (provider_symbol_yf fallback dot→dash).


##
Snapshot daily movers (15%)

Run after loading prices for the latest day:

select snapshot_movers_daily();      -- default 15%, latest trading day
-- or a specific day:
-- select snapshot_movers_daily(15, date '2025-08-20');

View results:
select * from vw_movers_latest order by abs_change_1d desc, symbol limit 50;

##
Useful checks

Rows & symbols per day:

select date, count(*) rows, count(distinct symbol) symbols
from prices_2025
group by 1 order by 1;


Latest vs previous trading day:

select max(date) as latest,
       (select max(date) from prices_2025 where date < max(date)) as previous
from prices_2025;


Symbols present on latest day but missing previous (won’t have %Δ):

with ld as (select max(date) d from prices_2025),
pd as (select max(date) d from prices_2025 where date < (select d from ld))
select l.symbol
from prices_2025 l
cross join ld
cross join pd
left join prices_2025 p on p.symbol = l.symbol and p.date = pd.d
where l.date = ld.d and p.symbol is null
limit 50;