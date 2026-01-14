"""
Console: show top movers for a date range using the current storage backend
(default: parquet via PRICES_PARQUET). Works with your 3-ticker sample parquet
or the full file. Supports CSV export.

Usage examples:
  py -3 -m scripts.movers_by_window 2025-08-19 2025-08-21 --min-pct 1
  py -3 -m scripts.movers_from_window 2025-03-03 2025-03-07 --min-pct 10 --out movers.csv
"""

import argparse
import os
import sys
import pandas as pd

# Pretty console display (does not change saved data)
pd.set_option("display.float_format", "{:,.2f}".format)

# Local imports from your stocks package
try:
    from stocks import storage
    from stocks.analytics import movers_by_range, trading_calendar, snap_range
except Exception as e:
    print("[FATAL] Could not import from 'stocks' package. Make sure you run from project root and that stocks/__init__.py exists.")
    print("Error:", e)
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description="Show top movers for a date range")
    ap.add_argument("start_date", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("end_date",   help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--min-pct", type=float, default=30.0, help="Minimum percent change (default: 30)")
    ap.add_argument("--min-price", type=float, default=None, help="Minimum ending price filter")
    ap.add_argument("--min-addv", type=float, default=None, help="Minimum ADDV-20 filter (dollar volume rolling avg)")
    ap.add_argument("--min-dollar-vol-end", type=float, default=None, help="Minimum dollar volume on end date")
    ap.add_argument("--use-raw", action="store_true", help="Use raw close (not adj_close) for % change")
    ap.add_argument("--out", default=None, help="Optional CSV path to export results")
    args = ap.parse_args()

    cols = ["symbol","name","date","adj_close","close","volume","dollar_volume","addv_20d"]

    # Pull rows from storage (parquet by default via PRICES_PARQUET env)
    df = storage.query_prices(args.start_date, args.end_date, columns=cols)
    if df.empty:
        p = os.getenv("PRICES_PARQUET", "data/prices_2025.parquet")
        print(f"[INFO] No data in range. Check your file and dates.\n  File in use: {p}")
        sys.exit(0)

    # Ensure types
    df["date"] = pd.to_datetime(df["date"])

    # Snap the requested window to available trading days
    cal = trading_calendar(df)
    try:
        s, e = snap_range(cal, pd.to_datetime(args.start_date), pd.to_datetime(args.end_date))
    except Exception as ex:
        print(f"[FATAL] Could not snap date range: {ex}")
        sys.exit(1)

    # Compute movers
    res = movers_by_range(
        df, s, e,
        min_pct=args.min_pct,
        use_raw=args.use_raw,
        min_price=args.min_price,
        min_addv=args.min_addv,
        min_dollar_vol_end=args.min_dollar_vol_end
    )

    print(f"Window: {s.date()} → {e.date()} | rows: {len(res):,}")

    if res.empty:
        print("[INFO] No movers matched your filters. Consider lowering thresholds (e.g., --min-pct 1).")
        sys.exit(0)

    # Be tolerant if name isn't present (older files): fill with symbol
    if "name" not in res.columns:
        res["name"] = res["symbol"]

    # Show top 50 to console
    print(res.head(50))

    # Optional CSV export
    if args.out:
        res.to_csv(args.out, index=False)
        print(f"[OK] Saved → {args.out}")


if __name__ == "__main__":
    main()
