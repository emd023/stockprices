import os
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Any

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

TABLE = "prices_2025"
ON_CONFLICT = "symbol,date"
BATCH = 150  # yfinance batch size
UPSERT_CHUNK = 800  # rows per upsert request


def sb_client():
    load_dotenv()  # reads .env in project root
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_KEY (check your .env or env vars).")
    return create_client(url, key)


def parse_args():
    ap = argparse.ArgumentParser(
        description="Load a date window of daily prices into Supabase (upsert into prices_2025)."
    )
    ap.add_argument("--start", required=True, help="Inclusive start date (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="Inclusive end date (YYYY-MM-DD)")
    ap.add_argument("--batch", type=int, default=BATCH, help=f"Tickers per yfinance call (default {BATCH})")
    ap.add_argument("--universe", choices=["tracked", "all"], default="tracked",
                    help="Use tickers.is_tracked=TRUE if available, else all (default: tracked)")
    ap.add_argument("--symbols", default=None,
                    help="Comma-separated symbols to limit (overrides universe filter). Example: AAPL,MSFT,SPY")
    ap.add_argument("--dry-run", action="store_true", help="Parse & fetch preview only; do not write to DB.")
    return ap.parse_args()


def get_universe(cli, universe: str, symbols_csv: str | None) -> pd.DataFrame:
    """
    Return DataFrame with columns: symbol, name, yf_symbol.
    Paginates through `tickers` to avoid the 1,000-row PostgREST cap.
    Respects:
      - `--symbols` (overrides everything)
      - `--universe tracked` (if `is_tracked` exists server-side)
    """
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    # Pre-parse symbol filter (overrides universe)
    symbols_set = None
    if symbols_csv:
        symbols_set = {s.strip().upper() for s in symbols_csv.split(",") if s.strip()}

    while True:
        q = cli.table("tickers").select("*").order("symbol")
        if symbols_set:
            q = q.in_("symbol", list(symbols_set))

        # Try server-side filter for tracked; if column doesn't exist, fall back gracefully
        try:
            if universe == "tracked":
                q = q.eq("is_tracked", True)
            page = q.range(offset, offset + page_size - 1).execute().data
        except Exception:
            # Column might not exist; retry without eq filter
            page = cli.table("tickers").select("*").order("symbol") \
                   .range(offset, offset + page_size - 1).execute().data

        if not page:
            break
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No rows returned from tickers (check filters or table contents).")

    # If we couldn’t filter tracked server-side, do it client-side when available
    if universe == "tracked" and "is_tracked" in df.columns:
        df = df[df["is_tracked"] == True]
        if df.empty:
            raise SystemExit("No tickers with is_tracked = TRUE after pagination.")

    # If --symbols was provided, enforce it client-side as well (safety)
    if symbols_set:
        df = df[df["symbol"].astype(str).str.upper().isin(symbols_set)]
        if df.empty:
            raise SystemExit("None of the requested --symbols were found after pagination.")

    # Build yf_symbol from provider override or dot→dash fallback
    if "provider_symbol_yf" in df.columns and df["provider_symbol_yf"].notna().any():
        df["yf_symbol"] = df["provider_symbol_yf"].fillna(
            df["symbol"].astype(str).str.replace(".", "-", regex=False)
        )
    else:
        df["yf_symbol"] = df["symbol"].astype(str).str.replace(".", "-", regex=False)

    # Ensure name exists
    if "name" not in df.columns:
        df["name"] = df["symbol"]

    out = df[["symbol", "name", "yf_symbol"]].drop_duplicates()
    if out.empty:
        raise SystemExit("Universe resolved to 0 symbols after de-duplication.")
    return out



def fetch_range(yf_syms: List[str], start_d: date, end_d: date) -> pd.DataFrame:
    """Fetch OHLCV for yf_syms over [start_d, end_d] (inclusive)."""
    if not yf_syms:
        return pd.DataFrame(columns=["date", "adj_close", "close", "volume", "yf_symbol"])
    # yfinance end is exclusive → add 1 day
    data = yf.download(
        tickers=yf_syms,
        start=start_d.isoformat(),
        end=(end_d + timedelta(days=1)).isoformat(),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    rows = []
    if isinstance(data.columns, pd.MultiIndex):
        for y in sorted({k for (k, _) in data.columns}):
            try:
                part = data[y].reset_index().rename(columns=str.lower)
                part = part.rename(columns={"adj close": "adj_close"})
                if part.empty:
                    continue
                part["yf_symbol"] = y
                rows.append(part[["date", "adj_close", "close", "volume", "yf_symbol"]])
            except Exception:
                continue
    else:
        # single symbol case
        part = data.reset_index().rename(columns=str.lower).rename(columns={"adj close": "adj_close"})
        if not part.empty:
            part["yf_symbol"] = yf_syms[0]
            rows.append(part[["date", "adj_close", "close", "volume", "yf_symbol"]])

    if not rows:
        return pd.DataFrame(columns=["date", "adj_close", "close", "volume", "yf_symbol"])
    out = pd.concat(rows, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    # Filter again just in case (yfinance sometimes returns a little extra)
    mask = (out["date"] >= start_d) & (out["date"] <= end_d)
    return out.loc[mask].copy()


def to_record(row: pd.Series, name_lookup: Dict[str, str]) -> Dict[str, Any]:
    """JSON-serializable row matching prices_2025 schema."""
    # date
    d = row["date"]
    d_str = d.isoformat() if hasattr(d, "isoformat") else str(d)

    # numeric casts
    def as_float(x): return None if pd.isna(x) else float(x)
    def as_int(x): return None if pd.isna(x) else int(x)

    sym = str(row["symbol"])
    nm = name_lookup.get(sym, sym)

    close = as_float(row.get("close"))
    vol = as_int(row.get("volume"))
    dv = None if (close is None or vol is None) else float(close * vol)

    return {
        "symbol": sym,
        "name": nm,
        "date": d_str,
        "adj_close": as_float(row.get("adj_close")),
        "close": close,
        "volume": vol,
        "dollar_volume": dv,
        "addv_20d": None,  # filled later if/when you want
    }


def upsert(cli, records: List[Dict[str, Any]]):
    total = 0
    for i in range(0, len(records), UPSERT_CHUNK):
        batch = records[i:i + UPSERT_CHUNK]
        cli.table(TABLE).upsert(batch, on_conflict=ON_CONFLICT).execute()
        total += len(batch)
        print(f"  • upserted {len(batch):,} rows (total {total:,})")
    return total


def main():
    args = parse_args()
    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_d = datetime.strptime(args.end, "%Y-%m-%d").date()
    if end_d < start_d:
        raise SystemExit("--end must be on/after --start")

    cli = sb_client()
    uni = get_universe(cli, args.universe, args.symbols)  # symbol, name, yf_symbol
    yf_to_sym = uni.drop_duplicates("yf_symbol").set_index("yf_symbol")["symbol"].to_dict()
    name_lookup = uni.drop_duplicates("symbol").set_index("symbol")["name"].to_dict()
    yf_list = list(yf_to_sym.keys())

    print(f"Date window: {start_d} → {end_d} (inclusive)")
    print(f"Universe: {len(yf_list)} tickers (mode: {args.universe}{' | filtered by --symbols' if args.symbols else ''})")

    total_rows = 0
    all_records: list[Dict[str, Any]] = []

    for i in range(0, len(yf_list), args.batch):
        batch = yf_list[i:i + args.batch]
        df = fetch_range(batch, start_d, end_d)
        if df.empty:
            continue

        # attach canonical symbol & name
        df["symbol"] = df["yf_symbol"].map(yf_to_sym)
        df["name"] = df["symbol"].map(name_lookup).fillna(df["symbol"])
        df = df.dropna(subset=["symbol", "close"])

        # build records (ensure JSON-safe types)
        df = df.sort_values(["symbol", "date"])
        for _, row in df.iterrows():
            all_records.append(to_record(row, name_lookup))

        total_rows += len(df)
        print(f"[{i + len(batch)}/{len(yf_list)}] fetched {len(df):,} rows")

    print(f"Total rows prepared: {total_rows:,}")
    if args.dry_run or total_rows == 0:
        print("Dry run or no data; exiting without DB writes.")
        return

    upserted = upsert(cli, all_records)
    print(f"Done. Upserted {upserted:,} rows into {TABLE}.")


if __name__ == "__main__":
    main()
