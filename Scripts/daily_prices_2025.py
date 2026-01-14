# Scripts/daily_prices_2025.py
# Adds ONLY the most recent trading day to prices_2025
# Matches schema: symbol, name, date, adj_close, close, volume, dollar_volume, addv_20d
# USE_TRACKED = True - for testing purposes, only updates only 5 tickers. False updates all.

import os
from datetime import date, timedelta
from typing import List, Dict, Any

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import yfinance as yf

TABLE = "prices_2025"
BATCH = 150
USE_TRACKED = False  # If tickers.is_tracked exists, filter to TRUE rows; else process all

# ---------- Supabase client ----------
def sb():
    load_dotenv()  # reads .env from your project root
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)

# ---------- Universe (tickers) ----------
def get_universe(cli) -> pd.DataFrame:
    """
    Fetch symbols + names, paginated to avoid the 1,000-row cap.
    Honors global USE_TRACKED when `is_tracked` exists.
    Returns columns: symbol, name, yf_symbol.
    """
    page_size = 1000
    offset = 0
    rows: list[dict] = []

    while True:
        q = cli.table("tickers").select("*").order("symbol")
        # Try server-side tracked filter; fall back if column missing
        try:
            if USE_TRACKED:
                q = q.eq("is_tracked", True)
            page = q.range(offset, offset + page_size - 1).execute().data
        except Exception:
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
        raise SystemExit("No tickers found in DB (table 'tickers' is empty or filtered out).")

    # If we couldn’t filter tracked server-side, do it client-side when available
    if USE_TRACKED and "is_tracked" in df.columns:
        df = df[df["is_tracked"] == True]
        if df.empty:
            raise SystemExit("No tickers with is_tracked = TRUE after pagination.")

    # Yahoo mapping
    if "provider_symbol_yf" in df.columns and df["provider_symbol_yf"].notna().any():
        df["yf_symbol"] = df["provider_symbol_yf"].fillna(
            df["symbol"].astype(str).str.replace(".", "-", regex=False)
        )
    else:
        df["yf_symbol"] = df["symbol"].astype(str).str.replace(".", "-", regex=False)

    if "name" not in df.columns:
        df["name"] = df["symbol"]

    out = df[["symbol", "name", "yf_symbol"]].drop_duplicates()
    if out.empty:
        raise SystemExit("Universe resolved to 0 symbols after de-duplication.")
    return out


# ---------- Resolve last market day ----------
def last_market_day() -> date:
    hist = yf.download("SPY", period="7d", interval="1d", progress=False, auto_adjust=False)
    if hist.empty:
        return date.today() - timedelta(days=1)
    return pd.to_datetime(hist.index[-1]).date()

# ---------- Fetch one day for a batch ----------
def fetch_day(yf_syms: List[str], d: date) -> pd.DataFrame:
    if not yf_syms:
        return pd.DataFrame()

    data = yf.download(
        tickers=yf_syms,
        start=d.isoformat(),
        end=(d + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    rows = []
    if isinstance(data.columns, pd.MultiIndex):
        # Multiple tickers: (ticker, field) columns
        for y in sorted({k for (k, _) in data.columns}):
            part = data[y].reset_index().rename(columns=str.lower).rename(columns={"adj close": "adj_close"})
            if part.empty:
                continue
            part["yf_symbol"] = y
            rows.append(part[["date", "adj_close", "close", "volume", "yf_symbol"]])
    else:
        # Single ticker case: plain columns
        part = data.reset_index().rename(columns=str.lower).rename(columns={"adj close": "adj_close"})
        if not part.empty:
            part["yf_symbol"] = yf_syms[0]
            rows.append(part[["date", "adj_close", "close", "volume", "yf_symbol"]])

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["date", "adj_close", "close", "volume", "yf_symbol"]
    )

# ---------- Upsert helper (JSON-serializable records) ----------
def to_record(row: pd.Series) -> Dict[str, Any]:
    """
    Build a JSON-safe dict matching the prices_2025 schema.
    - Convert date to ISO string
    - Cast numbers to Python built-ins
    - Leave addv_20d as None (can be filled in a later SQL step)
    """
    # Date to ISO string
    d = row["date"]
    d_str = d.isoformat() if hasattr(d, "isoformat") else str(d)

    # Safe numeric casts
    def as_float(x):
        return None if pd.isna(x) else float(x)

    def as_int(x):
        return None if pd.isna(x) else int(x)

    name_val = None if pd.isna(row.get("name")) else str(row.get("name"))

    return {
        "symbol": str(row["symbol"]),
        "name": name_val if name_val else str(row["symbol"]),
        "date": d_str,  # PostgREST will store as DATE
        "adj_close": as_float(row.get("adj_close")),
        "close": as_float(row.get("close")),
        "volume": as_int(row.get("volume")),
        "dollar_volume": as_float(row.get("dollar_volume")),
        "addv_20d": None,  # optional post-step to fill via SQL window function
    }

def upsert(cli, records: List[Dict[str, Any]], on_conflict="symbol,date", chunk=800):
    for i in range(0, len(records), chunk):
        cli.table(TABLE).upsert(records[i:i+chunk], on_conflict=on_conflict).execute()

# ---------- Main ----------
def main():
    cli = sb()
    uni = get_universe(cli)  # symbol, name, yf_symbol
    target_day = last_market_day()
    print(f"Target trading day: {target_day}")

    # Maps for joining
    map_yf_to_sym = uni.drop_duplicates("yf_symbol").set_index("yf_symbol")["symbol"].to_dict()
    map_sym_to_name = uni.drop_duplicates("symbol").set_index("symbol")["name"].to_dict()
    yf_list = list(map_yf_to_sym.keys())

    total_rows = 0

    for i in range(0, len(yf_list), BATCH):
        batch = yf_list[i:i + BATCH]
        df = fetch_day(batch, target_day)
        if df.empty:
            continue

        # Attach canonical symbol and stable name
        df["symbol"] = df["yf_symbol"].map(map_yf_to_sym)
        df["name"] = df["symbol"].map(map_sym_to_name).fillna(df["symbol"])
        df = df.dropna(subset=["symbol", "close"])

        # Normalize types in DataFrame (still convert to built-ins per-record below)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        for c in ["adj_close", "close"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        # Derived column
        df["dollar_volume"] = df["close"] * df["volume"]

        # Build JSON-safe records with EXACT columns
        wanted_cols = ["symbol", "name", "date", "adj_close", "close", "volume", "dollar_volume"]
        for c in wanted_cols:
            if c not in df.columns:
                df[c] = None

        recs = [to_record(row) for _, row in df[wanted_cols].drop_duplicates().iterrows()]
        if recs:
            upsert(cli, recs, on_conflict="symbol,date")
            total_rows += len(recs)
            print(f"[{i + len(batch)}/{len(yf_list)}] upserted {len(recs)} rows")

    print(f"Done. Inserted/updated {total_rows} rows for {target_day}.")

if __name__ == "__main__":
    main()
