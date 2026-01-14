# scripts/build_prices_ytd.py
# --------------------------------------------
# Builds StocksApp/data/prices_2025.parquet with YTD daily prices:
# columns: symbol, name, date, adj_close, close, volume, dollar_volume, addv_20d
# Uses checkpoints per batch in StocksApp/data/tmp/ so you don't lose progress.
# --------------------------------------------

import os
import time
import math
from pathlib import Path
import pandas as pd
import yfinance as yf

# Pretty print (console only; parquet stores numeric floats)
pd.set_option("display.float_format", "{:,.2f}".format)

# --------- PATHS (root-level data dir) ---------
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
OUT_DIR    = ROOT_DIR / "data"
TMP_DIR    = OUT_DIR / "tmp"
OUT_FILE   = OUT_DIR / "prices_2025 - August.parquet"
FAILED_FILE= OUT_DIR / "failed_symbols.csv"
TICKERS_CSV= SCRIPT_DIR / "tickersFull.csv"   # expects columns: symbol[,name]

OUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

# --------- CONFIG ---------
START_DATE = "2025-08-19"            # first trading day of 2025
END_DATE   = None                    # None = up to latest available
BATCH_SIZE = 300                     # drop to ~150 if throttled
PAUSE_SEC  = 1.0                     # pause after each successful batch
MAX_RETRIES= 3
BACKOFF_SEC= 20                      # sleep on timeout/rate-limit before retry

print(f"[INFO] Reading tickers from {TICKERS_CSV} ...")
tickers_df = pd.read_csv(TICKERS_CSV)
tickers_df.columns = [c.strip().lower() for c in tickers_df.columns]
if "symbol" not in tickers_df.columns:
    raise SystemExit("tickers.csv must have a 'symbol' column")
if "name" not in tickers_df.columns:
    tickers_df["name"] = pd.NA

tickers_df = tickers_df.dropna(subset=["symbol"])
tickers_df["symbol"] = tickers_df["symbol"].astype(str).str.strip()
tickers_df["name"]   = tickers_df["name"].astype(str).str.strip()
tickers_df = tickers_df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

symbols  = tickers_df["symbol"].tolist()
name_map = dict(zip(tickers_df["symbol"], tickers_df["name"]))

print(f"[INFO] Total symbols: {len(symbols):,}")
n_batches = math.ceil(len(symbols) / BATCH_SIZE)

failed_symbols: list[str] = []
written_batches = 0
_name_cache: dict[str, str] = {}

def resolve_name(sym: str) -> str:
    """Prefer name from tickers.csv; otherwise try Yahoo longName/shortName; fallback to symbol."""
    n = name_map.get(sym)
    if n and n.upper() != sym.upper() and n.lower() != "nan":
        return n
    if sym in _name_cache:
        return _name_cache[sym]
    try:
        t = yf.Ticker(sym)
        long_name = None
        try:
            info = t.get_info()
            long_name = info.get("longName") or info.get("shortName")
        except Exception:
            info = getattr(t, "info", {}) or {}
            long_name = info.get("longName") or info.get("shortName")
        if long_name:
            _name_cache[sym] = long_name
            return long_name
    except Exception:
        pass
    _name_cache[sym] = sym
    return sym

def parse_multi_ticker(df_multi: pd.DataFrame, batch_syms: list[str]) -> pd.DataFrame:
    """
    yfinance multi-ticker: columns are a MultiIndex: (field, ticker) or (ticker, field).
    Normalize to a tidy DataFrame.
    """
    if df_multi.empty:
        return pd.DataFrame(columns=["symbol","name","date","adj_close","close","volume"])

    if isinstance(df_multi.columns, pd.MultiIndex):
        # detect layout
        fields_first = set(["Adj Close","Close","Volume"]).issubset(set(df_multi.columns.get_level_values(0)))
        frames = []
        for sym in batch_syms:
            try:
                if fields_first:
                    sub = df_multi.loc[:, (["Adj Close","Close","Volume"], sym)]
                    sub.columns = ["adj_close","close","volume"]
                else:
                    sub = df_multi.loc[:, (sym, ["Adj Close","Close","Volume"])]
                    sub.columns = ["adj_close","close","volume"]
                sub = sub.reset_index().rename(columns={"Date":"date","index":"date"})
                sub["symbol"] = sym
                sub["name"]   = resolve_name(sym)
                frames.append(sub[["symbol","name","date","adj_close","close","volume"]])
            except Exception:
                continue
        out = pd.concat(frames, ignore_index=True) if frames else \
              pd.DataFrame(columns=["symbol","name","date","adj_close","close","volume"])
    else:
        # Single ticker fallback
        out = df_multi.reset_index().rename(columns=str.lower)
        out = out.rename(columns={"adj close":"adj_close"})
        out["symbol"] = batch_syms[0]
        out["name"]   = resolve_name(batch_syms[0])
        out = out[["symbol","name","date","adj_close","close","volume"]]
    return out

def save_batch(df: pd.DataFrame, idx: int):
    path = TMP_DIR / f"batch_{idx:03d}.parquet"
    if not df.empty:
        df.to_parquet(path, index=False)
    print(f"[INFO] Saved batch {idx+1} → {path} (rows: {len(df):,})")

# --------- DOWNLOAD LOOP ---------
for bi in range(n_batches):
    batch = symbols[bi*BATCH_SIZE : (bi+1)*BATCH_SIZE]
    if not batch:
        continue

    cp_path = TMP_DIR / f"batch_{bi:03d}.parquet"
    if cp_path.exists():
        print(f"[SKIP] Batch {bi+1}/{n_batches} already exists.")
        written_batches += 1
        continue

    print(f"[INFO] Batch {bi+1}/{n_batches}: tickers={len(batch)}")
    tickers_str = " ".join([s for s in batch if isinstance(s, str) and s.strip()])

    tries = 0
    last_err = None
    while tries < MAX_RETRIES:
        tries += 1
        try:
            data = yf.download(
                tickers_str,
                start=START_DATE,
                end=END_DATE,
                auto_adjust=False,
                group_by="ticker",
                progress=False,
                threads=True
            )
            df_batch = parse_multi_ticker(data, batch)
            df_batch = df_batch.dropna(subset=["adj_close","close","volume"])
            if not df_batch.empty:
                df_batch["date"]   = pd.to_datetime(df_batch["date"])
                df_batch["volume"] = pd.to_numeric(df_batch["volume"], errors="coerce")
                df_batch = df_batch.dropna(subset=["volume"])
            save_batch(df_batch, bi)
            written_batches += 1
            time.sleep(PAUSE_SEC)
            last_err = None
            break
        except Exception as e:
            last_err = e
            print(f"[WARN] Batch {bi+1} attempt {tries}/{MAX_RETRIES} failed: {e}")
            time.sleep(BACKOFF_SEC)

    if last_err is not None:
        salvaged = []
        for sym in batch:
            try:
                d = yf.download(sym, start=START_DATE, end=END_DATE, auto_adjust=False, progress=False)
                if d is None or d.empty:
                    failed_symbols.append(sym)
                    continue
                d = d.rename(columns=str.lower).reset_index().rename(columns={"adj close":"adj_close"})
                d["symbol"] = sym
                d["name"]   = resolve_name(sym)
                d = d[["symbol","name","date","adj_close","close","volume"]]
                salvaged.append(d)
                time.sleep(0.1)
            except Exception:
                failed_symbols.append(sym)
                continue
        if salvaged:
            df_salv = pd.concat(salvaged, ignore_index=True)
            df_salv["date"] = pd.to_datetime(df_salv["date"])
            df_salv = df_salv.dropna(subset=["adj_close","close","volume"])
            save_batch(df_salv, bi)
            written_batches += 1
        else:
            print(f"[ERROR] Could not retrieve any symbols in batch {bi+1}. Marking all as failed.")
            failed_symbols.extend(batch)

print(f"[INFO] Download complete. Written batches: {written_batches}/{n_batches}")

# --------- STITCH ---------
parts = [pd.read_parquet(TMP_DIR / f) for f in sorted(os.listdir(TMP_DIR)) if f.startswith("batch_") and f.endswith(".parquet")]
if not parts:
    raise SystemExit("[FATAL] No batch parquet files found. Nothing to stitch.")

all_df = pd.concat(parts, ignore_index=True)
print(f"[INFO] Combined rows before clean: {len(all_df):,}")

# Final tidy-up
all_df = all_df[["symbol", "name", "date", "adj_close", "close", "volume"]]
all_df["date"] = pd.to_datetime(all_df["date"])
all_df = all_df.dropna(subset=["adj_close"])
all_df = all_df.drop_duplicates(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)

# --- Liquidity metrics ---
all_df["dollar_volume"] = all_df["close"] * all_df["volume"]
all_df["addv_20d"] = (
    all_df
      .groupby("symbol", group_keys=False)["dollar_volume"]
      .apply(lambda s: s.rolling(window=20, min_periods=1).mean())
)

print("[INFO] Sample rows with names & ADDV:")
print(all_df.head(10))

# Atomic save to ROOT data/
tmp_out = OUT_FILE.with_suffix(".parquet.tmp")
all_df.to_parquet(tmp_out, index=False)
os.replace(tmp_out, OUT_FILE)
print(f"[INFO] Wrote final parquet → {OUT_FILE} (rows: {len(all_df):,}, symbols: {all_df['symbol'].nunique():,})")

# --------- FAILURES REPORT ---------
if failed_symbols:
    # Keep only failures that truly have zero rows in final
    present = set(all_df["symbol"].unique())
    failed_unique = sorted(set(sym for sym in failed_symbols if sym not in present))
    if failed_unique:
        pd.Series(failed_unique, name="symbol").to_csv(FAILED_FILE, index=False)
        print(f"[INFO] Wrote failed symbols ({len(failed_unique)}) → {FAILED_FILE}")
    else:
        print("[INFO] No unresolved failed symbols after stitching.")
else:
    print("[INFO] No failed symbols recorded.")

print("[INFO] All done.")
