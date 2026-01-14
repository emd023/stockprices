import pandas as pd
from pathlib import Path

IN_PATH  = Path(r"C:\Users\emd02\Files\Python\StockPrices\prices_2025.parquet")   # <-- change if needed
OUT_PATH = IN_PATH.with_name("prices_2025_from_2025-08-18.parquet")

# Load (uses pyarrow engine)
df = pd.read_parquet(IN_PATH)

# Ensure 'date' is datetime (handles string/date/tz-aware)
df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)
# Keep rows on/after 2025-08-18
cutoff = pd.Timestamp("2025-08-18")
df_out = df[df["date"] >= cutoff].copy()

# Optional: sort for sanity
if {"symbol", "date"}.issubset(df_out.columns):
    df_out = df_out.sort_values(["symbol", "date"])

# Write new Parquet
df_out.to_parquet(OUT_PATH, index=False)

print(f"Input rows: {len(df):,}")
print(f"Output rows (>= {cutoff.date()}): {len(df_out):,}")
print(f"Wrote: {OUT_PATH}")
