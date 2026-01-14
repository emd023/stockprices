create table if not exists tickers (
  symbol text primary key,            -- your canonical (e.g., BRK.B)
  name text,
  exchange text,
  asset_type text,                    -- equity/etf/crypto/adr/etc.
  provider_symbol_yf text,            -- e.g., BRK-B (Yahoo)
  provider_symbol_polygon text,       -- optional for later
  is_active boolean default true,
  is_tracked boolean default true,
  first_seen timestamptz default now(),
  last_seen  timestamptz default now(),
  source text
);
create index if not exists idx_tickers_tracked on tickers(is_tracked);
