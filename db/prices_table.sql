-- Daily OHLCV, keyed by (symbol, date)
create table if not exists prices_daily (
  symbol text not null references tickers(symbol) on delete cascade,
  date date not null,
  open double precision,
  high double precision,
  low  double precision,
  close double precision,
  volume bigint,
  source text,
  primary key (symbol, date)
);

-- Optional (only if you actually query by date across the whole universe)
-- create index if not exists prices_daily_date_idx on prices_daily(date);
