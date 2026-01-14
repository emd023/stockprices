-- Creates/updates a function that snapshots daily movers with a threshold
-- Usage:
--   select snapshot_movers_daily();                   -- uses latest trading day, 15% threshold
--   select snapshot_movers_daily(15);                 -- same
--   select snapshot_movers_daily(15, date '2025-08-20'); -- for a specific day

create or replace function snapshot_movers_daily(
  threshold_pct double precision default 15,
  in_as_of date default null
)
returns integer
language plpgsql
as $$
declare
  v_as_of date;
  v_prev  date;
  v_inserted integer := 0;
begin
  -- Resolve the snapshot date (latest if not provided)
  if in_as_of is null then
    select max(date) into v_as_of from prices_2025;
  else
    v_as_of := in_as_of;
  end if;

  if v_as_of is null then
    return 0; -- nothing to do
  end if;

  -- Find previous trading day present in prices_2025
  select max(date) into v_prev
  from prices_2025
  where date < v_as_of;

  if v_prev is null then
    return 0; -- need two days to compute 1D change
  end if;

  -- Remove any existing snapshot for that day (idempotent)
  delete from movers_daily where as_of = v_as_of;

  -- Insert movers for v_as_of with |%Δ| >= threshold_pct, ranked by absolute % change
  with movers as (
    select
      n.symbol,
      n.name,
      v_as_of as as_of,
      ((n.close / p.close) - 1.0) * 100.0 as pct_change_1d,
      p.close         as close_prev,
      n.close         as close_now,
      n.volume        as volume_now,
      n.dollar_volume as dollar_volume_now
    from prices_2025 n
    join prices_2025 p
      on p.symbol = n.symbol
     and p.date   = v_prev
    where n.date = v_as_of
      and p.close is not null and p.close <> 0
      and n.close is not null
  )
  insert into movers_daily
    (as_of, symbol, name, pct_change_1d, close_prev, close_now, volume_now, dollar_volume_now, rank_overall)
  select
    m.as_of,
    m.symbol,
    m.name,
    m.pct_change_1d,
    m.close_prev,
    m.close_now,
    m.volume_now,
    m.dollar_volume_now,
    dense_rank() over (order by abs(m.pct_change_1d) desc) as rank_overall
  from movers m
  where abs(m.pct_change_1d) >= threshold_pct;

  get diagnostics v_inserted = row_count;  -- rows inserted by the last INSERT
  return v_inserted;
end;
$$;
