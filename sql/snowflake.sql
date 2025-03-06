SELECT COUNT(*) AS total_records FROM dev.raw.stock_daily_prices;

SELECT * FROM dev.raw.stock_daily_prices
ORDER BY date DESC
LIMIT 10;

SELECT 
  MIN(date) AS earliest_date,
  MAX(date) AS latest_date,
  COUNT(DISTINCT date) AS total_days
FROM dev.raw.stock_daily_prices;

SELECT 
  symbol,
  COUNT(*) AS record_count,
  AVG(close) AS avg_close_price,
  MAX(high) AS highest_price,
  MIN(low) AS lowest_price
FROM dev.raw.stock_daily_prices
GROUP BY symbol;