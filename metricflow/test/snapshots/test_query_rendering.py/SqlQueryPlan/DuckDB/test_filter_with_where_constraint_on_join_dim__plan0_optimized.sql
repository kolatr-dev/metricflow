-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'booking__is_instant']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  booking__is_instant
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'booking__is_instant', 'listing__country_latest']
  SELECT
    subq_15.booking__is_instant AS booking__is_instant
    , listings_latest_src_10004.country AS listing__country_latest
    , subq_15.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'booking__is_instant', 'listing']
    SELECT
      listing_id AS listing
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
    WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
  ) subq_15
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_15.listing = listings_latest_src_10004.listing_id
) subq_20
WHERE listing__country_latest = 'us'
GROUP BY
  booking__is_instant
