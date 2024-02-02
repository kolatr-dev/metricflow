-- Join Standard Outputs
-- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time__day AS metric_time__day
  , subq_18.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
  , SUM(subq_13.bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_10044
) subq_13
LEFT OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
  SELECT
    lux_listings_src_10048.valid_from AS lux_listing__window_start__day
    , lux_listings_src_10048.valid_to AS lux_listing__window_end__day
    , lux_listing_mapping_src_10047.listing_id AS listing
    , lux_listings_src_10048.is_confirmed_lux AS lux_listing__is_confirmed_lux
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_10047
  LEFT OUTER JOIN
    ***************************.dim_lux_listings lux_listings_src_10048
  ON
    lux_listing_mapping_src_10047.lux_listing_id = lux_listings_src_10048.lux_listing_id
) subq_18
ON
  (
    subq_13.listing = subq_18.listing
  ) AND (
    (
      subq_13.metric_time__day >= subq_18.lux_listing__window_start__day
    ) AND (
      (
        subq_13.metric_time__day < subq_18.lux_listing__window_end__day
      ) OR (
        subq_18.lux_listing__window_end__day IS NULL
      )
    )
  )
GROUP BY
  subq_13.metric_time__day
  , subq_18.lux_listing__is_confirmed_lux
