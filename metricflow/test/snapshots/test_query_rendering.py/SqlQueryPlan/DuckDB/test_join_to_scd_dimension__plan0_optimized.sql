-- Constrain Output with WHERE
-- Pass Only Elements:
--   ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__capacity', 'metric_time__day']
  SELECT
    subq_14.metric_time__day AS metric_time__day
    , listings_src_10017.capacity AS listing__capacity
    , subq_14.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10015
    WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
  ) subq_14
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_10017
  ON
    (
      subq_14.listing = listings_src_10017.listing_id
    ) AND (
      (
        subq_14.metric_time__day >= listings_src_10017.active_from
      ) AND (
        (
          subq_14.metric_time__day < listings_src_10017.active_to
        ) OR (
          listings_src_10017.active_to IS NULL
        )
      )
    )
) subq_18
WHERE (listing__capacity > 2) AND (listing__capacity > 2)
GROUP BY
  metric_time__day
