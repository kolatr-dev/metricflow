-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_45.metric_time__day, subq_58.metric_time__day, subq_64.metric_time__day) AS metric_time__day
    , MAX(subq_45.average_booking_value) AS average_booking_value
    , MAX(subq_58.bookings) AS bookings
    , MAX(subq_64.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['average_booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['average_booking_value', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_36.metric_time__day AS metric_time__day
        , listings_latest_src_10004.is_lux AS listing__is_lux_latest
        , subq_36.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
        -- Pass Only Elements:
        --   ['average_booking_value', 'metric_time__day', 'listing']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_10001
        WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
      ) subq_36
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10004
      ON
        subq_36.listing = listings_latest_src_10004.listing_id
    ) subq_41
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_45
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['bookings', 'listing__is_lux_latest', 'metric_time__day']
      SELECT
        subq_49.metric_time__day AS metric_time__day
        , listings_latest_src_10004.is_lux AS listing__is_lux_latest
        , subq_49.bookings AS bookings
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
        FROM ***************************.fct_bookings bookings_source_src_10001
        WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
      ) subq_49
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10004
      ON
        subq_49.listing = listings_latest_src_10004.listing_id
    ) subq_54
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_58
  ON
    subq_45.metric_time__day = subq_58.metric_time__day
  FULL OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_10001
    WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
    GROUP BY
      DATE_TRUNC('day', ds)
  ) subq_64
  ON
    COALESCE(subq_45.metric_time__day, subq_58.metric_time__day) = subq_64.metric_time__day
  GROUP BY
    COALESCE(subq_45.metric_time__day, subq_58.metric_time__day, subq_64.metric_time__day)
) subq_65
