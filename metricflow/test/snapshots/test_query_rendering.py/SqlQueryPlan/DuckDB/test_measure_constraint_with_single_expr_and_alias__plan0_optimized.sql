-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , delayed_bookings * 2 AS double_counted_delayed_bookings
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS delayed_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'booking__is_instant', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
    WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
  ) subq_11
  WHERE NOT booking__is_instant
  GROUP BY
    metric_time__day
) subq_15
