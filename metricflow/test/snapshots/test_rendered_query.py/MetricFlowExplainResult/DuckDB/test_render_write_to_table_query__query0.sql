CREATE TABLE ***************************.test_table AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_1
    WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
  ) subq_3
  GROUP BY
    metric_time__day
)
