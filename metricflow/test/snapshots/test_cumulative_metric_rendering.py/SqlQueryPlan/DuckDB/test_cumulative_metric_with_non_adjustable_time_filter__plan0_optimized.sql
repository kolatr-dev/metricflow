-- Constrain Output with WHERE
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Self Over Time Range
  -- Pass Only Elements:
  --   ['bookers', 'metric_time__day']
  -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
  SELECT
    subq_13.metric_time__day AS metric_time__day
    , subq_12.bookers AS bookers
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_14
    WHERE ds BETWEEN '2000-01-01' AND '2040-12-31'
  ) subq_13
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [1999-12-30T00:00:00, 2040-12-31T00:00:00]
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_10001
    WHERE DATE_TRUNC('day', ds) BETWEEN '1999-12-30' AND '2040-12-31'
  ) subq_12
  ON
    (
      subq_12.metric_time__day <= subq_13.metric_time__day
    ) AND (
      subq_12.metric_time__day > subq_13.metric_time__day - INTERVAL 2 day
    )
  WHERE subq_13.metric_time__day BETWEEN '2000-01-01' AND '2040-12-31'
) subq_17
WHERE metric_time__day = '2020-01-03' or metric_time__day = '2020-01-07'
GROUP BY
  metric_time__day
