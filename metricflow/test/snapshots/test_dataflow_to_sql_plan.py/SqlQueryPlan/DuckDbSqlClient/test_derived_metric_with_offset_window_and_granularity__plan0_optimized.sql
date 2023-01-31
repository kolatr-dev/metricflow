-- Compute Metrics via Expressions
SELECT
  metric_time__quarter
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_18.metric_time__quarter, subq_26.metric_time__quarter) AS metric_time__quarter
    , subq_18.bookings AS bookings
    , subq_26.bookings_2_weeks_ago AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__quarter
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__quarter']
      SELECT
        DATE_TRUNC('quarter', ds) AS metric_time__quarter
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_16
    GROUP BY
      metric_time__quarter
  ) subq_18
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__quarter']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_21.metric_time__quarter AS metric_time__quarter
      , SUM(subq_20.bookings) AS bookings_2_weeks_ago
    FROM (
      -- Date Spine
      SELECT
        DATE_TRUNC('quarter', ds) AS metric_time__quarter
      FROM ***************************.mf_time_spine subq_22
      GROUP BY
        DATE_TRUNC('quarter', ds)
    ) subq_21
    INNER JOIN (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('quarter', ds) AS metric_time__quarter
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_20
    ON
      subq_21.metric_time__quarter - INTERVAL 14 day = subq_20.metric_time__quarter
    GROUP BY
      subq_21.metric_time__quarter
  ) subq_26
  ON
    (
      subq_18.metric_time__quarter = subq_26.metric_time__quarter
    ) OR (
      (
        subq_18.metric_time__quarter IS NULL
      ) AND (
        subq_26.metric_time__quarter IS NULL
      )
    )
) subq_27
