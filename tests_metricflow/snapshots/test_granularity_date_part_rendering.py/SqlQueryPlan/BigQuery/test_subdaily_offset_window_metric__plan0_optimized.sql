-- Compute Metrics via Expressions
SELECT
  metric_time__hour
  , archived_users AS subdaily_offset_window_metric
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['archived_users', 'metric_time__hour']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.ts AS metric_time__hour
    , SUM(subq_9.archived_users) AS archived_users
  FROM ***************************.mf_time_spine_hour subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'users_ds_source'
    -- Metric Time Dimension 'archived_at'
    SELECT
      DATETIME_TRUNC(archived_at, hour) AS metric_time__hour
      , 1 AS archived_users
    FROM ***************************.dim_users users_ds_source_src_28000
  ) subq_9
  ON
    DATE_SUB(CAST(subq_11.ts AS DATETIME), INTERVAL 1 hour) = subq_9.metric_time__hour
  GROUP BY
    metric_time__hour
) subq_15
