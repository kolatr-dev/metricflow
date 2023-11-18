-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(count_dogs) AS count_dogs
FROM (
  -- Read Elements From Semantic Model 'animals'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
  -- Pass Only Elements:
  --   ['count_dogs', 'metric_time__day']
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS count_dogs
  FROM ***************************.fct_animals animals_src_0
  WHERE DATE_TRUNC('day', ds) BETWEEN '2000-01-01' AND '2040-12-31'
) subq_3
GROUP BY
  metric_time__day
