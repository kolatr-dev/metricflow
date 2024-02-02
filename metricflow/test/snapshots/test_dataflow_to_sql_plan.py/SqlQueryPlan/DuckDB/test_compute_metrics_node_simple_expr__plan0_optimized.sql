-- Compute Metrics via Expressions
SELECT
  listing
  , listing__country_latest
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    bookings_source_src_10059.listing_id AS listing
    , listings_latest_src_10063.country AS listing__country_latest
    , SUM(bookings_source_src_10059.booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_10059
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10063
  ON
    bookings_source_src_10059.listing_id = listings_latest_src_10063.listing_id
  GROUP BY
    bookings_source_src_10059.listing_id
    , listings_latest_src_10063.country
) subq_11
