-- Replace the deliberate incremental-demo bad order with a clean five-order
-- landing snapshot before the final steady-state all-source run.
-- Adjust the Volume path if you changed the default catalog/schema/volume.

INSERT OVERWRITE DIRECTORY
  '/Volumes/workspace/bronze/md_ingestion_raw/orders'
USING JSON
SELECT *
FROM VALUES
  (5001, 101, 1001, 2,  79.0, '2026-08-11T08:00:00', 'SHIPPED'),
  (5002, 102, 1002, 1, 299.0, '2026-08-11T09:30:00', 'SHIPPED'),
  (5003, 101, 1003, 1, 149.0, '2026-08-11T10:15:00', 'CREATED'),
  (5004, 103, 1001, 1,  79.0, '2026-08-12T11:00:00', 'CREATED'),
  (5005, 104, 1004, 1,  99.0, '2026-08-12T15:00:00', 'CREATED')
AS t(
  order_id,
  customer_id,
  product_id,
  quantity,
  price,
  order_timestamp,
  status
);

SELECT *
FROM json.`/Volumes/workspace/bronze/md_ingestion_raw/orders`
ORDER BY order_id;
