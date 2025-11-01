USE olist;

LOAD DATA LOCAL INFILE '/tmp/dataset/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_reviews_dataset.csv'
IGNORE INTO TABLE order_reviews
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_geolocation_dataset.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;