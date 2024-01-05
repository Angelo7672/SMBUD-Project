CREATE INDEX customer_order_id_index FOR (o:Order) ON o.customer_id;

//Order
LOAD CSV WITH HEADERS FROM "file:///olist_orders_dataset.csv" AS orders
WITH orders WHERE orders.order_id IS NOT NULL
CREATE (o:Order{order_id: orders.order_id, 
	customer_id: orders.customer_id,
	purchase_timestamp: datetime({ epochMillis: apoc.date.parse(orders.order_purchase_timestamp, 'ms', 'yyyy-MM-dd HH:mm:ss') }),
	delivery_date: datetime({ epochMillis: apoc.date.parse(orders.order_estimated_delivery_date, 'ms', 'yyyy-MM-dd HH:mm:ss') })});

//Seller
LOAD CSV WITH HEADERS FROM "file:///olist_sellers_dataset.csv" AS sellers
WITH sellers WHERE sellers.seller_id IS NOT NULL
CREATE (s:Seller{seller_id: sellers.seller_id});

//Customer
LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers
WITH customers WHERE customers.customer_id IS NOT NULL AND customers.customer_unique_id IS NOT NULL
MERGE (c:Customer{customer_id: customers.customer_unique_id});

//City and State
LOAD CSV WITH HEADERS FROM "file:///olist_geolocation_dataset.csv" AS locations
WITH locations WHERE locations.geolocation_city IS NOT NULL AND locations.geolocation_state IS NOT NULL
MERGE (c:City{name_state: locations.geolocation_city + "-" + locations.geolocation_state})
ON CREATE SET c.name = locations.geolocation_city
MERGE (s:State {code: locations.geolocation_state})
MERGE (c)-[:PART_OF]->(s);

//Item
LOAD CSV WITH HEADERS FROM "file:///olist_order_items_dataset.csv" AS items
WITH items WHERE items.order_id IS NOT NULL AND items.order_item_id IS NOT NULL AND items.seller_id IS NOT NULL
CREATE (i:Item{item_id: items.order_id + "-" + items.order_item_id, price: toFloat(items.price)});

//Product
LOAD CSV WITH HEADERS FROM "file:///olist_products_dataset.csv" AS products
WITH products WHERE products.product_id IS NOT NULL
CREATE (p:Product{product_id: products.product_id, 
	photos_qty: toInteger(products.product_photos_qty), 
	weight_g: toInteger(products.product_weight_g), 
	length_cm: toInteger(products.product_length_cm), 
	height_cm: toInteger(products.product_height_cm), 
	width_cm: toInteger(products.product_width_cm)})
MERGE (c:Category{name: coalesce(products.product_category_name,"NA")});

//Review
LOAD CSV WITH HEADERS FROM "file:///olist_order_reviews_dataset.csv" AS reviews
WITH reviews WHERE reviews.review_id IS NOT NULL
MERGE (r:Review{review_id: reviews.review_id})
ON CREATE SET
  r.score = toInteger(reviews.review_score),
  r.comment_title = CASE WHEN trim(reviews.comment_title) = "" THEN null ELSE reviews.comment_title END,
  r.comment = CASE WHEN trim(reviews.review_comment_message) = "" THEN null ELSE reviews.review_comment_message END,
  r.creation_date = datetime({ epochMillis: apoc.date.parse(reviews.review_creation_date, 'ms', 'M/d/yyyy H:mm') }),
  r.answer_timestamp = datetime({ epochMillis: apoc.date.parse(reviews.review_answer_timestamp, 'ms', 'M/d/yyyy H:mm') });

//------Relations------

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (c:Customer{customer_id: customers_rel.customer_unique_id})
MATCH (ci:City{name_state: customers_rel.customer_city + "-" + customers_rel.customer_state})
MERGE (c)-[:LIVES_IN]->(ci);

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (c:Customer{customer_id: customers_rel.customer_unique_id})
MATCH (o:Order{customer_id: customers_rel.customer_id})
CREATE (c)-[:PLACED]->(o);

MATCH (o:Order)
REMOVE o.customer_id;	//remove the attribute customer_id

DROP INDEX customer_order_id_index;

LOAD CSV WITH HEADERS FROM "file:///olist_sellers_dataset.csv" AS sellers_rel
WITH sellers_rel WHERE sellers_rel.seller_id IS NOT NULL
MATCH (s:Seller{seller_id: sellers_rel.seller_id})
MATCH (c:City{name_state: sellers_rel.seller_city + "-" + sellers_rel.seller_state})
MERGE (s)-[:HAS_HEADQUARTERS_IN]->(c);

:auto LOAD CSV WITH HEADERS FROM "file:///olist_order_items_dataset.csv" AS items_rel
CALL{
	WITH items_rel
	WITH items_rel WHERE items_rel.order_id IS NOT NULL AND items_rel.order_item_id IS NOT NULL AND items_rel.seller_id IS NOT NULL
	MATCH (i:Item{item_id: items_rel.order_id + "-" + items_rel.order_item_id})
	MATCH (o:Order{order_id: items_rel.order_id})
	MATCH (s:Seller{seller_id: items_rel.seller_id})
	MATCH (p:Product{product_id: items_rel.product_id})
	MERGE (o)-[:COMPOSED_OF]->(i)
	MERGE (i)-[:SOLD_BY]->(s)
	MERGE (i)-[:CORRESPONDS_TO]->(p)
}IN TRANSACTIONS OF 500 ROWS;

LOAD CSV WITH HEADERS FROM "file:///olist_products_dataset.csv" AS products_rel
WITH products_rel WHERE products_rel.product_id IS NOT NULL
MATCH (p:Product{product_id: products_rel.product_id})
MATCH (c:Category{name: products_rel.product_category_name})
MERGE (p)-[:BELONGS_TO]->(c);

LOAD CSV WITH HEADERS FROM "file:///olist_order_payments_dataset.csv" AS payments_rel
WITH payments_rel WHERE payments_rel.order_id IS NOT NULL
MATCH (o:Order{order_id: payments_rel.order_id})
CREATE (p:Payment{value: toFloat(payments_rel.payment_value), type: payments_rel.payment_type })
MERGE (o)-[:PAID_BY]->(p);

LOAD CSV WITH HEADERS FROM "file:///olist_order_reviews_dataset.csv" AS reviews_rel
WITH reviews_rel
MATCH (o:Order{order_id: reviews_rel.order_id})
MATCH (r:Review{review_id: reviews_rel.review_id})
MERGE (o)-[:RECEIVED]->(r);