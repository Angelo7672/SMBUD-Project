//Order
LOAD CSV WITH HEADERS FROM "file:///olist_orders_dataset.csv" AS orders
WITH orders WHERE orders.order_id IS NOT NULL
CREATE (o:Order{order_id: orders.order_id, 
	customer_id: orders.customer_id,
	purchase_timestamp: datetime({ epochMillis: apoc.date.parse(orders.order_purchase_timestamp, 'ms', 'YYYY-mm-dd HH:mm:ss') }),
	delivery_date: datetime({ epochMillis: apoc.date.parse(orders.order_estimated_delivery_date, 'ms', 'YYYY-mm-dd HH:mm:ss') })});

//Seller
LOAD CSV WITH HEADERS FROM "file:///olist_sellers_dataset.csv" AS sellers
WITH sellers WHERE sellers.seller_id IS NOT NULL
CREATE (s:Seller{seller_id: sellers.seller_id})
MERGE (c:City{name_state: sellers.seller_city + "-" + sellers.seller_state})
MERGE (st:State {code: sellers.seller_state });

//Customer
LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers
WITH customers WHERE customers.customer_id IS NOT NULL AND customers.customer_unique_id IS NOT NULL
CREATE (Customer{customer_id: customers.customer_unique_id});

:auto LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers
CALL {
    WITH customers
    WITH customers WHERE customers.customer_id IS NOT NULL AND customers.customer_unique_id IS NOT NULL
    MERGE (ct:City {name_state: customers.customer_city + "-" +customers.customer_state})
    MERGE (s:State {code: customers.customer_state })
} IN TRANSACTIONS OF 500 ROWS;

//Item
LOAD CSV WITH HEADERS FROM "file:///olist_order_items_dataset.csv" AS items
WITH items WHERE items.order_id IS NOT NULL AND items.order_item_id IS NOT NULL AND items.seller_id IS NOT NULL
CREATE (i:Item{item_id: items.order_id + "-" + items.order_item_id, price: items.price});

//Product
LOAD CSV WITH HEADERS FROM "file:///olist_products_dataset.csv" AS products
WITH products WHERE products.product_id IS NOT NULL
CREATE (p:Product{product_id: products.product_id, photos_qty: products.product_photos_qty, weight_g: products.product_weight_g, length_cm: products.product_length_cm, height_cm: products.product_height_cm, width_cm: products.product_width_cm})
MERGE (c:Category{name: coalesce(products.product_category_name,"NA")});

//Payment
LOAD CSV WITH HEADERS FROM "file:///olist_order_payments_dataset.csv" AS payments
WITH payments WHERE payments.order_id IS NOT NULL
CREATE (p:Payment{order_id: payments.order_id, value: payments.payment_value, type: payments.payment_type });

//Review DA CONTROLLARE
LOAD CSV WITH HEADERS FROM "file:///olist_order_reviews_dataset.csv" AS reviews
WITH reviews WHERE reviews.review_id IS NOT NULL
CREATE (r:Review{
  review_id: reviews.review_id, 
  score: reviews.review_score,
  comment_title: CASE WHEN trim(reviews.comment_title) = "" THEN null ELSE reviews.comment_title END,
  comment: CASE WHEN trim(reviews.review_comment_message) = "" THEN null ELSE reviews.review_comment_message END,
  creation_date: datetime({ epochMillis: apoc.date.parse(reviews.review_creation_date, 'ms', 'M/d/yyyy H:mm') }),
  answer_timestamp: datetime({ epochMillis: apoc.date.parse(reviews.review_answer_timestamp, 'ms', 'M/d/yyyy H:mm') })
});

//------Relation------

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (c:Customer{customer_id: customers_rel.customer_unique_id})
MATCH (o:Order{customer_id: customers_rel.customer_id})
MERGE (c)-[:PLACED]->(o);

MATCH (o:Order)
REMOVE o.customer_id;	//remove the attribute customer_id

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (ct:City {name_state: customers_rel.customer_city + "-" +customers_rel.customer_state})
MATCH (st:State {code: customers_rel.customer_state })
MERGE (ct)-[:PART_OF]->(st);

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (ct:City {name_state: customers_rel.customer_city + "-" +customers_rel.customer_state})
MATCH (st:State {code: customers_rel.customer_state })
MERGE (ct)-[:PART_OF]->(st);

LOAD CSV WITH HEADERS FROM "file:///olist_sellers_dataset.csv" AS sellers_rel
WITH sellers_rel WHERE sellers_rel.seller_id IS NOT NULL
MATCH (s:Seller{seller_id: sellers_rel.seller_id})
MATCH (c:City{name_state: sellers_rel.seller_city + "-" + sellers_rel.seller_state})
MATCH (st:State {code: sellers_rel.seller_state })
MERGE (c)-[:PART_OF]->(st)
MERGE (s)-[:HAS_HEADQUARTERS_IN]->(c);

LOAD CSV WITH HEADERS FROM "file:///olist_order_items_dataset.csv" AS items_rel
WITH items_rel WHERE items_rel.order_id IS NOT NULL AND items_rel.order_item_id IS NOT NULL AND items_rel.seller_id IS NOT NULL
MATCH (i:Item{item_id: items_rel.order_id + "-" + items_rel.order_item_id})
MATCH (o:Order{order_id: items_rel.order_id})
MERGE (o)-[:COMPOSED_OF]->(i)
WITH items_rel,i
MATCH (s:Seller{seller_id: items_rel.seller_id})
MERGE (i)-[:SOLD_BY]->(s)
WITH items_rel,i
MATCH (p:Product{product_id: items_rel.product_id})
MERGE (i)-[:CORRESPONDS_TO]->(p);

LOAD CSV WITH HEADERS FROM "file:///olist_products_dataset.csv" AS products_rel
WITH products_rel WHERE products_rel.product_id IS NOT NULL
MATCH (p:Product{product_id: products_rel.product_id})
MATCH (c:Category{name: products_rel.product_category_name})
MERGE (p)-[:BELONGS_TO]->(c);


//-----QUeste ultime due loopano all'infinito


LOAD CSV WITH HEADERS FROM "file:///olist_order_reviews_dataset.csv" AS reviews_rel
WITH reviews_rel WHERE reviews_rel.review_id IS NOT NULL
MATCH (r:Review{review_id: reviews_rel.review_id})
MATCH (o:Order{order_id: reviews_rel.order_id})
MERGE (o)-[:RECEIVED]->(r);


LOAD CSV WITH HEADERS FROM "file:///olist_order_payments_dataset.csv" AS payments_rel
WITH payments_rel WHERE payments_rel.order_id IS NOT NULL
MATCH (o:Order{order_id: payments_rel.order_id})
MATCH (p:Payment{order_id: payments_rel.order_id})
REMOVE p.order_id
MERGE (o)-[:PAID_BY]->(p);


//per evitare di modificare il file, ma non funziona
LOAD CSV WITH HEADERS FROM "file:///olist_order_reviews_dataset.csv" AS reviews
WITH reviews, replace(reviews.review_comment_message, "\'", "") AS comment_message
WHERE reviews.review_id IS NOT NULL
CREATE (r:Review {
  review_id: reviews.review_id, 
  score: reviews.review_score,
  comment_title: CASE WHEN trim(reviews.comment_title) = "" THEN null ELSE reviews.comment_title END,
  comment: CASE WHEN trim(comment_message) = "" THEN null ELSE comment_message END,
  creation_date: datetime({ epochMillis: apoc.date.parse(reviews.review_creation_date, 'ms', 'M/d/yyyy H:mm') }),
  answer_timestamp: datetime({ epochMillis: apoc.date.parse(reviews.review_answer_timestamp, 'ms', 'M/d/yyyy H:mm') })
});

