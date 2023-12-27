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

//------Relation------

CREATE INDEX order_customer_id_index FOR (n:Order) ON (n.customer_id);
CREATE INDEX customer_id_index FOR (n:Customer) ON (n.customer_id);	//questo mi sa che non serve

LOAD CSV WITH HEADERS FROM "file:///olist_customers_dataset.csv" AS customers_rel
WITH customers_rel WHERE customers_rel.customer_id IS NOT NULL AND customers_rel.customer_unique_id IS NOT NULL
MATCH (c:Customer{customer_id: customers_rel.customer_unique_id})
MATCH (o:Order{customer_id: customers_rel.customer_id})
MERGE (c)-[:PLACED]->(o);

MATCH (o:Order)
REMOVE o.customer_id;	//remove the attribute customer_id

CREATE INDEX state_index FOR(s:State) ON (s.code);	//questo mi sa che non serve
CREATE INDEX city_index FOR(c:City) ON (c.name_state);	//questo mi sa che non serve

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

CREATE INDEX seller_id_index FOR (n:Seller) ON (n.seller_id);	//questo mi sa che non serve

LOAD CSV WITH HEADERS FROM "file:///olist_sellers_dataset.csv" AS sellers_rel
WITH sellers_rel WHERE sellers_rel.seller_id IS NOT NULL
MATCH (s:Seller{seller_id: sellers_rel.seller_id})
MATCH (c:City{name_state: sellers_rel.seller_city + "-" + sellers_rel.seller_state})
MATCH (st:State {code: sellers_rel.seller_state })
MERGE (c)-[:PART_OF]->(st)
MERGE (s)-[:HAS_HEADQUARTERS_IN]->(c);




