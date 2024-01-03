// 1) For each category the percentage of customers of each origin in descending order. E.g.: for the comb 30 percent Italian customers and 70 Argentinians
MATCH (c:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH c, toFloat(count(cu)) AS customer_number	//for each category how many customers
MATCH (c)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH c.name AS Category, st.code AS State, customer_number, round(100*count(cust)/customer_number, 2) AS Percentage 
RETURN Category, State, Percentage //for each category the percentage of customers of each state
ORDER BY Category ASC, Percentage DESC

// 2) For each company, the percentage of profit that each nation generates in decreasing order. For example, company X receives 70 percent from Italy and 30 percent from France
MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer),
		(cu)-[:LIVES_IN]->(:City)-[:PART_OF]->(:State)
WITH s, sum(i.price) AS revenue
MATCH (s:Seller)<-[:SOLD_BY]-(it:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer),
		(cust)-[:LIVES_IN]->(:City)-[:PART_OF]->(st:State)
WITH s.seller_id as Seller, st.code as State, revenue, round(100*sum(it.price)/revenue, 2) AS Percentage
RETURN Seller, State, Percentage
ORDER BY Seller ASC, Percentage DESC;

// 3) For each year the ranking of the most profitable companies
MATCH (n:Order)
WITH DISTINCT n.purchase_timestamp.year AS Year
WITH Year, COLLECT{
    MATCH (s:Seller)<-[:SOLD_BY]-(i:Item)<-[:COMPOSED_OF]-(o:Order)
    WHERE o.purchase_timestamp.year = Year
    WITH s AS seller, round(sum(i.price), 2) AS Revenue
    RETURN {seller: seller.seller_id, revenue: Revenue}
    ORDER BY Revenue desc
    LIMIT 10
} as bs
UNWIND bs as BestSellers
RETURN Year, BestSellers.seller as Seller, BestSellers.revenue as Revenue
ORDER BY Year DESC, Revenue DESC;

// 4) For each company the percentage of profit coming from each product in descending order
MATCH (:Product)<-[:CORRESPONDS_TO]-(i:Item)-[:SOLD_BY]->(s:Seller)
WITH s, sum(i.price) AS revenue
MATCH (p:Product)<-[:CORRESPONDS_TO]-(it:Item)-[:SOLD_BY]->(s:Seller)
WITH s.seller_id as Seller, p.product_id as Product, revenue, round(100*sum(it.price)/revenue, 2) AS Profit_percentage
RETURN Seller, Product, Profit_percentage
ORDER BY Seller ASC, Profit_percentage DESC

// 5) For each category, the top ten products with the most positive average reviews
MATCH (cat:Category)
WITH cat, COLLECT{
    MATCH (cat)<-[:BELONGS_TO]-(p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
    WITH round(avg(r.score),2) as Average, p.product_id as Id
    RETURN {id: Id, score: Average}
    ORDER BY Average desc
    LIMIT 10
} as bs
UNWIND bs as BestSellers
RETURN cat.name as Category, BestSellers.id as Product, BestSellers.score as Score
ORDER BY Category ASC, Score DESC;
	
// 6) For each company, the ranking of products that have been reordered on average several times by the same customer

//Non riesco a capire come l'avevo pensata

MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(c:Customer),
		(p)<-[:CORRESPONDS_TO]-(:Item)-[:SOLD_BY]->(s:Seller)
RETURN s, c, COUNT(p) as how_many_times		//mi sa che cosi' conta i prodotti comprati dalla stessa azienda, non controlla che sia lo stesso
ORDER BY how_many_times desc

// 7) For each customer, the average purchase frequency for each category

// VECCHIA

MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cu:Customer)
WITH cu,cat,COUNT(cat) as for_each_category
MATCH (cate:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(cust:Customer)
WITH cust,for_each_category, COUNT(cate) as total_category
MATCH (categ:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(custo:Customer)
RETURN custo,categ,for_each_category,total_category, 100*for_each_category/total_category as percentage

// DA SISTEMARE (L'ho cambiata come For each category the average purchase frequency. Ho considerato due ordini effettuati lo stesso giorno come ordine unico. Ci sarebbe anche specificare che si tratta della frequenza degli ordini contenenti un item della categoria considerata; ordini contenti un numero diverso di item della stessa categoria sono stati trattati ugualmente. Se la vogliamo fare come fatta all'inizio basta cambiare solo la return)

MATCH (cat:Category)<-[:BELONGS_TO]-(:Product)<-[:CORRESPONDS_TO]-(Item)<-[:COMPOSED_OF]-(o:Order)<-[:PLACED]-(cu:Customer)
WITH cu.customer_id AS Customer, cat.name AS Category, datetime.truncate('day', o.purchase_timestamp) AS Date
WITH Customer, Category, COUNT(DISTINCT Date) as Count, toFloat(max(Date).epochSeconds-min(Date).epochSeconds) AS TimeSpan
WHERE Count>1
WITH Customer, Category, (365*24*3600*Count)/TimeSpan AS Frequency
RETURN Category, round(avg(Frequency), 2) AS Average_Frequency
ORDER BY Average_Frequency DESC
		
// 8) For each company, the percentage of collections made by credit card and boleto

// VECCHIA
MATCH (p:Payment)<-[:PAID_BY]-(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH s,COUNT(p) as total_payment
MATCH (p:Payment)<-[:PAID_BY]-(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WHERE p.type = "credit_card" OR p.type = "boleto"
RETURN s,total_payment, 100*COUNT(p)/total_payment as percentage

// FUNZIONANTE
MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
WITH DISTINCT o.purchase_timestamp.year AS Year, s.seller_id AS Seller, SUM(p.value) AS Total
WITH Seller, Year, Total, COLLECT{
    MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
    WHERE p.type = "credit_card" AND s.seller_id = Seller AND o.purchase_timestamp.year=Year
    WITH s, Total, 100*SUM(p.value)/Total as Credit_Card_Percentage
    RETURN Credit_Card_Percentage
}[0] AS Credit_Card, COLLECT{
    MATCH (p:Payment)<-[:PAID_BY]-(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
    WHERE p.type = "boleto" AND s.seller_id = Seller  AND o.purchase_timestamp.year=Year
    WITH s, Total, 100*SUM(p.value)/Total as Boleto_Percentage 
    RETURN Boleto_Percentage 
}[0] AS Boleto
RETURN Seller, Year, round(Credit_Card, 2) AS Credit_Card_Percentage, round(Boleto, 2) AS Boleto_Percentage, round(100-(Credit_Card+Boleto), 2) AS Other_percentage
ORDER BY Seller ASC, Year DESC

// 9) The average revenue generated by products grouped on the number of photos published

//DA SISTEMARE

MATCH (p:Payment)<-[:PAID_BY]-(:Order)-[:COMPOSED_OF]->(:Item)-[:CORRESPONDS_TO]->(pr:Product)
RETURN DISTINCT pr.photos_qty AS Quantity,SUM(p.value) AS Revenue
ORDER BY Quantity DESC

// 10) For each year, the most purchased product
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(o:Order)
	WHERE o.purchase_timestamp.year = Year
	WITH p AS product, COUNT(p) AS Quantity_Sold
	RETURN {product: product.product_id, quantity_sold: Quantity_Sold}
	ORDER BY Quantity_Sold DESC
	LIMIT 1
} as bs
UNWIND bs as Best_Sellers
RETURN Year, Best_Sellers.product as Product, Best_Sellers.quantity_sold as Quantity_Sold
ORDER by Year DESC

// 11) For each customer the last 10 products purchased
MATCH (c:Customer)
WITH DISTINCT c.customer_id AS Customer
WITH Customer, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:CORRESPONDS_TO]->(p:Product)
	WHERE c.customer_id = Customer
	WITH p AS product, o
	RETURN {product: product.product_id}
	ORDER BY o.purchase_timestamp DESC
	LIMIT 10
} as lp
UNWIND lp as Last_Products
RETURN Customer, Last_Products.product as Product

// 12) For each seller the last 10 products selled
MATCH (s:Seller)
WITH DISTINCT s.seller_id AS Seller
WITH Seller, COLLECT{
	MATCH (o:Order)-[:COMPOSED_OF]->(i:Item)-[:CORRESPONDS_TO]->(p:Product),
		(i)-[:SOLD_BY]->(s:Seller)
	WHERE s.seller_id = Seller
	WITH p AS product, o
	RETURN {product: product.product_id}
	ORDER BY o.purchase_timestamp DESC
	LIMIT 10
} AS lp
UNWIND lp AS Last_Products
RETURN Seller, Last_Products.product AS Product

// 13) The average weight of items shipped to each city
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)<-[:PLACED]-(:Customer)-[:LIVES_IN]->(c:City)
RETURN c, avg(p.weight_g) AS Target_Weigth

// 14) For each customer the percentage of the state provenience of his/her orders
MATCH (c:Customer)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller)
WITH c, toFloat(count(se)) ASASAS seller_number
MATCH (c)-[:PLACED]->(:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(se:Seller),
		(se)-[:HAS_HEADQUARTERS_IN]->(:City)-[:PART_OF]->(st:State)
RETURN c, st.code AS State, seller_number, round(100*count(se)/seller_number,2) AS Percentage
ORDER BY c.customer_id DESC, Percentage DESC

// 15) For each seller the worst reviewed product
MATCH (se:Seller)
WITH DISTINCT se.seller_id AS Seller
WITH Seller, COLLECT{
	MATCH (s:Seller)<-[:SOLD_BY]-(:Item)-[:CORRESPONDS_TO]->(p:Product),
			(p)<-[:CORRESPONDS_TO]-(:Item)<-[:COMPOSED_OF]-(:Order)-[:RECEIVED]->(r:Review)
	WHERE s.seller_id = Seller
	WITH p AS product, sum(r.score) AS Score
	RETURN {product: product.product_id, score: Score}
	ORDER BY Score ASC
	LIMIT 1
} AS wp
UNWIND wp AS Worst_Product
RETURN Seller, Worst_Product.product AS Product, Worst_Product.score AS Score

// 16) For each seller the average size and weight of products selled
MATCH (p:Product)<-[:CORRESPONDS_TO]-(:Item)-[:SOLD_BY]->(s:Seller)
RETURN s, avg(p.width_cm) AS AVG_width_cm, avg(p.height_cm) AS AVG_height_cm, avg(p.length_cm) AS AVG_length_cm, avg(p.weight_g) AS AVG_weight

// 17) For each customer, the company from which he/she bought the most
MATCH (c:Customer)
WITH DISTINCT c.customer_id AS Customer
WITH Customer, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
	WHERE c.customer_id = Customer
	WITH s, COUNT(DISTINCT(o)) as Number_Orders
	RETURN {seller: s.seller_id, number_orders: Number_Orders}
	ORDER BY Number_Orders DESC
	LIMIT 1
} AS tp
UNWIND tp AS Top
RETURN Customer, Top.seller AS Seller, Top.number_orders AS Number_of_Orders

// 18) For each customer, the reviews that he/she wrote
MATCH (c:Customer)-[:PLACED]->(:Order)-[:RECEIVED]->(re:Review)
RETURN c,re

// 19) For each year, the best company based on the reviews score
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (o:Order)-[:RECEIVED]->(r:Review),
			(o)-[:COMPOSED_OF]->(:Item)-[:SOLD_BY]->(s:Seller)
	WHERE o.purchase_timestamp.year = Year
	WITH s, sum(r.score) AS Score
	RETURN {seller: s.seller_id, score: Score}
	ORDER BY Score DESC
	LIMIT 1
} AS bs
UNWIND bs AS Best_Seller
RETURN Year, Best_Seller.seller AS Seller, Best_Seller.score AS Score

// 20) For each customer, how much he/she spent every year
MATCH (o:Order)
WITH DISTINCT o.purchase_timestamp.year AS Year
WITH Year, COLLECT{
	MATCH (c:Customer)-[:PLACED]->(o:Order)-[:PAID_BY]->(p:Payment)
	WHERE o.purchase_timestamp.year = Year
	WITH c,sum(p.value) AS Total_Spending
	RETURN {customer: c.customer_id, total_spending: Total_Spending}
} AS ts
UNWIND ts AS Total_Spending
RETURN Year, Total_Spending.customer AS Customer, Total_Spending.total_spending AS Total_Spending